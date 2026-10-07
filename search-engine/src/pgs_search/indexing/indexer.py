"""Search indexer: PostgreSQL Silver -> OpenSearch, continuously.

    python -m pgs_search.indexing.indexer        # the `search-indexer` service

Silver is the system of record; this keeps the OpenSearch index a copy of it. Every save
of a page in Silver re-queues it (processing_status UNPROCESSED). Each round:

1. `SearchRepository.claim_for_indexing` moves up to INDEXER_BATCH_SIZE canonical pages
   to PROCESSING (SKIP LOCKED, so several indexers can run) and returns them in the
   SearchDocument shape; the claim is committed before the slow part;
2. they are bulk-indexed with `_id` = `document_id` (= pages.id), so indexing a page
   twice overwrites it -- re-runs and duplicate claims are harmless;
3. each page is marked PROCESSED, or FAILED with OpenSearch's reason when it rejected that
   document (retried when the page next changes). If OpenSearch cannot be reached at
   all, the batch goes back to UNPROCESSED and the loop backs off.

Pages left in PROCESSING by an indexer that died are released after
INDEXER_STALE_AFTER_SECONDS. Duplicates (pages folded into another) are never claimed.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

from opensearchpy import OpenSearch, helpers
from opensearchpy.exceptions import ConnectionError as OpenSearchConnectionError
from opensearchpy.exceptions import ConnectionTimeout, TransportError

from pgs_search.config import Settings, settings
from pgs_search.indexing.index_manager import ensure_index
from pgs_search.indexing.mappings import INDEXED_FIELDS

logger = logging.getLogger(__name__)

SUMMARY_CHARS = 300
_UNREACHABLE = (OpenSearchConnectionError, ConnectionTimeout)


@dataclass
class BatchResult:
    claimed: int = 0
    indexed: int = 0
    failed: int = 0
    released: int = 0


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {key: _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    return value


def to_index_document(document: Mapping[str, Any], now: datetime | None = None) -> dict[str, Any]:
    """Project a SearchRepository document onto the mapped fields (mappings.py)."""
    text = document.get("searchable_text") or ""
    projected = {field: document.get(field) for field in INDEXED_FIELDS if field in document}
    projected["summary"] = text[:SUMMARY_CHARS]
    projected["content_length"] = len(text)
    projected["indexed_at"] = (now or datetime.now(UTC)).isoformat()
    projected["geo_tags"] = list(document.get("geo_tags") or [])
    return _jsonable(projected)


class Indexer:
    def __init__(self, session_factory: Any, client: OpenSearch, config: Settings = settings):
        self.session_factory = session_factory
        self.client = client
        self.config = config

    def run_once(self) -> BatchResult:
        from pgs_db.repositories.search import SearchRepository

        with self.session_factory() as session, session.begin():
            documents = SearchRepository(session).claim_for_indexing(self.config.indexer_batch_size)
        result = BatchResult(claimed=len(documents))
        if not documents:
            return result

        page_ids = [int(document["document_id"]) for document in documents]
        actions = [
            {
                "_op_type": "index",
                "_index": self.config.opensearch_index,
                "_id": str(document["document_id"]),
                "_source": to_index_document(document),
            }
            for document in documents
        ]
        try:
            _, errors = helpers.bulk(
                self.client,
                actions,
                raise_on_error=False,
                max_retries=3,  # 429 (queue full) backs off and retries
                request_timeout=max(30.0, self.config.opensearch_timeout_seconds),
            )
        except (*_UNREACHABLE, TransportError):
            self._release(page_ids)
            result.released = len(page_ids)
            raise

        failures = self._failures(errors)
        self._mark(page_ids, failures)
        result.failed = len(failures)
        result.indexed = len(page_ids) - result.failed
        return result

    @staticmethod
    def _failures(errors: Sequence[Any]) -> dict[int, str]:
        failures: dict[int, str] = {}
        for error in errors:
            item = next(iter(error.values())) if isinstance(error, dict) and error else {}
            if not isinstance(item, dict) or "_id" not in item:
                continue
            reason = item.get("error")
            failures[int(item["_id"])] = str(reason)[:1000] if reason else "rejected by OpenSearch"
        return failures

    def _mark(self, page_ids: Sequence[int], failures: Mapping[int, str]) -> None:
        from pgs_db.repositories.silver import SilverRepository

        with self.session_factory() as session, session.begin():
            repo = SilverRepository(session)
            for page_id in page_ids:
                try:
                    repo.mark_processed(page_id, error=failures.get(page_id))
                except LookupError:
                    # Deleted (e.g. quarantined) since the claim; nothing to record.
                    logger.info("page %s disappeared before it was marked", page_id)

    def _release(self, page_ids: Sequence[int]) -> None:
        """Put a claimed batch back in the queue (OpenSearch was unreachable)."""
        from pgs_db.enums import ProcessingStatus
        from pgs_db.models import Page
        from sqlalchemy import update

        with self.session_factory() as session, session.begin():
            session.execute(
                update(Page)
                .where(Page.id.in_(page_ids), Page.processing_status == ProcessingStatus.PROCESSING)
                .values(processing_status=ProcessingStatus.UNPROCESSED)
            )

    def release_stale(self) -> int:
        from pgs_db.repositories.search import SearchRepository

        with self.session_factory() as session, session.begin():
            return SearchRepository(session).release_stale_indexing(
                timedelta(seconds=self.config.indexer_stale_after_seconds)
            )


def _touch(path: str | None) -> None:
    if path:
        Path(path).write_text(str(int(time.time())))


def run_forever(indexer: Indexer, stop: threading.Event) -> None:
    """Index until `stop` is set: busy while there is work, a short sleep when idle, an
    exponential backoff (to 60 s) while OpenSearch or PostgreSQL is down."""
    from sqlalchemy.exc import OperationalError

    backoff = indexer.config.indexer_idle_seconds
    next_release = 0.0
    while not stop.is_set():
        try:
            if time.monotonic() >= next_release:
                released = indexer.release_stale()
                if released:
                    logger.warning("released %d page(s) claimed by a dead indexer", released)
                next_release = time.monotonic() + 60
            result = indexer.run_once()
        except (*_UNREACHABLE, TransportError, OperationalError) as exc:
            logger.warning("dependency unavailable, retrying in %.0f s: %s", backoff, exc)
            stop.wait(backoff)
            backoff = min(backoff * 2, 60.0)
            continue
        backoff = indexer.config.indexer_idle_seconds
        _touch(indexer.config.indexer_heartbeat_file)
        if result.claimed:
            logger.info(
                "indexed %d page(s), %d rejected by OpenSearch", result.indexed, result.failed
            )
        else:
            stop.wait(indexer.config.indexer_idle_seconds)


def main() -> None:
    from pgs_db import make_session_factory

    from pgs_search.client.opensearch import get_opensearch_client

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    client = get_opensearch_client()
    ensure_index(client)
    stop = threading.Event()
    signal.signal(signal.SIGTERM, lambda *_: stop.set())
    signal.signal(signal.SIGINT, lambda *_: stop.set())
    logger.info("indexing PostgreSQL Silver into %s", settings.opensearch_index)
    run_forever(Indexer(make_session_factory(), client), stop)
    logger.info("indexer stopped")


if __name__ == "__main__":
    main()
