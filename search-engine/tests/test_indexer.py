from __future__ import annotations

from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, ClassVar

import pytest
from opensearchpy.exceptions import ConnectionError as OpenSearchConnectionError

from pgs_search.config import settings
from pgs_search.indexing import indexer as indexer_module
from pgs_search.indexing.indexer import Indexer, to_index_document
from pgs_search.indexing.mappings import INDEXED_FIELDS, index_body

DOCUMENT = {
    "document_id": "42",
    "title": "Budget",
    "description": None,
    "searchable_text": "x" * 1000,
    "source_url": "https://pokharamun.gov.np/budget",
    "domain": "pokharamun.gov.np",
    "language": "ne",
    "content_type": "web_page",
    "category": "government",
    "published_at": datetime(2026, 9, 30, tzinfo=UTC),
    "keywords": ["budget"],
    "geo": {"province_code": "P4", "district_code": "D38", "municipality_id": "MUN414"},
    "geo_location": {"province_code": "P4"},  # a duplicate block the index does not keep
    "geo_tags": [{"province_code": "P4", "district_code": "D38"}],
    "file_info": None,
}


def test_projection_keeps_exactly_the_mapped_fields() -> None:
    projected = to_index_document(DOCUMENT, now=datetime(2026, 10, 1, tzinfo=UTC))
    mapped = set(index_body()["mappings"]["properties"])
    assert set(projected) <= mapped
    assert set(INDEXED_FIELDS) == mapped
    assert "geo_location" not in projected
    assert projected["published_at"] == "2026-09-30T00:00:00+00:00"
    assert projected["summary"] == "x" * indexer_module.SUMMARY_CHARS
    assert projected["content_length"] == 1000
    assert projected["indexed_at"] == "2026-10-01T00:00:00+00:00"


class FakeSession:
    @contextmanager
    def begin(self):
        yield self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeSearchRepository:
    queue: ClassVar[list[list[dict[str, Any]]]] = []

    def __init__(self, session):
        pass

    def claim_for_indexing(self, limit):
        return FakeSearchRepository.queue.pop(0) if FakeSearchRepository.queue else []


class FakeSilverRepository:
    marked: ClassVar[list[tuple[int, str | None]]] = []

    def __init__(self, session):
        pass

    def mark_processed(self, page_id, *, error=None):
        FakeSilverRepository.marked.append((page_id, error))


@pytest.fixture
def repos(monkeypatch):
    import pgs_db.repositories.search as search_repo
    import pgs_db.repositories.silver as silver_repo

    FakeSearchRepository.queue = []
    FakeSilverRepository.marked = []
    monkeypatch.setattr(search_repo, "SearchRepository", FakeSearchRepository)
    monkeypatch.setattr(silver_repo, "SilverRepository", FakeSilverRepository)
    return FakeSearchRepository, FakeSilverRepository


def test_a_batch_is_indexed_by_page_id_and_marked(repos, monkeypatch) -> None:
    search_repo, silver_repo = repos
    search_repo.queue = [[DOCUMENT, {**DOCUMENT, "document_id": "43"}]]
    sent: list[list[dict[str, Any]]] = []

    def fake_bulk(client, actions, **kwargs):
        actions = list(actions)
        sent.append(actions)
        rejected = {"index": {"_id": "43", "status": 400, "error": {"type": "mapper_parsing"}}}
        return 1, [rejected]

    monkeypatch.setattr(indexer_module.helpers, "bulk", fake_bulk)
    result = Indexer(FakeSession, client=object(), config=settings).run_once()

    assert (result.claimed, result.indexed, result.failed) == (2, 1, 1)
    assert [action["_id"] for action in sent[0]] == ["42", "43"]
    assert sent[0][0]["_index"] == settings.opensearch_index
    assert silver_repo.marked[0] == (42, None)
    assert silver_repo.marked[1][0] == 43 and "mapper_parsing" in silver_repo.marked[1][1]


def test_an_unreachable_opensearch_puts_the_batch_back(repos, monkeypatch) -> None:
    search_repo, silver_repo = repos
    search_repo.queue = [[DOCUMENT]]
    released: list[list[int]] = []

    def failing_bulk(client, actions, **kwargs):
        list(actions)
        raise OpenSearchConnectionError("N/A", "refused", Exception("refused"))

    monkeypatch.setattr(indexer_module.helpers, "bulk", failing_bulk)
    monkeypatch.setattr(Indexer, "_release", lambda self, ids: released.append(list(ids)))
    with pytest.raises(OpenSearchConnectionError):
        Indexer(FakeSession, client=object(), config=settings).run_once()
    assert released == [[42]]
    assert silver_repo.marked == []


def test_an_empty_queue_does_nothing(repos) -> None:
    result = Indexer(FakeSession, client=object(), config=settings).run_once()
    assert result.claimed == 0
