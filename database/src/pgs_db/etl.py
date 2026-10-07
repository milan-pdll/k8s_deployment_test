"""The Bronze -> Silver loop, ready to call from Spark or Airflow.

The ETL supplies only the transform: a function from one Bronze row to a §5.2
payload (`docs/bronze-silver-contract.md` §3). Everything around it -- claiming
rows, one transaction per page, domain geo rules, deduplication, marking rows
PROCESSED or FAILED -- is done here, the same way every time:

    from pgs_db import make_session_factory
    from pgs_db.etl import process_bronze_batch, process_stored_file_batch

    Session = make_session_factory()
    while (result := process_bronze_batch(Session, transform_page)).claimed:
        print(result)
    process_stored_file_batch(Session, transform_pdf)   # PDFs / images in MinIO

The ETL's current pipeline (Kafka signal -> `transform_file` -> record) plugs in
without rewriting its transform: `save_transformed(Session, record,
geo_confidence=...)` finds the record's Bronze row by its MinIO `object_key` and
saves it; `payload_from_transform` does just the conversion.

A transform that raises marks just that row FAILED with the error; the batch
carries on. A transform whose virus scan flags the payload raises `Infected`
instead, after moving the object to the quarantine bucket: the row is recorded in
`quarantined_files` and parked as QUARANTINED, never FAILED. Run
`SilverRepository.release_stale` on a schedule to recover rows
from workers that died mid-batch.
"""

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.orm import Session, sessionmaker

from .ingest import upsert_run
from .models import DEFAULT_EMBEDDING_MODEL, CrawledDocument, Page, StoredFile
from .repositories.bronze import BronzeRepository
from .repositories.reference import ReferenceRepository
from .repositories.silver import SilverRepository

PageTransform = Callable[[CrawledDocument], Mapping[str, Any]]
FileTransform = Callable[[StoredFile], Mapping[str, Any]]

# Long enough to diagnose, short enough not to bloat the queue tables.
_MAX_ERROR_LENGTH = 2000


class Infected(Exception):
    """Raise from a transform when ClamAV flags the payload.

    Move the object to the quarantine bucket first, then raise with where it went:

        verdict = clamav.scan(data)
        if verdict.infected:
            path = move_to_quarantine(stored.storage_path)
            raise Infected(verdict.signature, path, scanner_version=verdict.version)
    """

    def __init__(
        self, threat_signature: str, quarantine_path: str, *, scanner_version: str | None = None
    ) -> None:
        super().__init__(f"{threat_signature} (isolated at {quarantine_path})")
        self.threat_signature = threat_signature
        self.quarantine_path = quarantine_path
        self.scanner_version = scanner_version


@dataclass
class BatchResult:
    """What one batch did. `errors` maps a Bronze row id to why it failed."""

    claimed: int = 0
    saved: int = 0
    duplicates: int = 0
    quarantined: int = 0
    failed: int = 0
    errors: dict[int, str] = field(default_factory=dict)


def process_bronze_batch(
    session_factory: sessionmaker[Session],
    transform: PageTransform,
    *,
    limit: int = 100,
    dedup: bool = True,
    domain_geo: bool = True,
) -> BatchResult:
    """Claim up to `limit` crawled pages, transform and save each, then mark it.

    `content_hash` and `sim_hash` come from the Bronze row unless the payload
    sets them. See `_save_one` for `dedup` and `domain_geo`.
    """
    with session_factory() as s, s.begin():
        claimed = SilverRepository(s).claim_bronze(limit)

    result = BatchResult(claimed=len(claimed))
    for doc in claimed:

        def save(repo: SilverRepository, payload: Mapping[str, Any], doc=doc) -> int:
            saved = repo.save_page(
                payload,
                crawled_document_id=doc.id,
                domain_id=doc.domain_id,
                content_hash=payload.get("content_hash") or doc.content_hash,
                sim_hash=payload.get("sim_hash", doc.sim_hash),
            )
            repo.mark_bronze_processed([doc.id])
            return saved.id

        _save_one(
            session_factory,
            result,
            row_id=doc.id,
            domain_id=doc.domain_id,
            build=lambda doc=doc: transform(doc),
            save=save,
            quarantine=lambda repo, inf, doc=doc: repo.quarantine(
                crawled_document_id=doc.id,
                threat_signature=inf.threat_signature,
                quarantine_path=inf.quarantine_path,
                scanner_version=inf.scanner_version,
            ),
            mark_failed=SilverRepository.mark_bronze_failed,
            dedup=dedup,
            domain_geo=domain_geo,
        )
    return result


def process_stored_file_batch(
    session_factory: sessionmaker[Session],
    transform: FileTransform,
    *,
    limit: int = 100,
    dedup: bool = True,
    domain_geo: bool = True,
) -> BatchResult:
    """`process_bronze_batch` for files in MinIO (PDFs, images, docs).

    The transform reads the file at `stored_file.storage_path` and returns the
    payload; the file's `sha256` is the content hash unless the payload sets
    one. The domain is the one of the page that linked the file, if known.
    """
    with session_factory() as s, s.begin():
        claimed = SilverRepository(s).claim_stored_files(limit)
        domains = {
            f.id: (f.crawled_document.domain_id if f.crawled_document else None) for f in claimed
        }

    result = BatchResult(claimed=len(claimed))
    for stored in claimed:
        domain_id = domains[stored.id]

        def save(
            repo: SilverRepository,
            payload: Mapping[str, Any],
            stored=stored,
            domain_id=domain_id,
        ) -> int:
            saved = repo.save_page(
                payload,
                stored_file_id=stored.id,
                domain_id=domain_id,
                content_hash=payload.get("content_hash") or stored.sha256,
            )
            repo.mark_stored_files_processed([stored.id])
            return saved.id

        _save_one(
            session_factory,
            result,
            row_id=stored.id,
            domain_id=domain_id,
            build=lambda stored=stored: transform(stored),
            save=save,
            quarantine=lambda repo, inf, stored=stored: repo.quarantine(
                stored_file_id=stored.id,
                threat_signature=inf.threat_signature,
                quarantine_path=inf.quarantine_path,
                scanner_version=inf.scanner_version,
            ),
            mark_failed=SilverRepository.mark_stored_file_failed,
            dedup=dedup,
            domain_geo=domain_geo,
        )
    return result


# ------------------------------------------------- the ETL's transform.py output


def payload_from_transform(document: Mapping[str, Any], *, geo_confidence: float) -> dict[str, Any]:
    """Turn one record of `ETL/spark/transform.py` (`transform_document` /
    `transform_file`) into a `save_page` payload.

    - `content_sha256` -> `content_hash`; `simhash` (hex) is read by `save_page`.
    - `geo_location`'s place names are kept: `save_page` resolves them to codes
      through the gazetteer. `resolve_geo` gives no confidence, so the caller says
      how much it trusts it (`geo_confidence`, 0..1) -- a guessed default would
      skew ranking.
    - A document-level `embedding` (LaBSE, the mean of its chunks) becomes one
      chunk under its `embedding_model`.
    - `duplicate` / `duplicate_of` are dropped: Silver deduplicates on save.
    """
    if not 0.0 <= geo_confidence <= 1.0:
        raise ValueError("geo_confidence must be within 0..1")
    text = document.get("searchable_text") or ""
    payload: dict[str, Any] = {
        "source_url": document.get("source_url") or None,
        "title": document.get("title") or None,
        "searchable_text": text,
        "language_detected": document.get("language_detected"),
        "word_count": document.get("word_count"),
        "content_hash": document.get("content_sha256") or document.get("content_hash"),
        "simhash": document.get("simhash"),
    }
    for passthrough in ("content_type", "category", "published_at", "description", "keywords"):
        if document.get(passthrough) is not None:
            payload[passthrough] = document[passthrough]
    geo = document.get("geo_location")
    if geo:
        payload["geo_location"] = {
            **geo,
            "method": geo.get("method") or "GAZETTEER",
            "confidence": geo.get("confidence", geo_confidence),
        }
    embedding = document.get("embedding")
    if embedding:
        payload["embeddings"] = {
            "model_name": document.get("embedding_model") or DEFAULT_EMBEDDING_MODEL,
            "chunks": [{"chunk_index": 0, "text": text[:2000] or "(empty)", "vector": embedding}],
        }
    return payload


@dataclass
class TransformedSave:
    """Where one transform.py record landed."""

    page_id: int
    crawled_document_id: int | None
    stored_file_id: int | None
    duplicate: bool


def save_transformed(
    session_factory: sessionmaker[Session],
    document: Mapping[str, Any],
    *,
    geo_confidence: float,
    bronze_document: Mapping[str, Any] | None = None,
    dedup: bool = True,
    domain_geo: bool = True,
) -> TransformedSave:
    """Save one transform.py record for the Kafka-signal pipeline, in one transaction.

    The Bronze row is found by the record's MinIO `object_key` (a stored file's
    `storage_path`, or a crawled page's `minio_path`), else by `source_url` (the
    newest crawl of it). That row is marked PROCESSED. Raises LookupError when the
    scraper never wrote the row: Silver never holds a page without its Bronze source.

    `bronze_document`: the scraper's `Document` as it arrived on Kafka
    (`--storage=kafka`, where nothing else writes Bronze). It is saved to Bronze
    first, in the same transaction, and becomes the page's source.
    """
    payload = payload_from_transform(document, geo_confidence=geo_confidence)
    object_key = document.get("object_key") or None
    source_url = document.get("source_url") or None
    with session_factory() as s, s.begin():
        repo = SilverRepository(s)
        stored = None
        crawled = None
        if bronze_document is not None:
            run_id = bronze_document.get("crawl_run_id") or None
            if run_id is not None:
                upsert_run(s, int(run_id), None)  # FK target; ingest fills it in later
            saved_doc = BronzeRepository(s).save_document(
                bronze_document,
                crawl_run_id=run_id,
                minio_path=object_key,
                register_unknown_domains=True,
            )
            crawled = s.get(CrawledDocument, saved_doc.id)
        elif object_key:
            stored = s.scalars(
                select(StoredFile).where(StoredFile.storage_path == object_key)
            ).first()
        if stored is None and crawled is None:
            conditions = []
            if object_key:
                conditions.append(CrawledDocument.minio_path == object_key)
            if source_url:
                conditions.append(CrawledDocument.normalized_url == source_url)
                conditions.append(CrawledDocument.url == source_url)
            if conditions:
                crawled = s.scalars(
                    select(CrawledDocument)
                    .where(or_(*conditions))
                    .order_by(CrawledDocument.fetched_at.desc(), CrawledDocument.id.desc())
                    .limit(1)
                ).first()
        if stored is None and crawled is None:
            raise LookupError(
                f"no Bronze row for object_key={object_key!r} / source_url={source_url!r}"
            )
        domain_id = (
            crawled.domain_id
            if crawled is not None
            else (stored.crawled_document.domain_id if stored and stored.crawled_document else None)
        )
        if domain_geo and payload.get("geo_location") is None:
            geo = ReferenceRepository(s).geo_for_domain(domain_id)
            if geo is not None:
                payload["geo_location"] = geo
        if stored is not None:
            # A file's source_url is the page that linked it; as the canonical URL it
            # would overwrite that page. The file keeps its own document_url.
            payload.pop("source_url", None)
            payload["content_hash"] = payload.get("content_hash") or stored.sha256
            saved = repo.save_page(payload, stored_file_id=stored.id, domain_id=domain_id)
            repo.mark_stored_files_processed([stored.id])
        else:
            assert crawled is not None
            payload["content_hash"] = payload.get("content_hash") or crawled.content_hash
            saved = repo.save_page(payload, crawled_document_id=crawled.id, domain_id=domain_id)
            repo.mark_bronze_processed([crawled.id])
        duplicate = dedup and _fold_duplicate(repo, saved.id)
        return TransformedSave(
            page_id=saved.id,
            crawled_document_id=crawled.id if crawled is not None else None,
            stored_file_id=stored.id if stored is not None else None,
            duplicate=duplicate,
        )


def _save_one(
    session_factory: sessionmaker[Session],
    result: BatchResult,
    *,
    row_id: int,
    domain_id: int | None,
    build: Callable[[], Mapping[str, Any]],
    save: Callable[[SilverRepository, Mapping[str, Any]], int],
    quarantine: Callable[[SilverRepository, Infected], object],
    mark_failed: Callable[[SilverRepository, int, str], None],
    dedup: bool,
    domain_geo: bool,
) -> None:
    """Transform, save and mark one row in a single transaction; on error, mark it FAILED.

    `domain_geo`: a payload with no `geo_location` on a local body's own site is
    tagged with that local body (ETL stage 4 domain rule).
    `dedup`: the page is folded into an older page with the same content hash,
    or within 3 SimHash bits (ETL stage 3); a page whose content no longer
    matches its old original is unfolded.
    """
    try:
        payload = dict(build())
        with session_factory() as s, s.begin():
            repo = SilverRepository(s)
            if domain_geo and payload.get("geo_location") is None:
                geo = ReferenceRepository(s).geo_for_domain(domain_id)
                if geo is not None:
                    payload["geo_location"] = geo
            page_id = save(repo, payload)
            if dedup and _fold_duplicate(repo, page_id):
                result.duplicates += 1
        result.saved += 1
    except Infected as infected:
        with session_factory() as s, s.begin():
            quarantine(SilverRepository(s), infected)
        result.quarantined += 1
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"[:_MAX_ERROR_LENGTH]
        with session_factory() as s, s.begin():
            mark_failed(SilverRepository(s), row_id, message)
        result.failed += 1
        result.errors[row_id] = message


def _fold_duplicate(repo: SilverRepository, page_id: int) -> bool:
    """Point the page at an older original if one exists. Returns True if folded.

    Only older pages are candidates, so two pages can never end up pointing at
    each other.
    """
    page = repo.session.get(Page, page_id)
    assert page is not None
    original = repo.find_exact_duplicate(page.content_hash, exclude_page_id=page.id)
    if (original is None or original.id > page.id) and page.sim_hash is not None:
        near = [
            p for p, _ in repo.find_near_duplicates(page.sim_hash, exclude_page_id=page.id)
            if p.id < page.id
        ]
        original = near[0] if near else None
    if original is not None and original.id < page.id:
        repo.mark_duplicate_of(page.id, original.id)
        return True
    page.duplicate_of_id = None
    return False
