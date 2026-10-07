"""The production pipeline's use of pgs_db, end to end, as the real service roles.

ETL/spark/site_pipeline.py (role pgs_etl) saves every transformed page with
`save_transformed(..., bronze_document=<the scraper's Document JSON>)`; the search
indexer (role pgs_search) claims the page, indexes it and marks it; the search engine
(role pgs_search) runs `vector_search` with a LaBSE query vector. Each step runs under
SET ROLE, so a missing grant fails here and not in production. Everything happens in
the test's outer transaction and is rolled back.
"""

from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import Connection, select, text
from sqlalchemy.orm import Session, sessionmaker

from pgs_db import make_engine
from pgs_db.enums import LogSeverity, ProcessingStatus, ServiceName
from pgs_db.etl import save_transformed
from pgs_db.models import CrawledDocument, CrawlRun, Page, PageEmbedding
from pgs_db.repositories.ops import OpsRepository
from pgs_db.repositories.search import SearchRepository
from pgs_db.repositories.silver import SilverRepository

# A seeded local-government site (database/data/domains.json links it to its local body).
HOST = "pokharamun.gov.np"
URL = f"https://{HOST}/notice/budget-2083"
RUN_ID = 990_001


def labse_vector(seed: int) -> list[float]:
    vector = [0.0] * 768
    vector[seed % 768] = 1.0
    return vector


def scraper_document(**overrides: Any) -> dict[str, Any]:
    """scraper/internal/model/document.go as stored in S3."""
    document: dict[str, Any] = {
        "url": URL + "?utm_source=fb",
        "normalized_url": URL,
        "host": HOST,
        "final_url": URL,
        "category": "government",
        "title": "Budget 2083",
        "meta_description": "Annual budget notice",
        "meta_keywords": ["budget", "notice"],
        "text": "Budget notice for the fiscal year 2083/84.",
        "links": [f"https://{HOST}/"],
        "crawl_run_id": RUN_ID,
        "html_key": f"html/{HOST}/{'a' * 64}.html",
        "depth": 1,
        "status_code": 200,
        "content_type": "text/html; charset=utf-8",
        "content_hash": "a" * 64,
        "sim_hash": 18_000_000_000_000_000_000,  # uint64 with the top bit set
        "fetched_at": "2026-10-01T10:00:00Z",
        "fetch_duration_ms": 120,
    }
    document.update(overrides)
    return document


def etl_record(**overrides: Any) -> dict[str, Any]:
    """What ETL/spark/site_pipeline.py process_page + finish_group produce."""
    record: dict[str, Any] = {
        "source_url": URL,
        "object_key": f"s3://crawled-pages/html/{HOST}/{'a' * 64}.html",
        "target_domain": HOST,
        "title": "Budget 2083",
        "description": "Annual budget notice",
        "language_detected": "en",
        "searchable_text": "Budget notice for the fiscal year 2083/84 of Pokhara.",
        "word_count": 10,
        "char_count": 54,
        "content_sha256": "b" * 64,
        "simhash": "f" * 16,  # unsigned, top bit set
        "crawl_run_id": RUN_ID,
        "fetched_at": "2026-10-01T10:00:00Z",
        "category": "government",
        "keywords": ["budget", "notice"],
        "published_at": "2026-09-30T08:00:00+00:00",
        "security_scan": {"verdict": "SAFE", "clamav_status": "CLEAN", "sha256": "c" * 64},
        "embedding": labse_vector(3),
        "embedding_model": "sentence-transformers/LaBSE",
        "embedding_dim": 768,
    }
    record.update(overrides)
    return record


@pytest.fixture()
def connection() -> Iterator[Connection]:
    conn = make_engine().connect()
    trans = conn.begin()
    try:
        yield conn
    finally:
        conn.execute(text("RESET ROLE"))
        trans.rollback()
        conn.close()


def save(etl: sessionmaker[Session], record: dict[str, Any], document: dict[str, Any]) -> Any:
    return save_transformed(etl, record, geo_confidence=0.5, bronze_document=document)


def as_role(conn: Connection, role: str) -> sessionmaker[Session]:
    conn.execute(text(f"SET ROLE {role}"))
    return sessionmaker(bind=conn, join_transaction_mode="create_savepoint")


def test_etl_save_index_and_search_as_the_service_roles(connection: Connection) -> None:
    etl = as_role(connection, "pgs_etl")
    saved = save_transformed(
        etl, etl_record(), geo_confidence=0.5, bronze_document=scraper_document()
    )
    assert saved.duplicate is False

    with etl() as s:
        page = s.get(Page, saved.page_id)
        assert page is not None
        assert page.canonical_url == URL
        assert page.processing_status == ProcessingStatus.UNPROCESSED
        assert s.scalar(select(CrawlRun.id).where(CrawlRun.id == RUN_ID)) == RUN_ID
        bronze = s.get(CrawledDocument, saved.crawled_document_id)
        assert bronze is not None and bronze.crawl_run_id == RUN_ID
        embeddings = s.scalars(select(PageEmbedding).where(PageEmbedding.page_id == page.id)).all()
        assert [(e.model_name, e.dimensions) for e in embeddings] == [
            ("sentence-transformers/LaBSE", 768)
        ]
    # The ETL also records page failures and dead-lettered sites.
    with etl() as s, s.begin():
        OpsRepository(s).log_error(
            ServiceName.ETL, LogSeverity.ERROR, "dead letter", error_type="SITE_DEAD_LETTERED"
        )

    search = as_role(connection, "pgs_search")
    with search() as s, s.begin():
        documents = SearchRepository(s).claim_for_indexing(100)
    by_id = {doc["document_id"]: doc for doc in documents}
    document = by_id[str(saved.page_id)]
    assert document["source_url"] == URL
    assert document["domain"] == HOST
    assert document["language"] == "en"
    # Pokhara's own site: its pages are located by the domain's local body.
    assert document["geo"]["district_code"]
    with search() as s, s.begin():
        SilverRepository(s).mark_processed(saved.page_id)
    with search() as s:
        hits = SearchRepository(s).vector_search(labse_vector(3), limit=5)
        assert hits[0]["page_id"] == saved.page_id
        assert hits[0]["score"] == pytest.approx(1.0)
        filtered = SearchRepository(s).vector_search(
            labse_vector(3), district_code=document["geo"]["district_code"], limit=5
        )
        assert saved.page_id in {hit["page_id"] for hit in filtered}


def test_reprocessing_a_site_is_idempotent(connection: Connection) -> None:
    etl = as_role(connection, "pgs_etl")
    first = save(etl, etl_record(), scraper_document())
    second = save(etl, etl_record(), scraper_document())
    assert second.page_id == first.page_id
    with etl() as s:
        assert s.scalar(select(Page.version).where(Page.id == first.page_id)) == 1
        count = s.scalar(
            select(text("count(*)")).select_from(PageEmbedding).where(
                PageEmbedding.page_id == first.page_id
            )
        )
        assert count == 1


def test_changed_content_bumps_the_page_version(connection: Connection) -> None:
    etl = as_role(connection, "pgs_etl")
    first = save(etl, etl_record(), scraper_document())
    changed = etl_record(content_sha256="d" * 64, searchable_text="Revised budget notice.")
    second = save_transformed(
        etl,
        changed,
        geo_confidence=0.5,
        bronze_document=scraper_document(content_hash="d" * 64, fetched_at="2026-10-02T10:00:00Z"),
    )
    assert second.page_id == first.page_id
    with etl() as s:
        assert s.scalar(select(Page.version).where(Page.id == first.page_id)) == 2


def test_an_unknown_host_is_registered(connection: Connection) -> None:
    etl = as_role(connection, "pgs_etl")
    host = "brand-new-site.example.np"
    saved = save_transformed(
        etl,
        etl_record(source_url=f"https://{host}/a", target_domain=host),
        geo_confidence=0.5,
        bronze_document=scraper_document(
            url=f"https://{host}/a", normalized_url=f"https://{host}/a", host=host
        ),
    )
    with etl() as s:
        page = s.get(Page, saved.page_id)
        assert page is not None and page.domain_id is not None
