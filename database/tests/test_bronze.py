from datetime import UTC, datetime
from typing import Any

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from pgs_db.enums import ProcessingStatus
from pgs_db.models import CrawledDocument, CrawlRun, StoredFile

NOW = datetime(2026, 9, 22, tzinfo=UTC)


def make_doc(**kw: Any) -> CrawledDocument:
    base = {
        "url": "https://mofaga.gov.np/notice/1?utm=x",
        "normalized_url": "https://mofaga.gov.np/notice/1",
        "host": "mofaga.gov.np",
        "title": "सूचना",
        "content_hash": "a" * 64,
        "fetched_at": NOW,
        "status_code": 200,
    }
    base.update(kw)
    return CrawledDocument(**base)


def test_document_with_run_and_file(session: Session) -> None:
    run = CrawlRun(started_at=NOW, category="government")
    doc = make_doc(
        crawl_run=run,
        meta_keywords=["notice", "सूचना"],
        headings=[{"level": 1, "text": "सूचना"}],
        open_graph={"og:title": "Notice"},
        emails=["info@mofaga.gov.np"],
    )
    doc.stored_files.append(
        StoredFile(
            source_page_url=doc.url,
            document_url="https://mofaga.gov.np/files/a.pdf",
            storage_path="raw/documents/mofaga.gov.np/a.pdf",
            sha256="b" * 64,
            size_bytes=1024,
            stored_at=NOW,
        )
    )
    session.add(doc)
    session.flush()

    got = session.scalars(select(CrawledDocument)).one()
    assert got.processing_status == ProcessingStatus.UNPROCESSED
    assert got.crawl_run_id == run.id
    assert got.meta_keywords == ["notice", "सूचना"]
    assert got.stored_files[0].size_bytes == 1024


def test_same_url_and_hash_is_rejected(session: Session) -> None:
    session.add(make_doc())
    session.flush()
    session.add(make_doc(url="https://mofaga.gov.np/notice/1?ref=fb"))
    with pytest.raises(IntegrityError):
        session.flush()


def test_changed_content_is_a_new_row(session: Session) -> None:
    session.add_all([make_doc(), make_doc(content_hash="c" * 64)])
    session.flush()
    assert len(session.scalars(select(CrawledDocument)).all()) == 2


def test_uint64_simhash_round_trips(session: Session) -> None:
    simhash = 0xFEDC_BA98_7654_3210  # > int64 max
    signed = simhash - (1 << 64) if simhash >= 1 << 63 else simhash  # Go: int64(v)
    session.add(make_doc(sim_hash=signed))
    session.flush()
    stored = session.scalars(select(CrawledDocument.sim_hash)).one()
    assert stored is not None  # the column is nullable; this row must have a value
    assert stored & 0xFFFF_FFFF_FFFF_FFFF == simhash  # Go: uint64(v)


def test_plain_sql_insert_like_go_writer(session: Session) -> None:
    """The Go scraper inserts plain strings; no enum casts needed."""
    session.execute(
        text(
            "INSERT INTO crawled_documents (url, normalized_url, content_hash, fetched_at, "
            "depth, processing_status, links) VALUES (:u, :u, :h, now(), 1, 'PROCESSING', :l)"
        ),
        {"u": "https://example.np/", "h": "d" * 64, "l": ["https://example.np/a"]},
    )


def test_invalid_status_is_rejected(session: Session) -> None:
    with pytest.raises(IntegrityError):
        session.execute(
            text(
                "INSERT INTO crawled_documents (url, normalized_url, content_hash, fetched_at, "
                "depth, processing_status) VALUES ('u', 'u', 'h', now(), 0, 'DONE')"
            )
        )
