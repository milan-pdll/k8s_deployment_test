"""quarantined_files: the ETL records infected payloads, the API lists and deletes them."""

from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import delete, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, sessionmaker

from pgs_db import BronzeRepository, QuarantineRepository, SilverRepository, make_engine
from pgs_db.enums import ProcessingStatus, QuarantineStatus
from pgs_db.etl import Infected, process_bronze_batch, process_stored_file_batch
from pgs_db.models import CrawledDocument, Page, QuarantinedFile, StoredFile
from pgs_db.schemas import QuarantinedFileRead, QuarantineSummary

HOST = "unsecuresite.com.np"
EXE_URL = f"https://{HOST}/files/update.exe"
SIGNATURE = "Win.Trojan.Generic-998"
_ANCIENT = datetime(1975, 1, 1, tzinfo=UTC)


@pytest.fixture()
def bronze(session: Session) -> BronzeRepository:
    return BronzeRepository(session)


@pytest.fixture()
def silver(session: Session) -> SilverRepository:
    return SilverRepository(session)


@pytest.fixture()
def admin(session: Session) -> QuarantineRepository:
    return QuarantineRepository(session)


def page_doc(n: int = 1, **overrides: Any) -> dict[str, Any]:
    url = f"https://{HOST}/page/{n}"
    doc: dict[str, Any] = {
        "url": url,
        "normalized_url": url,
        "host": HOST,
        "title": "Downloads",
        "text": "text",
        "content_hash": f"{n:064x}",
        "content_type": "text/html",
        "fetched_at": (_ANCIENT + timedelta(days=n)).isoformat(),
        "status_code": 200,
        "depth": 1,
    }
    doc.update(overrides)
    return doc


def stored_exe(**overrides: Any) -> dict[str, Any]:
    stored: dict[str, Any] = {
        "source_page_url": f"https://{HOST}/page/1",
        "document_url": EXE_URL,
        "storage_path": f"raw/documents/{HOST}/update.exe",
        "content_type": "application/x-msdownload",
        "sha256": "9" * 64,
        "size": 5 * 1024 * 1024,
        "stored_at": _ANCIENT.isoformat(),
    }
    stored.update(overrides)
    return stored


def quarantine_file(silver: SilverRepository, file_id: int, **kw: Any) -> int:
    args: dict[str, Any] = {
        "stored_file_id": file_id,
        "threat_signature": SIGNATURE,
        "quarantine_path": "s3://quarantine-lake/update.exe",
        "scanner_version": "ClamAV 1.4.0",
    }
    args.update(kw)
    return silver.quarantine(**args).id


class TestRecording:
    def test_a_stored_file_is_recorded_with_its_bronze_details(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        doc_id = bronze.save_document(page_doc(), register_unknown_domains=True).id
        file_id = bronze.save_stored_file(stored_exe(), crawled_document_id=doc_id).id
        record = silver.session.get(QuarantinedFile, quarantine_file(silver, file_id))

        assert record is not None
        assert (record.document_url, record.source_page_url) == (EXE_URL, f"https://{HOST}/page/1")
        assert record.original_path == f"raw/documents/{HOST}/update.exe"
        assert (record.sha256, record.size_bytes) == ("9" * 64, 5 * 1024 * 1024)
        assert record.domain_id is not None
        assert (record.status, record.scanner_engine) == (QuarantineStatus.QUARANTINED, "ClamAV")
        QuarantinedFileRead.model_validate(record)

    def test_the_bronze_row_is_parked_and_never_claimed(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        file_id = bronze.save_stored_file(stored_exe()).id
        quarantine_file(silver, file_id)

        status, error = silver.session.execute(
            select(StoredFile.processing_status, StoredFile.processing_error).where(
                StoredFile.id == file_id
            )
        ).one()
        assert status == ProcessingStatus.QUARANTINED
        assert error == f"quarantined: {SIGNATURE}"
        assert file_id not in [f.id for f in silver.claim_stored_files(limit=1000)]

    def test_a_crawled_page_can_be_quarantined_and_its_page_is_removed(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        doc_id = bronze.save_document(page_doc(), minio_path="raw/pages/1.json").id
        page_id = silver.save_page(
            {"searchable_text": "x", "content_hash": "a" * 64}, crawled_document_id=doc_id
        ).id
        record_id = silver.quarantine(
            crawled_document_id=doc_id,
            threat_signature="HTML.Phishing.Bank-1",
            quarantine_path="s3://quarantine-lake/1.json",
        ).id

        record = silver.session.get(QuarantinedFile, record_id)
        assert record is not None
        assert (record.document_url, record.original_path) == (
            f"https://{HOST}/page/1",
            "raw/pages/1.json",
        )
        assert silver.session.get(Page, page_id) is None

    def test_rescanning_the_same_file_updates_one_record(
        self, silver: SilverRepository, bronze: BronzeRepository, admin: QuarantineRepository
    ) -> None:
        file_id = bronze.save_stored_file(stored_exe()).id
        first = quarantine_file(silver, file_id)
        admin.mark_deleted(first, deleted_by="admin@pgs")
        again = silver.quarantine(
            stored_file_id=file_id,
            threat_signature="Win.Trojan.Generic-999",
            quarantine_path="s3://quarantine-lake/update-2.exe",
        )
        assert again.id == first and not again.inserted
        record = silver.session.get(QuarantinedFile, first)
        assert record is not None
        silver.session.refresh(record)
        # A new copy was isolated, so it is back in quarantine.
        assert (record.status, record.deleted_by) == (QuarantineStatus.QUARANTINED, None)
        assert record.threat_signature == "Win.Trojan.Generic-999"

    @pytest.mark.parametrize(
        "kw, error, match",
        [
            ({"stored_file_id": None}, ValueError, "exactly one"),
            ({"threat_signature": " "}, ValueError, "threat_signature"),
            ({"quarantine_path": ""}, ValueError, "quarantine_path"),
            ({"stored_file_id": 9_999_999}, LookupError, "stored file"),
        ],
    )
    def test_bad_calls_are_rejected(
        self,
        silver: SilverRepository,
        bronze: BronzeRepository,
        kw: dict[str, Any],
        error: type[Exception],
        match: str,
    ) -> None:
        file_id = bronze.save_stored_file(stored_exe()).id
        with pytest.raises(error, match=match):
            quarantine_file(silver, file_id, **kw)

    def test_the_record_outlives_bronze_retention(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        file_id = bronze.save_stored_file(stored_exe()).id
        record_id = quarantine_file(silver, file_id)
        silver.session.execute(delete(StoredFile).where(StoredFile.id == file_id))
        record = silver.session.get(QuarantinedFile, record_id)
        assert record is not None
        silver.session.refresh(record)
        assert record.stored_file_id is None and record.document_url == EXE_URL

    def test_database_rejects_two_sources(
        self, silver: SilverRepository, bronze: BronzeRepository
    ) -> None:
        doc_id = bronze.save_document(page_doc()).id
        file_id = bronze.save_stored_file(stored_exe()).id
        record_id = quarantine_file(silver, file_id)
        record = silver.session.get(QuarantinedFile, record_id)
        assert record is not None
        record.crawled_document_id = doc_id
        with pytest.raises(IntegrityError, match="ck_quarantined_files_at_most_one_source"):
            silver.session.flush()


class TestAdmin:
    def _three(self, silver: SilverRepository, bronze: BronzeRepository) -> list[int]:
        ids = []
        for n, size in enumerate((1024 * 1024, 2 * 1024 * 1024, 3 * 1024 * 1024), start=1):
            file_id = bronze.save_stored_file(
                stored_exe(document_url=f"{EXE_URL}?v={n}", sha256=f"{n:064x}", size=size)
            ).id
            ids.append(
                quarantine_file(
                    silver,
                    file_id,
                    threat_signature=f"Sig-{n}",
                    scanned_at=datetime(2026, 9, n, tzinfo=UTC),
                )
            )
        return ids

    def test_list_newest_first_with_total(
        self, silver: SilverRepository, bronze: BronzeRepository, admin: QuarantineRepository
    ) -> None:
        ids = self._three(silver, bronze)
        rows, total = admin.list_quarantined(limit=2)
        mine = [r.id for r in rows if r.id in ids]
        assert total >= 3 and mine == [ids[2], ids[1]]

    def test_summary_and_delete(
        self, silver: SilverRepository, bronze: BronzeRepository, admin: QuarantineRepository
    ) -> None:
        ids = self._three(silver, bronze)
        summary = QuarantineSummary.model_validate(admin.summary())
        assert (summary.count, summary.size_mb) == (3, 6)
        assert summary.latest_threat_detected == "Sig-3"

        deleted = admin.mark_deleted(ids[2], deleted_by="  admin@pgs ")
        assert (deleted.status, deleted.deleted_by) == (QuarantineStatus.DELETED, "admin@pgs")
        after = admin.summary()
        assert (after["count"], after["size_mb"], after["latest_threat_detected"]) == (
            2,
            3,
            "Sig-2",
        )
        audit, _ = admin.list_quarantined(status=None, limit=1000)
        assert ids[2] in [r.id for r in audit]

    def test_delete_errors(
        self, silver: SilverRepository, bronze: BronzeRepository, admin: QuarantineRepository
    ) -> None:
        (record_id, *_) = self._three(silver, bronze)
        with pytest.raises(ValueError, match="admin"):
            admin.mark_deleted(record_id, deleted_by=" ")
        admin.mark_deleted(record_id, deleted_by="admin")
        with pytest.raises(ValueError, match="already deleted"):
            admin.mark_deleted(record_id, deleted_by="admin")
        with pytest.raises(LookupError):
            admin.mark_deleted(9_999_999, deleted_by="admin")


# ------------------------------------------------------------------ the ETL loop


@pytest.fixture()
def session_factory() -> Iterator[sessionmaker[Session]]:
    engine = make_engine()
    conn = engine.connect()
    outer = conn.begin()
    try:
        yield sessionmaker(
            bind=conn, join_transaction_mode="create_savepoint", expire_on_commit=False
        )
    finally:
        outer.rollback()
        conn.close()


class TestEtlLoop:
    def test_infected_files_are_quarantined_not_failed(
        self, session_factory: sessionmaker[Session]
    ) -> None:
        with session_factory() as s, s.begin():
            repo = BronzeRepository(s)
            pdf = stored_exe(document_url=f"{EXE_URL}.pdf", sha256="1" * 64)
            clean = repo.save_stored_file(pdf).id
            bad = repo.save_stored_file(stored_exe()).id

        def transform(stored: StoredFile) -> dict[str, Any]:
            if stored.id == bad:
                raise Infected(SIGNATURE, "s3://quarantine-lake/x", scanner_version="1.4.0")
            return {"searchable_text": "clean pdf"}

        result = process_stored_file_batch(session_factory, transform, limit=1000)
        assert (result.quarantined, result.failed) == (1, 0)
        with session_factory() as s:
            statuses = dict(
                s.execute(
                    select(StoredFile.id, StoredFile.processing_status).where(
                        StoredFile.id.in_([clean, bad])
                    )
                ).all()
            )
            assert statuses == {
                clean: ProcessingStatus.PROCESSED,
                bad: ProcessingStatus.QUARANTINED,
            }
            record = s.scalar(select(QuarantinedFile).where(QuarantinedFile.stored_file_id == bad))
            assert record is not None and record.scanner_version == "1.4.0"
            assert s.scalar(select(Page).where(Page.stored_file_id == bad)) is None

    def test_infected_crawled_pages_too(self, session_factory: sessionmaker[Session]) -> None:
        with session_factory() as s, s.begin():
            doc_id = BronzeRepository(s).save_document(page_doc()).id

        def transform(doc: CrawledDocument) -> dict[str, Any]:
            raise Infected("HTML.Phishing.Bank-1", "s3://quarantine-lake/p.json")

        result = process_bronze_batch(session_factory, transform, limit=1000)
        assert result.quarantined >= 1
        with session_factory() as s:
            status = s.scalar(
                select(CrawledDocument.processing_status).where(CrawledDocument.id == doc_id)
            )
            assert status == ProcessingStatus.QUARANTINED
