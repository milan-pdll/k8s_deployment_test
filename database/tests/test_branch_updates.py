"""Fixes for the branches updated on 2026-09-30.

1. Kafka path (`--storage=kafka`): the ETL saves the raw Document to Bronze first.
2. `--storage=ndjson` (the scraper's default): `ingest_ndjson`.
3. Search export carries the location as both `geo` and `geo_location`.
4. Map files the UI can drop in (`scripts/export_boundaries.py`).
"""

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from pgs_db import BronzeRepository, ReferenceRepository, SearchRepository, SilverRepository
from pgs_db.enums import CrawlRunStatus
from pgs_db.etl import save_transformed
from pgs_db.ingest import ingest_ndjson
from pgs_db.models import CrawledDocument, CrawlRun, LocalBody, Page
from pgs_db.schemas import SearchDocumentOut

HOST = "branch-fix.gov.np"
DATABASE_DIR = Path(__file__).resolve().parent.parent


@pytest.fixture()
def factory(session: Session) -> sessionmaker[Session]:
    return sessionmaker(bind=session.connection(), join_transaction_mode="create_savepoint")


def kafka_document(n: int, **overrides: Any) -> dict[str, Any]:
    """model.Document as KafkaWriter marshals it (key = normalized_url)."""
    url = f"https://{HOST}/news/{n}"
    doc: dict[str, Any] = {
        "url": url,
        "normalized_url": url,
        "host": HOST,
        "title": f"News {n}",
        "text": f"news body {n}",
        "links": [],
        "crawl_run_id": 1_735_600_000_000_000_000 + n,
        "depth": 0,
        "status_code": 200,
        "content_type": "text/html",
        "content_hash": f"{n:064x}",
        "fetched_at": "2026-09-30T08:00:00Z",
        "fetch_duration_ms": 50,
    }
    doc.update(overrides)
    return doc


class TestKafkaPath:
    def test_the_raw_document_is_saved_to_bronze_with_the_page(
        self, session: Session, factory: sessionmaker[Session]
    ) -> None:
        message = kafka_document(1)
        record = {  # what transform.py makes of it
            "source_url": message["url"],
            "object_key": "",
            "title": "News 1",
            "searchable_text": "news body 1",
            "language_detected": "en",
            "content_sha256": "d" * 64,
            "simhash": "00000000000000ff",
            "geo_location": {"district": "Kaski", "source": "seed_gazetteer"},
        }
        saved = save_transformed(factory, record, geo_confidence=0.6, bronze_document=message)

        bronze = session.get(CrawledDocument, saved.crawled_document_id)
        assert bronze is not None
        expected = (message["url"], message["crawl_run_id"])
        assert (bronze.normalized_url, bronze.crawl_run_id) == expected
        run = session.get(CrawlRun, message["crawl_run_id"])
        assert run is not None and run.status == CrawlRunStatus.RUNNING  # placeholder
        page = session.get(Page, saved.page_id)
        assert page is not None and page.crawled_document_id == bronze.id
        status = session.scalar(
            select(CrawledDocument.processing_status).where(CrawledDocument.id == bronze.id)
        )
        assert status == "PROCESSED"

    def test_redelivered_messages_do_not_duplicate(
        self, session: Session, factory: sessionmaker[Session]
    ) -> None:
        message = kafka_document(2)
        record = {"source_url": message["url"], "searchable_text": "news body 2",
                  "content_sha256": "e" * 64}
        first = save_transformed(factory, record, geo_confidence=0.5, bronze_document=message)
        again = save_transformed(factory, record, geo_confidence=0.5, bronze_document=message)
        assert (again.crawled_document_id, again.page_id) == (
            first.crawled_document_id, first.page_id,
        )


class TestNdjson:
    def test_a_file_loads_bad_lines_are_skipped_and_reruns_are_no_ops(
        self, session: Session, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        path = tmp_path / "documents.ndjson"
        lines = [
            json.dumps(kafka_document(10, crawl_run_id=0)),  # 0 = no run tracking
            "",
            "{broken",
            json.dumps(kafka_document(11)),
        ]
        path.write_text("\n".join(lines), encoding="utf-8")

        report = ingest_ndjson(factory, path)
        assert (report.documents_loaded, report.documents_failed) == (2, 1)
        assert list(report.errors) == ["documents.ndjson:3"]

        def ours() -> int:
            return int(
                session.scalar(
                    select(func.count()).where(CrawledDocument.host == HOST)
                )
                or 0
            )

        loaded = ours()
        ingest_ndjson(factory, path)
        assert ours() == loaded == 2
        first = session.scalar(
            select(CrawledDocument).where(CrawledDocument.normalized_url.endswith("/news/10"))
        )
        assert first is not None and first.crawl_run_id is None


class TestSearchExport:
    def test_location_is_under_both_names(self, session: Session) -> None:
        bronze, silver = BronzeRepository(session), SilverRepository(session)
        url = f"https://{HOST}/geo"
        doc_id = bronze.save_document(
            {"url": url, "normalized_url": url, "host": HOST, "content_hash": "f" * 64,
             "fetched_at": "2026-09-30T08:00:00Z"}
        ).id
        page_id = silver.save_page(
            {"searchable_text": "x", "content_hash": "f" * 64,
             "geo_location": {"municipality_id": "MUN414", "method": "DOMAIN", "confidence": 1.0}},
            crawled_document_id=doc_id,
        ).id
        [doc] = SearchRepository(session).documents([page_id])
        out = SearchDocumentOut.model_validate(doc)
        assert out.geo == out.geo_location
        assert out.geo_location.municipality_id == "MUN414"
        assert out.geo_location.district_name_en == "Kaski"  # what count_by_region reads


class TestUiMapFiles:
    @pytest.fixture()
    def ref(self, session: Session) -> ReferenceRepository:
        if not session.scalar(select(func.count()).where(LocalBody.boundary.is_not(None))):
            pytest.skip("boundaries not seeded: run scripts/seed_boundaries.py")
        return ReferenceRepository(session)

    def test_legacy_property_names_the_ui_reads(self, ref: ReferenceRepository) -> None:
        geojson = ref.boundaries_geojson("province", legacy_properties=True)
        provinces = {f["properties"]["code"]: f["properties"] for f in geojson["features"]}
        assert (provinces["P4"]["ADM1_PCODE"], provinces["P4"]["ADM1_EN"]) == ("NP04", "4")

        kaski = next(f["properties"] for f in ref.boundaries_geojson(
            "district", within="P4", legacy_properties=True)["features"]
            if f["properties"]["code"] == "D38")
        assert kaski["DISTRICT"] == "KASKI"

        pokhara = next(f["properties"] for f in ref.boundaries_geojson(
            "local_body", within="D38", legacy_properties=True)["features"]
            if f["properties"]["code"] == "MUN414")
        assert (pokhara["NAME"], pokhara["DISTRICT"], pokhara["LEVEL"], pokhara["N_ID"]) == (
            "Pokhara", "Kaski", "Mahanagarpalika", "MUN414",
        )
        plain = ref.boundaries_geojson("province")["features"][0]["properties"]
        assert "ADM1_PCODE" not in plain  # only on request

    def test_the_export_script_writes_the_three_ui_files(
        self, ref: ReferenceRepository, tmp_path: Path
    ) -> None:
        result = subprocess.run(
            [sys.executable, str(DATABASE_DIR / "scripts" / "export_boundaries.py"), str(tmp_path)],
            capture_output=True, text=True, env={**os.environ}, check=False,
        )
        assert result.returncode == 0, result.stderr
        counts = {
            name: len(json.loads((tmp_path / name).read_text(encoding="utf-8"))["features"])
            for name in ("nepal-provinces.geojson", "nepal-districts.geojson",
                         "nepal-municipalities.geojson")
        }
        # 77 districts (the old file had 75); 755 shapes = 753 local bodies, 2 in two parts
        # are unioned in the database, so 753 features.
        assert counts == {
            "nepal-provinces.geojson": 7,
            "nepal-districts.geojson": 77,
            "nepal-municipalities.geojson": 753,
        }
