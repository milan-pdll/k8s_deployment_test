"""pgs_db.ingest: the scraper's S3 layout (scraper/docs/SCHEMA.md) loaded into Bronze.

Uses DirectorySource, the same layout on disk, so no bucket is needed; S3Source's
listing is checked against a stub client.
"""

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.orm import Session, sessionmaker

from pgs_db import jobs
from pgs_db.enums import CrawlRunStatus
from pgs_db.ingest import DirectorySource, S3Source, ingest
from pgs_db.models import BronzeIngestState, CrawledDocument, CrawlRun, ErrorLog

RUN = 1_735_500_000_000_000_000  # the scraper's UnixNano run id
HOST = "ingest-test.gov.np"
T0 = datetime(2026, 9, 29, 12, 0, tzinfo=UTC)


@pytest.fixture()
def factory(session: Session) -> sessionmaker[Session]:
    return sessionmaker(bind=session.connection(), join_transaction_mode="create_savepoint")


def document(n: int, **overrides: Any) -> dict[str, Any]:
    """A model.Document exactly as S3Writer marshals it."""
    url = f"https://{HOST}/page/{n}"
    doc: dict[str, Any] = {
        "url": url,
        "normalized_url": url,
        "host": HOST,
        "title": f"Page {n}",
        "text": f"body {n}",
        "links": [f"https://{HOST}/page/{n + 1}"],
        "geo": {"lat": 28.2, "lng": 83.9},
        "sim_hash": 18_000_000_000_000_000_000,  # uint64 above int64 max
        "crawl_run_id": RUN,
        "depth": 1,
        "status_code": 200,
        "content_type": "text/html",
        "content_hash": f"{n:064x}",
        "fetched_at": "2026-09-29T12:01:00Z",
        "fetch_duration_ms": 120,
    }
    doc.update(overrides)
    return doc


def write(path: Path, body: Any, when: datetime = T0) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body if isinstance(body, str) else json.dumps(body), encoding="utf-8")
    os.utime(path, (when.timestamp(), when.timestamp()))


def manifest(status: str = "completed", **overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "run_id": RUN,
        "status": status,
        "seed_count": 3,
        "max_depth": 2,
        "max_pages": 100,
        "fetched": 2,
        "succeeded": 2,
        "failed": 0,
        "skipped": 0,
        "domain_capped": 1,
        "error": "",
        "started_at": "2026-09-29T12:00:00Z",
        "updated_at": "2026-09-29T12:03:12Z",
    }
    body.update(overrides)
    return body


def bucket(root: Path, prefix: str = "staging") -> Path:
    base = root / prefix
    write(base / str(RUN) / "_run.json", manifest())
    write(base / str(RUN) / "aaa.json", document(1))
    write(base / str(RUN) / "bbb.json", document(2))
    write(base / "latest" / "aaa.json", document(1))  # the scraper's freshness index
    write(base / "not-a-run" / "x.json", {"junk": True})
    return base


def run_documents(session: Session) -> list[CrawledDocument]:
    return list(
        session.scalars(
            select(CrawledDocument)
            .where(CrawledDocument.crawl_run_id == RUN)
            .order_by(CrawledDocument.normalized_url)
        )
    )


class TestCompletedRun:
    def test_a_run_and_its_documents_land_in_bronze(
        self, session: Session, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        bucket(tmp_path)
        report = ingest(factory, DirectorySource(tmp_path, "staging"))

        assert (report.runs_seen, report.runs_finished, report.documents_loaded) == (1, 1, 2)
        run = session.get(CrawlRun, RUN)
        assert run is not None
        session.refresh(run)
        assert run.status == CrawlRunStatus.COMPLETED  # "completed" upper-cased
        assert (run.seed_count, run.max_pages, run.domain_capped_count) == (3, 100, 1)
        assert run.error is None  # "" means no error
        assert run.finished_at == datetime(2026, 9, 29, 12, 3, 12, tzinfo=UTC)

        docs = run_documents(session)
        assert [d.minio_path for d in docs] == [
            f"staging/{RUN}/aaa.json",
            f"staging/{RUN}/bbb.json",
        ]
        assert docs[0].text == "body 1" and docs[0].geo_lat == 28.2
        assert docs[0].sim_hash == 18_000_000_000_000_000_000 - 2**64
        assert docs[0].domain_id is not None  # host registered as a domain

    def test_a_finished_run_is_never_read_again(
        self, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        bucket(tmp_path)
        source = DirectorySource(tmp_path, "staging")
        ingest(factory, source)
        again = ingest(factory, source)
        assert (again.runs_seen, again.documents_loaded) == (0, 0)


class TestRunningRun:
    def test_new_objects_are_picked_up_and_nothing_duplicates(
        self, session: Session, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        base = bucket(tmp_path)
        write(base / str(RUN) / "_run.json", manifest("running"))
        source = DirectorySource(tmp_path, "staging")

        first = ingest(factory, source)
        assert (first.documents_loaded, first.runs_finished) == (2, 0)

        later = datetime(2026, 9, 29, 12, 5, tzinfo=UTC)
        write(base / str(RUN) / "ccc.json", document(3), later)
        write(base / str(RUN) / "_run.json", manifest("completed"))
        second = ingest(factory, source)

        assert second.runs_finished == 1
        assert len(run_documents(session)) == 3  # the re-read at the watermark is an upsert
        state = session.scalar(
            select(BronzeIngestState).where(BronzeIngestState.crawl_run_id == RUN)
        )
        assert state is not None
        session.refresh(state)
        assert state.finished and state.last_modified == later

    def test_documents_before_the_manifest_get_a_placeholder_run(
        self, session: Session, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        base = tmp_path / "staging"
        write(base / str(RUN) / "aaa.json", document(1))
        source = DirectorySource(tmp_path, "staging")

        ingest(factory, source)
        run = session.get(CrawlRun, RUN)
        assert run is not None and run.status == CrawlRunStatus.RUNNING
        assert len(run_documents(session)) == 1

        write(base / str(RUN) / "_run.json", manifest("failed", error="worker lost"))
        assert ingest(factory, source).runs_finished == 1
        session.refresh(run)
        assert (run.status, run.error) == (CrawlRunStatus.FAILED, "worker lost")


class TestBadObjects:
    def test_a_malformed_object_is_logged_and_the_rest_still_load(
        self, session: Session, factory: sessionmaker[Session], tmp_path: Path
    ) -> None:
        base = bucket(tmp_path)
        write(base / str(RUN) / "broken.json", "{not json")
        write(base / str(RUN) / "nourl.json", {"content_hash": "x"})

        report = ingest(factory, DirectorySource(tmp_path, "staging"))
        assert (report.documents_loaded, report.documents_failed) == (2, 2)
        assert set(report.errors) == {
            f"staging/{RUN}/broken.json",
            f"staging/{RUN}/nourl.json",
        }
        logged = session.scalar(
            select(func.count()).where(
                ErrorLog.crawl_run_id == RUN, ErrorLog.error_type == "INGEST_BAD_OBJECT"
            )
        )
        assert logged == 2


class _StubPaginator:
    def __init__(self, pages: dict[tuple[str, str | None], list[dict[str, Any]]]) -> None:
        self.pages = pages

    def paginate(self, *, Bucket: str, Prefix: str, Delimiter: str | None = None) -> Any:
        return self.pages[(Prefix, Delimiter)]


class _StubS3:
    """Just enough of boto3's S3 client for S3Source's listing."""

    class exceptions:
        class NoSuchKey(Exception):
            pass

    def __init__(self, pages: dict[tuple[str, str | None], list[dict[str, Any]]]) -> None:
        self._paginator = _StubPaginator(pages)

    def get_paginator(self, name: str) -> _StubPaginator:
        assert name == "list_objects_v2"
        return self._paginator


class TestS3Source:
    def test_listing_respects_the_prefix_and_skips_non_runs(self) -> None:
        pages = {
            ("staging/", "/"): [
                {"CommonPrefixes": [{"Prefix": f"staging/{RUN}/"}, {"Prefix": "staging/latest/"}]},
                {"CommonPrefixes": [{"Prefix": "staging/42/"}]},
            ],
            (f"staging/{RUN}/", None): [
                {"Contents": [
                    {"Key": f"staging/{RUN}/_run.json", "LastModified": T0},
                    {"Key": f"staging/{RUN}/aaa.json", "LastModified": T0},
                ]},
            ],
        }
        source = S3Source("pgs-crawl", "/staging/", client=_StubS3(pages))
        assert source.name == "s3://pgs-crawl/staging/"
        assert source.run_ids() == [42, RUN]
        assert [o.key for o in source.documents(RUN)] == [f"staging/{RUN}/aaa.json"]

    def test_the_job_is_a_no_op_until_a_bucket_is_configured(
        self, session: Session, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("PGS_S3_BUCKET", raising=False)
        report = jobs.run_job(session, "ingest", jobs.parse_args(["ingest"]))
        assert report["status"] == "ok"
        assert report["result"] == {"skipped": "PGS_S3_BUCKET not set"}
