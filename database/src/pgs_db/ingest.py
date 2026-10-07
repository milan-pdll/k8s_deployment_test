"""Load the scraper's S3 output into Bronze.

The scraper (`scraper/docs/SCHEMA.md`) no longer writes Postgres. It writes, per run:

    <prefix>/<crawl_run_id>/_run.json              run manifest
    <prefix>/<crawl_run_id>/<sha256(url)>.json     one model.Document
    <prefix>/latest/<sha256(url)>.json             its freshness index (ignored here)

`ingest(session_factory, source)` copies runs into `crawl_runs` (under the scraper's
own run id) and documents into `crawled_documents` (`minio_path` = the object key),
through `BronzeRepository`, so everything downstream -- the ETL queue, Silver, Gold --
works unchanged. It is safe to run as often as you like:

- A run is read while still `running`; later passes pick up only objects modified
  since the last one (`bronze_ingest_state.last_modified`). Saving a document twice is
  a no-op upsert, so the overlap at the watermark never duplicates anything.
- Once the manifest is `completed` / `failed` and all its objects are in, the run is
  marked finished and never listed again.
- A malformed object is skipped, counted, and written to `error_logs`; the rest of
  the run still loads.

Sources: `S3Source` (AWS S3 or MinIO, needs `pip install "pgs-db[s3]"`) and
`DirectorySource` (the same layout on disk: local dev and tests). `python -m
pgs_db.jobs ingest` builds an `S3Source` from the environment:

    PGS_S3_BUCKET        required; unset = the job reports "not configured" and exits ok
    PGS_S3_PREFIX        key prefix the scraper uses (default: none)
    PGS_S3_ENDPOINT_URL  MinIO / LocalStack endpoint (default: AWS)
    AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_REGION   standard boto3 settings

Run it every 5-10 minutes: the scraper expires run folders after ~90 days, and a
document's text is kept in Bronze, so nothing is lost once they expire.
"""

import json
import os
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from .enums import LogSeverity, ServiceName
from .models import BronzeIngestState, CrawlRun
from .repositories import BronzeRepository, OpsRepository
from .repositories._mapping import parse_timestamp

MANIFEST = "_run.json"
TERMINAL = {"COMPLETED", "FAILED"}
_CHUNK = 200  # documents per transaction


@dataclass
class SourceObject:
    key: str
    last_modified: datetime


class Source(Protocol):
    """Where the scraper's objects are read from."""

    name: str

    def run_ids(self) -> list[int]: ...

    def manifest(self, run_id: int) -> dict[str, Any] | None: ...

    def documents(self, run_id: int) -> Iterator[SourceObject]: ...

    def read(self, key: str) -> bytes: ...


def _run_id(name: str) -> int | None:
    return int(name) if name.isdigit() else None


class DirectorySource:
    """The bucket layout on local disk (a MinIO mirror, a test fixture)."""

    def __init__(self, root: str | Path, prefix: str = "") -> None:
        self.root = Path(root)
        self.prefix = prefix.strip("/")
        self.base = self.root / self.prefix if self.prefix else self.root
        self.name = f"file://{self.base.as_posix()}/"

    def run_ids(self) -> list[int]:
        if not self.base.is_dir():
            return []
        return sorted(r for p in self.base.iterdir() if p.is_dir() and (r := _run_id(p.name)))

    def manifest(self, run_id: int) -> dict[str, Any] | None:
        path = self.base / str(run_id) / MANIFEST
        return json.loads(path.read_bytes()) if path.is_file() else None

    def documents(self, run_id: int) -> Iterator[SourceObject]:
        folder = self.base / str(run_id)
        for path in sorted(folder.glob("*.json")):
            if path.name == MANIFEST:
                continue
            modified = datetime.fromtimestamp(path.stat().st_mtime, UTC)
            yield SourceObject(path.relative_to(self.root).as_posix(), modified)

    def read(self, key: str) -> bytes:
        return (self.root / key).read_bytes()


class S3Source:
    """An S3 or MinIO bucket, read-only."""

    def __init__(self, bucket: str, prefix: str = "", client: Any = None) -> None:
        if client is None:
            try:
                import boto3
            except ImportError as exc:  # pragma: no cover - depends on the install
                raise RuntimeError(
                    'S3 ingest needs the s3 extra: pip install "pgs-db[s3]"'
                ) from exc
            client = boto3.client("s3", endpoint_url=os.environ.get("PGS_S3_ENDPOINT_URL") or None)
        self.client = client
        self.bucket = bucket
        self.prefix = prefix.strip("/")
        self._root = f"{self.prefix}/" if self.prefix else ""
        self.name = f"s3://{bucket}/{self._root}"

    @classmethod
    def from_env(cls) -> "S3Source | None":
        bucket = os.environ.get("PGS_S3_BUCKET")
        return cls(bucket, os.environ.get("PGS_S3_PREFIX", "")) if bucket else None

    def run_ids(self) -> list[int]:
        ids: set[int] = set()
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=self._root, Delimiter="/"):
            for common in page.get("CommonPrefixes", []):
                run = _run_id(common["Prefix"][len(self._root):].strip("/"))
                if run is not None:
                    ids.add(run)
        return sorted(ids)

    def manifest(self, run_id: int) -> dict[str, Any] | None:
        try:
            return json.loads(self.read(f"{self._root}{run_id}/{MANIFEST}"))
        except self.client.exceptions.NoSuchKey:
            return None

    def documents(self, run_id: int) -> Iterator[SourceObject]:
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.bucket, Prefix=f"{self._root}{run_id}/"):
            for obj in page.get("Contents", []):
                if obj["Key"].endswith(".json") and not obj["Key"].endswith(MANIFEST):
                    yield SourceObject(obj["Key"], obj["LastModified"])

    def read(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()


@dataclass
class IngestReport:
    runs_seen: int = 0
    runs_finished: int = 0
    documents_loaded: int = 0
    documents_failed: int = 0
    errors: dict[str, str] = field(default_factory=dict)  # object key -> why


def ingest(
    session_factory: sessionmaker[Session],
    source: Source,
    *,
    register_unknown_domains: bool = True,
) -> IngestReport:
    """Load every unfinished run from `source` into Bronze. See the module docstring."""
    report = IngestReport()
    with session_factory() as s:
        finished = set(
            s.scalars(
                select(BronzeIngestState.crawl_run_id).where(
                    BronzeIngestState.finished.is_(True),
                    BronzeIngestState.source == source.name,
                )
            )
        )
    for run_id in source.run_ids():
        if run_id in finished:
            continue
        report.runs_seen += 1
        if _ingest_run(session_factory, source, run_id, report, register_unknown_domains):
            report.runs_finished += 1
    return report


def ingest_ndjson(
    session_factory: sessionmaker[Session],
    path: str | Path,
    *,
    register_unknown_domains: bool = True,
) -> IngestReport:
    """Load the scraper's `--storage=ndjson` output (one `Document` per line) into Bronze.

    The scraper's default for local and dev crawls, which has no run manifests. A
    document's `crawl_run_id`, when set, gets a placeholder run so its foreign key
    holds. Re-loading the same file is a no-op upsert; a bad line is skipped, counted
    and written to `error_logs` (key `<file>:<line>`).
    """
    report = IngestReport()
    path = Path(path)
    lines = path.read_text(encoding="utf-8").splitlines()
    for start in range(0, len(lines), _CHUNK):
        with session_factory.begin() as s:
            bronze = BronzeRepository(s)
            for number, line in enumerate(lines[start : start + _CHUNK], start=start + 1):
                if not line.strip():
                    continue
                key = f"{path.name}:{number}"
                try:
                    with s.begin_nested():
                        doc = json.loads(line)
                        run_id = doc.get("crawl_run_id") or None
                        if run_id is not None:
                            upsert_run(s, int(run_id), None)
                        bronze.save_document(
                            doc,
                            crawl_run_id=run_id,
                            register_unknown_domains=register_unknown_domains,
                        )
                    report.documents_loaded += 1
                except Exception as exc:
                    report.documents_failed += 1
                    message = f"{type(exc).__name__}: {exc}"[:2000]
                    report.errors[key] = message
                    OpsRepository(s).log_error(
                        ServiceName.SCRAPER,
                        LogSeverity.ERROR,
                        f"could not load {key}: {message}",
                        instance="pgs_db.ingest",
                        error_type="INGEST_BAD_OBJECT",
                        context={"source": str(path), "line": number},
                    )
    return report


def _ingest_run(
    session_factory: sessionmaker[Session],
    source: Source,
    run_id: int,
    report: IngestReport,
    register_unknown_domains: bool,
) -> bool:
    manifest = source.manifest(run_id)
    with session_factory.begin() as s:
        upsert_run(s, run_id, manifest)
        state = _state(s, run_id, source.name)
        watermark = state.last_modified

    # Read the manifest before listing: if it already said "completed", every object
    # listed afterwards belongs to the finished run.
    status = str((manifest or {}).get("status", "")).upper()
    pending = [
        o for o in source.documents(run_id) if watermark is None or o.last_modified >= watermark
    ]
    newest = watermark
    for start in range(0, len(pending), _CHUNK):
        chunk = pending[start : start + _CHUNK]
        loaded = failed = 0
        with session_factory.begin() as s:
            bronze = BronzeRepository(s)
            for obj in chunk:
                try:
                    with s.begin_nested():
                        doc = json.loads(source.read(obj.key))
                        bronze.save_document(
                            doc,
                            crawl_run_id=run_id,
                            minio_path=obj.key,
                            register_unknown_domains=register_unknown_domains,
                        )
                    loaded += 1
                except Exception as exc:
                    failed += 1
                    message = f"{type(exc).__name__}: {exc}"[:2000]
                    report.errors[obj.key] = message
                    OpsRepository(s).log_error(
                        ServiceName.SCRAPER,
                        LogSeverity.ERROR,
                        f"could not load {obj.key}: {message}",
                        instance="pgs_db.ingest",
                        error_type="INGEST_BAD_OBJECT",
                        crawl_run_id=run_id,
                        context={"source": source.name, "key": obj.key},
                    )
                if newest is None or obj.last_modified > newest:
                    newest = obj.last_modified
            state = _state(s, run_id, source.name)
            state.last_modified = newest
            state.objects_loaded += loaded
            state.objects_failed += failed
        report.documents_loaded += loaded
        report.documents_failed += failed

    if status in TERMINAL:
        with session_factory.begin() as s:
            state = _state(s, run_id, source.name)
            state.finished = True
            state.finished_at = datetime.now(UTC)
        return True
    return False


def _state(session: Session, run_id: int, source_name: str) -> BronzeIngestState:
    state = session.scalar(
        select(BronzeIngestState).where(BronzeIngestState.crawl_run_id == run_id)
    )
    if state is None:
        state = BronzeIngestState(crawl_run_id=run_id, source=source_name)
        session.add(state)
        session.flush()
    return state


def upsert_run(session: Session, run_id: int, manifest: dict[str, Any] | None) -> None:
    """Create or refresh the crawl_runs row, under the scraper's own run id.

    Without a manifest (not written yet) a placeholder RUNNING row is made so the
    run's documents can reference it; the next pass fills it in.
    """
    m = manifest or {}
    started = m.get("started_at")
    status = str(m.get("status") or "RUNNING").upper()
    values = {
        "id": run_id,
        "status": status,
        "started_at": parse_timestamp(started, "started_at") if started else datetime.now(UTC),
        "seed_count": m.get("seed_count"),
        "max_depth": m.get("max_depth"),
        "max_pages": m.get("max_pages"),
        "fetched_count": int(m.get("fetched") or 0),
        "succeeded_count": int(m.get("succeeded") or 0),
        "failed_count": int(m.get("failed") or 0),
        "skipped_count": int(m.get("skipped") or 0),
        "unique_url_count": int(m.get("unique_urls") or 0),
        "domain_capped_count": int(m.get("domain_capped") or 0),
        "error": m.get("error") or None,
        "finished_at": (
            parse_timestamp(m["updated_at"], "updated_at")
            if status in TERMINAL and m.get("updated_at")
            else None
        ),
    }
    stmt = insert(CrawlRun).values(values)
    update = {k: stmt.excluded[k] for k in values if k != "id"}
    if manifest is None:
        update = {}  # a placeholder never overwrites a real manifest's values
    session.execute(
        stmt.on_conflict_do_update(index_elements=[CrawlRun.id], set_=update)
        if update
        else stmt.on_conflict_do_nothing(index_elements=[CrawlRun.id])
    )
