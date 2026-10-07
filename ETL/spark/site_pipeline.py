"""PySpark ETL for one crawled website: S3 -> ClamAV -> text -> LaBSE -> PostgreSQL.

The scraper publishes one ``site_crawl_completed`` event per website whose crawl has
finished (site_event.py). The ``etl_ingestion_pipeline`` DAG hands batches of them to
Temporal; the ETL worker (ETL/temporal) is the Spark driver and calls
``run_site_pipeline`` once per site, on its long-lived SparkSession:

1. driver: skip the site if a previous run already finished it (a marker object in the
   bucket), check that ClamAV and PostgreSQL answer, list the site's Document JSON
   objects under ``documents_prefix``;
2. Spark tasks, per page: read the Document and its stored HTML from S3, run the
   intake security checks (rule checks + ClamAV; "cannot scan" fails the run, it never
   passes a page), extract and normalize the text (transform.py);
3. driver, in groups of ``ETL_PAGE_GROUP_SIZE`` pages (bounded memory): embed the
   accepted pages with LaBSE (embeddings.py) and save each one, in its own transaction,
   with ``pgs_db.etl.save_transformed`` -- Bronze (the scraper's Document) + Silver
   (page, embedding, domain geo tag, dedup). Silver queues the page for the search
   indexer. Malware is recorded in ``quarantined_files`` and never reaches Silver;
4. write the site's marker with its summary.

Idempotent: Silver upserts pages by canonical URL, so a re-run re-saves the same pages.
Errors are classified for Temporal: ``ValueError`` (a malformed event) is permanent;
``DependencyUnavailable`` / ``ScannerUnavailable`` (ClamAV, PostgreSQL or S3 down) are
retried and never dead-letter a site; a page whose data cannot be saved is counted as
failed and logged to ``error_logs`` while the rest of the site carries on.

Configuration (environment): S3_ENDPOINT (empty for AWS), AWS_REGION and the standard
AWS credential variables; CLAMD_HOST/CLAMD_PORT/CLAMD_TIMEOUT_SECONDS; DATABASE_URL
(role pgs_etl); SPARK_MASTER (default local[*]); EMBEDDING_* (embeddings.py);
ETL_PAGE_GROUP_SIZE (default 200).
"""

from __future__ import annotations

import json
import logging
import os
import posixpath
import re
import socket
import sys
from collections.abc import Iterable, Iterator, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from site_event import validate_event

logger = logging.getLogger(__name__)

_MODULE_DIR = Path(__file__).resolve().parent
# What the executors import, shipped to them with addPyFile.
_EXECUTOR_MODULES = ("site_event.py", "transform.py", "security_scanner.py", "site_pipeline.py")
PAGE_GROUP_SIZE = max(1, int(os.environ.get("ETL_PAGE_GROUP_SIZE", "200")))
# Bump to make every site run again although its marker exists (e.g. after a change to
# extraction that should reach already-processed pages).
PIPELINE_VERSION = 2
MARKER_PREFIX = "_etl"
# Only used for geo blocks that carry no confidence of their own; the pipeline sends
# none today (pages get their location from their domain, with confidence 1.0).
GEO_CONFIDENCE = 0.5

_CHARSET_RE = re.compile(rb"""<meta[^>]+charset\s*=\s*["']?([A-Za-z0-9_\-]+)""", re.IGNORECASE)


class ScannerUnavailable(RuntimeError):
    """ClamAV could not scan a page. The run fails and is retried later, instead of
    rejecting (or worse, accepting) every page of the site as unscanned."""


class DependencyUnavailable(RuntimeError):
    """A service the whole run needs (ClamAV, PostgreSQL, S3) is not answering."""


# ------------------------------------------------------------------------------- S3


def s3_client() -> Any:
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=os.environ.get("S3_ENDPOINT") or None,
        region_name=os.environ.get("AWS_REGION") or None,
        config=Config(
            retries={"mode": "standard", "max_attempts": 5},
            connect_timeout=5,
            read_timeout=60,
        ),
    )


def _is_missing(exc: Exception) -> bool:
    code = getattr(exc, "response", {}).get("Error", {}).get("Code")
    return code in {"NoSuchKey", "404", "NotFound"}


def list_document_keys(client: Any, bucket: str, prefix: str) -> list[str]:
    keys: list[str] = []
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        keys.extend(obj["Key"] for obj in page.get("Contents", []) if obj["Key"].endswith(".json"))
    return sorted(keys)


def _read(client: Any, bucket: str, key: str) -> bytes:
    return client.get_object(Bucket=bucket, Key=key)["Body"].read()


def _join(prefix: str | None, key: str) -> str:
    joined = posixpath.join((prefix or "").strip("/"), key) if prefix else key
    if joined.startswith("/") or ".." in joined.split("/"):
        raise ValueError(f"refusing object key outside the bucket prefix: {joined!r}")
    return joined


def marker_key(event: dict[str, Any]) -> str:
    """Where a finished site's summary is kept: <key_prefix>/_etl/<run>/<host>.json."""
    return _join(
        event.get("key_prefix"),
        f"{MARKER_PREFIX}/{event['crawl_run_id']}/{event['target_domain']}.json",
    )


def _read_marker(client: Any, event: dict[str, Any]) -> dict[str, Any] | None:
    from botocore.exceptions import ClientError

    try:
        marker = json.loads(_read(client, event["bucket"], marker_key(event)))
    except ClientError as exc:
        if _is_missing(exc):
            return None
        raise
    if not isinstance(marker, dict) or marker.get("pipeline_version") != PIPELINE_VERSION:
        return None
    return marker


def _write_marker(client: Any, event: dict[str, Any], summary: dict[str, Any]) -> None:
    body = {
        "pipeline_version": PIPELINE_VERSION,
        "finished_at": datetime.now(UTC).isoformat(),
        "summary": summary,
    }
    client.put_object(
        Bucket=event["bucket"],
        Key=marker_key(event),
        Body=json.dumps(body, sort_keys=True).encode("utf-8"),
        ContentType="application/json",
    )


# ----------------------------------------------------------------- per page (executors)


def _decode(content: bytes, content_type: str | None) -> str:
    """HTML bytes as text: the charset of the response's Content-Type, else a <meta>
    charset in the first 4 KB, else UTF-8. Undecodable bytes become U+FFFD."""
    candidates: list[str] = []
    match = re.search(r"charset\s*=\s*([A-Za-z0-9_\-]+)", content_type or "", re.IGNORECASE)
    if match:
        candidates.append(match.group(1))
    meta = _CHARSET_RE.search(content[:4096])
    if meta:
        candidates.append(meta.group(1).decode("ascii", "ignore"))
    for charset in [*candidates, "utf-8"]:
        try:
            return content.decode(charset, errors="replace")
        except LookupError:
            continue
    return content.decode("utf-8", errors="replace")


def _published_at(document: dict[str, Any]) -> str | None:
    """The page's own publication time, when its Open Graph tags declare one."""
    open_graph = document.get("open_graph") or {}
    for name in ("article:published_time", "og:published_time", "article:modified_time"):
        value = open_graph.get(name) if isinstance(open_graph, dict) else None
        if not isinstance(value, str) or not value.strip():
            continue
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            continue
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.isoformat()
    return None


def process_page(client: Any, event: dict[str, Any], document_key: str) -> dict[str, Any]:
    """Scan, extract and transform one stored page.

    Returns {"status": "transformed", "record", "document"}, {"status": "rejected", ...}
    (failed the security checks), {"status": "skipped", ...} (nothing to index: a failed
    fetch, no text) or {"status": "failed", "error"} (this page's objects are missing or
    corrupt). Raises ScannerUnavailable when ClamAV cannot scan, and lets S3 connection
    errors propagate, so the whole run is retried rather than pages being dropped.
    """
    from botocore.exceptions import ClientError
    from security_scanner import inspect_bytes
    from transform import extract_html_text, normalize_text, transform_document

    bucket = event["bucket"]
    try:
        document = json.loads(_read(client, bucket, document_key))
    except ClientError as exc:
        if _is_missing(exc):
            return {"status": "failed", "key": document_key, "error": "document object missing"}
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        return {"status": "failed", "key": document_key, "error": f"invalid document JSON: {exc}"}
    if not isinstance(document, dict):
        return {"status": "failed", "key": document_key, "error": "document is not a JSON object"}

    source_url = (
        document.get("normalized_url") or document.get("final_url") or document.get("url") or ""
    )
    if not source_url:
        return {"status": "failed", "key": document_key, "error": "document has no URL"}
    status_code = document.get("status_code") or 200
    if document.get("error") or not (isinstance(status_code, int) and 200 <= status_code < 300):
        return {"status": "skipped", "key": document_key, "reason": "fetch failed"}

    # The rendered capture holds JavaScript-built content the raw HTML lacks.
    html_key = document.get("rendered_html_key") or document.get("html_key")
    if html_key:
        object_key = _join(event.get("key_prefix"), html_key)
        try:
            content = _read(client, bucket, object_key)
        except ClientError as exc:
            if _is_missing(exc):
                return {"status": "failed", "key": document_key, "error": "HTML object missing"}
            raise
        filename = posixpath.basename(html_key)
    else:
        # No HTML stored for this page: scan and use the text the scraper extracted.
        object_key = document_key
        content = (document.get("text") or "").encode("utf-8")
        filename = posixpath.basename(document_key).removesuffix(".json") + ".txt"

    scan = inspect_bytes(filename, content)
    if scan["clamav_status"] == "ERROR":
        raise ScannerUnavailable(
            f"ClamAV could not scan s3://{bucket}/{object_key}: {scan.get('clamav_error')}"
        )
    s3_uri = f"s3://{bucket}/{object_key}"
    if not scan["accepted"]:
        return {
            "status": "rejected",
            "key": document_key,
            "object_key": s3_uri,
            "source_url": source_url,
            "security_scan": scan,
            "document": document,
        }

    if filename.endswith((".html", ".htm")):
        text = extract_html_text(_decode(content, document.get("content_type")))
    else:
        text = normalize_text(content.decode("utf-8", errors="replace"))
    if not text:
        return {"status": "skipped", "key": document_key, "reason": "no text"}

    record = transform_document(
        source_url=source_url,
        text=text,
        title=document.get("title") or "",
        description=document.get("meta_description") or "",
        target_domain=event["target_domain"],
        object_key=s3_uri,
    )
    record["crawl_run_id"] = event["crawl_run_id"]
    record["fetched_at"] = document.get("fetched_at")
    record["category"] = document.get("category") or None
    keywords = [k for k in document.get("meta_keywords") or [] if isinstance(k, str)]
    record["keywords"] = keywords or None
    record["published_at"] = _published_at(document)
    record["security_scan"] = {key: scan[key] for key in ("verdict", "clamav_status", "sha256")}
    return {"status": "transformed", "key": document_key, "record": record, "document": document}


def _process_partition(event: dict[str, Any], document_keys: Iterable[str]) -> Iterator[dict]:
    client = s3_client()  # one client per Spark task, not per page
    for key in document_keys:
        yield process_page(client, event, key)


# ------------------------------------------------------------------------------ Spark


def create_spark_session(app_name: str) -> Any:
    """A SparkSession on ``SPARK_MASTER`` with the executor modules shipped. On a
    standalone cluster the executors connect back to this process (the driver), so
    it advertises its own container address. Executors get S3 and ClamAV settings from
    their own container environment (the Spark workers run the same image and env);
    nothing secret is copied into the Spark configuration, which the UIs display."""
    from pyspark.sql import SparkSession

    # The executors must run this interpreter (the ETL venv, with boto3 and clamd). A
    # SparkContext created from Python takes the executors' interpreter from
    # PYSPARK_PYTHON (default "python3", which in the ETL image is Airflow's), not
    # from spark.pyspark.python.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    master = os.environ.get("SPARK_MASTER", "local[*]")
    builder = (
        SparkSession.builder.master(master)
        .appName(app_name)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.scheduler.mode", "FAIR")  # concurrent sites share the cluster
        .config("spark.task.maxFailures", "2")  # the activity retries the whole site
    )
    if master.startswith("spark://"):
        builder = (
            builder.config("spark.driver.host", socket.gethostbyname(socket.gethostname()))
            .config("spark.driver.bindAddress", "0.0.0.0")
            .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "1g"))
            .config("spark.executor.cores", os.environ.get("SPARK_EXECUTOR_CORES", "1"))
        )
        if os.environ.get("SPARK_CORES_MAX"):
            builder = builder.config("spark.cores.max", os.environ["SPARK_CORES_MAX"])
    spark = builder.getOrCreate()
    for module in _EXECUTOR_MODULES:
        spark.sparkContext.addPyFile(str(_MODULE_DIR / module))
    return spark


def job_group(event: dict[str, Any]) -> str:
    return f"etl/{event['crawl_run_id']}/{event['target_domain']}"


def _run_group(
    spark: Any,
    event: dict[str, Any],
    keys: Sequence[str],
    process: Any = _process_partition,
) -> list[dict[str, Any]]:
    """Process one group of pages on the cluster; results come back to the driver."""
    sc = spark.sparkContext
    sc.setJobGroup(job_group(event), f"PGS site ETL {event['target_domain']}", True)
    slices = max(1, min(len(keys), sc.defaultParallelism * 2))
    return (
        sc.parallelize(list(keys), slices)
        .mapPartitions(lambda part: process(event, part))
        .collect()
    )


# ----------------------------------------------------------------------------- driver


def _new_summary(event: dict[str, Any], documents: int) -> dict[str, Any]:
    return {
        "crawl_run_id": event["crawl_run_id"],
        "target_domain": event["target_domain"],
        "crawl_status": event.get("status"),
        "documents": documents,
        "saved": 0,
        "duplicates": 0,
        "rejected": 0,
        "infected": 0,
        "skipped": 0,
        "failed": 0,
    }


class SiteWriter:
    """Saves one site's pages to PostgreSQL through pgs_db (imported lazily: the module
    is also imported by the executors and by tests that never touch a database)."""

    def __init__(self, session_factory: Any) -> None:
        self.session_factory = session_factory

    def check(self) -> None:
        from sqlalchemy import text
        from sqlalchemy.exc import SQLAlchemyError

        try:
            with self.session_factory() as session:
                session.execute(text("SELECT 1"))
        except SQLAlchemyError as exc:
            raise DependencyUnavailable(f"PostgreSQL is not answering: {exc}") from exc

    def save(self, record: dict[str, Any], document: dict[str, Any]) -> bool:
        """Save one page; True if Silver folded it into an existing duplicate."""
        from pgs_db.etl import save_transformed

        saved = save_transformed(
            self.session_factory,
            record,
            geo_confidence=GEO_CONFIDENCE,
            bronze_document=document,
        )
        return bool(saved.duplicate)

    def log_page_error(self, event: dict[str, Any], url: str, error: str, error_type: str) -> None:
        from pgs_db.enums import LogSeverity, ServiceName
        from pgs_db.repositories.ops import OpsRepository

        with self.session_factory() as session, session.begin():
            OpsRepository(session).log_error(
                ServiceName.ETL,
                LogSeverity.ERROR,
                error[:2000],
                instance=socket.gethostname(),
                error_type=error_type,
                url=url or None,
                context={"event": event},
            )

    def log_rejection(self, event: dict[str, Any], result: dict[str, Any]) -> None:
        from pgs_db.enums import LogSeverity, ServiceName
        from pgs_db.repositories.ops import OpsRepository

        scan = result["security_scan"]
        with self.session_factory() as session, session.begin():
            OpsRepository(session).log_error(
                ServiceName.SECURITY,
                LogSeverity.ERROR if scan["clamav_status"] == "INFECTED" else LogSeverity.WARN,
                f"intake checks rejected {result['object_key']}: {', '.join(scan['findings'])}",
                instance=socket.gethostname(),
                error_type="INFECTED" if scan["clamav_status"] == "INFECTED" else "REJECTED",
                url=result.get("source_url") or None,
                context={"event": event, "security_scan": scan, "document_key": result["key"]},
            )


def _transient_db_error(exc: BaseException) -> bool:
    from sqlalchemy.exc import DBAPIError, OperationalError

    if isinstance(exc, OperationalError):
        return True
    return isinstance(exc, DBAPIError) and bool(exc.connection_invalidated)


def finish_group(
    results: Sequence[dict[str, Any]],
    event: dict[str, Any],
    summary: dict[str, Any],
    writer: SiteWriter,
    embedder: Any,
) -> None:
    """Embed and save one group of page results (driver side)."""
    from sqlalchemy.exc import SQLAlchemyError

    for result in results:
        status = result["status"]
        if status == "rejected":
            summary["rejected"] += 1
            if result["security_scan"]["clamav_status"] == "INFECTED":
                summary["infected"] += 1
            logger.warning(
                "rejected by intake checks: %s %s",
                result["object_key"],
                result["security_scan"]["findings"],
            )
            writer.log_rejection(event, result)
        elif status == "skipped":
            summary["skipped"] += 1
        elif status == "failed":
            summary["failed"] += 1
            logger.warning("page %s failed: %s", result["key"], result["error"])
            writer.log_page_error(event, "", f"{result['key']}: {result['error']}", "PAGE_INVALID")

    transformed = [result for result in results if result["status"] == "transformed"]
    vectors = embedder.embed([result["record"]["searchable_text"] for result in transformed])
    for result, vector in zip(transformed, vectors, strict=True):
        record = result["record"]
        if vector is not None:
            record["embedding"] = vector
            record["embedding_model"] = embedder.model_name
            record["embedding_dim"] = len(vector)
        try:
            duplicate = writer.save(record, result["document"])
        except SQLAlchemyError as exc:
            if _transient_db_error(exc):
                raise DependencyUnavailable(f"PostgreSQL failed while saving: {exc}") from exc
            summary["failed"] += 1
            logger.exception("could not save %s", record["source_url"])
            writer.log_page_error(event, record["source_url"], str(exc), type(exc).__name__)
            continue
        except (ValueError, LookupError) as exc:
            summary["failed"] += 1
            logger.warning("could not save %s: %s", record["source_url"], exc)
            writer.log_page_error(event, record["source_url"], str(exc), type(exc).__name__)
            continue
        summary["saved"] += 1
        summary["duplicates"] += int(duplicate)


def _groups(keys: Sequence[str], size: int) -> Iterator[Sequence[str]]:
    for start in range(0, len(keys), size):
        yield keys[start : start + size]


def run_site_pipeline(
    event: dict[str, Any],
    *,
    spark: Any,
    writer: SiteWriter,
    embedder: Any,
    s3: Any = None,
    check_scanner: bool = True,
) -> dict[str, Any]:
    """Run the whole ETL for one crawled site and return its summary."""
    from botocore.exceptions import BotoCoreError, ClientError

    event = validate_event(event)
    client = s3 or s3_client()
    try:
        marker = _read_marker(client, event)
    except (BotoCoreError, ClientError) as exc:
        raise DependencyUnavailable(f"S3 is not answering: {exc}") from exc
    if marker is not None:
        logger.info("site %s already processed (%s)", job_group(event), marker["finished_at"])
        return {**marker["summary"], "already_processed": True}

    if check_scanner:
        from security_scanner import ping_clamav

        try:
            ping_clamav()
        except Exception as exc:  # any clamd/socket failure means "cannot scan"
            raise ScannerUnavailable(f"ClamAV is not answering: {exc}") from exc
    writer.check()

    try:
        keys = list_document_keys(client, event["bucket"], event["documents_prefix"])
    except (BotoCoreError, ClientError) as exc:
        raise DependencyUnavailable(f"cannot list s3://{event['bucket']}: {exc}") from exc
    summary = _new_summary(event, len(keys))
    for group in _groups(keys, PAGE_GROUP_SIZE):
        results = _run_group(spark, event, group)
        finish_group(results, event, summary, writer, embedder)
        logger.info("site %s: %s", job_group(event), summary)

    try:
        _write_marker(client, event, summary)
    except (BotoCoreError, ClientError) as exc:
        # The pages are saved; without the marker a re-run just saves them again.
        logger.warning("could not write the marker for %s: %s", job_group(event), exc)
    return summary
