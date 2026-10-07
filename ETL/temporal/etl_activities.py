"""Temporal activities of the ETL worker.

The worker process is the Spark driver: it holds one long-lived SparkSession on the
standalone cluster (``SPARK_MASTER``), one LaBSE model and one PostgreSQL connection
pool, and each ``run_site_pipeline`` activity runs one site's job on them
(ETL/spark/site_pipeline.py).

Error contract with EtlBatchWorkflow (etl_workflows.py):

- ``InvalidEvent`` (non-retryable): the event can never be processed; dead-lettered.
- ``ScannerUnavailable`` / ``DependencyUnavailable`` (retryable): ClamAV, PostgreSQL or
  S3 is down. Retried with backoff; never dead-lettered -- if retries run out the
  batch fails and Airflow runs it again later, so nothing is skipped unscanned.
- any other failure is retried; from attempt ``ETL_MAX_SITE_ATTEMPTS`` on, with the
  dependencies healthy, it becomes ``SitePoisoned`` (non-retryable) and the site is
  dead-lettered instead of blocking its batch (and the Kafka partition) forever.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from temporalio import activity
from temporalio.exceptions import ApplicationError

SPARK_LIB = os.environ.get("SPARK_LIB", "/opt/airflow/spark_lib")
if SPARK_LIB not in sys.path:
    sys.path.insert(0, SPARK_LIB)

import site_pipeline  # noqa: E402  (needs SPARK_LIB on sys.path)
from embeddings import default_embedder  # noqa: E402
from site_event import validate_event  # noqa: E402

logger = logging.getLogger(__name__)

HEARTBEAT_SECONDS = 30
MAX_SITE_ATTEMPTS = int(os.environ.get("ETL_MAX_SITE_ATTEMPTS", "5"))
_RETRYABLE = (site_pipeline.ScannerUnavailable, site_pipeline.DependencyUnavailable)


class SiteRunner:
    """The process-wide Spark session, database pool and site thread pool."""

    def __init__(self, concurrency: int) -> None:
        self.executor = ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="etl-site")
        self._lock = threading.Lock()
        self._spark: Any = None
        self._writer: site_pipeline.SiteWriter | None = None

    def spark(self) -> Any:
        """The shared SparkSession, recreated if its SparkContext has stopped (for
        example after the Spark master restarted)."""
        with self._lock:
            jsc = self._spark.sparkContext._jsc if self._spark is not None else None
            if jsc is None or jsc.sc().isStopped():
                self._spark = site_pipeline.create_spark_session("pgs-etl-worker")
            return self._spark

    def writer(self) -> site_pipeline.SiteWriter:
        with self._lock:
            if self._writer is None:
                from pgs_db import make_session_factory

                self._writer = site_pipeline.SiteWriter(make_session_factory())
            return self._writer

    def run(self, event: dict[str, Any]) -> dict[str, Any]:
        return site_pipeline.run_site_pipeline(
            event, spark=self.spark(), writer=self.writer(), embedder=default_embedder()
        )

    def cancel(self, event: dict[str, Any]) -> None:
        if self._spark is not None:
            self._spark.sparkContext.cancelJobGroup(site_pipeline.job_group(event))

    def shutdown(self) -> None:
        self.executor.shutdown(wait=True, cancel_futures=True)
        with self._lock:
            if self._spark is not None:
                self._spark.stop()
                self._spark = None


_runner: SiteRunner | None = None


def install_runner(runner: SiteRunner) -> None:
    global _runner
    _runner = runner


def _get_runner() -> SiteRunner:
    if _runner is None:
        raise RuntimeError("install_runner() was not called by the worker")
    return _runner


@activity.defn(name="run_site_pipeline")
async def run_site_pipeline(event: dict[str, Any]) -> dict[str, Any]:
    """Run one site on a worker thread, heartbeating while it runs."""
    try:
        event = validate_event(event)
    except ValueError as exc:
        raise ApplicationError(str(exc), type="InvalidEvent", non_retryable=True) from exc
    runner = _get_runner()
    info = activity.info()
    site = site_pipeline.job_group(event)
    logger.info("site %s: attempt %d (workflow %s)", site, info.attempt, info.workflow_id)
    future = asyncio.get_running_loop().run_in_executor(runner.executor, runner.run, event)
    try:
        while True:
            done, _ = await asyncio.wait({future}, timeout=HEARTBEAT_SECONDS)
            if done:
                break
            activity.heartbeat(site)
        summary = future.result()
    except asyncio.CancelledError:
        logger.warning("site %s: cancelled, stopping its Spark jobs", site)
        runner.cancel(event)
        raise
    except _RETRYABLE as exc:
        logger.warning("site %s: dependency unavailable: %s", site, exc)
        raise ApplicationError(str(exc), type=type(exc).__name__) from exc
    except Exception as exc:
        if info.attempt >= MAX_SITE_ATTEMPTS:
            logger.exception("site %s: failed %d times, giving up", site, info.attempt)
            raise ApplicationError(
                f"failed {info.attempt} times: {exc}", type="SitePoisoned", non_retryable=True
            ) from exc
        logger.exception("site %s: attempt %d failed", site, info.attempt)
        raise
    logger.info("site %s: %s", site, summary)
    return summary


@activity.defn(name="record_dead_letter")
async def record_dead_letter(entry: dict[str, Any]) -> None:
    """Keep a site the ETL gave up on in error_logs, with its event for replay."""

    def write() -> None:
        from pgs_db.enums import LogSeverity, ServiceName
        from pgs_db.repositories.ops import OpsRepository

        event = entry["event"]
        writer = _get_runner().writer()
        with writer.session_factory() as session, session.begin():
            OpsRepository(session).log_error(
                ServiceName.ETL,
                LogSeverity.ERROR,
                f"ETL dead-lettered site {event.get('target_domain')} "
                f"(run {event.get('crawl_run_id')}): {entry['error']}"[:2000],
                instance=socket.gethostname(),
                error_type="SITE_DEAD_LETTERED",
                context={"event": event, "error": entry["error"], "batch": entry.get("batch")},
            )

    await asyncio.to_thread(write)
