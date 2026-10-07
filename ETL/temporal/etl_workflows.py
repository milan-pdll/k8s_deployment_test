"""Temporal workflow for one batch of crawled sites.

The ``etl_ingestion_pipeline`` Airflow DAG starts one ``EtlBatchWorkflow`` per batch of
``site_crawl_completed`` events (workflow ID = the batch's Kafka offset range) and waits
for it; the ETL worker (worker.py) runs it. Each site is one ``run_site_pipeline``
activity, at most ``site_concurrency`` at a time, so a failing site is retried on its own.

Outcome:
- every site finished, or was dead-lettered (an invalid event, or a site that kept
  failing while ClamAV, PostgreSQL and S3 were healthy -- recorded in error_logs with
  its event, for replay): the workflow completes, and the DAG commits the batch's
  offsets;
- a site still failed for a dependency reason after its retries, or EVERY site of the
  batch failed (then the pipeline is broken, not the sites): the workflow fails, the
  offsets stay uncommitted, and the next DAG run retries the batch (sites that finished
  are skipped by their marker, see site_pipeline.py).

Workflow code runs in Temporal's sandbox: only temporalio and the standard library are
imported here; activities are referenced by name.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

TASK_QUEUE = "etl-task-queue"
DEFAULT_SITE_CONCURRENCY = 4
# Error types after which a site is given up on (see etl_activities.py).
DEAD_LETTER_TYPES = frozenset({"InvalidEvent", "SitePoisoned"})

SITE_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=30),
    backoff_coefficient=2.0,
    maximum_interval=timedelta(minutes=10),
    # ~1.5 h of retries; a dependency outage longer than that fails the batch, which
    # Airflow then runs again.
    maximum_attempts=12,
    non_retryable_error_types=sorted(DEAD_LETTER_TYPES),
)
DEAD_LETTER_RETRY = RetryPolicy(
    initial_interval=timedelta(seconds=10),
    maximum_interval=timedelta(minutes=5),
    maximum_attempts=0,  # until PostgreSQL takes it: losing a dead letter loses the site
)


def _failure_type(error: BaseException) -> str:
    cause = error.cause if isinstance(error, ActivityError) else None
    if isinstance(cause, ApplicationError):
        return cause.type or "ApplicationError"
    return type(cause or error).__name__


def _describe(error: BaseException) -> str:
    cause = error.cause if isinstance(error, ActivityError) else None
    return f"{_failure_type(error)}: {cause or error}"


@workflow.defn(name="EtlBatchWorkflow")
class EtlBatchWorkflow:
    @workflow.run
    async def run(self, batch: dict[str, Any]) -> dict[str, Any]:
        events: list[dict[str, Any]] = batch["events"]
        concurrency = int(batch.get("site_concurrency", DEFAULT_SITE_CONCURRENCY))
        limit = asyncio.Semaphore(max(1, concurrency))
        batch_id = workflow.info().workflow_id

        async def site(event: dict[str, Any]) -> dict[str, Any]:
            async with limit:
                try:
                    return await workflow.execute_activity(
                        "run_site_pipeline",
                        event,
                        start_to_close_timeout=timedelta(hours=2),
                        # The activity heartbeats every 30 s: a dead worker is noticed
                        # within 2 minutes and the site retried on another one.
                        heartbeat_timeout=timedelta(minutes=2),
                        retry_policy=SITE_RETRY,
                    )
                except ActivityError as error:
                    if _failure_type(error) not in DEAD_LETTER_TYPES:
                        raise
                    return {
                        "crawl_run_id": event.get("crawl_run_id"),
                        "target_domain": event.get("target_domain"),
                        "dead_lettered": True,
                        "error": str(error.cause or error),
                        "event": event,
                    }

        results = await asyncio.gather(*(site(event) for event in events), return_exceptions=True)
        failed = [
            f"{event.get('target_domain')} (run {event.get('crawl_run_id')}): {_describe(result)}"
            for event, result in zip(events, results, strict=True)
            if isinstance(result, BaseException)
        ]
        summaries = [result for result in results if isinstance(result, dict)]
        poisoned = [summary for summary in summaries if summary.get("dead_lettered")]
        if poisoned and len(poisoned) == len(events):
            # Every site "poisoned" means the pipeline itself is broken (a bad image or
            # configuration), not the sites: keep the batch for a retry after the fix
            # instead of dead-lettering all of it.
            failed.extend(f"{s['target_domain']}: {s['error']}" for s in poisoned)
        if failed:
            # The batch's Kafka offsets stay uncommitted, so Airflow runs it again;
            # sites that finished are skipped then (their marker exists).
            raise ApplicationError(
                f"{len(failed)} of {len(events)} site(s) failed: " + "; ".join(failed),
                type="BatchIncomplete",
                non_retryable=True,
            )
        for summary in poisoned:
            await workflow.execute_activity(
                "record_dead_letter",
                {"event": summary.pop("event"), "error": summary["error"], "batch": batch_id},
                start_to_close_timeout=timedelta(minutes=1),
                retry_policy=DEAD_LETTER_RETRY,
            )
        return {
            "sites": len(events),
            "dead_lettered": sum(1 for summary in summaries if summary.get("dead_lettered")),
            "summaries": summaries,
        }
