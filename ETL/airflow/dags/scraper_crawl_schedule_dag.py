"""Scheduled crawl of every website in the database.

Every ``SCRAPER_CRAWL_SCHEDULE`` (default every 30 minutes) this DAG reads the
websites from PostgreSQL (``domains``; PAUSED ones are left out) and starts
one ``CrawlDomainsWorkflow`` on Temporal for all of them. The Go scraper
worker runs it: each website is crawled as its own child workflow, and when a
site is done the worker publishes its ``site_crawl_completed`` event to Kafka,
which ``etl_ingestion_pipeline`` processes in batches.

The workflow has a fixed ID, so a tick while the previous crawl is still
running is skipped instead of starting a second, overlapping crawl.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "temporal:7233")
SCRAPER_TASK_QUEUE = "scraper-task-queue"  # scraper/internal/workflows TaskQueueName
CRAWL_WORKFLOW_ID = "scheduled-crawl-domains"
# domains.priority -> crawl priority (higher is crawled first)
PRIORITY = {"HIGH": 10, "NORMAL": 5, "LOW": 1}


def _env_int(name: str, default: int) -> int:
    return int(os.environ.get(name) or default)


def _load_seeds() -> list[dict]:
    """One seed per website, as the Go workflow's Seed struct."""
    import psycopg2

    # pgs_etl's URL (postgresql+psycopg://...) in the libpq form psycopg2 takes.
    url = os.environ["DATABASE_URL"].replace("postgresql+psycopg://", "postgresql://", 1)
    with psycopg2.connect(url) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT domain, category, priority FROM domains "
            "WHERE status <> 'PAUSED' ORDER BY domain"
        )
        rows = cur.fetchall()
    return [
        {
            "URL": f"https://{domain}/",
            "Category": str(category).lower(),
            "Priority": PRIORITY.get(str(priority), 5),
        }
        for domain, category, priority in rows
    ]


@dag(
    dag_id="scraper_crawl_schedule",
    schedule=os.environ.get("SCRAPER_CRAWL_SCHEDULE", "*/30 * * * *"),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["scraper", "temporal"],
)
def scraper_crawl_schedule():
    @task
    def start_crawl() -> str:
        import asyncio

        from temporalio.client import Client
        from temporalio.common import WorkflowIDReusePolicy
        from temporalio.exceptions import WorkflowAlreadyStartedError

        seeds = _load_seeds()
        if not seeds:
            raise AirflowSkipException("no websites in the domains table")
        revisit_hours = _env_int("SCRAPER_REVISIT_AFTER_HOURS", 0)
        # CrawlDomainsInput (scraper/internal/workflows/domains_workflow.go); Go's JSON
        # decoding matches these field names.
        crawl = {
            "Seeds": seeds,
            "MaxConcurrentDomains": _env_int("SCRAPER_CONCURRENT_DOMAINS", 10),
            # Domains are crawled in batches: the next batch starts when the
            # whole previous one has published its site events.
            "DomainBatchSize": _env_int("SCRAPER_DOMAIN_BATCH_SIZE", 10),
            "PerDomain": {
                "MaxDepth": _env_int("SCRAPER_MAX_DEPTH", 3),
                "MaxPages": _env_int("SCRAPER_MAX_PAGES_PER_DOMAIN", 100),
                "Concurrency": 8,
                "MaxConcurrentPerHost": _env_int("SCRAPER_MAX_CONCURRENT_PER_HOST", 2),
                "TaskQueueShards": _env_int("SCRAPER_TASK_QUEUE_SHARDS", 1),
                "SameHostOnly": True,
                "CountryFilter": os.environ.get("SCRAPER_COUNTRY_FILTER", ""),
                "RevisitAfter": revisit_hours * 3600 * 10**9,  # time.Duration, nanoseconds
            },
        }

        async def start() -> str:
            client = await Client.connect(TEMPORAL_ADDRESS)
            try:
                handle = await client.start_workflow(
                    "CrawlDomainsWorkflow",
                    crawl,
                    id=CRAWL_WORKFLOW_ID,
                    task_queue=SCRAPER_TASK_QUEUE,
                    id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE,
                )
            except WorkflowAlreadyStartedError as exc:
                raise AirflowSkipException("the previous crawl is still running") from exc
            return handle.result_run_id or ""

        run_id = asyncio.run(start())
        print(f"started {CRAWL_WORKFLOW_ID} (run {run_id}) for {len(seeds)} website(s)")
        return run_id

    start_crawl()


scraper_crawl_schedule()
