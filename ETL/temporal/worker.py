"""ETL Temporal worker: runs EtlBatchWorkflow and its activities on the
``etl-task-queue`` task queue, as the Spark driver.

    TEMPORAL_ADDRESS=temporal:7233 SPARK_MASTER=spark://spark-master:7077 \
        DATABASE_URL=postgresql+psycopg://pgs_etl:...@postgres:5432/pgs python worker.py

On SIGTERM it stops taking new work and gives running sites ``ETL_SHUTDOWN_GRACE_SECONDS``
(default 90) to finish; whatever is still running then is cancelled and retried by
Temporal on the next worker (docker-compose gives the container a longer stop grace
period than this). While the worker runs it touches ``ETL_HEARTBEAT_FILE`` every 30 s;
the container healthcheck checks that file's age.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import signal
import time
from datetime import timedelta
from pathlib import Path

from etl_activities import SiteRunner, install_runner, record_dead_letter, run_site_pipeline
from etl_workflows import DEFAULT_SITE_CONCURRENCY, TASK_QUEUE, EtlBatchWorkflow
from temporalio.client import Client
from temporalio.worker import Worker


async def _heartbeat(path: str, stop: asyncio.Event) -> None:
    while not stop.is_set():
        Path(path).write_text(str(int(time.time())))
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=30)


async def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    concurrency = int(os.environ.get("ETL_SITE_CONCURRENCY", DEFAULT_SITE_CONCURRENCY))
    grace = timedelta(seconds=int(os.environ.get("ETL_SHUTDOWN_GRACE_SECONDS", "90")))
    client = await Client.connect(
        os.environ.get("TEMPORAL_ADDRESS", "localhost:7233"),
        namespace=os.environ.get("TEMPORAL_NAMESPACE", "default"),
    )
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    runner = SiteRunner(concurrency)
    install_runner(runner)
    worker = Worker(
        client,
        task_queue=TASK_QUEUE,
        workflows=[EtlBatchWorkflow],
        activities=[run_site_pipeline, record_dead_letter],
        # Sites, plus room for the small dead-letter writes.
        max_concurrent_activities=concurrency + 2,
        graceful_shutdown_timeout=grace,
    )
    logging.info("ETL worker on task queue %s (%d site(s) at once)", TASK_QUEUE, concurrency)
    heartbeat_file = os.environ.get("ETL_HEARTBEAT_FILE")
    try:
        async with worker:
            if heartbeat_file:
                heartbeat = asyncio.create_task(_heartbeat(heartbeat_file, stop))
            await stop.wait()
            if heartbeat_file:
                await heartbeat
            logging.info("stopping: finishing running sites (up to %s)", grace)
    finally:
        runner.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
