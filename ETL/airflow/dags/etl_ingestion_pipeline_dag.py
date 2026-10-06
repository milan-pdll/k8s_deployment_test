"""ETL ingestion pipeline DAG: batches of crawled sites, executed on Temporal.

The scraper publishes one ``site_crawl_completed`` event to Kafka
(``scraped_files_topic``) per website whose crawl has finished. This DAG
waits until a full batch has accumulated, then:

1. ``poll_batch`` reads the next ``ETL_BATCH_SIZE`` events (default 10), not
   committing them. With fewer waiting it skips the run, unless the oldest has
   waited ``ETL_BATCH_MAX_WAIT_MINUTES`` (default 60), so the tail of a crawl
   is not held back forever;
2. ``process_batch`` starts one ``EtlBatchWorkflow`` on Temporal for the batch
   and waits for it. The ETL worker (ETL/temporal) runs it as the Spark driver,
   on the Spark cluster: per site, security scan, extraction, dedup,
   embedding and output (ETL/spark/site_pipeline.py);
3. ``commit_offsets`` commits the batch once the workflow succeeded, so a
   failed batch is read and run again.

Airflow only schedules and tracks; the work itself runs on Temporal and Spark.
"""

from __future__ import annotations

import json
import os
import sys
import time
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException

SPARK_LIB = "/opt/airflow/spark_lib"
KAFKA_BOOTSTRAP = "kafka:29092"
KAFKA_TOPIC = "scraped_files_topic"
KAFKA_GROUP = "etl-ingestion-pipeline"
TEMPORAL_ADDRESS = os.environ.get("TEMPORAL_ADDRESS", "temporal:7233")
ETL_TASK_QUEUE = "etl-task-queue"  # ETL/temporal/etl_workflows.py
BATCH_SIZE = int(os.environ.get("ETL_BATCH_SIZE", "10"))
BATCH_MAX_WAIT = timedelta(minutes=int(os.environ.get("ETL_BATCH_MAX_WAIT_MINUTES", "60")))
SITE_CONCURRENCY = int(os.environ.get("ETL_SITE_CONCURRENCY", "4"))


def _site_consumer():
    """A consumer with the topic's partitions assigned (no group rebalancing),
    positioned at the group's committed offsets."""
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BOOTSTRAP],
        group_id=KAFKA_GROUP,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        # The scraper's first event creates the topic; a metadata request from here must
        # not (it races that creation and can leave the consumer waiting for a leader).
        allow_auto_create_topics=False,
    )
    partitions = consumer.partitions_for_topic(KAFKA_TOPIC) or set()
    consumer.assign([TopicPartition(KAFKA_TOPIC, p) for p in sorted(partitions)])
    return consumer


def _batch_workflow_id(polled: list[dict]) -> str:
    """The batch's offset range per partition, e.g. ``etl-batch-p0-100-199``."""
    ranges: dict[int, tuple[int, int]] = {}
    for item in polled:
        first, last = ranges.get(item["partition"], (item["offset"], item["offset"]))
        ranges[item["partition"]] = (min(first, item["offset"]), max(last, item["offset"]))
    return "etl-batch-" + "_".join(f"p{p}-{a}-{b}" for p, (a, b) in sorted(ranges.items()))


@dag(
    dag_id="etl_ingestion_pipeline",
    schedule=timedelta(minutes=2),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # one batch at a time: offsets are committed in order
    default_args={"retries": 2, "retry_delay": timedelta(minutes=2)},
    tags=["etl", "ingestion", "kafka", "temporal", "spark"],
)
def etl_ingestion_pipeline():
    @task(execution_timeout=timedelta(minutes=5))
    def poll_batch() -> list[dict]:
        """The batch, as {"partition", "offset", "event"} per message; "event" is
        None for a message that isn't a valid site event (it is still committed,
        so one bad message can't block the topic)."""
        if SPARK_LIB not in sys.path:
            sys.path.insert(0, SPARK_LIB)
        from site_pipeline import validate_event

        consumer = _site_consumer()
        polled: list[dict] = []
        oldest_ms: int | None = None
        try:
            assigned = list(consumer.assignment())
            if not assigned:
                raise AirflowSkipException(f"topic {KAFKA_TOPIC} does not exist yet")
            end = consumer.end_offsets(assigned)
            start = consumer.beginning_offsets(assigned)
            waiting = 0
            for tp in assigned:
                committed = consumer.committed(tp, timeout_ms=15000)
                waiting += end[tp] - (start[tp] if committed is None else committed)
            if waiting == 0:
                raise AirflowSkipException("no site events waiting")
            wanted = min(waiting, BATCH_SIZE)
            deadline = time.monotonic() + 60
            while len(polled) < wanted and time.monotonic() < deadline:
                batch = consumer.poll(timeout_ms=5000, max_records=wanted - len(polled))
                for messages in batch.values():
                    for message in messages:
                        ts = message.timestamp
                        oldest_ms = ts if oldest_ms is None else min(oldest_ms, ts)
                        try:
                            event = validate_event(json.loads(message.value))
                        except ValueError as exc:  # also json.JSONDecodeError
                            where = f"partition {message.partition} offset {message.offset}"
                            print(f"skipping {where}: {exc}")
                            event = None
                        position = {"partition": message.partition, "offset": message.offset}
                        polled.append({**position, "event": event})
        finally:
            consumer.close(autocommit=False)

        if len(polled) < BATCH_SIZE:
            waited = timedelta(milliseconds=time.time() * 1000 - (oldest_ms or 0))
            if waited < BATCH_MAX_WAIT:
                minutes = waited.total_seconds() / 60
                raise AirflowSkipException(
                    f"{len(polled)}/{BATCH_SIZE} site events waiting; the oldest is "
                    f"{minutes:.0f} min old (flushed after {BATCH_MAX_WAIT})"
                )
            print(f"flushing a partial batch: the oldest event waited {waited}")
        print(f"batch of {sum(1 for item in polled if item['event'])} site(s)")
        return polled

    @task(execution_timeout=timedelta(hours=12))
    def process_batch(polled: list[dict]) -> dict | None:
        """Run the batch as one Temporal workflow and wait for it. The workflow ID
        is the batch's offset range, so a retry of this task re-attaches to the
        same workflow instead of starting a second one."""
        import asyncio

        from temporalio.client import Client
        from temporalio.common import WorkflowIDReusePolicy
        from temporalio.exceptions import WorkflowAlreadyStartedError

        events = [item["event"] for item in polled if item["event"]]
        if not events:
            return None
        workflow_id = _batch_workflow_id(polled)

        async def run() -> dict:
            client = await Client.connect(TEMPORAL_ADDRESS)
            try:
                handle = await client.start_workflow(
                    "EtlBatchWorkflow",
                    {"events": events, "site_concurrency": SITE_CONCURRENCY},
                    id=workflow_id,
                    task_queue=ETL_TASK_QUEUE,
                    # A failed batch runs again under the same ID; a finished one is reused.
                    id_reuse_policy=WorkflowIDReusePolicy.ALLOW_DUPLICATE_FAILED_ONLY,
                )
                print(f"started Temporal workflow {workflow_id} for {len(events)} site(s)")
            except WorkflowAlreadyStartedError:
                handle = client.get_workflow_handle(workflow_id)
                print(f"waiting for the existing Temporal workflow {workflow_id}")
            return await handle.result()

        result = asyncio.run(run())
        for summary in result["summaries"]:
            print(json.dumps(summary))
        return {"workflow_id": workflow_id, "sites": result["sites"]}

    @task(execution_timeout=timedelta(minutes=5))
    def commit_offsets(polled: list[dict]) -> None:
        from kafka import OffsetAndMetadata, TopicPartition

        offsets: dict[int, int] = {}
        for item in polled:
            offsets[item["partition"]] = max(offsets.get(item["partition"], 0), item["offset"] + 1)
        consumer = _site_consumer()
        try:
            consumer.commit(
                {
                    TopicPartition(KAFKA_TOPIC, partition): OffsetAndMetadata(offset, "", -1)
                    for partition, offset in offsets.items()
                }
            )
        finally:
            consumer.close(autocommit=False)

    polled = poll_batch()
    process_batch(polled) >> commit_offsets(polled)


etl_ingestion_pipeline()
