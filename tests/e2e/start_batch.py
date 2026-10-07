"""Run one EtlBatchWorkflow for the site event given as argv[1] and print its result
(what the etl_ingestion_pipeline DAG does, minus the Kafka batching). Runs in the ETL
image, on the compose network (TEMPORAL_ADDRESS from the etl-worker environment)."""

from __future__ import annotations

import asyncio
import json
import os
import sys

from temporalio.client import Client


async def main() -> None:
    event = json.loads(sys.argv[1])
    client = await Client.connect(os.environ["TEMPORAL_ADDRESS"])
    result = await client.execute_workflow(
        "EtlBatchWorkflow",
        {"events": [event], "site_concurrency": 1},
        id=f"e2e-batch-{event['crawl_run_id']}",
        task_queue="etl-task-queue",
    )
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    asyncio.run(main())
