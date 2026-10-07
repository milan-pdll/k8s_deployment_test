"""EtlBatchWorkflow's outcome rules, on Temporal's time-skipping test server.

    cd ETL/temporal && /opt/etl-venv/bin/python -m unittest test_etl_workflows

(The test server binary is downloaded by temporalio on first use.)
"""

from __future__ import annotations

import unittest
import uuid
from typing import Any

from etl_workflows import EtlBatchWorkflow
from temporalio import activity
from temporalio.client import WorkflowFailureError
from temporalio.exceptions import ApplicationError
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Worker

QUEUE = "etl-test"
dead_letters: list[dict[str, Any]] = []
# target_domain -> "ok" | "poison" | "dependency"
behaviour: dict[str, str] = {}


@activity.defn(name="run_site_pipeline")
async def fake_run_site(event: dict[str, Any]) -> dict[str, Any]:
    mode = behaviour[event["target_domain"]]
    if mode == "poison":
        raise ApplicationError("corrupt site", type="SitePoisoned", non_retryable=True)
    if mode == "dependency":
        raise ApplicationError("ClamAV down", type="ScannerUnavailable", non_retryable=True)
    return {"target_domain": event["target_domain"], "saved": 1}


@activity.defn(name="record_dead_letter")
async def fake_dead_letter(entry: dict[str, Any]) -> None:
    dead_letters.append(entry)


def event(host: str) -> dict[str, Any]:
    return {
        "event_type": "site_crawl_completed",
        "crawl_run_id": 1,
        "target_domain": host,
        "bucket": "b",
        "documents_prefix": f"1/{host}/",
    }


class EtlBatchWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        dead_letters.clear()
        behaviour.clear()
        self.env = await WorkflowEnvironment.start_time_skipping()

    async def asyncTearDown(self) -> None:
        await self.env.shutdown()

    async def run_batch(self, hosts: dict[str, str]) -> dict[str, Any]:
        behaviour.update(hosts)
        async with Worker(
            self.env.client,
            task_queue=QUEUE,
            workflows=[EtlBatchWorkflow],
            activities=[fake_run_site, fake_dead_letter],
        ):
            return await self.env.client.execute_workflow(
                "EtlBatchWorkflow",
                {"events": [event(host) for host in hosts], "site_concurrency": 2},
                id=f"test-{uuid.uuid4()}",
                task_queue=QUEUE,
            )

    async def test_all_sites_succeed(self) -> None:
        result = await self.run_batch({"a.np": "ok", "b.np": "ok"})
        self.assertEqual((result["sites"], result["dead_lettered"]), (2, 0))
        self.assertEqual(dead_letters, [])

    async def test_a_poisoned_site_is_dead_lettered_and_the_batch_completes(self) -> None:
        result = await self.run_batch({"a.np": "ok", "bad.np": "poison"})
        self.assertEqual(result["dead_lettered"], 1)
        self.assertEqual([entry["event"]["target_domain"] for entry in dead_letters], ["bad.np"])

    async def test_every_site_poisoned_means_a_broken_pipeline_not_bad_sites(self) -> None:
        with self.assertRaises(WorkflowFailureError):
            await self.run_batch({"a.np": "poison", "b.np": "poison"})
        self.assertEqual(dead_letters, [])

    async def test_a_dependency_failure_fails_the_batch(self) -> None:
        with self.assertRaises(WorkflowFailureError):
            await self.run_batch({"a.np": "ok", "b.np": "dependency"})
        self.assertEqual(dead_letters, [])


if __name__ == "__main__":
    unittest.main()
