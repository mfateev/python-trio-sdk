"""E2E tests for upsert_search_attributes with real Temporal server."""

import time

import pytest
import trio
from temporalio.common import SearchAttributeKey

from temporalio_trio import workflow
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker

KW_KEY = SearchAttributeKey.for_keyword("CustomKeywordField")
INT_KEY = SearchAttributeKey.for_int("CustomIntField")


@pytest.fixture
async def trio_client():
    """Create a Trio-based Temporal client."""
    client = await Client.connect("localhost:7233", namespace="default")
    yield client
    await client.close()


@workflow.defn
class SearchAttributeWorkflow:
    """Workflow that upserts search attributes."""

    @workflow.run
    async def run(self, status: str) -> str:
        # Update search attribute to track workflow status
        workflow.upsert_search_attributes([
            KW_KEY.value_set(status),
        ])

        # Simulate some work
        await workflow.sleep(0.1)

        # Update to completed
        workflow.upsert_search_attributes([
            KW_KEY.value_set("completed"),
        ])

        return f"processed-{status}"


@workflow.defn
class MultipleSearchAttributesWorkflow:
    """Workflow that upserts multiple attributes."""

    @workflow.run
    async def run(self, count: int) -> str:
        # Set multiple attributes at once
        workflow.upsert_search_attributes([
            KW_KEY.value_set("processing"),
            INT_KEY.value_set(count),
        ])

        await workflow.sleep(0.1)

        # Update one attribute
        workflow.upsert_search_attributes([
            INT_KEY.value_set(count * 2),
        ])

        return f"done-{count}"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_upsert_search_attributes(trio_client) -> None:
    """Test upsert_search_attributes works end-to-end with real server."""
    task_queue = f"trio-e2e-search-attr-{int(time.time())}"

    async with trio.open_nursery() as nursery:
        worker = Worker(
            trio_client,
            task_queue=task_queue,
            workflows=[SearchAttributeWorkflow],
        )
        nursery.start_soon(worker.run)

        await trio.sleep(0)

        handle = await trio_client.start_workflow(
            "SearchAttributeWorkflow",
            "initial-status",
            id=f"search-attr-wf-{int(time.time())}",
            task_queue=task_queue,
        )

        result = await handle.result()
        assert result == "processed-initial-status"

        worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_multiple_search_attributes(trio_client) -> None:
    """Test upserting multiple search attributes."""
    task_queue = f"trio-e2e-multi-search-attr-{int(time.time())}"

    async with trio.open_nursery() as nursery:
        worker = Worker(
            trio_client,
            task_queue=task_queue,
            workflows=[MultipleSearchAttributesWorkflow],
        )
        nursery.start_soon(worker.run)

        await trio.sleep(0)

        handle = await trio_client.start_workflow(
            "MultipleSearchAttributesWorkflow",
            42,
            id=f"multi-search-attr-wf-{int(time.time())}",
            task_queue=task_queue,
        )

        result = await handle.result()
        assert result == "done-42"

        worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_search_attribute_during_replay(trio_client) -> None:
    """Test that search attributes work correctly during workflow replay."""
    task_queue = f"trio-e2e-replay-search-attr-{int(time.time())}"

    @workflow.defn
    class ReplaySearchAttrWorkflow:
        @workflow.run
        async def run(self) -> str:
            workflow.upsert_search_attributes([KW_KEY.value_set("step1")])
            await workflow.sleep(0.1)

            workflow.upsert_search_attributes([KW_KEY.value_set("step2")])
            await workflow.sleep(0.1)

            workflow.upsert_search_attributes([KW_KEY.value_set("step3")])

            return "completed"

    async with trio.open_nursery() as nursery:
        worker = Worker(
            trio_client,
            task_queue=task_queue,
            workflows=[ReplaySearchAttrWorkflow],
        )
        nursery.start_soon(worker.run)

        await trio.sleep(0)

        handle = await trio_client.start_workflow(
            "ReplaySearchAttrWorkflow",
            id=f"replay-search-attr-wf-{int(time.time())}",
            task_queue=task_queue,
        )

        result = await handle.result()
        assert result == "completed"

        worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()
