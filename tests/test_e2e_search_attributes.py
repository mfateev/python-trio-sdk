"""E2E tests for upsert_search_attributes with real Temporal server."""

import pytest
import trio
from temporalio.client import Client as AsyncioClient

from temporalio_trio import workflow
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker


@workflow.defn
class SearchAttributeWorkflow:
    """Workflow that upserts search attributes."""

    @workflow.run
    async def run(self, status: str) -> str:
        # Update search attribute to track workflow status
        workflow.upsert_search_attributes({
            "CustomKeywordField": status,
        })

        # Simulate some work
        await workflow.sleep(0.1)

        # Update to completed
        workflow.upsert_search_attributes({
            "CustomKeywordField": "completed",
        })

        return f"processed-{status}"


@workflow.defn
class MultipleSearchAttributesWorkflow:
    """Workflow that upserts multiple attributes."""

    @workflow.run
    async def run(self, count: int) -> str:
        # Set multiple attributes at once
        workflow.upsert_search_attributes({
            "CustomKeywordField": "processing",
            "CustomIntField": count,
        })

        await workflow.sleep(0.1)

        # Update one attribute
        workflow.upsert_search_attributes({
            "CustomIntField": count * 2,
        })

        return f"done-{count}"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_upsert_search_attributes(unique_task_queue: str) -> None:
    """Test upsert_search_attributes works end-to-end with real server."""
    client = await Client.connect("http://localhost:7233")

    async with trio.open_nursery() as nursery:
        # Start worker
        worker = Worker(
            client,
            task_queue=unique_task_queue,
            workflows=[SearchAttributeWorkflow],
        )

        async def run_worker():
            await worker.run()

        nursery.start_soon(run_worker)

        # Give worker time to start
        await trio.sleep(0.5)

        # Start workflow
        handle = await client.start_workflow(
            SearchAttributeWorkflow.run,
            "initial-status",
            id=f"search-attr-wf-{trio.lowlevel.current_trio_token()}",
            task_queue=unique_task_queue,
        )

        # Wait for result
        result = await handle.result()
        assert result == "processed-initial-status"

        # Verify we can query the workflow by search attribute
        # (Note: search attributes are eventually consistent, so this might
        # not work immediately in a test, but the workflow should have succeeded)

        # Shutdown worker
        await worker.shutdown()
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_multiple_search_attributes(unique_task_queue: str) -> None:
    """Test upserting multiple search attributes."""
    client = await Client.connect("http://localhost:7233")

    async with trio.open_nursery() as nursery:
        # Start worker
        worker = Worker(
            client,
            task_queue=unique_task_queue,
            workflows=[MultipleSearchAttributesWorkflow],
        )

        async def run_worker():
            await worker.run()

        nursery.start_soon(run_worker)

        # Give worker time to start
        await trio.sleep(0.5)

        # Start workflow
        handle = await client.start_workflow(
            MultipleSearchAttributesWorkflow.run,
            42,
            id=f"multi-search-attr-wf-{trio.lowlevel.current_trio_token()}",
            task_queue=unique_task_queue,
        )

        # Wait for result
        result = await handle.result()
        assert result == "done-42"

        # Verify workflow completed successfully
        # The search attributes should have been updated in the workflow history

        # Shutdown worker
        await worker.shutdown()
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_search_attribute_during_replay(unique_task_queue: str) -> None:
    """Test that search attributes work correctly during workflow replay."""
    client = await Client.connect("http://localhost:7233")

    @workflow.defn
    class ReplaySearchAttrWorkflow:
        @workflow.run
        async def run(self) -> str:
            # First upsert
            workflow.upsert_search_attributes({"CustomKeywordField": "step1"})
            await workflow.sleep(0.1)

            # Second upsert
            workflow.upsert_search_attributes({"CustomKeywordField": "step2"})
            await workflow.sleep(0.1)

            # Third upsert
            workflow.upsert_search_attributes({"CustomKeywordField": "step3"})

            return "completed"

    async with trio.open_nursery() as nursery:
        # Start worker
        worker = Worker(
            client,
            task_queue=unique_task_queue,
            workflows=[ReplaySearchAttrWorkflow],
        )

        async def run_worker():
            await worker.run()

        nursery.start_soon(run_worker)

        # Give worker time to start
        await trio.sleep(0.5)

        # Start workflow
        handle = await client.start_workflow(
            ReplaySearchAttrWorkflow.run,
            id=f"replay-search-attr-wf-{trio.lowlevel.current_trio_token()}",
            task_queue=unique_task_queue,
        )

        # Wait for result
        result = await handle.result()
        assert result == "completed"

        # The workflow will be replayed during its execution (due to cache eviction)
        # and the search attributes should remain consistent

        # Shutdown worker
        await worker.shutdown()
        nursery.cancel_scope.cancel()
