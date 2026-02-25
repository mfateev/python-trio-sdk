"""E2E tests for headers propagation with real Temporal server."""

import time

import pytest
import trio

from temporalio_trio import workflow
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker


@pytest.fixture
async def trio_client():
    """Create a Trio-based Temporal client."""
    client = await Client.connect("localhost:7233", namespace="default")
    yield client
    await client.close()


@workflow.defn
class HeadersInfoE2EWorkflow:
    """Workflow that verifies headers are accessible via info()."""

    @workflow.run
    async def run(self) -> str:
        info = workflow.info()
        # Headers should be a mapping (empty by default since no interceptors set them)
        header_count = len(info.headers)
        return f"headers-count:{header_count}"

    @workflow.query
    def get_status(self) -> str:
        return "ok"


@workflow.defn
class HeadersWithActivityE2EWorkflow:
    """Workflow that uses headers alongside an activity."""

    @workflow.run
    async def run(self) -> str:
        info = workflow.info()
        # Verify headers accessible before and after sleep
        pre_count = len(info.headers)
        await workflow.sleep(0.1)
        post_info = workflow.info()
        post_count = len(post_info.headers)
        return f"pre:{pre_count},post:{post_count}"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_headers_accessible_via_info(trio_client) -> None:
    """Test that workflow.info().headers is accessible in a real workflow."""
    task_queue = f"trio-e2e-headers-{int(time.time())}"

    async with trio.open_nursery() as nursery:
        worker = Worker(
            trio_client,
            task_queue=task_queue,
            workflows=[HeadersInfoE2EWorkflow],
        )
        nursery.start_soon(worker.run)

        await trio.sleep(0)

        handle = await trio_client.start_workflow(
            "HeadersInfoE2EWorkflow",
            id=f"headers-wf-{int(time.time())}",
            task_queue=task_queue,
        )

        result = await handle.result()
        # By default (no interceptors), headers should be empty
        assert result == "headers-count:0"

        await worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_headers_dont_break_execution(trio_client) -> None:
    """Test that headers propagation doesn't break normal workflow execution."""
    task_queue = f"trio-e2e-headers-exec-{int(time.time())}"

    async with trio.open_nursery() as nursery:
        worker = Worker(
            trio_client,
            task_queue=task_queue,
            workflows=[HeadersWithActivityE2EWorkflow],
        )
        nursery.start_soon(worker.run)

        await trio.sleep(0)

        handle = await trio_client.start_workflow(
            "HeadersWithActivityE2EWorkflow",
            id=f"headers-exec-wf-{int(time.time())}",
            task_queue=task_queue,
        )

        result = await handle.result()
        assert result == "pre:0,post:0"

        await worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()
