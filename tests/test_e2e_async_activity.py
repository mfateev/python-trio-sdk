"""E2E tests for async activity completion.

Tests that activities can be completed, failed, and heartbeated
externally via AsyncActivityHandle.
"""

import time
import uuid
from datetime import timedelta

import pytest
import trio

from temporalio_trio import activity, workflow
from temporalio_trio.client import (
    AsyncActivityCancelledError,
    AsyncActivityHandle,
    Client,
    WorkflowFailureError,
)
from temporalio_trio.worker import Worker

# --- Workflows & Activities ---


@activity.defn
async def async_complete_activity() -> str:
    """Activity that will be completed externally."""
    activity.raise_complete_async()


@activity.defn
async def async_fail_activity() -> str:
    """Activity that will be failed externally."""
    activity.raise_complete_async()


@workflow.defn
class AsyncActivityWorkflow:
    """Workflow that calls an async activity and returns its result."""

    @workflow.run
    async def run(self, activity_name: str) -> str:
        return await workflow.execute_activity(
            activity_name,
            start_to_close_timeout=timedelta(seconds=30),
        )


# A separate workflow that acts as an event to signal activity started
_activity_started_tokens: dict[str, bytes] = {}


@activity.defn
async def async_activity_with_token(workflow_id: str) -> str:
    """Activity that stores its task token globally then completes async."""
    _activity_started_tokens[workflow_id] = activity.info().task_token
    activity.raise_complete_async()


@workflow.defn
class AsyncTokenActivityWorkflow:
    """Workflow that calls async_activity_with_token."""

    @workflow.run
    async def run(self) -> str:
        wf_id = workflow.info().workflow_id
        return await workflow.execute_activity(
            async_activity_with_token,
            args=[wf_id],
            start_to_close_timeout=timedelta(seconds=30),
        )


# --- Fixtures ---


@pytest.fixture
async def client():
    c = await Client.connect("localhost:7233", namespace="default")
    yield c
    await c.close()


# --- Tests ---


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_manual_completion_by_id(client):
    """Test completing an async activity by workflow_id + activity_id."""
    task_queue = f"async-act-{uuid.uuid4()}"
    workflow_id = f"async-complete-{uuid.uuid4()}"

    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[AsyncActivityWorkflow],
        activities=[async_complete_activity],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)

        # Start workflow that invokes the async activity
        handle = await client.start_workflow(
            AsyncActivityWorkflow,
            "async_complete_activity",
            id=workflow_id,
            task_queue=task_queue,
        )

        # Give the activity time to start and call raise_complete_async
        await trio.sleep(2.0)

        # Complete the activity externally
        # The default activity_id assigned by the server is typically "1"
        activity_handle = client.get_async_activity_handle(
            workflow_id=workflow_id,
            run_id=None,
            activity_id="1",
        )
        await activity_handle.complete("externally-completed")

        # Verify the workflow result
        result = await handle.result(timeout=10.0)
        assert result == "externally-completed"

        await worker.shutdown()
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_manual_completion_by_token(client):
    """Test completing an async activity by task_token."""
    task_queue = f"async-tok-{uuid.uuid4()}"
    workflow_id = f"async-token-{uuid.uuid4()}"

    # Clear any stale token
    _activity_started_tokens.pop(workflow_id, None)

    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[AsyncTokenActivityWorkflow],
        activities=[async_activity_with_token],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)

        handle = await client.start_workflow(
            AsyncTokenActivityWorkflow,
            id=workflow_id,
            task_queue=task_queue,
        )

        # Wait for the activity to start and store its token
        deadline = time.time() + 10.0
        while workflow_id not in _activity_started_tokens:
            if time.time() > deadline:
                raise TimeoutError("Activity did not start in time")
            await trio.sleep(0.2)

        token = _activity_started_tokens[workflow_id]
        activity_handle = client.get_async_activity_handle(task_token=token)
        await activity_handle.complete("token-completed")

        result = await handle.result(timeout=10.0)
        assert result == "token-completed"

        await worker.shutdown()
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_manual_failure(client):
    """Test failing an async activity externally."""
    task_queue = f"async-fail-{uuid.uuid4()}"
    workflow_id = f"async-fail-{uuid.uuid4()}"

    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[AsyncActivityWorkflow],
        activities=[async_fail_activity],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)

        handle = await client.start_workflow(
            AsyncActivityWorkflow,
            "async_fail_activity",
            id=workflow_id,
            task_queue=task_queue,
        )

        await trio.sleep(2.0)

        activity_handle = client.get_async_activity_handle(
            workflow_id=workflow_id,
            run_id=None,
            activity_id="1",
        )
        from temporalio.exceptions import ApplicationError

        await activity_handle.fail(ApplicationError("Test failure", non_retryable=True))

        with pytest.raises(WorkflowFailureError):
            await handle.result(timeout=10.0)

        await worker.shutdown()
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_get_async_activity_handle_validation(client):
    """Test that get_async_activity_handle validates its parameters."""
    with pytest.raises(ValueError, match="Must specify either"):
        client.get_async_activity_handle()

    with pytest.raises(ValueError, match="Cannot specify both"):
        client.get_async_activity_handle(
            task_token=b"token",
            activity_id="1",
        )

    # These should succeed (no validation errors)
    handle1 = client.get_async_activity_handle(task_token=b"token")
    assert isinstance(handle1, AsyncActivityHandle)

    handle2 = client.get_async_activity_handle(workflow_id="wf-1", activity_id="1")
    assert isinstance(handle2, AsyncActivityHandle)
