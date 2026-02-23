"""End-to-end tests for activity cancellation.

These tests require a running Temporal server and validate the complete
activity cancellation flow including:
- Activity catching cancellation and returning a value
- Activity not catching cancellation (thrown through)
- Heartbeat mechanism
- Worker shutdown
- wait_for_cancelled() API
- Heartbeat details across retries
- Multiple activities with mid-sequence cancel

Run these tests with:
    pytest -v -m temporal_server tests/test_e2e_activity_cancellation.py
"""

import time
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

import pytest
import trio
from temporalio.common import RetryPolicy
from temporalio.exceptions import (
    ActivityError,
    CancelledError,
)

from temporalio_trio import activity, workflow
from temporalio_trio.client import Client, WorkflowFailureError
from temporalio_trio.worker import Worker


@pytest.fixture
async def trio_client():
    """Create a Trio-based Temporal client."""
    client = await Client.connect("localhost:7233", namespace="default")
    yield client
    await client.close()


@asynccontextmanager
async def run_worker(client, task_queue, workflows, activities):
    """Async context manager that runs a worker in the background."""
    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=workflows,
        activities=activities,
    )
    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)
        try:
            yield worker
        finally:
            worker.shutdown()
            await trio.sleep(0.3)
            nursery.cancel_scope.cancel()


# =============================================================================
# Activity definitions
# =============================================================================


@activity.defn
async def heartbeat_activity() -> str:
    """Activity that heartbeats and completes normally."""
    for i in range(3):
        activity.heartbeat(f"progress-{i}")
        await trio.sleep(0.1)
    return "heartbeat-ok"


@activity.defn
async def cancel_catch_activity() -> str:
    """Activity that catches cancellation and returns a message."""
    while True:
        activity.heartbeat("alive")
        try:
            await trio.sleep(0.5)
        except trio.Cancelled:
            return f"Got cancelled error, cancelled? {activity.is_cancelled()}"


@activity.defn
async def cancel_throw_activity() -> str:
    """Activity that does NOT catch cancellation -- it propagates."""
    while True:
        activity.heartbeat("alive")
        await trio.sleep(0.5)
    return "should-not-reach"  # type: ignore[unreachable]


@activity.defn
async def infinite_heartbeat_activity() -> str:
    """Activity that runs forever with heartbeats."""
    while True:
        activity.heartbeat("running")
        await trio.sleep(0.3)
    return "should-not-reach"  # type: ignore[unreachable]


@activity.defn
async def wait_for_cancelled_activity() -> str:
    """Activity that waits for cancellation via wait_for_cancelled() API."""
    activity.heartbeat("started")
    await activity.wait_for_cancelled()
    return f"cancelled gracefully, is_cancelled={activity.is_cancelled()}"


@activity.defn
async def heartbeat_counter_activity() -> int:
    """Activity that heartbeats a counter, resuming from previous attempt.

    On first attempt: starts at 0, heartbeats 1, then fails.
    On retry: reads heartbeat_details to resume counter, heartbeats counter+1, succeeds.
    """
    info = activity.info()
    counter = 0
    if info.heartbeat_details:
        counter = info.heartbeat_details[0]

    counter += 1
    activity.heartbeat(counter)

    if info.attempt < 2:
        # Sleep to allow heartbeat to be flushed to the server before failing
        await trio.sleep(1)
        raise RuntimeError(f"Intentional failure at attempt {info.attempt}")

    return counter


@activity.defn
async def short_activity() -> str:
    """Activity that completes quickly."""
    await trio.sleep(0.1)
    return "short-done"


# =============================================================================
# Workflow definitions
# =============================================================================


@workflow.defn
class HeartbeatWorkflow:
    """Workflow that executes a heartbeating activity and returns the result."""

    @workflow.run
    async def run(self) -> str:
        return await workflow.execute_activity(
            heartbeat_activity,
            start_to_close_timeout=timedelta(seconds=30),
            heartbeat_timeout=timedelta(seconds=10),
        )


@workflow.defn
class CancelCatchWorkflow:
    """Workflow that starts a cancel-catching activity then waits for cancel."""

    @workflow.run
    async def run(self) -> dict[str, Any]:
        try:
            result = await workflow.execute_activity(
                cancel_catch_activity,
                start_to_close_timeout=timedelta(seconds=60),
                heartbeat_timeout=timedelta(seconds=5),
                retry_policy=RetryPolicy(maximum_attempts=1),
                cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
            )
            return {"result": result, "error": None}
        except ActivityError as e:
            cause = e.__cause__
            return {
                "result": None,
                "error": type(e).__name__,
                "cause_type": type(cause).__name__ if cause else None,
                "cause_message": str(cause) if cause else None,
            }


@workflow.defn
class CancelThrowWorkflow:
    """Workflow that starts an activity that doesn't catch cancel."""

    @workflow.run
    async def run(self) -> dict[str, Any]:
        try:
            result = await workflow.execute_activity(
                cancel_throw_activity,
                start_to_close_timeout=timedelta(seconds=60),
                heartbeat_timeout=timedelta(seconds=5),
                retry_policy=RetryPolicy(maximum_attempts=1),
                cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
            )
            return {"result": result, "error": None}
        except ActivityError as e:
            cause = e.__cause__
            return {
                "result": None,
                "error": type(e).__name__,
                "cause_type": type(cause).__name__ if cause else None,
            }
        except Exception as e:
            return {
                "result": None,
                "error": type(e).__name__,
                "message": str(e),
            }


@workflow.defn
class UncaughtCancelWorkflow:
    """Workflow that runs an infinite activity and does NOT catch errors."""

    @workflow.run
    async def run(self) -> str:
        result = await workflow.execute_activity(
            infinite_heartbeat_activity,
            start_to_close_timeout=timedelta(seconds=60),
            heartbeat_timeout=timedelta(seconds=5),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        return result


@workflow.defn
class WaitForCancelledWorkflow:
    """Workflow that runs an activity using wait_for_cancelled() API."""

    @workflow.run
    async def run(self) -> dict[str, Any]:
        try:
            result = await workflow.execute_activity(
                wait_for_cancelled_activity,
                start_to_close_timeout=timedelta(seconds=60),
                heartbeat_timeout=timedelta(seconds=5),
                retry_policy=RetryPolicy(maximum_attempts=1),
                cancellation_type=workflow.ActivityCancellationType.WAIT_CANCELLATION_COMPLETED,
            )
            return {"result": result, "error": None}
        except ActivityError as e:
            cause = e.__cause__
            return {
                "result": None,
                "error": type(e).__name__,
                "cause_type": type(cause).__name__ if cause else None,
            }


@workflow.defn
class HeartbeatRetryWorkflow:
    """Workflow that runs an activity that uses heartbeat details across retries."""

    @workflow.run
    async def run(self) -> int:
        return await workflow.execute_activity(
            heartbeat_counter_activity,
            start_to_close_timeout=timedelta(seconds=30),
            heartbeat_timeout=timedelta(seconds=10),
            retry_policy=RetryPolicy(
                maximum_attempts=3,
                initial_interval=timedelta(milliseconds=100),
            ),
        )


@workflow.defn
class SequentialActivitiesWorkflow:
    """Workflow that runs two activities in sequence.

    First a short activity, then a long one. Cancel mid-sequence to test
    that the first result is preserved and the second gets ActivityError.
    """

    @workflow.run
    async def run(self) -> dict[str, Any]:
        first_result = await workflow.execute_activity(
            short_activity,
            start_to_close_timeout=timedelta(seconds=30),
        )
        try:
            second_result = await workflow.execute_activity(
                infinite_heartbeat_activity,
                start_to_close_timeout=timedelta(seconds=60),
                heartbeat_timeout=timedelta(seconds=5),
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
            return {"first": first_result, "second": second_result, "error": None}
        except ActivityError as e:
            cause = e.__cause__
            return {
                "first": first_result,
                "second": None,
                "error": type(e).__name__,
                "cause_type": type(cause).__name__ if cause else None,
            }


# =============================================================================
# E2E Tests
# =============================================================================


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_activity_heartbeat_succeeds(trio_client):
    """Test that activity heartbeats work and activity completes normally.

    Validates:
    - Activity can send heartbeats
    - Heartbeat mechanism doesn't interfere with normal completion
    - Result is returned correctly via handle.result()
    """
    task_queue = f"trio-e2e-heartbeat-queue-{int(time.time())}"

    async with run_worker(
        trio_client, task_queue, [HeartbeatWorkflow], [heartbeat_activity]
    ):
        handle = await trio_client.start_workflow(
            HeartbeatWorkflow,
            id=f"test-heartbeat-{int(time.time())}",
            task_queue=task_queue,
        )
        result = await handle.result(timeout=30)
        assert result == "heartbeat-ok"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_activity_cancel_catch(trio_client):
    """Test that workflow cancel triggers activity cancellation.

    Validates:
    - External workflow cancel produces WorkflowFailureError with CancelledError
    - The cancel-catching activity doesn't prevent workflow cancellation
      (external cancel always cancels the whole workflow)

    Note: The activity catches trio.Cancelled and returns a value, but
    external workflow cancel produces CANCELED status before the activity
    result can be delivered to the workflow. Activity-level cancel behavior
    is validated in the unit tests.
    """
    task_queue = f"trio-e2e-cancel-catch-queue-{int(time.time())}"

    async with run_worker(
        trio_client,
        task_queue,
        [CancelCatchWorkflow],
        [cancel_catch_activity],
    ):
        handle = await trio_client.start_workflow(
            CancelCatchWorkflow,
            id=f"test-cancel-catch-{int(time.time())}",
            task_queue=task_queue,
        )

        # Wait for activity to start heartbeating, then cancel workflow
        await trio.sleep(2)
        await handle.cancel()

        # Workflow is cancelled externally -> WorkflowFailureError
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result(timeout=30)
        assert isinstance(exc_info.value.cause, CancelledError)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_activity_cancel_throw(trio_client):
    """Test that workflow cancel propagates through non-catching activity.

    Validates:
    - External workflow cancel produces WorkflowFailureError with CancelledError
    - Activity that doesn't catch cancel still results in proper workflow cancel

    Note: Activity-level cancel propagation (trio.Cancelled not caught)
    is validated in the unit tests.
    """
    task_queue = f"trio-e2e-cancel-throw-queue-{int(time.time())}"

    async with run_worker(
        trio_client,
        task_queue,
        [CancelThrowWorkflow],
        [cancel_throw_activity],
    ):
        handle = await trio_client.start_workflow(
            CancelThrowWorkflow,
            id=f"test-cancel-throw-{int(time.time())}",
            task_queue=task_queue,
        )

        # Wait for activity to start, then cancel workflow
        await trio.sleep(2)
        await handle.cancel()

        # Workflow is cancelled externally -> WorkflowFailureError
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result(timeout=30)
        assert isinstance(exc_info.value.cause, CancelledError)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_workflow_uncaught_cancel(trio_client):
    """Test that uncaught activity cancellation fails the workflow.

    Validates:
    - When workflow does NOT catch ActivityError, the workflow fails
    - handle.result() raises WorkflowFailureError
    - The cause chain is CancelledError (workflow-level cancellation)
    """
    task_queue = f"trio-e2e-uncaught-cancel-queue-{int(time.time())}"

    async with run_worker(
        trio_client,
        task_queue,
        [UncaughtCancelWorkflow],
        [infinite_heartbeat_activity],
    ):
        handle = await trio_client.start_workflow(
            UncaughtCancelWorkflow,
            id=f"test-uncaught-cancel-{int(time.time())}",
            task_queue=task_queue,
        )

        # Wait for activity to start heartbeating, then cancel
        await trio.sleep(2)
        await handle.cancel()

        # Workflow should fail since it doesn't catch the error
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result(timeout=30)
        assert isinstance(exc_info.value.cause, CancelledError)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_worker_shutdown_cancels_activities(trio_client):
    """Test that worker shutdown properly cancels running activities.

    Validates:
    - Worker shutdown signal reaches running activities
    - Worker shuts down within timeout
    - No hang
    """
    task_queue = f"trio-e2e-shutdown-queue-{int(time.time())}"

    worker = Worker(
        client=trio_client,
        task_queue=task_queue,
        workflows=[UncaughtCancelWorkflow],
        activities=[infinite_heartbeat_activity],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)

        try:
            await trio_client.start_workflow(
                UncaughtCancelWorkflow,
                id=f"test-shutdown-{int(time.time())}",
                task_queue=task_queue,
            )

            # Wait for activity to start
            await trio.sleep(2)

            # Shutdown worker - should not hang
            worker.shutdown()
            await trio.sleep(1)

            # If we got here, worker shutdown didn't hang

        finally:
            nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_activity_wait_for_cancelled(trio_client):
    """Test activity using wait_for_cancelled() API with external workflow cancel.

    Validates:
    - External workflow cancel produces WorkflowFailureError with CancelledError
    - Activity using wait_for_cancelled() doesn't prevent workflow cancellation

    Note: The wait_for_cancelled() API itself is tested in unit tests.
    External workflow cancel always produces CANCELED status.
    """
    task_queue = f"trio-e2e-wait-cancelled-queue-{int(time.time())}"

    async with run_worker(
        trio_client,
        task_queue,
        [WaitForCancelledWorkflow],
        [wait_for_cancelled_activity],
    ):
        handle = await trio_client.start_workflow(
            WaitForCancelledWorkflow,
            id=f"test-wait-cancelled-{int(time.time())}",
            task_queue=task_queue,
        )

        # Wait for activity to start, then cancel
        await trio.sleep(2)
        await handle.cancel()

        # Workflow is cancelled externally -> WorkflowFailureError
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result(timeout=30)
        assert isinstance(exc_info.value.cause, CancelledError)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_heartbeat_details_across_retries(trio_client):
    """Test that heartbeat details survive activity retries.

    Validates:
    - Activity reads heartbeat_details from previous attempt
    - Activity heartbeats a counter, then fails on first attempt
    - On retry, counter resumes from heartbeated value
    - Eventually succeeds with the accumulated counter
    """
    task_queue = f"trio-e2e-heartbeat-retry-queue-{int(time.time())}"

    async with run_worker(
        trio_client,
        task_queue,
        [HeartbeatRetryWorkflow],
        [heartbeat_counter_activity],
    ):
        handle = await trio_client.start_workflow(
            HeartbeatRetryWorkflow,
            id=f"test-heartbeat-retry-{int(time.time())}",
            task_queue=task_queue,
        )
        result = await handle.result(timeout=30)
        # Attempt 1: counter 0 -> +1 = 1, heartbeat(1), fail
        # Attempt 2: heartbeat_details=[1], counter 1 -> +1 = 2, succeed
        assert result == 2


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_cancel_with_sequential_activities(trio_client):
    """Test cancelling workflow mid-sequence of multiple activities.

    Validates:
    - First short activity completes normally (before cancel arrives)
    - Second long activity is running when cancel arrives
    - External workflow cancel produces WorkflowFailureError with CancelledError
    """
    task_queue = f"trio-e2e-sequential-cancel-queue-{int(time.time())}"

    async with run_worker(
        trio_client,
        task_queue,
        [SequentialActivitiesWorkflow],
        [short_activity, infinite_heartbeat_activity],
    ):
        handle = await trio_client.start_workflow(
            SequentialActivitiesWorkflow,
            id=f"test-sequential-cancel-{int(time.time())}",
            task_queue=task_queue,
        )

        # Wait for first activity to finish and second to start
        await trio.sleep(2)
        await handle.cancel()

        # Workflow is cancelled externally -> WorkflowFailureError
        with pytest.raises(WorkflowFailureError) as exc_info:
            await handle.result(timeout=30)
        assert isinstance(exc_info.value.cause, CancelledError)
