"""Integration tests for Trio client with real Temporal server.

These tests validate the client implementation against a real Temporal server,
testing all client operations end-to-end.

Run these tests with:
    pytest -v -m temporal_server tests/test_e2e_client.py

Prerequisites:
    - Temporal server running on localhost:7233
    - Worker running to process workflows

Note:
    Some tests for cancel/terminate are skipped as those features require
    additional worker-side implementation (cancel_workflow job type).
"""

import time
import uuid

import pytest
import trio
from temporalio.exceptions import CancelledError, TerminatedError

from temporalio_trio import workflow
from temporalio_trio.client import Client, WorkflowFailureError
from temporalio_trio.worker import Worker


@workflow.defn
class GreetingWorkflow:
    """Simple workflow that returns a greeting."""

    @workflow.run
    async def run(self, name: str) -> str:
        """Return greeting for the given name."""
        return f"Hello, {name}!"


@workflow.defn
class LongRunningWorkflow:
    """Workflow that runs for a long time (for testing cancel/terminate)."""

    @workflow.run
    async def run(self, duration: float = 10.0) -> str:
        """Run for specified duration."""
        await workflow.sleep(duration)
        return f"Completed after {duration} seconds"


@workflow.defn
class SignalWithStartWorkflow:
    """Workflow that waits for a signal then returns its value."""

    def __init__(self) -> None:
        self._signal_value: str | None = None

    @workflow.signal
    def my_signal(self, value: str) -> None:
        self._signal_value = value

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._signal_value is not None)
        return f"signal: {self._signal_value}"


@pytest.fixture
async def client():
    """Create a Trio client for testing."""
    client = await Client.connect("localhost:7233", namespace="default")
    yield client
    await client.close()


@pytest.fixture
async def worker_with_workflows(client):
    """Start a worker with test workflows.

    Returns the task queue name that the worker is listening on.
    """
    task_queue = f"trio-client-test-{int(time.time())}"

    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[GreetingWorkflow, LongRunningWorkflow, SignalWithStartWorkflow],
    )

    async with trio.open_nursery() as nursery:
        # Start worker
        nursery.start_soon(worker.run)

        # Task queue is durable - no need to wait for worker startup
        await trio.sleep(0)

        yield task_queue

        # Shutdown worker gracefully
        await worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_client_connect():
    """Test connecting to Temporal server."""
    client = await Client.connect("localhost:7233", namespace="default")

    assert client is not None
    assert client.namespace == "default"

    await client.close()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_start_workflow(client, worker_with_workflows):
    """Test starting a workflow."""
    task_queue = worker_with_workflows
    workflow_id = f"greeting-{int(time.time())}"

    # Start workflow
    handle = await client.start_workflow(
        GreetingWorkflow,
        "World",
        id=workflow_id,
        task_queue=task_queue,
    )

    assert handle is not None
    assert handle.workflow_id == workflow_id
    # run_id is intentionally None on handles from start_workflow (tracks latest run)
    assert handle.run_id is None

    # Wait for result
    result = await handle.result()
    assert result == "Hello, World!"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_execute_workflow(client, worker_with_workflows):
    """Test executing a workflow (start + wait)."""
    task_queue = worker_with_workflows
    workflow_id = f"greeting-execute-{int(time.time())}"

    # Execute workflow
    result = await client.execute_workflow(
        GreetingWorkflow,
        "Alice",
        id=workflow_id,
        task_queue=task_queue,
    )

    assert result == "Hello, Alice!"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_get_workflow_handle(client, worker_with_workflows):
    """Test getting handle to existing workflow."""
    task_queue = worker_with_workflows
    workflow_id = f"greeting-handle-{int(time.time())}"

    # Start workflow
    handle1 = await client.start_workflow(
        GreetingWorkflow,
        "Bob",
        id=workflow_id,
        task_queue=task_queue,
    )

    # Get handle to same workflow
    handle2 = client.get_workflow_handle(workflow_id)

    assert handle2.workflow_id == workflow_id

    # Both handles should get same result
    result = await handle2.result()
    assert result == "Hello, Bob!"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_workflow_cancel(client, worker_with_workflows):
    """Test canceling a workflow."""
    task_queue = worker_with_workflows
    workflow_id = f"long-running-cancel-{int(time.time())}"

    # Start long-running workflow (runs for 10 seconds)
    handle = await client.start_workflow(
        LongRunningWorkflow,
        10.0,  # duration
        id=workflow_id,
        task_queue=task_queue,
    )

    # Give workflow time to start
    await trio.sleep(0.5)

    # Cancel workflow
    await handle.cancel()

    # Wait a bit for cancellation to process
    await trio.sleep(0.5)

    # Try to get result - should raise WorkflowFailureError with CancelledError cause
    with pytest.raises(WorkflowFailureError) as exc_info:
        await handle.result()
    assert isinstance(exc_info.value.cause, CancelledError)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_workflow_terminate(client, worker_with_workflows):
    """Test terminating a workflow."""
    task_queue = worker_with_workflows
    workflow_id = f"long-running-terminate-{int(time.time())}"

    # Start long-running workflow
    handle = await client.start_workflow(
        LongRunningWorkflow,
        10.0,  # duration
        id=workflow_id,
        task_queue=task_queue,
    )

    # Give workflow time to start
    await trio.sleep(0.5)

    # Terminate workflow
    await handle.terminate(reason="Test termination")

    # Wait a bit for termination to process
    await trio.sleep(0.5)

    # Try to get result - should raise WorkflowFailureError with TerminatedError cause
    with pytest.raises(WorkflowFailureError) as exc_info:
        await handle.result()
    assert isinstance(exc_info.value.cause, TerminatedError)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_multiple_workflows_parallel(client, worker_with_workflows):
    """Test executing multiple workflows in parallel."""
    task_queue = worker_with_workflows
    names = ["Alice", "Bob", "Charlie"]  # Reduced to 3 for faster test
    base_time = int(time.time())

    # Start all workflows sequentially first to ensure unique IDs
    handles = []
    for i, name in enumerate(names):
        workflow_id = f"greeting-parallel-{name}-{base_time}-{i}"
        handle = await client.start_workflow(
            GreetingWorkflow,
            name,
            id=workflow_id,
            task_queue=task_queue,
        )
        handles.append((name, handle))

    # Wait for all results in parallel
    async with trio.open_nursery() as nursery:
        results = []

        async def get_result(name, handle):
            result = await handle.result(timeout=30.0)
            results.append((name, result))

        for name, handle in handles:
            nursery.start_soon(get_result, name, handle)

    # Verify all results
    assert len(results) == len(names)
    for name, result in results:
        assert result == f"Hello, {name}!"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_workflow_with_timeout(client, worker_with_workflows):
    """Test workflow execution timeout."""
    task_queue = worker_with_workflows
    workflow_id = f"greeting-timeout-{int(time.time())}"

    # Execute workflow with short timeout
    result = await client.execute_workflow(
        GreetingWorkflow,
        "Timeout Test",
        id=workflow_id,
        task_queue=task_queue,
        execution_timeout=30.0,  # 30 second timeout
    )

    assert result == "Hello, Timeout Test!"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_signal_with_start(client, worker_with_workflows):
    """Test signal-with-start atomically starts a workflow and sends a signal."""
    task_queue = worker_with_workflows
    workflow_id = f"signal-with-start-{uuid.uuid4()}"

    handle = await client.start_workflow(
        SignalWithStartWorkflow,
        id=workflow_id,
        task_queue=task_queue,
        start_signal="my_signal",
        start_signal_args=["hello-sws"],
    )

    assert handle.workflow_id == workflow_id
    result = await handle.result(timeout=15.0)
    assert result == "signal: hello-sws"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_workflow_handle_properties(client, worker_with_workflows):
    """Test WorkflowHandle property accessors."""
    task_queue = worker_with_workflows
    workflow_id = f"handle-props-{uuid.uuid4()}"

    handle = await client.start_workflow(
        GreetingWorkflow,
        "Test",
        id=workflow_id,
        task_queue=task_queue,
    )

    # Verify properties
    assert handle.workflow_id == workflow_id
    assert handle.run_id is None  # tracks latest run
    assert handle.result_run_id is not None  # set from start response
    assert handle.first_execution_run_id is not None  # set from start response
    assert handle.first_execution_run_id == handle.result_run_id

    # Wait for completion
    result = await handle.result()
    assert result == "Hello, Test!"

    # get_workflow_handle_for returns same kind of handle
    handle2 = client.get_workflow_handle_for(GreetingWorkflow, workflow_id)
    assert handle2.workflow_id == workflow_id


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_fetch_history_events_with_params(client, worker_with_workflows):
    """Test fetch_history_events with event_filter_type and skip_archival."""
    task_queue = worker_with_workflows
    workflow_id = f"history-params-{uuid.uuid4()}"

    result = await client.execute_workflow(
        GreetingWorkflow,
        "HistoryTest",
        id=workflow_id,
        task_queue=task_queue,
    )
    assert result == "Hello, HistoryTest!"

    handle = client.get_workflow_handle(workflow_id)

    # Fetch all events (default)
    all_events = await handle.fetch_history_events()
    assert len(all_events) > 0

    # Fetch with skip_archival=True
    events_no_archive = await handle.fetch_history_events(skip_archival=True)
    assert len(events_no_archive) > 0
