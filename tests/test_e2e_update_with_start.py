"""E2E tests for update-with-start operations."""

import uuid

import pytest
import temporalio.common
import trio

from temporalio_trio import workflow
from temporalio_trio.client import (
    Client,
    WithStartWorkflowOperation,
    WorkflowUpdateHandle,
    WorkflowUpdateStage,
)
from temporalio_trio.worker import Worker


@workflow.defn
class UpdateWithStartWorkflow:
    """Workflow that supports updates for testing update-with-start."""

    def __init__(self) -> None:
        self._value: str = "initial"
        self._finish = False

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._finish)
        return self._value

    @workflow.update
    async def set_value(self, value: str) -> str:
        old = self._value
        self._value = value
        return f"old={old},new={value}"

    @workflow.update
    async def finish(self) -> str:
        self._finish = True
        return "finishing"


@pytest.fixture
async def client():
    c = await Client.connect("localhost:7233", namespace="default")
    yield c
    await c.close()


@pytest.fixture
async def worker(client):
    task_queue = f"uws-test-{uuid.uuid4()}"
    w = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[UpdateWithStartWorkflow],
    )
    async with trio.open_nursery() as nursery:
        nursery.start_soon(w.run)
        yield task_queue
        await w.shutdown()
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_execute_update_with_start(client, worker):
    """Test execute_update_with_start_workflow."""
    task_queue = worker
    workflow_id = f"uws-exec-{uuid.uuid4()}"

    start_op = WithStartWorkflowOperation(
        "UpdateWithStartWorkflow",
        id=workflow_id,
        task_queue=task_queue,
        id_conflict_policy=temporalio.common.WorkflowIDConflictPolicy.FAIL,
    )

    result = await client.execute_update_with_start_workflow(
        "set_value",
        "hello",
        start_workflow_operation=start_op,
    )

    assert result == "old=initial,new=hello"

    # Finish the workflow
    handle = client.get_workflow_handle(workflow_id)
    await handle.execute_update("finish")
    wf_result = await handle.result(timeout=10.0)
    assert wf_result == "hello"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_start_update_with_start(client, worker):
    """Test start_update_with_start_workflow."""
    task_queue = worker
    workflow_id = f"uws-start-{uuid.uuid4()}"

    start_op = WithStartWorkflowOperation(
        "UpdateWithStartWorkflow",
        id=workflow_id,
        task_queue=task_queue,
        id_conflict_policy=temporalio.common.WorkflowIDConflictPolicy.FAIL,
    )

    update_handle = await client.start_update_with_start_workflow(
        "set_value",
        "world",
        start_workflow_operation=start_op,
        wait_for_stage=WorkflowUpdateStage.COMPLETED,
    )

    assert isinstance(update_handle, WorkflowUpdateHandle)
    assert update_handle.result_value == "old=initial,new=world"

    # Finish the workflow
    handle = client.get_workflow_handle(workflow_id)
    await handle.execute_update("finish")
    wf_result = await handle.result(timeout=10.0)
    assert wf_result == "world"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_with_start_cannot_be_reused(client, worker):
    """Test that WithStartWorkflowOperation cannot be reused."""
    task_queue = worker
    workflow_id = f"uws-reuse-{uuid.uuid4()}"

    start_op = WithStartWorkflowOperation(
        "UpdateWithStartWorkflow",
        id=workflow_id,
        task_queue=task_queue,
        id_conflict_policy=temporalio.common.WorkflowIDConflictPolicy.FAIL,
    )

    await client.execute_update_with_start_workflow(
        "set_value",
        "first",
        start_workflow_operation=start_op,
    )

    # Second use should fail
    with pytest.raises(RuntimeError, match="cannot be reused"):
        await client.execute_update_with_start_workflow(
            "set_value",
            "second",
            start_workflow_operation=start_op,
        )

    # Cleanup
    handle = client.get_workflow_handle(workflow_id)
    await handle.execute_update("finish")
    await handle.result(timeout=10.0)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_with_start_requires_conflict_policy():
    """Test that WithStartWorkflowOperation requires id_conflict_policy."""
    with pytest.raises(ValueError, match="id_conflict_policy"):
        WithStartWorkflowOperation(
            "SomeWorkflow",
            id="test-id",
            task_queue="test-queue",
        )
