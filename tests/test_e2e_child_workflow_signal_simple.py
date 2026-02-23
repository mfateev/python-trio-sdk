"""Simplified E2E test for child workflow signaling.

This is a minimal test to validate child workflow signaling works.
"""

import json
import subprocess
import time
from uuid import uuid4

import pytest
import trio

from temporalio_trio import workflow
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker

TEMPORAL_CLI_PATH = "/home/dev/.temporalio/bin/temporal"


@pytest.fixture
async def trio_client():
    """Create a Trio-based Temporal client."""
    client = await Client.connect("localhost:7233", namespace="default")
    yield client
    await client.close()


@workflow.defn
class SimpleChildWorkflow:
    """Child workflow that receives a signal."""

    def __init__(self) -> None:
        self.value: str | None = None

    @workflow.run
    async def run(self) -> str:
        """Wait for signal and return value."""
        await workflow.wait_condition(lambda: self.value is not None)
        return self.value  # type: ignore

    @workflow.signal
    def set_value(self, val: str) -> None:
        """Set the value."""
        self.value = val


@workflow.defn
class SimpleParentWorkflow:
    """Parent workflow that signals child."""

    @workflow.run
    async def run(self, child_id: str) -> dict:
        """Start child and signal it."""
        # Start child
        child_handle = await workflow.start_child_workflow(
            SimpleChildWorkflow.run,
            id=child_id,
        )

        # Signal child
        await child_handle.signal("set_value", "test123")

        # Wait for child result
        child_result = await child_handle

        return {
            "success": True,
            "child_result": child_result,
        }


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_simple_child_workflow_signal(trio_client):
    """Minimal test for child workflow signaling."""
    task_queue = f"test-simple-{uuid4()}"
    workflow_id = f"parent-{uuid4()}"
    child_id = f"child-{uuid4()}"

    # Start parent workflow
    start_workflow_via_cli(
        workflow_id=workflow_id,
        workflow_type="SimpleParentWorkflow",
        task_queue=task_queue,
        input_data=json.dumps(child_id),
    )

    # Run worker
    async def run_worker():
        async with Worker(
            trio_client,
            task_queue=task_queue,
            workflows=[SimpleParentWorkflow, SimpleChildWorkflow],
        ):
            await trio.sleep(20)

    with trio.move_on_after(30):
        await run_worker()

    # Verify result
    parent_status = get_workflow_status_and_result_via_cli(workflow_id)
    assert parent_status["status"] == "COMPLETED", f"Status: {parent_status['status']}"

    result = parent_status["result"]
    if isinstance(result, str):
        result = json.loads(result)

    assert result["success"] is True
    assert result["child_result"] == "test123"


def start_workflow_via_cli(
    workflow_id: str,
    workflow_type: str,
    task_queue: str,
    input_data: str | None = None,
) -> None:
    """Start a workflow using the Temporal CLI."""
    cmd = [
        TEMPORAL_CLI_PATH,
        "workflow",
        "start",
        "--workflow-id",
        workflow_id,
        "--type",
        workflow_type,
        "--task-queue",
        task_queue,
    ]
    if input_data:
        cmd.extend(["--input", input_data])
    subprocess.run(cmd, capture_output=True, check=True)


def get_workflow_status_and_result_via_cli(workflow_id: str, timeout: int = 30) -> dict:
    """Get workflow status and result using the Temporal CLI."""
    cmd = [
        TEMPORAL_CLI_PATH,
        "workflow",
        "describe",
        "--workflow-id",
        workflow_id,
        "--output",
        "json",
    ]

    start_time = time.time()
    while time.time() - start_time < timeout:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            data = json.loads(result.stdout)
            workflow_info = data.get("workflowExecutionInfo", {})
            status_str = workflow_info.get("status", "UNKNOWN")

            if status_str.startswith("WORKFLOW_EXECUTION_STATUS_"):
                status_str = status_str.replace("WORKFLOW_EXECUTION_STATUS_", "")

            if status_str == "COMPLETED":
                return {
                    "status": status_str,
                    "result": data.get("result"),
                }
            elif status_str in ["FAILED", "TERMINATED", "CANCELLED"]:
                return {
                    "status": status_str,
                    "result": None,
                }
        time.sleep(0.5)
    raise TimeoutError(f"Workflow {workflow_id} did not complete within {timeout}s")
