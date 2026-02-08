"""End-to-end integration tests for signal_external_workflow.

These tests require a running Temporal server and validate the complete
signal external workflow execution path through the worker.

Run these tests with:
    pytest -v -m temporal_server tests/test_e2e_signal_external_workflow.py

Or skip them with:
    pytest -v -m "not temporal_server"
"""

from __future__ import annotations

import json
import subprocess
import time
from uuid import uuid4

import pytest
import trio

from temporalio_trio import workflow
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker

TEMPORAL_CLI_PATH = "/home/sprite/workarea/bin/temporal"


@pytest.fixture
async def trio_client():
    """Create a Trio-based Temporal client."""
    client = await Client.connect("localhost:7233", namespace="default")
    yield client
    await client.close()


# =============================================================================
# Test Workflows
# =============================================================================


@workflow.defn
class TargetWorkflow:
    """A workflow that waits for a signal from an external workflow."""

    def __init__(self) -> None:
        self._signal_received = False
        self._signal_data: str | None = None

    @workflow.run
    async def run(self) -> str:
        """Wait for signal and return the data."""
        # Wait for the signal
        await workflow.wait_condition(lambda: self._signal_received)
        return f"Received: {self._signal_data}"

    @workflow.signal
    def receive_signal(self, data: str) -> None:
        """Handle the signal from external workflow."""
        self._signal_data = data
        self._signal_received = True


@workflow.defn
class SignalerWorkflow:
    """A workflow that signals an external workflow."""

    @workflow.run
    async def run(self, target_workflow_id: str) -> str:
        """Signal the target workflow and return confirmation."""
        # Get handle to external workflow and send signal
        handle = workflow.get_external_workflow_handle(target_workflow_id)
        await handle.signal("receive_signal", "hello from signaler")
        return "signal_sent"


@workflow.defn
class SignalerWithRunIdWorkflow:
    """A workflow that signals an external workflow with a specific run ID."""

    @workflow.run
    async def run(self, target_workflow_id: str, target_run_id: str) -> str:
        """Signal a specific run of the target workflow."""
        handle = workflow.get_external_workflow_handle(
            target_workflow_id, run_id=target_run_id
        )
        await handle.signal("receive_signal", "hello with run_id")
        return "signal_sent_with_run_id"


@workflow.defn
class SignalerWithMethodRefWorkflow:
    """A workflow that signals using method reference."""

    @workflow.run
    async def run(self, target_workflow_id: str) -> str:
        """Signal using method reference."""
        handle = workflow.get_external_workflow_handle(target_workflow_id)
        await handle.signal(TargetWorkflow.receive_signal, "hello via method ref")
        return "signal_sent_via_method_ref"


# =============================================================================
# Helper Functions
# =============================================================================


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
    """Get workflow status and result using the Temporal CLI.

    Returns a dict with 'status' and 'result' keys.
    """
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


def get_workflow_run_id_via_cli(workflow_id: str) -> str:
    """Get the run ID of a workflow using the CLI."""
    cmd = [
        TEMPORAL_CLI_PATH,
        "workflow",
        "describe",
        "--workflow-id",
        workflow_id,
        "--output",
        "json",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    data = json.loads(result.stdout)
    return data["workflowExecutionInfo"]["execution"]["runId"]


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_signal_external_workflow_success(trio_client: Client) -> None:
    """Test signaling an external workflow successfully.

    This test:
    1. Starts a target workflow that waits for a signal
    2. Starts a signaler workflow that signals the target
    3. Verifies both workflows complete successfully
    """
    unique_task_queue = f"test-signal-external-{uuid4()}"
    target_workflow_id = f"target-workflow-{uuid4()}"
    signaler_workflow_id = f"signaler-workflow-{uuid4()}"

    # Start target workflow via CLI
    start_workflow_via_cli(
        workflow_id=target_workflow_id,
        workflow_type="TargetWorkflow",
        task_queue=unique_task_queue,
    )

    # Start signaler workflow via CLI
    start_workflow_via_cli(
        workflow_id=signaler_workflow_id,
        workflow_type="SignalerWorkflow",
        task_queue=unique_task_queue,
        input_data=json.dumps(target_workflow_id),
    )

    # Run worker to process both workflows
    async def run_worker():
        async with Worker(
            trio_client,
            task_queue=unique_task_queue,
            workflows=[TargetWorkflow, SignalerWorkflow],
        ):
            # Wait for both workflows to complete
            await trio.sleep(10)

    # Run with timeout
    with trio.move_on_after(30):
        await run_worker()

    # Verify results
    target_status = get_workflow_status_and_result_via_cli(target_workflow_id)
    signaler_status = get_workflow_status_and_result_via_cli(signaler_workflow_id)

    assert target_status["status"] == "COMPLETED"
    assert signaler_status["status"] == "COMPLETED"
    assert target_status["result"] == "Received: hello from signaler"
    assert signaler_status["result"] == "signal_sent"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_signal_external_workflow_with_method_ref(trio_client: Client) -> None:
    """Test signaling using method reference.

    This verifies that the signal name is correctly extracted from
    the method reference.
    """
    unique_task_queue = f"test-signal-method-ref-{uuid4()}"
    target_workflow_id = f"target-workflow-{uuid4()}"
    signaler_workflow_id = f"signaler-workflow-{uuid4()}"

    # Start target workflow via CLI
    start_workflow_via_cli(
        workflow_id=target_workflow_id,
        workflow_type="TargetWorkflow",
        task_queue=unique_task_queue,
    )

    # Start signaler workflow via CLI
    start_workflow_via_cli(
        workflow_id=signaler_workflow_id,
        workflow_type="SignalerWithMethodRefWorkflow",
        task_queue=unique_task_queue,
        input_data=json.dumps(target_workflow_id),
    )

    # Run worker
    async def run_worker():
        async with Worker(
            trio_client,
            task_queue=unique_task_queue,
            workflows=[TargetWorkflow, SignalerWithMethodRefWorkflow],
        ):
            await trio.sleep(10)

    with trio.move_on_after(30):
        await run_worker()

    # Verify results
    target_status = get_workflow_status_and_result_via_cli(target_workflow_id)
    signaler_status = get_workflow_status_and_result_via_cli(signaler_workflow_id)

    assert target_status["status"] == "COMPLETED"
    assert signaler_status["status"] == "COMPLETED"
    assert target_status["result"] == "Received: hello via method ref"
    assert signaler_status["result"] == "signal_sent_via_method_ref"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_get_external_workflow_handle_properties() -> None:
    """Test ExternalWorkflowHandle properties."""
    # This test doesn't need a Temporal server - just tests the handle object

    class MockRuntime(workflow._Runtime):
        def workflow_time_ns(self) -> int:
            return 0

        async def workflow_sleep(self, duration: float, summary: str | None) -> None:
            pass

        def workflow_info(self) -> workflow.Info:
            return workflow.Info(
                workflow_id="mock",
                workflow_type="MockWorkflow",
                run_id="mock-run",
                task_queue="mock-queue",
            )

        async def workflow_execute_activity(self, *args, **kwargs):
            pass

        async def workflow_start_child_workflow(self, *args, **kwargs):
            pass

        async def workflow_wait_child_workflow(self, *args, **kwargs):
            pass

        async def workflow_wait_condition(self, *args, **kwargs):
            pass

        def workflow_continue_as_new(self, *args, **kwargs):
            pass

        def workflow_get_external_workflow_handle(
            self,
            workflow_id: str,
            *,
            run_id: str | None,
        ) -> workflow.ExternalWorkflowHandle:
            return workflow.ExternalWorkflowHandle(self, workflow_id, run_id)

        async def workflow_signal_external_workflow(
            self,
            workflow_id: str,
            signal_name: str,
            args,
            *,
            run_id: str | None,
        ) -> None:
            pass

    mock = MockRuntime()
    token = workflow._Runtime.set_current(mock)
    try:
        # Test without run_id
        handle = workflow.get_external_workflow_handle("test-workflow-id")
        assert handle.id == "test-workflow-id"
        assert handle.run_id is None

        # Test with run_id
        handle_with_run = workflow.get_external_workflow_handle(
            "test-workflow-id", run_id="specific-run"
        )
        assert handle_with_run.id == "test-workflow-id"
        assert handle_with_run.run_id == "specific-run"

        # Test get_external_workflow_handle_for (same behavior, just typed)
        handle_for = workflow.get_external_workflow_handle_for(
            TargetWorkflow.run, "typed-workflow-id"
        )
        assert handle_for.id == "typed-workflow-id"
    finally:
        workflow._Runtime.reset_current(token)
