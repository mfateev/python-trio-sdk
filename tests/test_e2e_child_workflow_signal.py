"""End-to-end tests for child workflow signaling.

These tests validate that parent workflows can signal child workflows
and that signals are delivered correctly through the Temporal server.

Run these tests with:
    pytest -v -m temporal_server tests/test_e2e_child_workflow_signal.py
"""

import json
import subprocess
import time
from typing import Any
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


# =============================================================================
# Child Workflow Signal Tests
# =============================================================================


@workflow.defn
class SignalReceivingChildWorkflow:
    """Child workflow that receives signals and stores them."""

    def __init__(self) -> None:
        self.signals_received: list[str] = []
        self.completed = False

    @workflow.run
    async def run(self) -> dict[str, Any]:
        """Wait for signals and then complete."""
        # Wait for completion signal
        await workflow.wait_condition(lambda: self.completed)

        return {
            "signals_received": self.signals_received,
            "signal_count": len(self.signals_received),
        }

    @workflow.signal
    def add_signal(self, value: str) -> None:
        """Signal handler to add a value."""
        self.signals_received.append(value)

    @workflow.signal
    def complete(self) -> None:
        """Signal handler to complete the workflow."""
        self.completed = True


@workflow.defn
class ParentWorkflowSignalsChild:
    """Parent workflow that starts a child and signals it."""

    @workflow.run
    async def run(self, child_id: str) -> dict[str, Any]:
        """Start child workflow and send signals to it."""
        # Start child workflow
        child_handle = await workflow.start_child_workflow(
            SignalReceivingChildWorkflow.run,
            id=child_id,
        )

        # Give child time to start (in workflow time)
        await workflow.sleep(1)

        # Send signals to child using string signal name
        await child_handle.signal("add_signal", "signal1")
        await workflow.sleep(0.5)

        await child_handle.signal("add_signal", "signal2")
        await workflow.sleep(0.5)

        await child_handle.signal("add_signal", "signal3")
        await workflow.sleep(0.5)

        # Complete the child
        await child_handle.signal("complete")

        # Wait for child to complete
        child_result = await child_handle

        return {
            "parent_success": True,
            "child_result": child_result,
            "signals_sent": ["signal1", "signal2", "signal3"],
        }


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_child_workflow_signal_string_name(trio_client):
    """Test that parent can signal child workflow using string signal names.

    This test validates:
    1. Parent workflow can start a child workflow
    2. Parent can signal child using ChildWorkflowHandle.signal()
    3. Child receives signals in correct order
    4. All signals are delivered successfully through Temporal server
    """
    task_queue = f"test-child-signal-{uuid4()}"
    workflow_id = f"parent-workflow-{uuid4()}"
    child_id = f"child-workflow-{uuid4()}"

    # Start parent workflow via CLI
    start_workflow_via_cli(
        workflow_id=workflow_id,
        workflow_type="ParentWorkflowSignalsChild",
        task_queue=task_queue,
        input_data=json.dumps(child_id),
    )

    # Run worker with polling pattern
    worker = Worker(
        trio_client,
        task_queue=task_queue,
        workflows=[ParentWorkflowSignalsChild, SignalReceivingChildWorkflow],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)
        try:
            # Poll for workflow completion
            for _ in range(60):
                parent_status = get_workflow_status_and_result_via_cli(
                    workflow_id, timeout=1,
                )
                if parent_status["status"] in ("COMPLETED", "FAILED", "TERMINATED", "CANCELLED"):
                    break
                await trio.sleep(0.3)

            # Verify parent workflow completed
            assert parent_status["status"] == "COMPLETED", f"Parent status: {parent_status['status']}"

            result = parent_status["result"]
            if isinstance(result, str):
                result = json.loads(result)

            # Verify parent succeeded
            assert result["parent_success"] is True, "Parent workflow should succeed"

            # Verify child received all signals
            child_result = result["child_result"]
            assert child_result["signal_count"] == 3, (
                f"Expected 3 signals, got {child_result['signal_count']}"
            )
            assert child_result["signals_received"] == ["signal1", "signal2", "signal3"], (
                f"Expected signals in order, got {child_result['signals_received']}"
            )
        finally:
            worker.shutdown()
            await trio.sleep(0.3)
            nursery.cancel_scope.cancel()


# =============================================================================
# Test with Signal Method Reference
# =============================================================================


@workflow.defn
class ParentWorkflowSignalsChildWithMethod:
    """Parent workflow that signals child using method references."""

    @workflow.run
    async def run(self, child_id: str) -> dict[str, Any]:
        """Start child workflow and send signals using method references."""
        # Start child workflow
        child_handle = await workflow.start_child_workflow(
            SignalReceivingChildWorkflow.run,
            id=child_id,
        )

        # Give child time to start
        await workflow.sleep(1)

        # Send signals using method references (callable)
        await child_handle.signal(SignalReceivingChildWorkflow.add_signal, "method_signal1")
        await workflow.sleep(0.5)

        await child_handle.signal(SignalReceivingChildWorkflow.add_signal, "method_signal2")
        await workflow.sleep(0.5)

        # Complete using method reference
        await child_handle.signal(SignalReceivingChildWorkflow.complete)

        # Wait for child to complete
        child_result = await child_handle

        return {
            "parent_success": True,
            "child_result": child_result,
            "method": "callable",
        }


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_child_workflow_signal_with_method_ref(trio_client):
    """Test that parent can signal child workflow using method references.

    This test validates:
    1. Parent can signal child using method references (callables)
    2. Method references are correctly resolved to signal names
    3. Child receives signals successfully
    """
    task_queue = f"test-child-signal-method-{uuid4()}"
    workflow_id = f"parent-workflow-{uuid4()}"
    child_id = f"child-workflow-{uuid4()}"

    # Start parent workflow via CLI
    start_workflow_via_cli(
        workflow_id=workflow_id,
        workflow_type="ParentWorkflowSignalsChildWithMethod",
        task_queue=task_queue,
        input_data=json.dumps(child_id),
    )

    # Run worker with polling pattern
    worker = Worker(
        trio_client,
        task_queue=task_queue,
        workflows=[ParentWorkflowSignalsChildWithMethod, SignalReceivingChildWorkflow],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)
        try:
            # Poll for workflow completion
            for _ in range(60):
                parent_status = get_workflow_status_and_result_via_cli(
                    workflow_id, timeout=1,
                )
                if parent_status["status"] in ("COMPLETED", "FAILED", "TERMINATED", "CANCELLED"):
                    break
                await trio.sleep(0.3)

            # Verify results
            assert parent_status["status"] == "COMPLETED"

            result = parent_status["result"]
            if isinstance(result, str):
                result = json.loads(result)

            # Verify parent succeeded
            assert result["parent_success"] is True
            assert result["method"] == "callable"

            # Verify child received signals
            child_result = result["child_result"]
            assert child_result["signal_count"] == 2
            assert child_result["signals_received"] == ["method_signal1", "method_signal2"]
        finally:
            worker.shutdown()
            await trio.sleep(0.3)
            nursery.cancel_scope.cancel()


# =============================================================================
# Test Signal Before Child Starts
# =============================================================================


@workflow.defn
class QuickChildWorkflow:
    """Child workflow that completes quickly after receiving signal."""

    def __init__(self) -> None:
        self.received_value: str | None = None

    @workflow.run
    async def run(self) -> dict[str, Any]:
        """Wait for signal and complete."""
        await workflow.wait_condition(lambda: self.received_value is not None)
        return {"value": self.received_value}

    @workflow.signal
    def set_value(self, value: str) -> None:
        """Signal handler."""
        self.received_value = value


@workflow.defn
class ParentWorkflowEarlySignal:
    """Parent workflow that signals child immediately after starting."""

    @workflow.run
    async def run(self, child_id: str) -> dict[str, Any]:
        """Start child and signal it immediately."""
        # Start child workflow (returns handle immediately)
        child_handle = await workflow.start_child_workflow(
            QuickChildWorkflow.run,
            id=child_id,
        )

        # Signal immediately (before child might be fully started)
        # This tests that signals are buffered by Temporal
        await child_handle.signal("set_value", "early_signal")

        # Wait for child to complete
        child_result = await child_handle

        return {
            "parent_success": True,
            "child_result": child_result,
        }


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_child_workflow_signal_early(trio_client):
    """Test that signals sent immediately after child start are buffered.

    This test validates:
    1. Parent can signal child immediately after starting it
    2. Temporal buffers signals if child isn't fully started yet
    3. Child receives buffered signals when it starts processing
    """
    task_queue = f"test-child-signal-early-{uuid4()}"
    workflow_id = f"parent-workflow-{uuid4()}"
    child_id = f"child-workflow-{uuid4()}"

    # Start parent workflow via CLI
    start_workflow_via_cli(
        workflow_id=workflow_id,
        workflow_type="ParentWorkflowEarlySignal",
        task_queue=task_queue,
        input_data=json.dumps(child_id),
    )

    # Run worker with polling pattern
    worker = Worker(
        trio_client,
        task_queue=task_queue,
        workflows=[ParentWorkflowEarlySignal, QuickChildWorkflow],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)
        try:
            # Poll for workflow completion
            for _ in range(60):
                parent_status = get_workflow_status_and_result_via_cli(
                    workflow_id, timeout=1,
                )
                if parent_status["status"] in ("COMPLETED", "FAILED", "TERMINATED", "CANCELLED"):
                    break
                await trio.sleep(0.3)

            # Verify results
            assert parent_status["status"] == "COMPLETED"

            result = parent_status["result"]
            if isinstance(result, str):
                result = json.loads(result)

            # Verify parent succeeded
            assert result["parent_success"] is True

            # Verify child received the early signal
            child_result = result["child_result"]
            assert child_result["value"] == "early_signal"
        finally:
            worker.shutdown()
            await trio.sleep(0.3)
            nursery.cancel_scope.cancel()


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
        time.sleep(0.3)
    return {"status": "UNKNOWN", "result": None}
