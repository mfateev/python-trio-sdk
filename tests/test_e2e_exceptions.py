"""End-to-end tests for exception type preservation.

These tests validate that activities and child workflows that fail produce
properly typed exceptions (ActivityError, ChildWorkflowError, etc.) that
workflows can catch and inspect.

Run these tests with:
    pytest -v -m temporal_server tests/test_e2e_exceptions.py
"""

import json
import subprocess
import time
from datetime import timedelta
from typing import Any

import pytest
import trio

from temporalio.common import RetryPolicy
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    ChildWorkflowError,
)

from temporalio_trio import activity, workflow
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
# Activity Error Tests
# =============================================================================


@activity.defn
async def failing_activity() -> str:
    """Activity that always fails with an ApplicationError."""
    raise ApplicationError("Activity intentionally failed", type="IntentionalError")


@activity.defn
async def failing_activity_with_details() -> str:
    """Activity that fails with details."""
    raise ApplicationError(
        "Activity failed with details",
        "detail1",
        "detail2",
        type="DetailedError",
    )


@workflow.defn
class ActivityErrorWorkflow:
    """Workflow that catches ActivityError and returns information about it."""

    @workflow.run
    async def run(self) -> dict[str, Any]:
        """Execute workflow that catches activity error."""
        try:
            await workflow.execute_activity(
                failing_activity,
                start_to_close_timeout=timedelta(seconds=10),
                # No retries - we want to catch the error immediately
                retry_policy=RetryPolicy(maximum_attempts=1),
            )
            return {"success": True, "error": None}
        except ActivityError as e:
            result: dict[str, Any] = {
                "success": False,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "activity_type": e.activity_type,
                "activity_id": e.activity_id,
            }
            # Check the cause
            if e.__cause__ is not None:
                result["cause_type"] = type(e.__cause__).__name__
                result["cause_message"] = str(e.__cause__)
                if isinstance(e.__cause__, ApplicationError):
                    result["cause_app_type"] = e.__cause__.type
            return result
        except Exception as e:
            # Unexpected exception type
            return {
                "success": False,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "unexpected": True,
            }


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_activity_error_type_preserved(trio_client):
    """Test that ActivityError type is preserved when activity fails.

    This test validates:
    1. Failed activity raises ActivityError (not RuntimeError)
    2. ActivityError has correct metadata (activity_type, activity_id)
    3. __cause__ contains the ApplicationError from the activity
    """
    namespace = "default"
    task_queue = f"trio-e2e-activity-error-queue-{int(time.time())}"
    workflow_id = f"test-activity-error-{int(time.time())}"

    worker = Worker(
        client=trio_client,
        task_queue=task_queue,
        workflows=[ActivityErrorWorkflow],
        activities=[failing_activity],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)

        # Give worker time to start
        await trio.sleep(3)

        try:
            # Start workflow
            print(f"Starting workflow {workflow_id}...")
            _start_workflow_via_cli(
                workflow_id=workflow_id,
                workflow_type="ActivityErrorWorkflow",
                task_queue=task_queue,
                namespace=namespace,
                args=[],
            )

            # Wait for workflow completion
            max_wait = 60
            start_time_test = time.time()
            while time.time() - start_time_test < max_wait:
                cli_result = _query_workflow_via_cli(workflow_id, namespace)
                status = cli_result.get("status", "UNKNOWN")

                if status == "COMPLETED":
                    break
                elif status in ["FAILED", "TERMINATED", "CANCELLED"]:
                    raise RuntimeError(f"Workflow ended with status: {status}")

                await trio.sleep(0.5)
            else:
                raise TimeoutError(
                    f"Workflow did not complete within {max_wait} seconds"
                )

            # Get the result
            result = cli_result.get("result")
            print(f"Workflow result: {result}")

            # Parse result (it's a JSON string from CLI)
            if isinstance(result, str):
                result = json.loads(result)

            # Verify error was caught as ActivityError
            assert result["success"] is False, "Expected workflow to catch an error"
            assert result["error_type"] == "ActivityError", (
                f"Expected ActivityError, got {result['error_type']}"
            )
            assert result["activity_type"] == "failing_activity", (
                f"Expected activity_type='failing_activity', got {result['activity_type']}"
            )
            assert "unexpected" not in result, (
                f"Unexpected exception type caught: {result}"
            )

            # Verify cause is ApplicationError
            assert result.get("cause_type") == "ApplicationError", (
                f"Expected cause to be ApplicationError, got {result.get('cause_type')}"
            )
            assert result.get("cause_app_type") == "IntentionalError", (
                f"Expected cause app type 'IntentionalError', got {result.get('cause_app_type')}"
            )

            print("Verified:")
            print(f"  - Activity error caught as {result['error_type']}")
            print(f"  - Activity type: {result['activity_type']}")
            print(f"  - Cause type: {result.get('cause_type')}")
            print(f"  - Cause app type: {result.get('cause_app_type')}")
            print("E2E activity error type preservation test passed")

        finally:
            worker.shutdown()
            await trio.sleep(0.5)
            nursery.cancel_scope.cancel()


# =============================================================================
# Child Workflow Error Tests
# =============================================================================


@workflow.defn
class FailingChildWorkflow:
    """Child workflow that always fails."""

    @workflow.run
    async def run(self) -> str:
        """Execute and fail."""
        raise ApplicationError(
            "Child workflow intentionally failed",
            type="ChildError",
        )


@workflow.defn
class ChildWorkflowErrorWorkflow:
    """Workflow that catches ChildWorkflowError and returns information about it."""

    @workflow.run
    async def run(self) -> dict[str, Any]:
        """Execute workflow that catches child workflow error."""
        try:
            await workflow.execute_child_workflow(
                FailingChildWorkflow.run,
                id=f"failing-child-{workflow.info().workflow_id}",
            )
            return {"success": True, "error": None}
        except ChildWorkflowError as e:
            result: dict[str, Any] = {
                "success": False,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "workflow_type": e.workflow_type,
                "workflow_id": e.workflow_id,
                "namespace": e.namespace,
            }
            # Check the cause
            if e.__cause__ is not None:
                result["cause_type"] = type(e.__cause__).__name__
                result["cause_message"] = str(e.__cause__)
                if isinstance(e.__cause__, ApplicationError):
                    result["cause_app_type"] = e.__cause__.type
            return result
        except Exception as e:
            # Unexpected exception type
            return {
                "success": False,
                "error_type": type(e).__name__,
                "error_message": str(e),
                "unexpected": True,
            }


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_child_workflow_error_type_preserved(trio_client):
    """Test that ChildWorkflowError type is preserved when child workflow fails.

    This test validates:
    1. Failed child workflow raises ChildWorkflowError (not RuntimeError)
    2. ChildWorkflowError has correct metadata (workflow_type, workflow_id)
    3. __cause__ contains the ApplicationError from the child
    """
    namespace = "default"
    task_queue = f"trio-e2e-child-error-queue-{int(time.time())}"
    workflow_id = f"test-child-error-{int(time.time())}"

    worker = Worker(
        client=trio_client,
        task_queue=task_queue,
        workflows=[ChildWorkflowErrorWorkflow, FailingChildWorkflow],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)

        # Give worker time to start
        await trio.sleep(3)

        try:
            # Start workflow
            print(f"Starting workflow {workflow_id}...")
            _start_workflow_via_cli(
                workflow_id=workflow_id,
                workflow_type="ChildWorkflowErrorWorkflow",
                task_queue=task_queue,
                namespace=namespace,
                args=[],
            )

            # Wait for workflow completion (child workflow failure + parent handling)
            max_wait = 60
            start_time_test = time.time()
            while time.time() - start_time_test < max_wait:
                cli_result = _query_workflow_via_cli(workflow_id, namespace)
                status = cli_result.get("status", "UNKNOWN")

                if status == "COMPLETED":
                    break
                elif status in ["FAILED", "TERMINATED", "CANCELLED"]:
                    raise RuntimeError(f"Workflow ended with status: {status}")

                await trio.sleep(0.5)
            else:
                raise TimeoutError(
                    f"Workflow did not complete within {max_wait} seconds"
                )

            # Get the result
            result = cli_result.get("result")
            print(f"Workflow result: {result}")

            # Parse result (it's a JSON string from CLI)
            if isinstance(result, str):
                result = json.loads(result)

            # Verify error was caught as ChildWorkflowError
            assert result["success"] is False, "Expected workflow to catch an error"
            assert result["error_type"] == "ChildWorkflowError", (
                f"Expected ChildWorkflowError, got {result['error_type']}"
            )
            assert result["workflow_type"] == "FailingChildWorkflow", (
                f"Expected workflow_type='FailingChildWorkflow', got {result['workflow_type']}"
            )
            assert "unexpected" not in result, (
                f"Unexpected exception type caught: {result}"
            )

            # Verify cause is ApplicationError
            assert result.get("cause_type") == "ApplicationError", (
                f"Expected cause to be ApplicationError, got {result.get('cause_type')}"
            )
            assert result.get("cause_app_type") == "ChildError", (
                f"Expected cause app type 'ChildError', got {result.get('cause_app_type')}"
            )

            print("Verified:")
            print(f"  - Child workflow error caught as {result['error_type']}")
            print(f"  - Workflow type: {result['workflow_type']}")
            print(f"  - Cause type: {result.get('cause_type')}")
            print(f"  - Cause app type: {result.get('cause_app_type')}")
            print("E2E child workflow error type preservation test passed")

        finally:
            worker.shutdown()
            await trio.sleep(0.5)
            nursery.cancel_scope.cancel()


# =============================================================================
# Helper Functions
# =============================================================================


def _start_workflow_via_cli(
    workflow_id: str,
    workflow_type: str,
    task_queue: str,
    namespace: str,
    args: list[Any],
) -> None:
    """Start a workflow using Temporal CLI."""
    try:
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
            "--namespace",
            namespace,
        ]

        for arg in args:
            cmd.extend(["--input", json.dumps(arg)])

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )

        print(f"Workflow started successfully: {workflow_id}")
        if result.stdout:
            print(f"CLI output: {result.stdout.strip()}")

    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"Failed to start workflow: {e.stderr if e.stderr else e.stdout}"
        ) from e


def _query_workflow_via_cli(workflow_id: str, namespace: str) -> dict[str, Any]:
    """Query workflow status using Temporal CLI."""
    try:
        result = subprocess.run(
            [
                TEMPORAL_CLI_PATH,
                "workflow",
                "describe",
                "--workflow-id",
                workflow_id,
                "--namespace",
                namespace,
                "--output",
                "json",
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=True,
        )

        data = json.loads(result.stdout)
        workflow_info = data.get("workflowExecutionInfo", {})
        status_str = workflow_info.get("status", "UNKNOWN")

        if status_str.startswith("WORKFLOW_EXECUTION_STATUS_"):
            status_str = status_str.replace("WORKFLOW_EXECUTION_STATUS_", "")

        result_value = data.get("result")

        return {
            "status": status_str,
            "workflow_id": workflow_id,
            "result": result_value,
            "execution": workflow_info.get("execution", {}),
        }

    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"CLI query failed: {e.stderr if e.stderr else e.stdout}"
        ) from e
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse CLI output: {e}") from e
