"""End-to-end integration tests with real Temporal service.

These tests require a running Temporal server and validate the complete
workflow execution path from worker startup through workflow completion.

Run these tests with:
    pytest -v -m temporal_server tests/test_e2e_integration.py

Or skip them with:
    pytest -v -m "not temporal_server"
"""

import json
import subprocess
import time
from typing import Any

import pytest
import trio

from temporalio_trio import workflow
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker

TEMPORAL_CLI_PATH = "/home/sprite/workarea/bin/temporal"


@pytest.fixture
async def trio_client():
    """Create a Trio-based Temporal client.

    This fixture creates the client within Trio context using the
    native temporalio_trio.client.Client.
    """
    client = await Client.connect("localhost:7233", namespace="default")

    yield client

    await client.close()


@workflow.defn
class SimpleTimerWorkflow:
    """Simple workflow that sleeps for a specified duration."""

    @workflow.run
    async def run(self, duration: float) -> str:
        """Execute workflow with timer.

        Args:
            duration: Time to sleep in seconds

        Returns:
            Success message with duration
        """
        start_time = workflow.time()
        await workflow.sleep(duration)
        end_time = workflow.time()

        elapsed = end_time - start_time
        return f"Slept for {elapsed:.2f} seconds (requested {duration:.2f})"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_workflow_execution(trio_client):
    """Test end-to-end workflow execution with real Temporal server.

    This test:
    1. Starts a Trio worker with SimpleTimerWorkflow
    2. Uses temporal CLI to execute the workflow
    3. Validates the result via the Temporal CLI
    4. Cleans up resources

    Requires:
        - Temporal server running on localhost:7233
        - temporal CLI at /home/sprite/workarea/bin/temporal
    """
    # Configuration
    namespace = "default"
    task_queue = "trio-e2e-test-queue"
    workflow_id = f"test-workflow-{int(time.time())}"
    sleep_duration = 2.0

    # Create worker with the Trio client from fixture
    worker = Worker(
        client=trio_client,
        task_queue=task_queue,
        workflows=[SimpleTimerWorkflow],
    )

    async with trio.open_nursery() as nursery:
        # Start the worker
        nursery.start_soon(worker.run)

        # Give worker time to start and connect
        await trio.sleep(3)

        try:
            # Execute workflow using temporal CLI
            print(f"Starting workflow {workflow_id} via CLI...")
            _start_workflow_via_cli(
                workflow_id=workflow_id,
                workflow_type="SimpleTimerWorkflow",
                task_queue=task_queue,
                namespace=namespace,
                args=[sleep_duration],
            )

            # Poll for workflow completion (max 30 seconds)
            max_wait = 30
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

            # Validate result via CLI
            assert cli_result["status"] == "COMPLETED"
            assert cli_result["workflow_id"] == workflow_id

            # Validate result contains expected output
            cli_output = cli_result.get("result")
            if cli_output:
                assert "Slept for" in cli_output
                print(f"Workflow result from CLI: {cli_output}")

            print("✅ E2E test passed - workflow executed successfully")

        finally:
            # Shutdown worker
            worker.shutdown()
            await trio.sleep(0.5)
            nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_worker_connection(trio_client):
    """Test that worker can successfully start and shutdown.

    This is a minimal test that validates:
    1. Worker initialization succeeds
    2. Worker can start without crashing
    3. Graceful shutdown works

    Note: This test does NOT validate that the worker can actually poll for
    and process workflow tasks, as that requires the Rust bridge implementation
    (TrioAsyncBridge) which is not yet complete.
    """
    task_queue = "trio-e2e-connection-test"

    # Create worker with the Trio client
    worker = Worker(
        client=trio_client,
        task_queue=task_queue,
        workflows=[SimpleTimerWorkflow],
    )

    # Start worker and let it run briefly
    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)

        # Let worker run for a short period
        await trio.sleep(3)

        # Shutdown
        worker.shutdown()
        await trio.sleep(0.5)
        nursery.cancel_scope.cancel()

    print("✅ Worker connection test passed")


def _start_workflow_via_cli(
    workflow_id: str,
    workflow_type: str,
    task_queue: str,
    namespace: str,
    args: list[Any],
) -> None:
    """Start a workflow using Temporal CLI.

    Args:
        workflow_id: The workflow ID to use
        workflow_type: The workflow type name
        task_queue: Task queue to run workflow on
        namespace: Temporal namespace
        args: List of arguments to pass to the workflow

    Raises:
        RuntimeError: If CLI command fails
    """
    try:
        # Build the command
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

        # Add arguments as JSON
        for arg in args:
            cmd.extend(["--input", json.dumps(arg)])

        # Execute command
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
    except Exception as e:
        raise RuntimeError(f"Unexpected error starting workflow: {e}") from e


def _query_workflow_via_cli(workflow_id: str, namespace: str) -> dict[str, Any]:
    """Query workflow status using Temporal CLI.

    Args:
        workflow_id: The workflow ID to query
        namespace: Temporal namespace

    Returns:
        Dictionary with workflow information

    Raises:
        RuntimeError: If CLI query fails
    """
    try:
        # Query workflow status
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

        # Parse JSON output
        data = json.loads(result.stdout)

        # Extract key information
        workflow_info = data.get("workflowExecutionInfo", {})
        status_str = workflow_info.get("status", "UNKNOWN")

        # Normalize status string (e.g., "WORKFLOW_EXECUTION_STATUS_COMPLETED" -> "COMPLETED")
        if status_str.startswith("WORKFLOW_EXECUTION_STATUS_"):
            status_str = status_str.replace("WORKFLOW_EXECUTION_STATUS_", "")

        # Get result if workflow completed - CLI outputs result directly as a string
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
    except Exception as e:
        raise RuntimeError(f"Unexpected error querying workflow: {e}") from e


if __name__ == "__main__":
    # Allow running tests directly for debugging
    print("Running E2E integration tests...")
    print("Note: Requires Temporal server running on localhost:7233")
    print()

    import sys

    sys.exit(pytest.main([__file__, "-v", "-s"]))
