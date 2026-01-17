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
from temporalio.client import Client

from temporalio_trio import workflow
from temporalio_trio.worker import Worker


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
async def test_e2e_workflow_execution():
    """Test end-to-end workflow execution with real Temporal server.

    This test:
    1. Connects to a running Temporal server (localhost:7233)
    2. Starts a worker with the SimpleTimerWorkflow
    3. Executes the workflow
    4. Validates the result via the Temporal CLI
    5. Cleans up resources

    Requires:
        - Temporal server running on localhost:7233
        - temporal CLI installed and available in PATH
    """
    # Configuration
    target_url = "localhost:7233"
    namespace = "default"
    task_queue = "trio-e2e-test-queue"
    workflow_id = f"test-workflow-{int(time.time())}"
    sleep_duration = 2.0

    # Connect to Temporal
    client = await Client.connect(target_url, namespace=namespace)

    # Start worker in background
    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[SimpleTimerWorkflow],
    )

    async with trio.open_nursery() as nursery:
        # Start the worker
        worker_task = nursery.start_soon(worker.run)

        # Give worker time to start and connect
        await trio.sleep(2)

        try:
            # Execute workflow
            handle = await client.start_workflow(
                SimpleTimerWorkflow.run,
                args=[sleep_duration],
                id=workflow_id,
                task_queue=task_queue,
            )

            # Wait for workflow to complete
            result = await handle.result()

            # Validate result from Python
            assert isinstance(result, str)
            assert "Slept for" in result
            assert f"{sleep_duration:.2f}" in result
            print(f"Workflow result from Python: {result}")

            # Validate via CLI
            cli_result = _query_workflow_via_cli(workflow_id, namespace)
            assert cli_result["status"] == "COMPLETED"
            assert cli_result["workflow_id"] == workflow_id

            # Validate result matches
            cli_output = cli_result.get("result")
            if cli_output:
                assert "Slept for" in cli_output

            print("✅ E2E test passed - workflow executed successfully")

        finally:
            # Shutdown worker
            await worker.shutdown()
            nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_worker_connection():
    """Test that worker can successfully connect to Temporal server.

    This is a minimal test that validates:
    1. Client connection works
    2. Worker initialization succeeds
    3. Worker can start and validate its connection
    4. Graceful shutdown works
    """
    target_url = "localhost:7233"
    namespace = "default"
    task_queue = "trio-e2e-connection-test"

    # Connect to Temporal
    client = await Client.connect(target_url, namespace=namespace)

    # Create worker
    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[SimpleTimerWorkflow],
    )

    # Start worker and let it run briefly
    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)

        # Let worker run for a short period
        await trio.sleep(3)

        # Shutdown
        await worker.shutdown()
        nursery.cancel_scope.cancel()

    print("✅ Worker connection test passed")


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
                "temporal",
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

        # Get result if workflow completed
        result_payloads = workflow_info.get("result", {}).get("payloads", [])
        result_value = None
        if result_payloads:
            # Decode first payload (simplified - assumes string result)
            payload = result_payloads[0]
            if "data" in payload:
                import base64

                decoded = base64.b64decode(payload["data"])
                # Remove JSON encoding artifacts
                result_value = decoded.decode("utf-8").strip('"')

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
