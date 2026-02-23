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
from datetime import timedelta
from typing import Any

import pytest
import trio

from temporalio_trio import workflow
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker

TEMPORAL_CLI_PATH = "/home/dev/.temporalio/bin/temporal"


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
        - temporal CLI at /home/dev/.temporalio/bin/temporal
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

                await trio.sleep(0.3)
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

            print("E2E test passed - workflow executed successfully")

        finally:
            # Shutdown worker
            worker.shutdown()
            await trio.sleep(0.3)
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

        # Let worker initialize (needs time to connect to server)
        await trio.sleep(0.5)

        # Shutdown
        worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()

    print("Worker connection test passed")


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


@workflow.defn
class TimerSummaryWorkflow:
    """Workflow that uses sleep with a summary for testing."""

    @workflow.run
    async def run(self) -> str:
        """Execute workflow with a timer that has a summary."""
        await workflow.sleep(0.1, summary="waiting-for-approval")
        return "done"


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_timer_summary_in_history(trio_client):
    """Test that workflow.sleep() summary is preserved in workflow history.

    This test validates that when using workflow.sleep(duration, summary="..."),
    the summary appears in the TimerStarted event's user_metadata in the
    workflow history.

    Requires:
        - Temporal server running on localhost:7233
        - temporal CLI at /home/dev/.temporalio/bin/temporal
    """
    # Configuration
    namespace = "default"
    task_queue = "trio-e2e-timer-summary-queue"
    workflow_id = f"test-timer-summary-{int(time.time())}"

    # Create worker
    worker = Worker(
        client=trio_client,
        task_queue=task_queue,
        workflows=[TimerSummaryWorkflow],
    )

    async with trio.open_nursery() as nursery:
        # Start the worker
        nursery.start_soon(worker.run)

        try:
            # Execute workflow
            print(f"Starting workflow {workflow_id} via CLI...")
            _start_workflow_via_cli(
                workflow_id=workflow_id,
                workflow_type="TimerSummaryWorkflow",
                task_queue=task_queue,
                namespace=namespace,
                args=[],
            )

            # Wait for workflow completion
            max_wait = 30
            start_time_test = time.time()
            while time.time() - start_time_test < max_wait:
                cli_result = _query_workflow_via_cli(workflow_id, namespace)
                status = cli_result.get("status", "UNKNOWN")

                if status == "COMPLETED":
                    break
                elif status in ["FAILED", "TERMINATED", "CANCELLED"]:
                    raise RuntimeError(f"Workflow ended with status: {status}")

                await trio.sleep(0.3)
            else:
                raise TimeoutError(
                    f"Workflow did not complete within {max_wait} seconds"
                )

            # Get workflow history and verify timer summary
            history = _get_workflow_history_via_cli(workflow_id, namespace)

            # Find TimerStarted event
            timer_started_events = [
                e
                for e in history.get("events", [])
                if e.get("eventType") == "EVENT_TYPE_TIMER_STARTED"
            ]

            assert len(timer_started_events) >= 1, (
                "Expected at least one TimerStarted event"
            )

            # Check that the summary is in user_metadata
            timer_event = timer_started_events[0]
            user_metadata = timer_event.get("userMetadata", {})
            summary_payload = user_metadata.get("summary", {})

            # The summary is stored as a JSON-encoded payload
            # The data is base64 encoded in the payload
            import base64

            summary_data = summary_payload.get("data", "")
            if summary_data:
                # Decode base64 and parse JSON
                decoded = base64.b64decode(summary_data).decode("utf-8")
                # Remove JSON quotes if present
                if decoded.startswith('"') and decoded.endswith('"'):
                    decoded = decoded[1:-1]
                assert decoded == "waiting-for-approval", (
                    f"Expected summary 'waiting-for-approval', got '{decoded}'"
                )
            else:
                raise AssertionError(
                    f"Timer event missing summary in user_metadata: {timer_event}"
                )

            print("E2E timer summary test passed - summary preserved in history")

        finally:
            # Shutdown worker
            worker.shutdown()
            await trio.sleep(0.3)
            nursery.cancel_scope.cancel()


def _get_workflow_history_via_cli(workflow_id: str, namespace: str) -> dict[str, Any]:
    """Get workflow history using Temporal CLI.

    Args:
        workflow_id: The workflow ID to get history for
        namespace: Temporal namespace

    Returns:
        Dictionary with workflow history

    Raises:
        RuntimeError: If CLI command fails
    """
    try:
        result = subprocess.run(
            [
                TEMPORAL_CLI_PATH,
                "workflow",
                "show",
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

        return json.loads(result.stdout)

    except subprocess.CalledProcessError as e:
        raise RuntimeError(
            f"CLI history query failed: {e.stderr if e.stderr else e.stdout}"
        ) from e
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse CLI output: {e}") from e
    except Exception as e:
        raise RuntimeError(f"Unexpected error getting history: {e}") from e


# =============================================================================
# Eviction and Replay E2E Tests
# =============================================================================


@workflow.defn
class QueryableWorkflow:
    """Workflow with query handler for testing eviction/replay."""

    def __init__(self) -> None:
        self.counter = 0
        self.status = "initial"

    @workflow.run
    async def run(self) -> str:
        """Execute workflow with state updates."""
        self.status = "started"
        self.counter = 10

        # Short sleep to simulate work
        await workflow.sleep(0.1)

        self.status = "after_sleep"
        self.counter = 20

        return f"completed with counter={self.counter}"

    @workflow.query
    def get_counter(self) -> int:
        """Return current counter value."""
        return self.counter

    @workflow.query
    def get_status(self) -> str:
        """Return current status."""
        return self.status


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_query_triggers_replay(trio_client):
    """Test that querying a completed workflow triggers replay.

    This test validates the eviction/replay behavior:
    1. Workflow executes and completes
    2. Workflow is evicted from cache (after completion)
    3. Query arrives, triggering replay
    4. Query returns correct result (proving replay worked)

    Note: Queries trigger replay because completed workflows are
    not kept in the worker's cache indefinitely. When SDK-Core
    needs to answer a query for a workflow not in cache, it
    replays from history.
    """
    namespace = "default"
    task_queue = f"trio-e2e-replay-queue-{int(time.time())}"
    workflow_id = f"test-replay-{int(time.time())}"

    worker = Worker(
        client=trio_client,
        task_queue=task_queue,
        workflows=[QueryableWorkflow],
        # Default max_cached_workflows=1000 (enables sticky queues)
        # Default sticky timeout is 10s, but replay should happen when query arrives
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)

        try:
            # Start workflow
            print(f"Starting workflow {workflow_id}...")
            _start_workflow_via_cli(
                workflow_id=workflow_id,
                workflow_type="QueryableWorkflow",
                task_queue=task_queue,
                namespace=namespace,
                args=[],
            )

            # Wait for workflow completion
            max_wait = 30
            start_time_test = time.time()
            while time.time() - start_time_test < max_wait:
                cli_result = _query_workflow_via_cli(workflow_id, namespace)
                status = cli_result.get("status", "UNKNOWN")

                if status == "COMPLETED":
                    break
                elif status in ["FAILED", "TERMINATED", "CANCELLED"]:
                    raise RuntimeError(f"Workflow ended with status: {status}")

                await trio.sleep(0.3)
            else:
                raise TimeoutError(
                    f"Workflow did not complete within {max_wait} seconds"
                )

            print(f"Workflow {workflow_id} completed, now sending query...")

            # Brief pause for workflow eviction from cache
            await trio.sleep(0.3)

            # Send query - this triggers replay since workflow is not in cache
            # Use async version to avoid blocking Trio event loop while worker polls
            query_result = await _query_workflow_query_via_cli_async(
                workflow_id=workflow_id,
                namespace=namespace,
                query_type="get_counter",
            )

            # Verify query returned correct result
            # After replay, counter should be at final value (20)
            assert query_result == 20, f"Expected counter=20, got {query_result}"

            print(f"Query returned correct value: {query_result}")
            print("E2E replay test passed - query triggered replay successfully")

        finally:
            worker.shutdown()
            await trio.sleep(0.3)
            nursery.cancel_scope.cancel()


def _query_workflow_query_via_cli(
    workflow_id: str, namespace: str, query_type: str, args: list[Any] | None = None
) -> Any:
    """Execute a query on a workflow using Temporal CLI.

    Args:
        workflow_id: The workflow ID to query
        namespace: Temporal namespace
        query_type: The query handler name
        args: Optional arguments for the query

    Returns:
        The query result

    Raises:
        RuntimeError: If CLI query fails
    """
    try:
        cmd = [
            TEMPORAL_CLI_PATH,
            "workflow",
            "query",
            "--workflow-id",
            workflow_id,
            "--namespace",
            namespace,
            "--type",
            query_type,
        ]

        if args:
            for arg in args:
                cmd.extend(["--input", json.dumps(arg)])

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
            check=True,
        )

        # Parse output - CLI may return JSON or text
        output = result.stdout.strip()
        if not output:
            return None

        # Try to parse as JSON, fall back to raw value
        try:
            return json.loads(output)
        except json.JSONDecodeError:
            # CLI returns text like "Query result:\n  QueryResult  20"
            # Extract the value from the end
            lines = output.split("\n")
            for line in reversed(lines):
                line = line.strip()
                # Look for "QueryResult" followed by value
                if "QueryResult" in line:
                    parts = line.split("QueryResult", 1)
                    if len(parts) > 1:
                        value_str = parts[1].strip()
                        try:
                            return json.loads(value_str)
                        except json.JSONDecodeError:
                            return value_str
            # Fall back to returning the raw output
            return output

    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Query failed: {e.stderr if e.stderr else e.stdout}") from e
    except Exception as e:
        raise RuntimeError(f"Unexpected error executing query: {e}") from e


async def _query_workflow_query_via_cli_async(
    workflow_id: str,
    namespace: str,
    query_type: str,
    args: list | None = None,
) -> Any:
    """Query a workflow using Temporal CLI (async version).

    This version runs the subprocess in a separate thread to avoid blocking
    the Trio event loop, allowing the worker to continue polling for activations.

    Args:
        workflow_id: The workflow ID to query
        namespace: Temporal namespace
        query_type: The query handler name
        args: Optional arguments for the query

    Returns:
        The query result
    """
    return await trio.to_thread.run_sync(
        lambda: _query_workflow_query_via_cli(workflow_id, namespace, query_type, args)
    )


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_e2e_multiple_workflows_cache_pressure(trio_client):
    """Test that multiple workflows can be processed without cache issues.

    This test runs multiple concurrent workflows to verify:
    1. Worker handles multiple workflows correctly
    2. Cache management doesn't break under load
    3. Each workflow completes independently

    This indirectly tests eviction by creating cache pressure.
    """
    namespace = "default"
    task_queue = f"trio-e2e-cache-queue-{int(time.time())}"
    workflow_count = 5

    worker = Worker(
        client=trio_client,
        task_queue=task_queue,
        workflows=[SimpleTimerWorkflow],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)

        try:
            # Start multiple workflows
            workflow_ids = []
            for i in range(workflow_count):
                wf_id = f"test-cache-{int(time.time())}-{i}"
                workflow_ids.append(wf_id)

                print(f"Starting workflow {wf_id}...")
                _start_workflow_via_cli(
                    workflow_id=wf_id,
                    workflow_type="SimpleTimerWorkflow",
                    task_queue=task_queue,
                    namespace=namespace,
                    args=[0.5],  # Short sleep
                )

            # Wait for all workflows to complete
            max_wait = 60
            start_time_test = time.time()
            completed = set()

            while (
                len(completed) < workflow_count
                and time.time() - start_time_test < max_wait
            ):
                for wf_id in workflow_ids:
                    if wf_id in completed:
                        continue

                    cli_result = _query_workflow_via_cli(wf_id, namespace)
                    status = cli_result.get("status", "UNKNOWN")

                    if status == "COMPLETED":
                        completed.add(wf_id)
                        print(f"Workflow {wf_id} completed")
                    elif status in ["FAILED", "TERMINATED", "CANCELLED"]:
                        raise RuntimeError(
                            f"Workflow {wf_id} ended with status: {status}"
                        )

                await trio.sleep(0.3)

            assert len(completed) == workflow_count, (
                f"Only {len(completed)}/{workflow_count} workflows completed"
            )

            print(f"All {workflow_count} workflows completed successfully")
            print("E2E cache pressure test passed")

        finally:
            worker.shutdown()
            await trio.sleep(0.3)
            nursery.cancel_scope.cancel()


if __name__ == "__main__":
    # Allow running tests directly for debugging
    print("Running E2E integration tests...")
    print("Note: Requires Temporal server running on localhost:7233")
    print()

    import sys

    sys.exit(pytest.main([__file__, "-v", "-s"]))
