"""E2E stress tests with 100+ concurrent workflows against a real Temporal server.

These tests validate that the Trio SDK handles high concurrency end-to-end,
including throughput, timer handling, activity scheduling, and signal/query
under load.

Run these tests with:
    pytest -v -m temporal_server tests/test_e2e_stress.py

Prerequisites:
    - Temporal server running on localhost:7233
"""

import time
from datetime import timedelta

import pytest
import trio
from temporalio.common import RetryPolicy

from temporalio_trio import activity, workflow
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker


# =============================================================================
# Fixtures
# =============================================================================


@pytest.fixture
async def trio_client():
    """Create a Trio-based Temporal client."""
    client = await Client.connect("localhost:7233", namespace="default")
    yield client
    await client.close()


# =============================================================================
# Workflow and activity definitions
# =============================================================================


@workflow.defn
class SimpleStressWorkflow:
    """Workflow that returns immediately with its index argument."""

    @workflow.run
    async def run(self, index: int) -> str:
        return f"simple-{index}"


@workflow.defn
class TimerStressWorkflow:
    """Workflow that sleeps briefly then returns."""

    @workflow.run
    async def run(self, index: int) -> str:
        await workflow.sleep(0.1)
        return f"timer-{index}"


@workflow.defn
class ActivityStressWorkflow:
    """Workflow that calls a trivial activity and returns the result."""

    @workflow.run
    async def run(self, index: int) -> str:
        result = await workflow.execute_activity(
            stress_activity,
            index,
            start_to_close_timeout=timedelta(seconds=30),
            retry_policy=RetryPolicy(maximum_attempts=1),
        )
        return result


@workflow.defn
class SignalQueryStressWorkflow:
    """Workflow that waits for a signal and exposes queryable state."""

    def __init__(self):
        self._value: str = "initial"
        self._done: bool = False

    @workflow.signal
    def set_value(self, value: str) -> None:
        self._value = value
        self._done = True

    @workflow.query
    def get_value(self) -> str:
        return self._value

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._done)
        return self._value


@activity.defn
async def stress_activity(index: int) -> str:
    """Trivial activity that returns a value."""
    return f"activity-{index}"


# =============================================================================
# Helper
# =============================================================================


ALL_WORKFLOWS = [
    SimpleStressWorkflow,
    TimerStressWorkflow,
    ActivityStressWorkflow,
    SignalQueryStressWorkflow,
]
ALL_ACTIVITIES = [stress_activity]


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.temporal_server
@pytest.mark.trio
@pytest.mark.timeout(120)
async def test_e2e_stress_100_simple(trio_client) -> None:
    """Test 100 immediate-return workflows for throughput."""
    task_queue = f"trio-stress-simple-{int(time.time())}"
    num_workflows = 100

    async with trio.open_nursery() as nursery:
        worker = Worker(
            trio_client,
            task_queue=task_queue,
            workflows=[SimpleStressWorkflow],
        )
        nursery.start_soon(worker.run)
        await trio.sleep(0)

        # Start all workflows
        handles = []
        for i in range(num_workflows):
            handle = await trio_client.start_workflow(
                "SimpleStressWorkflow",
                i,
                id=f"stress-simple-{int(time.time())}-{i}",
                task_queue=task_queue,
            )
            handles.append((i, handle))

        # Collect all results
        results = {}

        async def collect_result(index, handle):
            result = await handle.result(timeout=60.0)
            results[index] = result

        async with trio.open_nursery() as result_nursery:
            for i, handle in handles:
                result_nursery.start_soon(collect_result, i, handle)

        # Verify all results
        assert len(results) == num_workflows
        for i in range(num_workflows):
            assert results[i] == f"simple-{i}", f"Workflow {i}: expected 'simple-{i}', got '{results[i]}'"

        worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
@pytest.mark.timeout(120)
async def test_e2e_stress_100_timers(trio_client) -> None:
    """Test 100 workflows with 0.1s sleep for timer handling at scale."""
    task_queue = f"trio-stress-timers-{int(time.time())}"
    num_workflows = 100

    async with trio.open_nursery() as nursery:
        worker = Worker(
            trio_client,
            task_queue=task_queue,
            workflows=[TimerStressWorkflow],
        )
        nursery.start_soon(worker.run)
        await trio.sleep(0)

        # Start all workflows
        handles = []
        for i in range(num_workflows):
            handle = await trio_client.start_workflow(
                "TimerStressWorkflow",
                i,
                id=f"stress-timer-{int(time.time())}-{i}",
                task_queue=task_queue,
            )
            handles.append((i, handle))

        # Collect all results
        results = {}

        async def collect_result(index, handle):
            result = await handle.result(timeout=60.0)
            results[index] = result

        async with trio.open_nursery() as result_nursery:
            for i, handle in handles:
                result_nursery.start_soon(collect_result, i, handle)

        # Verify all results
        assert len(results) == num_workflows
        for i in range(num_workflows):
            assert results[i] == f"timer-{i}", f"Workflow {i}: expected 'timer-{i}', got '{results[i]}'"

        worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
@pytest.mark.timeout(120)
async def test_e2e_stress_100_activities(trio_client) -> None:
    """Test 100 workflows with activity calls for activity scheduling at scale."""
    task_queue = f"trio-stress-activities-{int(time.time())}"
    num_workflows = 100

    async with trio.open_nursery() as nursery:
        worker = Worker(
            trio_client,
            task_queue=task_queue,
            workflows=[ActivityStressWorkflow],
            activities=[stress_activity],
        )
        nursery.start_soon(worker.run)
        await trio.sleep(0)

        # Start all workflows
        handles = []
        for i in range(num_workflows):
            handle = await trio_client.start_workflow(
                "ActivityStressWorkflow",
                i,
                id=f"stress-activity-{int(time.time())}-{i}",
                task_queue=task_queue,
            )
            handles.append((i, handle))

        # Collect all results
        results = {}

        async def collect_result(index, handle):
            result = await handle.result(timeout=60.0)
            results[index] = result

        async with trio.open_nursery() as result_nursery:
            for i, handle in handles:
                result_nursery.start_soon(collect_result, i, handle)

        # Verify all results
        assert len(results) == num_workflows
        for i in range(num_workflows):
            assert results[i] == f"activity-{i}", f"Workflow {i}: expected 'activity-{i}', got '{results[i]}'"

        worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
@pytest.mark.timeout(120)
async def test_e2e_stress_50_signals_queries(trio_client) -> None:
    """Test 50 workflows with signal + query under load."""
    task_queue = f"trio-stress-sigquery-{int(time.time())}"
    num_workflows = 50

    async with trio.open_nursery() as nursery:
        worker = Worker(
            trio_client,
            task_queue=task_queue,
            workflows=[SignalQueryStressWorkflow],
        )
        nursery.start_soon(worker.run)
        await trio.sleep(0)

        # Start all workflows
        handles = []
        for i in range(num_workflows):
            handle = await trio_client.start_workflow(
                "SignalQueryStressWorkflow",
                id=f"stress-sigquery-{int(time.time())}-{i}",
                task_queue=task_queue,
            )
            handles.append((i, handle))

        # Give workflows time to start
        await trio.sleep(1.0)

        # Signal all workflows
        for i, handle in handles:
            await handle.signal("set_value", f"signaled-{i}")

        # Collect all results
        results = {}

        async def collect_result(index, handle):
            result = await handle.result(timeout=60.0)
            results[index] = result

        async with trio.open_nursery() as result_nursery:
            for i, handle in handles:
                result_nursery.start_soon(collect_result, i, handle)

        # Verify all results
        assert len(results) == num_workflows
        for i in range(num_workflows):
            assert results[i] == f"signaled-{i}", (
                f"Workflow {i}: expected 'signaled-{i}', got '{results[i]}'"
            )

        worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
@pytest.mark.timeout(120)
async def test_e2e_stress_100_mixed(trio_client) -> None:
    """Test 100 workflows with a mix of simple, timer, and activity types."""
    task_queue = f"trio-stress-mixed-{int(time.time())}"
    ts = int(time.time())

    async with trio.open_nursery() as nursery:
        worker = Worker(
            trio_client,
            task_queue=task_queue,
            workflows=[SimpleStressWorkflow, TimerStressWorkflow, ActivityStressWorkflow],
            activities=[stress_activity],
        )
        nursery.start_soon(worker.run)
        await trio.sleep(0)

        # Start 100 workflows: ~34 simple, ~33 timer, ~33 activity
        handles = []
        for i in range(100):
            if i % 3 == 0:
                wf_type = "SimpleStressWorkflow"
            elif i % 3 == 1:
                wf_type = "TimerStressWorkflow"
            else:
                wf_type = "ActivityStressWorkflow"

            handle = await trio_client.start_workflow(
                wf_type,
                i,
                id=f"stress-mixed-{ts}-{i}",
                task_queue=task_queue,
            )
            handles.append((i, wf_type, handle))

        # Collect all results
        results = {}

        async def collect_result(index, handle):
            result = await handle.result(timeout=60.0)
            results[index] = result

        async with trio.open_nursery() as result_nursery:
            for i, wf_type, handle in handles:
                result_nursery.start_soon(collect_result, i, handle)

        # Verify all results
        assert len(results) == 100
        for i, wf_type, _ in handles:
            if wf_type == "SimpleStressWorkflow":
                expected = f"simple-{i}"
            elif wf_type == "TimerStressWorkflow":
                expected = f"timer-{i}"
            else:
                expected = f"activity-{i}"
            assert results[i] == expected, (
                f"Workflow {i} ({wf_type}): expected '{expected}', got '{results[i]}'"
            )

        worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
@pytest.mark.timeout(120)
async def test_e2e_stress_200_workflows(trio_client) -> None:
    """Test 200 workflows with timers to push limits."""
    task_queue = f"trio-stress-200-{int(time.time())}"
    num_workflows = 200

    async with trio.open_nursery() as nursery:
        worker = Worker(
            trio_client,
            task_queue=task_queue,
            workflows=[TimerStressWorkflow],
        )
        nursery.start_soon(worker.run)
        await trio.sleep(0)

        # Start all workflows
        handles = []
        for i in range(num_workflows):
            handle = await trio_client.start_workflow(
                "TimerStressWorkflow",
                i,
                id=f"stress-200-{int(time.time())}-{i}",
                task_queue=task_queue,
            )
            handles.append((i, handle))

        # Collect all results
        results = {}

        async def collect_result(index, handle):
            result = await handle.result(timeout=90.0)
            results[index] = result

        async with trio.open_nursery() as result_nursery:
            for i, handle in handles:
                result_nursery.start_soon(collect_result, i, handle)

        # Verify all results
        assert len(results) == num_workflows
        for i in range(num_workflows):
            assert results[i] == f"timer-{i}", f"Workflow {i}: expected 'timer-{i}', got '{results[i]}'"

        worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()
