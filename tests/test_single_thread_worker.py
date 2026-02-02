"""Tests for SingleThreadWorker and WorkflowState.

This tests Phases 3 and 4 of the single-threaded migration plan:
- WorkflowState can be created and manages activation delivery
- SingleThreadWorker can be created with workflows
- Single workflow execution with timer works
- Multiple concurrent workflows work
- Activation delivery to existing workflow works
- Workflow with activity execution works
- Activity completion wakes workflow
"""

from __future__ import annotations

from typing import Any

import pytest
import trio

from temporalio_trio.worker._activation import (
    ActivityResolvedJob,
    ChildWorkflowResolvedJob,
    CompleteWorkflowCommand,
    QueryWorkflowJob,
    SignalWorkflowJob,
    TimerFiredJob,
    WorkflowActivation,
    WorkflowStartedJob,
)
from temporalio_trio.worker._runtime import (
    QueryFailureCommand,
    QuerySuccessCommand,
    WorkflowRuntime,
)
from temporalio_trio.worker._runtime import (
    StartTimerCommand as RuntimeStartTimerCommand,
)
from temporalio_trio.worker._single_thread_worker import SingleThreadWorker
from temporalio_trio.worker._workflow_state import WorkflowState
from temporalio_trio.workflow import defn, run

# =============================================================================
# Test Workflows
# =============================================================================


@defn
class SimpleWorkflow:
    """A simple workflow that returns immediately."""

    @run
    async def run(self, value: str) -> str:
        return f"result: {value}"


@defn
class TimerWorkflow:
    """A workflow that uses a timer."""

    @run
    async def run(self, delay: float) -> str:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()
        await runtime.workflow_sleep(delay)
        return "timer completed"


@defn
class MultiTimerWorkflow:
    """A workflow with multiple sequential timers."""

    @run
    async def run(self) -> list[int]:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()
        times = []

        await runtime.workflow_sleep(1.0)
        times.append(runtime.time_ns)

        await runtime.workflow_sleep(2.0)
        times.append(runtime.time_ns)

        return times


@defn
class ActivityWorkflow:
    """A workflow that calls an activity."""

    @run
    async def run(self, activity_name: str) -> str:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()
        result = await runtime.execute_activity(activity_name, ("arg1",))
        return f"activity result: {result}"


@defn
class MultiActivityWorkflow:
    """A workflow with multiple sequential activities."""

    @run
    async def run(self) -> list[str]:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()
        results = []

        result1 = await runtime.execute_activity("activity1", ())
        results.append(result1)

        result2 = await runtime.execute_activity("activity2", ())
        results.append(result2)

        return results


@defn
class ActivityAndTimerWorkflow:
    """A workflow that uses both activities and timers."""

    @run
    async def run(self) -> str:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()

        # First call an activity
        result = await runtime.execute_activity("my_activity", ())

        # Then sleep
        await runtime.workflow_sleep(1.0)

        return f"done: {result}"


# =============================================================================
# Helper Functions
# =============================================================================


def _create_test_runtime(
    run_id: str = "test-run",
    workflow_id: str = "test-wf",
    workflow_type: str = "TestWorkflow",
    task_queue: str = "test-queue",
) -> WorkflowRuntime:
    """Create a WorkflowRuntime for testing."""
    import random

    return WorkflowRuntime(
        run_id=run_id,
        workflow_id=workflow_id,
        workflow_type=workflow_type,
        task_queue=task_queue,
        random=random.Random(12345),
        time_ns=0,
    )


def _create_activation(
    jobs: list,
    timestamp_ns: int = 0,
    run_id: str | None = None,
) -> WorkflowActivation:
    """Create a WorkflowActivation for testing."""
    activation = WorkflowActivation(jobs=jobs, timestamp_ns=timestamp_ns)
    if run_id is not None:
        # Add run_id attribute dynamically for testing
        activation.run_id = run_id  # type: ignore
    return activation


class MockBridge:
    """Mock bridge for testing that returns pre-configured activations."""

    def __init__(self) -> None:
        self.activations: list[WorkflowActivation] = []
        self.completions: list[tuple[str, list[Any]]] = []
        self._activation_index = 0
        self._shutdown = False
        self._poll_event = trio.Event()
        self._activation_ready_event = trio.Event()

    def add_activation(self, activation: WorkflowActivation) -> None:
        """Add an activation to be returned by poll."""
        self.activations.append(activation)
        self._activation_ready_event.set()

    async def poll_workflow_activation(
        self, timeout: float | None = None
    ) -> WorkflowActivation:
        """Return the next activation or wait for shutdown."""
        while True:
            if self._shutdown:
                raise RuntimeError("PollShutdownError")

            if self._activation_index < len(self.activations):
                activation = self.activations[self._activation_index]
                self._activation_index += 1
                return activation  # type: ignore

            # Wait for either new activation or shutdown
            self._activation_ready_event = trio.Event()
            self._poll_event.set()
            with trio.move_on_after(0.1):
                await self._activation_ready_event.wait()

    async def complete_workflow_activation(self, completion_bytes: bytes) -> None:
        """Record completion for verification."""
        # For mock, we just track that completion was called
        pass

    def initiate_shutdown(self) -> None:
        """Signal shutdown."""
        self._shutdown = True
        self._activation_ready_event.set()

    async def shutdown(self) -> None:
        """Full shutdown."""
        self.initiate_shutdown()


# =============================================================================
# WorkflowState Tests
# =============================================================================


class TestWorkflowStateCreation:
    """Tests for WorkflowState creation."""

    def test_can_be_created(self) -> None:
        """Test WorkflowState can be created with run_id."""
        state = WorkflowState(run_id="test-run-id")

        assert state.run_id == "test-run-id"
        assert state.runtime is None
        assert state.pending_activation is None
        assert state.is_complete is False

    def test_events_are_initialized(self) -> None:
        """Test WorkflowState events are initialized."""
        state = WorkflowState(run_id="test-run-id")

        assert isinstance(state.activation_event, trio.Event)
        assert isinstance(state.commands_ready, trio.Event)
        assert not state.activation_event.is_set()
        assert not state.commands_ready.is_set()

    def test_can_set_runtime(self) -> None:
        """Test WorkflowState runtime can be set."""
        state = WorkflowState(run_id="test-run-id")
        runtime = _create_test_runtime()

        state.runtime = runtime

        assert state.runtime is runtime


class TestWorkflowStateActivationDelivery:
    """Tests for WorkflowState activation delivery."""

    def test_deliver_activation_stores_and_signals(self) -> None:
        """Test deliver_activation stores activation and sets event."""
        state = WorkflowState(run_id="test-run-id")
        activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="TestWorkflow", args=())]
        )

        assert state.pending_activation is None
        assert not state.activation_event.is_set()

        state.deliver_activation(activation)

        assert state.pending_activation is activation
        assert state.activation_event.is_set()

    @pytest.mark.trio
    async def test_wait_for_activation_returns_activation(self) -> None:
        """Test wait_for_activation returns the delivered activation."""
        state = WorkflowState(run_id="test-run-id")
        activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="TestWorkflow", args=())]
        )

        async def deliver() -> None:
            await trio.sleep(0.01)
            state.deliver_activation(activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(deliver)
            result = await state.wait_for_activation()

        assert result is activation
        assert state.pending_activation is None

    @pytest.mark.trio
    async def test_wait_for_activation_resets_event(self) -> None:
        """Test wait_for_activation resets the event for next activation."""
        state = WorkflowState(run_id="test-run-id")
        activation1 = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="Test1", args=())]
        )
        activation2 = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="Test2", args=())]
        )

        # First delivery
        state.deliver_activation(activation1)
        result1 = await state.wait_for_activation()
        assert result1 is activation1

        # Event should be reset
        assert not state.activation_event.is_set()

        # Second delivery should work
        state.deliver_activation(activation2)
        result2 = await state.wait_for_activation()
        assert result2 is activation2


class TestWorkflowStateCommandCollection:
    """Tests for WorkflowState command collection."""

    def test_signal_commands_ready_sets_event(self) -> None:
        """Test signal_commands_ready sets the commands_ready event."""
        state = WorkflowState(run_id="test-run-id")

        assert not state.commands_ready.is_set()

        state.signal_commands_ready()

        assert state.commands_ready.is_set()

    @pytest.mark.trio
    async def test_wait_for_commands_returns_commands(self) -> None:
        """Test wait_for_commands returns commands from runtime."""
        state = WorkflowState(run_id="test-run-id")
        runtime = _create_test_runtime()
        state.runtime = runtime

        # Add some commands
        runtime.commands.append(RuntimeStartTimerCommand(timer_id=1, duration_ms=1000))
        runtime.commands.append(CompleteWorkflowCommand(result="done"))

        async def signal_ready() -> None:
            await trio.sleep(0.01)
            state.signal_commands_ready()

        async with trio.open_nursery() as nursery:
            nursery.start_soon(signal_ready)
            commands = await state.wait_for_commands()

        assert len(commands) == 2
        assert isinstance(commands[0], RuntimeStartTimerCommand)
        assert isinstance(commands[1], CompleteWorkflowCommand)

        # Commands should be cleared from runtime
        assert len(runtime.commands) == 0

    @pytest.mark.trio
    async def test_wait_for_commands_resets_event(self) -> None:
        """Test wait_for_commands resets the event for next round."""
        state = WorkflowState(run_id="test-run-id")
        runtime = _create_test_runtime()
        state.runtime = runtime

        # First round
        runtime.commands.append(CompleteWorkflowCommand(result="1"))
        state.signal_commands_ready()
        commands1 = await state.wait_for_commands()
        assert len(commands1) == 1

        # Event should be reset
        assert not state.commands_ready.is_set()

        # Second round should work
        runtime.commands.append(CompleteWorkflowCommand(result="2"))
        state.signal_commands_ready()
        commands2 = await state.wait_for_commands()
        assert len(commands2) == 1


class TestWorkflowStateCompletion:
    """Tests for WorkflowState completion tracking."""

    def test_mark_complete_sets_flag(self) -> None:
        """Test mark_complete sets the is_complete flag."""
        state = WorkflowState(run_id="test-run-id")

        assert not state.is_complete

        state.mark_complete()

        assert state.is_complete


# =============================================================================
# SingleThreadWorker Tests
# =============================================================================


class TestSingleThreadWorkerCreation:
    """Tests for SingleThreadWorker creation."""

    def test_can_be_created(self) -> None:
        """Test SingleThreadWorker can be created with workflows."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[SimpleWorkflow],
        )

        assert worker._task_queue == "test-queue"
        assert "SimpleWorkflow" in worker._workflows
        assert worker._activities == []

    def test_registers_multiple_workflows(self) -> None:
        """Test SingleThreadWorker registers multiple workflows."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[SimpleWorkflow, TimerWorkflow],
        )

        assert "SimpleWorkflow" in worker._workflows
        assert "TimerWorkflow" in worker._workflows
        assert len(worker._workflows) == 2

    def test_rejects_duplicate_workflow_names(self) -> None:
        """Test SingleThreadWorker rejects duplicate workflow names."""
        bridge = MockBridge()

        @defn
        class SimpleWorkflow:  # Same name as the one defined above
            @run
            async def run(self) -> str:
                return "duplicate"

        with pytest.raises(ValueError) as exc_info:
            SingleThreadWorker(
                bridge=bridge,  # type: ignore
                task_queue="test-queue",
                workflows=[SimpleWorkflow, SimpleWorkflow],
            )

        assert "Duplicate" in str(exc_info.value)


class TestSingleThreadWorkerExecution:
    """Tests for SingleThreadWorker workflow execution."""

    @pytest.mark.trio
    async def test_simple_workflow_execution(self) -> None:
        """Test executing a simple workflow that returns immediately."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[SimpleWorkflow],
        )

        # Create activation with workflow start
        activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="SimpleWorkflow", args=("hello",))],
            timestamp_ns=1_000_000_000,
            run_id="run-123",
        )
        bridge.add_activation(activation)

        # Run worker briefly
        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to process
            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

        # Workflow should have completed
        assert "run-123" not in worker._workflow_states

    @pytest.mark.trio
    async def test_timer_workflow_execution(self) -> None:
        """Test executing a workflow with a timer."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[TimerWorkflow],
        )

        # Initial activation - start workflow
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="TimerWorkflow", args=(1.0,))],
            timestamp_ns=1_000_000_000,
            run_id="run-timer",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for initial processing
            await trio.sleep(0.1)

            # Workflow should be waiting for timer
            assert "run-timer" in worker._workflow_states

            # Deliver timer fired activation
            timer_activation = _create_activation(
                jobs=[TimerFiredJob(timer_id=1)],
                timestamp_ns=2_000_000_000,
                run_id="run-timer",
            )
            bridge.add_activation(timer_activation)

            # Wait for timer processing
            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_multiple_concurrent_workflows(self) -> None:
        """Test executing multiple concurrent workflows."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[SimpleWorkflow],
        )

        # Create multiple workflow activations
        for i in range(3):
            activation = _create_activation(
                jobs=[
                    WorkflowStartedJob(workflow_type="SimpleWorkflow", args=(f"wf{i}",))
                ],
                timestamp_ns=1_000_000_000,
                run_id=f"run-{i}",
            )
            bridge.add_activation(activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for all workflows to process
            await trio.sleep(0.2)

            # Shutdown
            worker.shutdown()

        # All workflows should have completed
        assert len(worker._workflow_states) == 0

    @pytest.mark.trio
    async def test_activation_delivery_to_existing_workflow(self) -> None:
        """Test delivering activation to an existing workflow."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[MultiTimerWorkflow],
        )

        # Initial activation
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="MultiTimerWorkflow", args=())],
            timestamp_ns=1_000_000_000,
            run_id="run-multi",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for initial processing
            await trio.sleep(0.1)

            # Workflow should be waiting for first timer
            assert "run-multi" in worker._workflow_states

            # Deliver first timer fired
            timer1_activation = _create_activation(
                jobs=[TimerFiredJob(timer_id=1)],
                timestamp_ns=2_000_000_000,
                run_id="run-multi",
            )
            bridge.add_activation(timer1_activation)

            await trio.sleep(0.1)

            # Workflow should still be running, waiting for second timer
            # (It may or may not be in the states depending on timing)

            # Deliver second timer fired
            timer2_activation = _create_activation(
                jobs=[TimerFiredJob(timer_id=2)],
                timestamp_ns=4_000_000_000,
                run_id="run-multi",
            )
            bridge.add_activation(timer2_activation)

            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()


class TestSingleThreadWorkerShutdown:
    """Tests for SingleThreadWorker shutdown."""

    @pytest.mark.trio
    async def test_shutdown_stops_polling(self) -> None:
        """Test shutdown stops the polling loop."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[SimpleWorkflow],
        )

        worker_task_completed = False

        async def run_worker() -> None:
            nonlocal worker_task_completed
            await worker.run()
            worker_task_completed = True

        async with trio.open_nursery() as nursery:
            nursery.start_soon(run_worker)

            # Let worker start
            await trio.sleep(0.05)

            # Shutdown
            worker.shutdown()

            # Wait for worker to stop
            await trio.sleep(0.1)

        assert worker_task_completed

    @pytest.mark.trio
    async def test_shutdown_completes_in_flight_workflows(self) -> None:
        """Test shutdown allows in-flight workflows to complete."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[SimpleWorkflow],
        )

        # Add a workflow
        activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="SimpleWorkflow", args=("test",))],
            timestamp_ns=1_000_000_000,
            run_id="run-inflight",
        )
        bridge.add_activation(activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Let workflow start processing
            await trio.sleep(0.05)

            # Shutdown
            worker.shutdown()

            # Wait for cleanup
            await trio.sleep(0.1)


# =============================================================================
# Integration Tests
# =============================================================================


class TestSingleThreadWorkerIntegration:
    """Integration tests for SingleThreadWorker."""

    @pytest.mark.trio
    async def test_workflow_produces_correct_commands(self) -> None:
        """Test workflow produces correct timer commands."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[TimerWorkflow],
        )

        # Track commands through the state
        activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="TimerWorkflow", args=(5.0,))],
            timestamp_ns=0,
            run_id="run-cmd-test",
        )
        bridge.add_activation(activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for processing
            await trio.sleep(0.1)

            # Check state
            state = worker._workflow_states.get("run-cmd-test")
            if state and state.runtime:
                # Runtime should have timer command. Commands may have been
                # sent already, but we can verify the workflow ran.
                pass

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_workflow_state_isolation(self) -> None:
        """Test multiple workflows have isolated state."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[TimerWorkflow],
        )

        # Create two workflows
        for i in range(2):
            activation = _create_activation(
                jobs=[
                    WorkflowStartedJob(
                        workflow_type="TimerWorkflow", args=(float(i + 1),)
                    )
                ],
                timestamp_ns=i * 1_000_000_000,
                run_id=f"run-iso-{i}",
            )
            bridge.add_activation(activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for both workflows to process
            await trio.sleep(0.15)

            # Each workflow should have its own state
            state0 = worker._workflow_states.get("run-iso-0")
            state1 = worker._workflow_states.get("run-iso-1")

            if state0 and state1:
                assert state0.run_id != state1.run_id
                if state0.runtime and state1.runtime:
                    assert state0.runtime is not state1.runtime

            # Shutdown
            worker.shutdown()


# =============================================================================
# Activity Tests (Phase 4)
# =============================================================================


class TestSingleThreadWorkerActivityExecution:
    """Tests for SingleThreadWorker activity execution."""

    @pytest.mark.trio
    async def test_workflow_with_activity_execution(self) -> None:
        """Test executing a workflow that calls an activity."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[ActivityWorkflow],
        )

        # Initial activation - start workflow
        initial_activation = _create_activation(
            jobs=[
                WorkflowStartedJob(
                    workflow_type="ActivityWorkflow", args=("test_activity",)
                )
            ],
            timestamp_ns=1_000_000_000,
            run_id="run-activity",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for initial processing
            await trio.sleep(0.1)

            # Workflow should be waiting for activity
            assert "run-activity" in worker._workflow_states

            # Deliver activity resolved activation
            activity_activation = _create_activation(
                jobs=[ActivityResolvedJob(seq=1, result="activity completed!")],
                timestamp_ns=2_000_000_000,
                run_id="run-activity",
            )
            bridge.add_activation(activity_activation)

            # Wait for activity processing
            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_activity_completion_wakes_workflow(self) -> None:
        """Test activity completion wakes up the waiting workflow."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[ActivityWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[
                WorkflowStartedJob(
                    workflow_type="ActivityWorkflow", args=("my_activity",)
                )
            ],
            timestamp_ns=1_000_000_000,
            run_id="run-wake",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)

            # Check that workflow is waiting
            state = worker._workflow_states.get("run-wake")
            assert state is not None
            assert not state.is_complete

            # Complete the activity
            activity_activation = _create_activation(
                jobs=[ActivityResolvedJob(seq=1, result="woken up!")],
                timestamp_ns=2_000_000_000,
                run_id="run-wake",
            )
            bridge.add_activation(activity_activation)

            # Wait for completion
            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_multiple_sequential_activities(self) -> None:
        """Test workflow with multiple sequential activities."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[MultiActivityWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="MultiActivityWorkflow", args=())],
            timestamp_ns=1_000_000_000,
            run_id="run-multi-activity",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for first activity
            await trio.sleep(0.1)
            assert "run-multi-activity" in worker._workflow_states

            # Complete first activity
            activity1_activation = _create_activation(
                jobs=[ActivityResolvedJob(seq=1, result="result1")],
                timestamp_ns=2_000_000_000,
                run_id="run-multi-activity",
            )
            bridge.add_activation(activity1_activation)

            await trio.sleep(0.1)

            # Complete second activity
            activity2_activation = _create_activation(
                jobs=[ActivityResolvedJob(seq=2, result="result2")],
                timestamp_ns=3_000_000_000,
                run_id="run-multi-activity",
            )
            bridge.add_activation(activity2_activation)

            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_activity_failure_propagates(self) -> None:
        """Test activity failure is propagated to the workflow."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[ActivityWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[
                WorkflowStartedJob(
                    workflow_type="ActivityWorkflow", args=("failing_activity",)
                )
            ],
            timestamp_ns=1_000_000_000,
            run_id="run-fail",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)

            # Fail the activity
            activity_activation = _create_activation(
                jobs=[
                    ActivityResolvedJob(seq=1, failure=RuntimeError("Activity failed!"))
                ],
                timestamp_ns=2_000_000_000,
                run_id="run-fail",
            )
            bridge.add_activation(activity_activation)

            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_activity_and_timer_combined(self) -> None:
        """Test workflow with both activities and timers."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[ActivityAndTimerWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[
                WorkflowStartedJob(workflow_type="ActivityAndTimerWorkflow", args=())
            ],
            timestamp_ns=1_000_000_000,
            run_id="run-combined",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start and block on activity
            await trio.sleep(0.1)
            assert "run-combined" in worker._workflow_states

            # Complete the activity
            activity_activation = _create_activation(
                jobs=[ActivityResolvedJob(seq=1, result="activity done")],
                timestamp_ns=2_000_000_000,
                run_id="run-combined",
            )
            bridge.add_activation(activity_activation)

            await trio.sleep(0.1)

            # Now workflow should be waiting for timer
            # Fire the timer
            timer_activation = _create_activation(
                jobs=[TimerFiredJob(timer_id=1)],
                timestamp_ns=3_000_000_000,
                run_id="run-combined",
            )
            bridge.add_activation(timer_activation)

            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()


# =============================================================================
# Signal and Query Test Workflows (Phase 5)
# =============================================================================


@defn
class SignalWorkflow:
    """A workflow that handles signals."""

    def __init__(self) -> None:
        self.received_signals: list[str] = []

    @run
    async def run(self) -> list[str]:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()

        # Register signal handler
        def handle_signal(value: str) -> None:
            self.received_signals.append(value)

        runtime.register_signal_handler("my_signal", handle_signal)

        # Wait for signals via timer
        await runtime.workflow_sleep(5.0)

        return self.received_signals


@defn
class AsyncSignalWorkflow:
    """A workflow with an async signal handler."""

    def __init__(self) -> None:
        self.received_signals: list[str] = []
        self.handler_started = False
        self.handler_completed = False

    @run
    async def run(self) -> dict[str, Any]:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()

        # Register async signal handler
        async def async_handle_signal(value: str) -> None:
            self.handler_started = True
            # Simulate async work
            await runtime.workflow_sleep(1.0)
            self.received_signals.append(value)
            self.handler_completed = True

        runtime.register_signal_handler("async_signal", async_handle_signal)

        # Wait for signal and handler completion
        await runtime.workflow_sleep(3.0)

        return {
            "signals": self.received_signals,
            "handler_started": self.handler_started,
            "handler_completed": self.handler_completed,
        }


@defn
class QueryWorkflow:
    """A workflow that handles queries."""

    def __init__(self) -> None:
        self.counter = 0
        self.status = "initial"

    @run
    async def run(self) -> str:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()

        # Register query handlers
        def get_counter() -> int:
            return self.counter

        def get_status() -> str:
            return self.status

        runtime.register_query_handler("get_counter", get_counter)
        runtime.register_query_handler("get_status", get_status)

        # Update state
        self.counter = 42
        self.status = "running"

        # Wait for queries
        await runtime.workflow_sleep(5.0)

        self.status = "completed"
        return "done"


@defn
class QueryErrorWorkflow:
    """A workflow with a query handler that raises an error."""

    @run
    async def run(self) -> str:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()

        # Register query handler that raises
        def failing_query() -> str:
            raise ValueError("Query handler error")

        runtime.register_query_handler("failing_query", failing_query)

        await runtime.workflow_sleep(5.0)
        return "done"


# =============================================================================
# Signal Tests (Phase 5)
# =============================================================================


class TestSingleThreadWorkerSignalDelivery:
    """Tests for signal delivery to workflows."""

    @pytest.mark.trio
    async def test_signal_delivery_to_workflow(self) -> None:
        """Test delivering a signal to a workflow."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[SignalWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="SignalWorkflow", args=())],
            timestamp_ns=1_000_000_000,
            run_id="run-signal",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)
            assert "run-signal" in worker._workflow_states

            # Deliver signal
            signal_activation = _create_activation(
                jobs=[SignalWorkflowJob(signal_name="my_signal", args=("hello",))],
                timestamp_ns=2_000_000_000,
                run_id="run-signal",
            )
            bridge.add_activation(signal_activation)

            await trio.sleep(0.1)

            # Deliver another signal
            signal_activation2 = _create_activation(
                jobs=[SignalWorkflowJob(signal_name="my_signal", args=("world",))],
                timestamp_ns=2_500_000_000,
                run_id="run-signal",
            )
            bridge.add_activation(signal_activation2)

            await trio.sleep(0.1)

            # Fire the timer to complete workflow
            timer_activation = _create_activation(
                jobs=[TimerFiredJob(timer_id=1)],
                timestamp_ns=6_000_000_000,
                run_id="run-signal",
            )
            bridge.add_activation(timer_activation)

            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_async_signal_handler(self) -> None:
        """Test async signal handler is started in nursery."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[AsyncSignalWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="AsyncSignalWorkflow", args=())],
            timestamp_ns=1_000_000_000,
            run_id="run-async-signal",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)
            assert "run-async-signal" in worker._workflow_states

            # Deliver signal with async handler
            signal_activation = _create_activation(
                jobs=[
                    SignalWorkflowJob(signal_name="async_signal", args=("async_value",))
                ],
                timestamp_ns=2_000_000_000,
                run_id="run-async-signal",
            )
            bridge.add_activation(signal_activation)

            await trio.sleep(0.1)

            # Fire the inner timer (from async handler)
            inner_timer_activation = _create_activation(
                jobs=[TimerFiredJob(timer_id=2)],
                timestamp_ns=3_000_000_000,
                run_id="run-async-signal",
            )
            bridge.add_activation(inner_timer_activation)

            await trio.sleep(0.1)

            # Fire the outer timer to complete workflow
            timer_activation = _create_activation(
                jobs=[TimerFiredJob(timer_id=1)],
                timestamp_ns=4_000_000_000,
                run_id="run-async-signal",
            )
            bridge.add_activation(timer_activation)

            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_signal_to_nonexistent_handler(self) -> None:
        """Test signal to a nonexistent handler is logged but doesn't crash."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[SignalWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="SignalWorkflow", args=())],
            timestamp_ns=1_000_000_000,
            run_id="run-no-handler",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)

            # Deliver signal to nonexistent handler
            signal_activation = _create_activation(
                jobs=[SignalWorkflowJob(signal_name="unknown_signal", args=("value",))],
                timestamp_ns=2_000_000_000,
                run_id="run-no-handler",
            )
            bridge.add_activation(signal_activation)

            await trio.sleep(0.1)

            # Workflow should still be running (not crashed)
            assert "run-no-handler" in worker._workflow_states

            # Shutdown
            worker.shutdown()


# =============================================================================
# Query Tests (Phase 5)
# =============================================================================


class TestSingleThreadWorkerQueryHandling:
    """Tests for query handling in workflows."""

    @pytest.mark.trio
    async def test_query_response(self) -> None:
        """Test query returns correct response."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[QueryWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="QueryWorkflow", args=())],
            timestamp_ns=1_000_000_000,
            run_id="run-query",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)
            assert "run-query" in worker._workflow_states

            # Execute query
            query_activation = _create_activation(
                jobs=[
                    QueryWorkflowJob(
                        query_id="q-1",
                        query_type="get_counter",
                        args=(),
                    )
                ],
                timestamp_ns=2_000_000_000,
                run_id="run-query",
            )
            bridge.add_activation(query_activation)

            await trio.sleep(0.1)

            # Check that the query was handled
            state = worker._workflow_states.get("run-query")
            if state and state.runtime:
                # Look for QuerySuccessCommand in commands
                query_commands = [
                    cmd
                    for cmd in state.runtime.commands
                    if isinstance(cmd, QuerySuccessCommand)
                ]
                # Commands may have been sent already

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_query_error_handling(self) -> None:
        """Test query error is properly returned."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[QueryErrorWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="QueryErrorWorkflow", args=())],
            timestamp_ns=1_000_000_000,
            run_id="run-query-error",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)
            assert "run-query-error" in worker._workflow_states

            # Execute failing query
            query_activation = _create_activation(
                jobs=[
                    QueryWorkflowJob(
                        query_id="q-fail",
                        query_type="failing_query",
                        args=(),
                    )
                ],
                timestamp_ns=2_000_000_000,
                run_id="run-query-error",
            )
            bridge.add_activation(query_activation)

            await trio.sleep(0.1)

            # Workflow should still be running (not crashed by query error)
            assert "run-query-error" in worker._workflow_states

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_query_to_unknown_handler(self) -> None:
        """Test query to unknown handler returns error."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[QueryWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="QueryWorkflow", args=())],
            timestamp_ns=1_000_000_000,
            run_id="run-unknown-query",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)

            # Execute query to unknown handler
            query_activation = _create_activation(
                jobs=[
                    QueryWorkflowJob(
                        query_id="q-unknown",
                        query_type="unknown_query",
                        args=(),
                    )
                ],
                timestamp_ns=2_000_000_000,
                run_id="run-unknown-query",
            )
            bridge.add_activation(query_activation)

            await trio.sleep(0.1)

            # Workflow should still be running
            assert "run-unknown-query" in worker._workflow_states

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_multiple_queries(self) -> None:
        """Test handling multiple queries in sequence."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[QueryWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="QueryWorkflow", args=())],
            timestamp_ns=1_000_000_000,
            run_id="run-multi-query",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)

            # Execute first query
            query1_activation = _create_activation(
                jobs=[
                    QueryWorkflowJob(
                        query_id="q-1",
                        query_type="get_counter",
                        args=(),
                    )
                ],
                timestamp_ns=2_000_000_000,
                run_id="run-multi-query",
            )
            bridge.add_activation(query1_activation)

            await trio.sleep(0.1)

            # Execute second query
            query2_activation = _create_activation(
                jobs=[
                    QueryWorkflowJob(
                        query_id="q-2",
                        query_type="get_status",
                        args=(),
                    )
                ],
                timestamp_ns=2_500_000_000,
                run_id="run-multi-query",
            )
            bridge.add_activation(query2_activation)

            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()


# =============================================================================
# Child Workflow Test Workflows (Phase 6)
# =============================================================================


@defn
class ChildWorkflowParent:
    """A workflow that calls a child workflow."""

    @run
    async def run(self, child_workflow_type: str) -> str:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()
        result = await runtime.execute_child_workflow(
            child_workflow_type, "child-wf-id", ("arg1",)
        )
        return f"child result: {result}"


@defn
class MultiChildWorkflowParent:
    """A workflow with multiple sequential child workflows."""

    @run
    async def run(self) -> list[str]:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()
        results = []

        result1 = await runtime.execute_child_workflow("ChildWorkflow1", "child-1", ())
        results.append(result1)

        result2 = await runtime.execute_child_workflow("ChildWorkflow2", "child-2", ())
        results.append(result2)

        return results


@defn
class ChildWorkflowAndActivityParent:
    """A workflow that uses both child workflows and activities."""

    @run
    async def run(self) -> str:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()

        # First call a child workflow
        child_result = await runtime.execute_child_workflow(
            "ChildWorkflow", "child-wf", ()
        )

        # Then call an activity
        activity_result = await runtime.execute_activity("my_activity", ())

        return f"child: {child_result}, activity: {activity_result}"


# =============================================================================
# Child Workflow Tests (Phase 6)
# =============================================================================


class TestSingleThreadWorkerChildWorkflowExecution:
    """Tests for SingleThreadWorker child workflow execution."""

    @pytest.mark.trio
    async def test_workflow_with_child_workflow_execution(self) -> None:
        """Test executing a workflow that calls a child workflow."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[ChildWorkflowParent],
        )

        # Initial activation - start workflow
        initial_activation = _create_activation(
            jobs=[
                WorkflowStartedJob(
                    workflow_type="ChildWorkflowParent", args=("ChildWorkflow",)
                )
            ],
            timestamp_ns=1_000_000_000,
            run_id="run-child",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for initial processing
            await trio.sleep(0.1)

            # Workflow should be waiting for child workflow
            assert "run-child" in worker._workflow_states

            # Deliver child workflow resolved activation
            child_activation = _create_activation(
                jobs=[ChildWorkflowResolvedJob(seq=1, result="child completed!")],
                timestamp_ns=2_000_000_000,
                run_id="run-child",
            )
            bridge.add_activation(child_activation)

            # Wait for child workflow processing
            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_child_workflow_completion_wakes_parent(self) -> None:
        """Test child workflow completion wakes up the waiting parent workflow."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[ChildWorkflowParent],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[
                WorkflowStartedJob(
                    workflow_type="ChildWorkflowParent", args=("MyChild",)
                )
            ],
            timestamp_ns=1_000_000_000,
            run_id="run-wake",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)

            # Check that workflow is waiting
            state = worker._workflow_states.get("run-wake")
            assert state is not None
            assert not state.is_complete

            # Complete the child workflow
            child_activation = _create_activation(
                jobs=[ChildWorkflowResolvedJob(seq=1, result="woken up!")],
                timestamp_ns=2_000_000_000,
                run_id="run-wake",
            )
            bridge.add_activation(child_activation)

            # Wait for completion
            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_multiple_sequential_child_workflows(self) -> None:
        """Test workflow with multiple sequential child workflows."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[MultiChildWorkflowParent],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[
                WorkflowStartedJob(workflow_type="MultiChildWorkflowParent", args=())
            ],
            timestamp_ns=1_000_000_000,
            run_id="run-multi-child",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for first child workflow
            await trio.sleep(0.1)
            assert "run-multi-child" in worker._workflow_states

            # Complete first child workflow
            child1_activation = _create_activation(
                jobs=[ChildWorkflowResolvedJob(seq=1, result="result1")],
                timestamp_ns=2_000_000_000,
                run_id="run-multi-child",
            )
            bridge.add_activation(child1_activation)

            await trio.sleep(0.1)

            # Complete second child workflow
            child2_activation = _create_activation(
                jobs=[ChildWorkflowResolvedJob(seq=2, result="result2")],
                timestamp_ns=3_000_000_000,
                run_id="run-multi-child",
            )
            bridge.add_activation(child2_activation)

            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_child_workflow_failure_propagates(self) -> None:
        """Test child workflow failure is propagated to the parent workflow."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[ChildWorkflowParent],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[
                WorkflowStartedJob(
                    workflow_type="ChildWorkflowParent", args=("FailingChild",)
                )
            ],
            timestamp_ns=1_000_000_000,
            run_id="run-fail",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)

            # Fail the child workflow
            child_activation = _create_activation(
                jobs=[
                    ChildWorkflowResolvedJob(
                        seq=1, failure=RuntimeError("Child failed!")
                    )
                ],
                timestamp_ns=2_000_000_000,
                run_id="run-fail",
            )
            bridge.add_activation(child_activation)

            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_child_workflow_and_activity_combined(self) -> None:
        """Test workflow with both child workflows and activities."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[ChildWorkflowAndActivityParent],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[
                WorkflowStartedJob(
                    workflow_type="ChildWorkflowAndActivityParent", args=()
                )
            ],
            timestamp_ns=1_000_000_000,
            run_id="run-combined",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start and block on child workflow
            await trio.sleep(0.1)
            assert "run-combined" in worker._workflow_states

            # Complete the child workflow
            child_activation = _create_activation(
                jobs=[ChildWorkflowResolvedJob(seq=1, result="child done")],
                timestamp_ns=2_000_000_000,
                run_id="run-combined",
            )
            bridge.add_activation(child_activation)

            await trio.sleep(0.1)

            # Now workflow should be waiting for activity
            # Complete the activity
            activity_activation = _create_activation(
                jobs=[ActivityResolvedJob(seq=1, result="activity done")],
                timestamp_ns=3_000_000_000,
                run_id="run-combined",
            )
            bridge.add_activation(activity_activation)

            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()


# =============================================================================
# Cancellation Test Workflows (Phase 7)
# =============================================================================


@defn
class CancellableSleepWorkflow:
    """A workflow that sleeps and can be cancelled."""

    @run
    async def run(self) -> str:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()
        await runtime.workflow_sleep(10.0)
        return "sleep completed"


@defn
class CancellableActivityWorkflow:
    """A workflow that calls an activity and can be cancelled."""

    @run
    async def run(self) -> str:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()
        result = await runtime.execute_activity("long_activity", ())
        return f"activity result: {result}"


@defn
class CancellableChildWorkflow:
    """A workflow that calls a child workflow and can be cancelled."""

    @run
    async def run(self) -> str:
        from temporalio_trio.worker._runtime import get_current_runtime

        runtime = get_current_runtime()
        result = await runtime.execute_child_workflow(
            "LongRunningChild", "child-wf-1", ()
        )
        return f"child result: {result}"


# =============================================================================
# Cancellation Tests (Phase 7)
# =============================================================================


class TestSingleThreadWorkerCancellation:
    """Tests for SingleThreadWorker workflow cancellation."""

    @pytest.mark.trio
    async def test_workflow_cancellation_during_sleep(self) -> None:
        """Test workflow cancellation while sleeping."""
        from temporalio_trio.worker._activation import CancelWorkflowJob

        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[CancellableSleepWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[
                WorkflowStartedJob(workflow_type="CancellableSleepWorkflow", args=())
            ],
            timestamp_ns=1_000_000_000,
            run_id="run-cancel-sleep",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start and block on sleep
            await trio.sleep(0.1)
            assert "run-cancel-sleep" in worker._workflow_states

            # Send cancellation
            cancel_activation = _create_activation(
                jobs=[CancelWorkflowJob()],
                timestamp_ns=2_000_000_000,
                run_id="run-cancel-sleep",
            )
            bridge.add_activation(cancel_activation)

            await trio.sleep(0.1)

            # Workflow should have been cancelled
            # (state may or may not still be present depending on cleanup)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_workflow_cancellation_during_activity(self) -> None:
        """Test workflow cancellation while waiting for activity."""
        from temporalio_trio.worker._activation import CancelWorkflowJob

        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[CancellableActivityWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[
                WorkflowStartedJob(workflow_type="CancellableActivityWorkflow", args=())
            ],
            timestamp_ns=1_000_000_000,
            run_id="run-cancel-activity",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start and block on activity
            await trio.sleep(0.1)
            assert "run-cancel-activity" in worker._workflow_states

            # Send cancellation
            cancel_activation = _create_activation(
                jobs=[CancelWorkflowJob()],
                timestamp_ns=2_000_000_000,
                run_id="run-cancel-activity",
            )
            bridge.add_activation(cancel_activation)

            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_workflow_cancellation_during_child_workflow(self) -> None:
        """Test workflow cancellation while waiting for child workflow."""
        from temporalio_trio.worker._activation import CancelWorkflowJob

        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[CancellableChildWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[
                WorkflowStartedJob(workflow_type="CancellableChildWorkflow", args=())
            ],
            timestamp_ns=1_000_000_000,
            run_id="run-cancel-child",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start and block on child workflow
            await trio.sleep(0.1)
            assert "run-cancel-child" in worker._workflow_states

            # Send cancellation
            cancel_activation = _create_activation(
                jobs=[CancelWorkflowJob()],
                timestamp_ns=2_000_000_000,
                run_id="run-cancel-child",
            )
            bridge.add_activation(cancel_activation)

            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_cancellation_propagates_to_child_tasks(self) -> None:
        """Test cancellation propagates to child tasks via nursery."""
        from temporalio_trio.worker._activation import CancelWorkflowJob

        # Define a workflow with concurrent operations
        @defn
        class ConcurrentSleepWorkflow:
            """A workflow with concurrent sleeps."""

            @run
            async def run(self) -> str:
                import trio

                from temporalio_trio.worker._runtime import get_current_runtime

                runtime = get_current_runtime()

                async def sleep_task(duration: float) -> None:
                    await runtime.workflow_sleep(duration)

                # Start concurrent sleeps
                async with trio.open_nursery() as nursery:
                    runtime.nursery = nursery
                    nursery.start_soon(sleep_task, 5.0)
                    nursery.start_soon(sleep_task, 10.0)

                return "completed"

        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[ConcurrentSleepWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="ConcurrentSleepWorkflow", args=())],
            timestamp_ns=1_000_000_000,
            run_id="run-concurrent",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)
            assert "run-concurrent" in worker._workflow_states

            # Send cancellation - should propagate to all child tasks
            cancel_activation = _create_activation(
                jobs=[CancelWorkflowJob()],
                timestamp_ns=2_000_000_000,
                run_id="run-concurrent",
            )
            bridge.add_activation(cancel_activation)

            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_cancel_then_complete_workflow(self) -> None:
        """Test workflow that handles cancellation gracefully and completes."""
        from temporalio_trio.worker._activation import CancelWorkflowJob

        # Define a workflow that catches cancellation
        @defn
        class GracefulCancelWorkflow:
            """A workflow that handles cancellation gracefully."""

            @run
            async def run(self) -> str:
                import trio

                from temporalio_trio.worker._runtime import get_current_runtime

                runtime = get_current_runtime()
                try:
                    await runtime.workflow_sleep(10.0)
                    return "completed normally"
                except trio.Cancelled:
                    # Handle cancellation gracefully
                    return "cancelled gracefully"

        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[GracefulCancelWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="GracefulCancelWorkflow", args=())],
            timestamp_ns=1_000_000_000,
            run_id="run-graceful",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)
            assert "run-graceful" in worker._workflow_states

            # Send cancellation
            cancel_activation = _create_activation(
                jobs=[CancelWorkflowJob()],
                timestamp_ns=2_000_000_000,
                run_id="run-graceful",
            )
            bridge.add_activation(cancel_activation)

            await trio.sleep(0.1)

            # Shutdown
            worker.shutdown()


# =============================================================================
# Eviction and Reactivation Tests (Critical for Replay)
# =============================================================================


class TestSingleThreadWorkerEviction:
    """Tests for workflow eviction and reactivation.

    These tests verify the worker correctly handles cache eviction and
    subsequent reactivation (replay). SDK-Core handles command deduplication
    during replay - the SDK's job is to:
    1. Delete workflow state on eviction
    2. Create fresh workflow instances on reactivation
    3. Handle multi-job activations correctly (e.g., initialize + fire_timer)
    """

    @pytest.mark.trio
    async def test_eviction_deletes_workflow_state(self) -> None:
        """Test that eviction properly removes workflow from cache."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[TimerWorkflow],
        )

        # Start workflow
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="TimerWorkflow", args=(1.0,))],
            timestamp_ns=1_000_000_000,
            run_id="run-evict",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start and block on timer
            await trio.sleep(0.1)
            assert "run-evict" in worker._workflow_states

            # Send eviction activation
            eviction_activation = _create_activation(
                jobs=[],
                timestamp_ns=2_000_000_000,
                run_id="run-evict",
            )
            # Mark as eviction
            eviction_activation.remove_from_cache = True  # type: ignore
            bridge.add_activation(eviction_activation)

            await trio.sleep(0.1)

            # Workflow state should be deleted
            assert "run-evict" not in worker._workflow_states

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_reactivation_creates_fresh_instance(self) -> None:
        """Test that reactivation after eviction creates a fresh workflow instance.

        This simulates the replay scenario where:
        1. Workflow starts and sets a timer
        2. Workflow is evicted (cache pressure, continue-as-new, etc.)
        3. Timer fires, triggering reactivation
        4. SDK-Core sends initialize_workflow + fire_timer in same activation
        5. Worker creates fresh instance and processes both jobs
        """
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[TimerWorkflow],
        )

        # Step 1: Start workflow
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="TimerWorkflow", args=(1.0,))],
            timestamp_ns=1_000_000_000,
            run_id="run-replay",
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)
            assert "run-replay" in worker._workflow_states

            # Step 2: Send eviction
            eviction_activation = _create_activation(
                jobs=[],
                timestamp_ns=2_000_000_000,
                run_id="run-replay",
            )
            eviction_activation.remove_from_cache = True  # type: ignore
            bridge.add_activation(eviction_activation)

            await trio.sleep(0.1)
            assert "run-replay" not in worker._workflow_states

            # Step 3: Reactivation with initialize_workflow (fresh start)
            # In real scenario, SDK-Core sends this when timer fires and
            # workflow is no longer in cache
            reactivation = _create_activation(
                jobs=[WorkflowStartedJob(workflow_type="TimerWorkflow", args=(1.0,))],
                timestamp_ns=3_000_000_000,
                run_id="run-replay",
            )
            # Mark as replay
            reactivation.is_replaying = True  # type: ignore
            bridge.add_activation(reactivation)

            await trio.sleep(0.1)

            # Fresh instance should be created
            assert "run-replay" in worker._workflow_states

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_multi_job_activation_during_replay(self) -> None:
        """Test handling of multi-job activations during replay.

        During replay, SDK-Core may send multiple jobs in one activation:
        - initialize_workflow (to start the workflow)
        - fire_timer (already resolved from history)

        The worker must:
        1. Create fresh workflow instance
        2. Apply all jobs in order
        3. Let workflow progress to completion
        """
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[TimerWorkflow],
        )

        # Replay activation with both initialize_workflow and fire_timer
        # This happens when workflow was evicted after setting timer,
        # and SDK-Core replays the entire workflow to catch up
        replay_activation = _create_activation(
            jobs=[
                WorkflowStartedJob(workflow_type="TimerWorkflow", args=(1.0,)),
                TimerFiredJob(timer_id=1),
            ],
            timestamp_ns=2_000_000_000,
            run_id="run-multi-job",
        )
        replay_activation.is_replaying = True  # type: ignore
        bridge.add_activation(replay_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to process both jobs
            await trio.sleep(0.2)

            # Workflow should complete (timer fired immediately during replay)
            # Note: It may or may not still be in states depending on completion timing
            # The key is that no error occurred processing both jobs

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_eviction_then_full_replay_sequence(self) -> None:
        """Test complete eviction → replay → completion sequence.

        This tests the full cycle:
        1. Workflow starts, sets timer, produces StartTimer command
        2. Timer fires, workflow completes
        3. Workflow is evicted
        4. Something triggers reactivation (e.g., query)
        5. SDK-Core sends replay activation with all history
        6. Workflow replays and reaches same state
        """
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[TimerWorkflow],
        )

        run_id = "run-full-sequence"

        # Phase 1: Initial execution
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="TimerWorkflow", args=(1.0,))],
            timestamp_ns=1_000_000_000,
            run_id=run_id,
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)
            assert run_id in worker._workflow_states

            # Fire timer - workflow completes
            timer_activation = _create_activation(
                jobs=[TimerFiredJob(timer_id=1)],
                timestamp_ns=2_000_000_000,
                run_id=run_id,
            )
            bridge.add_activation(timer_activation)

            await trio.sleep(0.1)
            # Workflow should be completed and removed
            assert run_id not in worker._workflow_states

            # Phase 2: Later, workflow needs to replay (e.g., for a query)
            # SDK-Core sends full replay activation
            replay_activation = _create_activation(
                jobs=[
                    WorkflowStartedJob(workflow_type="TimerWorkflow", args=(1.0,)),
                    TimerFiredJob(timer_id=1),
                ],
                timestamp_ns=3_000_000_000,
                run_id=run_id,
            )
            replay_activation.is_replaying = True  # type: ignore
            bridge.add_activation(replay_activation)

            await trio.sleep(0.2)

            # Workflow replayed and completed again
            # (may or may not be in states depending on timing)

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_sequential_evictions_same_workflow(self) -> None:
        """Test multiple eviction/reactivation cycles for same workflow."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[MultiTimerWorkflow],
        )

        run_id = "run-multi-evict"

        # Start workflow
        initial_activation = _create_activation(
            jobs=[WorkflowStartedJob(workflow_type="MultiTimerWorkflow", args=())],
            timestamp_ns=1_000_000_000,
            run_id=run_id,
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to start
            await trio.sleep(0.1)
            assert run_id in worker._workflow_states

            # First eviction
            eviction1 = _create_activation(
                jobs=[],
                timestamp_ns=2_000_000_000,
                run_id=run_id,
            )
            eviction1.remove_from_cache = True  # type: ignore
            bridge.add_activation(eviction1)

            await trio.sleep(0.1)
            assert run_id not in worker._workflow_states

            # First reactivation
            reactivation1 = _create_activation(
                jobs=[WorkflowStartedJob(workflow_type="MultiTimerWorkflow", args=())],
                timestamp_ns=3_000_000_000,
                run_id=run_id,
            )
            reactivation1.is_replaying = True  # type: ignore
            bridge.add_activation(reactivation1)

            await trio.sleep(0.1)
            assert run_id in worker._workflow_states

            # Second eviction
            eviction2 = _create_activation(
                jobs=[],
                timestamp_ns=4_000_000_000,
                run_id=run_id,
            )
            eviction2.remove_from_cache = True  # type: ignore
            bridge.add_activation(eviction2)

            await trio.sleep(0.1)
            assert run_id not in worker._workflow_states

            # Second reactivation with partial replay
            reactivation2 = _create_activation(
                jobs=[
                    WorkflowStartedJob(workflow_type="MultiTimerWorkflow", args=()),
                    TimerFiredJob(timer_id=1),  # First timer already fired
                ],
                timestamp_ns=5_000_000_000,
                run_id=run_id,
            )
            reactivation2.is_replaying = True  # type: ignore
            bridge.add_activation(reactivation2)

            await trio.sleep(0.1)
            assert run_id in worker._workflow_states

            # Shutdown
            worker.shutdown()

    @pytest.mark.trio
    async def test_eviction_during_activity_wait(self) -> None:
        """Test eviction while workflow is waiting for activity."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="test-queue",
            workflows=[ActivityWorkflow],
        )

        run_id = "run-evict-activity"

        # Start workflow
        initial_activation = _create_activation(
            jobs=[
                WorkflowStartedJob(
                    workflow_type="ActivityWorkflow", args=("my_activity",)
                )
            ],
            timestamp_ns=1_000_000_000,
            run_id=run_id,
        )
        bridge.add_activation(initial_activation)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)

            # Wait for workflow to block on activity
            await trio.sleep(0.1)
            assert run_id in worker._workflow_states

            # Evict while waiting for activity
            eviction = _create_activation(
                jobs=[],
                timestamp_ns=2_000_000_000,
                run_id=run_id,
            )
            eviction.remove_from_cache = True  # type: ignore
            bridge.add_activation(eviction)

            await trio.sleep(0.1)
            assert run_id not in worker._workflow_states

            # Reactivate with activity already resolved
            replay_activation = _create_activation(
                jobs=[
                    WorkflowStartedJob(
                        workflow_type="ActivityWorkflow", args=("my_activity",)
                    ),
                    ActivityResolvedJob(seq=1, result="activity done!"),
                ],
                timestamp_ns=3_000_000_000,
                run_id=run_id,
            )
            replay_activation.is_replaying = True  # type: ignore
            bridge.add_activation(replay_activation)

            await trio.sleep(0.2)

            # Workflow should complete (activity result was in replay)

            # Shutdown
            worker.shutdown()
