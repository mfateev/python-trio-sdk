"""Tests for workflow activation handling (Phase 3).

These tests verify the full activation/completion cycle including:
- Workflow started handling
- Timer commands and timer fired handling
- Workflow completion (success and failure)
- Runtime context during activation
"""

from datetime import datetime, timezone

import pytest

from temporalio_trio import workflow
from temporalio_trio.worker import (
    CompleteWorkflowCommand,
    FailWorkflowCommand,
    StartTimerCommand,
    TimerFiredJob,
    TrioWorkflowInstance,
    WorkflowActivation,
    WorkflowActivationCompletion,
    WorkflowInstanceDetails,
    WorkflowStartedJob,
)

# =============================================================================
# Test Workflows
# =============================================================================


@workflow.defn
class SimpleWorkflow:
    """A simple workflow that returns immediately."""

    @workflow.run
    async def run(self) -> str:
        return "done"


@workflow.defn
class WorkflowWithArgs:
    """A workflow that accepts arguments."""

    @workflow.run
    async def run(self, name: str, count: int) -> str:
        return f"Hello {name}, count={count}"


@workflow.defn
class WorkflowWithSleep:
    """A workflow that uses workflow.sleep()."""

    @workflow.run
    async def run(self, duration: float) -> str:
        await workflow.sleep(duration)
        return "slept"


@workflow.defn
class WorkflowWithMultipleSleeps:
    """A workflow with multiple sleep calls."""

    @workflow.run
    async def run(self) -> list[float]:
        times: list[float] = []
        times.append(workflow.time())
        await workflow.sleep(5.0)
        times.append(workflow.time())
        await workflow.sleep(10.0)
        times.append(workflow.time())
        return times


@workflow.defn
class WorkflowThatFails:
    """A workflow that raises an exception."""

    @workflow.run
    async def run(self, message: str) -> None:
        raise ValueError(message)


@workflow.defn
class WorkflowWithTimeAccess:
    """A workflow that accesses workflow time."""

    @workflow.run
    async def run(self) -> dict[str, int | float]:
        return {
            "time": workflow.time(),
            "time_ns": workflow.time_ns(),
        }


# =============================================================================
# Helper Functions
# =============================================================================


def _create_instance(
    workflow_cls: type,
    workflow_id: str = "test-wf-1",
    run_id: str = "run-1",
    task_queue: str = "test-queue",
    randomness_seed: int = 12345,
) -> TrioWorkflowInstance:
    """Helper to create a TrioWorkflowInstance for tests."""
    defn = workflow._Definition.must_from_class(workflow_cls)
    info = workflow.Info(
        workflow_id=workflow_id,
        workflow_type=defn.name or "",
        run_id=run_id,
        task_queue=task_queue,
        namespace="default",
        attempt=1,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    details = WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=randomness_seed,
    )
    return TrioWorkflowInstance(details)


def _create_start_activation(
    workflow_type: str,
    args: tuple = (),
    timestamp_ns: int = 0,
) -> WorkflowActivation:
    """Helper to create a workflow start activation."""
    return WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type=workflow_type, args=args)],
        timestamp_ns=timestamp_ns,
    )


def _create_timer_fired_activation(
    timer_id: int,
    timestamp_ns: int,
) -> WorkflowActivation:
    """Helper to create a timer fired activation."""
    return WorkflowActivation(
        jobs=[TimerFiredJob(timer_id=timer_id)],
        timestamp_ns=timestamp_ns,
    )


# =============================================================================
# Test Classes
# =============================================================================


class TestSimpleWorkflowActivation:
    """Tests for simple workflow execution without timers."""

    def test_simple_workflow_completes(self) -> None:
        """Test a simple workflow returns CompleteWorkflowCommand."""
        instance = _create_instance(SimpleWorkflow)
        activation = _create_start_activation("SimpleWorkflow")

        completion = instance.activate(activation)

        assert isinstance(completion, WorkflowActivationCompletion)
        assert len(completion.commands) == 1
        assert isinstance(completion.commands[0], CompleteWorkflowCommand)
        assert completion.commands[0].result == "done"

    def test_workflow_with_args(self) -> None:
        """Test workflow receives and processes arguments."""
        instance = _create_instance(WorkflowWithArgs)
        activation = _create_start_activation("WorkflowWithArgs", args=("Alice", 42))

        completion = instance.activate(activation)

        assert len(completion.commands) == 1
        assert isinstance(completion.commands[0], CompleteWorkflowCommand)
        assert completion.commands[0].result == "Hello Alice, count=42"

    def test_workflow_time_access(self) -> None:
        """Test workflow can access workflow time during execution."""
        instance = _create_instance(WorkflowWithTimeAccess)
        activation = _create_start_activation(
            "WorkflowWithTimeAccess",
            timestamp_ns=5_000_000_000,  # 5 seconds
        )

        completion = instance.activate(activation)

        assert len(completion.commands) == 1
        result = completion.commands[0]
        assert isinstance(result, CompleteWorkflowCommand)
        assert result.result["time"] == 5.0
        assert result.result["time_ns"] == 5_000_000_000


class TestWorkflowWithSleep:
    """Tests for workflow with sleep/timer functionality."""

    def test_sleep_creates_timer_command(self) -> None:
        """Test workflow.sleep() creates a StartTimerCommand."""
        instance = _create_instance(WorkflowWithSleep)
        activation = _create_start_activation(
            "WorkflowWithSleep", args=(5.0,), timestamp_ns=0
        )

        completion = instance.activate(activation)

        # Should have exactly one command: StartTimerCommand
        # Workflow is blocked waiting for timer, no completion yet
        assert len(completion.commands) == 1
        assert isinstance(completion.commands[0], StartTimerCommand)
        assert completion.commands[0].timer_id == 0
        assert completion.commands[0].duration_ms == 5000

    def test_timer_fired_continues_workflow(self) -> None:
        """Test TimerFiredJob allows workflow to continue."""
        instance = _create_instance(WorkflowWithSleep)

        # First activation: start workflow
        start_act = _create_start_activation(
            "WorkflowWithSleep", args=(5.0,), timestamp_ns=0
        )
        completion1 = instance.activate(start_act)

        assert len(completion1.commands) == 1
        assert isinstance(completion1.commands[0], StartTimerCommand)
        timer_id = completion1.commands[0].timer_id

        # Second activation: timer fired
        timer_act = _create_timer_fired_activation(
            timer_id=timer_id, timestamp_ns=5_000_000_000
        )
        completion2 = instance.activate(timer_act)

        # Now workflow should complete
        assert len(completion2.commands) == 1
        assert isinstance(completion2.commands[0], CompleteWorkflowCommand)
        assert completion2.commands[0].result == "slept"

    def test_multiple_sleeps(self) -> None:
        """Test workflow with multiple sequential sleeps.

        Note: With replay semantics, the workflow re-runs from the beginning
        each activation, so workflow.time() always returns the current
        activation's timestamp. The times recorded are all from the final
        activation when the workflow completes.
        """
        instance = _create_instance(WorkflowWithMultipleSleeps)

        # First activation: start workflow
        start_act = _create_start_activation(
            "WorkflowWithMultipleSleeps",
            timestamp_ns=1_000_000_000,  # 1 second
        )
        completion1 = instance.activate(start_act)

        # Should have first timer command
        assert len(completion1.commands) == 1
        assert isinstance(completion1.commands[0], StartTimerCommand)
        assert completion1.commands[0].timer_id == 0
        assert completion1.commands[0].duration_ms == 5000

        # Second activation: first timer fired at t=6s
        timer1_act = _create_timer_fired_activation(
            timer_id=0, timestamp_ns=6_000_000_000
        )
        completion2 = instance.activate(timer1_act)

        # Should have second timer command
        assert len(completion2.commands) == 1
        assert isinstance(completion2.commands[0], StartTimerCommand)
        assert completion2.commands[0].timer_id == 1
        assert completion2.commands[0].duration_ms == 10000

        # Third activation: second timer fired at t=16s
        timer2_act = _create_timer_fired_activation(
            timer_id=1, timestamp_ns=16_000_000_000
        )
        completion3 = instance.activate(timer2_act)

        # Now workflow should complete
        # All times are from the final activation (replay semantics)
        assert len(completion3.commands) == 1
        assert isinstance(completion3.commands[0], CompleteWorkflowCommand)
        result = completion3.commands[0].result
        # All times are from the final activation timestamp
        assert result == [16.0, 16.0, 16.0]


class TestWorkflowFailure:
    """Tests for workflow failure handling."""

    def test_workflow_exception_creates_fail_command(self) -> None:
        """Test exception creates FailWorkflowCommand."""
        instance = _create_instance(WorkflowThatFails)
        activation = _create_start_activation("WorkflowThatFails", args=("test error",))

        completion = instance.activate(activation)

        assert len(completion.commands) == 1
        assert isinstance(completion.commands[0], FailWorkflowCommand)
        assert isinstance(completion.commands[0].exception, ValueError)
        assert str(completion.commands[0].exception) == "test error"


class TestTimerSequencing:
    """Tests for timer ID sequencing."""

    def test_timer_ids_are_sequential(self) -> None:
        """Test timer IDs are assigned sequentially."""
        instance = _create_instance(WorkflowWithMultipleSleeps)

        # Start workflow
        start_act = _create_start_activation(
            "WorkflowWithMultipleSleeps", timestamp_ns=0
        )
        completion1 = instance.activate(start_act)

        assert isinstance(completion1.commands[0], StartTimerCommand)
        first_timer_id = completion1.commands[0].timer_id
        assert first_timer_id == 0

        # First timer fires
        timer1_act = _create_timer_fired_activation(
            timer_id=0, timestamp_ns=5_000_000_000
        )
        completion2 = instance.activate(timer1_act)

        assert isinstance(completion2.commands[0], StartTimerCommand)
        second_timer_id = completion2.commands[0].timer_id
        assert second_timer_id == 1


class TestUnknownTimerFired:
    """Tests for handling unknown timer IDs."""

    def test_unknown_timer_id_is_ignored(self) -> None:
        """Test that timer fired for unknown ID doesn't complete workflow.

        With replay semantics, the workflow re-runs and hits the same sleep,
        so it will generate the same timer command again.
        """
        instance = _create_instance(WorkflowWithSleep)

        # Start workflow - creates timer 0
        start_act = _create_start_activation(
            "WorkflowWithSleep", args=(5.0,), timestamp_ns=0
        )
        completion1 = instance.activate(start_act)
        assert isinstance(completion1.commands[0], StartTimerCommand)
        assert completion1.commands[0].timer_id == 0

        # Fire timer with wrong ID (999) - workflow still waiting for timer 0
        wrong_timer_act = _create_timer_fired_activation(
            timer_id=999, timestamp_ns=5_000_000_000
        )
        completion2 = instance.activate(wrong_timer_act)

        # Workflow re-runs and blocks at the same sleep, generating same timer
        assert len(completion2.commands) == 1
        assert isinstance(completion2.commands[0], StartTimerCommand)
        assert completion2.commands[0].timer_id == 0  # Same timer again


class TestActivationTimestamp:
    """Tests for activation timestamp handling."""

    def test_timestamp_updates_workflow_time(self) -> None:
        """Test activation timestamp updates workflow time."""
        instance = _create_instance(WorkflowWithTimeAccess)

        # Start with specific timestamp
        activation = _create_start_activation(
            "WorkflowWithTimeAccess", timestamp_ns=12_345_678_900
        )
        completion = instance.activate(activation)

        result = completion.commands[0]
        assert isinstance(result, CompleteWorkflowCommand)
        assert result.result["time_ns"] == 12_345_678_900

    def test_subsequent_activation_updates_time(self) -> None:
        """Test each activation updates workflow time."""
        instance = _create_instance(WorkflowWithMultipleSleeps)

        # Start at t=0
        start_act = _create_start_activation(
            "WorkflowWithMultipleSleeps", timestamp_ns=0
        )
        instance.activate(start_act)

        # Verify internal time is updated
        assert instance.workflow_time_ns() == 0

        # Timer fired at t=5s
        timer_act = _create_timer_fired_activation(
            timer_id=0, timestamp_ns=5_000_000_000
        )
        instance.activate(timer_act)

        assert instance.workflow_time_ns() == 5_000_000_000


class TestDeterministicExecution:
    """Tests for deterministic execution properties."""

    def test_same_seed_same_random(self) -> None:
        """Test same randomness seed produces same internal random state."""
        instance1 = _create_instance(SimpleWorkflow, randomness_seed=12345)
        instance2 = _create_instance(SimpleWorkflow, randomness_seed=12345)

        # Internal random should produce same values
        random1_val = instance1._random.randint(0, 1000000)
        random2_val = instance2._random.randint(0, 1000000)
        assert random1_val == random2_val

    def test_different_seed_different_random(self) -> None:
        """Test different randomness seed produces different random state."""
        instance1 = _create_instance(SimpleWorkflow, randomness_seed=11111)
        instance2 = _create_instance(SimpleWorkflow, randomness_seed=22222)

        # Internal random should produce different values
        random1_val = instance1._random.randint(0, 1000000)
        random2_val = instance2._random.randint(0, 1000000)
        assert random1_val != random2_val


class TestWorkflowObjectLifecycle:
    """Tests for workflow object lifecycle."""

    def test_workflow_object_created_on_start(self) -> None:
        """Test workflow object is created when workflow starts."""
        instance = _create_instance(SimpleWorkflow)
        assert instance._workflow_obj is None

        activation = _create_start_activation("SimpleWorkflow")
        instance.activate(activation)

        assert instance._workflow_obj is not None
        assert isinstance(instance._workflow_obj, SimpleWorkflow)

    def test_workflow_object_recreated_on_replay(self) -> None:
        """Test workflow object is recreated on each activation (replay semantics).

        With replay semantics, the workflow re-runs from the beginning each
        activation, creating a new workflow object each time.
        """
        instance = _create_instance(WorkflowWithSleep)

        # Start workflow
        start_act = _create_start_activation(
            "WorkflowWithSleep", args=(5.0,), timestamp_ns=0
        )
        instance.activate(start_act)
        first_obj = instance._workflow_obj
        assert first_obj is not None

        # Timer fired - workflow re-runs, new object created
        timer_act = _create_timer_fired_activation(
            timer_id=0, timestamp_ns=5_000_000_000
        )
        instance.activate(timer_act)
        second_obj = instance._workflow_obj

        # Different objects due to replay semantics
        assert second_obj is not None
        assert first_obj is not second_obj
        assert isinstance(second_obj, WorkflowWithSleep)


class TestRuntimeContext:
    """Tests for runtime context during activation."""

    def test_runtime_not_accessible_outside_activation(self) -> None:
        """Test runtime is not accessible outside activation."""
        _create_instance(SimpleWorkflow)

        with pytest.raises(workflow._NotInWorkflowContextError):
            workflow.time()

    def test_runtime_reset_after_activation(self) -> None:
        """Test runtime is reset after activation completes."""
        instance = _create_instance(SimpleWorkflow)
        activation = _create_start_activation("SimpleWorkflow")

        instance.activate(activation)

        # After activation, runtime should not be accessible
        with pytest.raises(workflow._NotInWorkflowContextError):
            workflow.time()
