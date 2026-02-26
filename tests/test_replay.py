"""Tests for replay determinism (Phase 5).

These tests verify that replaying workflow history produces identical results,
which is the core requirement for Temporal workflow determinism.
"""

from datetime import datetime, timezone

import pytest

from temporalio_trio import workflow
from temporalio_trio.worker import (
    CompleteWorkflowCommand,
    StartTimerCommand,
    TimerFiredJob,
    TrioWorkflowRunner,
    WorkflowActivation,
    WorkflowActivationCompletion,
    WorkflowInstanceDetails,
    WorkflowStartedJob,
)


# Test workflows for replay verification
@workflow.defn
class MultiSleepWorkflow:
    """Workflow that sleeps multiple times."""

    @workflow.run
    async def run(self) -> list[float]:
        times = []
        for _ in range(3):
            times.append(workflow.time())
            await workflow.sleep(5)
        times.append(workflow.time())
        return times


@workflow.defn
class ConditionalSleepWorkflow:
    """Workflow with conditional sleep based on input."""

    @workflow.run
    async def run(self, count: int) -> list[float]:
        times = []
        for i in range(count):
            times.append(workflow.time())
            await workflow.sleep(i + 1)  # Sleep 1s, 2s, 3s, etc.
        times.append(workflow.time())
        return times


@workflow.defn
class NestedTimerWorkflow:
    """Workflow with nested timer patterns."""

    @workflow.run
    async def run(self) -> dict[str, float]:
        result = {}
        result["start"] = workflow.time()

        await workflow.sleep(10)
        result["after_first"] = workflow.time()

        await workflow.sleep(5)
        result["after_second"] = workflow.time()

        await workflow.sleep(15)
        result["end"] = workflow.time()

        return result


def _create_details(
    workflow_cls: type,
    workflow_id: str = "replay-test",
    randomness_seed: int = 42,
) -> WorkflowInstanceDetails:
    """Helper to create WorkflowInstanceDetails."""
    defn = workflow._Definition.must_from_class(workflow_cls)
    info = workflow.Info(
        workflow_id=workflow_id,
        workflow_type=defn.name or "",
        run_id="run-1",
        task_queue="test-queue",
        namespace="default",
        attempt=1,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    return WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=randomness_seed,
    )


def _execute_workflow(
    runner: TrioWorkflowRunner,
    details: WorkflowInstanceDetails,
    args: tuple = (),
) -> tuple[object, list[WorkflowActivation]]:
    """Execute a workflow and capture the history (activations).

    Returns:
        Tuple of (result, history) where history is the list of activations.
    """
    instance = runner.create_instance(details)
    history: list[WorkflowActivation] = []
    current_time_ns = 0

    # Start workflow
    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type=details.defn.name or "", args=args)],
        timestamp_ns=current_time_ns,
    )
    history.append(activation)
    completion = instance.activate(activation)

    # Process timer commands until complete
    while not any(isinstance(c, CompleteWorkflowCommand) for c in completion.commands):
        timer_cmds = [
            c for c in completion.commands if isinstance(c, StartTimerCommand)
        ]
        if not timer_cmds:
            break

        # Advance time and fire timer
        timer_cmd = timer_cmds[0]
        current_time_ns += timer_cmd.duration_ms * 1_000_000

        activation = WorkflowActivation(
            jobs=[TimerFiredJob(timer_id=timer_cmd.timer_id)],
            timestamp_ns=current_time_ns,
        )
        history.append(activation)
        completion = instance.activate(activation)

    # Get result
    result_cmd = next(
        (c for c in completion.commands if isinstance(c, CompleteWorkflowCommand)),
        None,
    )
    result = result_cmd.result if result_cmd else None

    return result, history


def _replay_workflow(
    runner: TrioWorkflowRunner,
    details: WorkflowInstanceDetails,
    history: list[WorkflowActivation],
) -> object:
    """Replay a workflow with recorded history.

    Returns:
        The workflow result.
    """
    instance = runner.create_instance(details)

    completion: WorkflowActivationCompletion | None = None
    for activation in history:
        completion = instance.activate(activation)

    if completion is None:
        return None

    result_cmd = next(
        (c for c in completion.commands if isinstance(c, CompleteWorkflowCommand)),
        None,
    )
    return result_cmd.result if result_cmd else None


class TestReplayDeterminism:
    """Tests for replay determinism."""

    def test_simple_replay_same_result(self) -> None:
        """Test that replaying a simple workflow produces identical result."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(MultiSleepWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(MultiSleepWorkflow)

        # Execute and capture history
        result1, history = _execute_workflow(runner, details)

        # Replay with same history
        result2 = _replay_workflow(runner, details, history)

        # Results must be identical
        assert result1 == result2
        # Verify the actual values make sense (times at each sleep point)
        assert isinstance(result1, list)
        assert len(result1) == 4  # 3 sleeps + final time

    def test_conditional_workflow_replay(self) -> None:
        """Test replay of workflow with conditional logic."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(ConditionalSleepWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(ConditionalSleepWorkflow)

        # Execute with count=3
        result1, history = _execute_workflow(runner, details, args=(3,))

        # Replay
        result2 = _replay_workflow(runner, details, history)

        assert result1 == result2
        assert isinstance(result1, list)
        assert len(result1) == 4  # 3 iterations + final

    def test_nested_timer_replay(self) -> None:
        """Test replay of workflow with nested timer patterns."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(NestedTimerWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(NestedTimerWorkflow)

        # Execute
        result1, history = _execute_workflow(runner, details)

        # Replay
        result2 = _replay_workflow(runner, details, history)

        assert result1 == result2
        assert isinstance(result1, dict)
        assert "start" in result1
        assert "end" in result1

    def test_multiple_replays_consistent(self) -> None:
        """Test that multiple replays all produce same result."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(MultiSleepWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(MultiSleepWorkflow)

        # Execute once
        result1, history = _execute_workflow(runner, details)

        # Replay multiple times
        results = []
        for _ in range(5):
            result = _replay_workflow(runner, details, history)
            results.append(result)

        # All replays should produce same result
        for result in results:
            assert result == result1

    def test_different_workflow_ids_same_seed_same_result(self) -> None:
        """Test that same seed produces same result regardless of workflow ID."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(MultiSleepWorkflow)
        runner.prepare_workflow(defn)

        seed = 12345

        # Execute with different workflow IDs but same seed
        details1 = _create_details(
            MultiSleepWorkflow, workflow_id="wf-a", randomness_seed=seed
        )
        details2 = _create_details(
            MultiSleepWorkflow, workflow_id="wf-b", randomness_seed=seed
        )

        result1, _ = _execute_workflow(runner, details1)
        result2, _ = _execute_workflow(runner, details2)

        # Same seed should produce same execution pattern
        assert result1 == result2


class TestReplayHistoryIntegrity:
    """Tests for history integrity during replay."""

    def test_history_length_matches_timer_count(self) -> None:
        """Test that history length matches number of timers + start."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(MultiSleepWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(MultiSleepWorkflow)
        _, history = _execute_workflow(runner, details)

        # History should have: 1 start + 3 timer fires = 4 activations
        assert len(history) == 4

        # First should be start
        assert any(isinstance(j, WorkflowStartedJob) for j in history[0].jobs)

        # Rest should be timer fires
        for activation in history[1:]:
            assert any(isinstance(j, TimerFiredJob) for j in activation.jobs)

    def test_timer_ids_sequential(self) -> None:
        """Test that timer IDs are assigned sequentially."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(MultiSleepWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(MultiSleepWorkflow)
        instance = runner.create_instance(details)

        # Start workflow
        completion = instance.activate(
            WorkflowActivation(
                jobs=[WorkflowStartedJob(workflow_type="MultiSleepWorkflow", args=())],
                timestamp_ns=0,
            )
        )

        # First timer should be ID 0
        timer_cmd = next(
            c for c in completion.commands if isinstance(c, StartTimerCommand)
        )
        assert timer_cmd.timer_id == 0

    def test_timestamps_advance_correctly(self) -> None:
        """Test that timestamps in history advance based on timer durations."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(ConditionalSleepWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(ConditionalSleepWorkflow)
        _, history = _execute_workflow(runner, details, args=(3,))

        # Timestamps should be: 0, 1s, 3s (1+2), 6s (1+2+3)
        expected_times_ns = [
            0,
            1_000_000_000,  # After 1s sleep
            3_000_000_000,  # After 1s + 2s
            6_000_000_000,  # After 1s + 2s + 3s
        ]

        for i, activation in enumerate(history):
            assert activation.timestamp_ns == expected_times_ns[i]


class TestReplayEdgeCases:
    """Tests for edge cases in replay."""

    def test_zero_sleep_duration(self) -> None:
        """Test workflow with zero-duration sleep."""

        @workflow.defn
        class ZeroSleepWorkflow:
            @workflow.run
            async def run(self) -> str:
                await workflow.sleep(0)
                return "done"

        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(ZeroSleepWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(ZeroSleepWorkflow)
        result1, history = _execute_workflow(runner, details)
        result2 = _replay_workflow(runner, details, history)

        assert result1 == result2 == "done"

    def test_large_sleep_duration(self) -> None:
        """Test workflow with large sleep duration."""

        @workflow.defn
        class LargeSleepWorkflow:
            @workflow.run
            async def run(self) -> float:
                await workflow.sleep(86400)  # 24 hours
                return workflow.time()

        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(LargeSleepWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(LargeSleepWorkflow)
        result1, history = _execute_workflow(runner, details)
        result2 = _replay_workflow(runner, details, history)

        assert result1 == result2
        # Time should be 86400 seconds
        assert result1 == 86400.0

    def test_workflow_with_no_sleeps(self) -> None:
        """Test replay of workflow with no sleeps."""

        @workflow.defn
        class NoSleepWorkflow:
            @workflow.run
            async def run(self) -> str:
                return f"Time is {workflow.time()}"

        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(NoSleepWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(NoSleepWorkflow)
        result1, history = _execute_workflow(runner, details)
        result2 = _replay_workflow(runner, details, history)

        assert result1 == result2
        # Only one activation (start)
        assert len(history) == 1
