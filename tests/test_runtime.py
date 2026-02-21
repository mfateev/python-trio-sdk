"""Tests for WorkflowRuntime and ContextVar-based runtime isolation.

This tests Phases 1, 2, and 4 of the single-threaded migration plan:
- Phase 1: WorkflowRuntime can be created with required fields
- Phase 1: ContextVar isolation (multiple runtimes don't interfere)
- Phase 1: ContextVar propagates to child tasks in Trio nursery
- Phase 1: Getting runtime when not set raises error
- Phase 2: next_timer_seq increments correctly
- Phase 2: workflow_sleep creates timer command and suspends
- Phase 2: apply_timer_fired wakes up suspended workflow
- Phase 2: Replay path (timer already fired returns immediately)
- Phase 2: Multiple timers in sequence and concurrent
- Phase 4: next_activity_seq increments correctly
- Phase 4: execute_activity creates command and suspends
- Phase 4: apply_activity_resolved wakes up suspended workflow
- Phase 4: Activity failure raises exception
- Phase 4: Replay path (activity already completed)
- Phase 4: Concurrent activities
"""

import random
from datetime import timedelta

import pytest
import trio

from temporalio_trio.worker._runtime import (
    NotInWorkflowRuntimeError,
    QueryFailureCommand,
    QuerySuccessCommand,
    ScheduleActivityCommand,
    StartChildWorkflowCommand,
    StartTimerCommand,
    WorkflowRuntime,
    get_current_runtime,
    maybe_get_current_runtime,
    reset_current_runtime,
    set_current_runtime,
)


def _create_test_runtime(
    run_id: str = "run-1",
    workflow_id: str = "wf-1",
    workflow_type: str = "TestWorkflow",
    task_queue: str = "test-queue",
    random_seed: int = 12345,
    time_ns: int = 0,
    is_replaying: bool = False,
) -> WorkflowRuntime:
    """Helper to create a WorkflowRuntime for tests."""
    return WorkflowRuntime(
        run_id=run_id,
        workflow_id=workflow_id,
        workflow_type=workflow_type,
        task_queue=task_queue,
        random=random.Random(random_seed),
        time_ns=time_ns,
        is_replaying=is_replaying,
    )


class TestWorkflowRuntimeCreation:
    """Tests for WorkflowRuntime dataclass creation."""

    def test_creation_with_required_fields(self) -> None:
        """Test WorkflowRuntime can be created with all required fields."""
        runtime = _create_test_runtime()

        assert runtime.run_id == "run-1"
        assert runtime.workflow_id == "wf-1"
        assert runtime.workflow_type == "TestWorkflow"
        assert runtime.task_queue == "test-queue"
        assert isinstance(runtime.random, random.Random)
        assert runtime.time_ns == 0
        assert runtime.is_replaying is False

    def test_default_values(self) -> None:
        """Test WorkflowRuntime has correct default values."""
        runtime = _create_test_runtime()

        # Sequence counters default to 0
        assert runtime.timer_seq == 0
        assert runtime.activity_seq == 0
        assert runtime.child_workflow_seq == 0
        assert runtime.signal_seq == 0

        # Fired events default to empty dicts
        assert runtime.fired_timers == {}
        assert runtime.completed_activities == {}
        assert runtime.completed_children == {}

        # Pending events default to empty dicts
        assert runtime.pending_timers == {}
        assert runtime.pending_activities == {}
        assert runtime.pending_children == {}

        # Commands default to empty list
        assert runtime.commands == []

        # Workflow instance defaults
        assert runtime.workflow_object is None
        assert runtime.nursery is None

    def test_creation_with_custom_time(self) -> None:
        """Test WorkflowRuntime can be created with custom time."""
        runtime = _create_test_runtime(time_ns=5_000_000_000)

        assert runtime.time_ns == 5_000_000_000

    def test_creation_with_replaying(self) -> None:
        """Test WorkflowRuntime can be created with is_replaying=True."""
        runtime = _create_test_runtime(is_replaying=True)

        assert runtime.is_replaying is True

    def test_random_is_seeded(self) -> None:
        """Test random generator is seeded consistently."""
        runtime1 = _create_test_runtime(random_seed=12345)
        runtime2 = _create_test_runtime(random_seed=12345)
        runtime3 = _create_test_runtime(random_seed=67890)

        # Same seed should produce same values
        val1 = runtime1.random.randint(0, 1000000)
        val2 = runtime2.random.randint(0, 1000000)
        val3 = runtime3.random.randint(0, 1000000)

        assert val1 == val2
        assert val1 != val3  # Different seeds

    def test_mutable_fields_are_independent(self) -> None:
        """Test mutable fields (dicts, lists) are independent between instances."""
        runtime1 = _create_test_runtime(run_id="run-1")
        runtime2 = _create_test_runtime(run_id="run-2")

        # Modify runtime1's mutable fields
        runtime1.fired_timers[1] = 1000
        runtime1.commands.append("command1")
        runtime1.pending_timers[1] = None  # type: ignore[assignment]

        # runtime2 should be unaffected
        assert runtime2.fired_timers == {}
        assert runtime2.commands == []
        assert runtime2.pending_timers == {}


class TestContextVarIsolation:
    """Tests for ContextVar-based runtime isolation."""

    def test_get_runtime_when_not_set_raises_error(self) -> None:
        """Test get_current_runtime raises error when not in workflow context."""
        # Ensure we're starting from a clean state
        assert maybe_get_current_runtime() is None

        with pytest.raises(NotInWorkflowRuntimeError) as exc_info:
            get_current_runtime()

        assert "Not in workflow runtime context" in str(exc_info.value)

    def test_maybe_get_runtime_returns_none_when_not_set(self) -> None:
        """Test maybe_get_current_runtime returns None when not set."""
        assert maybe_get_current_runtime() is None

    def test_set_and_get_runtime(self) -> None:
        """Test setting and getting the current runtime."""
        runtime = _create_test_runtime()

        token = set_current_runtime(runtime)
        try:
            assert get_current_runtime() is runtime
            assert maybe_get_current_runtime() is runtime
        finally:
            reset_current_runtime(token)

        # After reset, should be None again
        assert maybe_get_current_runtime() is None

    def test_reset_restores_previous_value(self) -> None:
        """Test reset restores the previous runtime value."""
        runtime1 = _create_test_runtime(run_id="run-1")
        runtime2 = _create_test_runtime(run_id="run-2")

        # Set first runtime
        token1 = set_current_runtime(runtime1)
        try:
            assert get_current_runtime() is runtime1

            # Set second runtime (nested)
            token2 = set_current_runtime(runtime2)
            try:
                assert get_current_runtime() is runtime2
            finally:
                reset_current_runtime(token2)

            # After resetting token2, should be back to runtime1
            assert get_current_runtime() is runtime1
        finally:
            reset_current_runtime(token1)

        # After resetting token1, should be None
        assert maybe_get_current_runtime() is None

    def test_multiple_runtimes_dont_interfere(self) -> None:
        """Test that multiple runtimes in different contexts don't interfere."""
        runtime1 = _create_test_runtime(run_id="run-1", time_ns=1_000_000_000)
        runtime2 = _create_test_runtime(run_id="run-2", time_ns=2_000_000_000)

        # Set runtime1, modify it
        token = set_current_runtime(runtime1)
        try:
            current = get_current_runtime()
            assert current.run_id == "run-1"
            assert current.time_ns == 1_000_000_000

            # Modify runtime1
            current.time_ns = 1_500_000_000
            current.timer_seq = 5

            # runtime2 should be unaffected
            assert runtime2.time_ns == 2_000_000_000
            assert runtime2.timer_seq == 0
        finally:
            reset_current_runtime(token)


class TestContextVarInTrioNursery:
    """Tests for ContextVar propagation in Trio nursery."""

    @pytest.mark.trio
    async def test_contextvar_propagates_to_child_tasks(self) -> None:
        """Test ContextVar propagates to child tasks in Trio nursery."""
        runtime = _create_test_runtime(run_id="parent-run")
        results: list[str] = []

        async def child_task(name: str) -> None:
            # Child task should see the parent's runtime
            current = get_current_runtime()
            results.append(f"{name}:{current.run_id}")

        token = set_current_runtime(runtime)
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(child_task, "child1")
                nursery.start_soon(child_task, "child2")
        finally:
            reset_current_runtime(token)

        # Both children should have seen the parent runtime
        assert "child1:parent-run" in results
        assert "child2:parent-run" in results

    @pytest.mark.trio
    async def test_nested_nurseries_inherit_runtime(self) -> None:
        """Test nested nurseries inherit the runtime from parent."""
        runtime = _create_test_runtime(run_id="root-run")
        results: list[str] = []

        async def grandchild_task() -> None:
            current = get_current_runtime()
            results.append(f"grandchild:{current.run_id}")

        async def child_task() -> None:
            current = get_current_runtime()
            results.append(f"child:{current.run_id}")
            async with trio.open_nursery() as nursery:
                nursery.start_soon(grandchild_task)

        token = set_current_runtime(runtime)
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(child_task)
        finally:
            reset_current_runtime(token)

        assert "child:root-run" in results
        assert "grandchild:root-run" in results

    @pytest.mark.trio
    async def test_child_can_read_runtime_state(self) -> None:
        """Test child tasks can read runtime state set by parent."""
        runtime = _create_test_runtime()
        runtime.time_ns = 5_000_000_000
        runtime.timer_seq = 10

        time_values: list[int] = []
        timer_seqs: list[int] = []

        async def child_task() -> None:
            current = get_current_runtime()
            time_values.append(current.time_ns)
            timer_seqs.append(current.timer_seq)

        token = set_current_runtime(runtime)
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(child_task)
                nursery.start_soon(child_task)
        finally:
            reset_current_runtime(token)

        assert time_values == [5_000_000_000, 5_000_000_000]
        assert timer_seqs == [10, 10]

    @pytest.mark.trio
    async def test_child_modifications_visible_to_parent(self) -> None:
        """Test modifications by child are visible (same runtime instance)."""
        runtime = _create_test_runtime()
        runtime.timer_seq = 0

        async def child_task() -> None:
            current = get_current_runtime()
            current.timer_seq += 1

        token = set_current_runtime(runtime)
        try:
            async with trio.open_nursery() as nursery:
                nursery.start_soon(child_task)
                nursery.start_soon(child_task)

            # Wait for children to complete - nursery exit ensures this
            # Both children incremented the counter
            assert runtime.timer_seq == 2
        finally:
            reset_current_runtime(token)

    @pytest.mark.trio
    async def test_different_runtimes_in_sequential_tasks(self) -> None:
        """Test different runtimes can be used for sequential tasks."""
        results: list[str] = []

        async def task_with_runtime(rt: WorkflowRuntime) -> None:
            token = set_current_runtime(rt)
            try:
                current = get_current_runtime()
                results.append(current.run_id)
            finally:
                reset_current_runtime(token)

        runtime1 = _create_test_runtime(run_id="first")
        runtime2 = _create_test_runtime(run_id="second")

        await task_with_runtime(runtime1)
        await task_with_runtime(runtime2)

        assert results == ["first", "second"]


class TestWorkflowRuntimeMutation:
    """Tests for mutating WorkflowRuntime state."""

    def test_increment_sequence_counters(self) -> None:
        """Test sequence counters can be incremented."""
        runtime = _create_test_runtime()

        runtime.timer_seq += 1
        runtime.activity_seq += 1
        runtime.child_workflow_seq += 1
        runtime.signal_seq += 1

        assert runtime.timer_seq == 1
        assert runtime.activity_seq == 1
        assert runtime.child_workflow_seq == 1
        assert runtime.signal_seq == 1

    def test_add_fired_events(self) -> None:
        """Test fired events can be added."""
        runtime = _create_test_runtime()

        runtime.fired_timers[1] = 1_000_000_000
        runtime.completed_activities[1] = "result"
        runtime.completed_children[1] = {"status": "done"}

        assert runtime.fired_timers == {1: 1_000_000_000}
        assert runtime.completed_activities == {1: "result"}
        assert runtime.completed_children == {1: {"status": "done"}}

    def test_add_commands(self) -> None:
        """Test commands can be added to the list."""
        runtime = _create_test_runtime()

        runtime.commands.append({"type": "start_timer", "id": 1})
        runtime.commands.append({"type": "complete_workflow", "result": "done"})

        assert len(runtime.commands) == 2
        assert runtime.commands[0] == {"type": "start_timer", "id": 1}

    def test_update_time(self) -> None:
        """Test time can be updated."""
        runtime = _create_test_runtime(time_ns=0)

        runtime.time_ns = 5_000_000_000
        assert runtime.time_ns == 5_000_000_000

        runtime.time_ns = 10_000_000_000
        assert runtime.time_ns == 10_000_000_000

    def test_set_workflow_object(self) -> None:
        """Test workflow object can be set."""
        runtime = _create_test_runtime()

        class MockWorkflow:
            value: int = 42

        workflow = MockWorkflow()
        runtime.workflow_object = workflow

        assert runtime.workflow_object is workflow
        assert runtime.workflow_object.value == 42


class TestWorkflowRuntimeWithTrioEvents:
    """Tests for WorkflowRuntime with trio.Event for pending operations."""

    @pytest.mark.trio
    async def test_pending_timer_with_trio_event(self) -> None:
        """Test pending timer can use trio.Event for suspension."""
        runtime = _create_test_runtime()
        event = trio.Event()

        runtime.pending_timers[1] = event

        # Simulate timer fire in another task
        async def fire_timer() -> None:
            await trio.sleep(0.01)
            event.set()

        async with trio.open_nursery() as nursery:
            nursery.start_soon(fire_timer)
            await runtime.pending_timers[1].wait()

        # Timer was fired
        assert event.is_set()

    @pytest.mark.trio
    async def test_multiple_pending_events(self) -> None:
        """Test multiple pending events can be tracked simultaneously."""
        runtime = _create_test_runtime()
        timer_event = trio.Event()
        activity_event = trio.Event()
        child_event = trio.Event()

        runtime.pending_timers[1] = timer_event
        runtime.pending_activities[1] = activity_event
        runtime.pending_children[1] = child_event

        # All should be pending
        assert not timer_event.is_set()
        assert not activity_event.is_set()
        assert not child_event.is_set()

        # Fire them
        timer_event.set()
        activity_event.set()
        child_event.set()

        # All should be complete
        assert timer_event.is_set()
        assert activity_event.is_set()
        assert child_event.is_set()

    @pytest.mark.trio
    async def test_pending_event_cleanup(self) -> None:
        """Test pending events can be cleaned up after completion."""
        runtime = _create_test_runtime()
        event = trio.Event()

        runtime.pending_timers[1] = event
        event.set()

        # Simulate cleanup
        runtime.fired_timers[1] = 1_000_000_000
        del runtime.pending_timers[1]

        assert 1 in runtime.fired_timers
        assert 1 not in runtime.pending_timers


# Phase 2 Tests: Timer Methods


class TestNextTimerSeq:
    """Tests for next_timer_seq method."""

    def test_next_timer_seq_increments_correctly(self) -> None:
        """Test next_timer_seq increments and returns the sequence number."""
        runtime = _create_test_runtime()

        assert runtime.timer_seq == 0

        seq1 = runtime.next_timer_seq()
        assert seq1 == 1
        assert runtime.timer_seq == 1

        seq2 = runtime.next_timer_seq()
        assert seq2 == 2
        assert runtime.timer_seq == 2

        seq3 = runtime.next_timer_seq()
        assert seq3 == 3
        assert runtime.timer_seq == 3

    def test_next_timer_seq_starts_from_initial_value(self) -> None:
        """Test next_timer_seq starts from initial timer_seq value."""
        runtime = _create_test_runtime()
        runtime.timer_seq = 100

        seq = runtime.next_timer_seq()
        assert seq == 101
        assert runtime.timer_seq == 101

    def test_next_timer_seq_independent_between_runtimes(self) -> None:
        """Test next_timer_seq is independent between runtime instances."""
        runtime1 = _create_test_runtime(run_id="run-1")
        runtime2 = _create_test_runtime(run_id="run-2")

        # Increment runtime1 several times
        runtime1.next_timer_seq()
        runtime1.next_timer_seq()
        runtime1.next_timer_seq()

        # runtime2 should still be at 0
        assert runtime2.timer_seq == 0
        seq = runtime2.next_timer_seq()
        assert seq == 1


class TestWorkflowSleep:
    """Tests for workflow_sleep method."""

    @pytest.mark.trio
    async def test_workflow_sleep_creates_timer_command(self) -> None:
        """Test workflow_sleep creates a StartTimerCommand."""
        runtime = _create_test_runtime()

        async def sleep_task() -> None:
            # This will suspend, waiting for the event
            await runtime.workflow_sleep(5.0)

        async def fire_timer() -> None:
            # Wait briefly for the sleep to register
            await trio.testing.wait_all_tasks_blocked()
            # Fire the timer (seq 1, since next_timer_seq increments first)
            runtime.apply_timer_fired(1, 5_000_000_000)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(sleep_task)
            nursery.start_soon(fire_timer)

        # Should have created a StartTimerCommand
        assert len(runtime.commands) == 1
        cmd = runtime.commands[0]
        assert isinstance(cmd, StartTimerCommand)
        assert cmd.timer_id == 1
        assert cmd.duration_ms == 5000

    @pytest.mark.trio
    async def test_workflow_sleep_suspends_on_event(self) -> None:
        """Test workflow_sleep suspends until event is set."""
        runtime = _create_test_runtime()
        sleep_completed = False

        async def sleep_task() -> None:
            nonlocal sleep_completed
            await runtime.workflow_sleep(1.0)
            sleep_completed = True

        async with trio.open_nursery() as nursery:
            nursery.start_soon(sleep_task)
            # Wait for sleep to be blocked
            await trio.testing.wait_all_tasks_blocked()

            # Sleep should not have completed yet
            assert not sleep_completed
            assert 1 in runtime.pending_timers
            assert not runtime.pending_timers[1].is_set()

            # Fire the timer
            runtime.apply_timer_fired(1, 1_000_000_000)

            # Wait for task completion
            await trio.testing.wait_all_tasks_blocked()

        # Now sleep should have completed
        assert sleep_completed

    @pytest.mark.trio
    async def test_apply_timer_fired_wakes_up_suspended_workflow(self) -> None:
        """Test apply_timer_fired wakes up a suspended workflow."""
        runtime = _create_test_runtime()
        wake_times: list[int] = []

        async def sleep_task() -> None:
            await runtime.workflow_sleep(2.5)
            wake_times.append(runtime.time_ns)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(sleep_task)
            await trio.testing.wait_all_tasks_blocked()

            # Timer should be pending
            assert 1 in runtime.pending_timers

            # Fire timer with specific time
            runtime.apply_timer_fired(1, 2_500_000_000)

        # Workflow should have woken with correct time
        assert wake_times == [2_500_000_000]
        assert runtime.time_ns == 2_500_000_000

    @pytest.mark.trio
    async def test_replay_path_timer_already_fired_returns_immediately(self) -> None:
        """Test replay path: when timer already fired, returns immediately."""
        runtime = _create_test_runtime(time_ns=0)

        # Pre-populate fired_timers (simulating replay)
        runtime.fired_timers[1] = 3_000_000_000

        # This should return immediately without suspending
        await runtime.workflow_sleep(3.0)

        # Time should be updated to fire time
        assert runtime.time_ns == 3_000_000_000

        # No command should be emitted (timer already exists in history)
        assert len(runtime.commands) == 0

        # No pending timer (didn't need to wait)
        assert 1 not in runtime.pending_timers

    @pytest.mark.trio
    async def test_workflow_sleep_cleans_up_pending_timer(self) -> None:
        """Test workflow_sleep cleans up the pending timer entry after completion."""
        runtime = _create_test_runtime()

        async def sleep_task() -> None:
            await runtime.workflow_sleep(1.0)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(sleep_task)
            await trio.testing.wait_all_tasks_blocked()

            # Should be pending
            assert 1 in runtime.pending_timers

            # Fire the timer
            runtime.apply_timer_fired(1, 1_000_000_000)

        # After completion, pending timer should be cleaned up
        assert 1 not in runtime.pending_timers
        # But fired_timers should still have the entry
        assert 1 in runtime.fired_timers


class TestApplyTimerFired:
    """Tests for apply_timer_fired method."""

    def test_apply_timer_fired_records_fire_time(self) -> None:
        """Test apply_timer_fired records the fire time."""
        runtime = _create_test_runtime(time_ns=0)

        runtime.apply_timer_fired(1, 5_000_000_000)

        assert runtime.fired_timers[1] == 5_000_000_000
        assert runtime.time_ns == 5_000_000_000

    def test_apply_timer_fired_updates_workflow_time(self) -> None:
        """Test apply_timer_fired updates the workflow time."""
        runtime = _create_test_runtime(time_ns=1_000_000_000)

        runtime.apply_timer_fired(1, 5_000_000_000)

        assert runtime.time_ns == 5_000_000_000

    @pytest.mark.trio
    async def test_apply_timer_fired_sets_pending_event(self) -> None:
        """Test apply_timer_fired sets the pending event if exists."""
        runtime = _create_test_runtime()
        event = trio.Event()
        runtime.pending_timers[1] = event

        assert not event.is_set()

        runtime.apply_timer_fired(1, 1_000_000_000)

        assert event.is_set()
        assert runtime.fired_timers[1] == 1_000_000_000

    def test_apply_timer_fired_no_pending_event(self) -> None:
        """Test apply_timer_fired works when no pending event (replay case)."""
        runtime = _create_test_runtime(time_ns=0)

        # No pending timer for seq 1
        assert 1 not in runtime.pending_timers

        # Should not raise, just record the fire time
        runtime.apply_timer_fired(1, 2_000_000_000)

        assert runtime.fired_timers[1] == 2_000_000_000
        assert runtime.time_ns == 2_000_000_000


class TestMultipleTimers:
    """Tests for multiple timers in sequence and concurrent."""

    @pytest.mark.trio
    async def test_multiple_timers_in_sequence(self) -> None:
        """Test multiple sequential timers work correctly."""
        runtime = _create_test_runtime(time_ns=0)
        times: list[int] = []

        async def workflow() -> None:
            # First sleep
            await runtime.workflow_sleep(1.0)
            times.append(runtime.time_ns)

            # Second sleep
            await runtime.workflow_sleep(2.0)
            times.append(runtime.time_ns)

            # Third sleep
            await runtime.workflow_sleep(3.0)
            times.append(runtime.time_ns)

        async def fire_timers() -> None:
            # Fire timer 1
            await trio.testing.wait_all_tasks_blocked()
            runtime.apply_timer_fired(1, 1_000_000_000)

            # Fire timer 2
            await trio.testing.wait_all_tasks_blocked()
            runtime.apply_timer_fired(2, 3_000_000_000)

            # Fire timer 3
            await trio.testing.wait_all_tasks_blocked()
            runtime.apply_timer_fired(3, 6_000_000_000)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(workflow)
            nursery.start_soon(fire_timers)

        # Should have three commands
        assert len(runtime.commands) == 3
        assert all(isinstance(cmd, StartTimerCommand) for cmd in runtime.commands)
        assert runtime.commands[0].timer_id == 1
        assert runtime.commands[1].timer_id == 2
        assert runtime.commands[2].timer_id == 3

        # Times should be recorded
        assert times == [1_000_000_000, 3_000_000_000, 6_000_000_000]

    @pytest.mark.trio
    async def test_concurrent_timers(self) -> None:
        """Test concurrent timers work correctly."""
        runtime = _create_test_runtime(time_ns=0)
        completed_names: list[str] = []

        async def sleep_and_record(name: str, duration: float) -> None:
            await runtime.workflow_sleep(duration)
            completed_names.append(name)

        async def fire_timers() -> None:
            # Wait for all sleeps to be blocked
            await trio.testing.wait_all_tasks_blocked()

            # Both timers should be pending
            assert 1 in runtime.pending_timers
            assert 2 in runtime.pending_timers

            # Fire timer 1 first (shorter duration)
            runtime.apply_timer_fired(1, 1_000_000_000)

            await trio.testing.wait_all_tasks_blocked()

            # Fire timer 2
            runtime.apply_timer_fired(2, 2_000_000_000)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(sleep_and_record, "fast", 1.0)
            nursery.start_soon(sleep_and_record, "slow", 2.0)
            nursery.start_soon(fire_timers)

        # Both should have completed
        assert "fast" in completed_names
        assert "slow" in completed_names

        # Should have two timer commands
        assert len(runtime.commands) == 2

        # Final time should be from the last timer fired
        assert runtime.time_ns == 2_000_000_000

    @pytest.mark.trio
    async def test_concurrent_timers_with_same_fire_time(self) -> None:
        """Test concurrent timers that fire at the same time."""
        runtime = _create_test_runtime(time_ns=0)
        completed_seqs: list[int] = []

        async def sleep_task(expected_seq: int) -> None:
            await runtime.workflow_sleep(1.0)
            completed_seqs.append(expected_seq)

        async def fire_timers() -> None:
            await trio.testing.wait_all_tasks_blocked()

            # Fire both timers at the same time
            runtime.apply_timer_fired(1, 1_000_000_000)
            runtime.apply_timer_fired(2, 1_000_000_000)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(sleep_task, 1)
            nursery.start_soon(sleep_task, 2)
            nursery.start_soon(fire_timers)

        # Both should have completed
        assert 1 in completed_seqs
        assert 2 in completed_seqs

    @pytest.mark.trio
    async def test_replay_with_multiple_timers(self) -> None:
        """Test replay path with multiple pre-fired timers."""
        runtime = _create_test_runtime(time_ns=0)

        # Pre-populate fired_timers (simulating replay)
        runtime.fired_timers[1] = 1_000_000_000
        runtime.fired_timers[2] = 2_000_000_000
        runtime.fired_timers[3] = 3_000_000_000

        times: list[int] = []

        # All sleeps should return immediately
        await runtime.workflow_sleep(1.0)
        times.append(runtime.time_ns)

        await runtime.workflow_sleep(1.0)
        times.append(runtime.time_ns)

        await runtime.workflow_sleep(1.0)
        times.append(runtime.time_ns)

        # No commands should be emitted
        assert len(runtime.commands) == 0

        # Times should be updated from fired_timers
        assert times == [1_000_000_000, 2_000_000_000, 3_000_000_000]


class TestStartTimerCommand:
    """Tests for StartTimerCommand dataclass."""

    def test_start_timer_command_creation(self) -> None:
        """Test StartTimerCommand can be created with required fields."""
        cmd = StartTimerCommand(timer_id=1, duration_ms=5000)

        assert cmd.timer_id == 1
        assert cmd.duration_ms == 5000

    def test_start_timer_command_equality(self) -> None:
        """Test StartTimerCommand equality comparison."""
        cmd1 = StartTimerCommand(timer_id=1, duration_ms=5000)
        cmd2 = StartTimerCommand(timer_id=1, duration_ms=5000)
        cmd3 = StartTimerCommand(timer_id=2, duration_ms=5000)
        cmd4 = StartTimerCommand(timer_id=1, duration_ms=10000)

        assert cmd1 == cmd2
        assert cmd1 != cmd3
        assert cmd1 != cmd4

    def test_start_timer_command_duration_conversion(self) -> None:
        """Test StartTimerCommand handles various duration values."""
        # 1 second
        cmd1 = StartTimerCommand(timer_id=1, duration_ms=1000)
        assert cmd1.duration_ms == 1000

        # Sub-second (500ms)
        cmd2 = StartTimerCommand(timer_id=2, duration_ms=500)
        assert cmd2.duration_ms == 500

        # Multiple seconds (30s)
        cmd3 = StartTimerCommand(timer_id=3, duration_ms=30000)
        assert cmd3.duration_ms == 30000

        # Very long duration (1 hour)
        cmd4 = StartTimerCommand(timer_id=4, duration_ms=3600000)
        assert cmd4.duration_ms == 3600000


# Phase 4 Tests: Activity Methods


class TestNextActivitySeq:
    """Tests for next_activity_seq method."""

    def test_next_activity_seq_increments_correctly(self) -> None:
        """Test next_activity_seq increments and returns the sequence number."""
        runtime = _create_test_runtime()

        assert runtime.activity_seq == 0

        seq1 = runtime.next_activity_seq()
        assert seq1 == 1
        assert runtime.activity_seq == 1

        seq2 = runtime.next_activity_seq()
        assert seq2 == 2
        assert runtime.activity_seq == 2

        seq3 = runtime.next_activity_seq()
        assert seq3 == 3
        assert runtime.activity_seq == 3

    def test_next_activity_seq_starts_from_initial_value(self) -> None:
        """Test next_activity_seq starts from initial activity_seq value."""
        runtime = _create_test_runtime()
        runtime.activity_seq = 100

        seq = runtime.next_activity_seq()
        assert seq == 101
        assert runtime.activity_seq == 101

    def test_next_activity_seq_independent_between_runtimes(self) -> None:
        """Test next_activity_seq is independent between runtime instances."""
        runtime1 = _create_test_runtime(run_id="run-1")
        runtime2 = _create_test_runtime(run_id="run-2")

        # Increment runtime1 several times
        runtime1.next_activity_seq()
        runtime1.next_activity_seq()
        runtime1.next_activity_seq()

        # runtime2 should still be at 0
        assert runtime2.activity_seq == 0
        seq = runtime2.next_activity_seq()
        assert seq == 1


class TestExecuteActivity:
    """Tests for execute_activity method."""

    @pytest.mark.trio
    async def test_execute_activity_creates_command(self) -> None:
        """Test execute_activity creates a ScheduleActivityCommand."""
        runtime = _create_test_runtime()

        async def activity_task() -> None:
            await runtime.execute_activity("my_activity", ("arg1", "arg2"))

        async def complete_activity() -> None:
            await trio.testing.wait_all_tasks_blocked()
            runtime.apply_activity_resolved(1, result="activity result")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(activity_task)
            nursery.start_soon(complete_activity)

        # Should have created a ScheduleActivityCommand
        assert len(runtime.commands) == 1
        cmd = runtime.commands[0]
        assert isinstance(cmd, ScheduleActivityCommand)
        assert cmd.seq == 1
        assert cmd.activity_type == "my_activity"
        assert cmd.args == ("arg1", "arg2")

    @pytest.mark.trio
    async def test_execute_activity_suspends_on_event(self) -> None:
        """Test execute_activity suspends until event is set."""
        runtime = _create_test_runtime()
        activity_completed = False

        async def activity_task() -> None:
            nonlocal activity_completed
            await runtime.execute_activity("test_activity", ())
            activity_completed = True

        async with trio.open_nursery() as nursery:
            nursery.start_soon(activity_task)
            await trio.testing.wait_all_tasks_blocked()

            # Activity should not have completed yet
            assert not activity_completed
            assert 1 in runtime.pending_activities
            assert not runtime.pending_activities[1].is_set()

            # Complete the activity
            runtime.apply_activity_resolved(1, result="done")

            await trio.testing.wait_all_tasks_blocked()

        # Now activity should have completed
        assert activity_completed

    @pytest.mark.trio
    async def test_execute_activity_returns_result(self) -> None:
        """Test execute_activity returns the activity result."""
        runtime = _create_test_runtime()
        result_holder: list[str] = []

        async def activity_task() -> None:
            result = await runtime.execute_activity("my_activity", ())
            result_holder.append(result)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(activity_task)
            await trio.testing.wait_all_tasks_blocked()
            runtime.apply_activity_resolved(1, result="the result value")

        assert result_holder == ["the result value"]

    @pytest.mark.trio
    async def test_execute_activity_with_timeout_options(self) -> None:
        """Test execute_activity with timeout options."""
        runtime = _create_test_runtime()

        async def activity_task() -> None:
            await runtime.execute_activity(
                "my_activity",
                ("arg",),
                start_to_close_timeout=timedelta(seconds=30),
                schedule_to_close_timeout=timedelta(minutes=5),
                schedule_to_start_timeout=timedelta(seconds=10),
                heartbeat_timeout=timedelta(seconds=5),
            )

        async def complete_activity() -> None:
            await trio.testing.wait_all_tasks_blocked()
            runtime.apply_activity_resolved(1, result="done")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(activity_task)
            nursery.start_soon(complete_activity)

        # Verify command has timeout options (stored as timedelta, not milliseconds)
        cmd = runtime.commands[0]
        assert isinstance(cmd, ScheduleActivityCommand)
        assert cmd.start_to_close_timeout == timedelta(seconds=30)
        assert cmd.schedule_to_close_timeout == timedelta(minutes=5)
        assert cmd.schedule_to_start_timeout == timedelta(seconds=10)
        assert cmd.heartbeat_timeout == timedelta(seconds=5)

    @pytest.mark.trio
    async def test_execute_activity_with_activity_id(self) -> None:
        """Test execute_activity with custom activity_id."""
        runtime = _create_test_runtime()

        async def activity_task() -> None:
            await runtime.execute_activity(
                "my_activity",
                (),
                activity_id="custom-activity-id",
            )

        async def complete_activity() -> None:
            await trio.testing.wait_all_tasks_blocked()
            runtime.apply_activity_resolved(1, result="done")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(activity_task)
            nursery.start_soon(complete_activity)

        cmd = runtime.commands[0]
        assert isinstance(cmd, ScheduleActivityCommand)
        assert cmd.activity_id == "custom-activity-id"

    @pytest.mark.trio
    async def test_execute_activity_with_task_queue(self) -> None:
        """Test execute_activity with custom task_queue."""
        runtime = _create_test_runtime()

        async def activity_task() -> None:
            await runtime.execute_activity(
                "my_activity",
                (),
                task_queue="custom-task-queue",
            )

        async def complete_activity() -> None:
            await trio.testing.wait_all_tasks_blocked()
            runtime.apply_activity_resolved(1, result="done")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(activity_task)
            nursery.start_soon(complete_activity)

        cmd = runtime.commands[0]
        assert isinstance(cmd, ScheduleActivityCommand)
        assert cmd.task_queue == "custom-task-queue"


class TestApplyActivityResolved:
    """Tests for apply_activity_resolved method."""

    def test_apply_activity_resolved_stores_result(self) -> None:
        """Test apply_activity_resolved stores the result."""
        runtime = _create_test_runtime()

        runtime.apply_activity_resolved(1, result="my result")

        assert runtime.completed_activities[1] == "my result"

    def test_apply_activity_resolved_stores_error(self) -> None:
        """Test apply_activity_resolved stores the error."""
        runtime = _create_test_runtime()
        error = ValueError("activity failed")

        runtime.apply_activity_resolved(1, error=error)

        assert runtime.completed_activities[1] is error

    @pytest.mark.trio
    async def test_apply_activity_resolved_wakes_up_workflow(self) -> None:
        """Test apply_activity_resolved wakes up a suspended workflow."""
        runtime = _create_test_runtime()
        results: list[str] = []

        async def activity_task() -> None:
            result = await runtime.execute_activity("my_activity", ())
            results.append(result)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(activity_task)
            await trio.testing.wait_all_tasks_blocked()

            # Activity should be pending
            assert 1 in runtime.pending_activities

            # Complete the activity
            runtime.apply_activity_resolved(1, result="wake up!")

        # Workflow should have woken with correct result
        assert results == ["wake up!"]

    @pytest.mark.trio
    async def test_apply_activity_resolved_sets_pending_event(self) -> None:
        """Test apply_activity_resolved sets the pending event if exists."""
        runtime = _create_test_runtime()
        event = trio.Event()
        runtime.pending_activities[1] = event

        assert not event.is_set()

        runtime.apply_activity_resolved(1, result="done")

        assert event.is_set()
        assert runtime.completed_activities[1] == "done"

    def test_apply_activity_resolved_no_pending_event(self) -> None:
        """Test apply_activity_resolved works when no pending event (replay case)."""
        runtime = _create_test_runtime()

        # No pending activity for seq 1
        assert 1 not in runtime.pending_activities

        # Should not raise, just store the result
        runtime.apply_activity_resolved(1, result="replayed result")

        assert runtime.completed_activities[1] == "replayed result"


class TestActivityFailure:
    """Tests for activity failure handling."""

    @pytest.mark.trio
    async def test_activity_failure_raises_exception(self) -> None:
        """Test activity failure raises the exception in the workflow."""
        runtime = _create_test_runtime()
        caught_exception: BaseException | None = None

        async def activity_task() -> None:
            nonlocal caught_exception
            try:
                await runtime.execute_activity("failing_activity", ())
            except Exception as e:
                caught_exception = e

        async with trio.open_nursery() as nursery:
            nursery.start_soon(activity_task)
            await trio.testing.wait_all_tasks_blocked()

            # Fail the activity
            error = ValueError("Activity failed with error")
            runtime.apply_activity_resolved(1, error=error)

        assert caught_exception is error
        assert str(caught_exception) == "Activity failed with error"

    @pytest.mark.trio
    async def test_activity_failure_cleans_up_pending(self) -> None:
        """Test activity failure cleans up the pending entry."""
        runtime = _create_test_runtime()

        async def activity_task() -> None:
            try:
                await runtime.execute_activity("failing_activity", ())
            except Exception:
                pass

        async with trio.open_nursery() as nursery:
            nursery.start_soon(activity_task)
            await trio.testing.wait_all_tasks_blocked()

            # Should be pending
            assert 1 in runtime.pending_activities

            # Fail the activity
            runtime.apply_activity_resolved(1, error=RuntimeError("failed"))

        # After completion, pending activity should be cleaned up
        assert 1 not in runtime.pending_activities
        # But completed_activities should still have the entry
        assert 1 in runtime.completed_activities


class TestActivityReplay:
    """Tests for activity replay path."""

    @pytest.mark.trio
    async def test_replay_path_activity_already_completed_returns_immediately(
        self,
    ) -> None:
        """Test replay path: when activity already completed, returns immediately."""
        runtime = _create_test_runtime()

        # Pre-populate completed_activities (simulating replay)
        runtime.completed_activities[1] = "replayed result"

        # This should return immediately without suspending
        result = await runtime.execute_activity("my_activity", ())

        # Should get the replayed result
        assert result == "replayed result"

        # No command should be emitted
        assert len(runtime.commands) == 0

        # No pending activity (didn't need to wait)
        assert 1 not in runtime.pending_activities

    @pytest.mark.trio
    async def test_replay_path_activity_already_failed_raises_immediately(self) -> None:
        """Test replay path: when activity already failed, raises immediately."""
        runtime = _create_test_runtime()

        # Pre-populate with error (simulating replay)
        error = ValueError("replayed error")
        runtime.completed_activities[1] = error

        # This should raise immediately without suspending
        with pytest.raises(ValueError, match="replayed error"):
            await runtime.execute_activity("my_activity", ())

        # No command should be emitted
        assert len(runtime.commands) == 0

        # No pending activity (didn't need to wait)
        assert 1 not in runtime.pending_activities

    @pytest.mark.trio
    async def test_replay_with_multiple_activities(self) -> None:
        """Test replay path with multiple pre-completed activities."""
        runtime = _create_test_runtime()

        # Pre-populate completed_activities (simulating replay)
        runtime.completed_activities[1] = "result1"
        runtime.completed_activities[2] = "result2"
        runtime.completed_activities[3] = ValueError("error3")

        results: list[str] = []
        errors: list[BaseException] = []

        # First activity should return immediately
        result1 = await runtime.execute_activity("activity1", ())
        results.append(result1)

        # Second activity should return immediately
        result2 = await runtime.execute_activity("activity2", ())
        results.append(result2)

        # Third activity should raise immediately
        try:
            await runtime.execute_activity("activity3", ())
        except ValueError as e:
            errors.append(e)

        # No commands should be emitted
        assert len(runtime.commands) == 0

        # Results should be from replay
        assert results == ["result1", "result2"]
        assert len(errors) == 1
        assert str(errors[0]) == "error3"


class TestConcurrentActivities:
    """Tests for concurrent activities."""

    @pytest.mark.trio
    async def test_concurrent_activities(self) -> None:
        """Test concurrent activities work correctly."""
        runtime = _create_test_runtime()
        completed_results: list[str] = []

        async def activity_and_record(activity_name: str) -> None:
            result = await runtime.execute_activity(activity_name, ())
            completed_results.append(result)

        async def complete_activities() -> None:
            # Wait for all activities to be blocked
            await trio.testing.wait_all_tasks_blocked()

            # Both activities should be pending
            assert 1 in runtime.pending_activities
            assert 2 in runtime.pending_activities

            # Complete activity 1 first
            runtime.apply_activity_resolved(1, result="result_1")

            await trio.testing.wait_all_tasks_blocked()

            # Complete activity 2
            runtime.apply_activity_resolved(2, result="result_2")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(activity_and_record, "activity_a")
            nursery.start_soon(activity_and_record, "activity_b")
            nursery.start_soon(complete_activities)

        # Both should have completed with results
        assert "result_1" in completed_results
        assert "result_2" in completed_results
        assert len(completed_results) == 2

        # Should have two activity commands
        assert len(runtime.commands) == 2

    @pytest.mark.trio
    async def test_concurrent_activities_with_different_outcomes(self) -> None:
        """Test concurrent activities with one success and one failure."""
        runtime = _create_test_runtime()
        results: list[str] = []
        errors: list[BaseException] = []

        async def activity_task(activity_name: str, is_success: bool) -> None:
            try:
                result = await runtime.execute_activity(activity_name, ())
                results.append(result)
            except Exception as e:
                errors.append(e)

        async def complete_activities() -> None:
            await trio.testing.wait_all_tasks_blocked()

            # Both activities should be pending
            assert 1 in runtime.pending_activities
            assert 2 in runtime.pending_activities

            # Complete first activity with success
            runtime.apply_activity_resolved(1, result="success!")

            await trio.testing.wait_all_tasks_blocked()

            # Complete second activity with failure
            runtime.apply_activity_resolved(2, error=RuntimeError("failed!"))

        async with trio.open_nursery() as nursery:
            nursery.start_soon(activity_task, "activity_1", True)
            nursery.start_soon(activity_task, "activity_2", False)
            nursery.start_soon(complete_activities)

        # One should succeed, one should fail
        assert len(results) == 1
        assert results[0] == "success!"
        assert len(errors) == 1
        assert str(errors[0]) == "failed!"


class TestScheduleActivityCommand:
    """Tests for ScheduleActivityCommand dataclass.

    Note: ScheduleActivityCommand is imported from _activation, which uses:
    - `args` (tuple) instead of `arguments`
    - `activity_id` (str, required) instead of optional
    - timedelta for timeouts instead of milliseconds
    - includes `retry_policy` field
    """

    def test_schedule_activity_command_creation(self) -> None:
        """Test ScheduleActivityCommand can be created with required fields."""
        cmd = ScheduleActivityCommand(
            seq=1,
            activity_id="act-1",
            activity_type="my_activity",
            args=("arg1", "arg2"),
        )

        assert cmd.seq == 1
        assert cmd.activity_id == "act-1"
        assert cmd.activity_type == "my_activity"
        assert cmd.args == ("arg1", "arg2")
        assert cmd.task_queue is None
        assert cmd.schedule_to_close_timeout is None
        assert cmd.schedule_to_start_timeout is None
        assert cmd.start_to_close_timeout is None
        assert cmd.heartbeat_timeout is None
        assert cmd.retry_policy is None

    def test_schedule_activity_command_with_all_options(self) -> None:
        """Test ScheduleActivityCommand with all optional fields."""
        from datetime import timedelta

        from temporalio.common import RetryPolicy

        cmd = ScheduleActivityCommand(
            seq=42,
            activity_id="custom-id",
            activity_type="complex_activity",
            args=("a", "b", "c"),
            task_queue="custom-queue",
            schedule_to_close_timeout=timedelta(minutes=5),
            schedule_to_start_timeout=timedelta(seconds=60),
            start_to_close_timeout=timedelta(seconds=30),
            heartbeat_timeout=timedelta(seconds=5),
            retry_policy=RetryPolicy(maximum_attempts=3),
        )

        assert cmd.seq == 42
        assert cmd.activity_id == "custom-id"
        assert cmd.activity_type == "complex_activity"
        assert cmd.args == ("a", "b", "c")
        assert cmd.task_queue == "custom-queue"
        assert cmd.schedule_to_close_timeout == timedelta(minutes=5)
        assert cmd.schedule_to_start_timeout == timedelta(seconds=60)
        assert cmd.start_to_close_timeout == timedelta(seconds=30)
        assert cmd.heartbeat_timeout == timedelta(seconds=5)
        assert cmd.retry_policy is not None
        assert cmd.retry_policy.maximum_attempts == 3

    def test_schedule_activity_command_equality(self) -> None:
        """Test ScheduleActivityCommand equality comparison."""
        cmd1 = ScheduleActivityCommand(seq=1, activity_id="1", activity_type="test", args=("arg",))
        cmd2 = ScheduleActivityCommand(seq=1, activity_id="1", activity_type="test", args=("arg",))
        cmd3 = ScheduleActivityCommand(seq=2, activity_id="2", activity_type="test", args=("arg",))
        cmd4 = ScheduleActivityCommand(seq=1, activity_id="1", activity_type="other", args=("arg",))

        assert cmd1 == cmd2
        assert cmd1 != cmd3
        assert cmd1 != cmd4


# Phase 5 Tests: Signals and Queries


class TestRegisterSignalHandler:
    """Tests for register_signal_handler method."""

    def test_register_signal_handler(self) -> None:
        """Test register_signal_handler stores the handler."""
        runtime = _create_test_runtime()

        def my_signal_handler(value: str) -> None:
            pass

        assert runtime.signal_handlers == {}

        runtime.register_signal_handler("my_signal", my_signal_handler)

        assert "my_signal" in runtime.signal_handlers
        assert runtime.signal_handlers["my_signal"] is my_signal_handler

    def test_register_multiple_signal_handlers(self) -> None:
        """Test registering multiple signal handlers."""
        runtime = _create_test_runtime()

        def handler1(value: str) -> None:
            pass

        def handler2(x: int, y: int) -> None:
            pass

        runtime.register_signal_handler("signal1", handler1)
        runtime.register_signal_handler("signal2", handler2)

        assert len(runtime.signal_handlers) == 2
        assert runtime.signal_handlers["signal1"] is handler1
        assert runtime.signal_handlers["signal2"] is handler2

    def test_register_signal_handler_replaces_existing(self) -> None:
        """Test register_signal_handler replaces an existing handler."""
        runtime = _create_test_runtime()

        def handler1(value: str) -> None:
            pass

        def handler2(value: str) -> None:
            pass

        runtime.register_signal_handler("my_signal", handler1)
        runtime.register_signal_handler("my_signal", handler2)

        assert runtime.signal_handlers["my_signal"] is handler2

    def test_register_async_signal_handler(self) -> None:
        """Test registering an async signal handler."""
        runtime = _create_test_runtime()

        async def async_handler(value: str) -> None:
            pass

        runtime.register_signal_handler("async_signal", async_handler)

        assert runtime.signal_handlers["async_signal"] is async_handler

    def test_signal_handlers_independent_between_runtimes(self) -> None:
        """Test signal handlers are independent between runtime instances."""
        runtime1 = _create_test_runtime(run_id="run-1")
        runtime2 = _create_test_runtime(run_id="run-2")

        def handler1() -> None:
            pass

        runtime1.register_signal_handler("signal", handler1)

        assert "signal" in runtime1.signal_handlers
        assert "signal" not in runtime2.signal_handlers


class TestRegisterQueryHandler:
    """Tests for register_query_handler method."""

    def test_register_query_handler(self) -> None:
        """Test register_query_handler stores the handler."""
        runtime = _create_test_runtime()

        def my_query_handler() -> str:
            return "query result"

        assert runtime.query_handlers == {}

        runtime.register_query_handler("my_query", my_query_handler)

        assert "my_query" in runtime.query_handlers
        assert runtime.query_handlers["my_query"] is my_query_handler

    def test_register_multiple_query_handlers(self) -> None:
        """Test registering multiple query handlers."""
        runtime = _create_test_runtime()

        def get_status() -> str:
            return "running"

        def get_count() -> int:
            return 42

        runtime.register_query_handler("get_status", get_status)
        runtime.register_query_handler("get_count", get_count)

        assert len(runtime.query_handlers) == 2
        assert runtime.query_handlers["get_status"] is get_status
        assert runtime.query_handlers["get_count"] is get_count

    def test_register_query_handler_replaces_existing(self) -> None:
        """Test register_query_handler replaces an existing handler."""
        runtime = _create_test_runtime()

        def handler1() -> str:
            return "v1"

        def handler2() -> str:
            return "v2"

        runtime.register_query_handler("my_query", handler1)
        runtime.register_query_handler("my_query", handler2)

        assert runtime.query_handlers["my_query"] is handler2

    def test_query_handlers_independent_between_runtimes(self) -> None:
        """Test query handlers are independent between runtime instances."""
        runtime1 = _create_test_runtime(run_id="run-1")
        runtime2 = _create_test_runtime(run_id="run-2")

        def handler1() -> str:
            return "result"

        runtime1.register_query_handler("query", handler1)

        assert "query" in runtime1.query_handlers
        assert "query" not in runtime2.query_handlers


class TestQuerySuccessCommand:
    """Tests for QuerySuccessCommand dataclass."""

    def test_query_success_command_creation(self) -> None:
        """Test QuerySuccessCommand can be created with required fields."""
        cmd = QuerySuccessCommand(query_id="q-123", result="query result")

        assert cmd.query_id == "q-123"
        assert cmd.result == "query result"

    def test_query_success_command_with_various_result_types(self) -> None:
        """Test QuerySuccessCommand with various result types."""
        # String result
        cmd1 = QuerySuccessCommand(query_id="q-1", result="string")
        assert cmd1.result == "string"

        # Integer result
        cmd2 = QuerySuccessCommand(query_id="q-2", result=42)
        assert cmd2.result == 42

        # List result
        cmd3 = QuerySuccessCommand(query_id="q-3", result=[1, 2, 3])
        assert cmd3.result == [1, 2, 3]

        # Dict result
        cmd4 = QuerySuccessCommand(query_id="q-4", result={"key": "value"})
        assert cmd4.result == {"key": "value"}

        # None result
        cmd5 = QuerySuccessCommand(query_id="q-5", result=None)
        assert cmd5.result is None

    def test_query_success_command_equality(self) -> None:
        """Test QuerySuccessCommand equality comparison."""
        cmd1 = QuerySuccessCommand(query_id="q-1", result="result")
        cmd2 = QuerySuccessCommand(query_id="q-1", result="result")
        cmd3 = QuerySuccessCommand(query_id="q-2", result="result")
        cmd4 = QuerySuccessCommand(query_id="q-1", result="other")

        assert cmd1 == cmd2
        assert cmd1 != cmd3
        assert cmd1 != cmd4


class TestQueryFailureCommand:
    """Tests for QueryFailureCommand dataclass."""

    def test_query_failure_command_creation(self) -> None:
        """Test QueryFailureCommand can be created with required fields."""
        error = ValueError("Query failed")
        cmd = QueryFailureCommand(query_id="q-123", error=error)

        assert cmd.query_id == "q-123"
        assert cmd.error is error

    def test_query_failure_command_with_various_error_types(self) -> None:
        """Test QueryFailureCommand with various exception types."""
        # ValueError
        error1 = ValueError("invalid value")
        cmd1 = QueryFailureCommand(query_id="q-1", error=error1)
        assert cmd1.error is error1

        # RuntimeError
        error2 = RuntimeError("runtime error")
        cmd2 = QueryFailureCommand(query_id="q-2", error=error2)
        assert cmd2.error is error2

        # Custom exception
        class CustomError(Exception):
            pass

        error3 = CustomError("custom error")
        cmd3 = QueryFailureCommand(query_id="q-3", error=error3)
        assert cmd3.error is error3

    def test_query_failure_command_preserves_exception_message(self) -> None:
        """Test QueryFailureCommand preserves the exception message."""
        error = ValueError("Detailed error message")
        cmd = QueryFailureCommand(query_id="q-1", error=error)

        assert str(cmd.error) == "Detailed error message"

    def test_query_failure_command_equality(self) -> None:
        """Test QueryFailureCommand equality with same exception instance."""
        error = ValueError("error")
        cmd1 = QueryFailureCommand(query_id="q-1", error=error)
        cmd2 = QueryFailureCommand(query_id="q-1", error=error)
        cmd3 = QueryFailureCommand(query_id="q-2", error=error)

        assert cmd1 == cmd2
        assert cmd1 != cmd3


# Phase 6 Tests: Child Workflows


class TestNextChildWorkflowSeq:
    """Tests for next_child_workflow_seq method."""

    def test_next_child_workflow_seq_increments_correctly(self) -> None:
        """Test next_child_workflow_seq increments and returns the sequence number."""
        runtime = _create_test_runtime()

        assert runtime.child_workflow_seq == 0

        seq1 = runtime.next_child_workflow_seq()
        assert seq1 == 1
        assert runtime.child_workflow_seq == 1

        seq2 = runtime.next_child_workflow_seq()
        assert seq2 == 2
        assert runtime.child_workflow_seq == 2

        seq3 = runtime.next_child_workflow_seq()
        assert seq3 == 3
        assert runtime.child_workflow_seq == 3

    def test_next_child_workflow_seq_starts_from_initial_value(self) -> None:
        """Test next_child_workflow_seq starts from initial child_workflow_seq value."""
        runtime = _create_test_runtime()
        runtime.child_workflow_seq = 100

        seq = runtime.next_child_workflow_seq()
        assert seq == 101
        assert runtime.child_workflow_seq == 101

    def test_next_child_workflow_seq_independent_between_runtimes(self) -> None:
        """Test next_child_workflow_seq is independent between runtime instances."""
        runtime1 = _create_test_runtime(run_id="run-1")
        runtime2 = _create_test_runtime(run_id="run-2")

        # Increment runtime1 several times
        runtime1.next_child_workflow_seq()
        runtime1.next_child_workflow_seq()
        runtime1.next_child_workflow_seq()

        # runtime2 should still be at 0
        assert runtime2.child_workflow_seq == 0
        seq = runtime2.next_child_workflow_seq()
        assert seq == 1


class TestExecuteChildWorkflow:
    """Tests for execute_child_workflow method."""

    @pytest.mark.trio
    async def test_execute_child_workflow_creates_command(self) -> None:
        """Test execute_child_workflow creates a StartChildWorkflowCommand."""
        runtime = _create_test_runtime()

        async def child_workflow_task() -> None:
            await runtime.execute_child_workflow(
                "ChildWorkflow", "child-wf-1", ("arg1", "arg2")
            )

        async def complete_child() -> None:
            await trio.testing.wait_all_tasks_blocked()
            runtime.apply_child_workflow_resolved(1, result="child result")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(child_workflow_task)
            nursery.start_soon(complete_child)

        # Should have created a StartChildWorkflowCommand
        assert len(runtime.commands) == 1
        cmd = runtime.commands[0]
        assert isinstance(cmd, StartChildWorkflowCommand)
        assert cmd.seq == 1
        assert cmd.workflow_type == "ChildWorkflow"
        assert cmd.workflow_id == "child-wf-1"
        assert cmd.args == ("arg1", "arg2")

    @pytest.mark.trio
    async def test_execute_child_workflow_suspends_on_event(self) -> None:
        """Test execute_child_workflow suspends until event is set."""
        runtime = _create_test_runtime()
        child_completed = False

        async def child_workflow_task() -> None:
            nonlocal child_completed
            await runtime.execute_child_workflow("ChildWorkflow", "child-wf-1", ())
            child_completed = True

        async with trio.open_nursery() as nursery:
            nursery.start_soon(child_workflow_task)
            await trio.testing.wait_all_tasks_blocked()

            # Child workflow should not have completed yet
            assert not child_completed
            assert 1 in runtime.pending_children
            assert not runtime.pending_children[1].is_set()

            # Complete the child workflow
            runtime.apply_child_workflow_resolved(1, result="done")

            await trio.testing.wait_all_tasks_blocked()

        # Now child workflow should have completed
        assert child_completed

    @pytest.mark.trio
    async def test_execute_child_workflow_returns_result(self) -> None:
        """Test execute_child_workflow returns the child workflow result."""
        runtime = _create_test_runtime()
        result_holder: list[str] = []

        async def child_workflow_task() -> None:
            result = await runtime.execute_child_workflow(
                "ChildWorkflow", "child-wf-1", ()
            )
            result_holder.append(result)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(child_workflow_task)
            await trio.testing.wait_all_tasks_blocked()
            runtime.apply_child_workflow_resolved(1, result="the child result")

        assert result_holder == ["the child result"]

    @pytest.mark.trio
    async def test_execute_child_workflow_with_timeout_options(self) -> None:
        """Test execute_child_workflow with timeout options."""
        from datetime import timedelta

        runtime = _create_test_runtime()

        async def child_workflow_task() -> None:
            await runtime.execute_child_workflow(
                "ChildWorkflow",
                "child-wf-1",
                ("arg",),
                execution_timeout=timedelta(minutes=10),
                run_timeout=timedelta(minutes=5),
                task_timeout=timedelta(seconds=30),
            )

        async def complete_child() -> None:
            await trio.testing.wait_all_tasks_blocked()
            runtime.apply_child_workflow_resolved(1, result="done")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(child_workflow_task)
            nursery.start_soon(complete_child)

        # Verify command has timeout options
        cmd = runtime.commands[0]
        assert isinstance(cmd, StartChildWorkflowCommand)
        assert cmd.execution_timeout == timedelta(minutes=10)
        assert cmd.run_timeout == timedelta(minutes=5)
        assert cmd.task_timeout == timedelta(seconds=30)

    @pytest.mark.trio
    async def test_execute_child_workflow_with_task_queue(self) -> None:
        """Test execute_child_workflow with custom task_queue."""
        runtime = _create_test_runtime()

        async def child_workflow_task() -> None:
            await runtime.execute_child_workflow(
                "ChildWorkflow",
                "child-wf-1",
                (),
                task_queue="custom-child-queue",
            )

        async def complete_child() -> None:
            await trio.testing.wait_all_tasks_blocked()
            runtime.apply_child_workflow_resolved(1, result="done")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(child_workflow_task)
            nursery.start_soon(complete_child)

        cmd = runtime.commands[0]
        assert isinstance(cmd, StartChildWorkflowCommand)
        assert cmd.task_queue == "custom-child-queue"


class TestApplyChildWorkflowResolved:
    """Tests for apply_child_workflow_resolved method."""

    def test_apply_child_workflow_resolved_stores_result(self) -> None:
        """Test apply_child_workflow_resolved stores the result."""
        runtime = _create_test_runtime()

        runtime.apply_child_workflow_resolved(1, result="child result")

        assert runtime.completed_children[1] == "child result"

    def test_apply_child_workflow_resolved_stores_error(self) -> None:
        """Test apply_child_workflow_resolved stores the error."""
        runtime = _create_test_runtime()
        error = ValueError("child workflow failed")

        runtime.apply_child_workflow_resolved(1, error=error)

        assert runtime.completed_children[1] is error

    @pytest.mark.trio
    async def test_apply_child_workflow_resolved_wakes_up_workflow(self) -> None:
        """Test apply_child_workflow_resolved wakes up a suspended workflow."""
        runtime = _create_test_runtime()
        results: list[str] = []

        async def child_workflow_task() -> None:
            result = await runtime.execute_child_workflow(
                "ChildWorkflow", "child-wf-1", ()
            )
            results.append(result)

        async with trio.open_nursery() as nursery:
            nursery.start_soon(child_workflow_task)
            await trio.testing.wait_all_tasks_blocked()

            # Child workflow should be pending
            assert 1 in runtime.pending_children

            # Complete the child workflow
            runtime.apply_child_workflow_resolved(1, result="wake up!")

        # Workflow should have woken with correct result
        assert results == ["wake up!"]

    @pytest.mark.trio
    async def test_apply_child_workflow_resolved_sets_pending_event(self) -> None:
        """Test apply_child_workflow_resolved sets the pending event if exists."""
        runtime = _create_test_runtime()
        event = trio.Event()
        runtime.pending_children[1] = event

        assert not event.is_set()

        runtime.apply_child_workflow_resolved(1, result="done")

        assert event.is_set()
        assert runtime.completed_children[1] == "done"

    def test_apply_child_workflow_resolved_no_pending_event(self) -> None:
        """Test apply_child_workflow_resolved works when no pending event (replay)."""
        runtime = _create_test_runtime()

        # No pending child for seq 1
        assert 1 not in runtime.pending_children

        # Should not raise, just store the result
        runtime.apply_child_workflow_resolved(1, result="replayed result")

        assert runtime.completed_children[1] == "replayed result"


class TestChildWorkflowFailure:
    """Tests for child workflow failure handling."""

    @pytest.mark.trio
    async def test_child_workflow_failure_raises_exception(self) -> None:
        """Test child workflow failure raises the exception in the parent workflow."""
        runtime = _create_test_runtime()
        caught_exception: BaseException | None = None

        async def child_workflow_task() -> None:
            nonlocal caught_exception
            try:
                await runtime.execute_child_workflow(
                    "FailingChildWorkflow", "child-wf-1", ()
                )
            except Exception as e:
                caught_exception = e

        async with trio.open_nursery() as nursery:
            nursery.start_soon(child_workflow_task)
            await trio.testing.wait_all_tasks_blocked()

            # Fail the child workflow
            error = ValueError("Child workflow failed with error")
            runtime.apply_child_workflow_resolved(1, error=error)

        assert caught_exception is error
        assert str(caught_exception) == "Child workflow failed with error"

    @pytest.mark.trio
    async def test_child_workflow_failure_cleans_up_pending(self) -> None:
        """Test child workflow failure cleans up the pending entry."""
        runtime = _create_test_runtime()

        async def child_workflow_task() -> None:
            try:
                await runtime.execute_child_workflow(
                    "FailingChildWorkflow", "child-wf-1", ()
                )
            except Exception:
                pass

        async with trio.open_nursery() as nursery:
            nursery.start_soon(child_workflow_task)
            await trio.testing.wait_all_tasks_blocked()

            # Should be pending
            assert 1 in runtime.pending_children

            # Fail the child workflow
            runtime.apply_child_workflow_resolved(1, error=RuntimeError("failed"))

        # After completion, pending child should be cleaned up
        assert 1 not in runtime.pending_children
        # But completed_children should still have the entry
        assert 1 in runtime.completed_children


class TestChildWorkflowReplay:
    """Tests for child workflow replay path."""

    @pytest.mark.trio
    async def test_replay_path_child_already_completed_returns_immediately(
        self,
    ) -> None:
        """Test replay path: when child already completed, returns immediately."""
        runtime = _create_test_runtime()

        # Pre-populate completed_children (simulating replay)
        runtime.completed_children[1] = "replayed result"

        # This should return immediately without suspending
        result = await runtime.execute_child_workflow("ChildWorkflow", "child-wf-1", ())

        # Should get the replayed result
        assert result == "replayed result"

        # No command should be emitted
        assert len(runtime.commands) == 0

        # No pending child (didn't need to wait)
        assert 1 not in runtime.pending_children

    @pytest.mark.trio
    async def test_replay_path_child_already_failed_raises_immediately(self) -> None:
        """Test replay path: when child already failed, raises immediately."""
        runtime = _create_test_runtime()

        # Pre-populate with error (simulating replay)
        error = ValueError("replayed error")
        runtime.completed_children[1] = error

        # This should raise immediately without suspending
        with pytest.raises(ValueError, match="replayed error"):
            await runtime.execute_child_workflow("ChildWorkflow", "child-wf-1", ())

        # No command should be emitted
        assert len(runtime.commands) == 0

        # No pending child (didn't need to wait)
        assert 1 not in runtime.pending_children

    @pytest.mark.trio
    async def test_replay_with_multiple_child_workflows(self) -> None:
        """Test replay path with multiple pre-completed child workflows."""
        runtime = _create_test_runtime()

        # Pre-populate completed_children (simulating replay)
        runtime.completed_children[1] = "result1"
        runtime.completed_children[2] = "result2"
        runtime.completed_children[3] = ValueError("error3")

        results: list[str] = []
        errors: list[BaseException] = []

        # First child workflow should return immediately
        result1 = await runtime.execute_child_workflow("Child1", "child-1", ())
        results.append(result1)

        # Second child workflow should return immediately
        result2 = await runtime.execute_child_workflow("Child2", "child-2", ())
        results.append(result2)

        # Third child workflow should raise immediately
        try:
            await runtime.execute_child_workflow("Child3", "child-3", ())
        except ValueError as e:
            errors.append(e)

        # No commands should be emitted
        assert len(runtime.commands) == 0

        # Results should be from replay
        assert results == ["result1", "result2"]
        assert len(errors) == 1
        assert str(errors[0]) == "error3"


class TestConcurrentChildWorkflows:
    """Tests for concurrent child workflows."""

    @pytest.mark.trio
    async def test_concurrent_child_workflows(self) -> None:
        """Test concurrent child workflows work correctly."""
        runtime = _create_test_runtime()
        completed_results: list[str] = []

        async def child_and_record(workflow_id: str) -> None:
            result = await runtime.execute_child_workflow(
                "ChildWorkflow", workflow_id, ()
            )
            completed_results.append(result)

        async def complete_children() -> None:
            # Wait for all children to be blocked
            await trio.testing.wait_all_tasks_blocked()

            # Both children should be pending
            assert 1 in runtime.pending_children
            assert 2 in runtime.pending_children

            # Complete child 1 first
            runtime.apply_child_workflow_resolved(1, result="result_1")

            await trio.testing.wait_all_tasks_blocked()

            # Complete child 2
            runtime.apply_child_workflow_resolved(2, result="result_2")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(child_and_record, "child-a")
            nursery.start_soon(child_and_record, "child-b")
            nursery.start_soon(complete_children)

        # Both should have completed with results
        assert "result_1" in completed_results
        assert "result_2" in completed_results
        assert len(completed_results) == 2

        # Should have two child workflow commands
        assert len(runtime.commands) == 2

    @pytest.mark.trio
    async def test_concurrent_child_workflows_with_different_outcomes(self) -> None:
        """Test concurrent child workflows with one success and one failure."""
        runtime = _create_test_runtime()
        results: list[str] = []
        errors: list[BaseException] = []

        async def child_task(workflow_id: str) -> None:
            try:
                result = await runtime.execute_child_workflow(
                    "ChildWorkflow", workflow_id, ()
                )
                results.append(result)
            except Exception as e:
                errors.append(e)

        async def complete_children() -> None:
            await trio.testing.wait_all_tasks_blocked()

            # Both children should be pending
            assert 1 in runtime.pending_children
            assert 2 in runtime.pending_children

            # Complete first child with success
            runtime.apply_child_workflow_resolved(1, result="success!")

            await trio.testing.wait_all_tasks_blocked()

            # Complete second child with failure
            runtime.apply_child_workflow_resolved(2, error=RuntimeError("failed!"))

        async with trio.open_nursery() as nursery:
            nursery.start_soon(child_task, "child-1")
            nursery.start_soon(child_task, "child-2")
            nursery.start_soon(complete_children)

        # One should succeed, one should fail
        assert len(results) == 1
        assert results[0] == "success!"
        assert len(errors) == 1
        assert str(errors[0]) == "failed!"


class TestStartChildWorkflowCommand:
    """Tests for StartChildWorkflowCommand dataclass."""

    def test_start_child_workflow_command_creation(self) -> None:
        """Test StartChildWorkflowCommand can be created with required fields."""
        cmd = StartChildWorkflowCommand(
            seq=1,
            workflow_type="ChildWorkflow",
            workflow_id="child-wf-1",
            args=("arg1", "arg2"),
        )

        assert cmd.seq == 1
        assert cmd.workflow_type == "ChildWorkflow"
        assert cmd.workflow_id == "child-wf-1"
        assert cmd.args == ("arg1", "arg2")
        assert cmd.task_queue is None
        assert cmd.execution_timeout is None
        assert cmd.run_timeout is None
        assert cmd.task_timeout is None

    def test_start_child_workflow_command_with_all_options(self) -> None:
        """Test StartChildWorkflowCommand with all optional fields."""
        from datetime import timedelta

        cmd = StartChildWorkflowCommand(
            seq=42,
            workflow_type="ComplexChild",
            workflow_id="child-42",
            args=("a", "b", "c"),
            task_queue="child-queue",
            execution_timeout=timedelta(minutes=10),
            run_timeout=timedelta(minutes=5),
            task_timeout=timedelta(seconds=30),
        )

        assert cmd.seq == 42
        assert cmd.workflow_type == "ComplexChild"
        assert cmd.workflow_id == "child-42"
        assert cmd.args == ("a", "b", "c")
        assert cmd.task_queue == "child-queue"
        assert cmd.execution_timeout == timedelta(minutes=10)
        assert cmd.run_timeout == timedelta(minutes=5)
        assert cmd.task_timeout == timedelta(seconds=30)

    def test_start_child_workflow_command_equality(self) -> None:
        """Test StartChildWorkflowCommand equality comparison."""
        cmd1 = StartChildWorkflowCommand(
            seq=1, workflow_type="test", workflow_id="wf-1", args=("arg",)
        )
        cmd2 = StartChildWorkflowCommand(
            seq=1, workflow_type="test", workflow_id="wf-1", args=("arg",)
        )
        cmd3 = StartChildWorkflowCommand(
            seq=2, workflow_type="test", workflow_id="wf-1", args=("arg",)
        )
        cmd4 = StartChildWorkflowCommand(
            seq=1, workflow_type="other", workflow_id="wf-1", args=("arg",)
        )

        assert cmd1 == cmd2
        assert cmd1 != cmd3
        assert cmd1 != cmd4


# Phase 7 Tests: Cancellation


class TestApplyCancelWorkflow:
    """Tests for apply_cancel_workflow method."""

    def test_apply_cancel_workflow_sets_flag(self) -> None:
        """Test apply_cancel_workflow sets the cancel_requested flag."""
        runtime = _create_test_runtime()

        assert runtime.cancel_requested is False

        runtime.apply_cancel_workflow()

        assert runtime.cancel_requested is True

    @pytest.mark.trio
    async def test_apply_cancel_workflow_cancels_nursery(self) -> None:
        """Test apply_cancel_workflow cancels the nursery."""
        runtime = _create_test_runtime()
        nursery_cancelled = False

        async def check_cancellation() -> None:
            nonlocal nursery_cancelled
            try:
                await trio.sleep_forever()
            except trio.Cancelled:
                nursery_cancelled = True
                raise

        async with trio.open_nursery() as nursery:
            runtime.nursery = nursery
            nursery.start_soon(check_cancellation)

            # Wait for task to start
            await trio.testing.wait_all_tasks_blocked()

            # Cancel the workflow
            runtime.apply_cancel_workflow()

            # The nursery cancellation propagates to child tasks
            # and the nursery will finish cleanly with Cancelled exceptions

        assert runtime.cancel_requested is True
        assert nursery_cancelled is True

    def test_apply_cancel_workflow_without_nursery(self) -> None:
        """Test apply_cancel_workflow works when nursery is None."""
        runtime = _create_test_runtime()
        assert runtime.nursery is None

        # Should not raise
        runtime.apply_cancel_workflow()

        assert runtime.cancel_requested is True


class TestWorkflowSleepCancellation:
    """Tests for workflow_sleep cancellation behavior."""

    @pytest.mark.trio
    async def test_workflow_sleep_raises_cancelled_error_when_cancelled(self) -> None:
        """Test workflow_sleep raises trio.Cancelled when cancelled."""
        runtime = _create_test_runtime()
        cancelled_error_raised = False

        async def sleep_task() -> None:
            nonlocal cancelled_error_raised
            try:
                await runtime.workflow_sleep(5.0)
            except trio.Cancelled:
                cancelled_error_raised = True
                # Don't re-raise - just record that we got the exception

        async with trio.open_nursery() as nursery:
            runtime.nursery = nursery
            nursery.start_soon(sleep_task)

            # Wait for sleep to be blocked
            await trio.testing.wait_all_tasks_blocked()

            # Set cancel flag
            runtime.cancel_requested = True

            # Fire the timer to wake up the sleep
            runtime.apply_timer_fired(1, 5_000_000_000)

        assert cancelled_error_raised is True

    @pytest.mark.trio
    async def test_workflow_sleep_does_not_raise_when_not_cancelled(self) -> None:
        """Test workflow_sleep completes normally when not cancelled."""
        runtime = _create_test_runtime()
        sleep_completed = False

        async def sleep_task() -> None:
            nonlocal sleep_completed
            await runtime.workflow_sleep(1.0)
            sleep_completed = True

        async with trio.open_nursery() as nursery:
            nursery.start_soon(sleep_task)
            await trio.testing.wait_all_tasks_blocked()

            # Fire the timer without cancellation
            runtime.apply_timer_fired(1, 1_000_000_000)

        assert sleep_completed is True
        assert runtime.cancel_requested is False


class TestExecuteActivityCancellation:
    """Tests for execute_activity cancellation behavior."""

    @pytest.mark.trio
    async def test_execute_activity_raises_cancelled_error_when_cancelled(self) -> None:
        """Test execute_activity raises trio.Cancelled when cancelled."""
        runtime = _create_test_runtime()
        cancelled_error_raised = False

        async def activity_task() -> None:
            nonlocal cancelled_error_raised
            try:
                await runtime.execute_activity("my_activity", ())
            except trio.Cancelled:
                cancelled_error_raised = True
                # Don't re-raise - just record that we got the exception

        async with trio.open_nursery() as nursery:
            runtime.nursery = nursery
            nursery.start_soon(activity_task)

            # Wait for activity to be blocked
            await trio.testing.wait_all_tasks_blocked()

            # Set cancel flag
            runtime.cancel_requested = True

            # Complete the activity to wake up the workflow
            runtime.apply_activity_resolved(1, result="done")

        assert cancelled_error_raised is True

    @pytest.mark.trio
    async def test_execute_activity_does_not_raise_when_not_cancelled(self) -> None:
        """Test execute_activity completes normally when not cancelled."""
        runtime = _create_test_runtime()
        activity_completed = False
        result_holder: list[str] = []

        async def activity_task() -> None:
            nonlocal activity_completed
            result = await runtime.execute_activity("my_activity", ())
            result_holder.append(result)
            activity_completed = True

        async with trio.open_nursery() as nursery:
            nursery.start_soon(activity_task)
            await trio.testing.wait_all_tasks_blocked()

            # Complete the activity without cancellation
            runtime.apply_activity_resolved(1, result="activity result")

        assert activity_completed is True
        assert result_holder == ["activity result"]
        assert runtime.cancel_requested is False


class TestExecuteChildWorkflowCancellation:
    """Tests for execute_child_workflow cancellation behavior."""

    @pytest.mark.trio
    async def test_execute_child_workflow_raises_cancelled_error_when_cancelled(
        self,
    ) -> None:
        """Test execute_child_workflow raises trio.Cancelled when cancelled."""
        runtime = _create_test_runtime()
        cancelled_error_raised = False

        async def child_workflow_task() -> None:
            nonlocal cancelled_error_raised
            try:
                await runtime.execute_child_workflow("ChildWorkflow", "child-wf-1", ())
            except trio.Cancelled:
                cancelled_error_raised = True
                # Don't re-raise - just record that we got the exception

        async with trio.open_nursery() as nursery:
            runtime.nursery = nursery
            nursery.start_soon(child_workflow_task)

            # Wait for child workflow to be blocked
            await trio.testing.wait_all_tasks_blocked()

            # Set cancel flag
            runtime.cancel_requested = True

            # Complete the child workflow to wake up the workflow
            runtime.apply_child_workflow_resolved(1, result="done")

        assert cancelled_error_raised is True

    @pytest.mark.trio
    async def test_execute_child_workflow_does_not_raise_when_not_cancelled(
        self,
    ) -> None:
        """Test execute_child_workflow completes normally when not cancelled."""
        runtime = _create_test_runtime()
        child_completed = False
        result_holder: list[str] = []

        async def child_workflow_task() -> None:
            nonlocal child_completed
            result = await runtime.execute_child_workflow(
                "ChildWorkflow", "child-wf-1", ()
            )
            result_holder.append(result)
            child_completed = True

        async with trio.open_nursery() as nursery:
            nursery.start_soon(child_workflow_task)
            await trio.testing.wait_all_tasks_blocked()

            # Complete the child workflow without cancellation
            runtime.apply_child_workflow_resolved(1, result="child result")

        assert child_completed is True
        assert result_holder == ["child result"]
        assert runtime.cancel_requested is False


class TestCancelWorkflowCommand:
    """Tests for CancelWorkflowCommand dataclass."""

    def test_cancel_workflow_command_creation(self) -> None:
        """Test CancelWorkflowCommand can be created."""
        from temporalio_trio.worker._runtime import CancelWorkflowCommand

        cmd = CancelWorkflowCommand()
        assert cmd is not None

    def test_cancel_workflow_command_equality(self) -> None:
        """Test CancelWorkflowCommand equality comparison."""
        from temporalio_trio.worker._runtime import CancelWorkflowCommand

        cmd1 = CancelWorkflowCommand()
        cmd2 = CancelWorkflowCommand()

        assert cmd1 == cmd2
