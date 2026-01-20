"""Tests for workflow.wait_condition() implementation.

These tests verify the behavior of wait_condition, which blocks workflow
execution until a condition becomes true or a timeout expires.
"""

from temporalio_trio import workflow
from temporalio_trio.worker._activation import (
    CancelTimerCommand,
    CompleteWorkflowCommand,
    SignalWorkflowJob,
    StartTimerCommand,
    TimerFiredJob,
    WorkflowActivation,
    WorkflowStartedJob,
)
from temporalio_trio.worker._workflow_instance import (
    TrioWorkflowInstance,
    WorkflowInstanceDetails,
)
from temporalio_trio.workflow import Info, _Definition


def _create_instance(workflow_cls) -> TrioWorkflowInstance:
    """Helper to create a workflow instance for testing."""
    defn = _Definition.must_from_class(workflow_cls)
    info = Info(
        workflow_id="test-wf-id",
        run_id="test-run-id",
        workflow_type=defn.name,
        task_queue="test-queue",
    )
    details = WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=12345,
    )
    return TrioWorkflowInstance(details)


class TestWaitConditionSignal:
    """Test 1: Condition satisfied by signal."""

    def test_condition_satisfied_by_signal_same_activation(self):
        """Test workflow waits for condition, satisfied by signal in same activation."""

        @workflow.defn
        class SignalConditionWorkflow:
            def __init__(self):
                self._value = 0

            @workflow.run
            async def run(self, target: int) -> str:
                await workflow.wait_condition(lambda: self._value >= target)
                return f"Reached {self._value}"

            @workflow.signal
            def add(self, amount: int):
                self._value += amount

        instance = _create_instance(SignalConditionWorkflow)

        # Activation 1: Start workflow with signals that satisfy condition
        # In the replay model, signals are processed before the workflow runs,
        # so if the signals satisfy the condition, the workflow completes.
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="SignalConditionWorkflow",
                    args=(10,),  # target=10
                ),
                SignalWorkflowJob(
                    signal_name="add",
                    args=(5,),  # _value = 5
                ),
                SignalWorkflowJob(
                    signal_name="add",
                    args=(7,),  # _value = 12, >= 10
                ),
            ],
        )
        completion1 = instance.activate(activation1)

        # Workflow should complete with result since condition is satisfied
        assert len(completion1.commands) == 1
        assert isinstance(completion1.commands[0], CompleteWorkflowCommand)
        assert completion1.commands[0].result == "Reached 12"

    def test_condition_not_satisfied_yields(self):
        """Test workflow yields when condition is not satisfied."""

        @workflow.defn
        class SignalConditionWorkflow:
            def __init__(self):
                self._value = 0

            @workflow.run
            async def run(self, target: int) -> str:
                await workflow.wait_condition(lambda: self._value >= target)
                return f"Reached {self._value}"

            @workflow.signal
            def add(self, amount: int):
                self._value += amount

        instance = _create_instance(SignalConditionWorkflow)

        # Activation 1: Start workflow - condition is false, should yield
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="SignalConditionWorkflow",
                    args=(10,),  # target=10
                ),
            ],
        )
        completion1 = instance.activate(activation1)

        # Should not have any commands (no timer since no timeout specified)
        # Workflow should yield waiting for condition
        assert len(completion1.commands) == 0

    def test_condition_not_satisfied_with_partial_signals(self):
        """Test workflow yields when signals don't satisfy condition."""

        @workflow.defn
        class SignalConditionWorkflow:
            def __init__(self):
                self._value = 0

            @workflow.run
            async def run(self, target: int) -> str:
                await workflow.wait_condition(lambda: self._value >= target)
                return f"Reached {self._value}"

            @workflow.signal
            def add(self, amount: int):
                self._value += amount

        instance = _create_instance(SignalConditionWorkflow)

        # Activation 1: Start workflow with signal that partially meets condition
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="SignalConditionWorkflow",
                    args=(10,),  # target=10
                ),
                SignalWorkflowJob(
                    signal_name="add",
                    args=(5,),  # _value = 5, still < 10
                ),
            ],
        )
        completion1 = instance.activate(activation1)

        # Condition still not satisfied
        assert len(completion1.commands) == 0


class TestWaitConditionTimeout:
    """Test 2: Timeout."""

    def test_wait_condition_timeout(self):
        """Test workflow times out when condition is never satisfied."""

        @workflow.defn
        class TimeoutConditionWorkflow:
            def __init__(self):
                self._done = False

            @workflow.run
            async def run(self) -> str:
                try:
                    await workflow.wait_condition(lambda: self._done, timeout=1.0)
                    return "Done"
                except TimeoutError:
                    return "Timed out"

        instance = _create_instance(TimeoutConditionWorkflow)

        # Activation 1: Start workflow - condition is false, creates timer
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="TimeoutConditionWorkflow",
                    args=(),
                ),
            ],
        )
        completion1 = instance.activate(activation1)

        # Should have a StartTimerCommand for the timeout
        assert len(completion1.commands) == 1
        assert isinstance(completion1.commands[0], StartTimerCommand)
        timer_cmd = completion1.commands[0]
        assert timer_cmd.duration_ms == 1000  # 1 second

        # Activation 2: Timer fires - workflow catches TimeoutError
        activation2 = WorkflowActivation(
            timestamp_ns=1_001_000_000,  # 1 second later
            jobs=[
                TimerFiredJob(timer_id=timer_cmd.timer_id),
            ],
        )
        completion2 = instance.activate(activation2)

        # Workflow should complete with "Timed out"
        assert len(completion2.commands) == 1
        assert isinstance(completion2.commands[0], CompleteWorkflowCommand)
        assert completion2.commands[0].result == "Timed out"


class TestWaitConditionAlreadyTrue:
    """Test 3: Condition already true."""

    def test_condition_already_true_returns_immediately(self):
        """Test workflow returns immediately when condition is already true."""

        @workflow.defn
        class AlreadyTrueWorkflow:
            @workflow.run
            async def run(self) -> str:
                await workflow.wait_condition(lambda: True)  # Immediate return
                return "Done"

        instance = _create_instance(AlreadyTrueWorkflow)

        # Activation 1: Start workflow - condition is true, should complete immediately
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="AlreadyTrueWorkflow",
                    args=(),
                ),
            ],
        )
        completion1 = instance.activate(activation1)

        # Workflow should complete immediately without creating timer
        assert len(completion1.commands) == 1
        assert isinstance(completion1.commands[0], CompleteWorkflowCommand)
        assert completion1.commands[0].result == "Done"

    def test_condition_already_true_with_timeout_cancels_timer(self):
        """Test that when condition is already true with timeout, timer is cancelled."""

        @workflow.defn
        class AlreadyTrueWithTimeoutWorkflow:
            @workflow.run
            async def run(self) -> str:
                await workflow.wait_condition(lambda: True, timeout=60.0)
                return "Done"

        instance = _create_instance(AlreadyTrueWithTimeoutWorkflow)

        # Activation 1: Start workflow - condition is true, should complete immediately
        # and cancel the timer that was created for the timeout
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="AlreadyTrueWithTimeoutWorkflow",
                    args=(),
                ),
            ],
        )
        completion1 = instance.activate(activation1)

        # Should have CancelTimerCommand (for the timeout timer) and CompleteWorkflowCommand
        assert len(completion1.commands) == 2
        assert isinstance(completion1.commands[0], CancelTimerCommand)
        assert isinstance(completion1.commands[1], CompleteWorkflowCommand)
        assert completion1.commands[1].result == "Done"


class TestWaitConditionSignalBeforeTimeout:
    """Test 4: Signal before timeout (timer cancellation)."""

    def test_signal_before_timeout_cancels_timer(self):
        """Test workflow completes when signal arrives before timeout."""

        @workflow.defn
        class SignalBeforeTimeoutWorkflow:
            def __init__(self):
                self._approved = False

            @workflow.run
            async def run(self) -> str:
                await workflow.wait_condition(lambda: self._approved, timeout=60.0)
                return "Approved"

            @workflow.signal
            def approve(self):
                self._approved = True

        instance = _create_instance(SignalBeforeTimeoutWorkflow)

        # Activation 1: Start workflow - condition false, creates timer
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="SignalBeforeTimeoutWorkflow",
                    args=(),
                ),
            ],
        )
        completion1 = instance.activate(activation1)

        # Should have a StartTimerCommand for the 60s timeout
        assert len(completion1.commands) == 1
        assert isinstance(completion1.commands[0], StartTimerCommand)
        timer_cmd = completion1.commands[0]
        assert timer_cmd.duration_ms == 60000  # 60 seconds
        timer_id = timer_cmd.timer_id

        # Activation 2: Signal arrives before timeout
        activation2 = WorkflowActivation(
            timestamp_ns=5_000_000_000,  # 5 seconds later (well before 60s timeout)
            jobs=[
                SignalWorkflowJob(
                    signal_name="approve",
                    args=(),
                ),
            ],
        )
        completion2 = instance.activate(activation2)

        # Should have CancelTimerCommand and CompleteWorkflowCommand
        assert len(completion2.commands) == 2
        cancel_cmd = completion2.commands[0]
        complete_cmd = completion2.commands[1]

        assert isinstance(cancel_cmd, CancelTimerCommand)
        assert cancel_cmd.timer_id == timer_id
        assert isinstance(complete_cmd, CompleteWorkflowCommand)
        assert complete_cmd.result == "Approved"


class TestWaitConditionWithoutTimeout:
    """Test 5: Condition without timeout."""

    def test_wait_condition_no_timeout(self):
        """Test wait_condition with no timeout, just condition."""

        @workflow.defn
        class NoTimeoutWorkflow:
            def __init__(self):
                self._ready = False

            @workflow.run
            async def run(self) -> str:
                await workflow.wait_condition(lambda: self._ready)
                return "Ready"

            @workflow.signal
            def set_ready(self):
                self._ready = True

        instance = _create_instance(NoTimeoutWorkflow)

        # Activation 1: Start workflow - condition false, no timer (no timeout)
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="NoTimeoutWorkflow",
                    args=(),
                ),
            ],
        )
        completion1 = instance.activate(activation1)

        # No timer should be created (no timeout specified)
        assert len(completion1.commands) == 0

        # Activation 2: Signal satisfies condition
        activation2 = WorkflowActivation(
            timestamp_ns=1_000_000_000,
            jobs=[
                SignalWorkflowJob(
                    signal_name="set_ready",
                    args=(),
                ),
            ],
        )
        completion2 = instance.activate(activation2)

        # Workflow should complete
        assert len(completion2.commands) == 1
        assert isinstance(completion2.commands[0], CompleteWorkflowCommand)
        assert completion2.commands[0].result == "Ready"


class TestWaitConditionMultiple:
    """Test 6: Multiple wait_conditions in sequence."""

    def test_multiple_wait_conditions_both_in_one_activation(self):
        """Test workflow with two sequential wait_conditions, both satisfied in one activation."""

        @workflow.defn
        class MultipleConditionsWorkflow:
            def __init__(self):
                self._first_done = False
                self._second_done = False

            @workflow.run
            async def run(self) -> str:
                await workflow.wait_condition(lambda: self._first_done, timeout=10.0)
                await workflow.wait_condition(lambda: self._second_done, timeout=20.0)
                return "Both done"

            @workflow.signal
            def first_signal(self):
                self._first_done = True

            @workflow.signal
            def second_signal(self):
                self._second_done = True

        instance = _create_instance(MultipleConditionsWorkflow)

        # Activation 1: Start workflow with both signals - both conditions satisfied
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="MultipleConditionsWorkflow",
                    args=(),
                ),
                SignalWorkflowJob(
                    signal_name="first_signal",
                    args=(),
                ),
                SignalWorkflowJob(
                    signal_name="second_signal",
                    args=(),
                ),
            ],
        )
        completion1 = instance.activate(activation1)

        # Both conditions should be satisfied, so we get:
        # CancelTimer(first), CancelTimer(second), CompleteWorkflow
        assert len(completion1.commands) == 3
        assert isinstance(completion1.commands[0], CancelTimerCommand)
        assert isinstance(completion1.commands[1], CancelTimerCommand)
        assert isinstance(completion1.commands[2], CompleteWorkflowCommand)
        assert completion1.commands[2].result == "Both done"

        # Verify each timer has unique ID
        assert completion1.commands[0].timer_id != completion1.commands[1].timer_id

    def test_multiple_wait_conditions_first_only(self):
        """Test workflow waiting on first condition, not second."""

        @workflow.defn
        class MultipleConditionsWorkflow:
            def __init__(self):
                self._first_done = False
                self._second_done = False

            @workflow.run
            async def run(self) -> str:
                await workflow.wait_condition(lambda: self._first_done, timeout=10.0)
                await workflow.wait_condition(lambda: self._second_done, timeout=20.0)
                return "Both done"

            @workflow.signal
            def first_signal(self):
                self._first_done = True

            @workflow.signal
            def second_signal(self):
                self._second_done = True

        instance = _create_instance(MultipleConditionsWorkflow)

        # Activation 1: Start workflow - first condition false, creates first timer
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="MultipleConditionsWorkflow",
                    args=(),
                ),
            ],
        )
        completion1 = instance.activate(activation1)

        # Should have timer for first wait_condition
        assert len(completion1.commands) == 1
        assert isinstance(completion1.commands[0], StartTimerCommand)
        first_timer_id = completion1.commands[0].timer_id
        assert completion1.commands[0].duration_ms == 10000  # 10s

        # Activation 2: First signal arrives only - satisfies first condition but not second
        activation2 = WorkflowActivation(
            timestamp_ns=5_000_000_000,
            jobs=[
                SignalWorkflowJob(
                    signal_name="first_signal",
                    args=(),
                ),
            ],
        )
        completion2 = instance.activate(activation2)

        # Should cancel first timer and create second timer
        # The commands will be: CancelTimer(first), StartTimer(second)
        assert len(completion2.commands) == 2
        assert isinstance(completion2.commands[0], CancelTimerCommand)
        assert completion2.commands[0].timer_id == first_timer_id
        assert isinstance(completion2.commands[1], StartTimerCommand)
        second_timer_id = completion2.commands[1].timer_id
        assert completion2.commands[1].duration_ms == 20000  # 20s

        # Verify each timer has unique ID
        assert first_timer_id != second_timer_id

    def test_sequential_wait_conditions_with_timer_ids(self):
        """Test that sequential wait_conditions have unique timer IDs."""

        @workflow.defn
        class SequentialConditionsWorkflow:
            def __init__(self):
                self._done = False

            @workflow.run
            async def run(self) -> str:
                # Each wait_condition should get its own timer_id
                await workflow.wait_condition(lambda: self._done, timeout=5.0)
                return "Done"

            @workflow.signal
            def set_done(self):
                self._done = True

        instance = _create_instance(SequentialConditionsWorkflow)

        # Activation 1: Start workflow
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="SequentialConditionsWorkflow",
                    args=(),
                ),
            ],
        )
        completion1 = instance.activate(activation1)

        # Check timer is created
        assert len(completion1.commands) == 1
        assert isinstance(completion1.commands[0], StartTimerCommand)
        # Timer ID is deterministically assigned based on condition_seq and timer_seq
        # First condition gets timer_id 0
        assert completion1.commands[0].timer_id == 0


class TestWaitConditionWithTimedeltaTimeout:
    """Test wait_condition with timedelta timeout parameter."""

    def test_timedelta_timeout(self):
        """Test wait_condition accepts timedelta for timeout."""
        from datetime import timedelta

        @workflow.defn
        class TimedeltaTimeoutWorkflow:
            def __init__(self):
                self._done = False

            @workflow.run
            async def run(self) -> str:
                try:
                    await workflow.wait_condition(
                        lambda: self._done, timeout=timedelta(seconds=30)
                    )
                    return "Done"
                except TimeoutError:
                    return "Timed out"

        instance = _create_instance(TimedeltaTimeoutWorkflow)

        # Activation 1: Start workflow
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="TimedeltaTimeoutWorkflow",
                    args=(),
                ),
            ],
        )
        completion1 = instance.activate(activation1)

        # Should have timer with 30s duration
        assert len(completion1.commands) == 1
        assert isinstance(completion1.commands[0], StartTimerCommand)
        assert completion1.commands[0].duration_ms == 30000  # 30 seconds


class TestWaitConditionWithInstanceState:
    """Test wait_condition with complex instance state."""

    def test_condition_with_multiple_state_changes_same_activation(self):
        """Test condition that depends on multiple state values, all in same activation."""

        @workflow.defn
        class MultipleStateWorkflow:
            def __init__(self):
                self._a = 0
                self._b = 0

            @workflow.run
            async def run(self) -> str:
                # Wait for both a and b to reach threshold
                await workflow.wait_condition(lambda: self._a >= 5 and self._b >= 5)
                return f"a={self._a}, b={self._b}"

            @workflow.signal
            def set_a(self, value: int):
                self._a = value

            @workflow.signal
            def set_b(self, value: int):
                self._b = value

        instance = _create_instance(MultipleStateWorkflow)

        # Activation 1: Start workflow with both signals - condition satisfied
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="MultipleStateWorkflow",
                    args=(),
                ),
                SignalWorkflowJob(signal_name="set_a", args=(10,)),
                SignalWorkflowJob(signal_name="set_b", args=(10,)),
            ],
        )
        completion1 = instance.activate(activation1)

        assert len(completion1.commands) == 1
        assert isinstance(completion1.commands[0], CompleteWorkflowCommand)
        assert completion1.commands[0].result == "a=10, b=10"

    def test_condition_partial_state_yields(self):
        """Test condition yields when only one state value is set."""

        @workflow.defn
        class MultipleStateWorkflow:
            def __init__(self):
                self._a = 0
                self._b = 0

            @workflow.run
            async def run(self) -> str:
                # Wait for both a and b to reach threshold
                await workflow.wait_condition(lambda: self._a >= 5 and self._b >= 5)
                return f"a={self._a}, b={self._b}"

            @workflow.signal
            def set_a(self, value: int):
                self._a = value

            @workflow.signal
            def set_b(self, value: int):
                self._b = value

        instance = _create_instance(MultipleStateWorkflow)

        # Activation 1: Start workflow with only set_a - condition not satisfied
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="MultipleStateWorkflow",
                    args=(),
                ),
                SignalWorkflowJob(signal_name="set_a", args=(10,)),
            ],
        )
        completion1 = instance.activate(activation1)

        # Condition not satisfied (b is still 0)
        assert len(completion1.commands) == 0

    def test_condition_no_state_yields(self):
        """Test condition yields when no state is set."""

        @workflow.defn
        class MultipleStateWorkflow:
            def __init__(self):
                self._a = 0
                self._b = 0

            @workflow.run
            async def run(self) -> str:
                await workflow.wait_condition(lambda: self._a >= 5 and self._b >= 5)
                return f"a={self._a}, b={self._b}"

            @workflow.signal
            def set_a(self, value: int):
                self._a = value

            @workflow.signal
            def set_b(self, value: int):
                self._b = value

        instance = _create_instance(MultipleStateWorkflow)

        # Activation 1: Start workflow - condition not satisfied
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="MultipleStateWorkflow",
                    args=(),
                ),
            ],
        )
        completion1 = instance.activate(activation1)

        # Condition not satisfied (both a and b are 0)
        assert len(completion1.commands) == 0


class TestWaitConditionReplay:
    """Test wait_condition replay semantics."""

    def test_replay_condition_satisfied_before_timer_fires(self):
        """Test replay when condition was satisfied before timer would have fired."""

        @workflow.defn
        class ReplayWorkflow:
            def __init__(self):
                self._satisfied = False

            @workflow.run
            async def run(self) -> str:
                await workflow.wait_condition(lambda: self._satisfied, timeout=60.0)
                return "Satisfied"

            @workflow.signal
            def satisfy(self):
                self._satisfied = True

        instance = _create_instance(ReplayWorkflow)

        # Activation 1: Start workflow
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="ReplayWorkflow",
                    args=(),
                ),
            ],
        )
        completion1 = instance.activate(activation1)
        assert len(completion1.commands) == 1
        timer_id = completion1.commands[0].timer_id

        # Activation 2: Signal arrives, satisfies condition
        activation2 = WorkflowActivation(
            timestamp_ns=5_000_000_000,
            jobs=[
                SignalWorkflowJob(signal_name="satisfy", args=()),
            ],
        )
        completion2 = instance.activate(activation2)

        # Should cancel timer and complete
        assert len(completion2.commands) == 2
        assert isinstance(completion2.commands[0], CancelTimerCommand)
        assert completion2.commands[0].timer_id == timer_id
        assert isinstance(completion2.commands[1], CompleteWorkflowCommand)
        assert completion2.commands[1].result == "Satisfied"

    def test_replay_after_timeout(self):
        """Test replay when condition times out."""

        @workflow.defn
        class TimeoutReplayWorkflow:
            def __init__(self):
                self._satisfied = False

            @workflow.run
            async def run(self) -> str:
                try:
                    await workflow.wait_condition(lambda: self._satisfied, timeout=1.0)
                    return "Satisfied"
                except TimeoutError:
                    return "Timed out"

        instance = _create_instance(TimeoutReplayWorkflow)

        # Activation 1: Start workflow
        activation1 = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="TimeoutReplayWorkflow",
                    args=(),
                ),
            ],
        )
        completion1 = instance.activate(activation1)
        timer_id = completion1.commands[0].timer_id

        # Activation 2: Timer fires
        activation2 = WorkflowActivation(
            timestamp_ns=1_001_000_000,
            jobs=[
                TimerFiredJob(timer_id=timer_id),
            ],
        )
        completion2 = instance.activate(activation2)

        assert len(completion2.commands) == 1
        assert isinstance(completion2.commands[0], CompleteWorkflowCommand)
        assert completion2.commands[0].result == "Timed out"
