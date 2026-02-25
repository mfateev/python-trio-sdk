"""Tests for workflow signal handlers."""

from datetime import datetime, timezone

import pytest
import trio

from temporalio_trio import workflow
from temporalio_trio.worker._activation import (
    SignalWorkflowJob,
    WorkflowActivation,
    WorkflowStartedJob,
)
from temporalio_trio.worker._workflow_instance import (
    TrioWorkflowInstance,
    WorkflowInstanceDetails,
)
from temporalio_trio.workflow import Info, _Definition, _SignalDefinition


class TestSignalDecorator:
    """Tests for @workflow.signal decorator."""

    def test_signal_decorator_marks_method(self):
        """Test that @signal decorator marks method with definition."""

        @workflow.defn
        class TestWorkflow:
            @workflow.signal
            def my_signal(self) -> None:
                pass

            @workflow.run
            async def run(self) -> None:
                pass

        defn = _SignalDefinition.from_fn(TestWorkflow.my_signal)
        assert defn is not None
        assert defn.name == "my_signal"
        assert defn.is_method is True

    def test_signal_decorator_with_custom_name(self):
        """Test signal decorator with custom name."""

        @workflow.defn
        class TestWorkflow:
            @workflow.signal(name="custom-signal")
            def my_signal(self) -> None:
                pass

            @workflow.run
            async def run(self) -> None:
                pass

        defn = _SignalDefinition.from_fn(TestWorkflow.my_signal)
        assert defn is not None
        assert defn.name == "custom-signal"

    def test_signal_decorator_dynamic(self):
        """Test dynamic signal decorator."""

        @workflow.defn
        class TestWorkflow:
            @workflow.signal(dynamic=True)
            def handle_any(self, name: str, *args) -> None:
                pass

            @workflow.run
            async def run(self) -> None:
                pass

        defn = _SignalDefinition.from_fn(TestWorkflow.handle_any)
        assert defn is not None
        assert defn.name is None  # Dynamic handlers have None name

    def test_signal_collected_in_definition(self):
        """Test that signals are collected in workflow definition."""

        @workflow.defn
        class TestWorkflow:
            @workflow.signal
            def signal_one(self) -> None:
                pass

            @workflow.signal(name="signal-two")
            def signal_two(self) -> None:
                pass

            @workflow.run
            async def run(self) -> None:
                pass

        defn = _Definition.must_from_class(TestWorkflow)
        assert "signal_one" in defn.signals
        assert "signal-two" in defn.signals
        assert len(defn.signals) == 2

    def test_duplicate_signal_name_raises(self):
        """Test that duplicate signal names raise error."""
        with pytest.raises(ValueError, match="Duplicate signal"):

            @workflow.defn
            class TestWorkflow:
                @workflow.signal(name="same-name")
                def signal_one(self) -> None:
                    pass

                @workflow.signal(name="same-name")
                def signal_two(self) -> None:
                    pass

                @workflow.run
                async def run(self) -> None:
                    pass


class TestSignalHandler:
    """Tests for signal handler invocation."""

    def _create_instance(self, workflow_cls) -> TrioWorkflowInstance:
        """Helper to create a workflow instance."""
        defn = _Definition.must_from_class(workflow_cls)
        info = Info(
            workflow_id="test-wf-id",
            run_id="test-run-id",
            workflow_type=defn.name,
            task_queue="test-queue",
            namespace="default",
            attempt=1,
            start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
        details = WorkflowInstanceDetails(
            defn=defn,
            info=info,
            randomness_seed=12345,
        )
        return TrioWorkflowInstance(details)

    def test_signal_handler_sync(self):
        """Test synchronous signal handler."""
        received = []

        @workflow.defn
        class TestWorkflow:
            @workflow.signal
            def my_signal(self, value: int) -> None:
                received.append(value)

            @workflow.run
            async def run(self) -> str:
                return "done"

        instance = self._create_instance(TestWorkflow)

        # Create activation with signal and start
        activation = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="TestWorkflow",
                    args=(),
                ),
                SignalWorkflowJob(
                    signal_name="my_signal",
                    args=(42,),
                ),
            ],
        )

        completion = instance.activate(activation)

        assert received == [42]
        assert len(completion.commands) > 0

    def test_signal_handler_async(self):
        """Test asynchronous signal handler."""
        received = []

        @workflow.defn
        class TestWorkflow:
            @workflow.signal
            async def my_signal(self, value: str) -> None:
                received.append(value)

            @workflow.run
            async def run(self) -> str:
                return "done"

        instance = self._create_instance(TestWorkflow)

        activation = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="TestWorkflow",
                    args=(),
                ),
                SignalWorkflowJob(
                    signal_name="my_signal",
                    args=("hello",),
                ),
            ],
        )

        completion = instance.activate(activation)

        assert received == ["hello"]
        assert len(completion.commands) > 0

    def test_signal_with_multiple_args(self):
        """Test signal handler with multiple arguments."""
        received = []

        @workflow.defn
        class TestWorkflow:
            @workflow.signal
            def my_signal(self, a: int, b: str, c: bool) -> None:
                received.append((a, b, c))

            @workflow.run
            async def run(self) -> str:
                return "done"

        instance = self._create_instance(TestWorkflow)

        activation = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="TestWorkflow",
                    args=(),
                ),
                SignalWorkflowJob(
                    signal_name="my_signal",
                    args=(1, "two", True),
                ),
            ],
        )

        completion = instance.activate(activation)

        assert received == [(1, "two", True)]

    def test_multiple_signals_same_activation(self):
        """Test multiple signals in same activation."""
        received = []

        @workflow.defn
        class TestWorkflow:
            @workflow.signal
            def signal_a(self, value: int) -> None:
                received.append(("a", value))

            @workflow.signal
            def signal_b(self, value: int) -> None:
                received.append(("b", value))

            @workflow.run
            async def run(self) -> str:
                return "done"

        instance = self._create_instance(TestWorkflow)

        activation = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="TestWorkflow",
                    args=(),
                ),
                SignalWorkflowJob(signal_name="signal_a", args=(1,)),
                SignalWorkflowJob(signal_name="signal_b", args=(2,)),
                SignalWorkflowJob(signal_name="signal_a", args=(3,)),
            ],
        )

        completion = instance.activate(activation)

        assert received == [("a", 1), ("b", 2), ("a", 3)]

    def test_signal_modifies_workflow_state(self):
        """Test that signals can modify workflow instance state."""

        @workflow.defn
        class CounterWorkflow:
            def __init__(self):
                self.count = 0

            @workflow.signal
            def increment(self, amount: int = 1) -> None:
                self.count += amount

            @workflow.run
            async def run(self) -> int:
                return self.count

        instance = self._create_instance(CounterWorkflow)

        activation = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(workflow_type="CounterWorkflow", args=()),
                SignalWorkflowJob(signal_name="increment", args=(5,)),
                SignalWorkflowJob(signal_name="increment", args=(3,)),
            ],
        )

        completion = instance.activate(activation)

        # The workflow should return the count after signals
        # Check that we have a completion command
        assert len(completion.commands) > 0
        # The completion should be a CompleteWorkflowCommand with result=8
        from temporalio_trio.worker._activation import CompleteWorkflowCommand

        complete_cmd = None
        for cmd in completion.commands:
            if isinstance(cmd, CompleteWorkflowCommand):
                complete_cmd = cmd
                break
        assert complete_cmd is not None
        assert complete_cmd.result == 8

    def test_unknown_signal_ignored(self):
        """Test that unknown signals are ignored with warning."""

        @workflow.defn
        class TestWorkflow:
            @workflow.signal
            def known_signal(self) -> None:
                pass

            @workflow.run
            async def run(self) -> str:
                return "done"

        instance = self._create_instance(TestWorkflow)

        activation = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(workflow_type="TestWorkflow", args=()),
                SignalWorkflowJob(signal_name="unknown_signal", args=()),
            ],
        )

        # Should not raise, just log warning
        completion = instance.activate(activation)
        # Workflow should still complete successfully
        assert len(completion.commands) > 0


class TestSignalDefinition:
    """Tests for _SignalDefinition class."""

    def test_from_fn_returns_none_for_non_signal(self):
        """Test that from_fn returns None for non-signal functions."""

        def regular_func():
            pass

        assert _SignalDefinition.from_fn(regular_func) is None

    def test_from_fn_returns_definition_for_signal(self):
        """Test that from_fn returns definition for signal functions."""

        @workflow.signal
        def my_signal():
            pass

        defn = _SignalDefinition.from_fn(my_signal)
        assert defn is not None
        assert defn.name == "my_signal"
