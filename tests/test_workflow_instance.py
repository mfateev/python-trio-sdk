"""Tests for workflow instance classes (Phase 2)."""

from dataclasses import FrozenInstanceError

import pytest

from temporalio_trio import workflow
from temporalio_trio.worker import (
    TrioWorkflowInstance,
    WorkflowInstance,
    WorkflowInstanceDetails,
)


# Test workflows
@workflow.defn
class SimpleWorkflow:
    """A simple workflow for testing."""

    @workflow.run
    async def run(self) -> str:
        return "done"


@workflow.defn(name="CustomNameWorkflow")
class WorkflowWithCustomName:
    """A workflow with a custom name."""

    @workflow.run
    async def run(self, value: str) -> str:
        return f"Hello, {value}!"


def _create_simple_details(
    workflow_cls: type = SimpleWorkflow,
    workflow_id: str = "test-wf-1",
    run_id: str = "run-1",
    task_queue: str = "test-queue",
    randomness_seed: int = 12345,
) -> WorkflowInstanceDetails:
    """Helper to create WorkflowInstanceDetails for tests."""
    defn = workflow._Definition.must_from_class(workflow_cls)
    info = workflow.Info(
        workflow_id=workflow_id,
        workflow_type=defn.name,
        run_id=run_id,
        task_queue=task_queue,
    )
    return WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=randomness_seed,
    )


class TestWorkflowInstanceDetails:
    """Tests for WorkflowInstanceDetails dataclass."""

    def test_creation_with_all_fields(self) -> None:
        """Test WorkflowInstanceDetails can be created with all fields."""
        details = _create_simple_details()

        assert details.defn.name == "SimpleWorkflow"
        assert details.info.workflow_id == "test-wf-1"
        assert details.info.workflow_type == "SimpleWorkflow"
        assert details.info.run_id == "run-1"
        assert details.info.task_queue == "test-queue"
        assert details.randomness_seed == 12345

    def test_immutability_defn(self) -> None:
        """Test WorkflowInstanceDetails.defn is immutable."""
        details = _create_simple_details()

        with pytest.raises(FrozenInstanceError):
            details.defn = workflow._Definition.must_from_class(  # type: ignore[misc]
                WorkflowWithCustomName
            )

    def test_immutability_info(self) -> None:
        """Test WorkflowInstanceDetails.info is immutable."""
        details = _create_simple_details()

        new_info = workflow.Info(
            workflow_id="other-id",
            workflow_type="OtherWorkflow",
            run_id="other-run",
            task_queue="other-queue",
        )
        with pytest.raises(FrozenInstanceError):
            details.info = new_info  # type: ignore[misc]

    def test_immutability_randomness_seed(self) -> None:
        """Test WorkflowInstanceDetails.randomness_seed is immutable."""
        details = _create_simple_details()

        with pytest.raises(FrozenInstanceError):
            details.randomness_seed = 99999  # type: ignore[misc]

    def test_equality(self) -> None:
        """Test WorkflowInstanceDetails equality comparison."""
        details1 = _create_simple_details()
        details2 = _create_simple_details()

        assert details1 == details2

    def test_inequality_different_seed(self) -> None:
        """Test WorkflowInstanceDetails inequality with different seed."""
        details1 = _create_simple_details(randomness_seed=12345)
        details2 = _create_simple_details(randomness_seed=67890)

        assert details1 != details2

    def test_inequality_different_workflow_id(self) -> None:
        """Test WorkflowInstanceDetails inequality with different workflow_id."""
        details1 = _create_simple_details(workflow_id="wf-1")
        details2 = _create_simple_details(workflow_id="wf-2")

        assert details1 != details2

    def test_with_custom_name_workflow(self) -> None:
        """Test WorkflowInstanceDetails with custom workflow name."""
        details = _create_simple_details(workflow_cls=WorkflowWithCustomName)

        assert details.defn.name == "CustomNameWorkflow"
        assert details.info.workflow_type == "CustomNameWorkflow"

    def test_not_hashable_due_to_nested_types(self) -> None:
        """Test WorkflowInstanceDetails is not hashable due to nested mutable types.

        Even though frozen=True, the _Definition contains a mutable type object,
        making the whole dataclass unhashable. This is expected behavior.
        """
        details = _create_simple_details()

        # _Definition contains mutable 'type' and 'Callable' which are unhashable
        with pytest.raises(TypeError, match="unhashable type"):
            hash(details)


class TestWorkflowInstance:
    """Tests for WorkflowInstance abstract base class."""

    def test_is_abstract(self) -> None:
        """Test WorkflowInstance is abstract and cannot be instantiated."""
        with pytest.raises(TypeError, match="abstract"):
            WorkflowInstance()  # type: ignore[abstract]

    def test_has_activate_method(self) -> None:
        """Test WorkflowInstance defines activate abstract method."""
        assert hasattr(WorkflowInstance, "activate")
        # Check it's abstract
        assert getattr(WorkflowInstance.activate, "__isabstractmethod__", False)


class TestTrioWorkflowInstance:
    """Tests for TrioWorkflowInstance class."""

    def test_creation(self) -> None:
        """Test TrioWorkflowInstance can be created."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        assert instance is not None

    def test_defn_property(self) -> None:
        """Test TrioWorkflowInstance.defn returns the definition."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        assert instance.defn is details.defn
        assert instance.defn.name == "SimpleWorkflow"

    def test_info_property(self) -> None:
        """Test TrioWorkflowInstance.info returns the info."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        assert instance.info is details.info
        assert instance.info.workflow_id == "test-wf-1"

    def test_is_workflow_instance(self) -> None:
        """Test TrioWorkflowInstance is a WorkflowInstance."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        assert isinstance(instance, WorkflowInstance)

    def test_is_runtime(self) -> None:
        """Test TrioWorkflowInstance is a _Runtime."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        assert isinstance(instance, workflow._Runtime)

    def test_workflow_time_ns_initial_value(self) -> None:
        """Test workflow_time_ns() returns 0 initially."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        assert instance.workflow_time_ns() == 0

    def test_internal_time_ns_can_be_modified(self) -> None:
        """Test internal _time_ns can be modified (for testing/implementation)."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        instance._time_ns = 5_000_000_000  # 5 seconds
        assert instance.workflow_time_ns() == 5_000_000_000

    def test_random_is_seeded(self) -> None:
        """Test random generator is seeded with randomness_seed."""
        details1 = _create_simple_details(randomness_seed=12345)
        details2 = _create_simple_details(randomness_seed=12345)
        details3 = _create_simple_details(randomness_seed=67890)

        instance1 = TrioWorkflowInstance(details1)
        instance2 = TrioWorkflowInstance(details2)
        instance3 = TrioWorkflowInstance(details3)

        # Same seed should produce same random values
        random1_value = instance1._random.randint(0, 1000000)
        random2_value = instance2._random.randint(0, 1000000)
        random3_value = instance3._random.randint(0, 1000000)

        assert random1_value == random2_value
        assert random1_value != random3_value  # Different seeds

    def test_timer_seq_initial_value(self) -> None:
        """Test timer sequence starts at 0."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        assert instance._timer_seq == 0

    def test_workflow_obj_initial_value(self) -> None:
        """Test workflow object is None initially."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        assert instance._workflow_obj is None

    def test_activate_simple_workflow(self) -> None:
        """Test activate() executes workflow and returns completion."""
        from temporalio_trio.worker import (
            CompleteWorkflowCommand,
            WorkflowActivation,
            WorkflowStartedJob,
        )

        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        activation = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="SimpleWorkflow", args=())],
            timestamp_ns=0,
        )
        completion = instance.activate(activation)

        assert len(completion.commands) == 1
        assert isinstance(completion.commands[0], CompleteWorkflowCommand)
        assert completion.commands[0].result == "done"

    def test_workflow_sleep_creates_timer(self) -> None:
        """Test workflow_sleep() creates StartTimerCommand."""
        from temporalio_trio.worker import (
            StartTimerCommand,
            WorkflowActivation,
            WorkflowStartedJob,
        )

        @workflow.defn
        class SleepWorkflow:
            @workflow.run
            async def run(self) -> str:
                await workflow.sleep(5.0)
                return "slept"

        defn = workflow._Definition.must_from_class(SleepWorkflow)
        info = workflow.Info(
            workflow_id="test-wf-sleep",
            workflow_type=defn.name,
            run_id="run-1",
            task_queue="test-queue",
        )
        details = WorkflowInstanceDetails(
            defn=defn,
            info=info,
            randomness_seed=12345,
        )
        instance = TrioWorkflowInstance(details)

        activation = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="SleepWorkflow", args=())],
            timestamp_ns=0,
        )
        completion = instance.activate(activation)

        # Should have a StartTimerCommand (workflow blocked on sleep)
        assert len(completion.commands) == 1
        assert isinstance(completion.commands[0], StartTimerCommand)
        assert completion.commands[0].duration_ms == 5000

    def test_can_set_as_current_runtime(self) -> None:
        """Test TrioWorkflowInstance can be set as current runtime."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        # Set as current runtime
        token = workflow._Runtime.set_current(instance)
        try:
            assert workflow._Runtime.current() is instance
            assert workflow._Runtime.maybe_current() is instance

            # Can access workflow time through the runtime
            assert workflow.time_ns() == 0
            assert workflow.time() == 0.0
        finally:
            workflow._Runtime.reset_current(token)

        # After reset, should be None again
        assert workflow._Runtime.maybe_current() is None

    def test_multiple_instances_isolated(self) -> None:
        """Test multiple instances have isolated state."""
        details1 = _create_simple_details(workflow_id="wf-1", randomness_seed=111)
        details2 = _create_simple_details(workflow_id="wf-2", randomness_seed=222)

        instance1 = TrioWorkflowInstance(details1)
        instance2 = TrioWorkflowInstance(details2)

        # Modify instance1's state
        instance1._time_ns = 10_000_000_000
        instance1._timer_seq = 5

        # instance2 should be unaffected
        assert instance2._time_ns == 0
        assert instance2._timer_seq == 0

    def test_with_different_workflow_types(self) -> None:
        """Test TrioWorkflowInstance works with different workflow types."""
        details1 = _create_simple_details(workflow_cls=SimpleWorkflow)
        details2 = _create_simple_details(workflow_cls=WorkflowWithCustomName)

        instance1 = TrioWorkflowInstance(details1)
        instance2 = TrioWorkflowInstance(details2)

        assert instance1.defn.name == "SimpleWorkflow"
        assert instance2.defn.name == "CustomNameWorkflow"


class TestRuntimeIntegration:
    """Tests for runtime integration with TrioWorkflowInstance."""

    def test_workflow_time_via_runtime(self) -> None:
        """Test workflow.time() works when instance is set as runtime."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)
        instance._time_ns = 3_500_000_000  # 3.5 seconds

        token = workflow._Runtime.set_current(instance)
        try:
            assert workflow.time() == 3.5
            assert workflow.time_ns() == 3_500_000_000
        finally:
            workflow._Runtime.reset_current(token)

    def test_runtime_context_switch(self) -> None:
        """Test switching between different instance runtimes."""
        details1 = _create_simple_details(workflow_id="wf-1")
        details2 = _create_simple_details(workflow_id="wf-2")

        instance1 = TrioWorkflowInstance(details1)
        instance2 = TrioWorkflowInstance(details2)

        instance1._time_ns = 1_000_000_000
        instance2._time_ns = 2_000_000_000

        # Set instance1 as current
        token1 = workflow._Runtime.set_current(instance1)
        try:
            assert workflow.time() == 1.0

            # Switch to instance2
            token2 = workflow._Runtime.set_current(instance2)
            try:
                assert workflow.time() == 2.0
            finally:
                workflow._Runtime.reset_current(token2)

            # Back to instance1
            assert workflow.time() == 1.0
        finally:
            workflow._Runtime.reset_current(token1)

    def test_runtime_not_accessible_after_reset(self) -> None:
        """Test runtime is not accessible after reset."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        workflow._Runtime.reset_current(token)

        with pytest.raises(workflow._NotInWorkflowContextError):
            workflow.time()
