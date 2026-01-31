"""Tests for continue_as_new functionality - Phase 2."""

from datetime import timedelta

import pytest

from temporalio_trio import workflow
from temporalio_trio.worker import (
    ContinueAsNewWorkflowCommand,
    TrioWorkflowInstance,
    WorkflowInstanceDetails,
)


# Test workflows
@workflow.defn
class SimpleWorkflow:
    """A simple workflow for testing."""

    @workflow.run
    async def run(self) -> str:
        return "done"


@workflow.defn(name="TargetWorkflow")
class TargetWorkflow:
    """Target workflow for continue-as-new to different workflow."""

    @workflow.run
    async def run(self, value: str) -> str:
        return f"target: {value}"


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


class TestContinueAsNewError:
    """Tests for ContinueAsNewError class."""

    def test_cannot_instantiate_directly(self) -> None:
        """Test ContinueAsNewError cannot be instantiated directly."""
        with pytest.raises(RuntimeError, match="Cannot instantiate"):
            workflow.ContinueAsNewError()

    def test_is_base_exception(self) -> None:
        """Test ContinueAsNewError is a BaseException (not Exception)."""
        # This ensures it won't be caught by `except Exception:`
        assert issubclass(workflow.ContinueAsNewError, BaseException)
        assert not issubclass(workflow.ContinueAsNewError, Exception)

    def test_internal_error_can_be_created(self) -> None:
        """Test _ContinueAsNewError can be created internally."""
        err = workflow._ContinueAsNewError(
            workflow=None,
            workflow_args=["arg1"],
            task_queue=None,
            run_timeout=None,
            task_timeout=None,
            memo=None,
        )
        assert isinstance(err, workflow.ContinueAsNewError)
        assert err.workflow is None
        assert err.workflow_args == ["arg1"]

    def test_internal_error_stores_all_fields(self) -> None:
        """Test _ContinueAsNewError stores all configuration fields."""
        memo_data = {"key": "value"}
        err = workflow._ContinueAsNewError(
            workflow="TargetWorkflow",
            workflow_args=["arg1", "arg2"],
            task_queue="new-queue",
            run_timeout=timedelta(hours=1),
            task_timeout=timedelta(minutes=5),
            memo=memo_data,
        )
        assert err.workflow == "TargetWorkflow"
        assert err.workflow_args == ["arg1", "arg2"]
        assert err.task_queue == "new-queue"
        assert err.run_timeout == timedelta(hours=1)
        assert err.task_timeout == timedelta(minutes=5)
        assert err.memo == memo_data


class TestContinueAsNewFunction:
    """Tests for workflow.continue_as_new() function."""

    def test_raises_outside_workflow_context(self) -> None:
        """Test continue_as_new() raises when not in workflow context."""
        with pytest.raises(workflow._NotInWorkflowContextError):
            workflow.continue_as_new()

    def test_raises_if_both_arg_and_args(self) -> None:
        """Test continue_as_new() raises if both arg and args are provided."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            with pytest.raises(ValueError, match="Cannot specify both arg and args"):
                workflow.continue_as_new("single_arg", args=["multiple"])
        finally:
            workflow._Runtime.reset_current(token)

    def test_with_single_arg(self) -> None:
        """Test continue_as_new() with single arg."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            with pytest.raises(workflow._ContinueAsNewError) as exc_info:
                workflow.continue_as_new("my_arg")
            assert list(exc_info.value.workflow_args) == ["my_arg"]
        finally:
            workflow._Runtime.reset_current(token)

    def test_with_multiple_args(self) -> None:
        """Test continue_as_new() with multiple args."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            with pytest.raises(workflow._ContinueAsNewError) as exc_info:
                workflow.continue_as_new(args=["arg1", "arg2", "arg3"])
            assert tuple(exc_info.value.workflow_args) == ("arg1", "arg2", "arg3")
        finally:
            workflow._Runtime.reset_current(token)

    def test_with_no_args(self) -> None:
        """Test continue_as_new() with no args."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            with pytest.raises(workflow._ContinueAsNewError) as exc_info:
                workflow.continue_as_new()
            assert list(exc_info.value.workflow_args) == []
        finally:
            workflow._Runtime.reset_current(token)

    def test_with_workflow_string(self) -> None:
        """Test continue_as_new() with workflow as string."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            with pytest.raises(workflow._ContinueAsNewError) as exc_info:
                workflow.continue_as_new(workflow="OtherWorkflow")
            assert exc_info.value.workflow == "OtherWorkflow"
        finally:
            workflow._Runtime.reset_current(token)

    def test_with_workflow_class(self) -> None:
        """Test continue_as_new() with workflow as class."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            with pytest.raises(workflow._ContinueAsNewError) as exc_info:
                workflow.continue_as_new(workflow=TargetWorkflow)
            assert exc_info.value.workflow == "TargetWorkflow"
        finally:
            workflow._Runtime.reset_current(token)

    def test_with_task_queue(self) -> None:
        """Test continue_as_new() with custom task queue."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            with pytest.raises(workflow._ContinueAsNewError) as exc_info:
                workflow.continue_as_new(task_queue="new-queue")
            assert exc_info.value.task_queue == "new-queue"
        finally:
            workflow._Runtime.reset_current(token)

    def test_with_timeouts(self) -> None:
        """Test continue_as_new() with custom timeouts."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            with pytest.raises(workflow._ContinueAsNewError) as exc_info:
                workflow.continue_as_new(
                    run_timeout=timedelta(hours=2),
                    task_timeout=timedelta(minutes=10),
                )
            assert exc_info.value.run_timeout == timedelta(hours=2)
            assert exc_info.value.task_timeout == timedelta(minutes=10)
        finally:
            workflow._Runtime.reset_current(token)

    def test_with_memo(self) -> None:
        """Test continue_as_new() with custom memo."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            memo_data = {"key1": "value1", "key2": 42}
            with pytest.raises(workflow._ContinueAsNewError) as exc_info:
                workflow.continue_as_new(memo=memo_data)
            assert exc_info.value.memo == memo_data
        finally:
            workflow._Runtime.reset_current(token)

    def test_with_all_options(self) -> None:
        """Test continue_as_new() with all options."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            with pytest.raises(workflow._ContinueAsNewError) as exc_info:
                workflow.continue_as_new(
                    "my_arg",
                    workflow="TargetWorkflow",
                    task_queue="new-queue",
                    run_timeout=timedelta(hours=1),
                    task_timeout=timedelta(minutes=5),
                    memo={"key": "value"},
                )
            err = exc_info.value
            assert list(err.workflow_args) == ["my_arg"]
            assert err.workflow == "TargetWorkflow"
            assert err.task_queue == "new-queue"
            assert err.run_timeout == timedelta(hours=1)
            assert err.task_timeout == timedelta(minutes=5)
            assert err.memo == {"key": "value"}
        finally:
            workflow._Runtime.reset_current(token)


class TestContinueAsNewWorkflowCommand:
    """Tests for ContinueAsNewWorkflowCommand dataclass."""

    def test_default_values(self) -> None:
        """Test ContinueAsNewWorkflowCommand has correct defaults."""
        cmd = ContinueAsNewWorkflowCommand()
        assert cmd.workflow is None
        assert cmd.args == []
        assert cmd.task_queue is None
        assert cmd.run_timeout is None
        assert cmd.task_timeout is None
        assert cmd.memo is None

    def test_with_all_fields(self) -> None:
        """Test ContinueAsNewWorkflowCommand with all fields set."""
        cmd = ContinueAsNewWorkflowCommand(
            workflow="MyWorkflow",
            args=["arg1", "arg2"],
            task_queue="my-queue",
            run_timeout=timedelta(hours=1),
            task_timeout=timedelta(minutes=10),
            memo={"key": "value"},
        )
        assert cmd.workflow == "MyWorkflow"
        assert cmd.args == ["arg1", "arg2"]
        assert cmd.task_queue == "my-queue"
        assert cmd.run_timeout == timedelta(hours=1)
        assert cmd.task_timeout == timedelta(minutes=10)
        assert cmd.memo == {"key": "value"}


class TestContinueAsNewInWorkflow:
    """Tests for continue_as_new behavior in workflows.

    Note: These tests use direct method calls on the workflow instance
    rather than full workflow execution, since the standard trio doesn't
    have the deterministic=True parameter needed for full execution.
    """

    def test_continue_as_new_not_caught_by_exception(self) -> None:
        """Test ContinueAsNewError is not caught by generic Exception handler."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            try:
                workflow.continue_as_new()
            except Exception:
                pytest.fail("ContinueAsNewError should not be caught by Exception")
            except workflow.ContinueAsNewError:
                pass  # Expected
        finally:
            workflow._Runtime.reset_current(token)

    def test_continue_as_new_caught_by_base_exception(self) -> None:
        """Test ContinueAsNewError can be caught by BaseException handler."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            caught = False
            try:
                workflow.continue_as_new()
            except BaseException:
                caught = True
            assert caught
        finally:
            workflow._Runtime.reset_current(token)


class TestContinueAsNewWithWorkflowResolution:
    """Tests for workflow resolution in continue_as_new."""

    def test_none_workflow_stays_none(self) -> None:
        """Test workflow=None stays None (same workflow)."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            with pytest.raises(workflow._ContinueAsNewError) as exc_info:
                workflow.continue_as_new(workflow=None)
            assert exc_info.value.workflow is None
        finally:
            workflow._Runtime.reset_current(token)

    def test_string_workflow_passed_through(self) -> None:
        """Test string workflow name is passed through."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            with pytest.raises(workflow._ContinueAsNewError) as exc_info:
                workflow.continue_as_new(workflow="SomeWorkflowName")
            assert exc_info.value.workflow == "SomeWorkflowName"
        finally:
            workflow._Runtime.reset_current(token)

    def test_decorated_class_resolves_to_name(self) -> None:
        """Test decorated workflow class resolves to its registered name."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            with pytest.raises(workflow._ContinueAsNewError) as exc_info:
                workflow.continue_as_new(workflow=TargetWorkflow)
            # TargetWorkflow is decorated with name="TargetWorkflow"
            assert exc_info.value.workflow == "TargetWorkflow"
        finally:
            workflow._Runtime.reset_current(token)

    def test_decorated_class_with_custom_name(self) -> None:
        """Test decorated workflow class with custom name."""

        @workflow.defn(name="CustomName")
        class WorkflowWithCustomName:
            @workflow.run
            async def run(self) -> None:
                pass

        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            with pytest.raises(workflow._ContinueAsNewError) as exc_info:
                workflow.continue_as_new(workflow=WorkflowWithCustomName)
            assert exc_info.value.workflow == "CustomName"
        finally:
            workflow._Runtime.reset_current(token)
