"""Tests for continue_as_new workflow API.

These tests verify:
- ContinueAsNewError cannot be instantiated directly
- ContinueAsNewError is a BaseException
- continue_as_new produces ContinueAsNewCommand
- continue_as_new with different workflow types
- continue_as_new with arguments
- continue_as_new with task queue
- continue_as_new with timeouts
"""

from datetime import timedelta

import pytest
import temporalio.common

from temporalio_trio import workflow
from temporalio_trio.worker import (
    TrioWorkflowInstance,
    WorkflowActivation,
    WorkflowInstanceDetails,
    WorkflowStartedJob,
)
from temporalio_trio.worker._activation import ContinueAsNewCommand

# =============================================================================
# Test Workflows
# =============================================================================


@workflow.defn
class ContinueAsNewWorkflow:
    """A workflow that calls continue_as_new."""

    @workflow.run
    async def run(self, iteration: int) -> str:
        if iteration >= 3:
            return f"done after {iteration} iterations"
        workflow.continue_as_new(iteration + 1)


@workflow.defn
class ContinueAsNewWithArgsWorkflow:
    """A workflow that calls continue_as_new with multiple args."""

    @workflow.run
    async def run(self, name: str, count: int) -> str:
        if count >= 3:
            return f"done {name} after {count}"
        workflow.continue_as_new(args=[name, count + 1])


@workflow.defn
class ContinueAsNewDifferentTypeWorkflow:
    """A workflow that continues as a different workflow type."""

    @workflow.run
    async def run(self) -> str:
        workflow.continue_as_new(workflow="OtherWorkflow")


@workflow.defn
class ContinueAsNewWithTaskQueueWorkflow:
    """A workflow that continues with a different task queue."""

    @workflow.run
    async def run(self) -> str:
        workflow.continue_as_new(task_queue="other-queue")


@workflow.defn
class ContinueAsNewWithTimeoutsWorkflow:
    """A workflow that continues with timeouts."""

    @workflow.run
    async def run(self) -> str:
        workflow.continue_as_new(
            run_timeout=timedelta(hours=1),
            task_timeout=timedelta(seconds=30),
        )


@workflow.defn
class ContinueAsNewWithRetryPolicyWorkflow:
    """A workflow that continues with retry policy."""

    @workflow.run
    async def run(self) -> str:
        workflow.continue_as_new(
            retry_policy=temporalio.common.RetryPolicy(
                initial_interval=timedelta(seconds=1),
                maximum_attempts=5,
            ),
        )


@workflow.defn
class ContinueAsNewWithClassReferenceWorkflow:
    """A workflow that continues as another workflow using class reference."""

    @workflow.run
    async def run(self) -> str:
        workflow.continue_as_new(workflow=ContinueAsNewWorkflow, args=[1])


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
        workflow_type=defn.name,
        run_id=run_id,
        task_queue=task_queue,
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


# =============================================================================
# Test ContinueAsNewError
# =============================================================================


class TestContinueAsNewError:
    """Tests for ContinueAsNewError exception class."""

    def test_cannot_instantiate_directly(self) -> None:
        """Test that ContinueAsNewError cannot be instantiated directly."""
        with pytest.raises(RuntimeError) as exc_info:
            workflow.ContinueAsNewError("test")
        assert "cannot be instantiated directly" in str(exc_info.value)

    def test_is_base_exception(self) -> None:
        """Test that ContinueAsNewError is a BaseException."""
        assert issubclass(workflow.ContinueAsNewError, BaseException)
        # Should NOT be caught by except Exception
        assert not issubclass(workflow.ContinueAsNewError, Exception)


# =============================================================================
# Test continue_as_new Command Generation
# =============================================================================


class TestContinueAsNewCommand:
    """Tests for continue_as_new producing ContinueAsNewCommand."""

    def test_continue_as_new_produces_command(self) -> None:
        """Test that continue_as_new produces ContinueAsNewCommand."""
        instance = _create_instance(ContinueAsNewWorkflow)
        activation = _create_start_activation("ContinueAsNewWorkflow", args=(1,))

        completion = instance.activate(activation)

        # Should have exactly one command - ContinueAsNewCommand
        assert len(completion.commands) == 1
        cmd = completion.commands[0]
        assert isinstance(cmd, ContinueAsNewCommand)
        assert cmd.workflow_type == "ContinueAsNewWorkflow"
        assert cmd.args == (2,)  # iteration + 1

    def test_continue_as_new_with_multiple_args(self) -> None:
        """Test continue_as_new with multiple arguments."""
        instance = _create_instance(ContinueAsNewWithArgsWorkflow)
        activation = _create_start_activation(
            "ContinueAsNewWithArgsWorkflow", args=("test", 1)
        )

        completion = instance.activate(activation)

        assert len(completion.commands) == 1
        cmd = completion.commands[0]
        assert isinstance(cmd, ContinueAsNewCommand)
        # args= is passed as a list but stored as tuple
        assert cmd.args == ("test", 2)

    def test_continue_as_new_with_different_workflow_type(self) -> None:
        """Test continue_as_new with a different workflow type (string)."""
        instance = _create_instance(ContinueAsNewDifferentTypeWorkflow)
        activation = _create_start_activation("ContinueAsNewDifferentTypeWorkflow")

        completion = instance.activate(activation)

        assert len(completion.commands) == 1
        cmd = completion.commands[0]
        assert isinstance(cmd, ContinueAsNewCommand)
        assert cmd.workflow_type == "OtherWorkflow"

    def test_continue_as_new_with_task_queue(self) -> None:
        """Test continue_as_new with a different task queue."""
        instance = _create_instance(ContinueAsNewWithTaskQueueWorkflow)
        activation = _create_start_activation("ContinueAsNewWithTaskQueueWorkflow")

        completion = instance.activate(activation)

        assert len(completion.commands) == 1
        cmd = completion.commands[0]
        assert isinstance(cmd, ContinueAsNewCommand)
        assert cmd.task_queue == "other-queue"

    def test_continue_as_new_with_timeouts(self) -> None:
        """Test continue_as_new with timeouts."""
        instance = _create_instance(ContinueAsNewWithTimeoutsWorkflow)
        activation = _create_start_activation("ContinueAsNewWithTimeoutsWorkflow")

        completion = instance.activate(activation)

        assert len(completion.commands) == 1
        cmd = completion.commands[0]
        assert isinstance(cmd, ContinueAsNewCommand)
        assert cmd.run_timeout == timedelta(hours=1)
        assert cmd.task_timeout == timedelta(seconds=30)

    def test_continue_as_new_with_retry_policy(self) -> None:
        """Test continue_as_new with retry policy."""
        instance = _create_instance(ContinueAsNewWithRetryPolicyWorkflow)
        activation = _create_start_activation("ContinueAsNewWithRetryPolicyWorkflow")

        completion = instance.activate(activation)

        assert len(completion.commands) == 1
        cmd = completion.commands[0]
        assert isinstance(cmd, ContinueAsNewCommand)
        assert cmd.retry_policy is not None
        assert cmd.retry_policy.initial_interval == timedelta(seconds=1)
        assert cmd.retry_policy.maximum_attempts == 5

    def test_continue_as_new_with_class_reference(self) -> None:
        """Test continue_as_new with workflow class reference."""
        instance = _create_instance(ContinueAsNewWithClassReferenceWorkflow)
        activation = _create_start_activation("ContinueAsNewWithClassReferenceWorkflow")

        completion = instance.activate(activation)

        assert len(completion.commands) == 1
        cmd = completion.commands[0]
        assert isinstance(cmd, ContinueAsNewCommand)
        # Should resolve the class to its definition name
        assert cmd.workflow_type == "ContinueAsNewWorkflow"
        assert cmd.args == (1,)

    def test_continue_as_new_defaults_to_same_workflow(self) -> None:
        """Test that continue_as_new defaults to the current workflow type."""
        instance = _create_instance(ContinueAsNewWorkflow)
        activation = _create_start_activation("ContinueAsNewWorkflow", args=(1,))

        completion = instance.activate(activation)

        assert len(completion.commands) == 1
        cmd = completion.commands[0]
        assert isinstance(cmd, ContinueAsNewCommand)
        # Should default to current workflow type
        assert cmd.workflow_type == "ContinueAsNewWorkflow"


# =============================================================================
# Test Workflow Completion After Iteration Limit
# =============================================================================


class TestContinueAsNewIteration:
    """Tests for workflows that iterate with continue_as_new."""

    def test_workflow_completes_after_iterations(self) -> None:
        """Test workflow returns result when iteration limit is reached."""
        instance = _create_instance(ContinueAsNewWorkflow)
        activation = _create_start_activation("ContinueAsNewWorkflow", args=(3,))

        completion = instance.activate(activation)

        # When iteration >= 3, should complete instead of continue_as_new
        from temporalio_trio.worker._activation import CompleteWorkflowCommand

        assert len(completion.commands) == 1
        cmd = completion.commands[0]
        assert isinstance(cmd, CompleteWorkflowCommand)
        assert cmd.result == "done after 3 iterations"


# =============================================================================
# Test continue_as_new Outside Workflow Context
# =============================================================================


class TestContinueAsNewContext:
    """Tests for continue_as_new context requirements."""

    def test_continue_as_new_outside_workflow_raises(self) -> None:
        """Test that continue_as_new raises when called outside workflow context."""
        with pytest.raises(workflow._NotInWorkflowContextError):
            workflow.continue_as_new()
