"""Tests for child workflow instance handling.

These tests verify that TrioWorkflowInstance correctly handles child workflow
commands and jobs (start, started, start failed, resolved).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import temporalio.common

from temporalio_trio import workflow
from temporalio_trio.worker._activation import (
    ChildWorkflowResolvedJob,
    ChildWorkflowStartedJob,
    ChildWorkflowStartFailedJob,
    StartChildWorkflowCommand,
    WorkflowActivation,
    WorkflowStartedJob,
)
from temporalio_trio.worker._workflow_instance import (
    TrioWorkflowInstance,
    TrioWorkflowRunner,
    WorkflowInstanceDetails,
)
from temporalio_trio.workflow import (
    ChildWorkflowCancellationType,
    Info,
    ParentClosePolicy,
    _Definition,
)


def make_instance(workflow_cls: type) -> TrioWorkflowInstance:
    """Create a workflow instance for testing."""
    defn = _Definition.must_from_class(workflow_cls)
    info = Info(
        workflow_id="test-wf-id",
        workflow_type=defn.name or "",
        run_id="test-run-id",
        task_queue="test-queue",
        namespace="default",
        attempt=1,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    det = WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=12345,
    )
    return TrioWorkflowInstance(det)


class TestChildWorkflowStartCommand:
    """Tests for StartChildWorkflowCommand generation."""

    def test_start_child_workflow_creates_command(self):
        """Test that starting a child workflow creates a StartChildWorkflowCommand."""

        @workflow.defn
        class ParentWorkflow:
            @workflow.run
            async def run(self) -> str:
                handle = await workflow.start_child_workflow(
                    "ChildWorkflow",
                    "arg1",
                    id="child-1",
                )
                return "waiting"

        instance = make_instance(ParentWorkflow)

        # First activation - workflow starts
        act = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="ParentWorkflow", args=())],
            timestamp_ns=1000000000,
        )
        completion = instance.activate(act)

        # Should have a start child workflow command
        assert len(completion.commands) == 1
        cmd = completion.commands[0]
        assert isinstance(cmd, StartChildWorkflowCommand)
        assert cmd.seq == 0
        assert cmd.workflow_id == "child-1"
        assert cmd.workflow_type == "ChildWorkflow"
        assert cmd.args == ("arg1",)

    def test_start_child_workflow_with_all_options(self):
        """Test starting a child workflow with all options."""

        retry_policy = temporalio.common.RetryPolicy(
            initial_interval=timedelta(seconds=1),
            maximum_attempts=3,
        )

        @workflow.defn
        class ParentWorkflow:
            @workflow.run
            async def run(self) -> str:
                handle = await workflow.start_child_workflow(
                    "ChildWorkflow",
                    args=["arg1", "arg2", 42],
                    id="child-1",
                    task_queue="child-queue",
                    execution_timeout=timedelta(hours=1),
                    run_timeout=timedelta(minutes=30),
                    task_timeout=timedelta(minutes=5),
                    parent_close_policy=ParentClosePolicy.ABANDON,
                    cancellation_type=ChildWorkflowCancellationType.TRY_CANCEL,
                    id_reuse_policy=temporalio.common.WorkflowIDReusePolicy.REJECT_DUPLICATE,
                    retry_policy=retry_policy,
                )
                return "waiting"

        instance = make_instance(ParentWorkflow)

        act = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="ParentWorkflow", args=())],
            timestamp_ns=1000000000,
        )
        completion = instance.activate(act)

        assert len(completion.commands) == 1
        cmd = completion.commands[0]
        assert isinstance(cmd, StartChildWorkflowCommand)
        assert cmd.workflow_id == "child-1"
        assert cmd.args == ("arg1", "arg2", 42)
        assert cmd.task_queue == "child-queue"
        assert cmd.execution_timeout == timedelta(hours=1)
        assert cmd.run_timeout == timedelta(minutes=30)
        assert cmd.task_timeout == timedelta(minutes=5)
        assert cmd.parent_close_policy == ParentClosePolicy.ABANDON.value
        assert cmd.cancellation_type == ChildWorkflowCancellationType.TRY_CANCEL.value
        assert (
            cmd.id_reuse_policy
            == temporalio.common.WorkflowIDReusePolicy.REJECT_DUPLICATE.value
        )
        assert cmd.retry_policy is retry_policy

    def test_start_child_workflow_with_class_reference(self):
        """Test starting a child workflow using class reference."""

        @workflow.defn
        class ChildWorkflow:
            @workflow.run
            async def run(self, arg: str) -> str:
                return arg

        @workflow.defn
        class ParentWorkflow:
            @workflow.run
            async def run(self) -> str:
                handle = await workflow.start_child_workflow(
                    ChildWorkflow,
                    "arg1",
                    id="child-1",
                )
                return "waiting"

        instance = make_instance(ParentWorkflow)

        act = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="ParentWorkflow", args=())],
            timestamp_ns=1000000000,
        )
        completion = instance.activate(act)

        assert len(completion.commands) == 1
        cmd = completion.commands[0]
        assert isinstance(cmd, StartChildWorkflowCommand)
        assert cmd.workflow_type == "ChildWorkflow"


class TestChildWorkflowStartedJob:
    """Tests for ChildWorkflowStartedJob handling."""

    def test_child_workflow_started_stored(self):
        """Test that ChildWorkflowStartedJob stores run_id."""

        @workflow.defn
        class ParentWorkflow:
            @workflow.run
            async def run(self) -> str:
                handle = await workflow.start_child_workflow(
                    "ChildWorkflow",
                    id="child-1",
                )
                return "waiting"

        instance = make_instance(ParentWorkflow)

        # First activation - start child workflow
        act1 = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="ParentWorkflow", args=())],
            timestamp_ns=1000000000,
        )
        completion1 = instance.activate(act1)
        assert len(completion1.commands) == 1
        assert isinstance(completion1.commands[0], StartChildWorkflowCommand)

        # Second activation - child workflow started
        act2 = WorkflowActivation(
            jobs=[ChildWorkflowStartedJob(seq=0, run_id="child-run-123")],
            timestamp_ns=2000000000,
        )
        completion2 = instance.activate(act2)

        # Workflow should still be waiting for child to complete
        # (no complete workflow command yet, just waiting)
        # The child has started but not resolved


class TestChildWorkflowResolvedJob:
    """Tests for ChildWorkflowResolvedJob handling."""

    def test_child_workflow_completed_result_available_on_replay(self):
        """Test that completed child workflow result is available on replay."""

        @workflow.defn
        class ParentWorkflow:
            @workflow.run
            async def run(self) -> str:
                result = await workflow.execute_child_workflow(
                    "ChildWorkflow",
                    id="child-1",
                )
                return f"child returned: {result}"

        instance = make_instance(ParentWorkflow)

        # First activation - start and get started
        act1 = WorkflowActivation(
            jobs=[
                WorkflowStartedJob(workflow_type="ParentWorkflow", args=()),
            ],
            timestamp_ns=1000000000,
        )
        completion1 = instance.activate(act1)
        assert len(completion1.commands) == 1
        assert isinstance(completion1.commands[0], StartChildWorkflowCommand)

        # Second activation - child started and resolved
        act2 = WorkflowActivation(
            jobs=[
                ChildWorkflowStartedJob(seq=0, run_id="child-run-123"),
                ChildWorkflowResolvedJob(seq=0, result="hello from child"),
            ],
            timestamp_ns=2000000000,
        )
        completion2 = instance.activate(act2)

        # Now the parent should complete with the child's result
        from temporalio_trio.worker._activation import CompleteWorkflowCommand

        assert len(completion2.commands) == 1
        assert isinstance(completion2.commands[0], CompleteWorkflowCommand)
        assert completion2.commands[0].result == "child returned: hello from child"

    def test_child_workflow_failed(self):
        """Test handling of failed child workflow."""

        @workflow.defn
        class ParentWorkflow:
            @workflow.run
            async def run(self) -> str:
                try:
                    result = await workflow.execute_child_workflow(
                        "ChildWorkflow",
                        id="child-1",
                    )
                    return f"child returned: {result}"
                except RuntimeError as e:
                    return f"child failed: {e}"

        instance = make_instance(ParentWorkflow)

        # First activation - start
        act1 = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="ParentWorkflow", args=())],
            timestamp_ns=1000000000,
        )
        completion1 = instance.activate(act1)

        # Second activation - child started and failed
        act2 = WorkflowActivation(
            jobs=[
                ChildWorkflowStartedJob(seq=0, run_id="child-run-123"),
                ChildWorkflowResolvedJob(
                    seq=0, failure=RuntimeError("Child workflow error")
                ),
            ],
            timestamp_ns=2000000000,
        )
        completion2 = instance.activate(act2)

        from temporalio_trio.worker._activation import CompleteWorkflowCommand

        assert len(completion2.commands) == 1
        assert isinstance(completion2.commands[0], CompleteWorkflowCommand)
        assert "child failed:" in completion2.commands[0].result


class TestChildWorkflowStartFailedJob:
    """Tests for ChildWorkflowStartFailedJob handling."""

    def test_child_workflow_start_failed(self):
        """Test handling of child workflow that failed to start."""

        @workflow.defn
        class ParentWorkflow:
            @workflow.run
            async def run(self) -> str:
                try:
                    handle = await workflow.start_child_workflow(
                        "ChildWorkflow",
                        id="child-1",
                    )
                    return "started"
                except RuntimeError as e:
                    return f"start failed: {e}"

        instance = make_instance(ParentWorkflow)

        # First activation - start
        act1 = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="ParentWorkflow", args=())],
            timestamp_ns=1000000000,
        )
        completion1 = instance.activate(act1)

        # Second activation - child failed to start
        act2 = WorkflowActivation(
            jobs=[
                ChildWorkflowStartFailedJob(
                    seq=0,
                    workflow_id="child-1",
                    workflow_type="ChildWorkflow",
                    cause="WORKFLOW_ALREADY_EXISTS",
                ),
            ],
            timestamp_ns=2000000000,
        )
        completion2 = instance.activate(act2)

        from temporalio_trio.worker._activation import CompleteWorkflowCommand

        assert len(completion2.commands) == 1
        assert isinstance(completion2.commands[0], CompleteWorkflowCommand)
        assert "start failed:" in completion2.commands[0].result


class TestMultipleChildWorkflows:
    """Tests for multiple child workflows."""

    def test_multiple_child_workflows_sequential(self):
        """Test starting multiple child workflows sequentially."""

        @workflow.defn
        class ParentWorkflow:
            @workflow.run
            async def run(self) -> str:
                result1 = await workflow.execute_child_workflow(
                    "ChildWorkflow",
                    "arg1",
                    id="child-1",
                )
                result2 = await workflow.execute_child_workflow(
                    "ChildWorkflow",
                    "arg2",
                    id="child-2",
                )
                return f"{result1}, {result2}"

        instance = make_instance(ParentWorkflow)

        # First activation - start first child
        act1 = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="ParentWorkflow", args=())],
            timestamp_ns=1000000000,
        )
        completion1 = instance.activate(act1)
        assert len(completion1.commands) == 1
        cmd1 = completion1.commands[0]
        assert isinstance(cmd1, StartChildWorkflowCommand)
        assert cmd1.seq == 0
        assert cmd1.workflow_id == "child-1"

        # Second activation - first child completes, start second child
        act2 = WorkflowActivation(
            jobs=[
                ChildWorkflowStartedJob(seq=0, run_id="child-run-1"),
                ChildWorkflowResolvedJob(seq=0, result="hello1"),
            ],
            timestamp_ns=2000000000,
        )
        completion2 = instance.activate(act2)
        assert len(completion2.commands) == 1
        cmd2 = completion2.commands[0]
        assert isinstance(cmd2, StartChildWorkflowCommand)
        assert cmd2.seq == 1
        assert cmd2.workflow_id == "child-2"

        # Third activation - second child completes
        act3 = WorkflowActivation(
            jobs=[
                ChildWorkflowStartedJob(seq=1, run_id="child-run-2"),
                ChildWorkflowResolvedJob(seq=1, result="hello2"),
            ],
            timestamp_ns=3000000000,
        )
        completion3 = instance.activate(act3)

        from temporalio_trio.worker._activation import CompleteWorkflowCommand

        assert len(completion3.commands) == 1
        assert isinstance(completion3.commands[0], CompleteWorkflowCommand)
        assert completion3.commands[0].result == "hello1, hello2"
