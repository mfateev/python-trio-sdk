"""Tests for child workflow activation types.

These tests verify the dataclasses used for child workflow commands and jobs.
"""

from __future__ import annotations

from datetime import timedelta

import temporalio.common

from temporalio_trio.worker._activation import (
    CancelChildWorkflowCommand,
    ChildWorkflowResolvedJob,
    ChildWorkflowStartedJob,
    ChildWorkflowStartFailedJob,
    StartChildWorkflowCommand,
    WorkflowCommand,
    WorkflowJob,
)


class TestChildWorkflowStartedJob:
    """Tests for ChildWorkflowStartedJob dataclass."""

    def test_creation(self):
        """Test creating a ChildWorkflowStartedJob."""
        job = ChildWorkflowStartedJob(seq=1, run_id="run-123")
        assert job.seq == 1
        assert job.run_id == "run-123"

    def test_equality(self):
        """Test equality comparison."""
        job1 = ChildWorkflowStartedJob(seq=1, run_id="run-123")
        job2 = ChildWorkflowStartedJob(seq=1, run_id="run-123")
        job3 = ChildWorkflowStartedJob(seq=2, run_id="run-456")

        assert job1 == job2
        assert job1 != job3

    def test_is_workflow_job(self):
        """Test that ChildWorkflowStartedJob is a WorkflowJob."""
        job = ChildWorkflowStartedJob(seq=1, run_id="run-123")
        # Should be valid as a WorkflowJob union member
        assert isinstance(job, ChildWorkflowStartedJob)
        # Check it's in the union (duck typing check)
        jobs: list[WorkflowJob] = [job]
        assert len(jobs) == 1


class TestChildWorkflowStartFailedJob:
    """Tests for ChildWorkflowStartFailedJob dataclass."""

    def test_creation(self):
        """Test creating a ChildWorkflowStartFailedJob."""
        job = ChildWorkflowStartFailedJob(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
            cause="WORKFLOW_ALREADY_EXISTS",
        )
        assert job.seq == 1
        assert job.workflow_id == "child-1"
        assert job.workflow_type == "ChildWorkflow"
        assert job.cause == "WORKFLOW_ALREADY_EXISTS"

    def test_equality(self):
        """Test equality comparison."""
        job1 = ChildWorkflowStartFailedJob(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
            cause="WORKFLOW_ALREADY_EXISTS",
        )
        job2 = ChildWorkflowStartFailedJob(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
            cause="WORKFLOW_ALREADY_EXISTS",
        )
        job3 = ChildWorkflowStartFailedJob(
            seq=2,
            workflow_id="child-2",
            workflow_type="OtherWorkflow",
            cause="OTHER_CAUSE",
        )

        assert job1 == job2
        assert job1 != job3

    def test_is_workflow_job(self):
        """Test that ChildWorkflowStartFailedJob is a WorkflowJob."""
        job = ChildWorkflowStartFailedJob(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
            cause="CAUSE",
        )
        jobs: list[WorkflowJob] = [job]
        assert len(jobs) == 1


class TestChildWorkflowResolvedJob:
    """Tests for ChildWorkflowResolvedJob dataclass."""

    def test_creation_with_result(self):
        """Test creating a ChildWorkflowResolvedJob with result."""
        job = ChildWorkflowResolvedJob(seq=1, result="hello")
        assert job.seq == 1
        assert job.result == "hello"
        assert job.failure is None

    def test_creation_with_failure(self):
        """Test creating a ChildWorkflowResolvedJob with failure."""
        error = RuntimeError("Child workflow failed")
        job = ChildWorkflowResolvedJob(seq=1, failure=error)
        assert job.seq == 1
        assert job.result is None
        assert job.failure is error

    def test_creation_defaults(self):
        """Test default values."""
        job = ChildWorkflowResolvedJob(seq=1)
        assert job.seq == 1
        assert job.result is None
        assert job.failure is None

    def test_equality(self):
        """Test equality comparison."""
        job1 = ChildWorkflowResolvedJob(seq=1, result="hello")
        job2 = ChildWorkflowResolvedJob(seq=1, result="hello")
        job3 = ChildWorkflowResolvedJob(seq=1, result="world")

        assert job1 == job2
        assert job1 != job3

    def test_is_workflow_job(self):
        """Test that ChildWorkflowResolvedJob is a WorkflowJob."""
        job = ChildWorkflowResolvedJob(seq=1, result="hello")
        jobs: list[WorkflowJob] = [job]
        assert len(jobs) == 1


class TestStartChildWorkflowCommand:
    """Tests for StartChildWorkflowCommand dataclass."""

    def test_creation_minimal(self):
        """Test creating with minimal arguments."""
        cmd = StartChildWorkflowCommand(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
        )
        assert cmd.seq == 1
        assert cmd.workflow_id == "child-1"
        assert cmd.workflow_type == "ChildWorkflow"
        assert cmd.args == ()
        assert cmd.task_queue is None
        assert cmd.execution_timeout is None
        assert cmd.run_timeout is None
        assert cmd.task_timeout is None
        assert cmd.parent_close_policy == 1  # TERMINATE
        assert cmd.cancellation_type == 2  # WAIT_CANCELLATION_COMPLETED
        assert cmd.retry_policy is None
        assert cmd.id_reuse_policy == 1  # ALLOW_DUPLICATE

    def test_creation_full(self):
        """Test creating with all arguments."""
        retry_policy = temporalio.common.RetryPolicy(
            initial_interval=timedelta(seconds=1),
            maximum_attempts=3,
        )
        cmd = StartChildWorkflowCommand(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
            args=("arg1", "arg2"),
            task_queue="child-queue",
            execution_timeout=timedelta(hours=1),
            run_timeout=timedelta(minutes=30),
            task_timeout=timedelta(minutes=5),
            parent_close_policy=2,  # ABANDON
            cancellation_type=1,  # TRY_CANCEL
            retry_policy=retry_policy,
            id_reuse_policy=2,  # ALLOW_DUPLICATE_FAILED_ONLY
        )
        assert cmd.seq == 1
        assert cmd.workflow_id == "child-1"
        assert cmd.workflow_type == "ChildWorkflow"
        assert cmd.args == ("arg1", "arg2")
        assert cmd.task_queue == "child-queue"
        assert cmd.execution_timeout == timedelta(hours=1)
        assert cmd.run_timeout == timedelta(minutes=30)
        assert cmd.task_timeout == timedelta(minutes=5)
        assert cmd.parent_close_policy == 2
        assert cmd.cancellation_type == 1
        assert cmd.retry_policy is retry_policy
        assert cmd.id_reuse_policy == 2

    def test_equality(self):
        """Test equality comparison."""
        cmd1 = StartChildWorkflowCommand(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
        )
        cmd2 = StartChildWorkflowCommand(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
        )
        cmd3 = StartChildWorkflowCommand(
            seq=2,
            workflow_id="child-2",
            workflow_type="OtherWorkflow",
        )

        assert cmd1 == cmd2
        assert cmd1 != cmd3

    def test_is_workflow_command(self):
        """Test that StartChildWorkflowCommand is a WorkflowCommand."""
        cmd = StartChildWorkflowCommand(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
        )
        commands: list[WorkflowCommand] = [cmd]
        assert len(commands) == 1


class TestCancelChildWorkflowCommand:
    """Tests for CancelChildWorkflowCommand dataclass."""

    def test_creation(self):
        """Test creating a CancelChildWorkflowCommand."""
        cmd = CancelChildWorkflowCommand(seq=1)
        assert cmd.seq == 1

    def test_equality(self):
        """Test equality comparison."""
        cmd1 = CancelChildWorkflowCommand(seq=1)
        cmd2 = CancelChildWorkflowCommand(seq=1)
        cmd3 = CancelChildWorkflowCommand(seq=2)

        assert cmd1 == cmd2
        assert cmd1 != cmd3

    def test_is_workflow_command(self):
        """Test that CancelChildWorkflowCommand is a WorkflowCommand."""
        cmd = CancelChildWorkflowCommand(seq=1)
        commands: list[WorkflowCommand] = [cmd]
        assert len(commands) == 1


class TestTypeUnions:
    """Tests for type union compatibility."""

    def test_all_child_workflow_jobs_in_union(self):
        """Test all child workflow jobs are in WorkflowJob union."""
        jobs: list[WorkflowJob] = [
            ChildWorkflowStartedJob(seq=1, run_id="run-1"),
            ChildWorkflowStartFailedJob(
                seq=2, workflow_id="wf-1", workflow_type="Wf", cause="CAUSE"
            ),
            ChildWorkflowResolvedJob(seq=3, result="result"),
        ]
        assert len(jobs) == 3

    def test_all_child_workflow_commands_in_union(self):
        """Test all child workflow commands are in WorkflowCommand union."""
        commands: list[WorkflowCommand] = [
            StartChildWorkflowCommand(seq=1, workflow_id="wf-1", workflow_type="Wf"),
            CancelChildWorkflowCommand(seq=2),
        ]
        assert len(commands) == 2
