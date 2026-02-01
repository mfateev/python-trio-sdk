"""Workflow activation and completion types for Temporal with Trio.

This module defines the data structures for workflow activations (jobs from the
Temporal server) and completions (commands sent back to the server).

These are simplified versions of the protocol buffer types used in the SDK,
designed for the POC to demonstrate the activation/completion pattern.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Mapping, Sequence

__all__ = [
    "WorkflowStartedJob",
    "TimerFiredJob",
    "DoUpdateJob",
    "WorkflowActivation",
    "StartTimerCommand",
    "CompleteWorkflowCommand",
    "FailWorkflowCommand",
    "ContinueAsNewWorkflowCommand",
    "UpdateAcceptedCommand",
    "UpdateCompletedCommand",
    "UpdateRejectedCommand",
    "WorkflowActivationCompletion",
]


# =============================================================================
# Activation Jobs (from Temporal server)
# =============================================================================


@dataclass
class WorkflowStartedJob:
    """Job to start workflow execution.

    This job is sent when a workflow is first started or when resuming
    from the beginning during replay.

    Attributes:
        workflow_type: Name of the workflow type to execute.
        args: Arguments to pass to the workflow's run method.
    """

    workflow_type: str
    """Name of the workflow type to execute."""

    args: tuple[Any, ...]
    """Arguments to pass to the workflow's run method."""


@dataclass
class TimerFiredJob:
    """Job indicating a timer has fired.

    This job is sent when a previously scheduled timer's duration has elapsed.

    Attributes:
        timer_id: The ID of the timer that fired (matches StartTimerCommand).
    """

    timer_id: int
    """The ID of the timer that fired."""


@dataclass
class DoUpdateJob:
    """Job to process a workflow update.

    This job is sent when the workflow receives an update request. The workflow
    should run the validator (if requested) and then the handler.

    Attributes:
        id: Unique identifier for this update.
        protocol_instance_id: Protocol-level identifier for tracking.
        name: Name of the update handler to invoke.
        args: Arguments to pass to the update handler.
        run_validator: Whether to run the validator before the handler.
    """

    id: str
    """Unique identifier for this update."""

    protocol_instance_id: str
    """Protocol-level identifier for tracking."""

    name: str
    """Name of the update handler to invoke."""

    args: tuple[Any, ...]
    """Arguments to pass to the update handler."""

    run_validator: bool = True
    """Whether to run the validator before the handler."""


# Type alias for all job types
WorkflowJob = WorkflowStartedJob | TimerFiredJob | DoUpdateJob
"""Union type for all possible activation jobs."""


@dataclass
class WorkflowActivation:
    """Activation containing jobs to process.

    An activation represents a batch of jobs from the Temporal server that
    the workflow needs to process. The workflow processes all jobs and returns
    a completion with commands.

    Attributes:
        jobs: List of jobs to process in this activation.
        timestamp_ns: Current workflow time in nanoseconds for this activation.
    """

    jobs: list[WorkflowStartedJob | TimerFiredJob | DoUpdateJob]
    """List of jobs to process in this activation."""

    timestamp_ns: int
    """Current workflow time in nanoseconds for this activation."""


# =============================================================================
# Completion Commands (to Temporal server)
# =============================================================================


@dataclass
class StartTimerCommand:
    """Command to start a timer.

    This command requests the Temporal server to start a timer. When the timer
    fires, the server will send a TimerFiredJob activation.

    Attributes:
        timer_id: Unique ID for this timer (used to match TimerFiredJob).
        duration_ms: Timer duration in milliseconds.
    """

    timer_id: int
    """Unique ID for this timer."""

    duration_ms: int
    """Timer duration in milliseconds."""


@dataclass
class CompleteWorkflowCommand:
    """Command to complete the workflow.

    This command indicates the workflow has finished successfully with a result.

    Attributes:
        result: The result value from the workflow.
    """

    result: Any
    """The result value from the workflow."""


@dataclass
class FailWorkflowCommand:
    """Command to fail the workflow.

    This command indicates the workflow has failed with an exception.

    Attributes:
        exception: The exception that caused the workflow to fail.
    """

    exception: BaseException
    """The exception that caused the workflow to fail."""


@dataclass
class UpdateAcceptedCommand:
    """Command indicating an update was accepted (validation passed).

    Sent after the validator passes (or if no validator) to indicate
    the update will be processed.

    Attributes:
        protocol_instance_id: Protocol-level identifier from DoUpdateJob.
    """

    protocol_instance_id: str
    """Protocol-level identifier from DoUpdateJob."""


@dataclass
class UpdateCompletedCommand:
    """Command indicating an update completed successfully.

    Sent when the update handler finishes with a result.

    Attributes:
        protocol_instance_id: Protocol-level identifier from DoUpdateJob.
        result: The result value from the update handler.
    """

    protocol_instance_id: str
    """Protocol-level identifier from DoUpdateJob."""

    result: Any
    """The result value from the update handler."""


@dataclass
class UpdateRejectedCommand:
    """Command indicating an update was rejected.

    Sent when the validator rejects the update or an error occurs.

    Attributes:
        protocol_instance_id: Protocol-level identifier from DoUpdateJob.
        exception: The exception that caused the rejection.
    """

    protocol_instance_id: str
    """Protocol-level identifier from DoUpdateJob."""

    exception: BaseException
    """The exception that caused the rejection."""


@dataclass
class ContinueAsNewWorkflowCommand:
    """Command to continue the workflow as a new execution.

    This command tells the Temporal server to start a new workflow execution
    with the same or different parameters, continuing from where this one left off.

    Attributes:
        workflow: The workflow type name to continue as. None means same workflow.
        args: Arguments to pass to the new workflow execution.
        task_queue: Task queue for the new execution. None means same task queue.
        run_timeout: Run timeout for the new execution. None means same timeout.
        task_timeout: Task timeout for the new execution. None means same timeout.
        memo: Memo for the new execution. None means same memo.
    """

    workflow: str | None = None
    """The workflow type name to continue as. None means same workflow."""

    args: Sequence[Any] = field(default_factory=list)
    """Arguments to pass to the new workflow execution."""

    task_queue: str | None = None
    """Task queue for the new execution. None means same task queue."""

    run_timeout: timedelta | None = None
    """Run timeout for the new execution. None means same timeout."""

    task_timeout: timedelta | None = None
    """Task timeout for the new execution. None means same timeout."""

    memo: Mapping[str, Any] | None = None
    """Memo for the new execution. None means same memo."""


# Type alias for all command types
WorkflowCommand = (
    StartTimerCommand
    | CompleteWorkflowCommand
    | FailWorkflowCommand
    | ContinueAsNewWorkflowCommand
    | UpdateAcceptedCommand
    | UpdateCompletedCommand
    | UpdateRejectedCommand
)
"""Union type for all possible completion commands."""


@dataclass
class WorkflowActivationCompletion:
    """Completion with commands to execute.

    A completion represents the workflow's response to an activation. It contains
    commands that tell the Temporal server what actions to take (e.g., start timer,
    complete workflow).

    Attributes:
        commands: List of commands to send to the server.
    """

    commands: list[
        StartTimerCommand
        | CompleteWorkflowCommand
        | FailWorkflowCommand
        | ContinueAsNewWorkflowCommand
        | UpdateAcceptedCommand
        | UpdateCompletedCommand
        | UpdateRejectedCommand
    ]
    """List of commands to send to the server."""
