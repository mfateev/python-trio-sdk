"""Workflow activation and completion types for Temporal with Trio.

This module defines the data structures for workflow activations (jobs from the
Temporal server) and completions (commands sent back to the server).

These are simplified versions of the protocol buffer types used in the SDK,
designed for the POC to demonstrate the activation/completion pattern.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import temporalio.common

__all__ = [
    # Workflow jobs
    "WorkflowStartedJob",
    "TimerFiredJob",
    "CancelWorkflowJob",
    "WorkflowActivation",
    # Workflow commands
    "StartTimerCommand",
    "CompleteWorkflowCommand",
    "FailWorkflowCommand",
    "CancelWorkflowCommand",
    "WorkflowActivationCompletion",
    # Activity jobs (for workflow-activity integration)
    "ActivityResolvedJob",
    "ActivityCancelledJob",
    # Activity commands (for workflow-activity integration)
    "ScheduleActivityCommand",
    "RequestCancelActivityCommand",
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
class CancelWorkflowJob:
    """Job indicating workflow cancellation was requested.

    This job is sent when a cancellation request has been received for the
    workflow. The workflow should clean up and exit gracefully.

    Attributes:
        details: Optional cancellation details/reason.
    """

    details: tuple[Any, ...] = ()
    """Optional cancellation details/reason."""


# Type alias for all job types
WorkflowJob = WorkflowStartedJob | TimerFiredJob | CancelWorkflowJob
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

    jobs: list[WorkflowStartedJob | TimerFiredJob | CancelWorkflowJob]
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
class CancelWorkflowCommand:
    """Command to mark workflow as cancelled.

    This command indicates the workflow was cancelled (in response to a
    cancellation request).
    """

    pass


# Type alias for all command types
WorkflowCommand = (
    StartTimerCommand | CompleteWorkflowCommand | FailWorkflowCommand | CancelWorkflowCommand
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
        StartTimerCommand | CompleteWorkflowCommand | FailWorkflowCommand | CancelWorkflowCommand
    ]
    """List of commands to send to the server."""


# =============================================================================
# Activity Jobs (for workflow-activity integration - Phase 2)
# =============================================================================


@dataclass
class ActivityResolvedJob:
    """Job indicating an activity has completed (success or failure).

    This job is sent when a previously scheduled activity finishes execution.
    The workflow can then access the result or handle the failure.

    Attributes:
        seq: Sequence number matching ScheduleActivityCommand.
        result: Activity result value (if successful).
        failure: Exception if activity failed (mutually exclusive with result).
    """

    seq: int
    """Sequence number matching the ScheduleActivityCommand."""

    result: Any | None = None
    """Activity result value (if successful)."""

    failure: BaseException | None = None
    """Exception if activity failed."""


@dataclass
class ActivityCancelledJob:
    """Job indicating an activity cancellation was acknowledged.

    This job is sent when a previously requested activity cancellation
    has been acknowledged by the server.

    Attributes:
        seq: Sequence number matching ScheduleActivityCommand.
    """

    seq: int
    """Sequence number matching the ScheduleActivityCommand."""


# =============================================================================
# Activity Commands (for workflow-activity integration - Phase 2)
# =============================================================================


@dataclass
class ScheduleActivityCommand:
    """Command to schedule an activity for execution.

    This command requests the Temporal server to schedule an activity.
    When the activity completes, the server will send an ActivityResolvedJob.

    Attributes:
        seq: Unique sequence number for this command.
        activity_id: User-provided activity ID (or auto-generated).
        activity_type: Name of the activity to execute.
        args: Arguments to pass to the activity.
        task_queue: Task queue to run activity on (defaults to workflow's).
        schedule_to_close_timeout: Max total time for activity.
        schedule_to_start_timeout: Max time to wait for worker to pick up.
        start_to_close_timeout: Max time for activity execution.
        heartbeat_timeout: Max time between heartbeats.
        retry_policy: Retry policy for the activity.
    """

    seq: int
    """Unique sequence number for this command."""

    activity_id: str
    """User-provided activity ID."""

    activity_type: str
    """Name of the activity to execute."""

    args: tuple[Any, ...] = field(default_factory=tuple)
    """Arguments to pass to the activity."""

    task_queue: str | None = None
    """Task queue to run activity on (defaults to workflow's)."""

    schedule_to_close_timeout: timedelta | None = None
    """Max total time for activity (schedule to completion)."""

    schedule_to_start_timeout: timedelta | None = None
    """Max time to wait for a worker to pick up the activity."""

    start_to_close_timeout: timedelta | None = None
    """Max time for activity execution (from start to completion)."""

    heartbeat_timeout: timedelta | None = None
    """Max time between heartbeats. Activity must heartbeat or be considered dead."""

    retry_policy: temporalio.common.RetryPolicy | None = None
    """Retry policy for the activity."""


@dataclass
class RequestCancelActivityCommand:
    """Command to request cancellation of a scheduled activity.

    This command requests cancellation of a previously scheduled activity.
    The activity will receive a cancellation request and should clean up
    and exit gracefully.

    Attributes:
        seq: Sequence number of the activity to cancel.
    """

    seq: int
    """Sequence number of the activity to cancel."""
