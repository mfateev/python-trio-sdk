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
    "SignalWorkflowJob",
    "QueryWorkflowJob",
    "WorkflowActivation",
    # Workflow commands
    "StartTimerCommand",
    "CancelTimerCommand",
    "CompleteWorkflowCommand",
    "FailWorkflowCommand",
    "CancelWorkflowCommand",
    "QueryResultCommand",
    "WorkflowActivationCompletion",
    # Activity jobs (for workflow-activity integration)
    "ActivityResolvedJob",
    "ActivityCancelledJob",
    # Activity commands (for workflow-activity integration)
    "ScheduleActivityCommand",
    "RequestCancelActivityCommand",
    # Child workflow jobs
    "ChildWorkflowStartedJob",
    "ChildWorkflowStartFailedJob",
    "ChildWorkflowResolvedJob",
    # Child workflow commands
    "StartChildWorkflowCommand",
    "CancelChildWorkflowCommand",
    # Continue-as-new command
    "ContinueAsNewCommand",
    # Signal external workflow
    "SignalExternalWorkflowCommand",
    "SignalExternalResolvedJob",
    # Search attribute commands
    "UpsertSearchAttributesCommand",
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


@dataclass
class SignalWorkflowJob:
    """Job indicating a signal was received.

    This job is sent when an external signal has been sent to the workflow.
    The workflow should invoke the appropriate signal handler.

    Attributes:
        signal_name: Name of the signal to invoke.
        args: Arguments to pass to the signal handler.
        headers: Optional headers associated with the signal.
    """

    signal_name: str
    """Name of the signal to invoke."""

    args: tuple[Any, ...]
    """Arguments to pass to the signal handler."""

    headers: dict[str, Any] | None = None
    """Optional headers associated with the signal."""


@dataclass
class QueryWorkflowJob:
    """Job to handle a query.

    This job is sent when a query is made against the workflow.
    The workflow should invoke the appropriate query handler and return a result.

    Attributes:
        query_id: Unique identifier for this query request.
        query_type: Name of the query to invoke.
        args: Arguments to pass to the query handler.
        headers: Optional headers associated with the query.
    """

    query_id: str
    """Unique identifier for this query request."""

    query_type: str
    """Name of the query to invoke."""

    args: tuple[Any, ...]
    """Arguments to pass to the query handler."""

    headers: dict[str, Any] | None = None
    """Optional headers associated with the query."""


# Type alias defined at bottom of file after all types are defined
# WorkflowJob = WorkflowStartedJob | TimerFiredJob | CancelWorkflowJob | ActivityResolvedJob | SignalWorkflowJob | QueryWorkflowJob


@dataclass
class WorkflowActivation:
    """Activation containing jobs to process.

    An activation represents a batch of jobs from the Temporal server that
    the workflow needs to process. The workflow processes all jobs and returns
    a completion with commands.

    Attributes:
        jobs: List of jobs to process in this activation.
        timestamp_ns: Current workflow time in nanoseconds for this activation.
        run_id: Unique identifier for this workflow run (optional for tests).
    """

    jobs: list  # Generic list - actual type is WorkflowJob defined at bottom of file
    """List of jobs to process in this activation."""

    timestamp_ns: int
    """Current workflow time in nanoseconds for this activation."""

    run_id: str = ""
    """Unique identifier for this workflow run (set by bridge, optional for unit tests)."""


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
        summary: Optional human-readable description for UI/CLI visibility.
    """

    timer_id: int
    """Unique ID for this timer."""

    duration_ms: int
    """Timer duration in milliseconds."""

    summary: str | None = None
    """Optional human-readable description for UI/CLI visibility."""


@dataclass
class CancelTimerCommand:
    """Command to cancel a pending timer.

    This command requests the Temporal server to cancel a previously started
    timer. If the timer has already fired, this command has no effect.

    Attributes:
        timer_id: The ID of the timer to cancel (must match a StartTimerCommand).
    """

    timer_id: int
    """The ID of the timer to cancel."""


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


# Type alias defined at bottom of file after all types are defined
# WorkflowCommand = StartTimerCommand | CompleteWorkflowCommand | ...


@dataclass
class WorkflowActivationCompletion:
    """Completion with commands to execute.

    A completion represents the workflow's response to an activation. It contains
    commands that tell the Temporal server what actions to take (e.g., start timer,
    complete workflow).

    Attributes:
        commands: List of commands to send to the server.
    """

    commands: (
        list  # Generic list - actual type is WorkflowCommand defined at bottom of file
    )
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


@dataclass
class QueryResultCommand:
    """Command to respond to a query.

    This command sends the result of a query back to the caller.
    Either result or error should be set, but not both.

    Attributes:
        query_id: The ID of the query being responded to.
        result: The result value (if successful).
        error: The error message (if failed).
    """

    query_id: str
    """The ID of the query being responded to."""

    result: Any | None = None
    """The result value (if successful)."""

    error: str | None = None
    """The error message (if failed)."""


# =============================================================================
# Child Workflow Jobs (for child workflow execution)
# =============================================================================


@dataclass
class ChildWorkflowStartedJob:
    """Job indicating a child workflow started successfully.

    This job is sent when a previously requested child workflow has been
    accepted and started by the Temporal server.

    Attributes:
        seq: Sequence number matching StartChildWorkflowCommand.
        run_id: The run ID of the started child workflow.
    """

    seq: int
    """Sequence number matching the StartChildWorkflowCommand."""

    run_id: str
    """The run ID of the started child workflow."""


@dataclass
class ChildWorkflowStartFailedJob:
    """Job indicating a child workflow failed to start.

    This job is sent when a child workflow could not be started, e.g., due to
    a workflow ID conflict or other server-side error.

    Attributes:
        seq: Sequence number matching StartChildWorkflowCommand.
        workflow_id: The requested workflow ID.
        workflow_type: The requested workflow type.
        cause: The reason the child workflow failed to start.
    """

    seq: int
    """Sequence number matching the StartChildWorkflowCommand."""

    workflow_id: str
    """The requested workflow ID."""

    workflow_type: str
    """The requested workflow type."""

    cause: str
    """The reason the child workflow failed to start."""


@dataclass
class ChildWorkflowResolvedJob:
    """Job indicating a child workflow completed, failed, or was cancelled.

    This job is sent when a previously started child workflow has finished
    execution. Either result or failure will be set, but not both.

    Attributes:
        seq: Sequence number matching StartChildWorkflowCommand.
        result: The result value (if completed successfully).
        failure: The exception (if failed or cancelled).
    """

    seq: int
    """Sequence number matching the StartChildWorkflowCommand."""

    result: Any | None = None
    """The result value (if completed successfully)."""

    failure: BaseException | None = None
    """The exception (if failed or cancelled)."""


# =============================================================================
# Child Workflow Commands (for child workflow execution)
# =============================================================================


@dataclass
class StartChildWorkflowCommand:
    """Command to start a child workflow.

    This command requests the Temporal server to start a new child workflow.
    When the child starts, the server will send a ChildWorkflowStartedJob or
    ChildWorkflowStartFailedJob. When the child completes, the server will
    send a ChildWorkflowResolvedJob.

    Attributes:
        seq: Unique sequence number for this command.
        workflow_id: ID for the child workflow.
        workflow_type: Name of the workflow type to execute.
        args: Arguments to pass to the child workflow.
        task_queue: Task queue to run child on (defaults to parent's).
        execution_timeout: Total timeout including retries.
        run_timeout: Timeout for a single run.
        task_timeout: Timeout for a single workflow task.
        parent_close_policy: What happens to child when parent closes.
        cancellation_type: How child reacts to parent cancellation.
        retry_policy: Retry policy for the child workflow.
        id_reuse_policy: How existing workflow IDs are treated.
    """

    seq: int
    """Unique sequence number for this command."""

    workflow_id: str
    """ID for the child workflow."""

    workflow_type: str
    """Name of the workflow type to execute."""

    args: tuple[Any, ...] = field(default_factory=tuple)
    """Arguments to pass to the child workflow."""

    task_queue: str | None = None
    """Task queue to run child on (defaults to parent's)."""

    execution_timeout: timedelta | None = None
    """Total timeout for child workflow including retries."""

    run_timeout: timedelta | None = None
    """Timeout for a single run of the child workflow."""

    task_timeout: timedelta | None = None
    """Timeout for a single workflow task of the child."""

    parent_close_policy: int = 1
    """What happens to child when parent closes. Default: TERMINATE (1)."""

    cancellation_type: int = 2
    """How child reacts to parent cancellation. Default: WAIT_CANCELLATION_COMPLETED (2)."""

    retry_policy: temporalio.common.RetryPolicy | None = None
    """Retry policy for the child workflow."""

    id_reuse_policy: int = 1
    """How existing workflow IDs are treated. Default: ALLOW_DUPLICATE (1)."""


@dataclass
class CancelChildWorkflowCommand:
    """Command to cancel a child workflow.

    This command requests cancellation of a running child workflow.
    The child workflow will receive a cancellation request.

    Attributes:
        seq: Sequence number of the child workflow to cancel.
    """

    seq: int
    """Sequence number of the child workflow to cancel."""


# =============================================================================
# Signal External Workflow (for signaling other workflows)
# =============================================================================


@dataclass
class SignalExternalWorkflowCommand:
    """Command to signal an external workflow.

    This command requests the Temporal server to send a signal to another
    workflow. When the signal is delivered, the server will send a
    SignalExternalResolvedJob.

    Attributes:
        seq: Unique sequence number for this command.
        workflow_id: Target workflow ID to signal.
        run_id: Optional specific run ID (empty string = current run).
        signal_name: Name of the signal to send.
        args: Arguments to pass with the signal.
    """

    seq: int
    """Unique sequence number for this command."""

    workflow_id: str
    """Target workflow ID to signal."""

    signal_name: str
    """Name of the signal to send."""

    run_id: str | None = None
    """Optional specific run ID (None/empty = current run)."""

    args: tuple[Any, ...] = field(default_factory=tuple)
    """Arguments to pass with the signal."""


@dataclass
class SignalExternalResolvedJob:
    """Job indicating a signal to external workflow was resolved.

    This job is sent when a previously requested signal to an external
    workflow has been delivered (success) or failed. Success is indicated
    by the absence of the failure field.

    Attributes:
        seq: Sequence number matching SignalExternalWorkflowCommand.
        failure: Exception if signal failed (None = success).
    """

    seq: int
    """Sequence number matching the SignalExternalWorkflowCommand."""

    failure: BaseException | None = None
    """Exception if signal failed. None indicates success."""


# =============================================================================
# Search Attribute Commands (for workflow visibility)
# =============================================================================


@dataclass
class UpsertSearchAttributesCommand:
    """Command to upsert workflow search attributes.

    This command updates the workflow's search attributes. Search attributes
    are used for workflow visibility and querying. The command performs an
    upsert operation - existing attributes are updated, new attributes are
    added, and missing attributes remain unchanged (not deleted).

    Attributes:
        search_attributes: Map of attribute name to value. Values are typed
            (Keyword, Int, Bool, etc.) but passed as Python values here;
            the data_converter handles encoding to Payloads.

    Note:
        - Search attributes are eventually consistent
        - Custom attributes must be registered with the Temporal server
        - This command does not generate a response job (one-way command)
    """

    search_attributes: dict[str, Any]
    """Map of attribute name to value."""


# =============================================================================
# Continue-As-New Command
# =============================================================================


@dataclass
class ContinueAsNewCommand:
    """Command to continue the workflow as a new execution.

    This command ends the current workflow execution and immediately starts
    a new execution with the same workflow ID but a new run ID. This is useful
    for long-running workflows to avoid unbounded history growth.

    Attributes:
        workflow_type: Name of the workflow type to execute (can be same or different).
        args: Arguments to pass to the new workflow execution.
        task_queue: Task queue for the new execution (defaults to current queue).
        run_timeout: Timeout for a single run of the new workflow.
        task_timeout: Timeout for a single workflow task of the new workflow.
        retry_policy: Retry policy for the new workflow.
    """

    workflow_type: str
    """Name of the workflow type to execute (can be same or different)."""

    args: tuple[Any, ...] = field(default_factory=tuple)
    """Arguments to pass to the new workflow execution."""

    task_queue: str | None = None
    """Task queue for the new execution (defaults to current queue)."""

    run_timeout: timedelta | None = None
    """Timeout for a single run of the new workflow."""

    task_timeout: timedelta | None = None
    """Timeout for a single workflow task of the new workflow."""

    retry_policy: temporalio.common.RetryPolicy | None = None
    """Retry policy for the new workflow."""


# =============================================================================
# Complete Type Aliases (defined after all types are available)
# =============================================================================

WorkflowJob = (
    WorkflowStartedJob
    | TimerFiredJob
    | CancelWorkflowJob
    | SignalWorkflowJob
    | QueryWorkflowJob
    | ActivityResolvedJob
    | ChildWorkflowStartedJob
    | ChildWorkflowStartFailedJob
    | ChildWorkflowResolvedJob
    | SignalExternalResolvedJob
)
"""Union type for all possible activation jobs."""

WorkflowCommand = (
    StartTimerCommand
    | CancelTimerCommand
    | CompleteWorkflowCommand
    | FailWorkflowCommand
    | CancelWorkflowCommand
    | ScheduleActivityCommand
    | RequestCancelActivityCommand
    | QueryResultCommand
    | StartChildWorkflowCommand
    | CancelChildWorkflowCommand
    | SignalExternalWorkflowCommand
    | UpsertSearchAttributesCommand
    | ContinueAsNewCommand
)
"""Union type for all possible completion commands."""
