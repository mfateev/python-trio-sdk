"""Workflow activation and completion types for Temporal with Trio.

This module defines the data structures for workflow activations (jobs from the
Temporal server) and completions (commands sent back to the server).

These are simplified versions of the protocol buffer types used in the SDK,
designed for the POC to demonstrate the activation/completion pattern.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import temporalio.api.common.v1
import temporalio.common

__all__ = [
    # Workflow jobs
    "WorkflowStartedJob",
    "TimerFiredJob",
    "CancelWorkflowJob",
    "SignalWorkflowJob",
    "QueryWorkflowJob",
    "NotifyHasPatchJob",
    "WorkflowActivation",
    # Workflow commands
    "StartTimerCommand",
    "CancelTimerCommand",
    "CompleteWorkflowCommand",
    "FailWorkflowCommand",
    "CancelWorkflowCommand",
    "QueryResultCommand",
    "SetPatchMarkerCommand",
    "WorkflowActivationCompletion",
    # Activity jobs (for workflow-activity integration)
    "ActivityResolvedJob",
    "ActivityCancelledJob",
    # Activity commands (for workflow-activity integration)
    "ScheduleActivityCommand",
    "ScheduleLocalActivityCommand",
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
    # Cancel external workflow
    "RequestCancelExternalWorkflowCommand",
    "CancelExternalResolvedJob",
    # Search attribute commands
    "UpsertSearchAttributesCommand",
    # Update workflow
    "UpdateWorkflowJob",
    "UpdateResponseCommand",
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

    workflow_id: str = ""
    """Workflow ID from the server."""

    headers: Mapping[str, temporalio.api.common.v1.Payload] = field(
        default_factory=dict
    )
    """Headers from the workflow start (e.g. for tracing/auth interceptors)."""

    namespace: str = "default"
    """Namespace the workflow is running in."""

    attempt: int = 1
    """Starting at 1, the number of attempts for this workflow."""

    start_time_ns: int = 0
    """When the workflow started, in nanoseconds since epoch."""

    execution_timeout_ms: int | None = None
    """Total workflow execution timeout in milliseconds."""

    run_timeout_ms: int | None = None
    """Timeout of a single workflow run in milliseconds."""

    task_timeout_ms: int | None = None
    """Timeout of a single workflow task in milliseconds."""

    retry_policy: temporalio.common.RetryPolicy | None = None
    """Retry policy for this workflow."""

    continued_run_id: str | None = None
    """Run ID of the previous workflow which continued-as-new into this one."""

    cron_schedule: str | None = None
    """Cron schedule if this workflow runs on a cron."""

    parent_namespace: str | None = None
    """Namespace of the parent workflow, if this is a child."""

    parent_workflow_id: str | None = None
    """Workflow ID of the parent workflow, if this is a child."""

    parent_run_id: str | None = None
    """Run ID of the parent workflow, if this is a child."""

    root_workflow_id: str | None = None
    """Workflow ID of the root workflow."""

    root_run_id: str | None = None
    """Run ID of the root workflow."""

    raw_memo: Mapping[str, temporalio.api.common.v1.Payload] = field(
        default_factory=dict
    )
    """Raw memo payloads from the workflow start."""

    priority: temporalio.common.Priority = field(
        default_factory=lambda: temporalio.common.Priority.default
    )
    """Priority for this workflow."""


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

    headers: Mapping[str, temporalio.api.common.v1.Payload] = field(
        default_factory=dict
    )
    """Headers associated with the signal (e.g. for tracing/auth interceptors)."""


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

    headers: Mapping[str, temporalio.api.common.v1.Payload] = field(
        default_factory=dict
    )
    """Headers associated with the query (e.g. for tracing/auth interceptors)."""


@dataclass
class NotifyHasPatchJob:
    """Job indicating a patch was recorded in history.

    This job is sent pre-emptively before workflow code runs to inform
    the runtime that a specific patch ID exists in the workflow history.
    This allows workflow.patched() to return the correct value during replay.

    Attributes:
        patch_id: The identifier of the patch that was recorded.
    """

    patch_id: str
    """The identifier of the patch that was recorded."""


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

    remove_from_cache: bool = False
    """Whether this is a cache eviction activation."""

    eviction_reason: int | None = None
    """Eviction reason code from remove_from_cache (if eviction)."""

    eviction_message: str | None = None
    """Eviction message from remove_from_cache (if eviction)."""

    is_replaying: bool = False
    """Whether this activation is replaying from history."""

    randomness_seed: int | None = None
    """Random seed for deterministic execution."""


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

    cancellation_type: int = 0
    """Activity cancellation type. Default: TRY_CANCEL (0)."""

    headers: Mapping[str, temporalio.api.common.v1.Payload] = field(
        default_factory=dict
    )
    """Headers to attach to the activity (e.g. for tracing/auth interceptors)."""


@dataclass
class ScheduleLocalActivityCommand:
    """Command to schedule a local activity for execution.

    Local activities run on the same task queue as the workflow and are
    optimized for short-lived activities that don't need to be recorded
    in the workflow history until they complete.

    This command requests the Temporal server to schedule a local activity.
    When the activity completes, the server will send an ActivityResolvedJob
    (same as regular activities).

    Attributes:
        seq: Unique sequence number for this command.
        activity_id: User-provided activity ID (or auto-generated).
        activity_type: Name of the activity to execute.
        args: Arguments to pass to the activity.
        schedule_to_close_timeout: Max total time for activity.
        schedule_to_start_timeout: Max time to wait for worker to pick up.
        start_to_close_timeout: Max time for activity execution.
        retry_policy: Retry policy for the activity.
        local_retry_threshold: Duration after which retries happen on the server
            instead of locally. If unset, retries always happen locally.
        cancellation_type: Activity cancellation type.
        headers: Headers to attach to the activity.
    """

    seq: int
    """Unique sequence number for this command."""

    activity_id: str
    """User-provided activity ID."""

    activity_type: str
    """Name of the activity to execute."""

    args: tuple[Any, ...] = field(default_factory=tuple)
    """Arguments to pass to the activity."""

    schedule_to_close_timeout: timedelta | None = None
    """Max total time for activity (schedule to completion)."""

    schedule_to_start_timeout: timedelta | None = None
    """Max time to wait for a worker to pick up the activity."""

    start_to_close_timeout: timedelta | None = None
    """Max time for activity execution (from start to completion)."""

    retry_policy: temporalio.common.RetryPolicy | None = None
    """Retry policy for the activity."""

    local_retry_threshold: timedelta | None = None
    """Duration after which retries happen on the server instead of locally."""

    cancellation_type: int = 0
    """Activity cancellation type. Default: TRY_CANCEL (0)."""

    headers: Mapping[str, temporalio.api.common.v1.Payload] = field(
        default_factory=dict
    )
    """Headers to attach to the activity (e.g. for tracing/auth interceptors)."""


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


@dataclass
class SetPatchMarkerCommand:
    """Command to record a patch marker in workflow history.

    This command is generated when workflow.patched() returns True,
    recording that the new code path was taken. During replay, the
    presence of this marker determines whether patched() returns True.

    Attributes:
        patch_id: The identifier for the patch.
        deprecated: Whether this patch is being marked as deprecated.
    """

    patch_id: str
    """The identifier for the patch."""

    deprecated: bool = False
    """Whether this patch is being marked as deprecated."""


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

    cron_schedule: str = ""
    """Cron schedule string for the child workflow."""

    memo: Mapping[str, Any] | None = None
    """Memo key-value pairs to attach to the child workflow."""

    search_attributes: temporalio.common.SearchAttributes | None = None
    """Search attributes to attach to the child workflow."""

    headers: Mapping[str, temporalio.api.common.v1.Payload] = field(
        default_factory=dict
    )
    """Headers to attach to the child workflow (e.g. for tracing/auth interceptors)."""


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

    headers: Mapping[str, temporalio.api.common.v1.Payload] = field(
        default_factory=dict
    )
    """Headers to attach to the signal (e.g. for tracing/auth interceptors)."""


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
# Cancel External Workflow (for cancelling other workflows)
# =============================================================================


@dataclass
class RequestCancelExternalWorkflowCommand:
    """Command to request cancellation of an external workflow.

    This command requests the Temporal server to send a cancellation request
    to another workflow. When the request is processed, the server will send
    a CancelExternalResolvedJob.

    Attributes:
        seq: Unique sequence number for this command.
        workflow_id: Target workflow ID to cancel.
        run_id: Optional specific run ID (empty string = current run).
    """

    seq: int
    """Unique sequence number for this command."""

    workflow_id: str
    """Target workflow ID to cancel."""

    run_id: str | None = None
    """Optional specific run ID (None/empty = current run)."""


@dataclass
class CancelExternalResolvedJob:
    """Job indicating a cancel request to external workflow was resolved.

    This job is sent when a previously requested cancellation of an external
    workflow has been processed (success) or failed. Success is indicated
    by the absence of the failure field.

    Attributes:
        seq: Sequence number matching RequestCancelExternalWorkflowCommand.
        failure: Exception if cancel request failed (None = success).
    """

    seq: int
    """Sequence number matching the RequestCancelExternalWorkflowCommand."""

    failure: BaseException | None = None
    """Exception if cancel request failed. None indicates success."""


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
        search_attributes: Typed search attribute updates. The bridge
            conversion uses ``encode_typed_search_attribute_value`` to
            produce payloads with the correct type metadata.

    Note:
        - Search attributes are eventually consistent
        - Custom attributes must be registered with the Temporal server
        - This command does not generate a response job (one-way command)
    """

    search_attributes: Sequence[temporalio.common.SearchAttributeUpdate]
    """Typed search attribute updates."""


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

    memo: Mapping[str, Any] | None = None
    """Memo key-value pairs to attach to the new execution."""

    search_attributes: temporalio.common.SearchAttributes | None = None
    """Search attributes to attach to the new execution."""

    headers: Mapping[str, temporalio.api.common.v1.Payload] = field(
        default_factory=dict
    )
    """Headers to attach to the new execution (e.g. for tracing/auth interceptors)."""


# =============================================================================
# Update Workflow (for workflow update handlers)
# =============================================================================


@dataclass
class UpdateWorkflowJob:
    """Job to invoke a workflow update handler.

    This job is sent when an update is requested on the workflow.
    The workflow should run the validator (if requested), accept/reject,
    then run the handler and return a result.

    Attributes:
        id: Workflow-unique identifier for this update.
        protocol_instance_id: Server-side tracking ID for the update protocol.
        name: Name of the update handler to invoke.
        args: Arguments to pass to the update handler.
        run_validator: Whether to run the validator before the handler.
        headers: Optional headers associated with the update.
    """

    id: str
    """Workflow-unique identifier for this update."""

    protocol_instance_id: str
    """Server-side tracking ID for the update protocol."""

    name: str
    """Name of the update handler to invoke."""

    args: tuple[Any, ...]
    """Arguments to pass to the update handler."""

    run_validator: bool = False
    """Whether to run the validator before the handler."""

    headers: Mapping[str, temporalio.api.common.v1.Payload] = field(
        default_factory=dict
    )
    """Headers associated with the update (e.g. for tracing/auth interceptors)."""


@dataclass
class UpdateResponseCommand:
    """Command to respond to a workflow update.

    Multiple instances with the same protocol_instance_id may be emitted:
    1. accepted=True (validator passed or not run)
    2. completed with result (handler done) OR rejected with failure (handler failed)

    Attributes:
        protocol_instance_id: Server-side tracking ID for the update protocol.
        accepted: Whether the update was accepted (validator passed).
        rejected_failure: Exception if validator or handler rejected the update.
        completed_result: Result value from the update handler.
    """

    protocol_instance_id: str
    """Server-side tracking ID for the update protocol."""

    accepted: bool = False
    """Whether the update was accepted (validator passed)."""

    rejected_failure: BaseException | None = None
    """Exception if validator or handler rejected the update."""

    completed_result: Any = None
    """Result value from the update handler."""

    _is_completed: bool = False
    """Internal flag to distinguish completion from empty/None result."""


# =============================================================================
# Complete Type Aliases (defined after all types are available)
# =============================================================================

WorkflowJob = (
    WorkflowStartedJob
    | TimerFiredJob
    | CancelWorkflowJob
    | SignalWorkflowJob
    | QueryWorkflowJob
    | NotifyHasPatchJob
    | ActivityResolvedJob
    | ChildWorkflowStartedJob
    | ChildWorkflowStartFailedJob
    | ChildWorkflowResolvedJob
    | SignalExternalResolvedJob
    | CancelExternalResolvedJob
    | UpdateWorkflowJob
)
"""Union type for all possible activation jobs."""

WorkflowCommand = (
    StartTimerCommand
    | CancelTimerCommand
    | CompleteWorkflowCommand
    | FailWorkflowCommand
    | CancelWorkflowCommand
    | ScheduleActivityCommand
    | ScheduleLocalActivityCommand
    | RequestCancelActivityCommand
    | QueryResultCommand
    | SetPatchMarkerCommand
    | StartChildWorkflowCommand
    | CancelChildWorkflowCommand
    | SignalExternalWorkflowCommand
    | RequestCancelExternalWorkflowCommand
    | UpsertSearchAttributesCommand
    | ContinueAsNewCommand
    | UpdateResponseCommand
)
"""Union type for all possible completion commands."""
