"""Per-workflow isolated runtime state for the single-threaded execution model.

This module provides the WorkflowRuntime class which holds all state for a single
workflow execution. Each workflow gets its own runtime instance stored in a
contextvars.ContextVar, ensuring proper isolation even when multiple workflows
run concurrently in the same thread.

This implements Phases 1, 2, and 4 of the single-threaded migration plan:
- Phase 1: ContextVar-based runtime
- Phase 2: Event-based timer implementation
- Phase 4: Activity support with event-based suspension
"""

from __future__ import annotations

import random
from collections.abc import Mapping, Sequence
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable, NoReturn, cast

import temporalio.activity
import temporalio.api.common.v1
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import trio

from temporalio_trio.workflow import ActivityCancellationType

if TYPE_CHECKING:
    from temporalio_trio.workflow import (
        ActivityHandle,
        ChildWorkflowCancellationType,
        ChildWorkflowHandle,
        ExternalWorkflowHandle,
        Info,
        ParentClosePolicy,
        VersioningIntent,
    )

from temporalio_trio.worker._activation import (
    CancelWorkflowCommand,
    ContinueAsNewCommand,
    RequestCancelExternalWorkflowCommand,
    ScheduleActivityCommand,
    ScheduleLocalActivityCommand,
    SignalExternalWorkflowCommand,
    StartChildWorkflowCommand,
    StartTimerCommand,
    UpdateResponseCommand,
    UpsertSearchAttributesCommand,
)

__all__ = [
    "WorkflowRuntime",
    "StartTimerCommand",
    "ScheduleActivityCommand",
    "StartChildWorkflowCommand",
    "QuerySuccessCommand",
    "QueryFailureCommand",
    "CancelWorkflowCommand",
    "SetPatchMarkerCommand",
    "get_current_runtime",
    "set_current_runtime",
    "reset_current_runtime",
    "NotInWorkflowRuntimeError",
]


class NotInWorkflowRuntimeError(RuntimeError):
    """Raised when workflow runtime is accessed outside of workflow context."""

    pass


@dataclass
class _ActivityBackoff:
    """Sentinel stored in completed_activities when a local activity needs to
    retry after a backoff delay. Contains the DoBackoff proto from sdk-core.
    """

    backoff: Any
    """The DoBackoff proto with attempt, backoff_duration, original_schedule_time."""


@dataclass
class QuerySuccessCommand:
    """Command to respond to a query with success.

    This command is emitted when a query handler executes successfully
    and returns a result.
    """

    query_id: str
    """The ID of the query being responded to."""

    result: Any
    """The result value from the query handler."""


@dataclass
class QueryFailureCommand:
    """Command to respond to a query with failure.

    This command is emitted when a query handler raises an exception.
    """

    query_id: str
    """The ID of the query being responded to."""

    error: BaseException
    """The exception raised by the query handler."""


@dataclass
class SetPatchMarkerCommand:
    """Command to record a patch marker in workflow history.

    This command is emitted when workflow.patched() is called on a new execution
    (not replaying). The marker is recorded in history so that during replay,
    patched() can return the correct value.
    """

    patch_id: str
    """The identifier for the patch."""

    deprecated: bool = False
    """Whether this patch is being marked as deprecated."""


@dataclass
class WorkflowRuntime:
    """Per-workflow isolated runtime state.

    This class holds all state for a single workflow execution. Each workflow
    gets its own runtime instance stored in a ContextVar, ensuring proper
    isolation when multiple workflows run concurrently in the same thread.

    The runtime tracks:
    - Identity: run_id, workflow_id, workflow_type, task_queue
    - Deterministic state: random generator, current time, replay flag
    - Sequence counters: for timers, activities, child workflows, signals
    - Fired events: events that have completed (for replay)
    - Pending events: events waiting for completion (with trio.Event for suspension)
    - Commands: workflow commands to emit
    - Workflow instance: the workflow object and its nursery
    """

    # Identity
    run_id: str
    """Unique identifier for this run of the workflow."""

    workflow_id: str
    """Unique identifier for the workflow."""

    workflow_type: str
    """Name of the workflow type."""

    task_queue: str
    """Task queue the workflow is running on."""

    # Deterministic state
    random: random.Random
    """Seeded random number generator for deterministic execution."""

    time_ns: int
    """Current workflow time in nanoseconds since epoch."""

    is_replaying: bool = False
    """Whether this activation is replaying from history."""

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

    retry_policy_obj: temporalio.common.RetryPolicy | None = None
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

    first_execution_run_id: str = ""
    """Run ID of the very first execution in the continue-as-new chain."""

    search_attributes_data: temporalio.common.SearchAttributes = field(
        default_factory=dict
    )
    """Search attributes (deprecated dict form)."""

    typed_search_attributes_data: temporalio.common.TypedSearchAttributes = field(
        default_factory=lambda: temporalio.common.TypedSearchAttributes([])
    )
    """Typed search attributes."""

    workflow_start_time_ns: int = 0
    """When the workflow was first started (not this run), in nanoseconds since epoch."""

    # Sequence counters
    timer_seq: int = 0
    """Sequence counter for timer IDs."""

    activity_seq: int = 0
    """Sequence counter for activity IDs."""

    child_workflow_seq: int = 0
    """Sequence counter for child workflow IDs."""

    signal_seq: int = 0
    """Sequence counter for signal IDs."""

    signal_external_seq: int = 0
    """Sequence counter for external signal IDs."""

    cancel_external_seq: int = 0
    """Sequence counter for external cancel IDs."""

    # Fired events (for replay) - populated from history
    fired_timers: dict[int, int] = field(default_factory=dict)
    """Timers that have fired: seq -> fire_time_ns."""

    completed_activities: dict[int, Any] = field(default_factory=dict)
    """Activities that have completed: seq -> result (or exception)."""

    completed_children: dict[int, Any] = field(default_factory=dict)
    """Child workflows that have completed: seq -> result (or exception)."""

    started_children: dict[int, str] = field(default_factory=dict)
    """Child workflows that have started: seq -> run_id."""

    child_workflow_ret_types: dict[int, type | None] = field(default_factory=dict)
    """Return type hints for child workflows: seq -> ret_type (for deserialization)."""

    completed_external_signals: dict[int, BaseException | None] = field(
        default_factory=dict
    )
    """External signals that have resolved: seq -> error (None = success)."""

    completed_external_cancels: dict[int, BaseException | None] = field(
        default_factory=dict
    )
    """External cancels that have resolved: seq -> error (None = success)."""

    # Pending events (for suspension) - trio.Event instances
    pending_timers: dict[int, trio.Event] = field(default_factory=dict)
    """Timers waiting to fire: seq -> trio.Event."""

    pending_activities: dict[int, trio.Event] = field(default_factory=dict)
    """Activities waiting to complete: seq -> trio.Event."""

    pending_child_starts: dict[int, trio.Event] = field(default_factory=dict)
    """Child workflows waiting to start: seq -> trio.Event."""

    pending_children: dict[int, trio.Event] = field(default_factory=dict)
    """Child workflows waiting to complete: seq -> trio.Event."""

    pending_external_signals: dict[int, trio.Event] = field(default_factory=dict)
    """External signals waiting for resolution: seq -> trio.Event."""

    pending_external_cancels: dict[int, trio.Event] = field(default_factory=dict)
    """External cancels waiting for resolution: seq -> trio.Event."""

    # Commands to emit
    commands: list[Any] = field(default_factory=list)
    """Commands generated during this activation."""

    # Workflow instance
    workflow_object: Any = None
    """The instantiated workflow class object."""

    nursery: trio.Nursery | None = None
    """The Trio nursery for this workflow's child tasks."""

    # Suspension callback (Phase 3)
    on_suspend: Callable[[], None] | None = None
    """Optional callback called when workflow is about to suspend on an event."""

    # Signal and query handlers (Phase 5)
    signal_handlers: dict[str, Callable[..., Any]] = field(default_factory=dict)
    """Signal handlers: signal_name -> handler function."""

    query_handlers: dict[str, Callable[..., Any]] = field(default_factory=dict)
    """Query handlers: query_name -> handler function."""

    # Update handlers
    update_handlers: dict[str | None, Callable[..., Any]] = field(default_factory=dict)
    """Update handlers: update_name -> handler function (None key for dynamic)."""

    update_validators: dict[str | None, Callable[..., None]] = field(
        default_factory=dict
    )
    """Update validators: update_name -> validator function (None key for dynamic)."""

    in_progress_updates: dict[str, str] = field(default_factory=dict)
    """In-progress update executions: update_id -> update_name."""

    # Interceptor chain
    inbound_interceptor: Any = None
    """The outermost inbound interceptor (or None if no interceptors)."""

    outbound_interceptor: Any = None
    """The outermost outbound interceptor (or None if no interceptors)."""

    # Cancellation (Phase 7)
    cancel_requested: bool = False
    """Whether workflow cancellation has been requested."""

    # Condition notification (for wait_condition)
    condition_waiters: list[trio.Event] = field(default_factory=list)
    """Events waiting for state changes (used by wait_condition)."""

    # Patching/Versioning (Phase 2 of feature parity)
    patches_notified: set[str] = field(default_factory=set)
    """Patch IDs that have been notified as existing in history (for replay)."""

    patches_memoized: dict[str, bool] = field(default_factory=dict)
    """Memoized results of patched() calls: patch_id -> result."""

    disable_eager_activity_execution: bool = False
    """If true, activities will not be eagerly dispatched to a local worker."""

    payload_converter: "temporalio.converter.PayloadConverter" = field(
        default_factory=lambda: temporalio.converter.DataConverter.default.payload_converter
    )
    """Payload converter for serializing/deserializing workflow data."""

    _read_only: bool = False
    """Whether the runtime is in read-only mode (e.g., during query handling)."""

    def notify_condition_waiters(self) -> None:
        """Notify all wait_condition waiters that state may have changed.

        This should be called after any event that might affect condition
        predicates, such as signal delivery.
        """
        for event in self.condition_waiters:
            event.set()
        self.condition_waiters.clear()

    def _assert_not_read_only(self, action_attempted: str) -> None:
        """Assert that the runtime is not in read-only mode.

        Args:
            action_attempted: Description of the action for the error message.

        Raises:
            ReadOnlyContextError: If the runtime is in read-only mode.
        """
        from temporalio_trio.workflow import ReadOnlyContextError

        if self._read_only:
            raise ReadOnlyContextError(
                f"While in read-only function, action attempted: {action_attempted}"
            )

    def _payload_converter_with_context(
        self,
        context: temporalio.converter.SerializationContext,
    ) -> temporalio.converter.PayloadConverter:
        """Construct payload converter with the given serialization context.

        This plays a similar role to sdk-python's _payload_converter_with_context.
        If the converter supports context, returns a context-aware wrapper.
        Otherwise returns the original converter.
        """
        payload_converter = self.payload_converter
        if isinstance(payload_converter, temporalio.converter.WithSerializationContext):
            payload_converter = payload_converter.with_context(context)
        return payload_converter

    def workflow_time_ns(self) -> int:
        """Get current workflow time in nanoseconds.

        Returns:
            Current workflow time in nanoseconds since epoch.
        """
        return self.time_ns

    def workflow_info(self) -> "Info":
        """Get information about the current workflow.

        Returns:
            Info about the current workflow execution.
        """
        from datetime import datetime, timedelta, timezone

        from temporalio_trio.workflow import Info, ParentInfo, RootInfo

        parent = None
        if self.parent_workflow_id is not None:
            parent = ParentInfo(
                namespace=self.parent_namespace or "",
                run_id=self.parent_run_id or "",
                workflow_id=self.parent_workflow_id,
            )

        root = None
        if self.root_workflow_id is not None:
            root = RootInfo(
                run_id=self.root_run_id or "",
                workflow_id=self.root_workflow_id,
            )

        return Info(
            attempt=self.attempt,
            continued_run_id=self.continued_run_id,
            cron_schedule=self.cron_schedule,
            execution_timeout=timedelta(milliseconds=self.execution_timeout_ms)
            if self.execution_timeout_ms is not None
            else None,
            first_execution_run_id=self.first_execution_run_id or self.run_id,
            headers=self.headers,
            namespace=self.namespace,
            parent=parent,
            root=root,
            priority=self.priority,
            raw_memo=self.raw_memo,
            retry_policy=self.retry_policy_obj,
            run_id=self.run_id,
            run_timeout=timedelta(milliseconds=self.run_timeout_ms)
            if self.run_timeout_ms is not None
            else None,
            search_attributes=self.search_attributes_data,
            start_time=datetime.fromtimestamp(
                self.start_time_ns / 1e9, tz=timezone.utc
            ),
            task_queue=self.task_queue,
            task_timeout=timedelta(milliseconds=self.task_timeout_ms)
            if self.task_timeout_ms is not None
            else timedelta(seconds=10),
            typed_search_attributes=self.typed_search_attributes_data,
            workflow_id=self.workflow_id,
            workflow_start_time=datetime.fromtimestamp(
                self.workflow_start_time_ns / 1e9, tz=timezone.utc
            ),
            workflow_type=self.workflow_type,
        )

    async def workflow_execute_activity(
        self,
        activity: str | Callable[..., Any],
        *args: Any,
        task_queue: str | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        retry_policy: Any = None,
        activity_id: str | None = None,
        cancellation_type: int = 0,
    ) -> Any:
        """Execute an activity and wait for its result.

        This is a wrapper around execute_activity that matches the _Runtime interface.

        Args:
            activity: Activity name (string) or function reference.
            *args: Arguments to pass to the activity.
            task_queue: Task queue to run the activity on.
            schedule_to_close_timeout: Max time for activity from schedule to completion.
            schedule_to_start_timeout: Max time waiting for worker to pick up activity.
            start_to_close_timeout: Max time for activity execution.
            heartbeat_timeout: Max time between heartbeats.
            retry_policy: Retry policy for the activity (not fully supported yet).
            activity_id: Optional unique identifier for the activity.

        Returns:
            The activity result.
        """
        # Extract activity name if a callable was passed
        activity_name = activity if isinstance(activity, str) else activity.__name__

        # Default task_queue to workflow's task queue if not specified
        actual_task_queue = task_queue if task_queue is not None else self.task_queue

        return await self.execute_activity(
            activity_name,
            args,
            activity_id=activity_id,
            task_queue=actual_task_queue,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            heartbeat_timeout=heartbeat_timeout,
            retry_policy=retry_policy,
            cancellation_type=cancellation_type,
        )

    async def workflow_execute_local_activity(
        self,
        activity: str | Callable[..., Any],
        *args: Any,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        retry_policy: Any = None,
        local_retry_threshold: timedelta | None = None,
        activity_id: str | None = None,
        cancellation_type: int = 0,
    ) -> Any:
        """Execute a local activity and wait for its result.

        This is a wrapper around execute_local_activity that matches the _Runtime interface.

        Args:
            activity: Activity name (string) or function reference.
            *args: Arguments to pass to the activity.
            schedule_to_close_timeout: Max time for activity from schedule to completion.
            schedule_to_start_timeout: Max time waiting for worker to pick up activity.
            start_to_close_timeout: Max time for activity execution.
            retry_policy: Retry policy for the activity.
            local_retry_threshold: Duration after which retries use the server.
            activity_id: Optional unique identifier for the activity.

        Returns:
            The activity result.
        """
        # Extract activity name if a callable was passed
        activity_name = activity if isinstance(activity, str) else activity.__name__

        return await self.execute_local_activity(
            activity_name,
            args,
            activity_id=activity_id,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            retry_policy=retry_policy,
            local_retry_threshold=local_retry_threshold,
            cancellation_type=cancellation_type,
        )

    async def workflow_start_child_workflow(
        self,
        workflow: Any,
        *args: Any,
        id: str,
        task_queue: str | None,
        result_type: type | None,
        cancellation_type: "ChildWorkflowCancellationType",
        parent_close_policy: "ParentClosePolicy",
        execution_timeout: timedelta | None,
        run_timeout: timedelta | None,
        task_timeout: timedelta | None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy,
        retry_policy: temporalio.common.RetryPolicy | None,
        cron_schedule: str,
        memo: Mapping[str, Any] | None,
        search_attributes: temporalio.common.SearchAttributes
        | temporalio.common.TypedSearchAttributes
        | None,
        versioning_intent: "VersioningIntent | None",
        static_summary: str | None = None,
        static_details: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> "ChildWorkflowHandle":
        """Start a child workflow and return a handle.

        Resolves workflow name, creates StartChildWorkflowInput, and dispatches
        through the outbound interceptor chain (matching sdk-python pattern).

        Args:
            workflow: Workflow class or type name.
            *args: Arguments to pass to the workflow.
            id: Unique workflow ID.
            task_queue: Task queue (defaults to parent's).
            result_type: Expected result type.
            cancellation_type: How child reacts to parent cancellation.
            parent_close_policy: What happens when parent closes.
            execution_timeout: Total timeout including retries.
            run_timeout: Timeout for a single run.
            task_timeout: Timeout for a single workflow task.
            id_reuse_policy: How existing IDs are treated.
            retry_policy: Retry policy for the workflow.
            cron_schedule: Optional cron schedule string.
            memo: Optional memo key-value pairs.
            search_attributes: Optional search attributes.

        Returns:
            A handle to the started child workflow.
        """
        from temporalio_trio.worker._interceptor import StartChildWorkflowInput
        from temporalio_trio.workflow import _Definition

        self._assert_not_read_only("start child workflow")

        # Resolve name (match sdk-python)
        name: str
        arg_types: list[type] | None = None
        ret_type = result_type
        if isinstance(workflow, str):
            name = workflow
        elif callable(workflow):
            defn = _Definition.must_from_run_fn(workflow)
            if not defn.name:
                raise TypeError("Cannot invoke dynamic workflow explicitly")
            name = defn.name
            arg_types = defn.arg_types
            ret_type = defn.ret_type
        else:
            raise TypeError("Workflow must be a string or callable")

        return await self.outbound_interceptor.start_child_workflow(
            StartChildWorkflowInput(
                workflow=name,
                args=args,
                id=id,
                task_queue=task_queue,
                cancellation_type=cancellation_type,
                parent_close_policy=parent_close_policy,
                execution_timeout=execution_timeout,
                run_timeout=run_timeout,
                task_timeout=task_timeout,
                id_reuse_policy=id_reuse_policy,
                retry_policy=retry_policy,
                cron_schedule=cron_schedule,
                memo=memo,
                search_attributes=search_attributes,
                headers={},
                arg_types=arg_types,
                ret_type=ret_type,
                versioning_intent=versioning_intent,
                static_summary=static_summary,
                static_details=static_details,
                priority=priority,
            )
        )

    async def _outbound_start_child_workflow(
        self, input: Any
    ) -> "ChildWorkflowHandle":
        """Create the StartChildWorkflowCommand from a StartChildWorkflowInput.

        Called by the terminal outbound interceptor (_WorkflowOutboundImpl).
        This contains the actual command creation logic.
        """
        from temporalio_trio.workflow import ChildWorkflowHandle

        # Get seq for this child workflow
        seq = self.next_child_workflow_seq()

        # Store ret_type for type-aware deserialization at resolution time
        # (matches sdk-python: handle._input.ret_type used in _convert_payloads)
        self.child_workflow_ret_types[seq] = input.ret_type

        # Default task_queue to workflow's task queue if not specified
        actual_task_queue = (
            input.task_queue if input.task_queue is not None else self.task_queue
        )

        # Create context-aware payload converter (matches sdk-python)
        converter = self._payload_converter_with_context(
            temporalio.converter.WorkflowSerializationContext(
                namespace=self.namespace,
                workflow_id=input.id,
            )
        )

        # Pre-encode args
        encoded_args = converter.to_payloads(list(input.args)) if input.args else []

        # Pre-encode memo
        encoded_memo = None
        if input.memo:
            encoded_memo = {
                k: converter.to_payloads([v])[0] for k, v in input.memo.items()
            }

        # Pre-encode static_summary and static_details
        encoded_summary = (
            converter.to_payload(input.static_summary) if input.static_summary else None
        )
        encoded_details = (
            converter.to_payload(input.static_details) if input.static_details else None
        )

        # Create the handle
        handle: ChildWorkflowHandle = ChildWorkflowHandle(
            seq=seq,
            id=input.id,
        )

        # Check if already started (replay path)
        if seq in self.started_children:
            run_id = self.started_children[seq]
            handle._first_execution_run_id = run_id

            # Check if also already completed
            if seq in self.completed_children:
                result = self.completed_children[seq]
                if isinstance(result, BaseException):
                    handle._set_failure(result)
                else:
                    handle._set_result(result)
            return handle

        # Create suspension events (one for start, one for completion)
        start_event = trio.Event()
        self.pending_child_starts[seq] = start_event

        completion_event = trio.Event()
        self.pending_children[seq] = completion_event

        # Emit command
        self.commands.append(
            StartChildWorkflowCommand(
                seq=seq,
                workflow_type=input.workflow,
                workflow_id=input.id,
                args=encoded_args,
                task_queue=actual_task_queue,
                execution_timeout=input.execution_timeout,
                run_timeout=input.run_timeout,
                task_timeout=input.task_timeout,
                parent_close_policy=int(input.parent_close_policy),
                cancellation_type=int(input.cancellation_type),
                id_reuse_policy=int(input.id_reuse_policy),
                retry_policy=input.retry_policy,
                cron_schedule=input.cron_schedule,
                encoded_memo=encoded_memo,
                search_attributes=input.search_attributes,
                headers=input.headers or {},
                versioning_intent=int(input.versioning_intent)
                if input.versioning_intent is not None
                else None,
                static_summary_payload=encoded_summary,
                static_details_payload=encoded_details,
                priority=input.priority,
            )
        )

        # Call suspension callback if set (for single-thread worker)
        if self.on_suspend is not None:
            self.on_suspend()

        # Wait for child to START (not complete)
        try:
            await start_event.wait()
        except trio.Cancelled:
            # If cancelled during start-wait, send cancel command for the child
            # (matching sdk-python's apply_child_cancel_error pattern)
            from temporalio_trio.worker._activation import CancelChildWorkflowCommand

            self.commands.append(CancelChildWorkflowCommand(seq=seq))
            raise

        # Get run_id and clean up start tracking
        run_id = self.started_children[seq]
        del self.pending_child_starts[seq]

        # Set run_id on handle
        handle._first_execution_run_id = run_id

        return handle

    async def workflow_wait_child_workflow(self, seq: int) -> Any:
        """Wait for a child workflow to complete and return the result.

        Args:
            seq: The child workflow sequence number.

        Returns:
            The child workflow result or raises an exception.
        """
        # If already completed (replay), return result immediately
        if seq in self.completed_children:
            result = self.completed_children[seq]
            if isinstance(result, BaseException):
                raise result
            return result

        # Wait for completion event
        if seq not in self.pending_children:
            # This shouldn't happen - the child should be pending
            raise RuntimeError(f"Child workflow seq={seq} is not pending")

        event = self.pending_children[seq]

        # Call suspension callback if set
        if self.on_suspend is not None:
            self.on_suspend()

        # Wait for child to complete
        try:
            await event.wait()
        except trio.Cancelled:
            # If cancelled during result-wait, send cancel command for the child
            # (matching sdk-python's apply_child_cancel_error pattern)
            from temporalio_trio.worker._activation import CancelChildWorkflowCommand

            self.commands.append(CancelChildWorkflowCommand(seq=seq))
            raise

        # Get result and clean up
        result = self.completed_children[seq]
        del self.pending_children[seq]

        # Return result or raise exception
        if isinstance(result, BaseException):
            raise result
        return result

    async def workflow_wait_condition(
        self,
        fn: Callable[[], bool],
        *,
        timeout: float | None = None,
        timeout_summary: str | None = None,
    ) -> None:
        """Wait until condition returns True or timeout expires.

        This method efficiently waits for conditions by registering for
        state change notifications. When signals, activity completions,
        or other events occur, all waiters are notified to re-check their
        conditions.

        Args:
            fn: A callable returning True when condition is met.
            timeout: Optional maximum wait time in seconds.
            timeout_summary: Optional description for Temporal UI.

        Raises:
            TimeoutError: If timeout expires before condition becomes true.
        """
        # Check if condition is already true
        if fn():
            return

        # Set up timeout if specified
        start_time_ns = self.time_ns
        timeout_ns = int(timeout * 1_000_000_000) if timeout else None

        while not fn():
            # Create an event for this wait and register it
            wait_event = trio.Event()
            self.condition_waiters.append(wait_event)

            try:
                # Signal that we're about to wait (so dispatcher knows we're ready)
                if self.on_suspend:
                    self.on_suspend()

                # Wait for the event to be set (by notify_condition_waiters)
                await wait_event.wait()
            finally:
                # Clean up if the event is still in the list
                if wait_event in self.condition_waiters:
                    self.condition_waiters.remove(wait_event)

            # Check timeout
            if timeout_ns is not None:
                elapsed_ns = self.time_ns - start_time_ns
                if elapsed_ns >= timeout_ns:
                    raise TimeoutError(
                        f"Condition not met within timeout of {timeout} seconds"
                    )

    def workflow_continue_as_new(
        self,
        *args: Any,
        workflow: str | type | None,
        task_queue: str | None,
        run_timeout: timedelta | None,
        task_timeout: timedelta | None,
        retry_policy: temporalio.common.RetryPolicy | None,
        memo: Mapping[str, Any] | None = None,
        search_attributes: temporalio.common.SearchAttributes
        | temporalio.common.TypedSearchAttributes
        | None = None,
        versioning_intent: Any = None,
    ) -> NoReturn:
        """Continue the workflow as a new execution.

        This method never returns - it raises ContinueAsNewError to stop the
        workflow and start a new execution.

        Args:
            *args: Arguments to pass to the new workflow execution.
            workflow: Workflow class or type name. None means same workflow type.
            task_queue: Task queue for the new execution. None means same queue.
            run_timeout: Timeout for a single run of the new workflow.
            task_timeout: Timeout for a single workflow task.
            retry_policy: Retry policy for the new workflow.
            memo: Optional memo key-value pairs for the new execution.
            search_attributes: Optional search attributes for the new execution.

        Raises:
            ContinueAsNewError: Always raised to stop the workflow.
        """
        from temporalio_trio.workflow import ContinueAsNewError, _Definition

        # Determine workflow type
        if workflow is None:
            workflow_type = self.workflow_type
        elif isinstance(workflow, str):
            workflow_type = workflow
        else:
            # It's a type - get the workflow definition name
            defn = _Definition.from_class(workflow)
            if defn is not None and defn.name is not None:
                workflow_type = defn.name
            else:
                # Fallback to class name
                workflow_type = getattr(workflow, "__name__", str(workflow))

        # Create internal exception that carries the command data
        class _ContinueAsNewError(ContinueAsNewError):
            """Internal continue-as-new error with command generation."""

            def __init__(self) -> None:
                super().__init__("Continue as new")
                self._workflow_type = workflow_type
                self._args = args
                self._task_queue = task_queue
                self._run_timeout = run_timeout
                self._task_timeout = task_timeout
                self._retry_policy = retry_policy
                self._memo = memo
                self._search_attributes = search_attributes
                self._versioning_intent = versioning_intent

            def _apply_command(self, commands: list) -> None:
                """Add ContinueAsNewCommand to commands list."""
                commands.append(
                    ContinueAsNewCommand(
                        workflow_type=self._workflow_type,
                        args=self._args,
                        task_queue=self._task_queue,
                        run_timeout=self._run_timeout,
                        task_timeout=self._task_timeout,
                        retry_policy=self._retry_policy,
                        memo=self._memo,
                        search_attributes=self._search_attributes,
                        versioning_intent=int(self._versioning_intent)
                        if self._versioning_intent is not None
                        else None,
                    )
                )

        raise _ContinueAsNewError()

    def workflow_get_external_workflow_handle(
        self,
        workflow_id: str,
        *,
        run_id: str | None,
    ) -> "ExternalWorkflowHandle[Any]":
        """Get a handle to an external workflow.

        Args:
            workflow_id: ID of the external workflow.
            run_id: Optional run ID to target a specific run.

        Returns:
            Handle to the external workflow.
        """
        from temporalio_trio.workflow import ExternalWorkflowHandle, _Runtime

        return ExternalWorkflowHandle(cast(_Runtime, self), workflow_id, run_id)

    async def workflow_signal_external_workflow(
        self,
        workflow_id: str,
        signal_name: str,
        args: Sequence[Any],
        *,
        run_id: str | None,
    ) -> None:
        """Signal an external workflow.

        This method sends a signal to an external workflow and waits for
        confirmation that it was delivered.

        Args:
            workflow_id: ID of the external workflow to signal.
            signal_name: Name of the signal to send.
            args: Arguments to pass with the signal.
            run_id: Optional run ID to target a specific run.

        Raises:
            RuntimeError: If the signal fails (e.g., workflow not found).
        """
        # Create context-aware payload converter
        converter = self._payload_converter_with_context(
            temporalio.converter.WorkflowSerializationContext(
                namespace=self.namespace,
                workflow_id=workflow_id,
            )
        )
        encoded_args = converter.to_payloads(list(args)) if args else []

        # Increment sequence number
        self.signal_external_seq += 1
        seq = self.signal_external_seq

        # Check if already completed (replay path)
        if seq in self.completed_external_signals:
            error = self.completed_external_signals[seq]
            if error is not None:
                raise error
            return

        # Create suspension event
        event = trio.Event()
        self.pending_external_signals[seq] = event

        # Emit command
        self.commands.append(
            SignalExternalWorkflowCommand(
                seq=seq,
                workflow_id=workflow_id,
                signal_name=signal_name,
                run_id=run_id,
                args=encoded_args,
            )
        )

        # Notify suspension callback if set
        if self.on_suspend is not None:
            self.on_suspend()

        # Wait for resolution
        await event.wait()

        # Check if there was an error
        if seq in self.completed_external_signals:
            error = self.completed_external_signals[seq]
            if error is not None:
                raise error

    def next_signal_external_seq(self) -> int:
        """Get the next signal external sequence number.

        Returns:
            The next sequence number for signal external commands.
        """
        self.signal_external_seq += 1
        return self.signal_external_seq

    async def workflow_cancel_external_workflow(
        self,
        workflow_id: str,
        *,
        run_id: str | None,
    ) -> None:
        """Cancel an external workflow.

        This method sends a cancellation request to an external workflow and
        waits for confirmation that it was processed.

        Args:
            workflow_id: ID of the external workflow to cancel.
            run_id: Optional run ID to target a specific run.

        Raises:
            RuntimeError: If the cancellation request fails.
        """
        # Increment sequence number
        self.cancel_external_seq += 1
        seq = self.cancel_external_seq

        # Check if already completed (replay path)
        if seq in self.completed_external_cancels:
            error = self.completed_external_cancels[seq]
            if error is not None:
                raise error
            return

        # Create suspension event
        event = trio.Event()
        self.pending_external_cancels[seq] = event

        # Emit command
        self.commands.append(
            RequestCancelExternalWorkflowCommand(
                seq=seq,
                workflow_id=workflow_id,
                run_id=run_id,
            )
        )

        # Notify suspension callback if set
        if self.on_suspend is not None:
            self.on_suspend()

        # Wait for resolution
        await event.wait()

        # Check if there was an error
        if seq in self.completed_external_cancels:
            error = self.completed_external_cancels[seq]
            if error is not None:
                raise error

    def register_signal_handler(self, name: str, handler: Callable[..., Any]) -> None:
        """Register a signal handler.

        Args:
            name: The name of the signal to handle.
            handler: The handler function (can be sync or async).
        """
        self.signal_handlers[name] = handler

    def register_query_handler(self, name: str, handler: Callable[..., Any]) -> None:
        """Register a query handler.

        Query handlers must be synchronous and must not modify workflow state.

        Args:
            name: The name of the query to handle.
            handler: The handler function (must be synchronous).
        """
        self.query_handlers[name] = handler

    def register_update_handler(
        self,
        name: str | None,
        handler: Callable[..., Any],
        validator: Callable[..., None] | None = None,
    ) -> None:
        """Register an update handler.

        Args:
            name: The name of the update to handle (None for dynamic).
            handler: The handler function (can be sync or async).
            validator: Optional validator function.
        """
        self.update_handlers[name] = handler
        if validator is not None:
            self.update_validators[name] = validator

    def workflow_instance(self) -> Any:
        """Get the current workflow instance object.

        Returns:
            The workflow object.
        """
        return self.workflow_object

    def workflow_get_signal_handler(self, name: str | None) -> Callable | None:
        """Get signal handler for the given name (None for dynamic)."""
        return self.signal_handlers.get(name)  # type: ignore[arg-type]

    def workflow_set_signal_handler(
        self, name: str | None, handler: Callable | None
    ) -> None:
        """Set signal handler for the given name (None for dynamic)."""
        if handler is None:
            self.signal_handlers.pop(name, None)  # type: ignore[arg-type]
        else:
            self.signal_handlers[name] = handler  # type: ignore[index]

    def workflow_get_query_handler(self, name: str | None) -> Callable | None:
        """Get query handler for the given name (None for dynamic)."""
        return self.query_handlers.get(name)  # type: ignore[arg-type]

    def workflow_set_query_handler(
        self, name: str | None, handler: Callable | None
    ) -> None:
        """Set query handler for the given name (None for dynamic)."""
        if handler is None:
            self.query_handlers.pop(name, None)  # type: ignore[arg-type]
        else:
            self.query_handlers[name] = handler  # type: ignore[index]

    def workflow_get_update_handler(self, name: str | None) -> Callable | None:
        """Get update handler for the given name (None for dynamic)."""
        return self.update_handlers.get(name)

    def workflow_set_update_handler(
        self,
        name: str | None,
        handler: Callable | None,
        *,
        validator: Callable | None = None,
    ) -> None:
        """Set update handler for the given name (None for dynamic)."""
        if handler is None:
            self.update_handlers.pop(name, None)
            self.update_validators.pop(name, None)
        else:
            self.update_handlers[name] = handler
            if validator is not None:
                self.update_validators[name] = validator

    def workflow_all_handlers_finished(self) -> bool:
        """Whether all update and signal handlers have finished executing.

        Returns:
            True if there are no in-progress update or signal handler executions.
        """
        return len(self.in_progress_updates) == 0

    # Timer methods (Phase 2)

    def next_timer_seq(self) -> int:
        """Increment and return the next timer sequence number.

        Returns:
            The next timer sequence number.
        """
        self.timer_seq += 1
        return self.timer_seq

    async def workflow_sleep(self, duration: float, summary: str | None = None) -> None:
        """Sleep for the specified duration using event-based suspension.

        This implements the event-based sleep pattern:
        1. Get next sequence number
        2. Check if timer already fired (replay path) - return immediately if so
        3. Create trio.Event for suspension
        4. Add StartTimerCommand to commands
        5. Await the event
        6. Clean up pending_timers entry

        Args:
            duration: Duration to sleep in seconds.
            summary: Optional human-readable description for UI/CLI visibility.
        """
        import trio

        seq = self.next_timer_seq()

        # Check if already fired (replay path)
        if seq in self.fired_timers:
            # Update time to fire time
            self.time_ns = self.fired_timers[seq]
            return

        # Create suspension event
        event = trio.Event()
        self.pending_timers[seq] = event

        # Emit command using StartTimerCommand from _activation.py
        # which expects timer_id and duration_ms fields
        self.commands.append(
            StartTimerCommand(
                timer_id=seq,
                duration_ms=int(duration * 1000),
                summary=summary,
            )
        )

        # Call suspension callback if set (for single-thread worker)
        if self.on_suspend is not None:
            self.on_suspend()

        # Suspend until timer fires
        await event.wait()

        # Clean up
        del self.pending_timers[seq]

        # Check for cancellation after waking (Phase 7)
        if self.cancel_requested:
            raise trio.Cancelled._create()

    def apply_timer_fired(self, seq: int, fire_time_ns: int) -> None:
        """Handle a timer fired job from an activation.

        This is called when the activation contains a TimerFired job.
        It records the fire time and wakes up any suspended workflow
        that was waiting for this timer.

        Args:
            seq: The timer sequence number that fired.
            fire_time_ns: The time when the timer fired, in nanoseconds.
        """
        self.fired_timers[seq] = fire_time_ns
        self.time_ns = fire_time_ns

        if seq in self.pending_timers:
            self.pending_timers[seq].set()

    # Activity methods (Phase 4)

    def next_activity_seq(self) -> int:
        """Increment and return the next activity sequence number.

        Returns:
            The next activity sequence number.
        """
        self.activity_seq += 1
        return self.activity_seq

    async def execute_activity(
        self,
        activity: str,
        args: tuple[Any, ...] = (),
        *,
        activity_id: str | None = None,
        task_queue: str | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cancellation_type: int = 0,
    ) -> Any:
        """Execute an activity using event-based suspension.

        This implements the event-based activity execution pattern:
        1. Get next sequence number
        2. Check if already completed (replay path) - return/raise immediately if so
        3. Create trio.Event for suspension
        4. Add ScheduleActivityCommand to commands
        5. Call on_suspend callback if set
        6. Await the event
        7. Return result or raise exception

        Args:
            activity: Name of the activity type to execute.
            args: Arguments to pass to the activity.
            activity_id: Optional user-provided activity ID.
            task_queue: Task queue to run activity on (defaults to workflow's).
            schedule_to_close_timeout: Max total time for activity.
            schedule_to_start_timeout: Max time to wait for worker to pick up.
            start_to_close_timeout: Max time for activity execution.
            heartbeat_timeout: Max time between heartbeats.
            retry_policy: Retry policy for the activity.

        Returns:
            The activity result.

        Raises:
            Exception: If the activity failed.
        """
        import trio

        seq = self.next_activity_seq()

        # Check if already completed (replay path)
        if seq in self.completed_activities:
            result = self.completed_activities[seq]
            if isinstance(result, BaseException):
                raise result
            return result

        # Create suspension event
        event = trio.Event()
        self.pending_activities[seq] = event

        # Generate activity_id if not provided
        actual_activity_id = activity_id if activity_id else str(seq)

        # Emit command using _activation.ScheduleActivityCommand
        # which uses timedelta directly (not milliseconds)
        self.commands.append(
            ScheduleActivityCommand(
                seq=seq,
                activity_id=actual_activity_id,
                activity_type=activity,
                args=args,
                task_queue=task_queue,
                schedule_to_close_timeout=schedule_to_close_timeout,
                schedule_to_start_timeout=schedule_to_start_timeout,
                start_to_close_timeout=start_to_close_timeout,
                heartbeat_timeout=heartbeat_timeout,
                retry_policy=retry_policy,
                cancellation_type=cancellation_type,
            )
        )

        # Call suspension callback if set (for single-thread worker)
        if self.on_suspend is not None:
            self.on_suspend()

        # Suspend until activity completes
        await event.wait()

        # Get result and clean up
        result = self.completed_activities[seq]
        del self.pending_activities[seq]

        # Check for cancellation after waking (Phase 7)
        if self.cancel_requested:
            raise trio.Cancelled._create()

        if isinstance(result, BaseException):
            raise result
        return result

    async def execute_local_activity(
        self,
        activity: str,
        args: tuple[Any, ...] = (),
        *,
        activity_id: str | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        local_retry_threshold: timedelta | None = None,
        cancellation_type: int = 0,
    ) -> Any:
        """Execute a local activity using event-based suspension.

        This implements the event-based local activity execution pattern,
        identical to execute_activity but emitting a ScheduleLocalActivityCommand
        instead. Local activities run on the same task queue and don't support
        heartbeats.

        Args:
            activity: Name of the activity type to execute.
            args: Arguments to pass to the activity.
            activity_id: Optional user-provided activity ID.
            schedule_to_close_timeout: Max total time for activity.
            schedule_to_start_timeout: Max time to wait for worker to pick up.
            start_to_close_timeout: Max time for activity execution.
            retry_policy: Retry policy for the activity.
            local_retry_threshold: Duration after which retries use the server.

        Returns:
            The activity result.

        Raises:
            Exception: If the activity failed.
        """
        import trio

        seq = self.next_activity_seq()

        # Check if already completed (replay path)
        if seq in self.completed_activities:
            result = self.completed_activities[seq]
            if isinstance(result, BaseException):
                raise result
            return result

        # Create suspension event
        event = trio.Event()
        self.pending_activities[seq] = event

        # Generate activity_id if not provided
        actual_activity_id = activity_id if activity_id else str(seq)

        # Emit command using _activation.ScheduleLocalActivityCommand
        self.commands.append(
            ScheduleLocalActivityCommand(
                seq=seq,
                activity_id=actual_activity_id,
                activity_type=activity,
                args=args,
                schedule_to_close_timeout=schedule_to_close_timeout,
                schedule_to_start_timeout=schedule_to_start_timeout,
                start_to_close_timeout=start_to_close_timeout,
                retry_policy=retry_policy,
                local_retry_threshold=local_retry_threshold,
                cancellation_type=cancellation_type,
            )
        )

        # Call suspension callback if set (for single-thread worker)
        if self.on_suspend is not None:
            self.on_suspend()

        # Suspend until activity completes
        await event.wait()

        # Get result and clean up
        result = self.completed_activities[seq]
        del self.pending_activities[seq]

        # Check for cancellation after waking
        if self.cancel_requested:
            raise trio.Cancelled._create()

        if isinstance(result, BaseException):
            raise result
        return result

    def workflow_start_activity(
        self,
        activity: str | Callable[..., Any],
        *args: Any,
        task_queue: str | None = None,
        result_type: type | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        activity_id: str | None = None,
        cancellation_type: "ActivityCancellationType" = ActivityCancellationType.TRY_CANCEL,
        versioning_intent: "VersioningIntent | None" = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> "ActivityHandle[Any]":
        """Start an activity and return an ActivityHandle without waiting.

        Resolves the activity definition, creates a StartActivityInput, and
        dispatches through the outbound interceptor chain (matching sdk-python).
        """
        self._assert_not_read_only("start activity")
        from temporalio_trio.worker._interceptor import StartActivityInput

        # Get activity definition if it's callable
        name: str
        arg_types: list[type] | None = None
        ret_type = result_type
        if isinstance(activity, str):
            name = activity
        elif callable(activity):
            defn = temporalio.activity._Definition.must_from_callable(activity)
            if not defn.name:
                raise ValueError("Cannot invoke dynamic activity explicitly")
            name = defn.name
            arg_types = defn.arg_types
            ret_type = defn.ret_type
        else:
            raise TypeError("Activity must be a string or callable")

        return self.outbound_interceptor.start_activity(
            StartActivityInput(
                activity=name,
                args=args,
                activity_id=activity_id,
                task_queue=task_queue,
                schedule_to_close_timeout=schedule_to_close_timeout,
                schedule_to_start_timeout=schedule_to_start_timeout,
                start_to_close_timeout=start_to_close_timeout,
                heartbeat_timeout=heartbeat_timeout,
                retry_policy=retry_policy,
                cancellation_type=cancellation_type,
                headers={},
                disable_eager_execution=self.disable_eager_activity_execution,
                versioning_intent=versioning_intent,
                summary=summary,
                priority=priority,
                arg_types=arg_types,
                ret_type=ret_type,
            )
        )

    def _outbound_schedule_activity(
        self,
        input: Any,
    ) -> "ActivityHandle[Any]":
        """Create the ScheduleActivityCommand from a StartActivityInput.

        Called by the terminal outbound interceptor (_WorkflowOutboundImpl).
        """
        from temporalio_trio.workflow import ActivityHandle

        # Validate timeouts
        if not input.start_to_close_timeout and not input.schedule_to_close_timeout:
            raise ValueError(
                "Activity must have start_to_close_timeout or schedule_to_close_timeout"
            )

        # Default task_queue to workflow's task queue if not specified
        actual_task_queue = (
            input.task_queue if input.task_queue is not None else self.task_queue
        )

        # Create context-aware payload converter (matches sdk-python)
        activity_id = input.activity_id or None
        converter = self._payload_converter_with_context(
            temporalio.converter.ActivitySerializationContext(
                namespace=self.namespace,
                workflow_id=self.workflow_id,
                workflow_type=self.workflow_type,
                activity_type=input.activity,
                activity_id=activity_id,
                activity_task_queue=actual_task_queue,
                is_local=False,
            )
        )

        # Encode arguments before creating command (matches sdk-python)
        encoded_args = converter.to_payloads(list(input.args)) if input.args else []

        # Encode summary
        encoded_summary = (
            converter.to_payload(input.summary) if input.summary else None
        )

        seq = self.next_activity_seq()

        # Check if already completed (replay path)
        if seq not in self.completed_activities:
            # Create suspension event
            event = trio.Event()
            self.pending_activities[seq] = event

        # Generate activity_id if not provided
        actual_activity_id = input.activity_id if input.activity_id else str(seq)

        # Emit command
        self.commands.append(
            ScheduleActivityCommand(
                seq=seq,
                activity_id=actual_activity_id,
                activity_type=input.activity,
                args=encoded_args,
                task_queue=actual_task_queue,
                schedule_to_close_timeout=input.schedule_to_close_timeout,
                schedule_to_start_timeout=input.schedule_to_start_timeout,
                start_to_close_timeout=input.start_to_close_timeout,
                heartbeat_timeout=input.heartbeat_timeout,
                retry_policy=input.retry_policy,
                cancellation_type=int(input.cancellation_type),
                headers=input.headers or {},
                do_not_eagerly_execute=input.disable_eager_execution,
                versioning_intent=int(input.versioning_intent)
                if input.versioning_intent is not None
                else None,
                summary_payload=encoded_summary,
                priority=input.priority,
            )
        )

        return ActivityHandle(seq=seq, is_local=False)

    def workflow_start_local_activity(
        self,
        activity: str | Callable[..., Any],
        *args: Any,
        result_type: type | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        local_retry_threshold: timedelta | None = None,
        activity_id: str | None = None,
        cancellation_type: "ActivityCancellationType" = ActivityCancellationType.TRY_CANCEL,
        summary: str | None = None,
    ) -> "ActivityHandle[Any]":
        """Start a local activity and return an ActivityHandle without waiting.

        Resolves the activity definition, creates a StartLocalActivityInput, and
        dispatches through the outbound interceptor chain (matching sdk-python).
        """
        self._assert_not_read_only("start local activity")
        from temporalio_trio.worker._interceptor import StartLocalActivityInput

        # Get activity definition if it's callable
        name: str
        arg_types: list[type] | None = None
        ret_type = result_type
        if isinstance(activity, str):
            name = activity
        elif callable(activity):
            defn = temporalio.activity._Definition.must_from_callable(activity)
            if not defn.name:
                raise ValueError("Cannot invoke dynamic activity explicitly")
            name = defn.name
            arg_types = defn.arg_types
            ret_type = defn.ret_type
        else:
            raise TypeError("Activity must be a string or callable")

        return self.outbound_interceptor.start_local_activity(
            StartLocalActivityInput(
                activity=name,
                args=args,
                activity_id=activity_id,
                schedule_to_close_timeout=schedule_to_close_timeout,
                schedule_to_start_timeout=schedule_to_start_timeout,
                start_to_close_timeout=start_to_close_timeout,
                retry_policy=retry_policy,
                local_retry_threshold=local_retry_threshold,
                cancellation_type=cancellation_type,
                headers={},
                summary=summary,
                arg_types=arg_types,
                ret_type=ret_type,
            )
        )

    def _outbound_schedule_local_activity(
        self,
        input: Any,
    ) -> "ActivityHandle[Any]":
        """Create the ScheduleLocalActivityCommand from a StartLocalActivityInput.

        Called by the terminal outbound interceptor (_WorkflowOutboundImpl).
        """
        from temporalio_trio.workflow import ActivityHandle

        # Validate timeouts
        if not input.start_to_close_timeout and not input.schedule_to_close_timeout:
            raise ValueError(
                "Activity must have start_to_close_timeout or schedule_to_close_timeout"
            )

        # Create context-aware payload converter (matches sdk-python)
        activity_id = input.activity_id or None
        converter = self._payload_converter_with_context(
            temporalio.converter.ActivitySerializationContext(
                namespace=self.namespace,
                workflow_id=self.workflow_id,
                workflow_type=self.workflow_type,
                activity_type=input.activity,
                activity_id=activity_id,
                activity_task_queue=self.task_queue,
                is_local=True,
            )
        )

        # Encode arguments before creating command (matches sdk-python)
        encoded_args = converter.to_payloads(list(input.args)) if input.args else []

        # Encode summary
        encoded_summary = (
            converter.to_payload(input.summary) if input.summary else None
        )

        seq = self.next_activity_seq()

        # Check if already completed (replay path)
        if seq not in self.completed_activities:
            # Create suspension event
            event = trio.Event()
            self.pending_activities[seq] = event

        # Generate activity_id if not provided
        actual_activity_id = input.activity_id if input.activity_id else str(seq)

        # Emit command
        self.commands.append(
            ScheduleLocalActivityCommand(
                seq=seq,
                activity_id=actual_activity_id,
                activity_type=input.activity,
                args=encoded_args,
                schedule_to_close_timeout=input.schedule_to_close_timeout,
                schedule_to_start_timeout=input.schedule_to_start_timeout,
                start_to_close_timeout=input.start_to_close_timeout,
                retry_policy=input.retry_policy,
                local_retry_threshold=input.local_retry_threshold,
                cancellation_type=int(input.cancellation_type),
                headers=input.headers or {},
                summary_payload=encoded_summary,
            )
        )

        return ActivityHandle(seq=seq, is_local=True)

    async def workflow_wait_activity(self, seq: int) -> Any:
        """Wait for an activity to complete by sequence number.

        This is used by ActivityHandle to wait for its result.
        Handles local activity backoff by sleeping and rescheduling.

        Args:
            seq: The activity sequence number to wait for.

        Returns:
            The activity result.

        Raises:
            Exception: If the activity failed.
        """
        import trio

        while True:
            # Check if already completed (replay path or fast completion)
            if seq in self.completed_activities:
                result = self.completed_activities[seq]
                if isinstance(result, _ActivityBackoff):
                    # Local activity needs backoff retry
                    seq = await self._handle_activity_backoff(seq, result)
                    continue
                if isinstance(result, BaseException):
                    raise result
                return result

            # Wait for the event
            event = self.pending_activities.get(seq)
            if event is None:
                # Event not yet created - shouldn't happen but handle gracefully
                event = trio.Event()
                self.pending_activities[seq] = event

            # Call suspension callback if set (for single-thread worker)
            if self.on_suspend is not None:
                self.on_suspend()

            # Suspend until activity completes
            await event.wait()

            # Get result and clean up
            result = self.completed_activities[seq]
            if seq in self.pending_activities:
                del self.pending_activities[seq]

            # Check for cancellation after waking
            if self.cancel_requested:
                raise trio.Cancelled._create()

            # Handle backoff (local activity retry)
            if isinstance(result, _ActivityBackoff):
                seq = await self._handle_activity_backoff(seq, result)
                continue

            if isinstance(result, BaseException):
                raise result
            return result

    async def _handle_activity_backoff(
        self,
        old_seq: int,
        backoff_info: _ActivityBackoff,
    ) -> int:
        """Handle a local activity backoff by sleeping and rescheduling.

        Args:
            old_seq: The original activity sequence number.
            backoff_info: The backoff sentinel with DoBackoff proto.

        Returns:
            The new activity sequence number for the rescheduled activity.
        """
        backoff = backoff_info.backoff

        # Sleep for the backoff duration (deterministic timer)
        backoff_seconds = (
            backoff.backoff_duration.seconds + backoff.backoff_duration.nanos / 1e9
        )
        if backoff_seconds > 0:
            await self.workflow_sleep(backoff_seconds)

        # Get the original ScheduleLocalActivityCommand to reschedule
        # We need to find the original input to reuse its parameters
        original_cmd = None
        for cmd in self.commands:
            if isinstance(cmd, ScheduleLocalActivityCommand) and cmd.seq == old_seq:
                original_cmd = cmd
                break

        # Allocate new sequence number
        new_seq = self.next_activity_seq()

        if original_cmd is not None:
            # Reschedule with backoff info
            self.commands.append(
                ScheduleLocalActivityCommand(
                    seq=new_seq,
                    activity_id=original_cmd.activity_id,
                    activity_type=original_cmd.activity_type,
                    args=original_cmd.args,
                    schedule_to_close_timeout=original_cmd.schedule_to_close_timeout,
                    schedule_to_start_timeout=original_cmd.schedule_to_start_timeout,
                    start_to_close_timeout=original_cmd.start_to_close_timeout,
                    retry_policy=original_cmd.retry_policy,
                    local_retry_threshold=original_cmd.local_retry_threshold,
                    cancellation_type=original_cmd.cancellation_type,
                    headers=original_cmd.headers,
                    attempt=backoff.attempt,
                    original_schedule_time=backoff.original_schedule_time,
                )
            )

        # Create pending event for the new seq
        event = trio.Event()
        self.pending_activities[new_seq] = event

        # Call suspension callback
        if self.on_suspend is not None:
            self.on_suspend()

        return new_seq

    def _workflow_runtime(self) -> "WorkflowRuntime":
        """Get the underlying WorkflowRuntime (self)."""
        return self

    def apply_activity_resolved(
        self,
        seq: int,
        result: Any = None,
        error: BaseException | None = None,
        backoff: Any | None = None,
    ) -> None:
        """Handle an activity resolution job from an activation.

        This is called when the activation contains an ActivityResolved job.
        It stores the result (or error or backoff) and wakes up any suspended
        workflow that was waiting for this activity.

        Args:
            seq: The activity sequence number that completed.
            result: The activity result (if successful).
            error: The exception (if the activity failed).
            backoff: DoBackoff proto for local activity retry (if backoff).
        """
        # Store result, error, or backoff sentinel
        if backoff is not None:
            self.completed_activities[seq] = _ActivityBackoff(backoff=backoff)
        elif error is not None:
            self.completed_activities[seq] = error
        else:
            self.completed_activities[seq] = result

        # Wake up the suspended workflow if waiting
        if seq in self.pending_activities:
            self.pending_activities[seq].set()

    # Child workflow methods (Phase 6)

    def next_child_workflow_seq(self) -> int:
        """Increment and return the next child workflow sequence number.

        Returns:
            The next child workflow sequence number.
        """
        self.child_workflow_seq += 1
        return self.child_workflow_seq

    async def execute_child_workflow(
        self,
        workflow: str,
        workflow_id: str,
        args: tuple[Any, ...] = (),
        *,
        task_queue: str | None = None,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
    ) -> Any:
        """Execute a child workflow using event-based suspension.

        This implements the event-based child workflow execution pattern:
        1. Get next sequence number
        2. Check if already completed (replay path) - return/raise immediately if so
        3. Create trio.Event for suspension
        4. Add StartChildWorkflowCommand to commands
        5. Call on_suspend callback if set
        6. Await the event
        7. Return result or raise exception

        Args:
            workflow: Name of the workflow type to execute.
            workflow_id: ID for the child workflow.
            args: Arguments to pass to the child workflow.
            task_queue: Task queue to run child on (defaults to parent's).
            execution_timeout: Total timeout for child workflow including retries.
            run_timeout: Timeout for a single run of the child workflow.
            task_timeout: Timeout for a single workflow task of the child.

        Returns:
            The child workflow result.

        Raises:
            Exception: If the child workflow failed.
        """
        import trio

        seq = self.next_child_workflow_seq()

        # Check if already completed (replay path)
        if seq in self.completed_children:
            result = self.completed_children[seq]
            if isinstance(result, BaseException):
                raise result
            return result

        # Create suspension event
        event = trio.Event()
        self.pending_children[seq] = event

        # Default task_queue to workflow's task queue if not specified
        actual_task_queue = task_queue if task_queue is not None else self.task_queue

        # Pre-encode args using the payload converter
        encoded_args = self.payload_converter.to_payloads(list(args)) if args else []

        # Emit command
        self.commands.append(
            StartChildWorkflowCommand(
                seq=seq,
                workflow_type=workflow,
                workflow_id=workflow_id,
                args=encoded_args,
                task_queue=actual_task_queue,
                execution_timeout=execution_timeout,
                run_timeout=run_timeout,
                task_timeout=task_timeout,
            )
        )

        # Call suspension callback if set (for single-thread worker)
        if self.on_suspend is not None:
            self.on_suspend()

        # Suspend until child workflow completes
        try:
            await event.wait()
        except trio.Cancelled:
            # If cancelled during child-wait, send cancel command for the child
            # (matching sdk-python's apply_child_cancel_error pattern)
            from temporalio_trio.worker._activation import CancelChildWorkflowCommand

            self.commands.append(CancelChildWorkflowCommand(seq=seq))
            raise

        # Get result and clean up
        result = self.completed_children[seq]
        del self.pending_children[seq]

        # Check for cancellation after waking (Phase 7)
        if self.cancel_requested:
            raise trio.Cancelled._create()

        if isinstance(result, BaseException):
            raise result
        return result

    def apply_child_workflow_resolved(
        self,
        seq: int,
        result: Any = None,
        error: BaseException | None = None,
    ) -> None:
        """Handle a child workflow resolution job from an activation.

        This is called when the activation contains a ChildWorkflowResolved job.
        The caller is responsible for decoding the result payload with proper
        ret_type hints before calling this method (matching sdk-python's pattern
        where _apply_resolve_child_workflow_execution decodes before resolving).

        Args:
            seq: The child workflow sequence number that completed.
            result: The decoded child workflow result (if successful).
            error: The exception (if the child workflow failed).
        """
        # Store result or error
        if error is not None:
            self.completed_children[seq] = error
        else:
            self.completed_children[seq] = result

        # Wake up the suspended workflow if waiting
        if seq in self.pending_children:
            self.pending_children[seq].set()

    def apply_child_workflow_started(self, seq: int, run_id: str) -> None:
        """Handle a child workflow started job from an activation.

        This is called when the activation contains a ChildWorkflowStartedJob.
        The child has been accepted and started by the server.

        Args:
            seq: The child workflow sequence number that started.
            run_id: The run ID of the started child workflow.
        """
        import logging

        logger = logging.getLogger(__name__)
        logger.debug(f"Child workflow seq={seq} started with run_id={run_id}")

        # Store the run_id
        self.started_children[seq] = run_id

        # Wake up any workflow waiting for the child to start
        if seq in self.pending_child_starts:
            self.pending_child_starts[seq].set()

    def apply_child_workflow_start_failed(
        self, seq: int, workflow_id: str, workflow_type: str, cause: int
    ) -> None:
        """Handle a child workflow start failed job from an activation.

        This is called when the activation contains a ChildWorkflowStartFailedJob.
        The child workflow could not be started (e.g., workflow ID conflict).

        Matches sdk-python's _apply_resolve_child_workflow_execution_start:
        - WORKFLOW_ALREADY_EXISTS cause -> WorkflowAlreadyStartedError
        - Other causes -> RuntimeError

        Args:
            seq: The child workflow sequence number that failed to start.
            workflow_id: The requested workflow ID.
            workflow_type: The requested workflow type.
            cause: The raw cause value (StartChildWorkflowExecutionFailedCause enum).
        """
        import temporalio.bridge.proto.child_workflow

        # Match sdk-python: check specific cause for proper exception type
        if (
            cause
            == int(
                temporalio.bridge.proto.child_workflow.StartChildWorkflowExecutionFailedCause.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS
            )
        ):
            error: BaseException = (
                temporalio.exceptions.WorkflowAlreadyStartedError(
                    workflow_id, workflow_type
                )
            )
        else:
            error = RuntimeError(
                f"Unknown child start fail cause: {cause}"
            )

        self.completed_children[seq] = error

        # Wake up the suspended workflow if waiting for start or completion
        if seq in self.pending_child_starts:
            self.pending_child_starts[seq].set()
        if seq in self.pending_children:
            self.pending_children[seq].set()

    # Cancellation methods (Phase 7)

    def apply_cancel_workflow(self) -> None:
        """Handle a workflow cancellation request.

        This is called when the activation contains a CancelWorkflow job.
        It sets the cancel_requested flag and cancels the workflow's nursery
        to propagate cancellation to all child tasks.
        """
        self.cancel_requested = True
        # Cancel the nursery to propagate to all child tasks
        if self.nursery is not None:
            self.nursery.cancel_scope.cancel()

    # Patching methods (Phase 2 of feature parity)

    def apply_notify_has_patch(self, patch_id: str) -> None:
        """Handle a notify_has_patch job from an activation.

        This is called when the activation contains a NotifyHasPatch job,
        which happens during replay to inform the runtime that a patch ID
        exists in the workflow history.

        Args:
            patch_id: The patch ID that exists in history.
        """
        self.patches_notified.add(patch_id)

    def workflow_patch(self, patch_id: str, *, deprecated: bool = False) -> bool:
        """Check if a patch should be applied.

        This implements the patching logic for safe code evolution:
        - If replaying and the patch is in history (notified), return True
        - If replaying and the patch is NOT in history, return False
        - If not replaying (new execution), emit a SetPatchMarkerCommand and return True

        Results are memoized so subsequent calls with the same patch_id return
        the same value without emitting additional commands.

        Args:
            patch_id: Unique identifier for this patch point.
            deprecated: If True, marks the patch as deprecated. Deprecated patches
                still return True for existing executions but the marker indicates
                the old code path can be removed in the future.

        Returns:
            True if the new code path should be taken, False if the old path
            should be taken (only during replay without the patch marker).
        """
        # Check if already memoized
        if patch_id in self.patches_memoized:
            return self.patches_memoized[patch_id]

        # Determine the result based on replay state
        if self.is_replaying:
            # During replay, check if patch was recorded in history
            result = patch_id in self.patches_notified
        else:
            # New execution - take the new path and record marker
            result = True
            self.commands.append(
                SetPatchMarkerCommand(patch_id=patch_id, deprecated=deprecated)
            )

        # Memoize and return
        self.patches_memoized[patch_id] = result
        return result

    def workflow_upsert_search_attributes(
        self,
        attributes: Sequence[temporalio.common.SearchAttributeUpdate],
    ) -> None:
        """Upsert search attributes for this workflow.

        Emits an UpsertSearchAttributesCommand with the typed updates.

        Args:
            attributes: Sequence of typed search attribute updates.
        """
        self.commands.append(
            UpsertSearchAttributesCommand(search_attributes=attributes)
        )

    # External signal methods

    def apply_signal_external_resolved(
        self, seq: int, error: BaseException | None
    ) -> None:
        """Handle a signal external workflow resolution.

        This is called when the activation contains a SignalExternalResolvedJob.
        It stores the result and wakes up any suspended workflow.

        Args:
            seq: The sequence number of the signal external command.
            error: The error if the signal failed, or None if successful.
        """
        # Store result for replay
        self.completed_external_signals[seq] = error

        # Wake up the suspended workflow if waiting
        if seq in self.pending_external_signals:
            self.pending_external_signals[seq].set()

    # External cancel methods

    def apply_cancel_external_resolved(
        self, seq: int, error: BaseException | None
    ) -> None:
        """Handle a cancel external workflow resolution.

        This is called when the activation contains a CancelExternalResolvedJob.
        It stores the result and wakes up any suspended workflow.

        Args:
            seq: The sequence number of the cancel external command.
            error: The error if the cancel failed, or None if successful.
        """
        # Store result for replay
        self.completed_external_cancels[seq] = error

        # Wake up the suspended workflow if waiting
        if seq in self.pending_external_cancels:
            self.pending_external_cancels[seq].set()


# Module-level ContextVar for current runtime
_current_runtime: ContextVar[WorkflowRuntime | None] = ContextVar(
    "workflow_runtime", default=None
)


def get_current_runtime() -> WorkflowRuntime:
    """Get the current workflow runtime.

    Returns:
        The current WorkflowRuntime instance.

    Raises:
        NotInWorkflowRuntimeError: If not in a workflow context.
    """
    runtime = _current_runtime.get()
    if runtime is None:
        raise NotInWorkflowRuntimeError(
            "Not in workflow runtime context. "
            "This function must be called from within a workflow."
        )
    return runtime


def maybe_get_current_runtime() -> WorkflowRuntime | None:
    """Get the current workflow runtime or None if not in workflow context.

    Returns:
        The current WorkflowRuntime instance, or None.
    """
    return _current_runtime.get()


def set_current_runtime(runtime: WorkflowRuntime) -> Token[WorkflowRuntime | None]:
    """Set the current workflow runtime.

    Args:
        runtime: The WorkflowRuntime instance to set as current.

    Returns:
        Token that can be used to reset the runtime.
    """
    return _current_runtime.set(runtime)


def reset_current_runtime(token: Token[WorkflowRuntime | None]) -> None:
    """Reset the current workflow runtime using a token.

    Args:
        token: Token from a previous set_current_runtime call.
    """
    _current_runtime.reset(token)
