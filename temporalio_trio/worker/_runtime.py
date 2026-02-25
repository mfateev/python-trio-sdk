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
from typing import TYPE_CHECKING, Any, Callable, NoReturn

import temporalio.api.common.v1
import temporalio.common
import trio

from temporalio_trio.worker._activation import (
    CancelWorkflowCommand,
    ContinueAsNewCommand,
    RequestCancelExternalWorkflowCommand,
    ScheduleActivityCommand,
    ScheduleLocalActivityCommand,
    SignalExternalWorkflowCommand,
    StartChildWorkflowCommand,
    StartTimerCommand,
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

    def notify_condition_waiters(self) -> None:
        """Notify all wait_condition waiters that state may have changed.

        This should be called after any event that might affect condition
        predicates, such as signal delivery.
        """
        for event in self.condition_waiters:
            event.set()
        self.condition_waiters.clear()

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
            workflow_id=self.workflow_id,
            workflow_type=self.workflow_type,
            run_id=self.run_id,
            task_queue=self.task_queue,
            namespace=self.namespace,
            attempt=self.attempt,
            start_time=datetime.fromtimestamp(
                self.start_time_ns / 1e9, tz=timezone.utc
            ),
            headers=self.headers,
            execution_timeout=timedelta(milliseconds=self.execution_timeout_ms)
            if self.execution_timeout_ms is not None
            else None,
            run_timeout=timedelta(milliseconds=self.run_timeout_ms)
            if self.run_timeout_ms is not None
            else None,
            task_timeout=timedelta(milliseconds=self.task_timeout_ms)
            if self.task_timeout_ms is not None
            else None,
            retry_policy=self.retry_policy_obj,
            continued_run_id=self.continued_run_id,
            cron_schedule=self.cron_schedule,
            parent=parent,
            root=root,
            raw_memo=self.raw_memo,
            priority=self.priority,
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
        )

    async def workflow_start_child_workflow(
        self,
        workflow: str | type,
        *args: Any,
        id: str,
        task_queue: str | None = None,
        cancellation_type: Any = None,
        parent_close_policy: Any = None,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: Any = None,
        retry_policy: Any = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: temporalio.common.SearchAttributes | None = None,
    ) -> "ChildWorkflowHandle":
        """Start a child workflow and return a handle.

        This is a wrapper that matches the _Runtime interface.

        Args:
            workflow: Workflow class or type name.
            *args: Arguments to pass to the workflow.
            id: Unique workflow ID.
            task_queue: Task queue (defaults to parent's).
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
        from temporalio_trio.workflow import ChildWorkflowHandle, _Definition

        # Extract workflow type name
        if isinstance(workflow, str):
            workflow_type = workflow
        elif isinstance(workflow, type):
            # It's a class
            defn = _Definition.from_class(workflow)
            workflow_type = defn.name if defn else workflow.__name__
        elif hasattr(workflow, "__temporal_workflow_run"):
            # It's a method decorated with @workflow.run
            # Extract class name from __qualname__ (e.g., "MyWorkflow.run" -> "MyWorkflow")
            qualname = getattr(workflow, "__qualname__", "")
            if "." in qualname:
                workflow_type = qualname.rsplit(".", 1)[0]
            else:
                workflow_type = getattr(workflow, "__name__", str(workflow))
        else:
            # Fallback - try to get the name
            workflow_type = getattr(workflow, "__name__", str(workflow))

        # Get seq for this child workflow
        seq = self.next_child_workflow_seq()

        # Create the handle
        handle: ChildWorkflowHandle = ChildWorkflowHandle(
            seq=seq,
            id=id,
            workflow_type=workflow_type,
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

        # Default task_queue to workflow's task queue if not specified
        actual_task_queue = task_queue if task_queue is not None else self.task_queue

        # Emit command
        self.commands.append(
            StartChildWorkflowCommand(
                seq=seq,
                workflow_type=workflow_type,
                workflow_id=id,
                args=args,
                task_queue=actual_task_queue,
                execution_timeout=execution_timeout,
                run_timeout=run_timeout,
                task_timeout=task_timeout,
                cron_schedule=cron_schedule,
                memo=memo,
                search_attributes=search_attributes,
            )
        )

        # Call suspension callback if set (for single-thread worker)
        if self.on_suspend is not None:
            self.on_suspend()

        # Wait for child to START (not complete)
        await start_event.wait()

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
        await event.wait()

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
        search_attributes: temporalio.common.SearchAttributes | None = None,
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
            if defn is not None:
                workflow_type = defn.name
            else:
                # Fallback to class name
                workflow_type = getattr(workflow, "__name__", str(workflow))

        # Create internal exception that carries the command data
        class _ContinueAsNewError(ContinueAsNewError):
            """Internal continue-as-new error with command generation."""

            def __init__(inner_self) -> None:
                super().__init__("Continue as new")
                inner_self._workflow_type = workflow_type
                inner_self._args = args
                inner_self._task_queue = task_queue
                inner_self._run_timeout = run_timeout
                inner_self._task_timeout = task_timeout
                inner_self._retry_policy = retry_policy
                inner_self._memo = memo
                inner_self._search_attributes = search_attributes

            def _apply_command(inner_self, commands: list) -> None:
                """Add ContinueAsNewCommand to commands list."""
                commands.append(
                    ContinueAsNewCommand(
                        workflow_type=inner_self._workflow_type,
                        args=inner_self._args,
                        task_queue=inner_self._task_queue,
                        run_timeout=inner_self._run_timeout,
                        task_timeout=inner_self._task_timeout,
                        retry_policy=inner_self._retry_policy,
                        memo=inner_self._memo,
                        search_attributes=inner_self._search_attributes,
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
        from temporalio_trio.workflow import ExternalWorkflowHandle

        return ExternalWorkflowHandle(self, workflow_id, run_id)

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
                args=tuple(args),
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

    def apply_activity_resolved(
        self,
        seq: int,
        result: Any = None,
        error: BaseException | None = None,
    ) -> None:
        """Handle an activity resolution job from an activation.

        This is called when the activation contains an ActivityResolved job.
        It stores the result (or error) and wakes up any suspended workflow
        that was waiting for this activity.

        Args:
            seq: The activity sequence number that completed.
            result: The activity result (if successful).
            error: The exception (if the activity failed).
        """
        # Store result or error
        if error is not None:
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

        # Emit command
        self.commands.append(
            StartChildWorkflowCommand(
                seq=seq,
                workflow_type=workflow,
                workflow_id=workflow_id,
                args=args,
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
        await event.wait()

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
        It stores the result (or error) and wakes up any suspended workflow
        that was waiting for this child workflow.

        Args:
            seq: The child workflow sequence number that completed.
            result: The child workflow result (if successful).
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
        self, seq: int, workflow_id: str, workflow_type: str, cause: str
    ) -> None:
        """Handle a child workflow start failed job from an activation.

        This is called when the activation contains a ChildWorkflowStartFailedJob.
        The child workflow could not be started (e.g., workflow ID conflict).
        We treat this as an immediate failure and wake up the waiting parent.

        Args:
            seq: The child workflow sequence number that failed to start.
            workflow_id: The requested workflow ID.
            workflow_type: The requested workflow type.
            cause: The reason the child workflow failed to start.
        """
        from temporalio.exceptions import ChildWorkflowError

        # Create an error to wake the parent with
        error = ChildWorkflowError(
            f"Child workflow {workflow_type} ({workflow_id}) failed to start: {cause}"
        )
        self.completed_children[seq] = error

        # Wake up the suspended workflow if waiting
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
