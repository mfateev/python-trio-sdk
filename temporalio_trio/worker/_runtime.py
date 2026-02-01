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
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable

import trio

from temporalio_trio.worker._activation import (
    CancelWorkflowCommand,
    StartChildWorkflowCommand,
    StartTimerCommand,
)

__all__ = [
    "WorkflowRuntime",
    "StartTimerCommand",
    "ScheduleActivityCommand",
    "StartChildWorkflowCommand",
    "QuerySuccessCommand",
    "QueryFailureCommand",
    "CancelWorkflowCommand",
    "get_current_runtime",
    "set_current_runtime",
    "reset_current_runtime",
    "NotInWorkflowRuntimeError",
]


class NotInWorkflowRuntimeError(RuntimeError):
    """Raised when workflow runtime is accessed outside of workflow context."""

    pass


@dataclass
class ScheduleActivityCommand:
    """Command to schedule an activity for execution.

    This command is emitted when execute_activity() is called and schedules
    an activity to be executed. When the activity completes, a resolution
    job will be delivered to the workflow.
    """

    seq: int
    """Sequence number identifying this activity."""

    activity_type: str
    """Name of the activity type to execute."""

    arguments: tuple[Any, ...]
    """Arguments to pass to the activity."""

    activity_id: str | None = None
    """Optional user-provided activity ID."""

    task_queue: str | None = None
    """Task queue to run activity on (defaults to workflow's task queue)."""

    schedule_to_close_timeout_ms: int | None = None
    """Max total time for activity (schedule to completion), in milliseconds."""

    schedule_to_start_timeout_ms: int | None = None
    """Max time to wait for a worker to pick up the activity, in milliseconds."""

    start_to_close_timeout_ms: int | None = None
    """Max time for activity execution (from start to completion), in milliseconds."""

    heartbeat_timeout_ms: int | None = None
    """Max time between heartbeats, in milliseconds."""


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
class StartChildWorkflowCommand:
    """Command to start a child workflow.

    This command is emitted when execute_child_workflow() is called and starts
    a new child workflow. When the child completes, a resolution job will be
    delivered to the parent workflow.
    """

    seq: int
    """Sequence number identifying this child workflow."""

    workflow_type: str
    """Name of the workflow type to execute."""

    workflow_id: str
    """ID for the child workflow."""

    arguments: tuple[Any, ...]
    """Arguments to pass to the child workflow."""

    task_queue: str | None = None
    """Task queue to run child on (defaults to parent's)."""

    execution_timeout_ms: int | None = None
    """Total timeout for child workflow including retries, in milliseconds."""

    run_timeout_ms: int | None = None
    """Timeout for a single run of the child workflow, in milliseconds."""

    task_timeout_ms: int | None = None
    """Timeout for a single workflow task of the child, in milliseconds."""


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

    # Sequence counters
    timer_seq: int = 0
    """Sequence counter for timer IDs."""

    activity_seq: int = 0
    """Sequence counter for activity IDs."""

    child_workflow_seq: int = 0
    """Sequence counter for child workflow IDs."""

    signal_seq: int = 0
    """Sequence counter for signal IDs."""

    # Fired events (for replay) - populated from history
    fired_timers: dict[int, int] = field(default_factory=dict)
    """Timers that have fired: seq -> fire_time_ns."""

    completed_activities: dict[int, Any] = field(default_factory=dict)
    """Activities that have completed: seq -> result (or exception)."""

    completed_children: dict[int, Any] = field(default_factory=dict)
    """Child workflows that have completed: seq -> result (or exception)."""

    # Pending events (for suspension) - trio.Event instances
    pending_timers: dict[int, trio.Event] = field(default_factory=dict)
    """Timers waiting to fire: seq -> trio.Event."""

    pending_activities: dict[int, trio.Event] = field(default_factory=dict)
    """Activities waiting to complete: seq -> trio.Event."""

    pending_children: dict[int, trio.Event] = field(default_factory=dict)
    """Child workflows waiting to complete: seq -> trio.Event."""

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
        from temporalio_trio.workflow import Info

        return Info(
            workflow_id=self.workflow_id,
            workflow_type=self.workflow_type,
            run_id=self.run_id,
            task_queue=self.task_queue,
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

        return await self.execute_activity(
            activity_name,
            args,
            activity_id=activity_id,
            task_queue=task_queue,
            schedule_to_close_timeout=schedule_to_close_timeout,
            schedule_to_start_timeout=schedule_to_start_timeout,
            start_to_close_timeout=start_to_close_timeout,
            heartbeat_timeout=heartbeat_timeout,
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

        Returns:
            A handle to the started child workflow.
        """
        from temporalio_trio.workflow import ChildWorkflowHandle, _Definition

        # Extract workflow type name
        if isinstance(workflow, str):
            workflow_type = workflow
        else:
            defn = _Definition.from_class(workflow)
            workflow_type = defn.name if defn else workflow.__name__

        # Get seq for this child workflow
        seq = self.next_child_workflow_seq()

        # Create the handle
        handle: ChildWorkflowHandle = ChildWorkflowHandle(
            seq=seq,
            id=id,
            workflow_type=workflow_type,
        )

        # Check if already completed (replay path)
        if seq in self.completed_children:
            result = self.completed_children[seq]
            if isinstance(result, BaseException):
                handle._set_failure(result)
            else:
                handle._set_result(result)
            return handle

        # Create suspension event
        event = trio.Event()
        self.pending_children[seq] = event

        # Emit command
        self.commands.append(
            StartChildWorkflowCommand(
                seq=seq,
                workflow_type=workflow_type,
                workflow_id=id,
                arguments=args,
                task_queue=task_queue,
                execution_timeout_ms=(
                    int(execution_timeout.total_seconds() * 1000)
                    if execution_timeout
                    else None
                ),
                run_timeout_ms=(
                    int(run_timeout.total_seconds() * 1000) if run_timeout else None
                ),
                task_timeout_ms=(
                    int(task_timeout.total_seconds() * 1000) if task_timeout else None
                ),
            )
        )

        # Call suspension callback if set (for single-thread worker)
        if self.on_suspend is not None:
            self.on_suspend()

        # Wait for child to complete
        await event.wait()

        # Get result and clean up
        result = self.completed_children[seq]
        del self.pending_children[seq]

        # Set result on handle
        if isinstance(result, BaseException):
            handle._set_failure(result)
        else:
            handle._set_result(result)

        return handle

    async def workflow_wait_condition(
        self,
        fn: Callable[[], bool],
        *,
        timeout: float | None = None,
        timeout_summary: str | None = None,
    ) -> None:
        """Wait until condition returns True or timeout expires.

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

        # If no timeout, we need to wait indefinitely
        # This requires polling with short sleeps
        start_time_ns = self.time_ns
        timeout_ns = int(timeout * 1_000_000_000) if timeout else None

        while not fn():
            # Sleep for a short interval
            await self.workflow_sleep(0.1, summary=timeout_summary)

            # Check timeout
            if timeout_ns is not None:
                elapsed_ns = self.time_ns - start_time_ns
                if elapsed_ns >= timeout_ns:
                    raise TimeoutError(
                        f"Condition not met within timeout of {timeout} seconds"
                    )

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

        # Convert timeouts to milliseconds
        schedule_to_close_timeout_ms = (
            int(schedule_to_close_timeout.total_seconds() * 1000)
            if schedule_to_close_timeout
            else None
        )
        schedule_to_start_timeout_ms = (
            int(schedule_to_start_timeout.total_seconds() * 1000)
            if schedule_to_start_timeout
            else None
        )
        start_to_close_timeout_ms = (
            int(start_to_close_timeout.total_seconds() * 1000)
            if start_to_close_timeout
            else None
        )
        heartbeat_timeout_ms = (
            int(heartbeat_timeout.total_seconds() * 1000) if heartbeat_timeout else None
        )

        # Emit command
        self.commands.append(
            ScheduleActivityCommand(
                seq=seq,
                activity_type=activity,
                arguments=args,
                activity_id=activity_id,
                task_queue=task_queue,
                schedule_to_close_timeout_ms=schedule_to_close_timeout_ms,
                schedule_to_start_timeout_ms=schedule_to_start_timeout_ms,
                start_to_close_timeout_ms=start_to_close_timeout_ms,
                heartbeat_timeout_ms=heartbeat_timeout_ms,
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

        # Convert timeouts to milliseconds
        execution_timeout_ms = (
            int(execution_timeout.total_seconds() * 1000) if execution_timeout else None
        )
        run_timeout_ms = (
            int(run_timeout.total_seconds() * 1000) if run_timeout else None
        )
        task_timeout_ms = (
            int(task_timeout.total_seconds() * 1000) if task_timeout else None
        )

        # Emit command
        self.commands.append(
            StartChildWorkflowCommand(
                seq=seq,
                workflow_type=workflow,
                workflow_id=workflow_id,
                arguments=args,
                task_queue=task_queue,
                execution_timeout_ms=execution_timeout_ms,
                run_timeout_ms=run_timeout_ms,
                task_timeout_ms=task_timeout_ms,
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
