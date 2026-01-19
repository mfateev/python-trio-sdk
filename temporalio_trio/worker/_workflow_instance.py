"""Workflow instance implementation for Temporal with Trio.

This module contains the core classes for creating and managing workflow instances.
"""

from __future__ import annotations

import inspect
import logging
import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable

import temporalio.common
import trio

logger = logging.getLogger(__name__)

from temporalio_trio.worker._activation import (
    ActivityResolvedJob,
    CancelChildWorkflowCommand,
    CancelWorkflowCommand,
    CancelWorkflowJob,
    ChildWorkflowResolvedJob,
    ChildWorkflowStartedJob,
    ChildWorkflowStartFailedJob,
    CompleteWorkflowCommand,
    FailWorkflowCommand,
    QueryResultCommand,
    QueryWorkflowJob,
    ScheduleActivityCommand,
    SignalWorkflowJob,
    StartChildWorkflowCommand,
    StartTimerCommand,
    TimerFiredJob,
    WorkflowActivation,
    WorkflowActivationCompletion,
    WorkflowStartedJob,
)
from temporalio_trio.worker._clock import WorkflowClock
from temporalio_trio.workflow import (
    ChildWorkflowCancellationType,
    ChildWorkflowHandle,
    Info,
    ParentClosePolicy,
    _Definition,
    _QueryDefinition,
    _Runtime,
    _SignalDefinition,
)

__all__ = [
    "WorkflowRunner",
    "TrioWorkflowRunner",
    "WorkflowInstanceDetails",
    "WorkflowInstance",
    "TrioWorkflowInstance",
]


class _WorkflowYield(BaseException):
    """Signal that workflow needs to yield back to the activation loop.

    This is raised when the workflow needs to wait for an external event
    (like a timer firing). It's a BaseException so it won't be caught by
    normal exception handlers in workflow code.
    """

    pass


class _WorkflowCancelled(BaseException):
    """Signal that workflow was cancelled.

    This is raised when the workflow receives a cancellation request.
    It's a BaseException so it won't be caught by normal exception handlers.
    """

    pass


class WorkflowRunner(ABC):
    """Abstract runner for workflows.

    Mirrors temporalio.worker.WorkflowRunner from the SDK.

    A workflow runner is responsible for:
    - Preparing workflow definitions for execution (validation, setup)
    - Creating workflow instances for each execution

    The runner abstracts the execution environment (Trio, asyncio, etc.)
    from the rest of the worker infrastructure.
    """

    @abstractmethod
    def prepare_workflow(self, defn: _Definition) -> None:
        """Prepare a workflow definition for execution.

        Called once per workflow type when a worker starts. This is used to
        validate the workflow and perform any setup needed before instances
        can be created.

        Args:
            defn: The workflow definition to prepare.

        Raises:
            ValueError: If the workflow is invalid or incompatible.
        """
        ...

    @abstractmethod
    def create_instance(self, det: WorkflowInstanceDetails) -> WorkflowInstance:
        """Create a workflow instance for execution.

        Called for each new workflow execution. The returned instance handles
        activations and produces completions.

        Args:
            det: Details for the workflow instance.

        Returns:
            A new workflow instance.

        Raises:
            ValueError: If the workflow has not been prepared.
        """
        ...


class TrioWorkflowRunner(WorkflowRunner):
    """Workflow runner that uses Trio for async execution.

    This runner creates TrioWorkflowInstance objects that execute workflow
    code using Trio's deterministic scheduling mode.
    """

    def __init__(self) -> None:
        """Initialize the Trio workflow runner."""
        self._prepared: set[str] = set()

    def prepare_workflow(self, defn: _Definition) -> None:
        """Prepare a workflow for Trio execution.

        Validates that the workflow is compatible with Trio execution.
        Note: Basic validation (run method exists, is async) is already done
        by @workflow.defn, but we re-validate here for safety.

        Args:
            defn: The workflow definition to prepare.

        Raises:
            ValueError: If the workflow is invalid.
        """
        import inspect

        # Re-validate that run_fn is async (should already be validated by @workflow.defn)
        if not inspect.iscoroutinefunction(defn.run_fn):
            raise ValueError(
                f"Workflow {defn.name} run method must be async "
                f"(defined with 'async def')"
            )

        self._prepared.add(defn.name)

    def create_instance(self, det: WorkflowInstanceDetails) -> WorkflowInstance:
        """Create a Trio-based workflow instance.

        Args:
            det: Details for the workflow instance.

        Returns:
            A new TrioWorkflowInstance.

        Raises:
            ValueError: If the workflow has not been prepared.
        """
        if det.defn.name not in self._prepared:
            raise ValueError(
                f"Workflow {det.defn.name} not prepared. Call prepare_workflow() first."
            )

        return TrioWorkflowInstance(det)


@dataclass(frozen=True)
class WorkflowInstanceDetails:
    """Immutable details for creating a workflow instance.

    Mirrors temporalio.worker.WorkflowInstanceDetails from the SDK.

    This dataclass contains all the information needed to create a new workflow
    instance, including the workflow definition, runtime info, and randomness seed.

    Attributes:
        defn: The workflow definition containing metadata and the run function.
        info: Information about the workflow execution (ID, type, run ID, etc.).
        randomness_seed: Seed for deterministic random number generation.
    """

    defn: _Definition
    """The workflow definition containing metadata and the run function."""

    info: Info
    """Information about the workflow execution."""

    randomness_seed: int
    """Seed for deterministic random number generation."""


class WorkflowInstance(ABC):
    """Abstract base class for workflow instances.

    Mirrors temporalio.worker.WorkflowInstance from the SDK.

    A workflow instance handles activations from the Temporal server and returns
    completions with commands to execute. Each workflow execution has one instance.

    The instance receives activation jobs (like workflow start, timer fired, etc.)
    and produces completion commands (like start timer, complete workflow, etc.).
    """

    @abstractmethod
    def activate(
        self,
        act: WorkflowActivation,
    ) -> WorkflowActivationCompletion:
        """Handle an activation and return completion.

        This method processes jobs from an activation (e.g., workflow started,
        timer fired) and returns commands to be executed (e.g., start timer,
        complete workflow).

        Args:
            act: The activation containing jobs to process.

        Returns:
            Completion containing commands to execute.
        """
        ...


class TrioWorkflowInstance(WorkflowInstance, _Runtime):
    """Trio-based workflow instance.

    Implements both WorkflowInstance (activation handling) and _Runtime
    (workflow API implementation). This is the core class that bridges
    Temporal's activation model with Trio's async execution.

    The instance uses Trio's deterministic scheduling mode to ensure
    consistent execution during replay.

    For simplicity in this POC, workflows re-execute from the beginning on each
    activation, with timers that have already fired completing immediately.
    This matches the replay semantics of Temporal.

    Attributes:
        _defn: The workflow definition.
        _info: Information about the workflow execution.
        _random: Seeded random number generator for determinism.
        _time_ns: Current workflow time in nanoseconds.
        _timer_seq: Sequence counter for timer IDs.
        _workflow_obj: The instantiated workflow class object.
        _fired_timers: Set of timer IDs that have already fired.
        _pending_timer_id: The timer ID we're currently waiting for (if any).
        _commands: Commands generated during the current activation.
        _start_args: Arguments from the WorkflowStartedJob (for replay).
    """

    def __init__(self, det: WorkflowInstanceDetails) -> None:
        """Initialize a Trio workflow instance.

        Args:
            det: Details for creating the workflow instance.
        """
        self._defn = det.defn
        self._info = det.info
        self._random = random.Random(det.randomness_seed)
        self._time_ns: int = 0
        self._timer_seq: int = 0
        self._activity_seq: int = 0
        self._workflow_obj: object | None = None
        self._fired_timers: set[int] = set()
        self._pending_timer_id: int | None = None
        # Track resolved activities: seq -> (result, failure)
        self._resolved_activities: dict[int, tuple[Any, BaseException | None]] = {}
        self._pending_activity_seq: int | None = None
        self._commands: list[
            StartTimerCommand
            | CompleteWorkflowCommand
            | FailWorkflowCommand
            | CancelWorkflowCommand
            | ScheduleActivityCommand
            | QueryResultCommand
            | StartChildWorkflowCommand
            | CancelChildWorkflowCommand
        ] = []
        self._start_args: tuple[Any, ...] | None = None
        self._cancel_requested: bool = False
        # Signal handling
        self._signals: dict[str | None, _SignalDefinition] = dict(det.defn.signals)
        self._pending_signals: list[SignalWorkflowJob] = []
        # Query handling
        self._queries: dict[str | None, _QueryDefinition] = dict(det.defn.queries)
        self._pending_queries: list[QueryWorkflowJob] = []
        # Child workflow handling
        self._child_workflow_seq: int = 0
        # Track child workflows waiting for start confirmation: seq -> handle
        self._pending_child_starts: dict[int, ChildWorkflowHandle[Any, Any]] = {}
        # Track child workflows waiting for completion: seq -> handle
        self._pending_child_results: dict[int, ChildWorkflowHandle[Any, Any]] = {}
        # Track resolved child workflows for replay: seq -> (result, failure)
        self._resolved_child_workflows: dict[int, tuple[Any, BaseException | None]] = {}
        # Track started child workflows (for determining run_id during replay): seq -> run_id
        self._started_child_workflows: dict[int, str] = {}
        # Current pending child workflow sequence (for yield tracking)
        self._pending_child_seq: int | None = None

    @property
    def defn(self) -> _Definition:
        """Get the workflow definition.

        Returns:
            The workflow definition.
        """
        return self._defn

    @property
    def info(self) -> Info:
        """Get the workflow info.

        Returns:
            Information about the workflow execution.
        """
        return self._info

    def activate(
        self,
        act: WorkflowActivation,
    ) -> WorkflowActivationCompletion:
        """Handle an activation and return completion.

        This method sets up the Trio runtime context and executes the workflow
        with deterministic scheduling. Each activation updates the workflow time
        and processes all jobs in the activation.

        Args:
            act: The activation containing jobs to process.

        Returns:
            Completion containing commands to execute.
        """
        # Update time from activation
        self._time_ns = act.timestamp_ns
        self._commands = []

        # Process jobs to update state before running workflow
        has_workflow_start = False
        for job in act.jobs:
            if isinstance(job, WorkflowStartedJob):
                has_workflow_start = True
                self._start_args = job.args
            elif isinstance(job, TimerFiredJob):
                self._fired_timers.add(job.timer_id)
                if self._pending_timer_id == job.timer_id:
                    self._pending_timer_id = None
            elif isinstance(job, ActivityResolvedJob):
                # Store activity result for replay
                self._resolved_activities[job.seq] = (job.result, job.failure)
                if self._pending_activity_seq == job.seq:
                    self._pending_activity_seq = None
            elif isinstance(job, CancelWorkflowJob):
                self._cancel_requested = True
            elif isinstance(job, SignalWorkflowJob):
                self._pending_signals.append(job)
            elif isinstance(job, QueryWorkflowJob):
                self._pending_queries.append(job)
            elif isinstance(job, ChildWorkflowStartedJob):
                # Child workflow started - store run_id for replay
                self._started_child_workflows[job.seq] = job.run_id
                if self._pending_child_seq == job.seq:
                    self._pending_child_seq = None
            elif isinstance(job, ChildWorkflowStartFailedJob):
                # Child workflow failed to start - store as failure for replay
                self._resolved_child_workflows[job.seq] = (
                    None,
                    RuntimeError(
                        f"Child workflow '{job.workflow_type}' (id={job.workflow_id}) "
                        f"failed to start: {job.cause}"
                    ),
                )
                if self._pending_child_seq == job.seq:
                    self._pending_child_seq = None
            elif isinstance(job, ChildWorkflowResolvedJob):
                # Child workflow completed - store result for replay
                self._resolved_child_workflows[job.seq] = (job.result, job.failure)
                if self._pending_child_seq == job.seq:
                    self._pending_child_seq = None

        # If we have start args (either new or from previous activation), run workflow
        if self._start_args is not None:
            # Reset sequences for deterministic replay
            self._timer_seq = 0
            self._activity_seq = 0
            self._child_workflow_seq = 0

            token = _Runtime.set_current(self)
            try:
                clock = WorkflowClock(self._time_ns)

                # Run with trio using deterministic scheduling
                try:
                    trio.run(
                        self._run_workflow,
                        deterministic=True,  # type: ignore[call-arg]
                        random_seed=self._random.getrandbits(64),  # type: ignore[call-arg]
                        clock=clock,
                    )
                except _WorkflowYield:
                    # Expected - workflow yielded waiting for external event
                    pass

            finally:
                _Runtime.reset_current(token)

        # Process queries after workflow has run (queries need access to workflow state)
        # Queries are processed synchronously and should not mutate state
        for query_job in self._pending_queries:
            self._apply_query(query_job)
        self._pending_queries.clear()

        return WorkflowActivationCompletion(commands=self._commands)

    async def _run_workflow(self) -> None:
        """Run the workflow from the beginning.

        This re-executes the workflow, with fired timers completing immediately.
        If cancellation was requested, the workflow will raise _WorkflowCancelled
        when it tries to wait for something (like a timer).
        """
        assert self._start_args is not None
        self._workflow_obj = self._defn.cls()

        # Process pending signals before running the main workflow
        for signal_job in self._pending_signals:
            await self._apply_signal(signal_job)
        self._pending_signals.clear()

        try:
            result = await self._defn.run_fn(self._workflow_obj, *self._start_args)
            self._commands.append(CompleteWorkflowCommand(result=result))
        except _WorkflowYield:
            # Re-raise to propagate yield signal
            raise
        except _WorkflowCancelled:
            # Workflow was cancelled - emit cancel command
            self._commands.append(CancelWorkflowCommand())
        except Exception as e:
            self._commands.append(FailWorkflowCommand(exception=e))

    async def _apply_signal(self, job: SignalWorkflowJob) -> None:
        """Apply a signal to the workflow.

        Args:
            job: The signal job containing signal name and arguments.
        """
        defn = self._signals.get(job.signal_name) or self._signals.get(None)
        if defn is None:
            # Buffer signal for later (dynamic handler may be added)
            logger.warning(f"No handler for signal '{job.signal_name}', ignoring")
            return

        handler = defn.fn
        if defn.is_method:
            handler = handler.__get__(self._workflow_obj, type(self._workflow_obj))

        result = handler(*job.args)
        if inspect.iscoroutine(result):
            await result

    def _apply_query(self, job: QueryWorkflowJob) -> None:
        """Apply a query to the workflow (synchronous, read-only).

        Query handlers must be synchronous and should not mutate workflow state.
        The result is sent back to the query caller via a QueryResultCommand.

        Args:
            job: The query job containing query type and arguments.
        """
        try:
            defn = self._queries.get(job.query_type) or self._queries.get(None)
            if defn is None:
                known = sorted([k for k in self._queries.keys() if k])
                raise RuntimeError(
                    f"Query handler for '{job.query_type}' not found. "
                    f"Known queries: [{', '.join(known)}]"
                )

            handler = defn.fn
            if defn.is_method:
                handler = handler.__get__(self._workflow_obj, type(self._workflow_obj))

            result = handler(*job.args)

            self._commands.append(
                QueryResultCommand(
                    query_id=job.query_id,
                    result=result,
                )
            )
        except Exception as e:
            self._commands.append(
                QueryResultCommand(
                    query_id=job.query_id,
                    error=str(e),
                )
            )

    # _Runtime implementation

    def workflow_time_ns(self) -> int:
        """Get current workflow time in nanoseconds.

        Returns:
            Current workflow time in nanoseconds since epoch.
        """
        return self._time_ns

    def workflow_info(self) -> Info:
        """Get information about the current workflow.

        Returns:
            Info about the current workflow execution.
        """
        return self._info

    async def workflow_sleep(self, duration: float, summary: str | None) -> None:
        """Sleep for the given duration.

        If this timer has already fired (during replay), this completes immediately.
        Otherwise, creates a timer command and yields back to the activation loop.

        Args:
            duration: Sleep duration in seconds.
            summary: Optional human-readable description for UI/CLI visibility.
        """
        timer_id = self._timer_seq
        self._timer_seq += 1

        # Check if this timer has already fired (replay)
        if timer_id in self._fired_timers:
            return

        # Check for cancellation before waiting for timer
        if self._cancel_requested:
            raise _WorkflowCancelled()

        # Timer hasn't fired yet - create command and yield
        self._commands.append(
            StartTimerCommand(
                timer_id=timer_id,
                duration_ms=int(duration * 1000),
                summary=summary,
            )
        )
        self._pending_timer_id = timer_id

        # Yield control - workflow will re-run when timer fires
        raise _WorkflowYield()

    # For backward compatibility with tests that access internal state
    @property
    def _pending_timers(self) -> dict[int, Any]:
        """Get pending timers (for test compatibility).

        Returns:
            Dict with pending timer ID if any.
        """
        if self._pending_timer_id is not None:
            return {self._pending_timer_id: None}
        return {}

    async def workflow_execute_activity(
        self,
        activity: str | Callable[..., Any],
        *args: Any,
        task_queue: str | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        activity_id: str | None = None,
    ) -> Any:
        """Execute an activity and wait for its result.

        If this activity has already resolved (during replay), returns immediately.
        Otherwise, creates a schedule activity command and yields back to the
        activation loop.

        Args:
            activity: Activity name or function reference.
            *args: Arguments to pass to the activity.
            task_queue: Task queue to run the activity on. Defaults to workflow's queue.
            schedule_to_close_timeout: Max time for activity from schedule to completion.
            schedule_to_start_timeout: Max time waiting for worker to pick up activity.
            start_to_close_timeout: Max time for activity execution.
            heartbeat_timeout: Max time between heartbeats.
            retry_policy: Retry policy for the activity.
            activity_id: Optional unique identifier for the activity.

        Returns:
            The activity result.

        Raises:
            RuntimeError: If the activity fails or is cancelled.
        """
        # Validate that at least one timeout is set
        if schedule_to_close_timeout is None and start_to_close_timeout is None:
            raise ValueError(
                "At least one of schedule_to_close_timeout or start_to_close_timeout "
                "must be set for activity execution"
            )

        # Get activity name from string or callable
        if isinstance(activity, str):
            activity_type = activity
        else:
            # Try to get name from activity definition
            defn = getattr(activity, "__temporal_activity_definition", None)
            if defn is not None:
                activity_type = defn.name
            else:
                # Fallback to function name
                activity_type = getattr(activity, "__name__", str(activity))

        # Get sequence number for this activity
        seq = self._activity_seq
        self._activity_seq += 1

        # Check if this activity has already resolved (replay)
        if seq in self._resolved_activities:
            result, failure = self._resolved_activities[seq]
            if failure is not None:
                raise failure
            return result

        # Check for cancellation before scheduling activity
        if self._cancel_requested:
            raise _WorkflowCancelled()

        # Generate activity ID if not provided
        if activity_id is None:
            activity_id = str(seq)

        # Activity hasn't resolved yet - create command and yield
        self._commands.append(
            ScheduleActivityCommand(
                seq=seq,
                activity_id=activity_id,
                activity_type=activity_type,
                args=tuple(args),
                task_queue=task_queue,
                schedule_to_close_timeout=schedule_to_close_timeout,
                schedule_to_start_timeout=schedule_to_start_timeout,
                start_to_close_timeout=start_to_close_timeout,
                heartbeat_timeout=heartbeat_timeout,
                retry_policy=retry_policy,
            )
        )
        self._pending_activity_seq = seq

        # Yield control - workflow will re-run when activity resolves
        raise _WorkflowYield()

    async def workflow_start_child_workflow(
        self,
        workflow: str | type,
        *args: Any,
        id: str,
        task_queue: str | None,
        cancellation_type: ChildWorkflowCancellationType,
        parent_close_policy: ParentClosePolicy,
        execution_timeout: timedelta | None,
        run_timeout: timedelta | None,
        task_timeout: timedelta | None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy,
        retry_policy: temporalio.common.RetryPolicy | None,
    ) -> ChildWorkflowHandle[Any, Any]:
        """Start a child workflow and return a handle.

        If this child workflow has already started or completed (during replay),
        returns immediately with the appropriate state. Otherwise, creates a
        start child workflow command and yields back to the activation loop.

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

        Raises:
            RuntimeError: If the child workflow fails to start.
        """
        # Get workflow name from class or string
        if isinstance(workflow, str):
            workflow_type = workflow
        else:
            # Try to get name from workflow definition
            from temporalio_trio.workflow import _Definition

            defn = _Definition.from_class(workflow)
            if defn is not None:
                workflow_type = defn.name
            else:
                # Fallback to class name
                workflow_type = getattr(workflow, "__name__", str(workflow))

        # Get sequence number for this child workflow
        seq = self._child_workflow_seq
        self._child_workflow_seq += 1

        # Create handle for this child workflow
        handle: ChildWorkflowHandle[Any, Any] = ChildWorkflowHandle(
            seq=seq,
            id=id,
            workflow_type=workflow_type,
        )

        # Check if this child workflow has already resolved (replay)
        if seq in self._resolved_child_workflows:
            result, failure = self._resolved_child_workflows[seq]

            # Check if child started - if not in _started_child_workflows but in
            # _resolved_child_workflows, it's a start failure - raise immediately
            if seq not in self._started_child_workflows:
                # Start failure - raise immediately from start_child_workflow
                if failure is not None:
                    raise failure
                # Shouldn't happen, but handle gracefully
                raise RuntimeError("Child workflow failed to start with unknown error")

            # Child started and resolved - set result/failure on handle
            handle._set_started(self._started_child_workflows[seq])
            if failure is not None:
                handle._set_failure(failure)
            else:
                handle._set_result(result)
            return handle

        # Check if this child workflow has started but not completed
        if seq in self._started_child_workflows:
            handle._set_started(self._started_child_workflows[seq])
            # Still need to wait for completion - yield
            self._pending_child_seq = seq
            raise _WorkflowYield()

        # Check for cancellation before starting child workflow
        if self._cancel_requested:
            raise _WorkflowCancelled()

        # Child workflow hasn't started yet - create command and yield
        self._commands.append(
            StartChildWorkflowCommand(
                seq=seq,
                workflow_id=id,
                workflow_type=workflow_type,
                args=tuple(args),
                task_queue=task_queue,
                execution_timeout=execution_timeout,
                run_timeout=run_timeout,
                task_timeout=task_timeout,
                parent_close_policy=parent_close_policy.value,
                cancellation_type=cancellation_type.value,
                retry_policy=retry_policy,
                id_reuse_policy=id_reuse_policy.value,
            )
        )
        self._pending_child_seq = seq

        # Yield control - workflow will re-run when child workflow starts/completes
        raise _WorkflowYield()
