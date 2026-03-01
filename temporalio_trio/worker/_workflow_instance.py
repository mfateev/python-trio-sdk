"""Workflow instance implementation for Temporal with Trio.

This module contains the core classes for creating and managing workflow instances.
"""

from __future__ import annotations

import inspect
import logging
import random
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Callable, NoReturn, Sequence

import outcome
import temporalio.api.common.v1
import temporalio.bridge.proto.child_workflow
import temporalio.common
import temporalio.converter
import temporalio.exceptions
import trio
import trio.lowlevel

logger = logging.getLogger(__name__)

from temporalio_trio.worker._activation import (
    ActivityResolvedJob,
    CancelChildWorkflowCommand,
    CancelExternalResolvedJob,
    CancelTimerCommand,
    CancelWorkflowCommand,
    CancelWorkflowJob,
    ChildWorkflowResolvedJob,
    ChildWorkflowStartedJob,
    ChildWorkflowStartFailedJob,
    CompleteWorkflowCommand,
    ContinueAsNewCommand,
    FailWorkflowCommand,
    NotifyHasPatchJob,
    QueryResultCommand,
    QueryWorkflowJob,
    RequestCancelExternalWorkflowCommand,
    ScheduleActivityCommand,
    ScheduleLocalActivityCommand,
    SetPatchMarkerCommand,
    SignalExternalResolvedJob,
    SignalExternalWorkflowCommand,
    SignalWorkflowJob,
    StartChildWorkflowCommand,
    StartTimerCommand,
    TimerFiredJob,
    UpdateResponseCommand,
    UpdateWorkflowJob,
    UpsertSearchAttributesCommand,
    WorkflowActivation,
    WorkflowActivationCompletion,
    WorkflowStartedJob,
)
from temporalio_trio.worker._clock import WorkflowClock
from temporalio_trio.workflow import (
    ChildWorkflowCancellationType,
    ChildWorkflowHandle,
    ContinueAsNewError,
    ExternalWorkflowHandle,
    Info,
    ParentClosePolicy,
    UpdateInfo,
    _Definition,
    _QueryDefinition,
    _Runtime,
    _set_current_update_info,
    _SignalDefinition,
    _UpdateDefinition,
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


class _ContinueAsNewError(ContinueAsNewError):
    """Internal continue-as-new error with command generation.

    This is the concrete implementation of ContinueAsNewError that stores
    the parameters needed to create a ContinueAsNewCommand when the exception
    is caught by the workflow instance.
    """

    def __init__(
        self,
        instance: "TrioWorkflowInstance",
        workflow_type: str,
        args: tuple[Any, ...],
        task_queue: str | None,
        run_timeout: timedelta | None,
        task_timeout: timedelta | None,
        retry_policy: temporalio.common.RetryPolicy | None,
        memo: Mapping[str, Any] | None = None,
        search_attributes: temporalio.common.SearchAttributes
        | temporalio.common.TypedSearchAttributes
        | None = None,
    ) -> None:
        """Initialize _ContinueAsNewError.

        Args:
            instance: The workflow instance that will generate the command.
            workflow_type: The workflow type for the new execution.
            args: Arguments to pass to the new execution.
            task_queue: Task queue for the new execution.
            run_timeout: Run timeout for the new execution.
            task_timeout: Task timeout for the new execution.
            retry_policy: Retry policy for the new execution.
            memo: Optional memo key-value pairs for the new execution.
            search_attributes: Optional search attributes for the new execution.
        """
        super().__init__("Continue as new")
        self._instance = instance
        self._workflow_type = workflow_type
        self._args = args
        self._task_queue = task_queue
        self._run_timeout = run_timeout
        self._task_timeout = task_timeout
        self._retry_policy = retry_policy
        self._memo = memo
        self._search_attributes = search_attributes

    def _apply_command(self) -> None:
        """Add ContinueAsNewCommand to instance's commands."""
        self._instance._commands.append(
            ContinueAsNewCommand(
                workflow_type=self._workflow_type,
                args=self._args,
                task_queue=self._task_queue,
                run_timeout=self._run_timeout,
                task_timeout=self._task_timeout,
                retry_policy=self._retry_policy,
                memo=self._memo,
                search_attributes=self._search_attributes,
            )
        )


class WorkflowRunner(ABC):
    """Abstract runner for workflows.

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

        assert defn.name is not None, "Workflow definition must have a name"
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
        self._condition_seq: int = 0
        self._workflow_obj: object | None = None
        self._fired_timers: set[int] = set()
        self._pending_timer_id: int | None = None
        # Track resolved activities: seq -> (result, failure)
        self._resolved_activities: dict[int, tuple[Any, BaseException | None]] = {}
        self._pending_activity_seq: int | None = None
        self._commands: list[
            StartTimerCommand
            | CancelTimerCommand
            | CompleteWorkflowCommand
            | FailWorkflowCommand
            | CancelWorkflowCommand
            | ScheduleActivityCommand
            | ScheduleLocalActivityCommand
            | QueryResultCommand
            | StartChildWorkflowCommand
            | CancelChildWorkflowCommand
            | SignalExternalWorkflowCommand
            | RequestCancelExternalWorkflowCommand
            | ContinueAsNewCommand
            | SetPatchMarkerCommand
            | UpsertSearchAttributesCommand
            | UpdateResponseCommand
        ] = []
        self._start_args: tuple[Any, ...] | None = None
        self._cancel_requested: bool = False
        # Signal handling
        self._signals: dict[str | None, _SignalDefinition] = dict(det.defn.signals)
        self._pending_signals: list[SignalWorkflowJob] = []
        # Query handling
        self._queries: dict[str | None, _QueryDefinition] = dict(det.defn.queries)
        self._pending_queries: list[QueryWorkflowJob] = []
        # Update handling
        self._updates: dict[str | None, _UpdateDefinition] = dict(det.defn.updates)
        self._pending_updates: list[UpdateWorkflowJob] = []
        self._in_progress_updates: dict[str, str] = {}  # update_id -> update_name
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

        # External signal handling
        self._signal_external_seq: int = 0
        # Track resolved external signals for replay: seq -> failure (None = success)
        self._resolved_external_signals: dict[int, BaseException | None] = {}
        # Current pending external signal sequence (for yield tracking)
        self._pending_external_signal_seq: int | None = None

        # External cancel handling
        self._cancel_external_seq: int = 0
        # Track resolved external cancels for replay: seq -> failure (None = success)
        self._resolved_external_cancels: dict[int, BaseException | None] = {}
        # Current pending external cancel sequence (for yield tracking)
        self._pending_external_cancel_seq: int | None = None

        # Guest mode state (Phase 1/2)
        self._guest_running: bool = False
        self._workflow_outcome: outcome.Outcome | None = None
        self._timer_events: dict[int, trio.Event] = {}
        self._activity_events: dict[int, trio.Event] = {}
        self._condition_events: dict[int, trio.Event] = {}
        self._child_workflow_events: dict[int, trio.Event] = {}
        self._external_signal_events: dict[int, trio.Event] = {}
        self._external_cancel_events: dict[int, trio.Event] = {}
        self._pending_conditions: list[tuple[int, Callable[[], bool], trio.Event]] = []
        self._pending_callbacks: list[Callable[[], object]] = []
        self._guest_clock: WorkflowClock | None = None

        # Patching/Versioning (Phase 2 of feature parity)
        self._patches_notified: set[str] = set()
        """Patch IDs notified via NotifyHasPatch jobs (from history during replay)."""

        self._patches_memoized: dict[str, bool] = {}
        """Memoized results of patched() calls to ensure consistency."""

        self._is_replaying: bool = False
        """Whether the current activation is replaying from history."""

        self._headers: Mapping[str, temporalio.api.common.v1.Payload] = {}
        """Headers from the workflow start (e.g. for tracing/auth interceptors)."""

        self._memo_cache: dict[str, Any] | None = None
        """Cached decoded memo values (lazily populated)."""

        self._current_details: str = ""
        """Current workflow details string for UI/CLI display."""

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

        In guest mode (Phase 2):
        - The guest run persists across activations
        - Jobs set events to wake the workflow
        - Workflow resumes from where it was suspended, not from the beginning

        Args:
            act: The activation containing jobs to process.

        Returns:
            Completion containing commands to execute.
        """
        # Update time from activation
        self._time_ns = act.timestamp_ns
        self._commands = []

        # Update replay flag from activation (if available)
        self._is_replaying = getattr(act, "is_replaying", False)

        # Update clock if guest is running
        if self._guest_running and self._guest_clock is not None:
            self._guest_clock.advance_to(self._time_ns)

        # Process jobs to update state and set events for waiting workflows
        has_workflow_start = False
        for job in act.jobs:
            if isinstance(job, WorkflowStartedJob):
                has_workflow_start = True
                self._start_args = job.args
                self._headers = job.headers
            elif isinstance(job, TimerFiredJob):
                self._fired_timers.add(job.timer_id)
                if self._pending_timer_id == job.timer_id:
                    self._pending_timer_id = None
                # Set event to wake workflow (guest mode)
                if job.timer_id in self._timer_events:
                    self._timer_events[job.timer_id].set()
            elif isinstance(job, ActivityResolvedJob):
                # Store activity result for replay
                # Note: backoff is stored as a RuntimeError here for the legacy
                # code path; the primary path (SingleThreadWorker) handles
                # backoff properly via _ActivityBackoff sentinel.
                if job.backoff is not None:
                    self._resolved_activities[job.seq] = (
                        None,
                        RuntimeError("Activity scheduled for retry (backoff)"),
                    )
                else:
                    self._resolved_activities[job.seq] = (job.result, job.failure)
                if self._pending_activity_seq == job.seq:
                    self._pending_activity_seq = None
                # Set event to wake workflow (guest mode)
                if job.seq in self._activity_events:
                    self._activity_events[job.seq].set()
            elif isinstance(job, CancelWorkflowJob):
                self._cancel_requested = True
                # Set ALL pending events to wake workflow for cancellation
                for event in self._timer_events.values():
                    event.set()
                for event in self._activity_events.values():
                    event.set()
                for _, _, event in self._pending_conditions:
                    event.set()
                for event in self._child_workflow_events.values():
                    event.set()
            elif isinstance(job, SignalWorkflowJob):
                self._pending_signals.append(job)
            elif isinstance(job, UpdateWorkflowJob):
                self._pending_updates.append(job)
            elif isinstance(job, QueryWorkflowJob):
                self._pending_queries.append(job)
            elif isinstance(job, ChildWorkflowStartedJob):
                # Child workflow started - store run_id for replay
                self._started_child_workflows[job.seq] = job.run_id
                if self._pending_child_seq == job.seq:
                    self._pending_child_seq = None
                # Set event to wake workflow (guest mode)
                if job.seq in self._child_workflow_events:
                    self._child_workflow_events[job.seq].set()
            elif isinstance(job, ChildWorkflowStartFailedJob):
                # Child workflow failed to start - match sdk-python exception types
                if job.cause == int(
                    temporalio.bridge.proto.child_workflow.StartChildWorkflowExecutionFailedCause.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS
                ):
                    err: BaseException = (
                        temporalio.exceptions.WorkflowAlreadyStartedError(
                            job.workflow_id, job.workflow_type
                        )
                    )
                else:
                    err = RuntimeError(f"Unknown child start fail cause: {job.cause}")
                self._resolved_child_workflows[job.seq] = (None, err)
                if self._pending_child_seq == job.seq:
                    self._pending_child_seq = None
                # Set event to wake workflow (guest mode)
                if job.seq in self._child_workflow_events:
                    self._child_workflow_events[job.seq].set()
            elif isinstance(job, ChildWorkflowResolvedJob):
                # Child workflow completed - decode result and failure
                decoded_result = None
                failure: BaseException | None = None
                if job.result_payload is not None:
                    converter = (
                        temporalio.converter.DataConverter.default.payload_converter
                    )
                    decoded_result = converter.from_payloads([job.result_payload])[0]
                elif job.failure_proto is not None:
                    if isinstance(job.failure_proto, BaseException):
                        # Already a Python exception (unit test path)
                        failure = job.failure_proto
                    else:
                        # Raw protobuf Failure from bridge - convert
                        from temporalio_trio.worker._failure_converter import (
                            failure_to_exception,
                        )

                        failure = failure_to_exception(
                            job.failure_proto,
                            temporalio.converter.DataConverter.default.payload_converter,
                        )
                self._resolved_child_workflows[job.seq] = (decoded_result, failure)
                if self._pending_child_seq == job.seq:
                    self._pending_child_seq = None
                # Set event to wake workflow (guest mode)
                if job.seq in self._child_workflow_events:
                    self._child_workflow_events[job.seq].set()
            elif isinstance(job, SignalExternalResolvedJob):
                # External signal resolved - store result for replay
                self._resolved_external_signals[job.seq] = job.failure
                if self._pending_external_signal_seq == job.seq:
                    self._pending_external_signal_seq = None
                # Set event to wake workflow (guest mode)
                if job.seq in self._external_signal_events:
                    self._external_signal_events[job.seq].set()
            elif isinstance(job, CancelExternalResolvedJob):
                # External cancel resolved - store result for replay
                self._resolved_external_cancels[job.seq] = job.failure
                if self._pending_external_cancel_seq == job.seq:
                    self._pending_external_cancel_seq = None
                # Set event to wake workflow (guest mode)
                if job.seq in self._external_cancel_events:
                    self._external_cancel_events[job.seq].set()
            elif isinstance(job, NotifyHasPatchJob):
                # Patch notification from history - record for patched() calls
                self._patches_notified.add(job.patch_id)

        # If we have start args (either new or from previous activation), run workflow
        if self._start_args is not None:
            # Reset sequences for deterministic replay
            # (In replay-from-beginning mode, we always re-run from the start)
            self._timer_seq = 0
            self._activity_seq = 0
            self._child_workflow_seq = 0
            self._condition_seq = 0
            self._signal_external_seq = 0
            self._cancel_external_seq = 0

            token = _Runtime.set_current(self)
            try:
                # Use trio.run() for each activation (replay-from-beginning approach)
                # This ensures isolation between workflow instances in the same thread.
                # Guest mode (event-based suspension) is available but not used by default
                # because Trio's global state prevents multiple guest runs per thread.
                clock = WorkflowClock(self._time_ns)

                # _WorkflowYield is caught inside _run_workflow_guarded so it
                # never escapes through trio.run() as a BaseException.  Letting
                # a BaseException propagate out of the main task can prevent
                # Trio from closing its internal wakeup socket pair, leaking a
                # file descriptor.
                trio.run(
                    self._run_workflow_guarded,
                    deterministic=True,  # type: ignore[call-arg]
                    random_seed=self._random.getrandbits(64),  # type: ignore[call-arg]
                    clock=clock,
                )

            finally:
                _Runtime.reset_current(token)

        # Process queries after workflow has run (queries need access to workflow state)
        # Queries are processed synchronously and should not mutate state
        for query_job in self._pending_queries:
            self._apply_query(query_job)
        self._pending_queries.clear()

        return WorkflowActivationCompletion(commands=self._commands)

    async def _run_workflow_guarded(self) -> None:
        """Wrapper that catches _WorkflowYield so it never escapes trio.run().

        Letting a BaseException propagate out of trio.run()'s main task can
        prevent Trio from closing its internal wakeup socket pair, causing a
        ResourceWarning for an unclosed file descriptor.
        """
        try:
            await self._run_workflow()
        except _WorkflowYield:
            # Expected – workflow needs to wait for an external event.
            # Swallowed here so trio.run() can exit cleanly.
            pass

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

        # Process pending updates before running the main workflow
        for update_job in self._pending_updates:
            await self._apply_update(update_job)
        self._pending_updates.clear()

        try:
            result = await self._defn.run_fn(self._workflow_obj, *self._start_args)
            self._commands.append(CompleteWorkflowCommand(result=result))
        except _WorkflowYield:
            # Re-raise to _run_workflow_guarded which swallows it
            raise
        except _ContinueAsNewError as err:
            # Workflow requested continue as new - emit command
            logger.debug("Workflow requested continue as new")
            err._apply_command()
        except _WorkflowCancelled:
            # Workflow was cancelled - emit cancel command
            self._commands.append(CancelWorkflowCommand())
        except Exception as e:
            self._commands.append(FailWorkflowCommand(exception=e))

    # Guest mode methods (Phase 1)

    def _schedule_sync(self, fn: Callable[[], object]) -> None:
        """Callback scheduler for guest mode.

        Called by Trio guest run via `run_sync_soon_threadsafe` to schedule
        callbacks on the host. These callbacks are processed synchronously
        during `_drive_guest_run()`.

        Args:
            fn: The callback function to schedule.
        """
        self._pending_callbacks.append(fn)

    def _on_workflow_done(self, main_outcome: outcome.Outcome) -> None:
        """Done callback for guest mode.

        Called when the Trio guest run completes (either successfully or
        with an exception). Stores the outcome and marks guest as no longer
        running.

        Args:
            main_outcome: The outcome of the guest run (Value or Error).
        """
        self._workflow_outcome = main_outcome
        self._guest_running = False

    def _start_guest_run(self) -> None:
        """Start the Trio guest run for this workflow.

        This initializes the guest mode by calling `trio.lowlevel.start_guest_run()`
        with the workflow's main coroutine. The guest run will execute until it
        suspends waiting for events or completes.

        Uses deterministic scheduling and a seeded random number generator
        to ensure replay consistency.
        """
        self._guest_running = True

        # Create the workflow clock for time control and store for later updates
        self._guest_clock = WorkflowClock(self._time_ns)

        # Start the guest run
        trio.lowlevel.start_guest_run(
            self._run_workflow,
            run_sync_soon_threadsafe=self._schedule_sync,
            done_callback=self._on_workflow_done,
            deterministic=True,  # type: ignore[call-arg]
            random_seed=self._random.getrandbits(64),  # type: ignore[call-arg]
            clock=self._guest_clock,
        )

    def _drive_guest_run(self) -> None:
        """Process pending callbacks from the Trio guest run.

        This method runs the guest run until it suspends waiting for events.
        It processes callbacks scheduled by `_schedule_sync()` in a loop
        until no more callbacks are pending.

        Note: The guest run may complete during callback processing,
        which will be signaled via `_on_workflow_done()`.
        """
        # Process any pending callbacks from Trio
        while self._pending_callbacks:
            callbacks = self._pending_callbacks
            self._pending_callbacks = []
            for cb in callbacks:
                cb()

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

    async def _apply_update(self, job: UpdateWorkflowJob) -> None:
        """Apply an update to the workflow.

        This handles the update protocol:
        1. Look up handler by name (or dynamic)
        2. Run validator if requested
        3. Accept the update
        4. Run the handler
        5. Emit completed/rejected response

        Args:
            job: The update job containing update info and args.
        """
        # Set current update info
        _set_current_update_info(UpdateInfo(id=job.id, name=job.name))

        defn = self._updates.get(job.name) or self._updates.get(None)
        if defn is None:
            known = sorted([k for k in self._updates.keys() if k])
            self._commands.append(
                UpdateResponseCommand(
                    protocol_instance_id=job.protocol_instance_id,
                    rejected_failure=RuntimeError(
                        f"Update handler for '{job.name}' expected but not found, "
                        f"and there is no dynamic handler. "
                        f"known updates: [{' '.join(known)}]"
                    ),
                )
            )
            return

        handler = defn.fn
        if defn.is_method:
            handler = handler.__get__(self._workflow_obj, type(self._workflow_obj))

        # Track in-progress
        self._in_progress_updates[job.id] = job.name

        try:
            # Run validator if requested
            if job.run_validator and defn.validator is not None:
                validator = defn.validator
                if defn.is_method:
                    validator = validator.__get__(
                        self._workflow_obj, type(self._workflow_obj)
                    )
                validator(*job.args)

            # Accept the update
            self._commands.append(
                UpdateResponseCommand(
                    protocol_instance_id=job.protocol_instance_id,
                    accepted=True,
                )
            )

            # Run the handler
            result = handler(*job.args)
            if inspect.iscoroutine(result):
                result = await result

            # Emit completed response
            self._commands.append(
                UpdateResponseCommand(
                    protocol_instance_id=job.protocol_instance_id,
                    completed_result=result,
                    _is_completed=True,
                )
            )
        except Exception as e:
            # Check if we already accepted (past validation)
            # If not accepted yet, this is a validator rejection
            self._commands.append(
                UpdateResponseCommand(
                    protocol_instance_id=job.protocol_instance_id,
                    rejected_failure=e,
                )
            )
        finally:
            self._in_progress_updates.pop(job.id, None)

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
        return Info(
            workflow_id=self._info.workflow_id,
            workflow_type=self._info.workflow_type,
            run_id=self._info.run_id,
            task_queue=self._info.task_queue,
            namespace=self._info.namespace,
            attempt=self._info.attempt,
            start_time=self._info.start_time,
            headers=self._headers,
            execution_timeout=self._info.execution_timeout,
            run_timeout=self._info.run_timeout,
            task_timeout=self._info.task_timeout,
            retry_policy=self._info.retry_policy,
            continued_run_id=self._info.continued_run_id,
            cron_schedule=self._info.cron_schedule,
            parent=self._info.parent,
            root=self._info.root,
            raw_memo=self._info.raw_memo,
            priority=self._info.priority,
        )

    def workflow_random(self) -> random.Random:
        """Get the deterministic random number generator for this workflow.

        Returns a seeded random.Random instance that produces deterministic
        results across workflow replays. The seed was provided when the
        workflow instance was created.

        Returns:
            A seeded random.Random instance.
        """
        return self._random

    def workflow_patch(self, patch_id: str, *, deprecated: bool = False) -> bool:
        """Check if a patch should be applied.

        This implements the patching logic for safe workflow code evolution:
        - If replaying and the patch is in history (notified), return True
        - If replaying and the patch is NOT in history, return False
        - If not replaying (new execution), emit a SetPatchMarkerCommand and return True

        Results are memoized so subsequent calls with the same patch_id return
        the same value without emitting additional commands.

        Args:
            patch_id: Unique identifier for this patch point.
            deprecated: If True, marks the patch as deprecated.

        Returns:
            True if the new code path should be taken, False if the old path
            should be taken (only during replay without the patch marker).
        """
        # Check if already memoized
        if patch_id in self._patches_memoized:
            return self._patches_memoized[patch_id]

        # Determine the result based on replay state
        if self._is_replaying:
            # During replay, check if patch was recorded in history
            result = patch_id in self._patches_notified
        else:
            # New execution - take the new path and record marker
            result = True
            self._commands.append(
                SetPatchMarkerCommand(patch_id=patch_id, deprecated=deprecated)
            )

        # Memoize and return
        self._patches_memoized[patch_id] = result
        return result

    def workflow_upsert_search_attributes(
        self,
        attributes: Sequence[temporalio.common.SearchAttributeUpdate],
    ) -> None:
        """Upsert search attributes for this workflow.

        This adds an UpsertSearchAttributesCommand to update the workflow's
        search attributes. Search attributes are used for workflow visibility
        and querying in the Temporal UI and CLI.

        This is a one-way command - it does not wait for a response or yield.
        The command is recorded in history during replay but doesn't produce
        a response job.

        Args:
            attributes: Sequence of typed search attribute updates.

        Note:
            - Search attributes are eventually consistent
            - Custom attributes must be registered with the Temporal server
            - This command does not block workflow execution
        """
        cmd = UpsertSearchAttributesCommand(search_attributes=attributes)
        self._commands.append(cmd)

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
        cancellation_type: int = 0,
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
                cancellation_type=int(cancellation_type),
            )
        )
        self._pending_activity_seq = seq

        # Yield control - workflow will re-run when activity resolves
        raise _WorkflowYield()

    async def workflow_execute_local_activity(
        self,
        activity: str | Callable[..., Any],
        *args: Any,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        local_retry_threshold: timedelta | None = None,
        activity_id: str | None = None,
        cancellation_type: int = 0,
    ) -> Any:
        """Execute a local activity and wait for its result.

        If this activity has already resolved (during replay), returns immediately.
        Otherwise, creates a schedule local activity command and yields back to the
        activation loop.

        Args:
            activity: Activity name or function reference.
            *args: Arguments to pass to the activity.
            schedule_to_close_timeout: Max time for activity from schedule to completion.
            schedule_to_start_timeout: Max time waiting for worker to pick up activity.
            start_to_close_timeout: Max time for activity execution.
            retry_policy: Retry policy for the activity.
            local_retry_threshold: Duration after which retries use the server.
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
                "must be set for local activity execution"
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

        # Get sequence number for this activity (shares seq space with regular activities)
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
            ScheduleLocalActivityCommand(
                seq=seq,
                activity_id=activity_id,
                activity_type=activity_type,
                args=tuple(args),
                schedule_to_close_timeout=schedule_to_close_timeout,
                schedule_to_start_timeout=schedule_to_start_timeout,
                start_to_close_timeout=start_to_close_timeout,
                retry_policy=retry_policy,
                local_retry_threshold=local_retry_threshold,
                cancellation_type=int(cancellation_type),
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
        result_type: type | None = None,
        cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
        parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: temporalio.common.SearchAttributes
        | temporalio.common.TypedSearchAttributes
        | None = None,
        versioning_intent: Any = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
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
            cron_schedule: Optional cron schedule string.
            memo: Optional memo key-value pairs.
            search_attributes: Optional search attributes.

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
            if defn is not None and defn.name is not None:
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

        # Pre-encode args, memo, summary, details using default converter
        import temporalio.converter

        converter = temporalio.converter.DataConverter.default.payload_converter
        encoded_args = converter.to_payloads(list(args)) if args else []
        encoded_memo = None
        if memo:
            encoded_memo = {k: converter.to_payloads([v])[0] for k, v in memo.items()}
        encoded_summary = (
            converter.to_payload(static_summary) if static_summary else None
        )
        encoded_details = (
            converter.to_payload(static_details) if static_details else None
        )

        # Child workflow hasn't started yet - create command and yield
        self._commands.append(
            StartChildWorkflowCommand(
                seq=seq,
                workflow_id=id,
                workflow_type=workflow_type,
                args=encoded_args,
                task_queue=task_queue,
                execution_timeout=execution_timeout,
                run_timeout=run_timeout,
                task_timeout=task_timeout,
                parent_close_policy=parent_close_policy.value,
                cancellation_type=cancellation_type.value,
                retry_policy=retry_policy,
                id_reuse_policy=id_reuse_policy.value,
                cron_schedule=cron_schedule,
                encoded_memo=encoded_memo,
                search_attributes=search_attributes,
                static_summary_payload=encoded_summary,
                static_details_payload=encoded_details,
                priority=priority,
            )
        )
        self._pending_child_seq = seq

        # Yield control - workflow will re-run when child workflow starts/completes
        raise _WorkflowYield()

    async def workflow_wait_child_workflow(
        self,
        handle: ChildWorkflowHandle[Any, Any],
    ) -> Any:
        """Wait for a child workflow to complete and return its result.

        This method is called by ChildWorkflowHandle.result() to wait for
        the child workflow to finish executing.

        Args:
            handle: The child workflow handle to wait for.

        Returns:
            The result of the child workflow.

        Raises:
            The exception raised by the child workflow, if any.
        """
        # Get the sequence number from the handle
        seq = handle._seq

        # Check if this child workflow has already resolved (replay)
        if seq in self._resolved_child_workflows:
            result, failure = self._resolved_child_workflows[seq]
            if failure is not None:
                raise failure
            return result

        # Check for cancellation before waiting
        if self._cancel_requested:
            raise _WorkflowCancelled()

        # Child workflow hasn't completed yet - yield and wait
        self._pending_child_seq = seq
        raise _WorkflowYield()

    async def workflow_wait_condition(
        self,
        fn: Callable[[], bool],
        *,
        timeout: float | None = None,
        timeout_summary: str | None = None,
    ) -> None:
        """Wait until condition returns True or timeout expires.

        Replay model: Each activation re-runs, condition is checked inline.

        Args:
            fn: A callable returning True when condition is met.
            timeout: Optional maximum wait time in seconds.
            timeout_summary: Optional description for Temporal UI.

        Raises:
            TimeoutError: If timeout expires before condition becomes true.
        """
        cond_seq = self._condition_seq
        self._condition_seq += 1

        # Determine timer_id deterministically (same on every activation)
        timer_id: int | None = None
        if timeout is not None:
            timer_id = self._timer_seq
            self._timer_seq += 1

        # Check if condition is satisfied now
        if fn():
            # Cancel timeout timer if exists and hasn't fired
            if timer_id is not None and timer_id not in self._fired_timers:
                self._commands.append(CancelTimerCommand(timer_id=timer_id))
            return

        # Check if timeout timer fired
        if timer_id is not None and timer_id in self._fired_timers:
            raise TimeoutError("Condition timed out")

        # Create timer command (Core SDK deduplicates by seq)
        if timer_id is not None:
            assert timeout is not None  # timer_id is only set if timeout is set
            self._commands.append(
                StartTimerCommand(
                    timer_id=timer_id,
                    duration_ms=int(timeout * 1000),
                    summary=timeout_summary,
                )
            )

        # Check for cancellation
        if self._cancel_requested:
            raise _WorkflowCancelled()

        raise _WorkflowYield()

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

        This method never returns - it raises _ContinueAsNewError to stop the
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
            _ContinueAsNewError: Always raised to stop the workflow.
        """
        # Determine workflow type
        if workflow is None:
            # Same workflow type as current
            workflow_type = self._defn.name or self._defn.cls.__name__
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

        raise _ContinueAsNewError(
            instance=self,
            workflow_type=workflow_type,
            args=args,
            task_queue=task_queue,
            run_timeout=run_timeout,
            task_timeout=task_timeout,
            retry_policy=retry_policy,
            memo=memo,
            search_attributes=search_attributes,
        )

    def workflow_get_external_workflow_handle(
        self,
        workflow_id: str,
        *,
        run_id: str | None,
    ) -> ExternalWorkflowHandle[Any]:
        """Get a handle to an external workflow.

        Args:
            workflow_id: ID of the external workflow.
            run_id: Optional run ID to target a specific run.

        Returns:
            Handle to the external workflow.
        """
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
        # Check if cancellation requested before starting new operations
        if self._cancel_requested:
            raise _WorkflowCancelled("Workflow cancelled")

        # Increment sequence number
        self._signal_external_seq += 1
        seq = self._signal_external_seq

        # Check if we already have the result from a previous replay
        if seq in self._resolved_external_signals:
            failure = self._resolved_external_signals[seq]
            if failure is not None:
                raise failure
            return

        # Pre-encode signal args
        import temporalio.converter

        converter = temporalio.converter.DataConverter.default.payload_converter
        encoded_args = converter.to_payloads(list(args)) if args else []

        # Add the command to signal the external workflow
        cmd = SignalExternalWorkflowCommand(
            seq=seq,
            workflow_id=workflow_id,
            signal_name=signal_name,
            run_id=run_id,
            args=encoded_args,
        )
        self._commands.append(cmd)

        # Mark this sequence as pending and yield to wait for resolution
        self._pending_external_signal_seq = seq
        raise _WorkflowYield()

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
        # Check if cancellation requested before starting new operations
        if self._cancel_requested:
            raise _WorkflowCancelled("Workflow cancelled")

        # Increment sequence number
        self._cancel_external_seq += 1
        seq = self._cancel_external_seq

        # Check if we already have the result from a previous replay
        if seq in self._resolved_external_cancels:
            failure = self._resolved_external_cancels[seq]
            if failure is not None:
                raise failure
            return

        # Add the command to cancel the external workflow
        cmd = RequestCancelExternalWorkflowCommand(
            seq=seq,
            workflow_id=workflow_id,
            run_id=run_id,
        )
        self._commands.append(cmd)

        # Mark this sequence as pending and yield to wait for resolution
        self._pending_external_cancel_seq = seq
        raise _WorkflowYield()

    def workflow_memo(self) -> Mapping[str, Any]:
        """Get the current workflow's memo values, converted to Python values.

        Decodes each Payload in the raw_memo using the default payload converter.

        Returns:
            Mapping of memo keys to their decoded values.
        """
        if self._memo_cache is None:
            import temporalio.converter

            converter = temporalio.converter.DataConverter.default.payload_converter
            self._memo_cache = {
                k: converter.from_payloads([v])[0]
                for k, v in self._info.raw_memo.items()
            }
        return self._memo_cache

    def workflow_payload_converter(
        self,
    ) -> temporalio.converter.PayloadConverter:
        """Get the payload converter for the current workflow.

        Returns:
            The default payload converter.
        """
        import temporalio.converter

        return temporalio.converter.DataConverter.default.payload_converter

    def workflow_instance(self) -> Any:
        """Get the current workflow instance object.

        Returns:
            The workflow object instance.
        """
        return self._workflow_obj

    def workflow_get_current_details(self) -> str:
        """Get the current workflow details string.

        Returns:
            The current details string.
        """
        return self._current_details

    def workflow_set_current_details(self, details: str) -> None:
        """Set the current workflow details string.

        Args:
            details: The details string to set.
        """
        self._current_details = details

    def workflow_get_current_build_id(self) -> str | None:
        """Get the current worker's build ID, if any.

        Returns:
            None - build ID tracking is not yet implemented.
        """
        return None

    def workflow_get_current_history_length(self) -> int:
        """Get the current number of events in the workflow's history.

        Returns:
            0 - would need activation tracking to implement.
        """
        return 0

    def workflow_get_current_history_size(self) -> int:
        """Get the current size of the workflow's history in bytes.

        Returns:
            0 - would need activation tracking to implement.
        """
        return 0

    def workflow_is_continue_as_new_suggested(self) -> bool:
        """Whether it's suggested to continue as new.

        Returns:
            False - not yet implemented.
        """
        return False

    def workflow_all_handlers_finished(self) -> bool:
        """Whether all update and signal handlers have finished executing.

        Returns:
            True if there are no in-progress update or signal handler executions.
        """
        return len(self._in_progress_updates) == 0

    @property
    def is_replaying(self) -> bool:
        """Whether the current activation is replaying from history."""
        return self._is_replaying
