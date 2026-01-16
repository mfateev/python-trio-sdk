"""Workflow instance implementation for Temporal with Trio.

This module contains the core classes for creating and managing workflow instances.
"""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import trio

from temporalio_trio.worker._activation import (
    CompleteWorkflowCommand,
    FailWorkflowCommand,
    StartTimerCommand,
    TimerFiredJob,
    WorkflowActivation,
    WorkflowActivationCompletion,
    WorkflowStartedJob,
)
from temporalio_trio.worker._clock import WorkflowClock
from temporalio_trio.workflow import Info, _Definition, _Runtime

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
        self._workflow_obj: object | None = None
        self._fired_timers: set[int] = set()
        self._pending_timer_id: int | None = None
        self._commands: list[
            StartTimerCommand | CompleteWorkflowCommand | FailWorkflowCommand
        ] = []
        self._start_args: tuple[Any, ...] | None = None

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

        # If we have start args (either new or from previous activation), run workflow
        if self._start_args is not None:
            # Reset timer sequence for deterministic replay
            self._timer_seq = 0

            # Set runtime context
            token = _Runtime.set_current(self)
            try:
                # Create clock at current workflow time
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

        return WorkflowActivationCompletion(commands=self._commands)

    async def _run_workflow(self) -> None:
        """Run the workflow from the beginning.

        This re-executes the workflow, with fired timers completing immediately.
        """
        assert self._start_args is not None
        self._workflow_obj = self._defn.cls()
        try:
            result = await self._defn.run_fn(self._workflow_obj, *self._start_args)
            self._commands.append(CompleteWorkflowCommand(result=result))
        except _WorkflowYield:
            # Re-raise to propagate yield signal
            raise
        except Exception as e:
            self._commands.append(FailWorkflowCommand(exception=e))

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
            summary: Optional description for debugging (currently unused).
        """
        timer_id = self._timer_seq
        self._timer_seq += 1

        # Check if this timer has already fired (replay)
        if timer_id in self._fired_timers:
            # Timer already fired, continue immediately
            return

        # Timer hasn't fired yet - create command and yield
        self._commands.append(
            StartTimerCommand(
                timer_id=timer_id,
                duration_ms=int(duration * 1000),
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
