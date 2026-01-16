"""Workflow instance implementation for Temporal with Trio.

This module contains the core classes for creating and managing workflow instances.
"""

from __future__ import annotations

import random
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from temporalio_trio.workflow import Info, _Definition, _Runtime

# Type aliases for activation types that will be defined in Phase 3.
# These match the structure from temporalio.bridge.proto but are placeholders
# until we implement the actual activation handling.
WorkflowActivation = Any
"""Placeholder type for workflow activation (implemented in Phase 3)."""

WorkflowActivationCompletion = Any
"""Placeholder type for workflow activation completion (implemented in Phase 3)."""

__all__ = [
    "WorkflowInstanceDetails",
    "WorkflowInstance",
    "TrioWorkflowInstance",
]


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

    This is a skeleton implementation for Phase 2. Full activation logic
    will be implemented in Phase 3.

    Attributes:
        _defn: The workflow definition.
        _info: Information about the workflow execution.
        _random: Seeded random number generator for determinism.
        _time_ns: Current workflow time in nanoseconds.
        _timer_seq: Sequence counter for timer IDs.
        _workflow_obj: The instantiated workflow class object.
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

        This is a skeleton implementation. Full activation logic with Trio
        execution will be implemented in Phase 3.

        Args:
            act: The activation containing jobs to process.

        Returns:
            Completion containing commands to execute.

        Raises:
            NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError(
            "TrioWorkflowInstance.activate() will be implemented in Phase 3"
        )

    # _Runtime implementation

    def workflow_time_ns(self) -> int:
        """Get current workflow time in nanoseconds.

        Returns:
            Current workflow time in nanoseconds since epoch.
        """
        return self._time_ns

    async def workflow_sleep(self, duration: float, summary: str | None) -> None:
        """Sleep for the given duration.

        This is a skeleton implementation. Full sleep logic with timer commands
        will be implemented in Phase 3.

        Args:
            duration: Sleep duration in seconds.
            summary: Optional description for debugging.

        Raises:
            NotImplementedError: This method is not yet implemented.
        """
        raise NotImplementedError(
            "TrioWorkflowInstance.workflow_sleep() will be implemented in Phase 3"
        )
