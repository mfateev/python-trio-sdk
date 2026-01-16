"""Workflow activation and completion types for Temporal with Trio.

This module defines the data structures for workflow activations (jobs from the
Temporal server) and completions (commands sent back to the server).

These are simplified versions of the protocol buffer types used in the SDK,
designed for the POC to demonstrate the activation/completion pattern.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

__all__ = [
    "WorkflowStartedJob",
    "TimerFiredJob",
    "WorkflowActivation",
    "StartTimerCommand",
    "CompleteWorkflowCommand",
    "FailWorkflowCommand",
    "WorkflowActivationCompletion",
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


# Type alias for all job types
WorkflowJob = WorkflowStartedJob | TimerFiredJob
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

    jobs: list[WorkflowStartedJob | TimerFiredJob]
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


# Type alias for all command types
WorkflowCommand = StartTimerCommand | CompleteWorkflowCommand | FailWorkflowCommand
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

    commands: list[StartTimerCommand | CompleteWorkflowCommand | FailWorkflowCommand]
    """List of commands to send to the server."""
