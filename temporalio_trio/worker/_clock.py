"""Workflow clock implementation for Temporal with Trio.

This module provides a clock implementation that is controlled by workflow
execution. Time only advances when explicitly set via the advance_to() method,
which happens when processing activation timestamps.
"""

from __future__ import annotations

from trio.abc import Clock

__all__ = [
    "WorkflowClock",
]


class WorkflowClock(Clock):
    """Clock controlled by workflow execution.

    Time only advances via advance_to(), which is called when processing
    activations from the Temporal server. This ensures deterministic execution
    during replay.

    This implements trio.abc.Clock and can be passed to trio.run(clock=...).

    Attributes:
        _current_time_ns: Current workflow time in nanoseconds.
    """

    __slots__ = ("_current_time_ns",)

    def __init__(self, start_time_ns: int = 0) -> None:
        """Initialize the workflow clock.

        Args:
            start_time_ns: Initial time in nanoseconds. Defaults to 0.
        """
        self._current_time_ns = start_time_ns

    def start_clock(self) -> None:
        """Called at the beginning of trio.run().

        No setup is needed for workflow clocks - time is controlled
        externally via advance_to().
        """
        pass

    def current_time(self) -> float:
        """Return the current time in seconds.

        Returns:
            Current workflow time in seconds.
        """
        return self._current_time_ns / 1e9

    def deadline_to_sleep_time(self, deadline: float) -> float:
        """Compute real seconds to sleep until deadline.

        For workflow clocks, if the deadline has passed, return 0.
        If deadline is inf, return inf (Trio uses this for "no deadline").
        Otherwise, raise an error because this indicates unauthorized
        use of trio.sleep() inside a workflow.

        Note: workflow.sleep() uses a custom _WorkflowYield mechanism
        and never goes through Trio's deadline system. So any call to
        this method with a finite future deadline means trio.sleep() was used.

        Args:
            deadline: The absolute deadline time in seconds.

        Returns:
            0.0 if deadline already passed.
            float('inf') if deadline is inf (no deadline).

        Raises:
            RuntimeError: If deadline is a finite future time, indicating
                trio.sleep() was used instead of workflow.sleep().
        """
        if deadline <= self.current_time():
            return 0.0
        # Trio uses inf to mean "no deadline" - this is fine
        if deadline == float("inf"):
            return float("inf")
        raise RuntimeError(
            "trio.sleep() cannot be used inside a workflow. "
            "Use workflow.sleep() instead for deterministic execution."
        )

    def advance_to(self, time_ns: int) -> None:
        """Advance the clock to the given time.

        Time can only move forward. Attempting to move time backwards
        raises ValueError.

        Args:
            time_ns: The new time in nanoseconds.

        Raises:
            ValueError: If time_ns is less than current time.
        """
        if time_ns < self._current_time_ns:
            raise ValueError(
                f"Cannot move time backwards: current={self._current_time_ns}, "
                f"requested={time_ns}"
            )
        self._current_time_ns = time_ns

    @property
    def current_time_ns(self) -> int:
        """Get current time in nanoseconds.

        Returns:
            Current workflow time in nanoseconds.
        """
        return self._current_time_ns
