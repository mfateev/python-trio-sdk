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
        Otherwise, return infinity to indicate that we should wait for
        an external event (like a timer fired activation) rather than
        actually sleeping.

        Args:
            deadline: The absolute deadline time in seconds.

        Returns:
            0.0 if deadline already passed, float('inf') otherwise.
        """
        if deadline <= self.current_time():
            return 0.0
        return float("inf")

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
