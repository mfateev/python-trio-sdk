"""Tests for WorkflowClock (Phase 3)."""

import pytest
from trio.abc import Clock

from temporalio_trio.worker import WorkflowClock


class TestWorkflowClockCreation:
    """Tests for WorkflowClock initialization."""

    def test_default_start_time(self) -> None:
        """Test WorkflowClock starts at time 0 by default."""
        clock = WorkflowClock()
        assert clock.current_time() == 0.0
        assert clock.current_time_ns == 0

    def test_custom_start_time(self) -> None:
        """Test WorkflowClock can start at custom time."""
        clock = WorkflowClock(start_time_ns=5_000_000_000)  # 5 seconds
        assert clock.current_time() == 5.0
        assert clock.current_time_ns == 5_000_000_000

    def test_implements_clock_interface(self) -> None:
        """Test WorkflowClock implements trio.abc.Clock."""
        clock = WorkflowClock()
        assert isinstance(clock, Clock)


class TestWorkflowClockCurrentTime:
    """Tests for current_time() method."""

    def test_current_time_returns_seconds(self) -> None:
        """Test current_time() returns time in seconds."""
        clock = WorkflowClock(start_time_ns=1_500_000_000)  # 1.5 seconds
        assert clock.current_time() == 1.5

    def test_current_time_sub_second_precision(self) -> None:
        """Test current_time() handles sub-second precision."""
        clock = WorkflowClock(start_time_ns=123_456_789)
        # Should be approximately 0.123456789 seconds
        assert abs(clock.current_time() - 0.123456789) < 1e-10

    def test_current_time_large_values(self) -> None:
        """Test current_time() handles large time values."""
        # Roughly year 2033 in nanoseconds
        large_ns = 2_000_000_000_000_000_000
        clock = WorkflowClock(start_time_ns=large_ns)
        assert clock.current_time() == 2_000_000_000.0


class TestWorkflowClockAdvanceTo:
    """Tests for advance_to() method."""

    def test_advance_to_future_time(self) -> None:
        """Test advance_to() can move time forward."""
        clock = WorkflowClock(start_time_ns=1_000_000_000)
        clock.advance_to(2_000_000_000)
        assert clock.current_time_ns == 2_000_000_000
        assert clock.current_time() == 2.0

    def test_advance_to_same_time(self) -> None:
        """Test advance_to() can set same time (no-op)."""
        clock = WorkflowClock(start_time_ns=1_000_000_000)
        clock.advance_to(1_000_000_000)
        assert clock.current_time_ns == 1_000_000_000

    def test_advance_to_past_raises(self) -> None:
        """Test advance_to() raises when moving backwards."""
        clock = WorkflowClock(start_time_ns=2_000_000_000)
        with pytest.raises(ValueError, match="Cannot move time backwards"):
            clock.advance_to(1_000_000_000)

    def test_advance_to_multiple_times(self) -> None:
        """Test advance_to() can be called multiple times."""
        clock = WorkflowClock()
        clock.advance_to(1_000_000_000)
        assert clock.current_time() == 1.0
        clock.advance_to(2_000_000_000)
        assert clock.current_time() == 2.0
        clock.advance_to(5_000_000_000)
        assert clock.current_time() == 5.0


class TestWorkflowClockStartClock:
    """Tests for start_clock() method."""

    def test_start_clock_is_noop(self) -> None:
        """Test start_clock() does nothing (time is externally controlled)."""
        clock = WorkflowClock(start_time_ns=1_000_000_000)
        clock.start_clock()
        # Time should not change
        assert clock.current_time_ns == 1_000_000_000

    def test_start_clock_can_be_called_multiple_times(self) -> None:
        """Test start_clock() can be called multiple times."""
        clock = WorkflowClock()
        clock.start_clock()
        clock.start_clock()
        clock.start_clock()
        # Should not raise


class TestWorkflowClockDeadlineToSleepTime:
    """Tests for deadline_to_sleep_time() method."""

    def test_deadline_in_past_returns_zero(self) -> None:
        """Test deadline_to_sleep_time() returns 0 for past deadlines."""
        clock = WorkflowClock(start_time_ns=5_000_000_000)  # 5 seconds
        # Deadline at 3 seconds is in the past
        assert clock.deadline_to_sleep_time(3.0) == 0.0

    def test_deadline_at_current_time_returns_zero(self) -> None:
        """Test deadline_to_sleep_time() returns 0 for current time."""
        clock = WorkflowClock(start_time_ns=5_000_000_000)  # 5 seconds
        assert clock.deadline_to_sleep_time(5.0) == 0.0

    def test_deadline_in_future_returns_inf(self) -> None:
        """Test deadline_to_sleep_time() returns inf for future deadlines.

        For workflow clocks, we never actually sleep - we wait for external
        events (timer fired activations) instead.
        """
        clock = WorkflowClock(start_time_ns=1_000_000_000)  # 1 second
        # Deadline at 5 seconds is in the future
        assert clock.deadline_to_sleep_time(5.0) == float("inf")

    def test_deadline_just_after_current_returns_inf(self) -> None:
        """Test deadline_to_sleep_time() returns inf for deadline just after current."""
        clock = WorkflowClock(start_time_ns=1_000_000_000)  # 1 second
        # Deadline at 1.001 seconds is just after current
        assert clock.deadline_to_sleep_time(1.001) == float("inf")


class TestWorkflowClockIntegration:
    """Integration tests for WorkflowClock behavior."""

    def test_clock_time_flow(self) -> None:
        """Test typical clock usage flow in workflow execution."""
        # Initial activation at t=0
        clock = WorkflowClock(start_time_ns=0)
        assert clock.current_time() == 0.0

        # Timer for 5 seconds requested, deadline check
        assert clock.deadline_to_sleep_time(5.0) == float("inf")

        # Timer fired activation at t=5s
        clock.advance_to(5_000_000_000)
        assert clock.current_time() == 5.0

        # Now the deadline has passed
        assert clock.deadline_to_sleep_time(5.0) == 0.0

        # Next timer for 10 more seconds (deadline at 15s)
        assert clock.deadline_to_sleep_time(15.0) == float("inf")

        # Timer fired at t=15s
        clock.advance_to(15_000_000_000)
        assert clock.deadline_to_sleep_time(15.0) == 0.0

    def test_multiple_clocks_isolated(self) -> None:
        """Test multiple clocks have isolated state."""
        clock1 = WorkflowClock(start_time_ns=1_000_000_000)
        clock2 = WorkflowClock(start_time_ns=2_000_000_000)

        clock1.advance_to(5_000_000_000)

        assert clock1.current_time() == 5.0
        assert clock2.current_time() == 2.0  # Unchanged
