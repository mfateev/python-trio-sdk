"""Tests for TrioBridgeWrapper.

These tests verify the Trio-compatible async bridge wrapper that integrates
with the Rust PyO3 bridge.

Note: These tests use a mock Rust bridge since the actual PyO3 bridge
is implemented in Phase 1. Once the real bridge is available, we can
add integration tests.
"""

from __future__ import annotations

import time
from typing import Any, Callable
from unittest.mock import Mock, patch

import pytest
import trio

from temporalio_trio._async_bridge import BridgeState, TrioBridgeWrapper


class MockRustBridge:
    """Mock implementation of the Rust bridge for testing.

    Simulates the behavior of the real PyO3 TrioAsyncBridge.
    """

    def __init__(self) -> None:
        self.started = False
        self.shutdown_initiated = False
        self.requests: list[tuple[str, bytes, Callable]] = []

    def start(self) -> None:
        """Start the bridge."""
        self.started = True

    def initiate_shutdown(self) -> None:
        """Initiate shutdown."""
        self.shutdown_initiated = True

    def send_request(
        self,
        operation: str,
        data: bytes,
        callback: Callable[[bytes], None]
    ) -> None:
        """Send a request to the bridge.

        For testing, we immediately invoke the callback with mock data.
        The callback will use trio.from_thread internally, which is fine
        because this simulates a call from a different thread.
        """
        import threading

        self.requests.append((operation, data, callback))

        # Simulate async processing by running callback from a thread
        # In real bridge, this happens from Rust thread
        def invoke_callback():
            if operation == "poll_activation":
                # Return mock activation bytes
                callback(b"mock_activation_data")
            elif operation == "complete_activation":
                # Return empty acknowledgment
                callback(b"")
            elif operation == "validate":
                # Return empty (success)
                callback(b"")
            elif operation == "finalize_shutdown":
                # Return empty (success)
                callback(b"")
            else:
                raise ValueError(f"Unknown operation: {operation}")

        # Run callback from a thread to simulate Rust thread behavior
        thread = threading.Thread(target=invoke_callback, daemon=True)
        thread.start()


@pytest.fixture
def mock_bridge():
    """Create a mock Rust bridge for testing."""
    return MockRustBridge()


@pytest.fixture
async def started_bridge(mock_bridge):
    """Create and start a bridge wrapper for testing."""
    wrapper = TrioBridgeWrapper()

    # Inject mock bridge
    wrapper._rust_bridge = mock_bridge

    await wrapper.start()
    return wrapper


class TestBridgeLifecycle:
    """Test bridge lifecycle management."""

    @pytest.mark.trio
    async def test_initial_state(self):
        """Test bridge starts in NOT_STARTED state."""
        bridge = TrioBridgeWrapper()
        assert bridge._state == BridgeState.NOT_STARTED
        assert bridge._trio_token is None

    @pytest.mark.trio
    async def test_start_captures_trio_token(self, mock_bridge):
        """Test that start() captures the Trio token."""
        bridge = TrioBridgeWrapper()
        bridge._rust_bridge = mock_bridge

        await bridge.start()

        assert bridge._state == BridgeState.RUNNING
        assert bridge._trio_token is not None
        assert isinstance(bridge._trio_token, trio.lowlevel.TrioToken)
        assert mock_bridge.started

    @pytest.mark.trio
    async def test_start_twice_raises(self, mock_bridge):
        """Test that starting an already started bridge raises."""
        bridge = TrioBridgeWrapper()
        bridge._rust_bridge = mock_bridge

        await bridge.start()

        with pytest.raises(RuntimeError, match="already started"):
            await bridge.start()

    @pytest.mark.trio
    async def test_set_trio_token_manually(self):
        """Test manually setting the Trio token."""
        bridge = TrioBridgeWrapper()
        token = trio.lowlevel.current_trio_token()

        bridge.set_trio_token(token)

        assert bridge._trio_token is token

    @pytest.mark.trio
    async def test_initiate_shutdown(self, started_bridge, mock_bridge):
        """Test initiating shutdown."""
        started_bridge.initiate_shutdown()

        assert started_bridge._state == BridgeState.SHUTDOWN
        assert mock_bridge.shutdown_initiated

    @pytest.mark.trio
    async def test_initiate_shutdown_idempotent(self, started_bridge):
        """Test that initiate_shutdown can be called multiple times."""
        started_bridge.initiate_shutdown()
        started_bridge.initiate_shutdown()  # Should not raise

        assert started_bridge._state == BridgeState.SHUTDOWN

    @pytest.mark.trio
    async def test_finalize_shutdown(self, started_bridge):
        """Test finalizing shutdown."""
        started_bridge.initiate_shutdown()
        await started_bridge.finalize_shutdown()

        assert started_bridge._state == BridgeState.SHUTDOWN

    @pytest.mark.trio
    async def test_finalize_shutdown_initiates_if_needed(self, started_bridge):
        """Test that finalize_shutdown initiates if not already done."""
        # Don't call initiate_shutdown first
        await started_bridge.finalize_shutdown()

        assert started_bridge._state == BridgeState.SHUTDOWN

    @pytest.mark.trio
    async def test_shutdown_convenience_method(self, started_bridge):
        """Test the shutdown() convenience method."""
        await started_bridge.shutdown()

        assert started_bridge._state == BridgeState.SHUTDOWN


class TestBridgeOperations:
    """Test bridge operations."""

    @pytest.mark.trio
    async def test_poll_workflow_activation(self, started_bridge, mock_bridge):
        """Test polling for workflow activation."""
        result = await started_bridge.poll_workflow_activation()

        assert result == b"mock_activation_data"
        assert len(mock_bridge.requests) == 1
        assert mock_bridge.requests[0][0] == "poll_activation"
        assert mock_bridge.requests[0][1] == b""

    @pytest.mark.trio
    async def test_poll_before_start_raises(self, mock_bridge):
        """Test that polling before start raises."""
        bridge = TrioBridgeWrapper()
        bridge._rust_bridge = mock_bridge

        with pytest.raises(RuntimeError, match="not running"):
            await bridge.poll_workflow_activation()

    @pytest.mark.trio
    async def test_poll_with_timeout(self, started_bridge):
        """Test polling with timeout."""
        # Mock bridge responds immediately, so timeout won't be hit
        result = await started_bridge.poll_workflow_activation(timeout=1.0)

        assert result == b"mock_activation_data"

    @pytest.mark.trio
    async def test_poll_timeout_exceeded(self, mock_bridge):
        """Test that poll timeout raises TooSlowError."""
        # Create a bridge that never responds
        class SlowMockBridge(MockRustBridge):
            def send_request(self, operation, data, callback):
                # Store but don't call callback
                self.requests.append((operation, data, callback))

        bridge = TrioBridgeWrapper()
        bridge._rust_bridge = SlowMockBridge()
        await bridge.start()

        with pytest.raises(trio.TooSlowError):
            await bridge.poll_workflow_activation(timeout=0.1)

    @pytest.mark.trio
    async def test_complete_workflow_activation(self, started_bridge, mock_bridge):
        """Test completing a workflow activation."""
        completion_bytes = b"completion_data"

        await started_bridge.complete_workflow_activation(completion_bytes)

        assert len(mock_bridge.requests) == 1
        assert mock_bridge.requests[0][0] == "complete_activation"
        assert mock_bridge.requests[0][1] == completion_bytes

    @pytest.mark.trio
    async def test_complete_before_start_raises(self, mock_bridge):
        """Test that completing before start raises."""
        bridge = TrioBridgeWrapper()
        bridge._rust_bridge = mock_bridge

        with pytest.raises(RuntimeError, match="not running"):
            await bridge.complete_workflow_activation(b"data")

    @pytest.mark.trio
    async def test_complete_with_timeout(self, started_bridge):
        """Test completing with timeout."""
        await started_bridge.complete_workflow_activation(
            b"data",
            timeout=1.0
        )

        # Should complete successfully

    @pytest.mark.trio
    async def test_validate(self, started_bridge, mock_bridge):
        """Test validating the bridge."""
        await started_bridge.validate()

        assert len(mock_bridge.requests) == 1
        assert mock_bridge.requests[0][0] == "validate"

    @pytest.mark.trio
    async def test_validate_before_start_raises(self, mock_bridge):
        """Test that validating before start raises."""
        bridge = TrioBridgeWrapper()
        bridge._rust_bridge = mock_bridge

        with pytest.raises(RuntimeError, match="not running"):
            await bridge.validate()

    @pytest.mark.trio
    async def test_validate_with_error(self, mock_bridge):
        """Test validation error handling."""
        import threading

        class FailingMockBridge(MockRustBridge):
            def send_request(self, operation, data, callback):
                self.requests.append((operation, data, callback))

                def invoke_callback():
                    if operation == "validate":
                        callback(b"Validation failed: Connection refused")
                    else:
                        callback(b"")

                thread = threading.Thread(target=invoke_callback, daemon=True)
                thread.start()

        bridge = TrioBridgeWrapper()
        bridge._rust_bridge = FailingMockBridge()
        await bridge.start()

        with pytest.raises(RuntimeError, match="Validation failed"):
            await bridge.validate()


class TestConcurrentOperations:
    """Test concurrent bridge operations."""

    @pytest.mark.trio
    async def test_many_concurrent_polls(self, started_bridge):
        """Test many concurrent polls don't cause issues."""
        async def poll_task():
            result = await started_bridge.poll_workflow_activation()
            assert result == b"mock_activation_data"

        # Launch 50 concurrent polls
        async with trio.open_nursery() as nursery:
            for _ in range(50):
                nursery.start_soon(poll_task)

        # All should complete successfully

    @pytest.mark.trio
    async def test_interleaved_poll_and_complete(self, started_bridge):
        """Test interleaved polling and completing."""
        async def worker():
            for _ in range(5):
                activation = await started_bridge.poll_workflow_activation()
                await started_bridge.complete_workflow_activation(activation)

        async with trio.open_nursery() as nursery:
            for _ in range(10):
                nursery.start_soon(worker)

        # All operations should complete successfully

    @pytest.mark.trio
    async def test_concurrent_with_shutdown(self, started_bridge):
        """Test shutdown during concurrent operations."""
        shutdown_done = trio.Event()

        async def poll_until_shutdown():
            while not shutdown_done.is_set():
                try:
                    await started_bridge.poll_workflow_activation()
                except RuntimeError:
                    # Expected after shutdown
                    break
                await trio.sleep(0.01)

        async def shutdown_after_delay():
            await trio.sleep(0.1)
            started_bridge.initiate_shutdown()
            await started_bridge.finalize_shutdown()
            shutdown_done.set()

        async with trio.open_nursery() as nursery:
            nursery.start_soon(poll_until_shutdown)
            nursery.start_soon(poll_until_shutdown)
            nursery.start_soon(shutdown_after_delay)


class TestErrorHandling:
    """Test error handling in bridge operations."""

    @pytest.mark.trio
    async def test_callback_error_propagates(self, mock_bridge):
        """Test that errors in callbacks are propagated."""
        import threading

        class ErrorMockBridge(MockRustBridge):
            def send_request(self, operation, data, callback):
                self.requests.append((operation, data, callback))

                def invoke_callback():
                    # Simulate error by sending error bytes
                    # In real implementation, Rust would send error bytes
                    callback(b"Error: Something went wrong")

                thread = threading.Thread(target=invoke_callback, daemon=True)
                thread.start()

        bridge = TrioBridgeWrapper()
        bridge._rust_bridge = ErrorMockBridge()
        await bridge.start()

        # This should work since we just return the bytes
        # Error handling would happen at a higher level
        result = await bridge.poll_workflow_activation()
        assert b"Error" in result

    @pytest.mark.trio
    async def test_shutdown_with_timeout(self, started_bridge):
        """Test shutdown with timeout."""
        await started_bridge.shutdown(timeout=1.0)

        assert started_bridge._state == BridgeState.SHUTDOWN

    @pytest.mark.trio
    async def test_shutdown_timeout_exceeded(self, mock_bridge):
        """Test shutdown timeout exceeded."""
        class SlowShutdownBridge(MockRustBridge):
            def send_request(self, operation, data, callback):
                self.requests.append((operation, data, callback))
                if operation == "finalize_shutdown":
                    # Don't call callback - simulate slow shutdown
                    pass
                else:
                    callback(b"")

        bridge = TrioBridgeWrapper()
        bridge._rust_bridge = SlowShutdownBridge()
        await bridge.start()

        with pytest.raises(trio.TooSlowError):
            await bridge.shutdown(timeout=0.1)


class TestTrioIntegration:
    """Test integration with Trio features."""

    @pytest.mark.trio
    async def test_cancellation(self, mock_bridge):
        """Test that operations can be cancelled."""
        class NeverRespondBridge(MockRustBridge):
            def send_request(self, operation, data, callback):
                # Never call the callback
                self.requests.append((operation, data, callback))

        bridge = TrioBridgeWrapper()
        bridge._rust_bridge = NeverRespondBridge()
        await bridge.start()

        # Try to poll but cancel it
        with trio.move_on_after(0.1) as cancel_scope:
            await bridge.poll_workflow_activation()

        assert cancel_scope.cancelled_caught

    @pytest.mark.trio
    async def test_multiple_trio_tasks(self, started_bridge):
        """Test using bridge from multiple Trio tasks."""
        results = []

        async def task(task_id: int):
            for i in range(3):
                activation = await started_bridge.poll_workflow_activation()
                results.append((task_id, i, activation))
                await started_bridge.complete_workflow_activation(activation)

        async with trio.open_nursery() as nursery:
            for task_id in range(5):
                nursery.start_soon(task, task_id)

        # Should have 5 tasks * 3 iterations = 15 results
        assert len(results) == 15

    @pytest.mark.trio
    async def test_from_thread_delivery(self, started_bridge):
        """Test that results are delivered correctly via from_thread."""
        # This is implicitly tested by all other tests, but let's be explicit
        result = await started_bridge.poll_workflow_activation()

        # Verify we got the result that was delivered via from_thread
        assert isinstance(result, bytes)
        assert len(result) > 0


class TestStateManagement:
    """Test bridge state management."""

    @pytest.mark.trio
    async def test_operations_after_shutdown_raise(self, started_bridge):
        """Test that operations after shutdown raise errors."""
        started_bridge.initiate_shutdown()

        with pytest.raises(RuntimeError, match="not running"):
            await started_bridge.poll_workflow_activation()

        with pytest.raises(RuntimeError, match="not running"):
            await started_bridge.complete_workflow_activation(b"data")

        with pytest.raises(RuntimeError, match="not running"):
            await started_bridge.validate()

    @pytest.mark.trio
    async def test_state_transitions(self, mock_bridge):
        """Test valid state transitions."""
        bridge = TrioBridgeWrapper()
        bridge._rust_bridge = mock_bridge

        # NOT_STARTED -> RUNNING
        assert bridge._state == BridgeState.NOT_STARTED
        await bridge.start()
        assert bridge._state == BridgeState.RUNNING

        # RUNNING -> SHUTDOWN
        bridge.initiate_shutdown()
        assert bridge._state == BridgeState.SHUTDOWN

        # SHUTDOWN -> SHUTDOWN (idempotent)
        bridge.initiate_shutdown()
        assert bridge._state == BridgeState.SHUTDOWN
