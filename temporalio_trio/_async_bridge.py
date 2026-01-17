"""Trio-compatible wrapper for the async Rust bridge.

This module provides a Python wrapper around the PyO3 Rust bridge that integrates
seamlessly with Trio's async primitives. It eliminates the need for trio-asyncio by
using Trio Events for waiting and trio.from_thread callbacks for result delivery.

Architecture:
    - TrioAsyncBridge (Rust/PyO3): Single thread with Tokio runtime
    - TrioBridgeWrapper (Python/Trio): Async API using Trio primitives
    - Communication: Queue-based requests + callback-based results

Pattern:
    1. Trio task sends request to Rust bridge (non-blocking)
    2. Creates Trio Event to await result
    3. Rust processes async in Tokio runtime
    4. Rust delivers result via trio.from_thread.run_sync callback
    5. Trio task wakes up and returns result

This approach provides true async on both sides without blocking threads.
"""

from __future__ import annotations

import enum
from typing import Optional

import trio

# Import the Rust bridge (compiled separately, may not be available during type checking)
from temporalio_trio_bridge import TrioAsyncBridge  # type: ignore[attr-defined]


class BridgeState(enum.Enum):
    """Bridge lifecycle states."""

    NOT_STARTED = "not_started"
    RUNNING = "running"
    SHUTDOWN = "shutdown"


class TrioBridgeWrapper:
    """Trio-friendly wrapper for the Rust async bridge.

    This class wraps the PyO3 TrioAsyncBridge with a Trio-native async API.
    All operations use Trio primitives (Events, cancel scopes) and results
    are delivered via trio.from_thread callbacks.

    Example:
        async def use_bridge():
            bridge = TrioBridgeWrapper()
            await bridge.start()

            try:
                # Poll for activation - fully async, no blocked threads
                activation_bytes = await bridge.poll_workflow_activation()

                # Process activation...

                # Complete activation
                await bridge.complete_workflow_activation(completion_bytes)
            finally:
                await bridge.shutdown()

    Attributes:
        _rust_bridge: The underlying Rust bridge instance
        _trio_token: Trio token for from_thread callbacks
        _state: Current lifecycle state
    """

    def __init__(self) -> None:
        """Initialize the bridge wrapper.

        The bridge is not started automatically. Call start() to begin operation.
        """
        self._rust_bridge = TrioAsyncBridge()
        self._trio_token: Optional[trio.lowlevel.TrioToken] = None
        self._state: BridgeState = BridgeState.NOT_STARTED

    async def start(self) -> None:
        """Start the bridge and capture the Trio token.

        This must be called before any other operations. It captures the current
        Trio token which is used for all from_thread callbacks.

        Raises:
            RuntimeError: If bridge is already started
        """
        if self._state != BridgeState.NOT_STARTED:
            raise RuntimeError(f"Bridge already started (state: {self._state})")

        # Capture Trio token for from_thread callbacks
        self._trio_token = trio.lowlevel.current_trio_token()

        # Note: The Rust bridge starts automatically in its constructor
        # The Tokio runtime thread is already running

        self._state = BridgeState.RUNNING

    def set_trio_token(self, token: trio.lowlevel.TrioToken) -> None:
        """Set the Trio token for from_thread callbacks.

        This is typically called automatically by start(), but can be called
        explicitly if needed.

        Args:
            token: The Trio token to use for callbacks
        """
        self._trio_token = token

    async def initialize_with_config(
        self,
        target_url: str,
        namespace: str,
        task_queue: str,
        identity: Optional[str] = None,
        max_cached_workflows: int = 1000,
        max_concurrent_workflow_task_polls: int = 5,
        timeout: Optional[float] = None,
    ) -> None:
        """Initialize the bridge worker with Temporal configuration.

        Must be called after start() and before poll_workflow_activation().
        This connects to the Temporal server and initializes the SDK Core worker.

        Args:
            target_url: Temporal server URL (e.g., "localhost:7233")
            namespace: Temporal namespace
            task_queue: Task queue name
            identity: Worker identity (optional, defaults to "trio-worker")
            max_cached_workflows: Maximum cached workflows (default: 1000)
            max_concurrent_workflow_task_polls: Max concurrent polls (default: 5)
            timeout: Optional timeout in seconds

        Raises:
            RuntimeError: If bridge is not running or initialization fails
            trio.TooSlowError: If timeout is exceeded

        Example:
            bridge = TrioBridgeWrapper()
            await bridge.start()
            await bridge.initialize_with_config(
                target_url="localhost:7233",
                namespace="default",
                task_queue="my-task-queue"
            )
        """
        self._check_running()

        import json

        config = {
            "target_url": target_url,
            "namespace": namespace,
            "task_queue": task_queue,
            "identity": identity or "",
            "max_cached_workflows": max_cached_workflows,
            "max_concurrent_workflow_task_polls": max_concurrent_workflow_task_polls,
        }

        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for initialization result."""
            try:
                # Handle RequestResult struct directly
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(f"Init failed: {error_msg}"))
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request(
            "initialize", json.dumps(config).encode("utf-8"), deliver_result
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("initialize_with_config timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

    async def poll_workflow_activation(self, timeout: Optional[float] = None) -> bytes:
        """Poll for a workflow activation.

        This is a fully async operation that does not block any threads. It sends
        a request to the Rust bridge and awaits the result using a Trio Event.

        The Rust bridge processes the request asynchronously in its Tokio runtime
        and delivers the result back via a trio.from_thread callback.

        Args:
            timeout: Optional timeout in seconds. If specified and exceeded,
                    raises trio.TooSlowError.

        Returns:
            Serialized workflow activation bytes (protobuf)

        Raises:
            RuntimeError: If bridge is not running or trio_token not set
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge

        Example:
            # Poll with no timeout
            activation = await bridge.poll_workflow_activation()

            # Poll with 30 second timeout
            try:
                activation = await bridge.poll_workflow_activation(timeout=30.0)
            except trio.TooSlowError:
                print("Poll timed out")
        """
        self._check_running()

        event = trio.Event()

        # Container to store result (or error)
        # We use a list so the callback can modify it
        result_container: list = []
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback invoked from Rust thread when result is ready.

            This is called from the Rust thread and uses trio.from_thread.run_sync
            to safely deliver the result into the Trio context.

            Args:
                result: RequestResult struct from the Rust bridge
            """
            try:
                # Handle RequestResult struct directly - no JSON parsing!
                if result.success:
                    # Extract the protobuf bytes from the struct
                    data_bytes = result.get_data()
                    if data_bytes is not None:
                        result_container.append(bytes(data_bytes))
                    else:
                        result_container.append(b"")
                else:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                # This is the magic: Rust thread -> Trio async
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        # Send request to Rust bridge (non-blocking)
        self._rust_bridge.send_request("poll_activation", b"", deliver_result)

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("poll_workflow_activation timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        return result_container[0]

    async def complete_workflow_activation(
        self, completion_bytes: bytes, timeout: Optional[float] = None
    ) -> None:
        """Complete a workflow activation.

        Sends the completion back to the Rust bridge for delivery to Temporal.

        Args:
            completion_bytes: Serialized workflow activation completion (protobuf)
            timeout: Optional timeout in seconds

        Raises:
            RuntimeError: If bridge is not running or trio_token not set
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for completion acknowledgment."""
            try:
                # Handle RequestResult struct directly
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(
                        RuntimeError(f"Complete activation failed: {error_msg}")
                    )
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request(
            "complete_activation", completion_bytes, deliver_result
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("complete_workflow_activation timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

    async def validate(self, timeout: Optional[float] = None) -> None:
        """Validate the bridge connection and configuration.

        This should be called after initialization to ensure the bridge is
        properly configured and can communicate with Temporal.

        Args:
            timeout: Optional timeout in seconds

        Raises:
            RuntimeError: If bridge is not running
            trio.TooSlowError: If timeout is exceeded
            Exception: Any validation errors from the Rust bridge
        """
        self._check_running()

        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for validation result."""
            try:
                # Handle RequestResult struct directly
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(
                        RuntimeError(f"Validation failed: {error_msg}")
                    )
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request("validate", b"", deliver_result)

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("validate timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

    def initiate_shutdown(self) -> None:
        """Initiate bridge shutdown (synchronous).

        This is a synchronous method that signals the bridge to begin shutdown.
        It does not wait for shutdown to complete - call finalize_shutdown() for that.

        This two-phase shutdown allows:
        1. Synchronous initiation (e.g., from signal handlers)
        2. Async finalization (waits for in-flight operations)

        Note:
            After calling this, poll_workflow_activation will stop returning new work.
        """
        if self._state == BridgeState.SHUTDOWN:
            return

        # Call shutdown on the Rust bridge
        # Note: The Rust bridge only has shutdown(), not initiate_shutdown()
        self._rust_bridge.shutdown()

        self._state = BridgeState.SHUTDOWN

    async def finalize_shutdown(self, timeout: Optional[float] = None) -> None:
        """Finalize bridge shutdown (async).

        Waits for all in-flight operations to complete and cleans up resources.
        This should be called after initiate_shutdown().

        Args:
            timeout: Optional timeout in seconds

        Raises:
            trio.TooSlowError: If timeout is exceeded
            Exception: Any errors during shutdown
        """
        if self._state != BridgeState.SHUTDOWN:
            # Initiate if not already done
            self.initiate_shutdown()

        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for shutdown completion."""
            try:
                # Handle RequestResult struct directly
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(f"Shutdown error: {error_msg}"))
            except Exception as e:
                error_container.append(e)
            finally:
                if self._trio_token:
                    trio.from_thread.run_sync(event.set, trio_token=self._trio_token)
                else:
                    # If trio_token is None, just set the event directly
                    # This shouldn't happen, but handle gracefully
                    event.set()

        self._rust_bridge.send_request("finalize_shutdown", b"", deliver_result)

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("finalize_shutdown timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

    async def shutdown(self, timeout: Optional[float] = None) -> None:
        """Convenience method for full shutdown.

        Combines initiate_shutdown() and finalize_shutdown() into one call.

        Args:
            timeout: Optional timeout in seconds

        Raises:
            trio.TooSlowError: If timeout is exceeded
            Exception: Any errors during shutdown
        """
        self.initiate_shutdown()
        await self.finalize_shutdown(timeout=timeout)

    def _check_running(self) -> None:
        """Check that the bridge is in running state.

        Raises:
            RuntimeError: If bridge is not running or trio_token not set
        """
        if self._state != BridgeState.RUNNING:
            raise RuntimeError(
                f"Bridge is not running (state: {self._state}). Call start() first."
            )

        if self._trio_token is None:
            raise RuntimeError(
                "Trio token not set. This should not happen if start() was called."
            )


__all__ = [
    "TrioBridgeWrapper",
    "BridgeState",
]
