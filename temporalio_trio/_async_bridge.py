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
        self._shutdown_finalized: bool = False

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
        sticky_queue_schedule_to_start_timeout_millis: int = 10_000,
        timeout: Optional[float] = None,
        telemetry: Optional[dict] = None,
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
            sticky_queue_schedule_to_start_timeout_millis: How long a workflow task is
                allowed to sit on the sticky queue before it is timed out and moved
                to the non-sticky queue. In milliseconds. (default: 10000 = 10s)
            timeout: Optional timeout in seconds
            telemetry: Optional telemetry configuration dict for metrics export

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
            "sticky_queue_schedule_to_start_timeout_millis": sticky_queue_schedule_to_start_timeout_millis,
        }

        if telemetry is not None:
            config["telemetry"] = telemetry

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

    async def initialize_client(
        self,
        target_url: str,
        namespace: str,
        identity: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """Initialize the bridge client with Temporal configuration.

        Must be called after start() and before calling client operations.
        This connects to the Temporal server and initializes the SDK Core client.

        Args:
            target_url: Temporal server URL (e.g., "localhost:7233")
            namespace: Temporal namespace
            identity: Client identity (optional, defaults to "trio-client")
            timeout: Optional timeout in seconds

        Raises:
            RuntimeError: If bridge is not running or initialization fails
            trio.TooSlowError: If timeout is exceeded

        Example:
            bridge = TrioBridgeWrapper()
            await bridge.start()
            await bridge.initialize_client(
                target_url="localhost:7233",
                namespace="default"
            )
        """
        self._check_running()

        import json

        config = {
            "target_url": target_url,
            "namespace": namespace,
            "identity": identity or "",
        }

        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for client initialization result."""
            try:
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(
                        RuntimeError(f"Client init failed: {error_msg}")
                    )
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request(
            "initialize_client", json.dumps(config).encode("utf-8"), deliver_result
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("initialize_client timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

    async def start_workflow_execution(
        self, request_bytes: bytes, timeout: Optional[float] = None
    ) -> bytes:
        """Start a workflow execution.

        Args:
            request_bytes: Serialized StartWorkflowExecutionRequest (protobuf)
            timeout: Optional timeout in seconds

        Returns:
            Serialized StartWorkflowExecutionResponse (protobuf)

        Raises:
            RuntimeError: If bridge is not running
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        event = trio.Event()
        result_container: list = []
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for start workflow result."""
            try:
                if result.success:
                    data_bytes = result.get_data()
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError(
                                f"start_workflow returned success without data. "
                                f"This indicates a bridge bug (request_id: {result.request_id})"
                            )
                        )
                    else:
                        result_container.append(bytes(data_bytes))
                else:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request("start_workflow", request_bytes, deliver_result)

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("start_workflow_execution timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        return result_container[0]

    async def get_workflow_result(
        self,
        workflow_id: str,
        run_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> bytes:
        """Get workflow execution result (blocks until workflow completes).

        Args:
            workflow_id: Workflow ID
            run_id: Optional run ID
            timeout: Optional timeout in seconds

        Returns:
            Serialized GetWorkflowExecutionHistoryResponse (protobuf)

        Raises:
            RuntimeError: If bridge is not running
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        import json

        request_data = {"workflow_id": workflow_id, "run_id": run_id}

        event = trio.Event()
        result_container: list = []
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for get result."""
            try:
                if result.success:
                    data_bytes = result.get_data()
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError(
                                f"get_workflow_result returned success without data. "
                                f"This indicates a bridge bug (request_id: {result.request_id})"
                            )
                        )
                    else:
                        result_container.append(bytes(data_bytes))
                else:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request(
            "get_workflow_result",
            json.dumps(request_data).encode("utf-8"),
            deliver_result,
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("get_workflow_result timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        return result_container[0]

    async def cancel_workflow_execution(
        self,
        workflow_id: str,
        run_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """Cancel a workflow execution.

        Args:
            workflow_id: Workflow ID
            run_id: Optional run ID
            timeout: Optional timeout in seconds

        Raises:
            RuntimeError: If bridge is not running
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        import json

        request_data = {"workflow_id": workflow_id, "run_id": run_id}

        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for cancel result."""
            try:
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request(
            "cancel_workflow", json.dumps(request_data).encode("utf-8"), deliver_result
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("cancel_workflow_execution timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

    async def terminate_workflow_execution(
        self,
        workflow_id: str,
        run_id: Optional[str] = None,
        reason: str = "",
        timeout: Optional[float] = None,
    ) -> None:
        """Terminate a workflow execution.

        Args:
            workflow_id: Workflow ID
            run_id: Optional run ID
            reason: Termination reason
            timeout: Optional timeout in seconds

        Raises:
            RuntimeError: If bridge is not running
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        import json

        request_data = {
            "workflow_id": workflow_id,
            "run_id": run_id,
            "reason": reason,
        }

        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for terminate result."""
            try:
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request(
            "terminate_workflow",
            json.dumps(request_data).encode("utf-8"),
            deliver_result,
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("terminate_workflow_execution timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

    async def query_workflow(
        self,
        workflow_id: str,
        run_id: Optional[str],
        query_type: str,
        args_bytes: bytes,
        timeout: Optional[float] = None,
    ) -> bytes:
        """Query a workflow execution.

        Args:
            workflow_id: Workflow ID
            run_id: Optional run ID
            query_type: Query type name
            args_bytes: Serialized query arguments (Payloads protobuf)
            timeout: Optional timeout in seconds

        Returns:
            Serialized QueryWorkflowResponse (protobuf)

        Raises:
            RuntimeError: If bridge is not running
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        import json

        request_data = {
            "workflow_id": workflow_id,
            "run_id": run_id,
            "query_type": query_type,
            "args_bytes": list(args_bytes),  # Convert bytes to list for JSON
        }

        event = trio.Event()
        result_container: list = []
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for query result."""
            try:
                if result.success:
                    data_bytes = result.get_data()
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError(
                                f"query_workflow returned success without data. "
                                f"This indicates a bridge bug (request_id: {result.request_id})"
                            )
                        )
                    else:
                        result_container.append(bytes(data_bytes))
                else:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request(
            "query_workflow", json.dumps(request_data).encode("utf-8"), deliver_result
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("query_workflow timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        return result_container[0]

    async def signal_workflow(
        self,
        workflow_id: str,
        run_id: Optional[str],
        signal_name: str,
        args_bytes: bytes,
        timeout: Optional[float] = None,
    ) -> None:
        """Signal a workflow execution.

        Args:
            workflow_id: Workflow ID
            run_id: Optional run ID
            signal_name: Signal name
            args_bytes: Serialized signal arguments (Payloads protobuf)
            timeout: Optional timeout in seconds

        Raises:
            RuntimeError: If bridge is not running
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        import json

        request_data = {
            "workflow_id": workflow_id,
            "run_id": run_id,
            "signal_name": signal_name,
            "args_bytes": list(args_bytes),  # Convert bytes to list for JSON
        }

        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for signal result."""
            try:
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request(
            "signal_workflow", json.dumps(request_data).encode("utf-8"), deliver_result
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("signal_workflow timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

    async def poll_activity_task(self, timeout: Optional[float] = None) -> bytes:
        """Poll for an activity task.

        This is a fully async operation that polls for activity tasks from
        the Temporal server via the Rust bridge.

        Args:
            timeout: Optional timeout in seconds.

        Returns:
            Serialized activity task bytes (protobuf)

        Raises:
            RuntimeError: If bridge is not running
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        event = trio.Event()
        result_container: list = []
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for activity task result."""
            import logging

            logger = logging.getLogger(__name__)
            logger.debug(
                f"poll_activity_task callback received: success={result.success}"
            )
            try:
                if result.success:
                    data_bytes = result.get_data()
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError(
                                f"poll_activity_task returned success without data. "
                                f"This indicates a bridge bug (request_id: {result.request_id})"
                            )
                        )
                    else:
                        result_container.append(bytes(data_bytes))
                        logger.debug(f"poll_activity_task got {len(data_bytes)} bytes")
                else:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
                    logger.debug(f"poll_activity_task error: {error_msg}")
            except Exception as e:
                error_container.append(e)
                logger.debug(f"poll_activity_task callback exception: {e}")
            finally:
                logger.debug("poll_activity_task setting event")
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        import logging

        logging.getLogger(__name__).debug(
            "Sending poll_activity_task request to bridge"
        )
        self._rust_bridge.send_request("poll_activity_task", b"", deliver_result)

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("poll_activity_task timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        return result_container[0]

    async def complete_activity_task(
        self, completion_bytes: bytes, timeout: Optional[float] = None
    ) -> None:
        """Complete an activity task.

        Sends the activity completion back to the Rust bridge for delivery to Temporal.

        Args:
            completion_bytes: Serialized activity task completion (protobuf)
            timeout: Optional timeout in seconds

        Raises:
            RuntimeError: If bridge is not running
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for activity completion acknowledgment."""
            try:
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(
                        RuntimeError(f"Complete activity task failed: {error_msg}")
                    )
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request(
            "complete_activity_task", completion_bytes, deliver_result
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("complete_activity_task timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

    async def record_activity_heartbeat(
        self, heartbeat_bytes: bytes, timeout: Optional[float] = None
    ) -> bytes:
        """Record an activity heartbeat.

        Sends heartbeat to the server. Returns any cancellation details if the
        activity was cancelled.

        Args:
            heartbeat_bytes: Serialized heartbeat data (protobuf)
            timeout: Optional timeout in seconds

        Returns:
            Serialized heartbeat response (may contain cancellation info)

        Raises:
            RuntimeError: If bridge is not running
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        event = trio.Event()
        result_container: list = []
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for heartbeat result."""
            try:
                if result.success:
                    data_bytes = result.get_data()
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError(
                                f"record_activity_heartbeat returned success without data. "
                                f"This indicates a bridge bug (request_id: {result.request_id})"
                            )
                        )
                    else:
                        result_container.append(bytes(data_bytes))
                else:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request(
            "record_activity_heartbeat", heartbeat_bytes, deliver_result
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("record_activity_heartbeat timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        return result_container[0]

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
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError(
                                f"poll_workflow_activation returned success without data. "
                                f"This indicates a bridge bug (request_id: {result.request_id})"
                            )
                        )
                    else:
                        result_container.append(bytes(data_bytes))
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

        Note: This method allows completions during SHUTDOWN state to support
        draining in-flight activations during graceful shutdown.

        Args:
            completion_bytes: Serialized workflow activation completion (protobuf)
            timeout: Optional timeout in seconds

        Raises:
            RuntimeError: If bridge is not started or trio_token not set
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_can_complete()

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
            However, complete_workflow_activation will continue to work, allowing
            in-flight activations to complete gracefully.
        """
        if self._state == BridgeState.SHUTDOWN:
            return

        # Send initiate_shutdown request to the core worker (fire-and-forget)
        # This tells the core worker to stop polling, which unblocks poll_workflow_activation
        # but does NOT close the bridge - completions can still be sent
        def noop_callback(result: object) -> None:
            pass  # Ignore result - this is fire-and-forget

        try:
            self._rust_bridge.send_request("initiate_shutdown", b"", noop_callback)
        except RuntimeError as e:
            # Bridge may already be shut down - expected during shutdown
            error_str = str(e).lower()
            if "shutdown" in error_str or "not running" in error_str:
                # Expected shutdown error, safe to ignore
                pass
            else:
                # Unexpected error during shutdown initiation
                import logging

                logger = logging.getLogger(__name__)
                logger.warning(
                    f"Unexpected error during initiate_shutdown: {e}. "
                    f"This may indicate a bridge issue."
                )

        self._state = BridgeState.SHUTDOWN

    async def finalize_shutdown(self, timeout: Optional[float] = None) -> None:
        """Finalize bridge shutdown (async).

        Waits for all in-flight operations to complete and cleans up resources.
        This should be called after initiate_shutdown().

        This method is idempotent - calling it multiple times after the first
        successful call will return immediately without error.

        Args:
            timeout: Optional timeout in seconds

        Raises:
            trio.TooSlowError: If timeout is exceeded
            Exception: Any errors during shutdown
        """
        # Return early if already finalized (idempotent)
        if self._shutdown_finalized:
            return

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

        try:
            self._rust_bridge.send_request("finalize_shutdown", b"", deliver_result)
        except RuntimeError as e:
            # Handle case where bridge is already fully shutdown
            error_str = str(e).lower()
            if "shutdown" in error_str or "not running" in error_str:
                # Expected shutdown error - bridge already finalized
                self._shutdown_finalized = True
                return
            # Unexpected error - re-raise
            import logging

            logger = logging.getLogger(__name__)
            logger.warning(f"Unexpected error during finalize_shutdown: {e}")
            raise

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("finalize_shutdown timed out")
        else:
            await event.wait()

        self._shutdown_finalized = True

        # Now it's safe to close the Rust bridge completely
        # All operations have been drained
        try:
            self._rust_bridge.shutdown()
        except RuntimeError:
            # Bridge may already be shut down, ignore
            pass

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

    def _check_can_complete(self) -> None:
        """Check that the bridge can accept completions.

        Completions are allowed during both RUNNING and SHUTDOWN states.
        This is necessary because in-flight activations may need to complete
        after shutdown has been initiated.

        Raises:
            RuntimeError: If bridge is not started or trio_token not set
        """
        if self._state == BridgeState.NOT_STARTED:
            raise RuntimeError(
                f"Bridge is not started (state: {self._state}). Call start() first."
            )

        if self._trio_token is None:
            raise RuntimeError(
                "Trio token not set. This should not happen if start() was called."
            )


__all__ = [
    "TrioBridgeWrapper",
    "BridgeState",
]
