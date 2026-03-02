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
from typing import Any, Optional

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
        nonsticky_to_sticky_poll_ratio: float = 0.2,
        max_concurrent_activity_task_polls: int = 5,
        no_remote_activities: bool = False,
        sticky_queue_schedule_to_start_timeout_millis: int = 10_000,
        max_heartbeat_throttle_interval_millis: int = 60_000,
        default_heartbeat_throttle_interval_millis: int = 30_000,
        max_activities_per_second: Optional[float] = None,
        max_task_queue_activities_per_second: Optional[float] = None,
        graceful_shutdown_period_millis: Optional[int] = None,
        max_concurrent_workflow_tasks: Optional[int] = None,
        max_concurrent_activities: Optional[int] = None,
        max_concurrent_local_activities: Optional[int] = None,
        build_id: Optional[str] = None,
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
            max_concurrent_workflow_task_polls: Max concurrent workflow task polls (default: 5)
            nonsticky_to_sticky_poll_ratio: Ratio for nonsticky to sticky polls (default: 0.2)
            max_concurrent_activity_task_polls: Max concurrent activity task polls (default: 5)
            no_remote_activities: If True, disable remote activity polling (default: False)
            sticky_queue_schedule_to_start_timeout_millis: Sticky queue timeout in ms (default: 10000)
            max_heartbeat_throttle_interval_millis: Max heartbeat throttle in ms (default: 60000)
            default_heartbeat_throttle_interval_millis: Default heartbeat throttle in ms (default: 30000)
            max_activities_per_second: Max activities per second for this worker
            max_task_queue_activities_per_second: Max activities per second for the task queue
            graceful_shutdown_period_millis: Graceful shutdown period in ms
            max_concurrent_workflow_tasks: Max concurrent workflow tasks
            max_concurrent_activities: Max concurrent activities
            max_concurrent_local_activities: Max concurrent local activities
            build_id: Build identifier for worker versioning
            timeout: Optional timeout in seconds
            telemetry: Optional telemetry configuration dict for metrics export

        Raises:
            RuntimeError: If bridge is not running or initialization fails
            trio.TooSlowError: If timeout is exceeded
        """
        self._check_running()

        import json

        config: dict = {
            "target_url": target_url,
            "namespace": namespace,
            "task_queue": task_queue,
            "identity": identity or "",
            "max_cached_workflows": max_cached_workflows,
            "max_concurrent_workflow_task_polls": max_concurrent_workflow_task_polls,
            "nonsticky_to_sticky_poll_ratio": nonsticky_to_sticky_poll_ratio,
            "max_concurrent_activity_task_polls": max_concurrent_activity_task_polls,
            "no_remote_activities": no_remote_activities,
            "sticky_queue_schedule_to_start_timeout_millis": sticky_queue_schedule_to_start_timeout_millis,
            "max_heartbeat_throttle_interval_millis": max_heartbeat_throttle_interval_millis,
            "default_heartbeat_throttle_interval_millis": default_heartbeat_throttle_interval_millis,
        }

        if max_activities_per_second is not None:
            config["max_activities_per_second"] = max_activities_per_second
        if max_task_queue_activities_per_second is not None:
            config["max_task_queue_activities_per_second"] = (
                max_task_queue_activities_per_second
            )
        if graceful_shutdown_period_millis is not None:
            config["graceful_shutdown_period_millis"] = graceful_shutdown_period_millis
        if max_concurrent_workflow_tasks is not None:
            config["max_concurrent_workflow_tasks"] = max_concurrent_workflow_tasks
        if max_concurrent_activities is not None:
            config["max_concurrent_activities"] = max_concurrent_activities
        if max_concurrent_local_activities is not None:
            config["max_concurrent_local_activities"] = max_concurrent_local_activities
        if build_id is not None:
            config["build_id"] = build_id

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

    @staticmethod
    def _encode_worker_id_prefix(worker_id: str, data: bytes) -> bytes:
        """Encode worker_id as a length-prefixed prefix before data.

        Format: 4 bytes (big-endian) worker_id length + worker_id UTF-8 + data.
        Used by complete/heartbeat operations that send protobuf data.
        """
        wid_bytes = worker_id.encode("utf-8")
        return len(wid_bytes).to_bytes(4, byteorder="big") + wid_bytes + data

    async def create_worker(
        self,
        *,
        namespace: str,
        task_queue: str,
        identity: Optional[str] = None,
        max_cached_workflows: int = 1000,
        max_concurrent_workflow_task_polls: int = 5,
        nonsticky_to_sticky_poll_ratio: float = 0.2,
        max_concurrent_activity_task_polls: int = 5,
        no_remote_activities: bool = False,
        sticky_queue_schedule_to_start_timeout_millis: int = 10_000,
        max_heartbeat_throttle_interval_millis: int = 60_000,
        default_heartbeat_throttle_interval_millis: int = 30_000,
        max_activities_per_second: Optional[float] = None,
        max_task_queue_activities_per_second: Optional[float] = None,
        graceful_shutdown_period_millis: Optional[int] = None,
        max_concurrent_workflow_tasks: Optional[int] = None,
        max_concurrent_activities: Optional[int] = None,
        max_concurrent_local_activities: Optional[int] = None,
        build_id: Optional[str] = None,
        telemetry: Optional[dict] = None,
        timeout: Optional[float] = None,
    ) -> str:
        """Create a worker using the already-connected client's gRPC connection.

        Returns a worker_id for use with poll/complete operations.
        The client must already be initialized via initialize_client().

        Args:
            namespace: Temporal namespace.
            task_queue: Task queue name.
            identity: Worker identity.
            max_cached_workflows: Maximum cached workflows.
            ... (same params as initialize_with_config minus target_url and telemetry)
            timeout: Optional timeout in seconds.

        Returns:
            worker_id string to pass to all subsequent worker operations.

        Raises:
            RuntimeError: If bridge is not running or creation fails.
            trio.TooSlowError: If timeout is exceeded.
        """
        self._check_running()

        import json

        config: dict = {
            "namespace": namespace,
            "task_queue": task_queue,
            "identity": identity or "",
            "max_cached_workflows": max_cached_workflows,
            "max_concurrent_workflow_task_polls": max_concurrent_workflow_task_polls,
            "nonsticky_to_sticky_poll_ratio": nonsticky_to_sticky_poll_ratio,
            "max_concurrent_activity_task_polls": max_concurrent_activity_task_polls,
            "no_remote_activities": no_remote_activities,
            "sticky_queue_schedule_to_start_timeout_millis": sticky_queue_schedule_to_start_timeout_millis,
            "max_heartbeat_throttle_interval_millis": max_heartbeat_throttle_interval_millis,
            "default_heartbeat_throttle_interval_millis": default_heartbeat_throttle_interval_millis,
        }

        if max_activities_per_second is not None:
            config["max_activities_per_second"] = max_activities_per_second
        if max_task_queue_activities_per_second is not None:
            config["max_task_queue_activities_per_second"] = (
                max_task_queue_activities_per_second
            )
        if graceful_shutdown_period_millis is not None:
            config["graceful_shutdown_period_millis"] = graceful_shutdown_period_millis
        if max_concurrent_workflow_tasks is not None:
            config["max_concurrent_workflow_tasks"] = max_concurrent_workflow_tasks
        if max_concurrent_activities is not None:
            config["max_concurrent_activities"] = max_concurrent_activities
        if max_concurrent_local_activities is not None:
            config["max_concurrent_local_activities"] = max_concurrent_local_activities
        if build_id is not None:
            config["build_id"] = build_id
        if telemetry is not None:
            config["telemetry"] = telemetry

        event = trio.Event()
        result_container: list = []
        error_container: list = []

        def deliver_result(result) -> None:
            try:
                if result.success:
                    data_bytes = result.get_data()
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError("create_worker returned success without data")
                        )
                    else:
                        # worker_id is returned as UTF-8 bytes
                        result_container.append(bytes(data_bytes).decode("utf-8"))
                else:
                    error_msg = result.error or "Unknown error"
                    error_container.append(
                        RuntimeError(f"create_worker failed: {error_msg}")
                    )
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request(
            "create_worker", json.dumps(config).encode("utf-8"), deliver_result
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()
            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("create_worker timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        return result_container[0]

    async def initialize_client(
        self,
        target_url: str,
        namespace: str,
        identity: Optional[str] = None,
        api_key: Optional[str] = None,
        tls_config: Optional[dict] = None,
        rpc_metadata: Optional[dict[str, str]] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """Initialize the bridge client with Temporal configuration.

        Must be called after start() and before calling client operations.
        This connects to the Temporal server and initializes the SDK Core client.

        Args:
            target_url: Temporal server URL (e.g., "localhost:7233")
            namespace: Temporal namespace
            identity: Client identity (optional, defaults to "trio-client")
            api_key: API key for Temporal Cloud authentication
            tls_config: TLS configuration dict (base64-encoded cert fields)
            rpc_metadata: Headers to include on every RPC call
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

        config: dict = {
            "target_url": target_url,
            "namespace": namespace,
            "identity": identity or "",
        }
        if api_key is not None:
            config["api_key"] = api_key
        if tls_config is not None:
            config["tls_config"] = tls_config
        if rpc_metadata is not None:
            config["rpc_metadata"] = rpc_metadata

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

    async def signal_with_start_workflow_execution(
        self, request_bytes: bytes, timeout: Optional[float] = None
    ) -> bytes:
        """Signal-with-start a workflow execution.

        Args:
            request_bytes: Serialized SignalWithStartWorkflowExecutionRequest (protobuf)
            timeout: Optional timeout in seconds

        Returns:
            Serialized SignalWithStartWorkflowExecutionResponse (protobuf)

        Raises:
            RuntimeError: If bridge is not running
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        event = trio.Event()
        result_container: list[bytes] = []
        error_container: list[Exception] = []

        def deliver_result(result) -> None:  # type: ignore[no-untyped-def]
            """Callback for signal-with-start result."""
            try:
                if result.success:
                    data_bytes = result.get_data()
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError(
                                f"signal_with_start_workflow returned success without data. "
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
            "signal_with_start_workflow", request_bytes, deliver_result
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError(
                    "signal_with_start_workflow_execution timed out"
                )
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

    async def get_workflow_execution_history(
        self,
        workflow_id: str,
        run_id: Optional[str] = None,
        next_page_token: bytes = b"",
        timeout: Optional[float] = None,
        event_filter_type: Optional[int] = None,
        skip_archival: Optional[bool] = None,
    ) -> bytes:
        """Get workflow execution history (all events).

        Unlike get_workflow_result which uses long-polling and only returns
        close events, this method returns the full history with all event types.

        Args:
            workflow_id: Workflow ID
            run_id: Optional run ID
            next_page_token: Token for pagination (empty for first page)
            timeout: Optional timeout in seconds
            event_filter_type: Optional filter for event types (0=all, 1=close, 2=all)
            skip_archival: Optional flag to skip archival

        Returns:
            Serialized GetWorkflowExecutionHistoryResponse (protobuf)

        Raises:
            RuntimeError: If bridge is not running
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        import json

        request_data: dict[str, Any] = {
            "workflow_id": workflow_id,
            "run_id": run_id,
            "next_page_token": list(next_page_token) if next_page_token else [],
        }
        if event_filter_type is not None:
            request_data["event_filter_type"] = event_filter_type
        if skip_archival is not None:
            request_data["skip_archival"] = skip_archival

        event = trio.Event()
        result_container: list = []
        error_container: list = []

        def deliver_result(result) -> None:
            """Callback for get history."""
            try:
                if result.success:
                    data_bytes = result.get_data()
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError(
                                f"get_workflow_execution_history returned success without data. "
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
            "get_workflow_execution_history",
            json.dumps(request_data).encode("utf-8"),
            deliver_result,
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("get_workflow_execution_history timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        return result_container[0]

    async def cancel_workflow_execution(
        self,
        workflow_id: str,
        run_id: Optional[str] = None,
        first_execution_run_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """Cancel a workflow execution.

        Args:
            workflow_id: Workflow ID
            run_id: Optional run ID
            first_execution_run_id: Optional first execution run ID
            timeout: Optional timeout in seconds

        Raises:
            RuntimeError: If bridge is not running
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        import json

        request_data: dict = {"workflow_id": workflow_id, "run_id": run_id}
        if first_execution_run_id is not None:
            request_data["first_execution_run_id"] = first_execution_run_id

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
        first_execution_run_id: Optional[str] = None,
        details_payloads_bytes: Optional[bytes] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """Terminate a workflow execution.

        Args:
            workflow_id: Workflow ID
            run_id: Optional run ID
            reason: Termination reason
            first_execution_run_id: Optional first execution run ID
            details_payloads_bytes: Optional encoded Payloads bytes for details
            timeout: Optional timeout in seconds

        Raises:
            RuntimeError: If bridge is not running
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        import json

        request_data: dict = {
            "workflow_id": workflow_id,
            "run_id": run_id,
            "reason": reason,
        }
        if first_execution_run_id is not None:
            request_data["first_execution_run_id"] = first_execution_run_id
        if details_payloads_bytes is not None:
            request_data["details_bytes"] = list(details_payloads_bytes)

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
        reject_condition: Optional[int] = None,
        timeout: Optional[float] = None,
    ) -> bytes:
        """Query a workflow execution.

        Args:
            workflow_id: Workflow ID
            run_id: Optional run ID
            query_type: Query type name
            args_bytes: Serialized query arguments (Payloads protobuf)
            reject_condition: Optional query reject condition (protobuf enum value)
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

        request_data: dict = {
            "workflow_id": workflow_id,
            "run_id": run_id,
            "query_type": query_type,
            "args_bytes": list(args_bytes),  # Convert bytes to list for JSON
        }

        if reject_condition is not None:
            request_data["reject_condition"] = reject_condition

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

    async def describe_workflow(
        self,
        workflow_id: str,
        run_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> bytes:
        """Describe a workflow execution.

        Args:
            workflow_id: Workflow ID
            run_id: Optional run ID
            timeout: Optional timeout in seconds

        Returns:
            Serialized DescribeWorkflowExecutionResponse bytes (protobuf)

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
            """Callback for describe result."""
            try:
                if result.success:
                    data_bytes = result.get_data()
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError(
                                "describe_workflow returned success without data"
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
            "describe_workflow",
            json.dumps(request_data).encode("utf-8"),
            deliver_result,
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("describe_workflow timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        if not result_container:
            raise RuntimeError("describe_workflow returned no result")

        return result_container[0]

    async def list_workflows(
        self,
        request_bytes: bytes,
        timeout: Optional[float] = None,
    ) -> bytes:
        """List workflow executions.

        Args:
            request_bytes: Serialized ListWorkflowExecutionsRequest bytes (protobuf)
            timeout: Optional timeout in seconds

        Returns:
            Serialized ListWorkflowExecutionsResponse bytes (protobuf)

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
            """Callback for list workflows result."""
            try:
                if result.success:
                    data_bytes = result.get_data()
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError("list_workflows returned success without data")
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

        self._rust_bridge.send_request("list_workflows", request_bytes, deliver_result)

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("list_workflows timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        if not result_container:
            raise RuntimeError("list_workflows returned no result")

        return result_container[0]

    async def count_workflows(
        self,
        request_bytes: bytes,
        timeout: Optional[float] = None,
    ) -> bytes:
        """Count workflow executions.

        Args:
            request_bytes: Serialized CountWorkflowExecutionsRequest bytes (protobuf)
            timeout: Optional timeout in seconds

        Returns:
            Serialized CountWorkflowExecutionsResponse bytes (protobuf)

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
            """Callback for count workflows result."""
            try:
                if result.success:
                    data_bytes = result.get_data()
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError(
                                "count_workflows returned success without data"
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

        self._rust_bridge.send_request("count_workflows", request_bytes, deliver_result)

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("count_workflows timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        if not result_container:
            raise RuntimeError("count_workflows returned no result")

        return result_container[0]

    async def update_workflow(
        self,
        request_bytes: bytes,
        timeout: Optional[float] = None,
    ) -> bytes:
        """Send an update to a workflow execution.

        Args:
            request_bytes: Serialized UpdateWorkflowExecutionRequest bytes (protobuf)
            timeout: Optional timeout in seconds

        Returns:
            Serialized UpdateWorkflowExecutionResponse bytes (protobuf)

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
            """Callback for update workflow result."""
            try:
                if result.success:
                    data_bytes = result.get_data()
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError(
                                "update_workflow returned success without data"
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

        self._rust_bridge.send_request("update_workflow", request_bytes, deliver_result)

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("update_workflow timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        if not result_container:
            raise RuntimeError("update_workflow returned no result")

        return result_container[0]

    async def poll_workflow_execution_update(
        self,
        request_bytes: bytes,
        timeout: Optional[float] = None,
    ) -> bytes:
        """Poll for a workflow execution update result.

        Args:
            request_bytes: Serialized PollWorkflowExecutionUpdateRequest bytes (protobuf)
            timeout: Optional timeout in seconds

        Returns:
            Serialized PollWorkflowExecutionUpdateResponse bytes (protobuf)

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
            """Callback for poll update result."""
            try:
                if result.success:
                    data_bytes = result.get_data()
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError(
                                "poll_workflow_execution_update returned success without data"
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
            "poll_workflow_execution_update", request_bytes, deliver_result
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("poll_workflow_execution_update timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        if not result_container:
            raise RuntimeError("poll_workflow_execution_update returned no result")

        return result_container[0]

    async def poll_activity_task(
        self,
        worker_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> bytes:
        """Poll for an activity task.

        Args:
            worker_id: Optional worker_id for multi-worker routing.
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
                else:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        data = worker_id.encode("utf-8") if worker_id else b""
        self._rust_bridge.send_request("poll_activity_task", data, deliver_result)

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
        self,
        completion_bytes: bytes,
        worker_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """Complete an activity task.

        Args:
            completion_bytes: Serialized activity task completion (protobuf)
            worker_id: Optional worker_id for multi-worker routing.
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

        data = (
            self._encode_worker_id_prefix(worker_id, completion_bytes)
            if worker_id
            else completion_bytes
        )
        self._rust_bridge.send_request("complete_activity_task", data, deliver_result)

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
        self,
        heartbeat_bytes: bytes,
        worker_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> bytes:
        """Record an activity heartbeat.

        Args:
            heartbeat_bytes: Serialized heartbeat data (protobuf)
            worker_id: Optional worker_id for multi-worker routing.
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

        data = (
            self._encode_worker_id_prefix(worker_id, heartbeat_bytes)
            if worker_id
            else heartbeat_bytes
        )
        self._rust_bridge.send_request(
            "record_activity_heartbeat", data, deliver_result
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

    async def poll_workflow_activation(
        self,
        worker_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> bytes:
        """Poll for a workflow activation.

        This is a fully async operation that does not block any threads. It sends
        a request to the Rust bridge and awaits the result using a Trio Event.

        Args:
            worker_id: Optional worker_id for multi-worker routing. If provided,
                routes to the worker created by create_worker(). If None, uses
                the legacy single worker.
            timeout: Optional timeout in seconds. If specified and exceeded,
                    raises trio.TooSlowError.

        Returns:
            Serialized workflow activation bytes (protobuf)

        Raises:
            RuntimeError: If bridge is not running or trio_token not set
            trio.TooSlowError: If timeout is exceeded
            Exception: Any error from the Rust bridge
        """
        self._check_running()

        event = trio.Event()
        result_container: list = []
        error_container: list = []

        def deliver_result(result) -> None:
            try:
                if result.success:
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
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        # If worker_id is provided, send it as the data for multi-worker routing
        data = worker_id.encode("utf-8") if worker_id else b""
        self._rust_bridge.send_request("poll_activation", data, deliver_result)

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
        self,
        completion_bytes: bytes,
        worker_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """Complete a workflow activation.

        Sends the completion back to the Rust bridge for delivery to Temporal.

        Note: This method allows completions during SHUTDOWN state to support
        draining in-flight activations during graceful shutdown.

        Args:
            completion_bytes: Serialized workflow activation completion (protobuf)
            worker_id: Optional worker_id for multi-worker routing.
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
            try:
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(
                        RuntimeError(f"Complete activation failed: {error_msg}")
                    )
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        # If worker_id, prefix the data with length-prefixed worker_id
        data = (
            self._encode_worker_id_prefix(worker_id, completion_bytes)
            if worker_id
            else completion_bytes
        )
        self._rust_bridge.send_request("complete_activation", data, deliver_result)

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

    def initiate_shutdown(self, worker_id: Optional[str] = None) -> None:
        """Initiate shutdown (synchronous).

        When called with a worker_id, shuts down just that worker (the bridge
        stays alive for the client and other workers). When called without a
        worker_id, shuts down the legacy single worker and marks the bridge
        as shutting down.

        Args:
            worker_id: Optional worker_id. If provided, only that worker is
                shut down. If None, the bridge-level shutdown is initiated.
        """
        if worker_id is None and self._state == BridgeState.SHUTDOWN:
            return

        def noop_callback(result: object) -> None:
            pass

        try:
            data = worker_id.encode("utf-8") if worker_id else b""
            self._rust_bridge.send_request("initiate_shutdown", data, noop_callback)
        except RuntimeError as e:
            error_str = str(e).lower()
            if "shutdown" in error_str or "not running" in error_str:
                pass
            else:
                import logging

                logger = logging.getLogger(__name__)
                logger.warning(
                    f"Unexpected error during initiate_shutdown: {e}. "
                    f"This may indicate a bridge issue."
                )

        if worker_id is None:
            self._state = BridgeState.SHUTDOWN

    async def finalize_shutdown(
        self,
        worker_id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """Finalize shutdown (async).

        When called with a worker_id, finalizes shutdown of just that worker
        (removes it from the bridge's worker map). When called without a
        worker_id, finalizes the bridge-level shutdown (legacy worker +
        closes the Rust bridge).

        Args:
            worker_id: Optional worker_id to finalize.
            timeout: Optional timeout in seconds.

        Raises:
            trio.TooSlowError: If timeout is exceeded.
            Exception: Any errors during shutdown.
        """
        # For bridge-level shutdown, check idempotency
        if worker_id is None:
            if self._shutdown_finalized:
                return

            if self._state != BridgeState.SHUTDOWN:
                self.initiate_shutdown()

        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            try:
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(f"Shutdown error: {error_msg}"))
            except Exception as e:
                error_container.append(e)
            finally:
                if self._trio_token:
                    trio.from_thread.run_sync(event.set, trio_token=self._trio_token)
                else:
                    event.set()

        try:
            data = worker_id.encode("utf-8") if worker_id else b""
            self._rust_bridge.send_request("finalize_shutdown", data, deliver_result)
        except RuntimeError as e:
            error_str = str(e).lower()
            if "shutdown" in error_str or "not running" in error_str:
                if worker_id is None:
                    self._shutdown_finalized = True
                return
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

        if worker_id is None:
            self._shutdown_finalized = True

            # Now it's safe to close the Rust bridge completely
            try:
                self._rust_bridge.shutdown()
            except RuntimeError:
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

    # ==========================================================================
    # Replay Worker Operations
    # ==========================================================================

    async def initialize_replay_worker(
        self,
        namespace: str,
        task_queue: str,
        build_id: Optional[str] = None,
        identity: Optional[str] = None,
        nondeterminism_as_workflow_fail: bool = False,
        nondeterminism_as_workflow_fail_for_types: Optional[set[str]] = None,
        timeout: Optional[float] = None,
    ) -> None:
        """Initialize a replay worker in the bridge.

        Args:
            namespace: Temporal namespace.
            task_queue: Task queue name.
            build_id: Build identifier for worker versioning.
            identity: Worker identity.
            nondeterminism_as_workflow_fail: If True, treat nondeterminism
                errors as workflow failures instead of task failures.
            nondeterminism_as_workflow_fail_for_types: Set of workflow type
                names for which nondeterminism should be treated as workflow
                failure.
            timeout: Optional timeout in seconds.
        """
        self._check_running()

        import json

        config: dict = {
            "namespace": namespace,
            "task_queue": task_queue,
        }
        if build_id is not None:
            config["build_id"] = build_id
        if identity is not None:
            config["identity"] = identity
        config["nondeterminism_as_workflow_fail"] = nondeterminism_as_workflow_fail
        config["nondeterminism_as_workflow_fail_for_types"] = list(
            nondeterminism_as_workflow_fail_for_types or []
        )

        config_bytes = json.dumps(config).encode("utf-8")

        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            try:
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request(
            "initialize_replay_worker", config_bytes, deliver_result
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()
            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("initialize_replay_worker timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

    async def push_replay_history(
        self,
        workflow_id: str,
        history_bytes: bytes,
        timeout: Optional[float] = None,
    ) -> None:
        """Push a workflow history for replay.

        Args:
            workflow_id: The workflow ID for this history.
            history_bytes: Serialized History protobuf bytes.
            timeout: Optional timeout in seconds.
        """
        self._check_running()

        # Length-prefixed format: 4 bytes workflow_id length (big endian) +
        # workflow_id bytes + history protobuf bytes
        wf_id_bytes = workflow_id.encode("utf-8")
        data = (
            len(wf_id_bytes).to_bytes(4, byteorder="big") + wf_id_bytes + history_bytes
        )

        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            try:
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request("push_replay_history", data, deliver_result)

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()
            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("push_replay_history timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

    async def close_replay_pusher(self, timeout: Optional[float] = None) -> None:
        """Close the replay history pusher (no more histories will be pushed).

        Args:
            timeout: Optional timeout in seconds.
        """
        self._check_running()

        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            try:
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request("close_replay_pusher", b"", deliver_result)

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()
            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("close_replay_pusher timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

    async def poll_replay_activation(self, timeout: Optional[float] = None) -> bytes:
        """Poll for a workflow activation from the replay worker.

        Args:
            timeout: Optional timeout in seconds.

        Returns:
            Serialized workflow activation bytes (protobuf).
        """
        self._check_running()

        event = trio.Event()
        result_container: list = []
        error_container: list = []

        def deliver_result(result) -> None:
            try:
                if result.success:
                    data_bytes = result.get_data()
                    if data_bytes is None:
                        error_container.append(
                            RuntimeError(
                                "poll_replay_activation returned success without data"
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

        self._rust_bridge.send_request("poll_replay_activation", b"", deliver_result)

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()
            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("poll_replay_activation timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        return result_container[0]

    async def complete_replay_activation(
        self, completion_bytes: bytes, timeout: Optional[float] = None
    ) -> None:
        """Complete a workflow activation for the replay worker.

        Args:
            completion_bytes: Serialized workflow activation completion (protobuf).
            timeout: Optional timeout in seconds.
        """
        self._check_can_complete()

        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            try:
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.send_request(
            "complete_replay_activation", completion_bytes, deliver_result
        )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()
            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("complete_replay_activation timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

    async def initiate_replay_shutdown(self, timeout: Optional[float] = None) -> None:
        """Initiate graceful shutdown of the replay worker.

        Args:
            timeout: Optional timeout in seconds.
        """
        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            try:
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                try:
                    trio.from_thread.run_sync(event.set, trio_token=self._trio_token)
                except Exception:
                    pass

        try:
            self._rust_bridge.send_request(
                "initiate_replay_shutdown", b"", deliver_result
            )
        except RuntimeError:
            return

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()
            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("initiate_replay_shutdown timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

    async def finalize_replay_shutdown(self, timeout: Optional[float] = None) -> None:
        """Finalize shutdown of the replay worker.

        Args:
            timeout: Optional timeout in seconds.
        """
        event = trio.Event()
        error_container: list = []

        def deliver_result(result) -> None:
            try:
                if not result.success:
                    error_msg = result.error or "Unknown error"
                    error_container.append(RuntimeError(error_msg))
            except Exception as e:
                error_container.append(e)
            finally:
                try:
                    trio.from_thread.run_sync(event.set, trio_token=self._trio_token)
                except Exception:
                    pass

        try:
            self._rust_bridge.send_request(
                "finalize_replay_shutdown", b"", deliver_result
            )
        except RuntimeError:
            return

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()
            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError("finalize_replay_shutdown timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

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
