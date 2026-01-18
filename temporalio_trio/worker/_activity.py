"""Trio-based activity worker that polls from the Temporal bridge.

This module provides TrioActivityWorker which executes activities using Trio
for the async runtime instead of asyncio.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Any, Callable, Sequence

import temporalio.bridge.proto
import temporalio.bridge.proto.activity_result
import temporalio.bridge.proto.activity_task
import temporalio.common
import temporalio.converter
import trio

from temporalio_trio import activity
from temporalio_trio._async_bridge import TrioBridgeWrapper

if TYPE_CHECKING:
    pass

__all__ = ["TrioActivityWorker"]

logger = logging.getLogger(__name__)


@dataclass
class _RunningActivity:
    """State for a running activity."""

    task_token: bytes
    """Unique token for this activity task."""

    info: activity.Info
    """Activity info."""

    cancelled_event: activity._TrioEvent
    """Event to signal cancellation."""

    heartbeat_send: trio.MemorySendChannel[tuple[Any, ...]]
    """Channel to send heartbeat details."""

    heartbeat_receive: trio.MemoryReceiveChannel[tuple[Any, ...]]
    """Channel to receive heartbeat details."""

    cancel_scope: trio.CancelScope | None = None
    """Cancel scope for the activity execution."""


class TrioActivityWorker:
    """Trio-based activity worker that polls from the bridge.

    This worker:
    1. Polls activity tasks from the Temporal bridge
    2. Executes activities in the Trio event loop
    3. Handles heartbeats with throttling
    4. Handles cancellation
    5. Sends completions back to the bridge

    All activities must be async (defined with `async def`).

    Args:
        bridge_wrapper: Bridge wrapper instance for async polling
        task_queue: Task queue name
        activities: List of activity functions to register
        data_converter: Data converter for payload serialization
        max_heartbeat_throttle_interval: Max time between heartbeats
        default_heartbeat_throttle_interval: Default time between heartbeats
    """

    def __init__(
        self,
        bridge_wrapper: TrioBridgeWrapper,
        task_queue: str,
        activities: Sequence[Callable[..., Any]],
        data_converter: temporalio.converter.DataConverter | None = None,
        max_heartbeat_throttle_interval: timedelta = timedelta(seconds=60),
        default_heartbeat_throttle_interval: timedelta = timedelta(seconds=30),
    ) -> None:
        """Initialize the Trio activity worker."""
        self._bridge = bridge_wrapper
        self._task_queue = task_queue
        self._data_converter = data_converter or temporalio.converter.DataConverter()
        self._max_heartbeat_throttle_interval = max_heartbeat_throttle_interval
        self._default_heartbeat_throttle_interval = default_heartbeat_throttle_interval
        self._shutdown_event = trio.Event()
        self._worker_shutdown_event = activity._TrioEvent(trio.Event())

        # Prepare activities
        self._activities: dict[str, activity._Definition] = {}
        for activity_fn in activities:
            defn = activity._Definition.must_from_callable(activity_fn)
            if not defn.is_async:
                raise TypeError(
                    f"Activity {defn.name} must be async (defined with 'async def'). "
                    f"This Trio-based SDK only supports async activities."
                )
            if defn.name in self._activities:
                raise ValueError(f"Duplicate activity name: {defn.name}")
            self._activities[defn.name] = defn

        # Track running activities by task token
        self._running_activities: dict[bytes, _RunningActivity] = {}

    async def run(self) -> None:
        """Run the worker until shutdown.

        This method:
        1. Continuously polls for activity tasks
        2. Dispatches them in parallel using a nursery
        3. Handles shutdown gracefully
        """
        logger.info(f"Starting Trio activity worker on {self._task_queue}")

        async with trio.open_nursery() as nursery:
            # Start the polling loop
            nursery.start_soon(self._poll_loop)

            # Wait for shutdown signal
            await self._shutdown_event.wait()

            # Signal worker shutdown to running activities
            self._worker_shutdown_event.set()

            # Cancel the nursery to stop polling and activities
            nursery.cancel_scope.cancel()

        logger.info("Trio activity worker stopped")

    async def _poll_loop(self) -> None:
        """Poll for activity tasks until shutdown."""
        try:
            while not self._shutdown_event.is_set():
                try:
                    task_bytes = await self._bridge.poll_activity_task()
                    task = temporalio.bridge.proto.activity_task.ActivityTask()
                    task.ParseFromString(task_bytes)
                except Exception as e:
                    error_str = str(e)
                    # Check if it's a shutdown error
                    if (
                        e.__class__.__name__ == "PollShutdownError"
                        or "PollShutdownError" in error_str
                        or "shutdown" in error_str.lower()
                    ):
                        logger.info("Activity worker shutdown signal received")
                        break
                    # Log and continue for other errors
                    logger.warning(f"Activity poll error: {error_str}")
                    await trio.sleep(1.0)  # Brief delay before retrying
                    continue

                # Handle the activity task
                async with trio.open_nursery() as task_nursery:
                    task_nursery.start_soon(self._handle_task, task)

        except Exception:
            logger.exception("Error in activity poll loop")
            raise

    async def _handle_task(
        self,
        task: temporalio.bridge.proto.activity_task.ActivityTask,
    ) -> None:
        """Handle an activity task.

        Routes to start or cancel handler based on task type.
        """
        task_token = task.task_token

        if task.HasField("start"):
            await self._handle_start_activity(task_token, task.start)
        elif task.HasField("cancel"):
            self._handle_cancel_activity(task_token, task.cancel)
        else:
            logger.warning(f"Unknown activity task type for token {task_token!r}")

    async def _handle_start_activity(
        self,
        task_token: bytes,
        start: temporalio.bridge.proto.activity_task.Start,
    ) -> None:
        """Execute an activity.

        Sets up context, executes the activity function, and sends completion.
        """
        activity_type = start.activity_type

        # Find the activity definition
        defn = self._activities.get(activity_type)
        if defn is None:
            logger.error(f"Unknown activity type: {activity_type}")
            await self._send_failure(
                task_token,
                f"Activity type not found: {activity_type}",
                "ActivityNotFoundError",
            )
            return

        # Create activity info from protobuf
        info = self._create_info(task_token, start)

        # Set up cancellation event
        cancelled_event = activity._TrioEvent(trio.Event())

        # Set up heartbeat channel (with buffer)
        heartbeat_send, heartbeat_receive = trio.open_memory_channel[tuple[Any, ...]](
            1000
        )

        # Create running activity state
        running = _RunningActivity(
            task_token=task_token,
            info=info,
            cancelled_event=cancelled_event,
            heartbeat_send=heartbeat_send,
            heartbeat_receive=heartbeat_receive,
        )
        self._running_activities[task_token] = running

        # Create heartbeat callback
        def queue_heartbeat(*details: Any) -> None:
            """Queue heartbeat for processing."""
            try:
                running.heartbeat_send.send_nowait(details)
            except trio.WouldBlock:
                logger.warning("Heartbeat queue full, dropping heartbeat")
            except trio.ClosedResourceError:
                pass  # Activity finished

        # Create activity context
        context = activity._Context(
            info=lambda: info,
            heartbeat=queue_heartbeat,
            cancelled_event=cancelled_event,
            worker_shutdown_event=self._worker_shutdown_event,
            payload_converter_class_or_instance=self._data_converter.payload_converter,
        )

        # Decode arguments
        try:
            args = await self._decode_args(start.input, defn)
        except Exception as e:
            logger.error(f"Failed to decode activity arguments: {e}")
            await self._send_failure(task_token, str(e), "InputDecodingError")
            return

        # Execute the activity with context
        token = activity._Context.set(context)
        try:
            async with trio.open_nursery() as nursery:
                # Store cancel scope for potential cancellation
                running.cancel_scope = nursery.cancel_scope

                # Start heartbeat processor
                nursery.start_soon(
                    self._process_heartbeats, task_token, heartbeat_receive
                )

                try:
                    # Execute the activity
                    result = await defn.fn(*args)

                    # Encode and send success result
                    await self._send_success(task_token, result, defn)

                except trio.Cancelled:
                    # Activity was cancelled
                    await self._send_cancellation(task_token)
                    raise
                except Exception as e:
                    # Activity failed with exception
                    logger.exception(f"Activity {activity_type} failed")
                    await self._send_failure(task_token, str(e), type(e).__name__)

                # Cancel heartbeat processor
                nursery.cancel_scope.cancel()

        finally:
            activity._Context.reset(token)
            # Clean up
            del self._running_activities[task_token]
            await heartbeat_send.aclose()
            await heartbeat_receive.aclose()

    def _handle_cancel_activity(
        self,
        task_token: bytes,
        cancel: temporalio.bridge.proto.activity_task.Cancel,
    ) -> None:
        """Handle activity cancellation request.

        Sets the cancellation event for the running activity.
        """
        running = self._running_activities.get(task_token)
        if running is None:
            logger.warning(f"Cancel for unknown activity token: {task_token!r}")
            return

        logger.info(f"Cancelling activity {running.info.activity_type}")
        running.cancelled_event.set()

        # Also cancel the activity's cancel scope if available
        if running.cancel_scope is not None:
            running.cancel_scope.cancel()

    async def _process_heartbeats(
        self,
        task_token: bytes,
        receive_channel: trio.MemoryReceiveChannel[tuple[Any, ...]],
    ) -> None:
        """Process heartbeats with throttling.

        Sends heartbeats to the server at most once per throttle interval.
        """
        running = self._running_activities.get(task_token)
        if running is None:
            return

        # Calculate throttle interval based on heartbeat timeout
        info = running.info
        if info.heartbeat_timeout:
            # Use 80% of heartbeat timeout, but not more than max
            interval = min(
                info.heartbeat_timeout.total_seconds() * 0.8,
                self._max_heartbeat_throttle_interval.total_seconds(),
            )
        else:
            interval = self._default_heartbeat_throttle_interval.total_seconds()

        last_heartbeat_time = 0.0
        pending_details: tuple[Any, ...] | None = None

        try:
            async with receive_channel:
                async for details in receive_channel:
                    pending_details = details
                    now = trio.current_time()

                    # Check if we should send now
                    if now - last_heartbeat_time >= interval:
                        await self._send_heartbeat(task_token, pending_details)
                        last_heartbeat_time = now
                        pending_details = None
        except trio.Cancelled:
            # Send any pending heartbeat before shutdown
            if pending_details is not None:
                try:
                    await self._send_heartbeat(task_token, pending_details)
                except Exception:
                    pass  # Best effort
            raise

    async def _send_heartbeat(
        self, task_token: bytes, details: tuple[Any, ...]
    ) -> None:
        """Send heartbeat to the server."""
        running = self._running_activities.get(task_token)
        if running is None:
            return

        try:
            # Encode details to payloads
            payloads = None
            if details:
                payloads = await self._data_converter.encode(list(details))

            # Create heartbeat request
            heartbeat = temporalio.bridge.proto.ActivityHeartbeat()
            heartbeat.task_token = task_token
            if payloads:
                heartbeat.details.CopyFrom(payloads)

            # Send to bridge
            response_bytes = await self._bridge.record_activity_heartbeat(
                heartbeat.SerializeToString()
            )

            # Note: Cancellation info comes via poll_activity_task (Cancel task type),
            # not from heartbeat response. The response is empty on success.
            # SDK Core handles heartbeat delivery internally.

        except Exception as e:
            # Heartbeat failure should cancel the activity (matches SDK behavior)
            logger.warning(f"Heartbeat failed, cancelling activity: {e}")
            running.cancelled_event.set()
            if running.cancel_scope is not None:
                running.cancel_scope.cancel()

    async def _send_success(
        self, task_token: bytes, result: Any, defn: activity._Definition
    ) -> None:
        """Send activity success completion."""
        completion = temporalio.bridge.proto.ActivityTaskCompletion()
        completion.task_token = task_token

        # Encode result - encode returns a list of Payload objects
        if result is not None:
            payloads = await self._data_converter.encode([result])
            if payloads:
                # CopyFrom the first (and only) payload
                completion.result.completed.result.CopyFrom(payloads[0])
        else:
            completion.result.completed.SetInParent()

        await self._bridge.complete_activity_task(completion.SerializeToString())

    async def _send_failure(
        self, task_token: bytes, message: str, error_type: str
    ) -> None:
        """Send activity failure completion."""
        completion = temporalio.bridge.proto.ActivityTaskCompletion()
        completion.task_token = task_token
        completion.result.failed.failure.message = message
        completion.result.failed.failure.source = "PythonSDK"
        completion.result.failed.failure.application_failure_info.type = error_type

        await self._bridge.complete_activity_task(completion.SerializeToString())

    async def _send_cancellation(self, task_token: bytes) -> None:
        """Send activity cancellation completion."""
        completion = temporalio.bridge.proto.ActivityTaskCompletion()
        completion.task_token = task_token
        completion.result.cancelled.failure.message = "Activity cancelled"
        completion.result.cancelled.failure.cancelled_failure_info.SetInParent()

        await self._bridge.complete_activity_task(completion.SerializeToString())

    def _create_info(
        self,
        task_token: bytes,
        start: temporalio.bridge.proto.activity_task.Start,
    ) -> activity.Info:
        """Create activity Info from start message."""

        # Helper to convert protobuf duration to timedelta
        def duration_to_timedelta(d: Any) -> timedelta | None:
            if d is None or not d.ByteSize():
                return None
            return timedelta(seconds=d.seconds, microseconds=d.nanos // 1000)

        # Helper to convert protobuf timestamp to datetime
        def timestamp_to_datetime(ts: Any) -> datetime:
            if ts is None or not ts.ByteSize():
                return datetime.now(timezone.utc)
            return datetime.fromtimestamp(ts.seconds + ts.nanos / 1e9, tz=timezone.utc)

        # Decode heartbeat details from previous attempt (for retries)
        heartbeat_details: list[Any] = []
        if start.heartbeat_details and len(start.heartbeat_details.payloads) > 0:
            try:
                # Decode heartbeat details synchronously
                # These are details from the previous attempt's last heartbeat
                for payload in start.heartbeat_details.payloads:
                    value = self._data_converter.payload_converter.from_payload(payload)
                    heartbeat_details.append(value)
            except Exception as e:
                logger.warning(f"Failed to decode heartbeat details: {e}")

        # Create retry policy from proto
        retry_policy = None
        if start.HasField("retry_policy"):
            rp = start.retry_policy
            retry_policy = temporalio.common.RetryPolicy(
                initial_interval=(
                    timedelta(
                        seconds=rp.initial_interval.seconds,
                        microseconds=rp.initial_interval.nanos // 1000,
                    )
                    if rp.HasField("initial_interval")
                    else timedelta(seconds=1)
                ),
                backoff_coefficient=rp.backoff_coefficient or 2.0,
                maximum_interval=(
                    timedelta(
                        seconds=rp.maximum_interval.seconds,
                        microseconds=rp.maximum_interval.nanos // 1000,
                    )
                    if rp.HasField("maximum_interval")
                    else None
                ),
                maximum_attempts=rp.maximum_attempts or 0,
            )

        return activity.Info(
            activity_id=start.activity_id,
            activity_type=start.activity_type,
            attempt=start.attempt,
            current_attempt_scheduled_time=timestamp_to_datetime(
                start.current_attempt_scheduled_time
            ),
            heartbeat_details=heartbeat_details,
            heartbeat_timeout=duration_to_timedelta(start.heartbeat_timeout),
            is_local=start.is_local,
            schedule_to_close_timeout=duration_to_timedelta(
                start.schedule_to_close_timeout
            ),
            scheduled_time=timestamp_to_datetime(start.scheduled_time),
            start_to_close_timeout=duration_to_timedelta(start.start_to_close_timeout),
            started_time=timestamp_to_datetime(start.started_time),
            task_queue=self._task_queue,
            task_token=task_token,
            workflow_id=start.workflow_execution.workflow_id,
            workflow_namespace=start.workflow_namespace,
            workflow_run_id=start.workflow_execution.run_id,
            workflow_type=start.workflow_type,
            priority=temporalio.common.Priority._from_proto(start.priority),
            retry_policy=retry_policy,
        )

    async def _decode_args(
        self,
        input_payloads: Any,  # Protobuf payloads (RepeatedCompositeContainer)
        defn: activity._Definition,
    ) -> tuple[Any, ...]:
        """Decode activity arguments from payloads.

        Note: input_payloads is a RepeatedCompositeContainer (list-like),
        not an object with a .payloads attribute.
        """
        if not input_payloads or len(input_payloads) == 0:
            return ()

        # Get type hints for the activity
        type_hints = defn.arg_types or []

        # Decode each payload
        args = []
        for i, payload in enumerate(input_payloads):
            type_hint = type_hints[i] if i < len(type_hints) else None
            type_hint_list: list[type] = [type_hint] if type_hint is not None else []
            value = await self._data_converter.decode([payload], type_hint_list)
            args.append(value[0] if value else None)

        return tuple(args)

    def shutdown(self) -> None:
        """Initiate graceful shutdown of the worker.

        This signals the worker to stop polling and complete in-flight activities.
        """
        logger.info("Initiating activity worker shutdown")
        self._shutdown_event.set()
