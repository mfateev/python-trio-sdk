"""Activity test environment for Trio.

Provides an environment for testing activities without a server.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import temporalio.common
import temporalio.converter
import trio

from temporalio_trio import activity

_utc_zero = datetime.fromtimestamp(0).replace(tzinfo=timezone.utc)
_default_info = activity.Info(
    activity_id="test",
    activity_type="unknown",
    attempt=1,
    current_attempt_scheduled_time=_utc_zero,
    heartbeat_details=[],
    heartbeat_timeout=None,
    is_local=False,
    schedule_to_close_timeout=timedelta(seconds=1),
    scheduled_time=_utc_zero,
    start_to_close_timeout=timedelta(seconds=1),
    started_time=_utc_zero,
    task_queue="test",
    task_token=b"test",
    workflow_id="test",
    workflow_namespace="default",
    workflow_run_id="test-run",
    workflow_type="test",
    priority=temporalio.common.Priority.default,
    retry_policy=None,
)


class ActivityEnvironment:
    """Activity environment for testing activities without a server.

    Attributes:
        info: The activity info returned from activity.info().
        on_heartbeat: Called on each heartbeat invocation.
        payload_converter: Payload converter for the activity context.
    """

    def __init__(self) -> None:
        """Create an ActivityEnvironment for running activity code."""
        self.info = _default_info
        self.on_heartbeat: Callable[..., None] = lambda *args: None
        self.payload_converter = (
            temporalio.converter.DataConverter.default.payload_converter
        )
        self._cancelled = False
        self._worker_shutdown = False
        self._activities: set[_Activity] = set()

    def cancel(self) -> None:
        """Cancel the activity.

        This only has an effect on the first call.
        """
        if self._cancelled:
            return
        self._cancelled = True
        for act in self._activities:
            act.cancel()

    def worker_shutdown(self) -> None:
        """Notify the activity that the worker is shutting down.

        This only has an effect on the first call.
        """
        if self._worker_shutdown:
            return
        self._worker_shutdown = True
        for act in self._activities:
            act.worker_shutdown()

    async def run(self, fn: Callable, *args: Any, **kwargs: Any) -> Any:
        """Run the given async callable in an activity context.

        Args:
            fn: The async function to run.
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            The function's result.
        """
        return await _Activity(self, fn).run(*args, **kwargs)


class _Activity:
    def __init__(self, env: ActivityEnvironment, fn: Callable) -> None:
        self.env = env
        self.fn = fn
        self.cancel_scope: trio.CancelScope | None = None
        self.context = activity._Context(
            info=lambda: env.info,
            heartbeat=lambda *args: env.on_heartbeat(*args),
            cancelled_event=activity._TrioEvent(trio.Event()),
            worker_shutdown_event=activity._TrioEvent(trio.Event()),
            payload_converter_class_or_instance=env.payload_converter,
        )

    async def run(self, *args: Any, **kwargs: Any) -> Any:
        if self.env._cancelled:
            self.context.cancelled_event.set()
        if self.env._worker_shutdown:
            self.context.worker_shutdown_event.set()
        self.env._activities.add(self)
        token = activity._Context.set(self.context)
        try:
            with trio.CancelScope() as scope:
                self.cancel_scope = scope
                if self.env._cancelled:
                    scope.cancel()
                return await self.fn(*args, **kwargs)
        finally:
            activity._Context.reset(token)
            self.env._activities.discard(self)

    def cancel(self) -> None:
        self.context.cancelled_event.set()
        if self.cancel_scope:
            self.cancel_scope.cancel()

    def worker_shutdown(self) -> None:
        self.context.worker_shutdown_event.set()
