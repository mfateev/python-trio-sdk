"""Activity definitions and context for Temporal with Trio.

This module provides activity functionality for the Trio-based Temporal SDK.
It mirrors temporalio.activity from the SDK but uses Trio primitives.

Important: This implementation is async-only. All activities must be defined
with `async def`. This is a deliberate design choice for the Trio SDK to
maintain simplicity and leverage Trio's structured concurrency model.

Most of these functions use contextvars to obtain the current activity
context. This is set before the activity starts and should be propagated
appropriately if making calls in separate tasks.
"""

from __future__ import annotations

import contextvars
import dataclasses
import inspect
import logging
from collections.abc import Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, NoReturn, overload

import temporalio.common
import temporalio.converter
import trio

from temporalio_trio.types import CallableType

__all__ = [
    "defn",
    "Info",
    "info",
    "heartbeat",
    "is_cancelled",
    "wait_for_cancelled",
    "is_worker_shutdown",
    "wait_for_worker_shutdown",
    "in_activity",
    "raise_complete_async",
    "payload_converter",
    "metric_meter",
    "logger",
    "LoggerAdapter",
    "_CompleteAsyncError",
    "_Definition",
    "_Context",
    "_TrioEvent",
]


@overload
def defn(fn: CallableType) -> CallableType: ...


@overload
def defn(
    *, name: str | None = None
) -> Callable[[CallableType], CallableType]: ...


@overload
def defn(
    *, dynamic: bool = False
) -> Callable[[CallableType], CallableType]: ...


def defn(
    fn: CallableType | None = None,
    *,
    name: str | None = None,
    dynamic: bool = False,
):
    """Decorator for activity functions.

    In this Trio-based SDK, activities must be async (defined with `async def`).
    This is a design choice to leverage Trio's structured concurrency model.

    Args:
        fn: The function to decorate.
        name: Name to use for the activity. Defaults to function ``__name__``.
            This cannot be set if dynamic is set.
        dynamic: If true, this activity will be dynamic. Dynamic activities have
            to accept a single 'Sequence[RawValue]' parameter. This cannot be
            set to true if name is present.

    Returns:
        The decorated function.

    Raises:
        TypeError: If the function is not async.

    Example:
        @activity.defn
        async def my_activity(arg: str) -> str:
            return f"Processed: {arg}"

        @activity.defn(name="custom-name")
        async def another_activity() -> None:
            pass
    """

    def decorator(fn: CallableType) -> CallableType:
        # Validate that the activity is async
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(
                f"Activity {fn.__name__} must be async (defined with 'async def'). "
                f"This Trio-based SDK only supports async activities."
            )
        # Apply the definition
        _Definition._apply_to_callable(
            fn,
            activity_name=name or fn.__name__ if not dynamic else None,
        )
        return fn

    if fn is not None:
        return decorator(fn)
    return decorator


@dataclass(frozen=True)
class Info:
    """Information about the running activity.

    Retrieved inside an activity via :py:func:`info`.
    """

    activity_id: str
    """Unique ID for this activity execution."""

    activity_type: str
    """Name of the activity type."""

    attempt: int
    """Current attempt number (starts at 1)."""

    current_attempt_scheduled_time: datetime
    """Time when this attempt was scheduled."""

    heartbeat_details: Sequence[Any]
    """Details from the last heartbeat (for retry)."""

    heartbeat_timeout: timedelta | None
    """Heartbeat timeout if set."""

    is_local: bool
    """Whether this is a local activity."""

    schedule_to_close_timeout: timedelta | None
    """Schedule to close timeout if set."""

    scheduled_time: datetime
    """Time when the activity was scheduled."""

    start_to_close_timeout: timedelta | None
    """Start to close timeout if set."""

    started_time: datetime
    """Time when this activity attempt started."""

    task_queue: str
    """Task queue this activity is running on."""

    task_token: bytes
    """Unique token for this activity task."""

    workflow_id: str
    """ID of the workflow that scheduled this activity."""

    workflow_namespace: str
    """Namespace of the workflow."""

    workflow_run_id: str
    """Run ID of the workflow."""

    workflow_type: str
    """Type of the workflow that scheduled this activity."""

    priority: temporalio.common.Priority
    """Priority of the activity."""

    retry_policy: temporalio.common.RetryPolicy | None
    """Retry policy for this activity, if set."""

    def _logger_details(self) -> Mapping[str, Any]:
        """Get details for logging."""
        return {
            "activity_id": self.activity_id,
            "activity_type": self.activity_type,
            "attempt": self.attempt,
            "namespace": self.workflow_namespace,
            "task_queue": self.task_queue,
            "workflow_id": self.workflow_id,
            "workflow_run_id": self.workflow_run_id,
            "workflow_type": self.workflow_type,
        }


@dataclass
class _TrioEvent:
    """Wrapper for trio.Event providing a consistent interface.

    This is a simplified version of the SDK's _CompositeEvent that uses
    only Trio primitives (no threading.Event since we're async-only).
    """

    trio_event: trio.Event

    def set(self) -> None:
        """Set the event."""
        self.trio_event.set()

    def is_set(self) -> bool:
        """Check if the event is set."""
        return self.trio_event.is_set()

    async def wait(self) -> None:
        """Async wait for the event to be set."""
        await self.trio_event.wait()


@dataclass
class _Context:
    """Activity context stored in ContextVar.

    This holds all the state for a running activity, including callbacks
    for getting info and sending heartbeats.
    """

    info: Callable[[], Info]
    """Callback to get activity info."""

    heartbeat: Callable[..., None] | None
    """Callback to send heartbeat (None during interceptor init)."""

    cancelled_event: _TrioEvent
    """Event that gets set when cancellation is requested."""

    worker_shutdown_event: _TrioEvent
    """Event that gets set when worker shutdown is requested."""

    payload_converter_class_or_instance: (
        type[temporalio.converter.PayloadConverter]
        | temporalio.converter.PayloadConverter
    )
    """Payload converter for serialization."""

    _logger_details: Mapping[str, Any] | None = None
    _payload_converter: temporalio.converter.PayloadConverter | None = None

    @staticmethod
    def current() -> _Context:
        """Get the current activity context.

        Returns:
            The current activity context.

        Raises:
            RuntimeError: If not in an activity context.
        """
        context = _current_context.get(None)
        if not context:
            raise RuntimeError("Not in activity context")
        return context

    @staticmethod
    def set(context: _Context) -> contextvars.Token:
        """Set the current activity context.

        Args:
            context: The context to set.

        Returns:
            Token that can be used to reset the context.
        """
        return _current_context.set(context)

    @staticmethod
    def reset(token: contextvars.Token) -> None:
        """Reset the activity context using a token.

        Args:
            token: Token from a previous set() call.
        """
        _current_context.reset(token)

    @property
    def logger_details(self) -> Mapping[str, Any]:
        """Get details for logging."""
        if self._logger_details is None:
            self._logger_details = self.info()._logger_details()
        return self._logger_details

    @property
    def payload_converter(self) -> temporalio.converter.PayloadConverter:
        """Get the payload converter."""
        if not self._payload_converter:
            if isinstance(
                self.payload_converter_class_or_instance,
                temporalio.converter.PayloadConverter,
            ):
                self._payload_converter = self.payload_converter_class_or_instance
            else:
                self._payload_converter = self.payload_converter_class_or_instance()
        return self._payload_converter


# ContextVar for current activity context
_current_context: contextvars.ContextVar[_Context] = contextvars.ContextVar("activity")


def in_activity() -> bool:
    """Whether the current code is inside an activity.

    Returns:
        True if in an activity, False otherwise.
    """
    return _current_context.get(None) is not None


def info() -> Info:
    """Get the current activity's info.

    Returns:
        Info for the currently running activity.

    Raises:
        RuntimeError: When not in an activity.
    """
    return _Context.current().info()


def heartbeat(*details: Any) -> None:
    """Send a heartbeat for the current activity.

    Heartbeats are throttled by the worker to avoid overwhelming the server.
    If heartbeat fails (e.g., activity cancelled), this is reported asynchronously.

    Args:
        *details: Optional details to include with the heartbeat. These are
            available in heartbeat_details on retry.

    Raises:
        RuntimeError: When not in an activity or heartbeat not initialized.
    """
    heartbeat_fn = _Context.current().heartbeat
    if not heartbeat_fn:
        raise RuntimeError("Can only execute heartbeat after interceptor init")
    heartbeat_fn(*details)


def is_cancelled() -> bool:
    """Whether a cancellation was ever requested on this activity.

    Returns:
        True if the activity has had a cancellation request, False otherwise.

    Raises:
        RuntimeError: When not in an activity.
    """
    return _Context.current().cancelled_event.is_set()


async def wait_for_cancelled() -> None:
    """Asynchronously wait for this activity to get a cancellation request.

    Raises:
        RuntimeError: When not in an activity.
    """
    await _Context.current().cancelled_event.wait()


def is_worker_shutdown() -> bool:
    """Whether shutdown has been invoked on the worker.

    Returns:
        True if shutdown has been called on the worker, False otherwise.

    Raises:
        RuntimeError: When not in an activity.
    """
    return _Context.current().worker_shutdown_event.is_set()


async def wait_for_worker_shutdown() -> None:
    """Asynchronously wait for shutdown to be called on the worker.

    Raises:
        RuntimeError: When not in an activity.
    """
    await _Context.current().worker_shutdown_event.wait()


def payload_converter() -> temporalio.converter.PayloadConverter:
    """Get the payload converter for the current activity.

    This is often used for dynamic activities to convert payloads.

    Returns:
        The payload converter for the current activity.

    Raises:
        RuntimeError: When not in an activity.
    """
    return _Context.current().payload_converter


def raise_complete_async() -> NoReturn:
    """Raise an error that says the activity will be completed asynchronously.

    This is used when an activity wants to be completed by an external process
    rather than returning a value directly. The activity must be completed
    using the async completion client with the activity's task token.

    Raises:
        _CompleteAsyncError: Always raised to signal async completion.
    """
    raise _CompleteAsyncError()


class _CompleteAsyncError(BaseException):
    """Error that is raised to signal that an activity will be completed
    asynchronously.
    """

    pass


def metric_meter() -> temporalio.common.MetricMeter:
    """Get the metric meter for the current activity.

    Returns a no-op metric meter since full metrics support is not yet
    implemented. This allows activity code that calls metric_meter() to
    work without breaking.

    Returns:
        A no-op metric meter.

    Raises:
        RuntimeError: When not in an activity.
    """
    # Verify we're in an activity context
    _Context.current()
    return temporalio.common.MetricMeter.noop


class LoggerAdapter(logging.LoggerAdapter):
    """Adapter that adds details to the log about the running activity.

    Attributes:
        activity_info_on_message: Boolean for whether a string representation of
            a dict of some activity info will be appended to each message.
            Default is True.
        activity_info_on_extra: Boolean for whether a ``temporal_activity``
            dictionary value will be added to the ``extra`` dictionary with some
            activity info, making it present on the ``LogRecord.__dict__`` for
            use by others. Default is True.
        full_activity_info_on_extra: Boolean for whether an ``activity_info``
            value will be added to the ``extra`` dictionary with the entire
            activity info, making it present on the ``LogRecord.__dict__`` for
            use by others. Default is False.
    """

    def __init__(self, logger: logging.Logger, extra: Mapping[str, Any] | None) -> None:
        """Create the logger adapter."""
        super().__init__(logger, extra or {})
        self.activity_info_on_message = True
        self.activity_info_on_extra = True
        self.full_activity_info_on_extra = False

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        """Override to add activity details."""
        if (
            self.activity_info_on_message
            or self.activity_info_on_extra
            or self.full_activity_info_on_extra
        ):
            context = _current_context.get(None)
            if context:
                if self.activity_info_on_message:
                    msg = f"{msg} ({context.logger_details})"
                if self.activity_info_on_extra:
                    extra = kwargs.get("extra", None) or {}
                    extra["temporal_activity"] = context.logger_details
                    kwargs["extra"] = extra
                if self.full_activity_info_on_extra:
                    extra = kwargs.get("extra", None) or {}
                    extra["activity_info"] = context.info()
                    kwargs["extra"] = extra
        return (msg, kwargs)

    @property
    def base_logger(self) -> logging.Logger:
        """Underlying logger usable for actions such as adding
        handlers/formatters.
        """
        return self.logger


logger = LoggerAdapter(logging.getLogger(__name__), None)
"""Logger that will have contextual activity details embedded."""


@dataclass(frozen=True)
class _Definition:
    """Activity definition metadata.

    This stores metadata about an activity function that was decorated with
    @activity.defn. The definition is attached to the function and can be
    retrieved using from_callable() or must_from_callable().
    """

    name: str | None
    """Name of the activity (used for registration and lookup). None for dynamic activities."""

    fn: Callable
    """The activity function."""

    is_async: bool
    """Whether the activity is async (always True for this SDK)."""

    # Types loaded on post init if both are None
    arg_types: list[type] | None = None
    ret_type: type | None = None

    @staticmethod
    def from_callable(fn: Callable) -> _Definition | None:
        """Get definition from a decorated function.

        Args:
            fn: The function to get definition from.

        Returns:
            The activity definition, or None if not an activity.
        """
        defn = getattr(fn, "__temporal_activity_definition", None)
        if isinstance(defn, _Definition):
            # Replace the function with the given callable in case
            # it's a method or partial
            defn = dataclasses.replace(defn, fn=fn)
        return defn

    @staticmethod
    def must_from_callable(fn: Callable) -> _Definition:
        """Get definition from a decorated function, raising if not found.

        Args:
            fn: The function to get definition from.

        Returns:
            The activity definition.

        Raises:
            TypeError: If the function is not an activity.
        """
        ret = _Definition.from_callable(fn)
        if ret:
            return ret
        fn_name = getattr(fn, "__name__", "<unknown>")
        raise TypeError(
            f"Activity {fn_name} missing attributes, was it decorated with @activity.defn?"
        )

    @staticmethod
    def _apply_to_callable(fn: Callable, *, activity_name: str | None) -> None:
        """Apply activity definition to a callable.

        Args:
            fn: The function to apply definition to.
            activity_name: Name for the activity. None for dynamic activities.

        Raises:
            ValueError: If function already has activity definition.
            TypeError: If function is not callable or not async.
        """
        if hasattr(fn, "__temporal_activity_definition"):
            raise ValueError("Function already contains activity definition")
        elif not callable(fn):
            raise TypeError("Activity is not callable")

        # Validate no keyword-only arguments
        sig = inspect.signature(fn)
        for param in sig.parameters.values():
            if param.kind == inspect.Parameter.KEYWORD_ONLY:
                raise TypeError("Activity cannot have keyword-only arguments")

        # Validate it's async (already done in decorator, but double-check)
        if not inspect.iscoroutinefunction(fn):
            raise TypeError(
                f"Activity {fn.__name__} must be async (defined with 'async def')"
            )

        setattr(
            fn,
            "__temporal_activity_definition",
            _Definition(
                name=activity_name,
                fn=fn,
                is_async=True,  # Always true for this SDK
            ),
        )

    def __post_init__(self) -> None:
        """Post-init to load type hints."""
        if self.arg_types is None and self.ret_type is None:
            dynamic = self.name is None
            arg_types, ret_type = temporalio.common._type_hints_from_func(self.fn)
            # If dynamic, must be a sequence of raw values
            if dynamic and (
                not arg_types
                or len(arg_types) != 1
                or arg_types[0] != Sequence[temporalio.common.RawValue]
            ):
                raise TypeError(
                    "Dynamic activity must accept a single Sequence[temporalio.common.RawValue]"
                )
            object.__setattr__(self, "arg_types", arg_types)
            object.__setattr__(self, "ret_type", ret_type)
