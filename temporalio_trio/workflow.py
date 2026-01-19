"""Workflow definitions and runtime for Temporal with Trio.

This module mirrors temporalio.workflow from the SDK.
"""

from __future__ import annotations

import inspect
from abc import ABC, abstractmethod
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Awaitable, Callable, TypeVar

import temporalio.common

__all__ = [
    "defn",
    "run",
    "signal",
    "sleep",
    "time",
    "time_ns",
    "info",
    "execute_activity",
    "Info",
    "_Runtime",
    "_Definition",
    "_SignalDefinition",
    "_NotInWorkflowContextError",
]

# Type variable for decorator
F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")

# Context variable for current runtime (Trio doesn't have event loops like asyncio)
_current_runtime: ContextVar[_Runtime | None] = ContextVar(
    "_current_runtime", default=None
)


class _NotInWorkflowContextError(RuntimeError):
    """Raised when workflow API is called outside workflow context."""

    pass


class _Runtime(ABC):
    """Abstract runtime that provides workflow APIs.

    Mirrors temporalio.workflow._Runtime from the SDK.

    All workflow APIs (sleep, time, info, etc.) delegate to _Runtime.current().
    This pattern allows adding features without changing the public API.
    """

    @staticmethod
    def current() -> _Runtime:
        """Get the current runtime, raising if not in a workflow context.

        Returns:
            The current workflow runtime.

        Raises:
            _NotInWorkflowContextError: If not in a workflow context.
        """
        runtime = _current_runtime.get()
        if runtime is None:
            raise _NotInWorkflowContextError(
                "Not in workflow context. "
                "This function must be called from within a workflow."
            )
        return runtime

    @staticmethod
    def maybe_current() -> _Runtime | None:
        """Get the current runtime or None if not in workflow context.

        Returns:
            The current workflow runtime, or None.
        """
        return _current_runtime.get()

    @staticmethod
    def set_current(runtime: _Runtime | None) -> Token[_Runtime | None]:
        """Set the current runtime.

        Args:
            runtime: The runtime to set, or None to clear.

        Returns:
            Token that can be used to reset the runtime.
        """
        return _current_runtime.set(runtime)

    @staticmethod
    def reset_current(token: Token[_Runtime | None]) -> None:
        """Reset the current runtime using a token.

        Args:
            token: Token from a previous set_current call.
        """
        _current_runtime.reset(token)

    # Abstract methods - implemented by TrioWorkflowInstance
    @abstractmethod
    def workflow_time_ns(self) -> int:
        """Get current workflow time in nanoseconds.

        Returns:
            Current workflow time in nanoseconds since epoch.
        """
        ...

    @abstractmethod
    async def workflow_sleep(self, duration: float, summary: str | None) -> None:
        """Sleep for the given duration.

        Args:
            duration: Sleep duration in seconds.
            summary: Optional description for debugging.
        """
        ...

    @abstractmethod
    def workflow_info(self) -> Info:
        """Get information about the current workflow.

        Returns:
            Info about the current workflow execution.
        """
        ...

    @abstractmethod
    async def workflow_execute_activity(
        self,
        activity: str | Callable[..., Any],
        *args: Any,
        task_queue: str | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        activity_id: str | None = None,
    ) -> Any:
        """Execute an activity and wait for its result.

        Args:
            activity: Activity name or function reference.
            *args: Arguments to pass to the activity.
            task_queue: Task queue to run the activity on. Defaults to workflow's queue.
            schedule_to_close_timeout: Max time for activity from schedule to completion.
            schedule_to_start_timeout: Max time waiting for worker to pick up activity.
            start_to_close_timeout: Max time for activity execution.
            heartbeat_timeout: Max time between heartbeats.
            retry_policy: Retry policy for the activity.
            activity_id: Optional unique identifier for the activity.

        Returns:
            The activity result.

        Raises:
            RuntimeError: If the activity fails or is cancelled.
        """
        ...


@dataclass
class _SignalDefinition:
    """Signal handler definition metadata."""

    name: str | None  # None for dynamic handlers
    fn: Callable[..., None | Awaitable[None]]
    is_method: bool
    description: str | None = None

    @staticmethod
    def from_fn(fn: Callable) -> "_SignalDefinition | None":
        """Get signal definition from a function if it has one."""
        return getattr(fn, "__temporal_signal_definition", None)


@dataclass
class _Definition:
    """Workflow definition metadata.

    Mirrors temporalio.workflow._Definition from the SDK.

    This stores metadata about a workflow class that was decorated with
    @workflow.defn. The definition is attached to the class and can be
    retrieved using from_class() or must_from_class().
    """

    name: str
    """Name of the workflow (used for registration and lookup)."""

    cls: type
    """The workflow class."""

    run_fn: Callable[..., Awaitable[Any]]
    """The method decorated with @workflow.run."""

    signals: dict[str | None, _SignalDefinition] = field(default_factory=dict)
    """Signal handlers for this workflow, keyed by signal name (None for dynamic)."""

    _ATTR_NAME: str = "__temporal_workflow_definition"

    @staticmethod
    def from_class(workflow_cls: type) -> _Definition | None:
        """Get definition from a decorated class.

        Args:
            workflow_cls: The class to get definition from.

        Returns:
            The workflow definition, or None if not a workflow class.
        """
        return getattr(workflow_cls, _Definition._ATTR_NAME, None)

    @staticmethod
    def must_from_class(workflow_cls: type) -> _Definition:
        """Get definition from a decorated class, raising if not found.

        Args:
            workflow_cls: The class to get definition from.

        Returns:
            The workflow definition.

        Raises:
            ValueError: If the class is not a workflow class.
        """
        defn = _Definition.from_class(workflow_cls)
        if defn is None:
            raise ValueError(
                f"{workflow_cls.__name__} is not a workflow class. "
                f"Did you forget to add @workflow.defn?"
            )
        return defn


def run(fn: F) -> F:
    """Decorator for workflow run method.

    Mirrors @temporalio.workflow.run from the SDK.

    This decorator marks a method as the main entry point for a workflow.
    Each workflow class must have exactly one method decorated with @workflow.run.

    The decorated method must be an async function.

    Example:
        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self, arg: str) -> str:
                return f"Hello, {arg}!"

    Args:
        fn: The async method to decorate.

    Returns:
        The decorated method.

    Raises:
        ValueError: If the method is not async.
    """
    if not inspect.iscoroutinefunction(fn):
        raise ValueError(
            f"Workflow run method {fn.__name__} must be async (defined with 'async def')"
        )
    setattr(fn, "__temporal_workflow_run", True)
    return fn


def signal(
    fn: Callable | None = None,
    *,
    name: str | None = None,
    dynamic: bool = False,
    description: str | None = None,
) -> Any:
    """Decorator for workflow signal handler methods.

    Signal handlers can be sync or async and receive arguments from the signal sender.

    Example:
        @workflow.defn
        class MyWorkflow:
            @workflow.signal
            def my_signal(self, value: int) -> None:
                self.value = value

            @workflow.signal(name="custom-name")
            async def another_signal(self) -> None:
                pass

    Args:
        fn: The function to decorate (when used without parentheses)
        name: Custom signal name. Defaults to function name.
        dynamic: If True, this is a dynamic handler for any signal name.
        description: Optional description for the signal.
    """

    def decorator(fn: Callable) -> Callable:
        if dynamic:
            signal_name = None
        elif name:
            signal_name = name
        else:
            signal_name = fn.__name__

        defn = _SignalDefinition(
            name=signal_name,
            fn=fn,
            is_method=True,
            description=description,
        )
        setattr(fn, "__temporal_signal_definition", defn)
        return fn

    if fn is not None:
        return decorator(fn)
    return decorator


def defn(cls: type | None = None, *, name: str | None = None) -> Any:
    """Decorator for workflow classes.

    Mirrors @temporalio.workflow.defn from the SDK.

    This decorator marks a class as a Temporal workflow. The class must have
    exactly one method decorated with @workflow.run.

    Example:
        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self, arg: str) -> str:
                await workflow.sleep(10)
                return f"Hello, {arg}!"

        # With custom name
        @workflow.defn(name="CustomWorkflowName")
        class AnotherWorkflow:
            @workflow.run
            async def run(self) -> None:
                pass

    Args:
        cls: The class to decorate (when used without parentheses).
        name: Optional custom workflow name. Defaults to class name.

    Returns:
        The decorated class, or a decorator function.

    Raises:
        ValueError: If the class doesn't have a @workflow.run method,
            or has multiple @workflow.run methods.
    """

    def decorator(cls: type) -> type:
        run_fn: Callable[..., Awaitable[Any]] | None = None
        signals: dict[str | None, _SignalDefinition] = {}

        for attr_name in dir(cls):
            if attr_name.startswith("_"):
                continue

            method = getattr(cls, attr_name, None)
            if method is None:
                continue

            if getattr(method, "__temporal_workflow_run", False):
                if run_fn is not None:
                    raise ValueError(
                        f"Workflow class {cls.__name__} has multiple @workflow.run methods. "
                        f"Only one is allowed."
                    )
                run_fn = method

            # Collect signal handlers
            signal_defn = _SignalDefinition.from_fn(method)
            if signal_defn is not None:
                if signal_defn.name in signals:
                    raise ValueError(
                        f"Duplicate signal handler for '{signal_defn.name}'"
                    )
                signals[signal_defn.name] = signal_defn

        if run_fn is None:
            raise ValueError(
                f"Workflow class {cls.__name__} must have a @workflow.run method"
            )

        definition = _Definition(
            name=name or cls.__name__,
            cls=cls,
            run_fn=run_fn,
            signals=signals,
        )
        setattr(cls, _Definition._ATTR_NAME, definition)
        return cls

    # Handle both @workflow.defn and @workflow.defn()
    if cls is not None:
        return decorator(cls)
    return decorator


# =============================================================================
# Public Workflow API
# =============================================================================
# These functions delegate to _Runtime.current() and will be fully implemented
# once TrioWorkflowInstance is ready. For now, they provide the correct interface.


async def sleep(duration: float, *, summary: str | None = None) -> None:
    """Sleep for the given duration.

    Mirrors temporalio.workflow.sleep from the SDK.

    This pauses workflow execution for the specified duration. During replay,
    the sleep completes immediately based on recorded history.

    Args:
        duration: Sleep duration in seconds.
        summary: Optional description for debugging/visibility.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    await _Runtime.current().workflow_sleep(duration, summary)


def time() -> float:
    """Get current workflow time in seconds.

    Mirrors temporalio.workflow.time from the SDK.

    Returns the current workflow time, which is deterministic and based on
    the workflow's history during replay.

    Returns:
        Current workflow time in seconds since epoch.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_time_ns() / 1e9


def time_ns() -> int:
    """Get current workflow time in nanoseconds.

    Mirrors temporalio.workflow.time_ns from the SDK.

    Returns the current workflow time, which is deterministic and based on
    the workflow's history during replay.

    Returns:
        Current workflow time in nanoseconds since epoch.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_time_ns()


def info() -> Info:
    """Get information about the current workflow.

    Mirrors temporalio.workflow.info from the SDK.

    Returns information about the currently executing workflow, including
    its ID, type, run ID, and task queue.

    Returns:
        Information about the current workflow.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_info()


async def execute_activity(
    activity: str | Callable[..., Any],
    *args: Any,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    activity_id: str | None = None,
) -> Any:
    """Execute an activity and wait for its result.

    Mirrors temporalio.workflow.execute_activity from the SDK.

    This schedules an activity for execution and waits for it to complete.
    At least one of ``schedule_to_close_timeout`` or ``start_to_close_timeout``
    must be provided.

    Args:
        activity: Activity name (string) or function reference decorated with
            @activity.defn.
        *args: Arguments to pass to the activity.
        task_queue: Task queue to run the activity on. Defaults to the current
            workflow's task queue.
        schedule_to_close_timeout: Max amount of time the activity can take from
            first being scheduled to being completed. This is inclusive of all
            retries.
        schedule_to_start_timeout: Max amount of time the activity can take to
            be started from first being scheduled.
        start_to_close_timeout: Max amount of time a single activity run can
            take from when it starts to when it completes. This is per retry.
        heartbeat_timeout: How frequently an activity must invoke heartbeat
            while running before it is considered timed out.
        retry_policy: How an activity is retried on failure. If unset, a
            server-defined default is used. Set maximum attempts to 1 to disable
            retries.
        activity_id: Optional unique identifier for the activity.

    Returns:
        The result of the activity execution.

    Raises:
        RuntimeError: If the activity fails or is cancelled.
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return await _Runtime.current().workflow_execute_activity(
        activity,
        *args,
        task_queue=task_queue,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        activity_id=activity_id,
    )


@dataclass
class Info:
    """Information about a running workflow.

    Mirrors temporalio.workflow.Info from the SDK.
    Simplified for POC - SDK has many more fields.
    """

    workflow_id: str
    """Unique identifier for the workflow execution."""

    workflow_type: str
    """Name of the workflow type (from @workflow.defn)."""

    run_id: str
    """Unique identifier for this specific run."""

    task_queue: str
    """Task queue the workflow is running on."""
