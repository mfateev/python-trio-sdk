"""Workflow definitions and runtime for Temporal with Trio.

This module mirrors temporalio.workflow from the SDK.
"""

from __future__ import annotations

import inspect
import uuid
from abc import ABC, abstractmethod
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from functools import partial
from random import Random
from typing import Any, Awaitable, Callable, Mapping, NoReturn, Sequence, TypeVar

__all__ = [
    "defn",
    "run",
    "update",
    "sleep",
    "time",
    "time_ns",
    "now",
    "info",
    "Info",
    "random",
    "uuid4",
    "memo",
    "memo_value",
    "continue_as_new",
    "ContinueAsNewError",
    "HandlerUnfinishedPolicy",
    "_Runtime",
    "_Definition",
    "_UpdateDefinition",
    "_NotInWorkflowContextError",
]

# Type variable for decorator
F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")

# Sentinel for unset values
_unset = object()

# Context variable for current runtime (Trio doesn't have event loops like asyncio)
_current_runtime: ContextVar[_Runtime | None] = ContextVar(
    "_current_runtime", default=None
)


class HandlerUnfinishedPolicy(Enum):
    """Actions taken if a workflow terminates with running handlers.

    Mirrors temporalio.workflow.HandlerUnfinishedPolicy from the SDK.

    Policy defining actions taken when a workflow exits while update or signal
    handlers are running. The workflow exit may be due to successful return,
    failure, cancellation, or continue-as-new.
    """

    WARN_AND_ABANDON = 1
    """Issue a warning in addition to abandoning."""

    ABANDON = 2
    """Abandon the handler.

    In the case of an update handler this means that the client will receive
    an error rather than the update result.
    """


@dataclass
class _UpdateDefinition:
    """Update handler definition metadata.

    Mirrors temporalio.workflow._UpdateDefinition from the SDK.

    Stores metadata about a method decorated with @workflow.update.
    """

    name: str | None
    """Update name, or None if this is a dynamic update handler."""

    fn: Callable[..., Any | Awaitable[Any]]
    """The update handler function."""

    is_method: bool
    """Whether this is a method (True) or standalone function (False)."""

    unfinished_policy: HandlerUnfinishedPolicy = (
        HandlerUnfinishedPolicy.WARN_AND_ABANDON
    )
    """Policy for what happens if workflow terminates with running handler."""

    description: str | None = None
    """Optional description for this update handler."""

    validator: Callable[..., None] | None = None
    """Optional validator function called before the handler."""

    def set_validator(self, validator_fn: Callable[..., None]) -> None:
        """Set the validator function for this update handler.

        Args:
            validator_fn: The validator function to call before the handler.
        """
        object.__setattr__(self, "validator", validator_fn)

    def bind_fn(self, obj: Any) -> Callable[..., Any]:
        """Bind the handler function to an object instance.

        Args:
            obj: The workflow instance to bind to.

        Returns:
            Bound method callable.
        """
        return getattr(obj, self.fn.__name__)

    def bind_validator(self, obj: Any) -> Callable[..., None] | None:
        """Bind the validator function to an object instance.

        Args:
            obj: The workflow instance to bind to.

        Returns:
            Bound validator method callable, or None if no validator.
        """
        if self.validator is None:
            return None
        return getattr(obj, self.validator.__name__)


class _NotInWorkflowContextError(RuntimeError):
    """Raised when workflow API is called outside workflow context."""

    pass


class ContinueAsNewError(BaseException):
    """Error thrown by continue_as_new().

    Mirrors temporalio.workflow.ContinueAsNewError from the SDK.

    This should not be caught, but instead be allowed to throw out of the
    workflow which then triggers the continue as new. This should never be
    instantiated directly.

    Note: This is a BaseException (not Exception) so it won't be caught by
    generic `except Exception:` handlers, ensuring the continue-as-new request
    propagates correctly.
    """

    def __init__(self, *args: object) -> None:
        """Direct instantiation is disabled. Use continue_as_new()."""
        if type(self) == ContinueAsNewError:
            raise RuntimeError("Cannot instantiate ContinueAsNewError directly")
        super().__init__(*args)


class _ContinueAsNewError(ContinueAsNewError):
    """Internal continue-as-new error with command data.

    This is the actual error raised by continue_as_new(). It contains all
    the data needed to generate a ContinueAsNewWorkflowCommand.
    """

    def __init__(
        self,
        workflow: str | None,
        workflow_args: Sequence[Any],
        task_queue: str | None,
        run_timeout: timedelta | None,
        task_timeout: timedelta | None,
        memo: Mapping[str, Any] | None,
    ) -> None:
        """Initialize the continue-as-new error.

        Args:
            workflow: Workflow type name or None for same workflow.
            workflow_args: Arguments for the new workflow execution.
            task_queue: Task queue or None for same task queue.
            run_timeout: Run timeout or None for same timeout.
            task_timeout: Task timeout or None for same timeout.
            memo: Memo or None for same memo.
        """
        super().__init__("Continue as new")
        self.workflow = workflow
        self.workflow_args = workflow_args
        self.task_queue = task_queue
        self.run_timeout = run_timeout
        self.task_timeout = task_timeout
        self.memo = memo


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
    def workflow_random(self) -> Random:
        """Get the deterministic random number generator for this workflow.

        Returns:
            The seeded random instance for this workflow execution.
        """
        ...

    @abstractmethod
    def workflow_memo(self) -> Mapping[str, Any]:
        """Get all memo values for this workflow.

        Returns:
            Mapping of memo keys to values.
        """
        ...

    @abstractmethod
    def workflow_memo_value(
        self, key: str, default: Any, *, type_hint: type | None
    ) -> Any:
        """Get a specific memo value.

        Args:
            key: The memo key to retrieve.
            default: Default value if key not found (use _unset to raise KeyError).
            type_hint: Optional type hint for conversion.

        Returns:
            The memo value.

        Raises:
            KeyError: If key not found and no default provided.
        """
        ...

    @abstractmethod
    def workflow_continue_as_new(
        self,
        *args: Any,
        workflow: str | Callable | None,
        task_queue: str | None,
        run_timeout: timedelta | None,
        task_timeout: timedelta | None,
        memo: Mapping[str, Any] | None,
    ) -> NoReturn:
        """Continue the workflow as a new execution.

        Args:
            *args: Arguments for the new workflow execution.
            workflow: Workflow type or name, or None for same workflow.
            task_queue: Task queue or None for same task queue.
            run_timeout: Run timeout or None for same timeout.
            task_timeout: Task timeout or None for same timeout.
            memo: Memo or None for same memo.

        Raises:
            _ContinueAsNewError: Always raised to signal continue-as-new.
        """
        ...


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

    updates: Mapping[str | None, _UpdateDefinition] = field(default_factory=dict)
    """Update handlers, keyed by name (None for dynamic handler)."""

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


def update(
    fn: F | None = None,
    *,
    name: str | None = None,
    dynamic: bool = False,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: str | None = None,
) -> F | Callable[[F], F]:
    """Decorator for workflow update handler method.

    Mirrors @temporalio.workflow.update from the SDK.

    This is used on any async or non-async method that you wish to be called
    upon receiving an update. If a function overrides one with this decorator,
    it too must be decorated.

    You may optionally define a validator method that will be called before
    this handler. Specify the validator with ``@update_handler_function.validator``.

    Update methods can only have positional parameters. The handler may return
    a serializable value which will be sent back to the caller of the update.

    Example:
        @workflow.defn
        class MyWorkflow:
            @workflow.update
            async def my_update(self, value: str) -> str:
                return f"Updated: {value}"

            @my_update.validator
            def validate_my_update(self, value: str) -> None:
                if not value:
                    raise ValueError("value cannot be empty")

            @workflow.run
            async def run(self) -> None:
                pass

    Args:
        fn: The function to decorate.
        name: Update name. Defaults to method ``__name__``. Cannot be present
            when ``dynamic`` is present.
        dynamic: If true, this handles all updates not otherwise handled.
            Cannot be present when ``name`` is present.
        unfinished_policy: Actions taken if a workflow terminates with
            a running instance of this handler.
        description: A short description of the update.

    Returns:
        The decorated method with a ``validator`` attribute for setting validator.
    """

    def decorator(
        update_name: str | None,
        unfinished_policy: HandlerUnfinishedPolicy,
        fn: F,
    ) -> F:
        if not update_name and not dynamic:
            update_name = fn.__name__
        defn = _UpdateDefinition(
            name=update_name,
            fn=fn,
            is_method=True,
            unfinished_policy=unfinished_policy,
            description=description,
        )
        setattr(fn, "_update_defn", defn)
        setattr(fn, "validator", partial(_update_validator, defn))
        return fn

    if fn is None:
        if name is not None and dynamic:
            raise ValueError("Cannot provide both name and dynamic=True")
        return partial(decorator, name, unfinished_policy)  # type: ignore
    else:
        return decorator(fn.__name__, unfinished_policy, fn)  # type: ignore


def _update_validator(
    update_def: _UpdateDefinition, fn: Callable[..., None] | None = None
) -> Callable[..., None] | None:
    """Decorator for a workflow update validator method.

    Internal helper used to implement the @handler.validator pattern.
    """
    if fn is not None:
        update_def.set_validator(fn)
    return fn


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
        # Find the @workflow.run method and @workflow.update methods
        run_fn: Callable[..., Awaitable[Any]] | None = None
        updates: dict[str | None, _UpdateDefinition] = {}

        for attr_name in dir(cls):
            # Skip private/magic attributes
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

            # Check for update handlers
            update_defn = getattr(method, "_update_defn", None)
            if update_defn is not None:
                if update_defn.name in updates:
                    defn_name = update_defn.name or "<dynamic>"
                    raise ValueError(
                        f"Workflow class {cls.__name__} has multiple @workflow.update "
                        f"methods for '{defn_name}'"
                    )
                updates[update_defn.name] = update_defn

        if run_fn is None:
            raise ValueError(
                f"Workflow class {cls.__name__} must have a @workflow.run method"
            )

        # Create and attach definition
        definition = _Definition(
            name=name or cls.__name__,
            cls=cls,
            run_fn=run_fn,
            updates=updates,
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


def now() -> datetime:
    """Get current workflow time as a datetime.

    Mirrors temporalio.workflow.now from the SDK.

    This is the workflow equivalent of datetime.now(timezone.utc).
    Returns a timezone-aware datetime in UTC.

    Returns:
        UTC datetime for the current workflow time.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return datetime.fromtimestamp(time(), timezone.utc)


def random() -> Random:
    """Get a deterministic pseudo-random number generator.

    Mirrors temporalio.workflow.random from the SDK.

    Note: This random number generator is not cryptographically safe and
    should not be used for security purposes. It is seeded deterministically
    to ensure replay safety.

    Returns:
        The deterministically-seeded pseudo-random number generator.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_random()


def uuid4() -> uuid.UUID:
    """Get a new, determinism-safe v4 UUID.

    Mirrors temporalio.workflow.uuid4 from the SDK.

    This uses the deterministic random() function to generate a UUID that
    is safe for replay.

    Note: This UUID is not cryptographically safe and should not be used
    for security purposes.

    Returns:
        A deterministically-seeded v4 UUID.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return uuid.UUID(bytes=random().getrandbits(16 * 8).to_bytes(16, "big"), version=4)


def memo() -> Mapping[str, Any]:
    """Get current workflow's memo values.

    Mirrors temporalio.workflow.memo from the SDK.

    Returns all memo values that were set when the workflow was started
    or updated via upsert_memo.

    Returns:
        Mapping of all memo keys and their values.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_memo()


def memo_value(
    key: str,
    default: Any = _unset,
    *,
    type_hint: type | None = None,
) -> Any:
    """Get a specific memo value.

    Mirrors temporalio.workflow.memo_value from the SDK.

    Args:
        key: Key to get memo value for.
        default: Default to use if key is not present. If unset, a
            KeyError is raised when the key does not exist.
        type_hint: Type hint to use when converting (currently unused in POC).

    Returns:
        Memo value for the given key.

    Raises:
        KeyError: Key not present and default not set.
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_memo_value(key, default, type_hint=type_hint)


def continue_as_new(
    arg: Any = _unset,
    *,
    args: Sequence[Any] = [],
    workflow: str | Callable | None = None,
    task_queue: str | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    memo: Mapping[str, Any] | None = None,
) -> NoReturn:
    """Stop the workflow immediately and continue as new.

    Mirrors temporalio.workflow.continue_as_new from the SDK.

    This function never returns. It always raises a ContinueAsNewError which
    should not be caught, but allowed to propagate out of the workflow to
    trigger the continue-as-new behavior.

    Args:
        arg: Single argument to the continued workflow.
        args: Multiple arguments to the continued workflow. Cannot be set if arg
            is provided.
        workflow: Specific workflow to continue to. Can be a workflow class,
            workflow function, or workflow name string. Defaults to the current
            workflow type.
        task_queue: Task queue to run the workflow on. Defaults to the current
            workflow's task queue.
        run_timeout: Timeout of a single workflow run. Defaults to the current
            workflow's run timeout.
        task_timeout: Timeout of a single workflow task. Defaults to the current
            workflow's task timeout.
        memo: Memo for the workflow. Defaults to the current workflow's memo.

    Returns:
        Never returns, always raises ContinueAsNewError.

    Raises:
        ContinueAsNewError: Always raised by this function.
        ValueError: If both arg and args are provided.
        _NotInWorkflowContextError: If not in a workflow context.
    """
    # Handle arg vs args
    if arg is not _unset:
        if args:
            raise ValueError("Cannot specify both arg and args")
        final_args: Sequence[Any] = [arg]
    else:
        final_args = args

    _Runtime.current().workflow_continue_as_new(
        *final_args,
        workflow=workflow,
        task_queue=task_queue,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        memo=memo,
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

    raw_memo: Mapping[str, Any] = field(default_factory=dict)
    """Raw memo values for the workflow.

    In the full SDK, this contains Payload objects that need conversion.
    For the POC, this contains already-converted Python values.
    """
