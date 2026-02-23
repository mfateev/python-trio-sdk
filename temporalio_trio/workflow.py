"""Workflow definitions and runtime for Temporal with Trio.

This module mirrors temporalio.workflow from the SDK.
"""

from __future__ import annotations

import inspect
import random as _random_module
import uuid as _uuid_module
from abc import ABC, abstractmethod
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import timedelta
from enum import IntEnum
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Generic,
    Mapping,
    NoReturn,
    Sequence,
    TypeVar,
)

import temporalio.api.common.v1
import temporalio.common

if TYPE_CHECKING:
    pass

__all__ = [
    "defn",
    "run",
    "signal",
    "query",
    "sleep",
    "time",
    "time_ns",
    "info",
    "random",
    "uuid4",
    "patched",
    "deprecate_patch",
    "execute_activity",
    "wait_condition",
    "start_child_workflow",
    "execute_child_workflow",
    "continue_as_new",
    "get_external_workflow_handle",
    "get_external_workflow_handle_for",
    "upsert_search_attributes",
    "Info",
    "ChildWorkflowHandle",
    "ExternalWorkflowHandle",
    "ActivityCancellationType",
    "ChildWorkflowCancellationType",
    "ParentClosePolicy",
    "ContinueAsNewError",
    "_Runtime",
    "_Definition",
    "_SignalDefinition",
    "_QueryDefinition",
    "_NotInWorkflowContextError",
]

# Type variable for decorator
F = TypeVar("F", bound=Callable[..., Any])
T = TypeVar("T")
SelfType = TypeVar("SelfType")
ReturnType = TypeVar("ReturnType")


# =============================================================================
# Child Workflow Enums
# =============================================================================


class ChildWorkflowCancellationType(IntEnum):
    """How a child workflow reacts to cancellation of its parent."""

    ABANDON = 0
    """Do not request cancellation of the child workflow if already scheduled."""

    TRY_CANCEL = 1
    """Initiate a cancellation request and immediately report cancellation to the parent."""

    WAIT_CANCELLATION_COMPLETED = 2
    """Wait for child cancellation completion before reporting cancellation."""

    WAIT_CANCELLATION_REQUESTED = 3
    """Request cancellation and wait for confirmation that the request was received."""


class ActivityCancellationType(IntEnum):
    """How an activity cancellation should be handled."""

    TRY_CANCEL = 0
    """Initiate a cancellation request and immediately report cancellation to the parent."""

    WAIT_CANCELLATION_COMPLETED = 1
    """Wait for activity cancellation completion before reporting cancellation."""

    ABANDON = 2
    """Do not request cancellation of the activity if already scheduled."""


class ParentClosePolicy(IntEnum):
    """What happens to a child workflow when the parent workflow closes."""

    UNSPECIFIED = 0
    """Let the server set the default policy."""

    TERMINATE = 1
    """Terminate the child workflow when the parent closes."""

    ABANDON = 2
    """Do nothing to the child workflow when the parent closes."""

    REQUEST_CANCEL = 3
    """Request cancellation of the child workflow when the parent closes."""


# Context variable for current runtime (Trio doesn't have event loops like asyncio)
_current_runtime: ContextVar[_Runtime | None] = ContextVar(
    "_current_runtime", default=None
)


class _NotInWorkflowContextError(RuntimeError):
    """Raised when workflow API is called outside workflow context."""

    pass


class ContinueAsNewError(BaseException):
    """Error raised by continue_as_new() to stop workflow and continue as new.

    This exception is raised by :py:func:`continue_as_new` to signal that the
    workflow should stop and continue as a new execution. It is a BaseException
    (not Exception) so it won't be caught by normal exception handlers.

    This class should not be instantiated directly - use :py:func:`continue_as_new`
    instead.
    """

    def __init__(self, *args: object) -> None:
        """Initialize ContinueAsNewError.

        Raises:
            RuntimeError: If instantiated directly (not via a subclass).
        """
        if type(self) is ContinueAsNewError:
            raise RuntimeError(
                "ContinueAsNewError cannot be instantiated directly. "
                "Use workflow.continue_as_new() instead."
            )
        super().__init__(*args)


class _Runtime(ABC):
    """Abstract runtime that provides workflow APIs.

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
        cancellation_type: "ActivityCancellationType" = ActivityCancellationType.TRY_CANCEL,
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
            cancellation_type: How an activity cancellation should be handled.

        Returns:
            The activity result.

        Raises:
            RuntimeError: If the activity fails or is cancelled.
        """
        ...

    @abstractmethod
    async def workflow_start_child_workflow(
        self,
        workflow: str | type,
        *args: Any,
        id: str,
        task_queue: str | None,
        cancellation_type: ChildWorkflowCancellationType,
        parent_close_policy: ParentClosePolicy,
        execution_timeout: timedelta | None,
        run_timeout: timedelta | None,
        task_timeout: timedelta | None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy,
        retry_policy: temporalio.common.RetryPolicy | None,
    ) -> "ChildWorkflowHandle[Any, Any]":
        """Start a child workflow and return a handle.

        Args:
            workflow: Workflow class or type name.
            *args: Arguments to pass to the workflow.
            id: Unique workflow ID.
            task_queue: Task queue (defaults to parent's).
            cancellation_type: How child reacts to parent cancellation.
            parent_close_policy: What happens when parent closes.
            execution_timeout: Total timeout including retries.
            run_timeout: Timeout for a single run.
            task_timeout: Timeout for a single workflow task.
            id_reuse_policy: How existing IDs are treated.
            retry_policy: Retry policy for the workflow.

        Returns:
            A handle to the started child workflow.
        """
        ...

    @abstractmethod
    async def workflow_wait_child_workflow(self, seq: int) -> None:
        """Wait for a child workflow to complete.

        Args:
            seq: The child workflow sequence number.
        """
        ...

    @abstractmethod
    async def workflow_wait_condition(
        self,
        fn: Callable[[], bool],
        *,
        timeout: float | None = None,
        timeout_summary: str | None = None,
    ) -> None:
        """Wait until condition returns True or timeout expires.

        Args:
            fn: A callable returning True when condition is met.
            timeout: Optional maximum wait time in seconds.
            timeout_summary: Optional description for Temporal UI.

        Raises:
            TimeoutError: If timeout expires before condition becomes true.
        """
        ...

    @abstractmethod
    def workflow_continue_as_new(
        self,
        *args: Any,
        workflow: str | type | None,
        task_queue: str | None,
        run_timeout: timedelta | None,
        task_timeout: timedelta | None,
        retry_policy: temporalio.common.RetryPolicy | None,
    ) -> NoReturn:
        """Continue the workflow as a new execution.

        This method never returns - it raises ContinueAsNewError to stop the
        workflow and start a new execution.

        Args:
            *args: Arguments to pass to the new workflow execution.
            workflow: Workflow class or type name. None means same workflow type.
            task_queue: Task queue for the new execution. None means same queue.
            run_timeout: Timeout for a single run of the new workflow.
            task_timeout: Timeout for a single workflow task.
            retry_policy: Retry policy for the new workflow.

        Raises:
            ContinueAsNewError: Always raised to stop the workflow.
        """
        ...

    @abstractmethod
    def workflow_get_external_workflow_handle(
        self,
        workflow_id: str,
        *,
        run_id: str | None,
    ) -> "ExternalWorkflowHandle[Any]":
        """Get a handle to an external workflow.

        Args:
            workflow_id: ID of the external workflow.
            run_id: Optional run ID to target a specific run.

        Returns:
            Handle to the external workflow.
        """
        ...

    @abstractmethod
    async def workflow_signal_external_workflow(
        self,
        workflow_id: str,
        signal_name: str,
        args: Sequence[Any],
        *,
        run_id: str | None,
    ) -> None:
        """Signal an external workflow.

        Args:
            workflow_id: ID of the external workflow to signal.
            signal_name: Name of the signal to send.
            args: Arguments to pass with the signal.
            run_id: Optional run ID to target a specific run.

        Raises:
            RuntimeError: If the signal fails (e.g., workflow not found).
        """
        ...

    @abstractmethod
    def workflow_random(self) -> _random_module.Random:
        """Get the deterministic random number generator for this workflow.

        Returns a seeded random.Random instance that produces deterministic
        results across workflow replays. The seed is provided by the Temporal
        server and is consistent for each workflow execution.

        Returns:
            A seeded random.Random instance.
        """
        ...

    @abstractmethod
    def workflow_patch(self, patch_id: str, *, deprecated: bool = False) -> bool:
        """Check if a patch should be applied.

        This is used for safe code evolution. When you need to change workflow
        code in a way that would break replay, use patched() to gate the change.

        Args:
            patch_id: Unique identifier for this patch point.
            deprecated: If True, marks the patch as deprecated.

        Returns:
            True if the new code path should be taken.
        """
        ...

    @abstractmethod
    def workflow_upsert_search_attributes(
        self,
        attributes: Sequence[temporalio.common.SearchAttributeUpdate],
    ) -> None:
        """Upsert search attributes for this workflow.

        Search attributes are used for workflow visibility and querying.
        Receives typed search attribute updates and converts them to the
        appropriate bridge command.

        Args:
            attributes: Sequence of typed search attribute updates.
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
class _QueryDefinition:
    """Query handler definition metadata."""

    name: str | None  # None for dynamic handlers
    fn: Callable[..., Any]
    is_method: bool
    description: str | None = None

    @staticmethod
    def from_fn(fn: Callable) -> "_QueryDefinition | None":
        """Get query definition from a function if it has one."""
        return getattr(fn, "__temporal_query_definition", None)


@dataclass
class _Definition:
    """Workflow definition metadata.

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

    queries: dict[str | None, _QueryDefinition] = field(default_factory=dict)
    """Query handlers for this workflow, keyed by query name (None for dynamic)."""

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


def query(
    fn: Callable | None = None,
    *,
    name: str | None = None,
    dynamic: bool = False,
    description: str | None = None,
) -> Any:
    """Decorator for workflow query handler methods.

    Query handlers must be synchronous (not async) and should not mutate workflow state.
    They return a value that is sent back to the query caller.

    Example:
        @workflow.defn
        class MyWorkflow:
            def __init__(self):
                self.status = "starting"

            @workflow.query
            def get_status(self) -> str:
                return self.status

            @workflow.query(name="custom-query")
            def another_query(self, param: int) -> int:
                return param * 2

    Args:
        fn: The function to decorate (when used without parentheses)
        name: Custom query name. Defaults to function name.
        dynamic: If True, this is a dynamic handler for any query name.
        description: Optional description for the query.

    Raises:
        ValueError: If the function is async (queries must be synchronous)
    """

    def decorator(fn: Callable) -> Callable:
        # Queries must not be async
        if inspect.iscoroutinefunction(fn):
            raise ValueError(
                f"Query handler '{fn.__name__}' must be synchronous (not async). "
                "Queries are read-only and must complete immediately."
            )

        if dynamic:
            query_name = None
        elif name:
            query_name = name
        else:
            query_name = fn.__name__

        defn = _QueryDefinition(
            name=query_name,
            fn=fn,
            is_method=True,
            description=description,
        )
        setattr(fn, "__temporal_query_definition", defn)
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
        queries: dict[str | None, _QueryDefinition] = {}

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

            # Collect query handlers
            query_defn = _QueryDefinition.from_fn(method)
            if query_defn is not None:
                if query_defn.name in queries:
                    raise ValueError(f"Duplicate query handler for '{query_defn.name}'")
                queries[query_defn.name] = query_defn

        if run_fn is None:
            raise ValueError(
                f"Workflow class {cls.__name__} must have a @workflow.run method"
            )

        definition = _Definition(
            name=name or cls.__name__,
            cls=cls,
            run_fn=run_fn,
            signals=signals,
            queries=queries,
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

    Returns information about the currently executing workflow, including
    its ID, type, run ID, and task queue.

    Returns:
        Information about the current workflow.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_info()


def random() -> _random_module.Random:
    """Get the deterministic random number generator for this workflow.

    Returns a seeded random.Random instance that produces deterministic
    results across workflow replays. The seed is provided by the Temporal
    server and is consistent for each workflow execution.

    IMPORTANT: Always use this function instead of the standard library
    random module to ensure workflow determinism. Using Python's default
    random module will break replay.

    Example:
        # Get a random integer
        value = workflow.random().randint(1, 100)

        # Get a random float
        value = workflow.random().random()

        # Shuffle a list deterministically
        items = [1, 2, 3, 4, 5]
        workflow.random().shuffle(items)

    Returns:
        A seeded random.Random instance.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_random()


def uuid4() -> _uuid_module.UUID:
    """Generate a deterministic UUID4 based on the workflow's random generator.

    This generates a UUID that is deterministic across workflow replays,
    unlike the standard library uuid.uuid4() which uses system entropy.

    IMPORTANT: Always use this function instead of the standard library
    uuid.uuid4() to ensure workflow determinism. Using Python's default
    uuid4 will break replay.

    Example:
        # Generate a unique ID for a resource
        resource_id = str(workflow.uuid4())

    Returns:
        A deterministic UUID4.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    rng = _Runtime.current().workflow_random()
    # Generate 16 random bytes and convert to UUID4
    # This matches the official SDK's implementation
    random_bytes = rng.getrandbits(16 * 8).to_bytes(16, "big")
    return _uuid_module.UUID(bytes=random_bytes, version=4)


def patched(patch_id: str) -> bool:
    """Check if a patch should be applied for safe workflow code evolution.

    Use this function when you need to change workflow code in a way that would
    break existing workflow executions during replay. This allows new code to
    run for new executions while existing executions continue with the old code
    path during replay.

    The patch ID must be unique within the workflow. Once a patch is applied,
    the workflow history records that fact, and subsequent replays will take
    the new code path.

    Example:
        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> str:
                if workflow.patched("my-change-v2"):
                    # New code path
                    result = await workflow.execute_activity(
                        new_activity,
                        start_to_close_timeout=timedelta(seconds=30),
                    )
                else:
                    # Old code path (for replaying old executions)
                    result = await workflow.execute_activity(
                        old_activity,
                        start_to_close_timeout=timedelta(seconds=30),
                    )
                return result

    Once all old workflow executions have completed, you can remove the
    patched() check and use deprecate_patch() to mark the patch as deprecated
    before fully removing it.

    Args:
        patch_id: A unique identifier for this patch point within the workflow.
            Must be consistent across code versions.

    Returns:
        True if the new code path should be taken (new execution or replaying
        an execution that has the patch marker), False if the old code path
        should be taken (replaying an execution without the patch marker).

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_patch(patch_id, deprecated=False)


def deprecate_patch(patch_id: str) -> None:
    """Mark a patch as deprecated.

    Use this after all old workflow executions (that don't have the patch)
    have completed. This is an intermediate step before fully removing the
    patched() call from your code.

    The deprecation workflow is:
    1. Add patched() check with old and new code paths
    2. Wait for all old executions to complete
    3. Replace patched() with deprecate_patch() (keep only new code)
    4. Wait for all executions with the patch marker to complete
    5. Remove deprecate_patch() call entirely

    Example:
        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> str:
                # Old code used patched("my-change-v2"), now deprecated
                workflow.deprecate_patch("my-change-v2")
                # Only new code path remains
                result = await workflow.execute_activity(
                    new_activity,
                    start_to_close_timeout=timedelta(seconds=30),
                )
                return result

    Args:
        patch_id: The same identifier used in the original patched() call.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    _Runtime.current().workflow_patch(patch_id, deprecated=True)


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
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
) -> Any:
    """Execute an activity and wait for its result.

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
        cancellation_type: How an activity cancellation should be handled.
            Default: TRY_CANCEL.

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
        cancellation_type=cancellation_type,
    )


async def wait_condition(
    fn: Callable[[], bool],
    *,
    timeout: timedelta | float | None = None,
    timeout_summary: str | None = None,
) -> None:
    """Wait until a condition becomes true.

    The condition function is evaluated after each signal is processed.
    If the condition becomes true, execution continues immediately.
    If a timeout is specified and expires first, TimeoutError is raised.


    Args:
        fn: A callable returning True when condition is met.
            Must be deterministic and side-effect free.
        timeout: Optional maximum wait time (timedelta or seconds).
        timeout_summary: Optional description for Temporal UI.

    Raises:
        TimeoutError: If timeout expires before condition becomes true.
        CancelledError: If workflow is cancelled while waiting.

    Example:
        # Wait for approval signal
        await workflow.wait_condition(lambda: self._approved)

        # Wait with timeout
        try:
            await workflow.wait_condition(
                lambda: self._approved,
                timeout=timedelta(hours=1),
            )
        except TimeoutError:
            # Handle timeout
            pass
    """
    runtime = _Runtime.current()

    if isinstance(timeout, timedelta):
        timeout = timeout.total_seconds()

    await runtime.workflow_wait_condition(
        fn,
        timeout=timeout,
        timeout_summary=timeout_summary,
    )


@dataclass
class Info:
    """Information about a running workflow.

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

    headers: Mapping[str, temporalio.api.common.v1.Payload] = field(
        default_factory=dict
    )
    """Headers from the workflow start (e.g. for tracing/auth interceptors)."""


# =============================================================================
# Child Workflow Handle
# =============================================================================


class ChildWorkflowHandle(Generic[SelfType, ReturnType]):
    """Handle for interacting with a started child workflow.

    This handle is returned by :py:func:`start_child_workflow` and provides
    methods to get the result, signal the child, and access its metadata.

    Example:
        handle = await workflow.start_child_workflow(
            ChildWorkflow.run,
            "arg",
            id="child-1",
        )
        # Wait for result
        result = await handle.result()
        # Or just await the handle
        result = await handle
    """

    def __init__(
        self,
        seq: int,
        id: str,
        workflow_type: str,
        first_execution_run_id: str | None = None,
    ) -> None:
        """Initialize a child workflow handle.

        Args:
            seq: Internal sequence number.
            id: Workflow ID.
            workflow_type: Type name of the child workflow.
            first_execution_run_id: Run ID of the first execution (if known).
        """
        self._seq = seq
        self._id = id
        self._workflow_type = workflow_type
        self._first_execution_run_id = first_execution_run_id
        self._result: ReturnType | None = None
        self._failure: BaseException | None = None
        self._completed = False

    @property
    def id(self) -> str:
        """Workflow ID of the child workflow."""
        return self._id

    @property
    def workflow_type(self) -> str:
        """Type name of the child workflow."""
        return self._workflow_type

    @property
    def first_execution_run_id(self) -> str | None:
        """Run ID of the first execution (available after started)."""
        return self._first_execution_run_id

    async def result(self) -> ReturnType:
        """Wait for and return the result of the child workflow.

        Returns:
            The child workflow's return value.

        Raises:
            RuntimeError: If the child workflow failed or was cancelled.
        """
        # If not yet completed, wait for completion via the runtime
        if not self._completed:
            try:
                result = await _Runtime.current().workflow_wait_child_workflow(
                    self._seq
                )
                self._set_result(result)
            except BaseException as e:
                self._set_failure(e)
                raise

        # Now return the result or raise the failure
        if self._failure:
            raise self._failure
        return self._result  # type: ignore[return-value]

    async def signal(
        self,
        signal: str | Callable,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
    ) -> None:
        """Send a signal to the child workflow.

        Args:
            signal: Signal name or decorated method reference.
            arg: Single argument to the signal.
            args: Multiple arguments (cannot be set if arg is set).

        Raises:
            RuntimeError: If the signal fails (e.g., workflow not found).
        """
        # Get signal name from string or callable
        if callable(signal):
            signal_defn = _SignalDefinition.from_fn(signal)
            if signal_defn and signal_defn.name:
                signal_name = signal_defn.name
            else:
                signal_name = signal.__name__
        else:
            signal_name = signal

        # Use the same signaling mechanism as external workflows
        # A child workflow is just an external workflow that we started
        await _Runtime.current().workflow_signal_external_workflow(
            self._id,
            signal_name,
            temporalio.common._arg_or_args(arg, args),
            run_id=self._first_execution_run_id,
        )

    def __await__(self):
        """Support `await handle` syntax.

        This is a shortcut for `await handle.result()`.
        """
        return self.result().__await__()

    def _set_started(self, run_id: str) -> None:
        """Mark the child workflow as started with the given run ID.

        Internal method called by the workflow instance.
        """
        self._first_execution_run_id = run_id

    def _set_result(self, result: Any) -> None:
        """Set the successful result of the child workflow.

        Internal method called by the workflow instance.
        """
        self._result = result
        self._completed = True

    def _set_failure(self, failure: BaseException) -> None:
        """Set the failure of the child workflow.

        Internal method called by the workflow instance.
        """
        self._failure = failure
        self._completed = True


# =============================================================================
# External Workflow Handle
# =============================================================================


class ExternalWorkflowHandle(Generic[SelfType]):
    """Handle to an external workflow for signaling or cancellation.

    Mirrors temporalio.workflow.ExternalWorkflowHandle from the SDK.

    This handle can be used to send signals to an external workflow (a workflow
    running separately that is not a child of this workflow).

    Example:
        # Get a handle to an external workflow
        handle = workflow.get_external_workflow_handle("other-workflow-id")

        # Send a signal
        await handle.signal("my_signal", "arg1", "arg2")

    Note:
        External workflow handles do NOT support waiting for results - that's
        what child workflows are for. External handles are for signaling
        workflows that are not hierarchically related.
    """

    def __init__(
        self,
        runtime: _Runtime,
        workflow_id: str,
        run_id: str | None = None,
    ) -> None:
        """Initialize external workflow handle.

        Args:
            runtime: The workflow runtime.
            workflow_id: ID of the external workflow.
            run_id: Optional run ID to target a specific run.
        """
        self._runtime = runtime
        self._workflow_id = workflow_id
        self._run_id = run_id

    @property
    def id(self) -> str:
        """Get the workflow ID."""
        return self._workflow_id

    @property
    def run_id(self) -> str | None:
        """Get the run ID if specified."""
        return self._run_id

    async def signal(
        self,
        signal: str | Callable,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
    ) -> None:
        """Send a signal to the external workflow.

        Args:
            signal: Signal name or decorated method reference.
            arg: Single argument to the signal.
            args: Multiple arguments (cannot be set if arg is set).

        Raises:
            RuntimeError: If the signal fails (e.g., workflow not found).
        """
        # Get signal name from string or callable
        if callable(signal):
            signal_defn = _SignalDefinition.from_fn(signal)
            if signal_defn and signal_defn.name:
                signal_name = signal_defn.name
            else:
                signal_name = signal.__name__
        else:
            signal_name = signal

        await self._runtime.workflow_signal_external_workflow(
            self._workflow_id,
            signal_name,
            temporalio.common._arg_or_args(arg, args),
            run_id=self._run_id,
        )


# =============================================================================
# External Workflow Public API
# =============================================================================


def get_external_workflow_handle(
    workflow_id: str,
    *,
    run_id: str | None = None,
) -> ExternalWorkflowHandle[Any]:
    """Get a handle to an external workflow by its ID.

    Mirrors temporalio.workflow.get_external_workflow_handle from the SDK.

    This returns a handle that can be used to signal an external workflow (a
    workflow running separately that is not a child of this workflow).

    Example:
        # Get a handle and send a signal
        handle = workflow.get_external_workflow_handle("other-workflow-id")
        await handle.signal("my_signal", "data")

        # Signal a specific run
        handle = workflow.get_external_workflow_handle(
            "other-workflow-id",
            run_id="specific-run-id"
        )
        await handle.signal("my_signal")

    Args:
        workflow_id: ID of the external workflow.
        run_id: Optional run ID to target a specific run.

    Returns:
        Handle to the external workflow.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_get_external_workflow_handle(
        workflow_id, run_id=run_id
    )


def get_external_workflow_handle_for(
    workflow: Callable[..., Awaitable[Any]],
    workflow_id: str,
    *,
    run_id: str | None = None,
) -> ExternalWorkflowHandle[Any]:
    """Get a typed handle to an external workflow.

    Mirrors temporalio.workflow.get_external_workflow_handle_for from the SDK.

    This is the same as :py:func:`get_external_workflow_handle` but allows
    passing a workflow type for better IDE support. Note that the workflow
    type is not validated - it's only for typing purposes.

    Example:
        @workflow.defn
        class OtherWorkflow:
            @workflow.run
            async def run(self) -> str:
                return "result"

            @workflow.signal
            def my_signal(self, data: str) -> None:
                pass

        # Get typed handle
        handle = workflow.get_external_workflow_handle_for(
            OtherWorkflow.run,
            "other-workflow-id",
        )
        # IDE knows handle.signal expects OtherWorkflow signals
        await handle.signal(OtherWorkflow.my_signal, "data")

    Args:
        workflow: The workflow run method (for typing only, not validated).
        workflow_id: ID of the external workflow.
        run_id: Optional run ID to target a specific run.

    Returns:
        Handle to the external workflow.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return get_external_workflow_handle(workflow_id, run_id=run_id)


# =============================================================================
# Child Workflow Public API
# =============================================================================


async def start_child_workflow(
    workflow: type | str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    id: str | None = None,
    task_queue: str | None = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: timedelta | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: temporalio.common.RetryPolicy | None = None,
) -> ChildWorkflowHandle[Any, Any]:
    """Start a child workflow and return a handle.

    This starts a child workflow and returns a handle that can be used to
    wait for the result, send signals, or get metadata.

    Example:
        # Start and wait for result
        handle = await workflow.start_child_workflow(
            ChildWorkflow.run,
            "arg1",
            id="child-1",
        )
        result = await handle.result()

        # Or use execute_child_workflow for simpler cases
        result = await workflow.execute_child_workflow(
            ChildWorkflow.run,
            "arg1",
            id="child-1",
        )

    Args:
        workflow: Workflow class decorated with @workflow.defn, or workflow
            type name as a string.
        arg: Single argument to the workflow.
        args: Multiple arguments. Cannot be set if arg is set.
        id: Unique workflow ID. Defaults to a UUID.
        task_queue: Task queue to run the child on. Defaults to the parent
            workflow's task queue.
        cancellation_type: How the child workflow should react when the parent
            workflow is cancelled.
        parent_close_policy: What happens to the child when the parent closes.
        execution_timeout: Total timeout including retries and continue-as-new.
        run_timeout: Timeout for a single run (not including retries).
        task_timeout: Timeout for a single workflow task.
        id_reuse_policy: How existing workflow IDs are handled.
        retry_policy: Retry policy for the workflow.

    Returns:
        A handle to the started child workflow.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
        RuntimeError: If the child workflow fails to start.
    """
    return await _Runtime.current().workflow_start_child_workflow(
        workflow,
        *temporalio.common._arg_or_args(arg, args),
        id=id or str(uuid4()),  # Uses our deterministic uuid4()
        task_queue=task_queue,
        cancellation_type=cancellation_type,
        parent_close_policy=parent_close_policy,
        execution_timeout=execution_timeout,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        id_reuse_policy=id_reuse_policy,
        retry_policy=retry_policy,
    )


async def execute_child_workflow(
    workflow: type | str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    id: str | None = None,
    task_queue: str | None = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: timedelta | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: temporalio.common.RetryPolicy | None = None,
) -> Any:
    """Start a child workflow and wait for its result.

    This is a convenience method equivalent to:
        handle = await start_child_workflow(...)
        return await handle.result()

    Example:
        result = await workflow.execute_child_workflow(
            ChildWorkflow.run,
            "arg1",
            id="child-1",
        )

    Args:
        workflow: Workflow class decorated with @workflow.defn, or workflow
            type name as a string.
        arg: Single argument to the workflow.
        args: Multiple arguments. Cannot be set if arg is set.
        id: Unique workflow ID. Defaults to a UUID.
        task_queue: Task queue to run the child on. Defaults to the parent
            workflow's task queue.
        cancellation_type: How the child workflow should react when the parent
            workflow is cancelled.
        parent_close_policy: What happens to the child when the parent closes.
        execution_timeout: Total timeout including retries and continue-as-new.
        run_timeout: Timeout for a single run (not including retries).
        task_timeout: Timeout for a single workflow task.
        id_reuse_policy: How existing workflow IDs are handled.
        retry_policy: Retry policy for the workflow.

    Returns:
        The result of the child workflow.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
        RuntimeError: If the child workflow fails.
    """
    handle = await start_child_workflow(
        workflow,
        arg,
        args=args,
        id=id,
        task_queue=task_queue,
        cancellation_type=cancellation_type,
        parent_close_policy=parent_close_policy,
        execution_timeout=execution_timeout,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        id_reuse_policy=id_reuse_policy,
        retry_policy=retry_policy,
    )
    return await handle.result()


def continue_as_new(
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    workflow: str | type | None = None,
    task_queue: str | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
) -> NoReturn:
    """Stop the workflow immediately and continue as a new execution.

    Mirrors temporalio.workflow.continue_as_new from the SDK.

    This function never returns. It raises :py:class:`ContinueAsNewError` to
    signal that the workflow should stop and start a new execution with the
    same workflow ID but a new run ID.

    This is useful for long-running workflows to avoid unbounded history growth.
    When a workflow continues as new, it starts fresh with a new event history.

    Example:
        @workflow.defn
        class LongRunningWorkflow:
            @workflow.run
            async def run(self, iteration: int) -> str:
                if iteration >= 100:
                    return "done"
                # Process iteration...
                await workflow.sleep(60)
                # Continue as new to reset history
                workflow.continue_as_new(iteration + 1)

    Args:
        arg: Single argument to the new workflow execution.
        args: Multiple arguments. Cannot be set if arg is set.
        workflow: Workflow class or type name for the new execution. If None,
            uses the same workflow type as the current workflow.
        task_queue: Task queue for the new execution. If None, uses the same
            task queue as the current workflow.
        run_timeout: Timeout for a single run of the new workflow.
        task_timeout: Timeout for a single workflow task.
        retry_policy: Retry policy for the new workflow.

    Raises:
        ContinueAsNewError: Always raised to stop the workflow.
        _NotInWorkflowContextError: If not in a workflow context.
    """
    _Runtime.current().workflow_continue_as_new(
        *temporalio.common._arg_or_args(arg, args),
        workflow=workflow,
        task_queue=task_queue,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        retry_policy=retry_policy,
    )


def upsert_search_attributes(
    attributes: Sequence[temporalio.common.SearchAttributeUpdate],
) -> None:
    """Upsert search attributes for this workflow.

    Mirrors temporalio.workflow.upsert_search_attributes from the SDK (typed
    form only; the deprecated dict form is not supported).

    Search attributes are metadata fields that can be used to filter and search
    for workflows in the Temporal UI and CLI. This function updates existing
    attributes and adds new ones. Attributes not mentioned are left unchanged.

    Custom search attributes must be registered with the Temporal server before
    use.

    Example:
        from temporalio.common import SearchAttributeKey

        MY_KEYWORD = SearchAttributeKey.for_keyword("CustomKeywordField")
        MY_INT = SearchAttributeKey.for_int("CustomIntField")

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> str:
                workflow.upsert_search_attributes([
                    MY_KEYWORD.value_set("processing"),
                    MY_INT.value_set(42),
                ])
                await workflow.sleep(10)
                workflow.upsert_search_attributes([
                    MY_KEYWORD.value_set("completed"),
                ])
                return "done"

    Args:
        attributes: A sequence of search attribute updates created via
            ``SearchAttributeKey.value_set()`` or
            ``SearchAttributeKey.value_unset()``.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
        RuntimeError: If an attribute hasn't been registered on the server.

    Note:
        - Search attributes are eventually consistent
        - Custom attributes must be registered with the Temporal server
        - This is a one-way command with no response
    """
    if not attributes:
        return
    _Runtime.current().workflow_upsert_search_attributes(attributes)
