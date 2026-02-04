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
from typing import TYPE_CHECKING, Any, Awaitable, Callable, Generic, Sequence, TypeVar

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
    "execute_activity",
    "wait_condition",
    "start_child_workflow",
    "execute_child_workflow",
    "Info",
    "ChildWorkflowHandle",
    "ChildWorkflowCancellationType",
    "ParentClosePolicy",
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

class ParentClosePolicy(IntEnum):
    """What happens to a child workflow when the parent workflow closes.
    """

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
    def workflow_random(self) -> _random_module.Random:
        """Get the deterministic random number generator for this workflow.

        Returns a seeded random.Random instance that produces deterministic
        results across workflow replays. The seed is provided by the Temporal
        server and is consistent for each workflow execution.

        Returns:
            A seeded random.Random instance.
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
        # This will be called after the workflow has completed
        # The actual waiting is handled by the workflow instance
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
        """
        # TODO: Implement signal external workflow
        raise NotImplementedError("Child workflow signaling not yet implemented")

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
