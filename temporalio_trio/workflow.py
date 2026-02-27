"""Workflow definitions and runtime for Temporal with Trio.

This module mirrors temporalio.workflow from the SDK.
"""

from __future__ import annotations

import inspect
import logging
import random as _random_module
import uuid as _uuid_module
from abc import ABC, abstractmethod
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import IntEnum
from typing import (
    TYPE_CHECKING,
    Any,
    Awaitable,
    Callable,
    Generic,
    Mapping,
    NoReturn,
    Optional,
    Sequence,
    TypeVar,
)

import temporalio.api.common.v1
import temporalio.common
import temporalio.converter
import temporalio.exceptions
from typing_extensions import TypedDict

if TYPE_CHECKING:
    pass


def _interceptor_mod():
    """Lazily import interceptor module to avoid circular imports."""
    from temporalio_trio.worker import _interceptor

    return _interceptor


__all__ = [
    "defn",
    "init",
    "run",
    "signal",
    "query",
    "sleep",
    "time",
    "time_ns",
    "now",
    "in_workflow",
    "info",
    "memo",
    "instance",
    "payload_converter",
    "get_current_details",
    "set_current_details",
    "get_current_build_id",
    "get_current_history_length",
    "get_current_history_size",
    "is_continue_as_new_suggested",
    "has_last_completion_result",
    "get_last_completion_result",
    "get_last_failure",
    "metric_meter",
    "random",
    "uuid4",
    "patched",
    "deprecate_patch",
    "start_activity",
    "execute_activity",
    "start_local_activity",
    "execute_local_activity",
    "start_activity_class",
    "execute_activity_class",
    "start_activity_method",
    "execute_activity_method",
    "start_local_activity_class",
    "execute_local_activity_class",
    "start_local_activity_method",
    "execute_local_activity_method",
    "wait_condition",
    "start_child_workflow",
    "execute_child_workflow",
    "continue_as_new",
    "get_external_workflow_handle",
    "get_external_workflow_handle_for",
    "upsert_search_attributes",
    "LoggerAdapter",
    "logger",
    "ParentInfo",
    "RootInfo",
    "Info",
    "ChildWorkflowHandle",
    "ExternalWorkflowHandle",
    "ActivityCancellationType",
    "ActivityConfig",
    "ActivityHandle",
    "ChildWorkflowCancellationType",
    "ParentClosePolicy",
    "HandlerUnfinishedPolicy",
    "VersioningIntent",
    "LocalActivityConfig",
    "ChildWorkflowConfig",
    "ContinueAsNewError",
    "NondeterminismError",
    "ReadOnlyContextError",
    "get_signal_handler",
    "set_signal_handler",
    "get_dynamic_signal_handler",
    "set_dynamic_signal_handler",
    "get_query_handler",
    "set_query_handler",
    "get_dynamic_query_handler",
    "set_dynamic_query_handler",
    "get_update_handler",
    "set_update_handler",
    "get_dynamic_update_handler",
    "set_dynamic_update_handler",
    "all_handlers_finished",
    "update",
    "UpdateInfo",
    "current_update_info",
    "_Runtime",
    "_Definition",
    "_SignalDefinition",
    "_QueryDefinition",
    "_UpdateDefinition",
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


class ActivityHandle(Generic[ReturnType]):
    """Handle returned from :py:func:`start_activity` and
    :py:func:`start_local_activity`.

    This is the Trio equivalent of sdk-python's ActivityHandle (which extends
    asyncio.Task). It wraps an event-based suspension and can be awaited to
    get the activity result.
    """

    def __init__(
        self,
        seq: int,
        *,
        is_local: bool = False,
    ) -> None:
        self._seq = seq
        self._is_local = is_local

    def cancel(self) -> bool:
        """Request cancellation of the activity.

        Returns:
            Always True (the cancellation request is always sent).
        """
        from temporalio_trio.worker._activation import RequestCancelActivityCommand

        runtime = _Runtime.current()
        rt = runtime._workflow_runtime()
        rt.commands.append(RequestCancelActivityCommand(seq=self._seq))
        return True

    def __await__(self) -> Any:
        return self._wait().__await__()

    async def _wait(self) -> ReturnType:
        """Wait for the activity to complete and return its result."""
        runtime = _Runtime.current()
        return await runtime.workflow_wait_activity(self._seq)


class ActivityConfig(TypedDict, total=False):
    """TypedDict of config that can be used for :py:func:`start_activity` and
    :py:func:`execute_activity`.
    """

    task_queue: Optional[str]
    schedule_to_close_timeout: Optional[timedelta]
    schedule_to_start_timeout: Optional[timedelta]
    start_to_close_timeout: Optional[timedelta]
    heartbeat_timeout: Optional[timedelta]
    retry_policy: Optional[temporalio.common.RetryPolicy]
    cancellation_type: ActivityCancellationType
    activity_id: Optional[str]
    versioning_intent: Optional[VersioningIntent]
    summary: Optional[str]
    priority: temporalio.common.Priority


class LocalActivityConfig(TypedDict, total=False):
    """TypedDict of config that can be used for :py:func:`start_local_activity`
    and :py:func:`execute_local_activity`.
    """

    schedule_to_close_timeout: Optional[timedelta]
    schedule_to_start_timeout: Optional[timedelta]
    start_to_close_timeout: Optional[timedelta]
    retry_policy: Optional[temporalio.common.RetryPolicy]
    local_retry_threshold: Optional[timedelta]
    cancellation_type: ActivityCancellationType
    activity_id: Optional[str]
    summary: Optional[str]


class ChildWorkflowConfig(TypedDict, total=False):
    """TypedDict of config that can be used for :py:func:`start_child_workflow`
    and :py:func:`execute_child_workflow`.
    """

    id: Optional[str]
    task_queue: Optional[str]
    cancellation_type: "ChildWorkflowCancellationType"
    parent_close_policy: "ParentClosePolicy"
    execution_timeout: Optional[timedelta]
    run_timeout: Optional[timedelta]
    task_timeout: Optional[timedelta]
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy
    retry_policy: Optional[temporalio.common.RetryPolicy]
    cron_schedule: str
    memo: Optional[Mapping[str, Any]]
    search_attributes: Optional[
        temporalio.common.SearchAttributes | temporalio.common.TypedSearchAttributes
    ]
    versioning_intent: Optional["VersioningIntent"]
    static_summary: Optional[str]
    static_details: Optional[str]
    priority: temporalio.common.Priority


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


class HandlerUnfinishedPolicy(IntEnum):
    """Policy for handling unfinished handlers when a workflow completes."""

    WARN_AND_ABANDON = 1
    """Log a warning and abandon the handler."""

    ABANDON = 2
    """Silently abandon the handler."""


class VersioningIntent(IntEnum):
    """Indicates whether the user intends certain commands to be run on a compatible or current worker build-ID."""

    COMPATIBLE = 1
    """Use the most recent compatible version."""

    DEFAULT = 2
    """Use the current deployment default."""


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


class NondeterminismError(temporalio.exceptions.FailureError):
    """Error raised when a workflow is non-deterministic."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class ReadOnlyContextError(Exception):
    """Error raised when a mutating operation is attempted in a read-only context (query/validator)."""

    pass


@dataclass(frozen=True)
class UpdateInfo:
    """Information about a workflow update."""

    id: str
    """Update ID."""

    name: str
    """Update type name."""


_current_update_info: ContextVar[UpdateInfo | None] = ContextVar(
    "__temporal_current_update_info", default=None
)


def _set_current_update_info(info: UpdateInfo) -> None:
    """Set the current update info (internal use)."""
    _current_update_info.set(info)


def current_update_info() -> UpdateInfo | None:
    """Info for the current update if any.

    This is powered by :py:mod:`contextvars` so it is only valid within the
    update handler and coroutines/tasks it has started.

    Returns:
        Info for the current update handler the code calling this is executing
        within if any.
    """
    return _current_update_info.get(None)


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
    async def workflow_execute_local_activity(
        self,
        activity: str | Callable[..., Any],
        *args: Any,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        local_retry_threshold: timedelta | None = None,
        activity_id: str | None = None,
        cancellation_type: "ActivityCancellationType" = ActivityCancellationType.TRY_CANCEL,
    ) -> Any:
        """Execute a local activity and wait for its result.

        Local activities run on the same task queue as the workflow and are
        optimized for short-lived activities. They do not have a heartbeat
        timeout or a separate task queue parameter.

        Args:
            activity: Activity name or function reference.
            *args: Arguments to pass to the activity.
            schedule_to_close_timeout: Max time for activity from schedule to completion.
            schedule_to_start_timeout: Max time waiting for worker to pick up activity.
            start_to_close_timeout: Max time for activity execution.
            retry_policy: Retry policy for the activity.
            local_retry_threshold: Duration after which retries happen on the server
                instead of locally. If unset, retries always happen locally.
            activity_id: Optional unique identifier for the activity.
            cancellation_type: How an activity cancellation should be handled.

        Returns:
            The activity result.

        Raises:
            RuntimeError: If the activity fails or is cancelled.
        """
        ...

    def workflow_start_activity(
        self,
        activity: Any,
        *args: Any,
        task_queue: str | None = None,
        result_type: type | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        cancellation_type: "ActivityCancellationType" = ActivityCancellationType.TRY_CANCEL,
        activity_id: str | None = None,
        versioning_intent: "VersioningIntent | None" = None,
        summary: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> "ActivityHandle[Any]":
        """Start an activity and return a handle without waiting for completion.

        Subclasses should override this method. The default implementation
        raises NotImplementedError.
        """
        raise NotImplementedError

    def workflow_start_local_activity(
        self,
        activity: Any,
        *args: Any,
        result_type: type | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        retry_policy: temporalio.common.RetryPolicy | None = None,
        local_retry_threshold: timedelta | None = None,
        cancellation_type: "ActivityCancellationType" = ActivityCancellationType.TRY_CANCEL,
        activity_id: str | None = None,
        summary: str | None = None,
    ) -> "ActivityHandle[Any]":
        """Start a local activity and return a handle without waiting for completion.

        Subclasses should override this method. The default implementation
        raises NotImplementedError.
        """
        raise NotImplementedError

    async def workflow_wait_activity(self, seq: int) -> Any:
        """Wait for an activity to complete by sequence number.

        Subclasses should override this method. The default implementation
        raises NotImplementedError.
        """
        raise NotImplementedError

    def _workflow_runtime(self) -> Any:
        """Get the underlying WorkflowRuntime for direct access.

        Subclasses should override this method. The default implementation
        raises NotImplementedError.
        """
        raise NotImplementedError

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
        cron_schedule: str = "",
        memo: Mapping[str, Any] | None = None,
        search_attributes: temporalio.common.SearchAttributes
        | temporalio.common.TypedSearchAttributes
        | None = None,
        versioning_intent: "VersioningIntent | None" = None,
        static_summary: str | None = None,
        static_details: str | None = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
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
            cron_schedule: Optional cron schedule string.
            memo: Optional memo key-value pairs.
            search_attributes: Optional search attributes.

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
        memo: Mapping[str, Any] | None = None,
        search_attributes: temporalio.common.SearchAttributes
        | temporalio.common.TypedSearchAttributes
        | None = None,
        versioning_intent: "VersioningIntent | None" = None,
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
            memo: Optional memo key-value pairs for the new execution.
            search_attributes: Optional search attributes for the new execution.

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
    async def workflow_cancel_external_workflow(
        self,
        workflow_id: str,
        *,
        run_id: str | None,
    ) -> None:
        """Cancel an external workflow.

        Args:
            workflow_id: ID of the external workflow to cancel.
            run_id: Optional run ID to target a specific run.

        Raises:
            RuntimeError: If the cancellation request fails.
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

    @abstractmethod
    def workflow_memo(self) -> Mapping[str, Any]:
        """Get the current workflow's memo values, converted to Python values.

        Returns:
            Mapping of memo keys to their decoded values.
        """
        ...

    @abstractmethod
    def workflow_payload_converter(self) -> temporalio.converter.PayloadConverter:
        """Get the payload converter for the current workflow.

        Returns:
            The payload converter in use.
        """
        ...

    @abstractmethod
    def workflow_instance(self) -> Any:
        """Get the current workflow instance object.

        Returns:
            The currently running workflow instance.
        """
        ...

    @abstractmethod
    def workflow_get_current_details(self) -> str:
        """Get the current workflow details string.

        Returns:
            The current details string.
        """
        ...

    @abstractmethod
    def workflow_set_current_details(self, details: str) -> None:
        """Set the current workflow details string.

        Args:
            details: The details string to set.
        """
        ...

    @abstractmethod
    def workflow_get_current_build_id(self) -> str | None:
        """Get the current worker's build ID, if any.

        Returns:
            The build ID string, or None if not set.
        """
        ...

    @abstractmethod
    def workflow_get_current_history_length(self) -> int:
        """Get the current number of events in the workflow's history.

        Returns:
            The number of history events.
        """
        ...

    @abstractmethod
    def workflow_get_current_history_size(self) -> int:
        """Get the current size of the workflow's history in bytes.

        Returns:
            The size of the history in bytes.
        """
        ...

    @abstractmethod
    def workflow_is_continue_as_new_suggested(self) -> bool:
        """Whether it's suggested to continue as new.

        Returns:
            True if continue-as-new is suggested.
        """
        ...

    def workflow_get_signal_handler(self, name: str | None) -> Callable | None:
        """Get the current signal handler for the given name."""
        raise NotImplementedError

    def workflow_set_signal_handler(
        self, name: str | None, handler: Callable | None
    ) -> None:
        """Set the signal handler for the given name."""
        raise NotImplementedError

    def workflow_get_query_handler(self, name: str | None) -> Callable | None:
        """Get the current query handler for the given name."""
        raise NotImplementedError

    def workflow_set_query_handler(
        self, name: str | None, handler: Callable | None
    ) -> None:
        """Set the query handler for the given name."""
        raise NotImplementedError

    def workflow_get_update_handler(self, name: str | None) -> Callable | None:
        """Get the current update handler for the given name."""
        raise NotImplementedError

    def workflow_set_update_handler(
        self,
        name: str | None,
        handler: Callable | None,
        *,
        validator: Callable | None = None,
    ) -> None:
        """Set the update handler for the given name."""
        raise NotImplementedError

    @abstractmethod
    def workflow_all_handlers_finished(self) -> bool:
        """Whether all update and signal handlers have finished executing.

        Returns:
            True if there are no in-progress update or signal handler executions.
        """
        ...

    def workflow_has_last_completion_result(self) -> bool:
        """Whether there is a last completion result.

        Returns:
            True if a last completion result is present.
        """
        raise NotImplementedError

    def workflow_last_completion_result(
        self, type_hint: type | None = None
    ) -> Any | None:
        """Get the last completion result if any.

        Args:
            type_hint: Optional type hint for deserialization.

        Returns:
            The last completion result, or None.
        """
        raise NotImplementedError

    def workflow_last_failure(self) -> BaseException | None:
        """Get the last failure if any.

        Returns:
            The last failure, or None.
        """
        raise NotImplementedError

    def workflow_metric_meter(self) -> temporalio.common.MetricMeter:
        """Get the metric meter for this workflow.

        Returns:
            The metric meter.
        """
        raise NotImplementedError

    @property
    def is_replaying(self) -> bool:
        """Whether the current activation is replaying from history.

        Subclasses should override this to return the actual replay state.
        """
        return False


@dataclass
class _SignalDefinition:
    """Signal handler definition metadata."""

    name: str | None  # None for dynamic handlers
    fn: Callable[..., None | Awaitable[None]]
    is_method: bool
    description: str | None = None
    unfinished_policy: HandlerUnfinishedPolicy = (
        HandlerUnfinishedPolicy.WARN_AND_ABANDON
    )
    arg_types: list[type] | None = None

    @staticmethod
    def from_fn(fn: Callable) -> "_SignalDefinition | None":
        """Get signal definition from a function if it has one."""
        return getattr(fn, "__temporal_signal_definition", None)

    def __post_init__(self) -> None:
        if self.arg_types is None:
            arg_types, _ = temporalio.common._type_hints_from_func(self.fn)
            self.arg_types = arg_types


@dataclass
class _QueryDefinition:
    """Query handler definition metadata."""

    name: str | None  # None for dynamic handlers
    fn: Callable[..., Any]
    is_method: bool
    description: str | None = None
    arg_types: list[type] | None = None
    ret_type: type | None = None

    @staticmethod
    def from_fn(fn: Callable) -> "_QueryDefinition | None":
        """Get query definition from a function if it has one."""
        return getattr(fn, "__temporal_query_definition", None)

    def __post_init__(self) -> None:
        if self.arg_types is None and self.ret_type is None:
            arg_types, ret_type = temporalio.common._type_hints_from_func(self.fn)
            self.arg_types = arg_types
            self.ret_type = ret_type


@dataclass
class _UpdateDefinition:
    """Update handler definition metadata."""

    name: str | None  # None for dynamic handlers
    fn: Callable[..., Any]
    is_method: bool
    unfinished_policy: HandlerUnfinishedPolicy = (
        HandlerUnfinishedPolicy.WARN_AND_ABANDON
    )
    description: str | None = None
    arg_types: list[type] | None = None
    ret_type: type | None = None
    validator: Callable[..., None] | None = None
    dynamic_vararg: bool = False

    @staticmethod
    def from_fn(fn: Callable) -> "_UpdateDefinition | None":
        """Get update definition from a function if it has one."""
        return getattr(fn, "__temporal_update_definition", None)

    def set_validator(self, validator: Callable[..., None]) -> None:
        """Set the validator function for this update handler."""
        if self.validator is not None:
            raise RuntimeError(f"Validator already set for update {self.name}")
        self.validator = validator

    def __post_init__(self) -> None:
        if self.arg_types is None:
            arg_types, ret_type = temporalio.common._type_hints_from_func(self.fn)
            self.arg_types = arg_types
            self.ret_type = ret_type


@dataclass
class _Definition:
    """Workflow definition metadata.

    This stores metadata about a workflow class that was decorated with
    @workflow.defn. The definition is attached to the class and can be
    retrieved using from_class() or must_from_class().
    """

    name: str | None
    """Name of the workflow (used for registration and lookup). None for dynamic workflows."""

    cls: type
    """The workflow class."""

    run_fn: Callable[..., Awaitable[Any]]
    """The method decorated with @workflow.run."""

    signals: dict[str | None, _SignalDefinition] = field(default_factory=dict)
    """Signal handlers for this workflow, keyed by signal name (None for dynamic)."""

    queries: dict[str | None, _QueryDefinition] = field(default_factory=dict)
    """Query handlers for this workflow, keyed by query name (None for dynamic)."""

    updates: dict[str | None, _UpdateDefinition] = field(default_factory=dict)
    """Update handlers for this workflow, keyed by update name (None for dynamic)."""

    sandboxed: bool = True
    """Whether the workflow should run in a sandbox. Accepted but ignored
    (Trio SDK does not implement sandboxing)."""

    dynamic: bool = False
    """If True, this workflow accepts dynamic dispatching and name is None."""

    failure_exception_types: Sequence[type[BaseException]] = field(default_factory=list)
    """Exception types that cause workflow failure instead of task failure."""

    init_fn: Callable | None = None
    """The ``__init__`` method if decorated with ``@workflow.init``, or None."""

    arg_types: list[type] | None = None
    """Argument types extracted from the run method's type hints."""

    ret_type: type | None = None
    """Return type extracted from the run method's return type annotation."""

    versioning_behavior: temporalio.common.VersioningBehavior | None = None
    """Versioning behavior for this workflow definition."""

    _ATTR_NAME: str = "__temporal_workflow_definition"

    def __post_init__(self) -> None:
        if self.arg_types is None and self.ret_type is None:
            arg_types, ret_type = temporalio.common._type_hints_from_func(self.run_fn)
            self.arg_types = arg_types
            self.ret_type = ret_type

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

    @staticmethod
    def from_run_fn(fn: Callable[..., Awaitable[Any]]) -> _Definition | None:
        """Get definition from a workflow's run method.

        The definition is attached to the run method by the ``@workflow.defn``
        decorator.

        Args:
            fn: The run method to get definition from.

        Returns:
            The workflow definition, or None if not a workflow run method.
        """
        return getattr(fn, "__temporal_workflow_definition", None)


def init(fn: F) -> F:
    """Decorator for the workflow init method.

    This may be used on the ``__init__`` method of the workflow class to specify
    that it accepts the same workflow input arguments as the ``@workflow.run``
    method. If used, the parameters of your ``__init__`` and ``@workflow.run``
    methods must be identical.

    Args:
        fn: The ``__init__`` method to decorate.

    Returns:
        The decorated method.

    Raises:
        ValueError: If used on a method other than ``__init__``.
    """
    if fn.__name__ != "__init__":
        raise ValueError("@workflow.init may only be used on the __init__ method")
    setattr(fn, "__temporal_workflow_init", True)
    return fn


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
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
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
        unfinished_policy: Policy for handling unfinished signal handlers
            when the workflow completes. Defaults to WARN_AND_ABANDON.
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
            unfinished_policy=unfinished_policy,
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


def _update_validator(
    update_def: _UpdateDefinition, fn: Callable[..., None] | None = None
) -> Callable[..., None] | None:
    """Decorator for a workflow update validator method."""
    if fn is not None:
        update_def.set_validator(fn)
    return fn


def update(
    fn: Callable | None = None,
    *,
    name: str | None = None,
    dynamic: bool = False,
    unfinished_policy: HandlerUnfinishedPolicy = HandlerUnfinishedPolicy.WARN_AND_ABANDON,
    description: str | None = None,
) -> Any:
    """Decorator for workflow update handler methods.

    Update handlers can be sync or async and return a value to the caller.
    They also support an optional validator that runs before the handler.

    Example:
        @workflow.defn
        class MyWorkflow:
            @workflow.update
            async def my_update(self, value: int) -> str:
                self.value = value
                return f"updated to {value}"

            @workflow.update(name="custom-name")
            async def another_update(self) -> None:
                pass

    Validators:
        @workflow.defn
        class MyWorkflow:
            @workflow.update
            async def my_update(self, value: int) -> str:
                self.value = value
                return f"updated to {value}"

            @my_update.validator
            def validate_my_update(self, value: int) -> None:
                if value < 0:
                    raise ValueError("Value must be non-negative")

    Args:
        fn: The function to decorate (when used without parentheses)
        name: Custom update name. Defaults to function name.
        dynamic: If True, this is a dynamic handler for any update name.
        unfinished_policy: Policy for handling unfinished update handlers
            when the workflow completes. Defaults to WARN_AND_ABANDON.
        description: Optional description for the update.
    """
    from functools import partial

    def decorator(fn: Callable) -> Callable:
        if dynamic:
            update_name = None
        elif name:
            update_name = name
        else:
            update_name = fn.__name__

        defn = _UpdateDefinition(
            name=update_name,
            fn=fn,
            is_method=True,
            unfinished_policy=unfinished_policy,
            description=description,
        )
        setattr(fn, "__temporal_update_definition", defn)
        setattr(fn, "validator", partial(_update_validator, defn))
        return fn

    if fn is not None:
        return decorator(fn)
    return decorator


def defn(
    cls: type | None = None,
    *,
    name: str | None = None,
    sandboxed: bool = True,
    dynamic: bool = False,
    failure_exception_types: Sequence[type[BaseException]] = [],
    versioning_behavior: temporalio.common.VersioningBehavior = temporalio.common.VersioningBehavior.UNSPECIFIED,
) -> Any:
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
        sandboxed: Whether to run in a sandbox. Accepted but ignored
            (Trio SDK does not implement sandboxing).
        dynamic: If True, the workflow name is None and it accepts
            dynamic dispatching.
        failure_exception_types: Exception types that cause workflow
            failure instead of task failure.

    Returns:
        The decorated class, or a decorator function.

    Raises:
        ValueError: If the class doesn't have a @workflow.run method,
            or has multiple @workflow.run methods.
    """

    def decorator(cls: type) -> type:
        init_fn: Callable | None = None
        run_fn: Callable[..., Awaitable[Any]] | None = None
        signals: dict[str | None, _SignalDefinition] = {}
        queries: dict[str | None, _QueryDefinition] = {}
        updates: dict[str | None, _UpdateDefinition] = {}

        for attr_name in dir(cls):
            method = getattr(cls, attr_name, None)
            if method is None:
                continue

            # Check for @workflow.init on __init__
            if attr_name == "__init__" and hasattr(method, "__temporal_workflow_init"):
                init_fn = method
                continue

            if attr_name.startswith("_"):
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

            # Collect update handlers
            update_defn = _UpdateDefinition.from_fn(method)
            if update_defn is not None:
                if update_defn.name in updates:
                    raise ValueError(
                        f"Duplicate update handler for '{update_defn.name}'"
                    )
                updates[update_defn.name] = update_defn

        if run_fn is None:
            raise ValueError(
                f"Workflow class {cls.__name__} must have a @workflow.run method"
            )

        # When dynamic=True, the workflow name is None (accepts dynamic dispatching)
        if dynamic:
            workflow_name = None
        else:
            workflow_name = name or cls.__name__

        definition = _Definition(
            name=workflow_name,
            cls=cls,
            run_fn=run_fn,
            signals=signals,
            queries=queries,
            updates=updates,
            sandboxed=sandboxed,
            dynamic=dynamic,
            failure_exception_types=list(failure_exception_types),
            init_fn=init_fn,
            versioning_behavior=versioning_behavior,
        )
        setattr(cls, _Definition._ATTR_NAME, definition)
        # Also attach definition to the run method for from_run_fn() lookup
        setattr(run_fn, "__temporal_workflow_definition", definition)
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


async def sleep(duration: float | timedelta, *, summary: str | None = None) -> None:
    """Sleep for the given duration.

    This pauses workflow execution for the specified duration. During replay,
    the sleep completes immediately based on recorded history.

    Args:
        duration: Sleep duration in seconds or as a timedelta.
        summary: Optional description for debugging/visibility.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    await _Runtime.current().workflow_sleep(
        duration=duration.total_seconds()
        if isinstance(duration, timedelta)
        else duration,
        summary=summary,
    )


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


def now() -> datetime:
    """Current time from the workflow perspective.

    This is the workflow equivalent of :py:func:`datetime.now` with the
    :py:attr:`timezone.utc` parameter.

    Returns:
        UTC datetime for the current workflow time. The datetime does have UTC
        set as the time zone.
    """
    return datetime.fromtimestamp(time(), timezone.utc)


def in_workflow() -> bool:
    """Whether the code is currently running in a workflow."""
    return _Runtime.maybe_current() is not None


def info() -> Info:
    """Get information about the current workflow.

    Returns information about the currently executing workflow, including
    its ID, type, run ID, and task queue.

    Returns:
        Information about the current workflow.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    runtime = _Runtime.current()
    outbound = getattr(runtime, "outbound_interceptor", None)
    if outbound is not None:
        return outbound.info()
    return runtime.workflow_info()


def memo() -> Mapping[str, Any]:
    """Current workflow's memo values, converted without type hints.

    Since type hints are not used, the default converted values will come back.
    For example, if the memo was originally created with a dataclass, the value
    will be a dict.

    Returns:
        Mapping of all memo keys and their values without type hints.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_memo()


def payload_converter() -> temporalio.converter.PayloadConverter:
    """Get the payload converter for the current workflow.

    This is often used for dynamic workflows/signals/queries to convert
    payloads.

    Returns:
        The payload converter in use.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_payload_converter()


def instance() -> Any:
    """Get the current workflow's instance object.

    Returns:
        The currently running workflow instance.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_instance()


def get_current_details() -> str:
    """Get the current details of the workflow which may appear in the UI/CLI.

    Unlike static details set at start, this value can be updated throughout
    the life of the workflow and is independent of the static details.
    This can be in Temporal markdown format and can span multiple lines.

    Returns:
        The current details string.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_get_current_details()


def set_current_details(description: str) -> None:
    """Set the current details of the workflow which may appear in the UI/CLI.

    Unlike static details set at start, this value can be updated throughout
    the life of the workflow and is independent of the static details.
    This can be in Temporal markdown format and can span multiple lines.

    Args:
        description: The details string to set.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    _Runtime.current().workflow_set_current_details(description)


def has_last_completion_result() -> bool:
    """Whether there is a last completion result.

    This is typically used in cron-like workflows to check if the previous
    run completed successfully.

    Returns:
        True if there is a last completion result.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_has_last_completion_result()


def get_last_completion_result(type_hint: type | None = None) -> Any | None:
    """Get the last completion result if any.

    This is typically used in cron-like workflows to carry state from the
    previous run.

    Args:
        type_hint: Optional type hint for deserialization.

    Returns:
        The last completion result, or None.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_last_completion_result(type_hint)


def get_last_failure() -> BaseException | None:
    """Get the failure from the previous run if any.

    This is typically used in cron-like workflows to handle previous failures.

    Returns:
        The last failure, or None.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_last_failure()


def metric_meter() -> temporalio.common.MetricMeter:
    """Get the metric meter for this workflow.

    Returns:
        The metric meter.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_metric_meter()


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


def start_activity(
    activity: str | Callable[..., Any],
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    activity_id: str | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[Any]:
    """Start an activity and return its handle.

    At least one of ``schedule_to_close_timeout`` or ``start_to_close_timeout``
    must be present.

    Args:
        activity: Activity name or function reference.
        arg: Single argument to the activity.
        args: Multiple arguments to the activity. Cannot be set if arg is.
        result_type: For string activities, this can set the specific result
            type hint to deserialize into.
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
        cancellation_type: How the activity is treated when it is cancelled from
            the workflow.
        activity_id: Optional unique identifier for the activity. This is an
            advanced setting that should not be set unless users are sure they
            need to. Contact Temporal before setting this value.
        versioning_intent: When using the Worker Versioning feature, specifies
            whether this Activity should run on a worker with a compatible
            Build Id or not.
        summary: A single-line fixed summary for this activity that may appear
            in UI/CLI. This can be in single-line Temporal markdown format.
        priority: Priority of the activity.

    Returns:
        An activity handle that can be awaited for the result.
    """
    return _Runtime.current().workflow_start_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        task_queue=task_queue,
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        activity_id=activity_id,
        cancellation_type=cancellation_type,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


async def execute_activity(
    activity: str | Callable[..., Any],
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    activity_id: str | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> Any:
    """Start an activity and wait for completion.

    This is a shortcut for ``await`` :py:meth:`start_activity`.
    """
    # We call the runtime directly instead of top-level start_activity to ensure
    # we don't miss new parameters
    return await _Runtime.current().workflow_start_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        task_queue=task_queue,
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        activity_id=activity_id,
        cancellation_type=cancellation_type,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


def start_local_activity(
    activity: str | Callable[..., Any],
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    activity_id: str | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    summary: str | None = None,
) -> ActivityHandle[Any]:
    """Start a local activity and return its handle.

    At least one of ``schedule_to_close_timeout`` or ``start_to_close_timeout``
    must be present.

    Args:
        activity: Activity name or function reference.
        arg: Single argument to the activity.
        args: Multiple arguments to the activity. Cannot be set if arg is.
        result_type: For string activities, this can set the specific result
            type hint to deserialize into.
        schedule_to_close_timeout: Max amount of time the activity can take from
            first being scheduled to being completed before it times out. This
            is inclusive of all retries.
        schedule_to_start_timeout: Max amount of time the activity can take to
            be started from first being scheduled.
        start_to_close_timeout: Max amount of time a single activity run can
            take from when it starts to when it completes. This is per retry.
        retry_policy: How an activity is retried on failure. If unset, an
            SDK-defined default is used. Set maximum attempts to 1 to disable
            retries.
        local_retry_threshold: Duration after which retries happen on the server
            instead of locally.
        activity_id: Optional unique identifier for the activity. This is an
            advanced setting that should not be set unless users are sure they
            need to. Contact Temporal before setting this value.
        cancellation_type: How the activity is treated when it is cancelled from
            the workflow.

    Returns:
        An activity handle that can be awaited for the result.
    """
    return _Runtime.current().workflow_start_local_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        activity_id=activity_id,
        cancellation_type=cancellation_type,
        summary=summary,
    )


async def execute_local_activity(
    activity: str | Callable[..., Any],
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    activity_id: str | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    summary: str | None = None,
) -> Any:
    """Start a local activity and wait for completion.

    This is a shortcut for ``await`` :py:meth:`start_local_activity`.
    """
    # We call the runtime directly instead of top-level start_local_activity to
    # ensure we don't miss new parameters
    return await _Runtime.current().workflow_start_local_activity(
        activity,
        *temporalio.common._arg_or_args(arg, args),
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        activity_id=activity_id,
        cancellation_type=cancellation_type,
        summary=summary,
    )


# Activity class/method variants - delegate to start_activity/start_local_activity
# These match the sdk-python API exactly


def start_activity_class(
    activity: type,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    activity_id: str | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[Any]:
    """Start a class-based activity and return its handle.

    This is the same as :py:func:`start_activity` but typed for class-based activities.
    """
    return start_activity(
        activity,
        arg,
        args=args,
        result_type=result_type,
        task_queue=task_queue,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        activity_id=activity_id,
        cancellation_type=cancellation_type,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


async def execute_activity_class(
    activity: type,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    activity_id: str | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> Any:
    """Start a class-based activity and wait for completion.

    This is the same as :py:func:`execute_activity` but typed for class-based activities.
    """
    return await execute_activity(
        activity,
        arg,
        args=args,
        result_type=result_type,
        task_queue=task_queue,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        activity_id=activity_id,
        cancellation_type=cancellation_type,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


def start_activity_method(
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    activity_id: str | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> ActivityHandle[Any]:
    """Start a method-based activity and return its handle.

    This is the same as :py:func:`start_activity` but typed for method-based activities.
    """
    return start_activity(
        activity,
        arg,
        args=args,
        result_type=result_type,
        task_queue=task_queue,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        activity_id=activity_id,
        cancellation_type=cancellation_type,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


async def execute_activity_method(
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    task_queue: str | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    heartbeat_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    activity_id: str | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    versioning_intent: VersioningIntent | None = None,
    summary: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
) -> Any:
    """Start a method-based activity and wait for completion.

    This is the same as :py:func:`execute_activity` but typed for method-based activities.
    """
    return await execute_activity(
        activity,
        arg,
        args=args,
        result_type=result_type,
        task_queue=task_queue,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        heartbeat_timeout=heartbeat_timeout,
        retry_policy=retry_policy,
        activity_id=activity_id,
        cancellation_type=cancellation_type,
        versioning_intent=versioning_intent,
        summary=summary,
        priority=priority,
    )


def start_local_activity_class(
    activity: type,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    activity_id: str | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    summary: str | None = None,
) -> ActivityHandle[Any]:
    """Start a class-based local activity and return its handle.

    This is the same as :py:func:`start_local_activity` but typed for class-based activities.
    """
    return start_local_activity(
        activity,
        arg,
        args=args,
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        activity_id=activity_id,
        cancellation_type=cancellation_type,
        summary=summary,
    )


async def execute_local_activity_class(
    activity: type,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    activity_id: str | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    summary: str | None = None,
) -> Any:
    """Start a class-based local activity and wait for completion.

    This is the same as :py:func:`execute_local_activity` but typed for class-based activities.
    """
    return await execute_local_activity(
        activity,
        arg,
        args=args,
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        activity_id=activity_id,
        cancellation_type=cancellation_type,
        summary=summary,
    )


def start_local_activity_method(
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    activity_id: str | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    summary: str | None = None,
) -> ActivityHandle[Any]:
    """Start a method-based local activity and return its handle.

    This is the same as :py:func:`start_local_activity` but typed for method-based activities.
    """
    return start_local_activity(
        activity,
        arg,
        args=args,
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        activity_id=activity_id,
        cancellation_type=cancellation_type,
        summary=summary,
    )


async def execute_local_activity_method(
    activity: Callable,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    result_type: type | None = None,
    schedule_to_close_timeout: timedelta | None = None,
    schedule_to_start_timeout: timedelta | None = None,
    start_to_close_timeout: timedelta | None = None,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    local_retry_threshold: timedelta | None = None,
    activity_id: str | None = None,
    cancellation_type: ActivityCancellationType = ActivityCancellationType.TRY_CANCEL,
    summary: str | None = None,
) -> Any:
    """Start a method-based local activity and wait for completion.

    This is the same as :py:func:`execute_local_activity` but typed for method-based activities.
    """
    return await execute_local_activity(
        activity,
        arg,
        args=args,
        result_type=result_type,
        schedule_to_close_timeout=schedule_to_close_timeout,
        schedule_to_start_timeout=schedule_to_start_timeout,
        start_to_close_timeout=start_to_close_timeout,
        retry_policy=retry_policy,
        local_retry_threshold=local_retry_threshold,
        activity_id=activity_id,
        cancellation_type=cancellation_type,
        summary=summary,
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


@dataclass(frozen=True)
class ParentInfo:
    """Information about a parent workflow."""

    namespace: str
    """Namespace of the parent workflow."""

    run_id: str
    """Run ID of the parent workflow."""

    workflow_id: str
    """Workflow ID of the parent workflow."""


@dataclass(frozen=True)
class RootInfo:
    """Information about the root workflow."""

    run_id: str
    """Run ID of the root workflow."""

    workflow_id: str
    """Workflow ID of the root workflow."""


@dataclass(frozen=True)
class Info:
    """Information about a running workflow."""

    workflow_id: str
    """Unique identifier for the workflow execution."""

    workflow_type: str
    """Name of the workflow type (from @workflow.defn)."""

    run_id: str
    """Unique identifier for this specific run."""

    task_queue: str
    """Task queue the workflow is running on."""

    namespace: str
    """Namespace the workflow is running in."""

    attempt: int
    """Starting at 1, the number of attempts for this workflow."""

    start_time: datetime
    """When the workflow execution started."""

    # Fields with defaults (order matches sdk-python semantics)
    continued_run_id: str | None = None
    """Run ID of the previous workflow which continued-as-new into this one."""

    cron_schedule: str | None = None
    """Cron schedule if this workflow runs on a cron."""

    execution_timeout: timedelta | None = None
    """Total workflow execution timeout including retries and continue as new."""

    first_execution_run_id: str = ""
    """Run ID of the very first execution in the continue-as-new chain."""

    headers: Mapping[str, temporalio.api.common.v1.Payload] = field(
        default_factory=dict
    )
    """Headers from the workflow start (e.g. for tracing/auth interceptors)."""

    parent: ParentInfo | None = None
    """Information about the parent workflow, if this is a child."""

    root: RootInfo | None = None
    """Information about the root workflow."""

    priority: temporalio.common.Priority = field(
        default_factory=lambda: temporalio.common.Priority.default
    )
    """Priority for this workflow."""

    raw_memo: Mapping[str, temporalio.api.common.v1.Payload] = field(
        default_factory=dict
    )
    """Raw memo payloads from the workflow start."""

    retry_policy: temporalio.common.RetryPolicy | None = None
    """Retry policy for this workflow."""

    run_timeout: timedelta | None = None
    """Timeout of a single workflow run."""

    search_attributes: temporalio.common.SearchAttributes = field(default_factory=dict)
    """Search attributes for this workflow (deprecated, use typed_search_attributes)."""

    task_timeout: timedelta = field(default_factory=lambda: timedelta(seconds=10))
    """Timeout of a single workflow task."""

    typed_search_attributes: temporalio.common.TypedSearchAttributes = field(
        default_factory=lambda: temporalio.common.TypedSearchAttributes([])
    )
    """Typed search attributes for this workflow."""

    workflow_start_time: datetime = field(
        default_factory=lambda: datetime(1970, 1, 1, tzinfo=timezone.utc)
    )
    """When the workflow was first started (not this run)."""


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

        # Route through outbound interceptor if available
        runtime = _Runtime.current()
        resolved_args = temporalio.common._arg_or_args(arg, args)
        outbound = getattr(runtime, "outbound_interceptor", None)
        if outbound is not None:
            await outbound.signal_child_workflow(
                _interceptor_mod().SignalChildWorkflowInput(
                    signal=signal_name,
                    args=resolved_args,
                    child_workflow_id=self._id,
                    headers={},
                )
            )
            return
        await runtime.workflow_signal_external_workflow(
            self._id,
            signal_name,
            resolved_args,
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

        resolved_args = temporalio.common._arg_or_args(arg, args)
        outbound = getattr(self._runtime, "outbound_interceptor", None)
        if outbound is not None:
            await outbound.signal_external_workflow(
                _interceptor_mod().SignalExternalWorkflowInput(
                    signal=signal_name,
                    args=resolved_args,
                    namespace=getattr(self._runtime, "namespace", "default"),
                    workflow_id=self._workflow_id,
                    workflow_run_id=self._run_id,
                    headers={},
                )
            )
            return
        await self._runtime.workflow_signal_external_workflow(
            self._workflow_id,
            signal_name,
            resolved_args,
            run_id=self._run_id,
        )

    async def cancel(self) -> None:
        """Send a cancellation request to this external workflow.

        This will fail if the workflow cannot accept the request (e.g. if the
        workflow is not found).
        """
        await self._runtime.workflow_cancel_external_workflow(
            self._workflow_id,
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
    result_type: type | None = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: timedelta | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cron_schedule: str = "",
    memo: Mapping[str, Any] | None = None,
    search_attributes: temporalio.common.SearchAttributes
    | temporalio.common.TypedSearchAttributes
    | None = None,
    versioning_intent: VersioningIntent | None = None,
    static_summary: str | None = None,
    static_details: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
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
        cron_schedule: Optional cron schedule string for the child workflow.
        memo: Optional memo key-value pairs to attach to the child workflow.
        search_attributes: Optional search attributes for the child workflow.

    Returns:
        A handle to the started child workflow.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
        RuntimeError: If the child workflow fails to start.
    """
    runtime = _Runtime.current()
    resolved_args = temporalio.common._arg_or_args(arg, args)
    resolved_id = id or str(uuid4())
    outbound = getattr(runtime, "outbound_interceptor", None)
    if outbound is not None:
        # Get workflow name
        if isinstance(workflow, str):
            wf_name = workflow
        elif isinstance(workflow, type):
            defn = _Definition.from_class(workflow)
            wf_name = (defn.name or workflow.__name__) if defn else workflow.__name__
        else:
            # It's a method reference (e.g., MyWorkflow.run)
            defn = _Definition.from_run_fn(workflow)
            if defn and defn.name:
                wf_name = defn.name
            else:
                qualname = getattr(workflow, "__qualname__", "")
                wf_name = (
                    qualname.rsplit(".", 1)[0]
                    if "." in qualname
                    else getattr(workflow, "__name__", str(workflow))
                )
        return await outbound.start_child_workflow(
            _interceptor_mod().StartChildWorkflowInput(
                workflow=wf_name,
                args=resolved_args,
                id=resolved_id,
                task_queue=task_queue,
                cancellation_type=cancellation_type,
                parent_close_policy=parent_close_policy,
                execution_timeout=execution_timeout,
                run_timeout=run_timeout,
                task_timeout=task_timeout,
                id_reuse_policy=id_reuse_policy,
                retry_policy=retry_policy,
                cron_schedule=cron_schedule,
                memo=memo,
                search_attributes=search_attributes,
                headers={},
                versioning_intent=versioning_intent,
                static_summary=static_summary,
                static_details=static_details,
                priority=priority,
                arg_types=None,
                ret_type=result_type,
            )
        )
    return await runtime.workflow_start_child_workflow(
        workflow,
        *resolved_args,
        id=resolved_id,
        task_queue=task_queue,
        cancellation_type=cancellation_type,
        parent_close_policy=parent_close_policy,
        execution_timeout=execution_timeout,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        id_reuse_policy=id_reuse_policy,
        retry_policy=retry_policy,
        cron_schedule=cron_schedule,
        memo=memo,
        search_attributes=search_attributes,
    )


async def execute_child_workflow(
    workflow: type | str,
    arg: Any = temporalio.common._arg_unset,
    *,
    args: Sequence[Any] = [],
    id: str | None = None,
    task_queue: str | None = None,
    result_type: type | None = None,
    cancellation_type: ChildWorkflowCancellationType = ChildWorkflowCancellationType.WAIT_CANCELLATION_COMPLETED,
    parent_close_policy: ParentClosePolicy = ParentClosePolicy.TERMINATE,
    execution_timeout: timedelta | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = temporalio.common.WorkflowIDReusePolicy.ALLOW_DUPLICATE,
    retry_policy: temporalio.common.RetryPolicy | None = None,
    cron_schedule: str = "",
    memo: Mapping[str, Any] | None = None,
    search_attributes: temporalio.common.SearchAttributes
    | temporalio.common.TypedSearchAttributes
    | None = None,
    versioning_intent: VersioningIntent | None = None,
    static_summary: str | None = None,
    static_details: str | None = None,
    priority: temporalio.common.Priority = temporalio.common.Priority.default,
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
        cron_schedule: Optional cron schedule string for the child workflow.
        memo: Optional memo key-value pairs to attach to the child workflow.
        search_attributes: Optional search attributes for the child workflow.

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
        result_type=result_type,
        cancellation_type=cancellation_type,
        parent_close_policy=parent_close_policy,
        execution_timeout=execution_timeout,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        id_reuse_policy=id_reuse_policy,
        retry_policy=retry_policy,
        cron_schedule=cron_schedule,
        memo=memo,
        search_attributes=search_attributes,
        versioning_intent=versioning_intent,
        static_summary=static_summary,
        static_details=static_details,
        priority=priority,
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
    memo: Mapping[str, Any] | None = None,
    search_attributes: temporalio.common.SearchAttributes
    | temporalio.common.TypedSearchAttributes
    | None = None,
    versioning_intent: VersioningIntent | None = None,
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
        memo: Optional memo key-value pairs for the new execution.
        search_attributes: Optional search attributes for the new execution.

    Raises:
        ContinueAsNewError: Always raised to stop the workflow.
        _NotInWorkflowContextError: If not in a workflow context.
    """
    runtime = _Runtime.current()
    resolved_args = temporalio.common._arg_or_args(arg, args)
    outbound = getattr(runtime, "outbound_interceptor", None)
    if outbound is not None:
        # Get workflow name
        wf_name = None
        if workflow is not None:
            if isinstance(workflow, str):
                wf_name = workflow
            else:
                defn = _Definition.from_class(workflow)
                wf_name = (
                    defn.name if defn else getattr(workflow, "__name__", str(workflow))
                )
        outbound.continue_as_new(
            _interceptor_mod().ContinueAsNewInput(
                workflow=wf_name,
                args=resolved_args,
                task_queue=task_queue,
                run_timeout=run_timeout,
                task_timeout=task_timeout,
                retry_policy=retry_policy,
                memo=memo,
                search_attributes=search_attributes,
                headers={},
                versioning_intent=versioning_intent,
                arg_types=None,
            )
        )
    runtime.workflow_continue_as_new(
        *resolved_args,
        workflow=workflow,
        task_queue=task_queue,
        run_timeout=run_timeout,
        task_timeout=task_timeout,
        retry_policy=retry_policy,
        memo=memo,
        search_attributes=search_attributes,
        versioning_intent=versioning_intent,
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


def get_signal_handler(name: str) -> Callable | None:
    """Get the current signal handler for the given name.

    Args:
        name: The signal name.

    Returns:
        The handler or None if not set.
    """
    return _Runtime.current().workflow_get_signal_handler(name)


def set_signal_handler(name: str, handler: Callable | None) -> None:
    """Set or remove the signal handler for the given name.

    Args:
        name: The signal name.
        handler: The handler to set, or None to remove.
    """
    _Runtime.current().workflow_set_signal_handler(name, handler)


def get_dynamic_signal_handler() -> Callable | None:
    """Get the current dynamic signal handler.

    Returns:
        The dynamic handler or None if not set.
    """
    return _Runtime.current().workflow_get_signal_handler(None)


def set_dynamic_signal_handler(handler: Callable | None) -> None:
    """Set or remove the dynamic signal handler.

    Args:
        handler: The handler to set, or None to remove.
    """
    _Runtime.current().workflow_set_signal_handler(None, handler)


def get_query_handler(name: str) -> Callable | None:
    """Get the current query handler for the given name.

    Args:
        name: The query name.

    Returns:
        The handler or None if not set.
    """
    return _Runtime.current().workflow_get_query_handler(name)


def set_query_handler(name: str, handler: Callable | None) -> None:
    """Set or remove the query handler for the given name.

    Args:
        name: The query name.
        handler: The handler to set, or None to remove.
    """
    _Runtime.current().workflow_set_query_handler(name, handler)


def get_dynamic_query_handler() -> Callable | None:
    """Get the current dynamic query handler.

    Returns:
        The dynamic handler or None if not set.
    """
    return _Runtime.current().workflow_get_query_handler(None)


def set_dynamic_query_handler(handler: Callable | None) -> None:
    """Set or remove the dynamic query handler.

    Args:
        handler: The handler to set, or None to remove.
    """
    _Runtime.current().workflow_set_query_handler(None, handler)


def get_update_handler(name: str) -> Callable | None:
    """Get the current update handler for the given name.

    Args:
        name: The update name.

    Returns:
        The handler or None if not set.
    """
    return _Runtime.current().workflow_get_update_handler(name)


def set_update_handler(
    name: str, handler: Callable | None, *, validator: Callable | None = None
) -> None:
    """Set or remove the update handler for the given name.

    Args:
        name: The update name.
        handler: The handler to set, or None to remove.
        validator: Optional validator function.
    """
    _Runtime.current().workflow_set_update_handler(name, handler, validator=validator)


def get_dynamic_update_handler() -> Callable | None:
    """Get the current dynamic update handler.

    Returns:
        The dynamic handler or None if not set.
    """
    return _Runtime.current().workflow_get_update_handler(None)


def set_dynamic_update_handler(
    handler: Callable | None, *, validator: Callable | None = None
) -> None:
    """Set or remove the dynamic update handler.

    Args:
        handler: The handler to set, or None to remove.
        validator: Optional validator function.
    """
    _Runtime.current().workflow_set_update_handler(None, handler, validator=validator)


def all_handlers_finished() -> bool:
    """Whether update and signal handlers have finished executing.

    Consider waiting on this condition before workflow return or continue-as-new,
    to prevent interruption of in-progress handlers by workflow exit:
    ``await workflow.wait_condition(lambda: workflow.all_handlers_finished())``

    Returns:
        True if there are no in-progress update or signal handler executions.
    """
    return _Runtime.current().workflow_all_handlers_finished()


def get_current_build_id() -> str | None:
    """Get the current worker's build ID, if any.

    Returns:
        The build ID string, or None if not set.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_get_current_build_id()


def get_current_history_length() -> int:
    """Get the current number of events in the workflow's history.

    Returns:
        The number of history events.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_get_current_history_length()


def get_current_history_size() -> int:
    """Get the current size of the workflow's history in bytes.

    Returns:
        The size of the history in bytes.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_get_current_history_size()


def is_continue_as_new_suggested() -> bool:
    """Whether it's suggested to continue as new.

    This is based on the workflow's history size and length. When this
    returns True, the workflow should consider calling continue_as_new()
    to keep the history manageable.

    Returns:
        True if continue-as-new is suggested.

    Raises:
        _NotInWorkflowContextError: If not in a workflow context.
    """
    return _Runtime.current().workflow_is_continue_as_new_suggested()


# =============================================================================
# Replay-safe Logger
# =============================================================================


class LoggerAdapter(logging.LoggerAdapter):
    """Workflow-aware logger that suppresses messages during replay.

    This adapter checks whether the current workflow activation is replaying
    from history. If so, log messages are suppressed to avoid duplicate log
    output during replay.

    Attributes:
        log_during_replay: If True, logs are emitted even during replay.
            Default is False.
    """

    def __init__(self, logger: logging.Logger, extra: Mapping[str, Any] | None) -> None:
        """Create the logger adapter.

        Args:
            logger: The underlying logger.
            extra: Extra context to add to log records.
        """
        super().__init__(logger, extra or {})
        self.log_during_replay = False

    def isEnabledFor(self, level: int) -> bool:
        """Override to suppress log messages during replay.

        Args:
            level: The log level to check.

        Returns:
            False if replaying and log_during_replay is False, otherwise
            delegates to the underlying logger.
        """
        if not self.log_during_replay:
            runtime = _Runtime.maybe_current()
            if runtime is not None and runtime.is_replaying:
                return False
        return super().isEnabledFor(level)

    @property
    def base_logger(self) -> logging.Logger:
        """Underlying logger usable for actions such as adding
        handlers/formatters.
        """
        return self.logger


logger: LoggerAdapter = LoggerAdapter(logging.getLogger("temporalio.workflow"), {})
"""Logger that will have contextual workflow details embedded.

Logs are skipped during replay by default.
"""
