"""Workflow interceptor framework for Temporal with Trio.

This module provides the interceptor infrastructure for workflows, allowing
custom logic to be injected into workflow execution, signal/query handling,
and outbound calls.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable, Mapping, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, NoReturn

__all__ = [
    # Main interceptor
    "Interceptor",
    "WorkflowInterceptorClassInput",
    # Inbound interceptor
    "WorkflowInboundInterceptor",
    # Outbound interceptor
    "WorkflowOutboundInterceptor",
    # Input dataclasses
    "ExecuteWorkflowInput",
    "HandleSignalInput",
    "HandleQueryInput",
    "HandleUpdateInput",
    "ContinueAsNewInput",
    "StartActivityInput",
    "StartChildWorkflowInput",
]


# =============================================================================
# Input Dataclasses
# =============================================================================


@dataclass
class ExecuteWorkflowInput:
    """Input for WorkflowInboundInterceptor.execute_workflow."""

    type: type
    """The workflow class type."""

    run_fn: Callable[..., Awaitable[Any]]
    """The workflow's run method (unbound)."""

    args: Sequence[Any]
    """Arguments to pass to the workflow."""

    headers: Mapping[str, Any]
    """Headers from the workflow start request."""


@dataclass
class HandleSignalInput:
    """Input for WorkflowInboundInterceptor.handle_signal."""

    signal: str
    """Name of the signal being handled."""

    args: Sequence[Any]
    """Arguments passed to the signal handler."""

    headers: Mapping[str, Any]
    """Headers from the signal request."""


@dataclass
class HandleQueryInput:
    """Input for WorkflowInboundInterceptor.handle_query."""

    id: str
    """Unique identifier for this query."""

    query: str
    """Name of the query being handled."""

    args: Sequence[Any]
    """Arguments passed to the query handler."""

    headers: Mapping[str, Any]
    """Headers from the query request."""


@dataclass
class HandleUpdateInput:
    """Input for WorkflowInboundInterceptor.handle_update_validator
    and WorkflowInboundInterceptor.handle_update_handler.
    """

    id: str
    """Unique identifier for this update."""

    update: str
    """Name of the update being handled."""

    args: Sequence[Any]
    """Arguments passed to the update handler/validator."""

    headers: Mapping[str, Any]
    """Headers from the update request."""


@dataclass
class ContinueAsNewInput:
    """Input for WorkflowOutboundInterceptor.continue_as_new."""

    workflow: str | None
    """Target workflow type name. None means same workflow."""

    args: Sequence[Any]
    """Arguments for the new workflow execution."""

    task_queue: str | None
    """Task queue for the new execution. None means same task queue."""

    run_timeout: timedelta | None
    """Run timeout for the new execution. None means same timeout."""

    task_timeout: timedelta | None
    """Task timeout for the new execution. None means same timeout."""

    memo: Mapping[str, Any] | None
    """Memo for the new execution. None means same memo."""

    headers: Mapping[str, Any]
    """Headers to send with continue-as-new."""


@dataclass
class StartActivityInput:
    """Input for WorkflowOutboundInterceptor.start_activity."""

    activity: str
    """Name of the activity to start."""

    args: Sequence[Any]
    """Arguments to pass to the activity."""

    activity_id: str | None
    """Optional custom activity ID."""

    task_queue: str | None
    """Task queue for the activity. None means workflow's task queue."""

    schedule_to_close_timeout: timedelta | None
    """Maximum time from scheduling to completion."""

    schedule_to_start_timeout: timedelta | None
    """Maximum time from scheduling to start."""

    start_to_close_timeout: timedelta | None
    """Maximum time from start to completion."""

    heartbeat_timeout: timedelta | None
    """Maximum time between heartbeats."""

    headers: Mapping[str, Any]
    """Headers to send with the activity request."""


@dataclass
class StartChildWorkflowInput:
    """Input for WorkflowOutboundInterceptor.start_child_workflow."""

    workflow: str
    """Name of the child workflow type."""

    args: Sequence[Any]
    """Arguments to pass to the child workflow."""

    id: str
    """Workflow ID for the child."""

    task_queue: str | None
    """Task queue for the child. None means parent's task queue."""

    execution_timeout: timedelta | None
    """Maximum time for the workflow execution including retries."""

    run_timeout: timedelta | None
    """Maximum time for a single workflow run."""

    task_timeout: timedelta | None
    """Maximum time for a single workflow task."""

    memo: Mapping[str, Any] | None
    """Memo for the child workflow."""

    headers: Mapping[str, Any]
    """Headers to send with the child workflow request."""


# =============================================================================
# Interceptor Classes
# =============================================================================


@dataclass
class WorkflowInterceptorClassInput:
    """Input for Interceptor.workflow_interceptor_class."""

    unsafe_extern_functions: MutableMapping[str, Callable]
    """Set of external functions that can be called from the workflow.

    Warning:
        Exposing external functions to the workflow is dangerous and should
        be avoided. Use at your own risk.
    """


class WorkflowInboundInterceptor:
    """Inbound interceptor to wrap workflow execution and signal/query handling.

    This should be extended by any workflow inbound interceptors.
    """

    def __init__(self, next: WorkflowInboundInterceptor) -> None:
        """Create the inbound interceptor.

        Args:
            next: The next interceptor in the chain. The default implementation
                of all calls is to delegate to the next interceptor.
        """
        self.next = next

    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        """Initialize with an outbound interceptor.

        To add a custom outbound interceptor, wrap the given interceptor before
        sending to the next ``init`` call.
        """
        self.next.init(outbound)

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        """Called to run the workflow."""
        return await self.next.execute_workflow(input)

    async def handle_signal(self, input: HandleSignalInput) -> None:
        """Called to handle a signal."""
        return await self.next.handle_signal(input)

    async def handle_query(self, input: HandleQueryInput) -> Any:
        """Called to handle a query."""
        return await self.next.handle_query(input)

    def handle_update_validator(self, input: HandleUpdateInput) -> None:
        """Called to handle an update's validation stage."""
        self.next.handle_update_validator(input)

    async def handle_update_handler(self, input: HandleUpdateInput) -> Any:
        """Called to handle an update's handler."""
        return await self.next.handle_update_handler(input)


class WorkflowOutboundInterceptor:
    """Outbound interceptor to wrap calls made from within workflows.

    This should be extended by any workflow outbound interceptors.
    """

    def __init__(self, next: WorkflowOutboundInterceptor) -> None:
        """Create the outbound interceptor.

        Args:
            next: The next interceptor in the chain. The default implementation
                of all calls is to delegate to the next interceptor.
        """
        self.next = next

    def continue_as_new(self, input: ContinueAsNewInput) -> NoReturn:
        """Called for every workflow.continue_as_new call."""
        self.next.continue_as_new(input)

    def start_activity(self, input: StartActivityInput) -> Any:
        """Called for every workflow.start_activity and execute_activity call."""
        return self.next.start_activity(input)

    async def start_child_workflow(self, input: StartChildWorkflowInput) -> Any:
        """Called for every workflow.start_child_workflow and
        execute_child_workflow call.
        """
        return await self.next.start_child_workflow(input)


class Interceptor:
    """Interceptor for workers.

    This should be extended by any worker interceptors.
    """

    def workflow_interceptor_class(
        self,
        input: WorkflowInterceptorClassInput,
    ) -> type[WorkflowInboundInterceptor] | None:
        """Class that will be instantiated and used to intercept workflows.

        This method is called on workflow start. The class must have the same
        init as WorkflowInboundInterceptor.__init__. The input can be altered
        to do things like add additional extern functions.

        Args:
            input: Input to this method that contains mutable properties that
                can be altered by this interceptor.

        Returns:
            The class to construct to intercept each workflow.
        """
        return None
