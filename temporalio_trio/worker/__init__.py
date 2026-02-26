"""Worker module for Temporal with Trio support.

This module provides the infrastructure for executing workflows and activities
using Trio as the async runtime.
"""

from temporalio_trio.worker._activation import (
    # Activity types (for workflow-activity integration)
    ActivityCancelledJob,
    ActivityResolvedJob,
    # Workflow commands
    CompleteWorkflowCommand,
    FailWorkflowCommand,
    RequestCancelActivityCommand,
    ScheduleActivityCommand,
    StartTimerCommand,
    # Workflow jobs
    TimerFiredJob,
    WorkflowActivation,
    WorkflowActivationCompletion,
    WorkflowStartedJob,
)
from temporalio_trio.worker._activity import TrioActivityWorker
from temporalio_trio.worker._clock import WorkflowClock
from temporalio_trio.worker._interceptor import (
    ActivityInboundInterceptor,
    ActivityOutboundInterceptor,
    ContinueAsNewInput,
    ExecuteActivityInput,
    ExecuteWorkflowInput,
    HandleQueryInput,
    HandleSignalInput,
    HandleUpdateInput,
    Interceptor,
    SignalChildWorkflowInput,
    SignalExternalWorkflowInput,
    StartActivityInput,
    StartChildWorkflowInput,
    StartLocalActivityInput,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)
from temporalio_trio.worker._replayer import (
    Replayer,
    ReplayerConfig,
    WorkflowReplayResult,
    WorkflowReplayResults,
)
from temporalio_trio.worker._single_thread_worker import SingleThreadWorker
from temporalio_trio.worker._worker import Worker
from temporalio_trio.worker._workflow_instance import (
    TrioWorkflowInstance,
    TrioWorkflowRunner,
    WorkflowInstance,
    WorkflowInstanceDetails,
    WorkflowRunner,
)

__all__ = [
    # Interceptor framework
    "Interceptor",
    "WorkflowInterceptorClassInput",
    "ExecuteActivityInput",
    "ActivityInboundInterceptor",
    "ActivityOutboundInterceptor",
    "ContinueAsNewInput",
    "ExecuteWorkflowInput",
    "HandleSignalInput",
    "HandleQueryInput",
    "HandleUpdateInput",
    "SignalChildWorkflowInput",
    "SignalExternalWorkflowInput",
    "StartActivityInput",
    "StartChildWorkflowInput",
    "StartLocalActivityInput",
    "WorkflowInboundInterceptor",
    "WorkflowOutboundInterceptor",
    # High-level Worker API
    "Worker",
    # Replayer
    "Replayer",
    "ReplayerConfig",
    "WorkflowReplayResult",
    "WorkflowReplayResults",
    # Single-threaded worker (new execution model)
    "SingleThreadWorker",
    # Activity worker
    "TrioActivityWorker",
    # Workflow activation types
    "WorkflowActivation",
    "WorkflowActivationCompletion",
    "WorkflowStartedJob",
    "TimerFiredJob",
    # Workflow command types
    "StartTimerCommand",
    "CompleteWorkflowCommand",
    "FailWorkflowCommand",
    # Activity types (for workflow-activity integration)
    "ActivityResolvedJob",
    "ActivityCancelledJob",
    "ScheduleActivityCommand",
    "RequestCancelActivityCommand",
    # Clock
    "WorkflowClock",
    # Runner types
    "WorkflowRunner",
    "TrioWorkflowRunner",
    # Instance types
    "WorkflowInstanceDetails",
    "WorkflowInstance",
    "TrioWorkflowInstance",
]
