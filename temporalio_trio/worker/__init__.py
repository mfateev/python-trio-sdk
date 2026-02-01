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
    # High-level Worker API
    "Worker",
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
