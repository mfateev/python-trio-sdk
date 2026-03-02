"""Temporal client for Trio async runtime.

This module provides a pure Trio client for interacting with Temporal workflows.
The API matches the official Temporal Python SDK, but uses Trio instead of asyncio.

Example:
    import trio
    from temporalio_trio.client import Client

    async def main():
        # Connect to Temporal
        client = await Client.connect("localhost:7233")

        try:
            # Start a workflow
            handle = await client.start_workflow(
                "MyWorkflow",
                "arg1",
                id="workflow-123",
                task_queue="my-queue",
            )

            # Wait for result
            result = await handle.result()
            print(f"Result: {result}")

        finally:
            await client.close()

    trio.run(main)
"""

from ._async_activity_handle import (
    AsyncActivityCancelledError,
    AsyncActivityHandle,
    AsyncActivityIDReference,
)
from ._client import (
    BuildIdOp,
    BuildIdOpAddNewCompatible,
    BuildIdOpAddNewDefault,
    BuildIdOpMergeSets,
    BuildIdOpPromoteBuildIdWithinSet,
    BuildIdOpPromoteSetByBuildId,
    BuildIdReachability,
    BuildIdVersionSet,
    Client,
    ClientConfig,
    HttpConnectProxyConfig,
    KeepAliveConfig,
    TLSConfig,
    WorkerBuildIdVersionSets,
    WorkerTaskReachability,
    WorkflowExecutionInfo,
)
from ._schedule import (
    Schedule,
    ScheduleAction,
    ScheduleActionExecutionStartWorkflow,
    ScheduleActionResult,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleBackfill,
    ScheduleCalendarSpec,
    ScheduleDescription,
    ScheduleHandle,
    ScheduleInfo,
    ScheduleIntervalSpec,
    ScheduleListEntry,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleRange,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
    ScheduleUpdateInput,
)
from ._with_start import WithStartWorkflowOperation
from ._workflow_handle import (
    WorkflowContinuedAsNewError,
    WorkflowExecutionDescription,
    WorkflowExecutionStatus,
    WorkflowFailureError,
    WorkflowHandle,
    WorkflowHistory,
    WorkflowQueryRejectedError,
    WorkflowUpdateHandle,
    WorkflowUpdateStage,
)

__all__ = [
    "AsyncActivityCancelledError",
    "AsyncActivityHandle",
    "AsyncActivityIDReference",
    "BuildIdOp",
    "BuildIdOpAddNewCompatible",
    "BuildIdOpAddNewDefault",
    "BuildIdOpMergeSets",
    "BuildIdOpPromoteBuildIdWithinSet",
    "BuildIdOpPromoteSetByBuildId",
    "BuildIdReachability",
    "BuildIdVersionSet",
    "Client",
    "ClientConfig",
    "Schedule",
    "ScheduleAction",
    "ScheduleActionExecutionStartWorkflow",
    "ScheduleActionResult",
    "ScheduleActionStartWorkflow",
    "ScheduleAlreadyRunningError",
    "ScheduleBackfill",
    "ScheduleCalendarSpec",
    "ScheduleDescription",
    "ScheduleHandle",
    "ScheduleInfo",
    "ScheduleIntervalSpec",
    "ScheduleListEntry",
    "ScheduleOverlapPolicy",
    "SchedulePolicy",
    "ScheduleRange",
    "ScheduleSpec",
    "ScheduleState",
    "ScheduleUpdate",
    "ScheduleUpdateInput",
    "HttpConnectProxyConfig",
    "KeepAliveConfig",
    "TLSConfig",
    "WithStartWorkflowOperation",
    "WorkerBuildIdVersionSets",
    "WorkerTaskReachability",
    "WorkflowContinuedAsNewError",
    "WorkflowExecutionDescription",
    "WorkflowExecutionInfo",
    "WorkflowExecutionStatus",
    "WorkflowFailureError",
    "WorkflowHandle",
    "WorkflowHistory",
    "WorkflowQueryRejectedError",
    "WorkflowUpdateHandle",
    "WorkflowUpdateStage",
]
