"""Type converters between bridge protobuf types and POC activation types.

This module provides functions to convert between:
- Bridge WorkflowActivation (protobuf) → POC WorkflowActivation (dataclass)
- POC WorkflowActivationCompletion (dataclass) → Bridge WorkflowActivationCompletion (protobuf)

These converters allow the POC runtime to work with the Temporal bridge without
needing to know about protobuf details.
"""

from __future__ import annotations

from datetime import timedelta

import google.protobuf.duration_pb2
import temporalio.api.common.v1
import temporalio.bridge.proto.activity_result.activity_result_pb2 as act_result_pb
import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb
import temporalio.bridge.proto.workflow_commands.workflow_commands_pb2 as cmd_pb
import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as comp_pb
import temporalio.converter

from temporalio_trio.worker._activation import (
    ActivityResolvedJob,
    CancelChildWorkflowCommand,
    CancelWorkflowCommand,
    CancelWorkflowJob,
    ChildWorkflowResolvedJob,
    ChildWorkflowStartedJob,
    ChildWorkflowStartFailedJob,
    CompleteWorkflowCommand,
    FailWorkflowCommand,
    QueryResultCommand,
    QueryWorkflowJob,
    RequestCancelActivityCommand,
    ScheduleActivityCommand,
    SignalWorkflowJob,
    StartChildWorkflowCommand,
    StartTimerCommand,
    TimerFiredJob,
    WorkflowActivation,
    WorkflowActivationCompletion,
    WorkflowStartedJob,
)

__all__ = [
    "bridge_to_poc_activation",
    "poc_to_bridge_completion",
]


def bridge_to_poc_activation(
    bridge_act: act_pb.WorkflowActivation,
    data_converter: temporalio.converter.DataConverter,
) -> WorkflowActivation:
    """Convert bridge WorkflowActivation to POC WorkflowActivation.

    Args:
        bridge_act: Bridge activation from poll_workflow_activation()
        data_converter: Data converter for deserializing payloads

    Returns:
        POC WorkflowActivation with converted jobs

    Raises:
        NotImplementedError: If an unsupported job type is encountered
    """
    # Convert timestamp to nanoseconds
    timestamp_ns = (
        bridge_act.timestamp.seconds * 1_000_000_000 + bridge_act.timestamp.nanos
    )

    # Convert jobs
    poc_jobs: list[
        WorkflowStartedJob
        | TimerFiredJob
        | CancelWorkflowJob
        | SignalWorkflowJob
        | QueryWorkflowJob
        | ActivityResolvedJob
        | ChildWorkflowStartedJob
        | ChildWorkflowStartFailedJob
        | ChildWorkflowResolvedJob
    ] = []
    for job in bridge_act.jobs:
        # Check which job type this is (oneof field)
        job_type = job.WhichOneof("variant")

        if job_type == "initialize_workflow":
            poc_jobs.append(
                _convert_initialize_workflow(job.initialize_workflow, data_converter)
            )
        elif job_type == "fire_timer":
            poc_jobs.append(_convert_fire_timer(job.fire_timer))
        elif job_type == "cancel_workflow":
            poc_jobs.append(_convert_cancel_workflow(job.cancel_workflow))
        elif job_type == "signal_workflow":
            poc_jobs.append(
                _convert_signal_workflow(job.signal_workflow, data_converter)
            )
        elif job_type == "query_workflow":
            poc_jobs.append(_convert_query_workflow(job.query_workflow, data_converter))
        elif job_type == "resolve_activity":
            poc_jobs.append(
                _convert_resolve_activity(job.resolve_activity, data_converter)
            )
        elif job_type == "resolve_child_workflow_execution_start":
            poc_jobs.append(
                _convert_resolve_child_workflow_start(
                    job.resolve_child_workflow_execution_start, data_converter
                )
            )
        elif job_type == "resolve_child_workflow_execution":
            poc_jobs.append(
                _convert_resolve_child_workflow(
                    job.resolve_child_workflow_execution, data_converter
                )
            )
        elif job_type == "remove_from_cache":
            # Eviction jobs are handled separately in the bridge worker
            # They should not be passed to the workflow instance
            continue
        else:
            # Unsupported job types - raise error with helpful message
            raise NotImplementedError(
                f"Job type '{job_type}' not yet supported. "
                f"Please file an issue if this is needed."
            )

    return WorkflowActivation(
        jobs=poc_jobs,
        timestamp_ns=timestamp_ns,
    )


def _convert_initialize_workflow(
    init: act_pb.InitializeWorkflow,
    data_converter: temporalio.converter.DataConverter,
) -> WorkflowStartedJob:
    """Convert InitializeWorkflow to WorkflowStartedJob.

    Args:
        init: Bridge InitializeWorkflow job
        data_converter: Data converter for deserializing arguments

    Returns:
        POC WorkflowStartedJob
    """
    # Convert protobuf Payload arguments to Python values
    # For now, we'll just pass them through as-is since the POC uses simple types
    # In a full implementation, we'd use the data converter here
    args: tuple = ()
    if init.arguments:
        # Convert each payload to Python value
        values = []
        for payload in init.arguments:
            # Use the data converter to decode the payload
            value = data_converter.payload_converter.from_payload(payload)
            values.append(value)
        args = tuple(values)

    return WorkflowStartedJob(
        workflow_type=init.workflow_type,
        args=args,
    )


def _convert_fire_timer(fire: act_pb.FireTimer) -> TimerFiredJob:
    """Convert FireTimer to TimerFiredJob.

    Args:
        fire: Bridge FireTimer job

    Returns:
        POC TimerFiredJob
    """
    return TimerFiredJob(timer_id=fire.seq)


def _convert_cancel_workflow(cancel: act_pb.CancelWorkflow) -> CancelWorkflowJob:
    """Convert CancelWorkflow to CancelWorkflowJob.

    Args:
        cancel: Bridge CancelWorkflow job

    Returns:
        POC CancelWorkflowJob
    """
    # CancelWorkflow has optional details field - for now we ignore it
    return CancelWorkflowJob()


def _convert_signal_workflow(
    signal: act_pb.SignalWorkflow,
    data_converter: temporalio.converter.DataConverter,
) -> SignalWorkflowJob:
    """Convert bridge SignalWorkflow to POC SignalWorkflowJob.

    Args:
        signal: Bridge SignalWorkflow job
        data_converter: Data converter for deserializing arguments

    Returns:
        POC SignalWorkflowJob
    """
    args = tuple(data_converter.payload_converter.from_payload(p) for p in signal.input)
    return SignalWorkflowJob(
        signal_name=signal.signal_name,
        args=args,
    )


def _convert_query_workflow(
    query: act_pb.QueryWorkflow,
    data_converter: temporalio.converter.DataConverter,
) -> QueryWorkflowJob:
    """Convert bridge QueryWorkflow to POC QueryWorkflowJob.

    Args:
        query: Bridge QueryWorkflow job
        data_converter: Data converter for deserializing arguments

    Returns:
        POC QueryWorkflowJob
    """
    args = tuple(
        data_converter.payload_converter.from_payload(p) for p in query.arguments
    )
    return QueryWorkflowJob(
        query_id=query.query_id,
        query_type=query.query_type,
        args=args,
    )


def _convert_resolve_activity(
    resolve: act_pb.ResolveActivity,
    data_converter: temporalio.converter.DataConverter,
) -> ActivityResolvedJob:
    """Convert ResolveActivity to ActivityResolvedJob.

    Args:
        resolve: Bridge ResolveActivity job
        data_converter: Data converter for deserializing result payload

    Returns:
        POC ActivityResolvedJob with result or failure
    """
    seq = resolve.seq
    result = None
    failure = None

    # Get the activity resolution
    resolution = resolve.result
    status = resolution.WhichOneof("status")

    if status == "completed":
        # Activity completed successfully - decode result
        if resolution.completed.result.ByteSize() > 0:
            result = data_converter.payload_converter.from_payload(
                resolution.completed.result
            )
    elif status == "failed":
        # Activity failed - convert to exception
        failure_msg = resolution.failed.failure.message
        failure = RuntimeError(f"Activity failed: {failure_msg}")
    elif status == "cancelled":
        # Activity was cancelled
        cancel_msg = (
            resolution.cancelled.failure.message
            if resolution.cancelled.failure.message
            else "Activity cancelled"
        )
        failure = RuntimeError(f"Activity cancelled: {cancel_msg}")
    elif status == "backoff":
        # Activity needs to retry after backoff - treat as transient failure
        # The SDK core handles retry scheduling, so this shouldn't typically reach here
        failure = RuntimeError("Activity scheduled for retry (backoff)")
    else:
        failure = RuntimeError(f"Unknown activity resolution status: {status}")

    return ActivityResolvedJob(
        seq=seq,
        result=result,
        failure=failure,
    )


def _convert_resolve_child_workflow_start(
    resolve: act_pb.ResolveChildWorkflowExecutionStart,
    data_converter: temporalio.converter.DataConverter,
) -> ChildWorkflowStartedJob | ChildWorkflowStartFailedJob:
    """Convert ResolveChildWorkflowExecutionStart to POC job.

    Args:
        resolve: Bridge ResolveChildWorkflowExecutionStart job
        data_converter: Data converter (not currently used but kept for consistency)

    Returns:
        Either ChildWorkflowStartedJob (if succeeded) or ChildWorkflowStartFailedJob (if failed/cancelled)
    """
    seq = resolve.seq
    status = resolve.WhichOneof("status")

    if status == "succeeded":
        return ChildWorkflowStartedJob(
            seq=seq,
            run_id=resolve.succeeded.run_id,
        )
    elif status == "failed":
        return ChildWorkflowStartFailedJob(
            seq=seq,
            workflow_id=resolve.failed.workflow_id,
            workflow_type=resolve.failed.workflow_type,
            cause=str(resolve.failed.cause),
        )
    elif status == "cancelled":
        # Cancelled during start - treat as start failure
        cancel_msg = (
            resolve.cancelled.failure.message
            if resolve.cancelled.failure.message
            else "Child workflow start cancelled"
        )
        return ChildWorkflowStartFailedJob(
            seq=seq,
            workflow_id="",
            workflow_type="",
            cause=f"CANCELLED: {cancel_msg}",
        )
    else:
        raise RuntimeError(f"Unknown child workflow start status: {status}")


def _convert_resolve_child_workflow(
    resolve: act_pb.ResolveChildWorkflowExecution,
    data_converter: temporalio.converter.DataConverter,
) -> ChildWorkflowResolvedJob:
    """Convert ResolveChildWorkflowExecution to ChildWorkflowResolvedJob.

    Args:
        resolve: Bridge ResolveChildWorkflowExecution job
        data_converter: Data converter for deserializing result payload

    Returns:
        POC ChildWorkflowResolvedJob with result or failure
    """
    seq = resolve.seq
    result = None
    failure = None

    # Get the child workflow result
    child_result = resolve.result
    status = child_result.WhichOneof("status")

    if status == "completed":
        # Child completed successfully - decode result
        if child_result.completed.result.ByteSize() > 0:
            result = data_converter.payload_converter.from_payload(
                child_result.completed.result
            )
    elif status == "failed":
        # Child workflow failed
        failure_msg = child_result.failed.failure.message
        failure = RuntimeError(f"Child workflow failed: {failure_msg}")
    elif status == "cancelled":
        # Child workflow was cancelled
        cancel_msg = (
            child_result.cancelled.failure.message
            if child_result.cancelled.failure.message
            else "Child workflow cancelled"
        )
        failure = RuntimeError(f"Child workflow cancelled: {cancel_msg}")
    else:
        failure = RuntimeError(f"Unknown child workflow result status: {status}")

    return ChildWorkflowResolvedJob(
        seq=seq,
        result=result,
        failure=failure,
    )


def _set_duration(
    duration_proto: google.protobuf.duration_pb2.Duration,
    td: timedelta,
) -> None:
    """Set a protobuf Duration from a Python timedelta.

    Args:
        duration_proto: The protobuf Duration to set
        td: The Python timedelta value
    """
    total_seconds = td.total_seconds()
    duration_proto.seconds = int(total_seconds)
    duration_proto.nanos = int((total_seconds - int(total_seconds)) * 1_000_000_000)


def poc_to_bridge_completion(
    run_id: str,
    poc_comp: WorkflowActivationCompletion,
    data_converter: temporalio.converter.DataConverter,
) -> comp_pb.WorkflowActivationCompletion:
    """Convert POC WorkflowActivationCompletion to bridge completion.

    Args:
        run_id: The run ID from the activation
        poc_comp: POC completion with commands
        data_converter: Data converter for serializing payloads

    Returns:
        Bridge WorkflowActivationCompletion protobuf
    """
    bridge_comp = comp_pb.WorkflowActivationCompletion()
    bridge_comp.run_id = run_id

    # Set successful status
    bridge_comp.successful.SetInParent()

    # Convert commands
    for cmd in poc_comp.commands:
        bridge_cmd = cmd_pb.WorkflowCommand()

        if isinstance(cmd, StartTimerCommand):
            # Convert StartTimerCommand to StartTimer
            bridge_cmd.start_timer.seq = cmd.timer_id
            # Convert milliseconds to Duration protobuf
            duration = google.protobuf.duration_pb2.Duration()
            duration.seconds = cmd.duration_ms // 1000
            duration.nanos = (cmd.duration_ms % 1000) * 1_000_000
            bridge_cmd.start_timer.start_to_fire_timeout.CopyFrom(duration)

        elif isinstance(cmd, CompleteWorkflowCommand):
            # Convert CompleteWorkflowCommand to CompleteWorkflowExecution
            # Serialize result to payload
            payload = data_converter.payload_converter.to_payload(cmd.result)
            bridge_cmd.complete_workflow_execution.result.CopyFrom(payload)

        elif isinstance(cmd, FailWorkflowCommand):
            # Convert FailWorkflowCommand to FailWorkflowExecution
            # For now, just create a simple failure message
            # In a full implementation, we'd convert the exception properly
            bridge_cmd.fail_workflow_execution.failure.message = str(cmd.exception)
            bridge_cmd.fail_workflow_execution.failure.stack_trace = ""
            if hasattr(cmd.exception, "__traceback__") and cmd.exception.__traceback__:
                import traceback

                bridge_cmd.fail_workflow_execution.failure.stack_trace = "".join(
                    traceback.format_tb(cmd.exception.__traceback__)
                )

        elif isinstance(cmd, CancelWorkflowCommand):
            # Convert CancelWorkflowCommand to CancelWorkflowExecution
            bridge_cmd.cancel_workflow_execution.SetInParent()

        elif isinstance(cmd, ScheduleActivityCommand):
            # Convert ScheduleActivityCommand to ScheduleActivity
            bridge_cmd.schedule_activity.seq = cmd.seq
            bridge_cmd.schedule_activity.activity_id = cmd.activity_id
            bridge_cmd.schedule_activity.activity_type = cmd.activity_type
            if cmd.task_queue:
                bridge_cmd.schedule_activity.task_queue = cmd.task_queue

            # Encode arguments
            for arg in cmd.args:  # type: ignore[union-attr]
                payload = data_converter.payload_converter.to_payload(arg)
                bridge_cmd.schedule_activity.arguments.append(payload)

            # Convert timeouts to Duration protobufs
            if cmd.schedule_to_close_timeout:
                _set_duration(
                    bridge_cmd.schedule_activity.schedule_to_close_timeout,
                    cmd.schedule_to_close_timeout,
                )
            if cmd.schedule_to_start_timeout:
                _set_duration(
                    bridge_cmd.schedule_activity.schedule_to_start_timeout,
                    cmd.schedule_to_start_timeout,
                )
            if cmd.start_to_close_timeout:
                _set_duration(
                    bridge_cmd.schedule_activity.start_to_close_timeout,
                    cmd.start_to_close_timeout,
                )
            if cmd.heartbeat_timeout:
                _set_duration(
                    bridge_cmd.schedule_activity.heartbeat_timeout,
                    cmd.heartbeat_timeout,
                )

            # Convert retry policy if present
            if cmd.retry_policy:
                if cmd.retry_policy.initial_interval:
                    _set_duration(
                        bridge_cmd.schedule_activity.retry_policy.initial_interval,
                        cmd.retry_policy.initial_interval,
                    )
                if cmd.retry_policy.maximum_interval:
                    _set_duration(
                        bridge_cmd.schedule_activity.retry_policy.maximum_interval,
                        cmd.retry_policy.maximum_interval,
                    )
                if cmd.retry_policy.backoff_coefficient:
                    bridge_cmd.schedule_activity.retry_policy.backoff_coefficient = (
                        cmd.retry_policy.backoff_coefficient
                    )
                if cmd.retry_policy.maximum_attempts:
                    bridge_cmd.schedule_activity.retry_policy.maximum_attempts = (
                        cmd.retry_policy.maximum_attempts
                    )
                for exc_type in cmd.retry_policy.non_retryable_error_types or []:  # type: ignore[union-attr]
                    bridge_cmd.schedule_activity.retry_policy.non_retryable_error_types.append(
                        exc_type
                    )

        elif isinstance(cmd, RequestCancelActivityCommand):
            # Convert RequestCancelActivityCommand to RequestCancelActivity
            bridge_cmd.request_cancel_activity.seq = cmd.seq

        elif isinstance(cmd, QueryResultCommand):
            # Convert QueryResultCommand to RespondToQuery
            bridge_cmd.respond_to_query.query_id = cmd.query_id
            if cmd.error:
                bridge_cmd.respond_to_query.failed.message = cmd.error
            else:
                payload = data_converter.payload_converter.to_payload(cmd.result)
                bridge_cmd.respond_to_query.succeeded.response.CopyFrom(payload)

        elif isinstance(cmd, StartChildWorkflowCommand):
            # Convert StartChildWorkflowCommand to StartChildWorkflowExecution
            bridge_cmd.start_child_workflow_execution.seq = cmd.seq
            bridge_cmd.start_child_workflow_execution.workflow_id = cmd.workflow_id
            bridge_cmd.start_child_workflow_execution.workflow_type = cmd.workflow_type
            if cmd.task_queue:
                bridge_cmd.start_child_workflow_execution.task_queue = cmd.task_queue

            # Encode arguments
            for arg in cmd.args:
                payload = data_converter.payload_converter.to_payload(arg)
                bridge_cmd.start_child_workflow_execution.input.append(payload)

            # Convert timeouts to Duration protobufs
            if cmd.execution_timeout:
                _set_duration(
                    bridge_cmd.start_child_workflow_execution.workflow_execution_timeout,
                    cmd.execution_timeout,
                )
            if cmd.run_timeout:
                _set_duration(
                    bridge_cmd.start_child_workflow_execution.workflow_run_timeout,
                    cmd.run_timeout,
                )
            if cmd.task_timeout:
                _set_duration(
                    bridge_cmd.start_child_workflow_execution.workflow_task_timeout,
                    cmd.task_timeout,
                )

            # Set policies
            bridge_cmd.start_child_workflow_execution.parent_close_policy = (
                cmd.parent_close_policy
            )
            bridge_cmd.start_child_workflow_execution.cancellation_type = (
                cmd.cancellation_type
            )
            bridge_cmd.start_child_workflow_execution.workflow_id_reuse_policy = (
                cmd.id_reuse_policy
            )

            # Convert retry policy if present
            if cmd.retry_policy:
                if cmd.retry_policy.initial_interval:
                    _set_duration(
                        bridge_cmd.start_child_workflow_execution.retry_policy.initial_interval,
                        cmd.retry_policy.initial_interval,
                    )
                if cmd.retry_policy.maximum_interval:
                    _set_duration(
                        bridge_cmd.start_child_workflow_execution.retry_policy.maximum_interval,
                        cmd.retry_policy.maximum_interval,
                    )
                if cmd.retry_policy.backoff_coefficient:
                    bridge_cmd.start_child_workflow_execution.retry_policy.backoff_coefficient = cmd.retry_policy.backoff_coefficient
                if cmd.retry_policy.maximum_attempts:
                    bridge_cmd.start_child_workflow_execution.retry_policy.maximum_attempts = cmd.retry_policy.maximum_attempts
                for exc_type in cmd.retry_policy.non_retryable_error_types or []:
                    bridge_cmd.start_child_workflow_execution.retry_policy.non_retryable_error_types.append(
                        exc_type
                    )

        elif isinstance(cmd, CancelChildWorkflowCommand):
            # Convert CancelChildWorkflowCommand to CancelChildWorkflowExecution
            bridge_cmd.cancel_child_workflow_execution.child_workflow_seq = cmd.seq

        else:
            raise NotImplementedError(
                f"Command type {type(cmd).__name__} not yet supported"
            )

        bridge_comp.successful.commands.append(bridge_cmd)

    return bridge_comp
