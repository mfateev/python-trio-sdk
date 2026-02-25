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
import temporalio.api.sdk.v1.user_metadata_pb2 as user_metadata_pb
import temporalio.bridge.proto.activity_result.activity_result_pb2 as act_result_pb
import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb
import temporalio.bridge.proto.workflow_commands.workflow_commands_pb2 as cmd_pb
import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as comp_pb
import temporalio.common
import temporalio.converter

from temporalio_trio.worker._activation import (
    ActivityResolvedJob,
    CancelChildWorkflowCommand,
    CancelExternalResolvedJob,
    CancelTimerCommand,
    CancelWorkflowCommand,
    CancelWorkflowJob,
    ChildWorkflowResolvedJob,
    ChildWorkflowStartedJob,
    ChildWorkflowStartFailedJob,
    CompleteWorkflowCommand,
    ContinueAsNewCommand,
    FailWorkflowCommand,
    NotifyHasPatchJob,
    QueryResultCommand,
    QueryWorkflowJob,
    RequestCancelActivityCommand,
    RequestCancelExternalWorkflowCommand,
    ScheduleActivityCommand,
    ScheduleLocalActivityCommand,
    SetPatchMarkerCommand,
    SignalExternalResolvedJob,
    SignalExternalWorkflowCommand,
    SignalWorkflowJob,
    StartChildWorkflowCommand,
    StartTimerCommand,
    TimerFiredJob,
    UpdateResponseCommand,
    UpdateWorkflowJob,
    UpsertSearchAttributesCommand,
    WorkflowActivation,
    WorkflowActivationCompletion,
    WorkflowStartedJob,
)
from temporalio_trio.worker._failure_converter import failure_to_exception
from temporalio_trio.worker._runtime import (
    QueryFailureCommand,
    QuerySuccessCommand,
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
        | SignalExternalResolvedJob
        | NotifyHasPatchJob
    ] = []
    is_eviction = False
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
        elif job_type == "resolve_signal_external_workflow":
            poc_jobs.append(
                _convert_resolve_signal_external_workflow(
                    job.resolve_signal_external_workflow, data_converter
                )
            )
        elif job_type == "resolve_request_cancel_external_workflow":
            poc_jobs.append(
                _convert_resolve_cancel_external_workflow(
                    job.resolve_request_cancel_external_workflow, data_converter
                )
            )
        elif job_type == "do_update":
            poc_jobs.append(
                _convert_do_update(job.do_update, data_converter)
            )
        elif job_type == "notify_has_patch":
            poc_jobs.append(_convert_notify_has_patch(job.notify_has_patch))
        elif job_type == "remove_from_cache":
            # Track eviction - this activation is a cache eviction request
            is_eviction = True
            continue
        else:
            # Unsupported job types - raise error with helpful message
            raise NotImplementedError(
                f"Job type '{job_type}' not yet supported. "
                f"Please file an issue if this is needed."
            )

    # Get randomness_seed safely - it may not be present on all activations
    randomness_seed = getattr(bridge_act, "randomness_seed", None)
    if randomness_seed == 0:
        randomness_seed = None  # 0 is the protobuf default for unset

    return WorkflowActivation(
        jobs=poc_jobs,
        timestamp_ns=timestamp_ns,
        run_id=bridge_act.run_id,
        remove_from_cache=is_eviction,
        is_replaying=bridge_act.is_replaying,
        randomness_seed=randomness_seed,
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
    args: tuple = ()
    if init.arguments:
        # Convert each payload to Python value
        values = []
        for payload in init.arguments:
            # Use the data converter to decode the payload
            value = data_converter.payload_converter.from_payload(payload)
            values.append(value)
        args = tuple(values)

    # Extract timeouts (Duration -> milliseconds)
    # These are message fields, so HasField is valid in proto3
    execution_timeout_ms = None
    if init.HasField("workflow_execution_timeout"):
        d = init.workflow_execution_timeout
        execution_timeout_ms = int(d.seconds * 1000 + d.nanos / 1_000_000)

    run_timeout_ms = None
    if init.HasField("workflow_run_timeout"):
        d = init.workflow_run_timeout
        run_timeout_ms = int(d.seconds * 1000 + d.nanos / 1_000_000)

    task_timeout_ms = None
    if init.HasField("workflow_task_timeout"):
        d = init.workflow_task_timeout
        task_timeout_ms = int(d.seconds * 1000 + d.nanos / 1_000_000)

    # Extract retry policy (message field)
    retry_policy = None
    if init.HasField("retry_policy"):
        retry_policy = temporalio.common.RetryPolicy.from_proto(init.retry_policy)

    # Extract start_time (Timestamp -> nanoseconds, message field)
    start_time_ns = 0
    if init.HasField("start_time"):
        start_time_ns = init.start_time.seconds * 1_000_000_000 + init.start_time.nanos

    # Extract parent info (message field)
    parent_namespace = None
    parent_workflow_id = None
    parent_run_id = None
    if init.HasField("parent_workflow_info"):
        p = init.parent_workflow_info
        parent_namespace = p.namespace
        parent_workflow_id = p.workflow_id
        parent_run_id = p.run_id

    # Extract root workflow (message field)
    root_workflow_id = None
    root_run_id = None
    if init.HasField("root_workflow"):
        r = init.root_workflow
        root_workflow_id = r.workflow_id
        root_run_id = r.run_id

    # Extract memo (message field)
    raw_memo: dict[str, temporalio.api.common.v1.Payload] = {}
    if init.HasField("memo"):
        raw_memo = dict(init.memo.fields)

    # Extract priority (message field)
    priority = temporalio.common.Priority.default
    if init.HasField("priority"):
        priority = temporalio.common.Priority._from_proto(init.priority)

    return WorkflowStartedJob(
        workflow_type=init.workflow_type,
        workflow_id=init.workflow_id,
        args=args,
        headers=dict(init.headers),
        namespace="default",  # namespace not in InitializeWorkflow, plumbed from worker later
        attempt=init.attempt or 1,
        start_time_ns=start_time_ns,
        execution_timeout_ms=execution_timeout_ms,
        run_timeout_ms=run_timeout_ms,
        task_timeout_ms=task_timeout_ms,
        retry_policy=retry_policy,
        continued_run_id=init.continued_from_execution_run_id or None,
        cron_schedule=init.cron_schedule or None,
        parent_namespace=parent_namespace,
        parent_workflow_id=parent_workflow_id,
        parent_run_id=parent_run_id,
        root_workflow_id=root_workflow_id,
        root_run_id=root_run_id,
        raw_memo=raw_memo,
        priority=priority,
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
        headers=dict(signal.headers),
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
        headers=dict(query.headers),
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
        POC ActivityResolvedJob with result or failure.
        Failures are converted to proper exception types (ActivityError, etc.)
        using the failure converter.
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
        # Activity failed - convert to proper exception type
        # The failure converter produces ActivityError with __cause__ set to
        # the underlying exception (e.g., ApplicationError)
        failure = failure_to_exception(
            resolution.failed.failure,
            data_converter.payload_converter,
        )
    elif status == "cancelled":
        # Activity was cancelled - convert to proper exception type
        failure = failure_to_exception(
            resolution.cancelled.failure,
            data_converter.payload_converter,
        )
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
        POC ChildWorkflowResolvedJob with result or failure.
        Failures are converted to proper exception types (ChildWorkflowError, etc.)
        using the failure converter.
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
        # Child workflow failed - convert to proper exception type
        # The failure converter produces ChildWorkflowError with __cause__ set
        # to the underlying exception (e.g., ApplicationError)
        failure = failure_to_exception(
            child_result.failed.failure,
            data_converter.payload_converter,
        )
    elif status == "cancelled":
        # Child workflow was cancelled - convert to proper exception type
        failure = failure_to_exception(
            child_result.cancelled.failure,
            data_converter.payload_converter,
        )
    else:
        failure = RuntimeError(f"Unknown child workflow result status: {status}")

    return ChildWorkflowResolvedJob(
        seq=seq,
        result=result,
        failure=failure,
    )


def _convert_resolve_signal_external_workflow(
    resolve: act_pb.ResolveSignalExternalWorkflow,
    data_converter: temporalio.converter.DataConverter,
) -> SignalExternalResolvedJob:
    """Convert ResolveSignalExternalWorkflow to SignalExternalResolvedJob.

    Args:
        resolve: Bridge ResolveSignalExternalWorkflow job
        data_converter: Data converter for deserializing failure details

    Returns:
        POC SignalExternalResolvedJob with failure (if any).
        Failures are converted to proper exception types using the failure converter.

    Note:
        Success is indicated by ABSENCE of failure field.
        Check: if resolve.failure.ByteSize() > 0 then failed, else succeeded.
    """
    seq = resolve.seq
    failure = None

    # Check if failure is present - success is indicated by absence of failure
    if resolve.failure.ByteSize() > 0:
        failure = failure_to_exception(
            resolve.failure,
            data_converter.payload_converter,
        )

    return SignalExternalResolvedJob(
        seq=seq,
        failure=failure,
    )


def _convert_resolve_cancel_external_workflow(
    resolve: act_pb.ResolveRequestCancelExternalWorkflow,
    data_converter: temporalio.converter.DataConverter,
) -> CancelExternalResolvedJob:
    """Convert ResolveRequestCancelExternalWorkflow to CancelExternalResolvedJob.

    Args:
        resolve: Bridge ResolveRequestCancelExternalWorkflow job
        data_converter: Data converter for deserializing failure details

    Returns:
        POC CancelExternalResolvedJob with failure (if any).
        Failures are converted to proper exception types using the failure converter.

    Note:
        Success is indicated by ABSENCE of failure field.
        Check: if resolve.failure.ByteSize() > 0 then failed, else succeeded.
    """
    seq = resolve.seq
    failure = None

    # Check if failure is present - success is indicated by absence of failure
    if resolve.failure.ByteSize() > 0:
        failure = failure_to_exception(
            resolve.failure,
            data_converter.payload_converter,
        )

    return CancelExternalResolvedJob(
        seq=seq,
        failure=failure,
    )


def _convert_notify_has_patch(notify: act_pb.NotifyHasPatch) -> NotifyHasPatchJob:
    """Convert NotifyHasPatch to NotifyHasPatchJob.

    Args:
        notify: Bridge NotifyHasPatch job

    Returns:
        POC NotifyHasPatchJob
    """
    return NotifyHasPatchJob(patch_id=notify.patch_id)


def _convert_do_update(
    update: act_pb.DoUpdate,
    data_converter: temporalio.converter.DataConverter,
) -> UpdateWorkflowJob:
    """Convert bridge DoUpdate to POC UpdateWorkflowJob.

    Args:
        update: Bridge DoUpdate job
        data_converter: Data converter for deserializing arguments

    Returns:
        POC UpdateWorkflowJob
    """
    args = tuple(
        data_converter.payload_converter.from_payload(p) for p in update.input
    )
    return UpdateWorkflowJob(
        id=update.id,
        protocol_instance_id=update.protocol_instance_id,
        name=update.name,
        args=args,
        run_validator=update.run_validator,
        headers=dict(update.headers),
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
            # Add summary as user_metadata if provided
            if cmd.summary:
                summary_payload = data_converter.payload_converter.to_payload(
                    cmd.summary
                )
                bridge_cmd.user_metadata.CopyFrom(
                    user_metadata_pb.UserMetadata(summary=summary_payload)
                )

        elif isinstance(cmd, CancelTimerCommand):
            # Convert CancelTimerCommand to CancelTimer
            bridge_cmd.cancel_timer.seq = cmd.timer_id

        elif isinstance(cmd, CompleteWorkflowCommand):
            # Convert CompleteWorkflowCommand to CompleteWorkflowExecution
            # Serialize result to payload
            payload = data_converter.payload_converter.to_payload(cmd.result)
            bridge_cmd.complete_workflow_execution.result.CopyFrom(payload)

        elif isinstance(cmd, FailWorkflowCommand):
            # Convert FailWorkflowCommand to FailWorkflowExecution
            # Use the SDK's failure converter to properly set application_failure_info
            # for ApplicationError, etc. to_failure modifies the failure object in place.
            failure_converter = temporalio.converter.DefaultFailureConverter()
            failure = temporalio.api.failure.v1.Failure()
            failure_converter.to_failure(
                cmd.exception,
                data_converter.payload_converter,
                failure,
            )
            bridge_cmd.fail_workflow_execution.failure.CopyFrom(failure)

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

            # Set cancellation type
            bridge_cmd.schedule_activity.cancellation_type = cmd.cancellation_type

            # Apply headers
            temporalio.common._apply_headers(
                cmd.headers, bridge_cmd.schedule_activity.headers
            )

        elif isinstance(cmd, ScheduleLocalActivityCommand):
            # Convert ScheduleLocalActivityCommand to ScheduleLocalActivity
            bridge_cmd.schedule_local_activity.seq = cmd.seq
            bridge_cmd.schedule_local_activity.activity_id = cmd.activity_id
            bridge_cmd.schedule_local_activity.activity_type = cmd.activity_type

            # Encode arguments
            for arg in cmd.args:  # type: ignore[union-attr]
                payload = data_converter.payload_converter.to_payload(arg)
                bridge_cmd.schedule_local_activity.arguments.append(payload)

            # Convert timeouts to Duration protobufs
            if cmd.schedule_to_close_timeout:
                _set_duration(
                    bridge_cmd.schedule_local_activity.schedule_to_close_timeout,
                    cmd.schedule_to_close_timeout,
                )
            if cmd.schedule_to_start_timeout:
                _set_duration(
                    bridge_cmd.schedule_local_activity.schedule_to_start_timeout,
                    cmd.schedule_to_start_timeout,
                )
            if cmd.start_to_close_timeout:
                _set_duration(
                    bridge_cmd.schedule_local_activity.start_to_close_timeout,
                    cmd.start_to_close_timeout,
                )

            # Convert local retry threshold
            if cmd.local_retry_threshold:
                _set_duration(
                    bridge_cmd.schedule_local_activity.local_retry_threshold,
                    cmd.local_retry_threshold,
                )

            # Convert retry policy if present
            if cmd.retry_policy:
                if cmd.retry_policy.initial_interval:
                    _set_duration(
                        bridge_cmd.schedule_local_activity.retry_policy.initial_interval,
                        cmd.retry_policy.initial_interval,
                    )
                if cmd.retry_policy.maximum_interval:
                    _set_duration(
                        bridge_cmd.schedule_local_activity.retry_policy.maximum_interval,
                        cmd.retry_policy.maximum_interval,
                    )
                if cmd.retry_policy.backoff_coefficient:
                    bridge_cmd.schedule_local_activity.retry_policy.backoff_coefficient = (
                        cmd.retry_policy.backoff_coefficient
                    )
                if cmd.retry_policy.maximum_attempts:
                    bridge_cmd.schedule_local_activity.retry_policy.maximum_attempts = (
                        cmd.retry_policy.maximum_attempts
                    )
                for exc_type in cmd.retry_policy.non_retryable_error_types or []:  # type: ignore[union-attr]
                    bridge_cmd.schedule_local_activity.retry_policy.non_retryable_error_types.append(
                        exc_type
                    )

            # Set cancellation type
            bridge_cmd.schedule_local_activity.cancellation_type = cmd.cancellation_type

            # Apply headers
            temporalio.common._apply_headers(
                cmd.headers, bridge_cmd.schedule_local_activity.headers
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

        elif isinstance(cmd, QuerySuccessCommand):
            # Convert QuerySuccessCommand to RespondToQuery with success
            bridge_cmd.respond_to_query.query_id = cmd.query_id
            payload = data_converter.payload_converter.to_payload(cmd.result)
            bridge_cmd.respond_to_query.succeeded.response.CopyFrom(payload)

        elif isinstance(cmd, QueryFailureCommand):
            # Convert QueryFailureCommand to RespondToQuery with failure
            bridge_cmd.respond_to_query.query_id = cmd.query_id
            error_msg = str(cmd.error) if cmd.error else "Query handler failed"
            bridge_cmd.respond_to_query.failed.message = error_msg

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

            # Set policies (int values work for protobuf enums at runtime)
            bridge_cmd.start_child_workflow_execution.parent_close_policy = (
                cmd.parent_close_policy  # type: ignore[assignment]
            )
            bridge_cmd.start_child_workflow_execution.cancellation_type = (
                cmd.cancellation_type  # type: ignore[assignment]
            )
            bridge_cmd.start_child_workflow_execution.workflow_id_reuse_policy = (
                cmd.id_reuse_policy  # type: ignore[assignment]
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

            # Set cron schedule
            if cmd.cron_schedule:
                bridge_cmd.start_child_workflow_execution.cron_schedule = (
                    cmd.cron_schedule
                )

            # Encode memo
            if cmd.memo:
                for k, val in cmd.memo.items():
                    bridge_cmd.start_child_workflow_execution.memo[k].CopyFrom(
                        data_converter.payload_converter.to_payloads([val])[0]
                    )

            # Encode search attributes
            if cmd.search_attributes:
                _encode_search_attributes(
                    cmd.search_attributes,
                    bridge_cmd.start_child_workflow_execution.search_attributes,
                )

            # Apply headers
            temporalio.common._apply_headers(
                cmd.headers, bridge_cmd.start_child_workflow_execution.headers
            )

        elif isinstance(cmd, CancelChildWorkflowCommand):
            # Convert CancelChildWorkflowCommand to CancelChildWorkflowExecution
            bridge_cmd.cancel_child_workflow_execution.child_workflow_seq = cmd.seq

        elif isinstance(cmd, SignalExternalWorkflowCommand):
            # Convert SignalExternalWorkflowCommand to SignalExternalWorkflowExecution
            bridge_cmd.signal_external_workflow_execution.seq = cmd.seq
            bridge_cmd.signal_external_workflow_execution.workflow_execution.workflow_id = cmd.workflow_id
            # Set run_id only if provided (empty = signal current run)
            if cmd.run_id:
                bridge_cmd.signal_external_workflow_execution.workflow_execution.run_id = cmd.run_id
            bridge_cmd.signal_external_workflow_execution.signal_name = cmd.signal_name

            # Encode signal arguments
            for arg in cmd.args:
                payload = data_converter.payload_converter.to_payload(arg)
                bridge_cmd.signal_external_workflow_execution.args.append(payload)

            # Apply headers
            temporalio.common._apply_headers(
                cmd.headers,
                bridge_cmd.signal_external_workflow_execution.headers,
            )

        elif isinstance(cmd, RequestCancelExternalWorkflowCommand):
            # Convert RequestCancelExternalWorkflowCommand to RequestCancelExternalWorkflowExecution
            bridge_cmd.request_cancel_external_workflow_execution.seq = cmd.seq
            bridge_cmd.request_cancel_external_workflow_execution.workflow_execution.workflow_id = cmd.workflow_id
            # Set run_id only if provided (empty = cancel current run)
            if cmd.run_id:
                bridge_cmd.request_cancel_external_workflow_execution.workflow_execution.run_id = cmd.run_id

        elif isinstance(cmd, UpsertSearchAttributesCommand):
            # Convert UpsertSearchAttributesCommand to UpsertWorkflowSearchAttributes
            # Uses encode_typed_search_attribute_value to produce payloads with
            # correct type metadata (matching sdk-python's typed path)
            for update in cmd.search_attributes:
                bridge_cmd.upsert_workflow_search_attributes.search_attributes[
                    update.key.name
                ].CopyFrom(
                    temporalio.converter.encode_typed_search_attribute_value(
                        update.key, update.value
                    )
                )

        elif isinstance(cmd, ContinueAsNewCommand):
            # Convert ContinueAsNewCommand to ContinueAsNewWorkflowExecution
            bridge_cmd.continue_as_new_workflow_execution.workflow_type = (
                cmd.workflow_type
            )

            # Set task queue if provided
            if cmd.task_queue:
                bridge_cmd.continue_as_new_workflow_execution.task_queue = (
                    cmd.task_queue
                )

            # Encode arguments
            for arg in cmd.args:
                payload = data_converter.payload_converter.to_payload(arg)
                bridge_cmd.continue_as_new_workflow_execution.arguments.append(payload)

            # Convert timeouts to Duration protobufs
            if cmd.run_timeout:
                _set_duration(
                    bridge_cmd.continue_as_new_workflow_execution.workflow_run_timeout,
                    cmd.run_timeout,
                )
            if cmd.task_timeout:
                _set_duration(
                    bridge_cmd.continue_as_new_workflow_execution.workflow_task_timeout,
                    cmd.task_timeout,
                )

            # Convert retry policy if present
            if cmd.retry_policy:
                if cmd.retry_policy.initial_interval:
                    _set_duration(
                        bridge_cmd.continue_as_new_workflow_execution.retry_policy.initial_interval,
                        cmd.retry_policy.initial_interval,
                    )
                if cmd.retry_policy.maximum_interval:
                    _set_duration(
                        bridge_cmd.continue_as_new_workflow_execution.retry_policy.maximum_interval,
                        cmd.retry_policy.maximum_interval,
                    )
                if cmd.retry_policy.backoff_coefficient:
                    bridge_cmd.continue_as_new_workflow_execution.retry_policy.backoff_coefficient = cmd.retry_policy.backoff_coefficient
                if cmd.retry_policy.maximum_attempts:
                    bridge_cmd.continue_as_new_workflow_execution.retry_policy.maximum_attempts = cmd.retry_policy.maximum_attempts
                for exc_type in cmd.retry_policy.non_retryable_error_types or []:
                    bridge_cmd.continue_as_new_workflow_execution.retry_policy.non_retryable_error_types.append(
                        exc_type
                    )

            # Encode memo
            if cmd.memo:
                for k, val in cmd.memo.items():
                    bridge_cmd.continue_as_new_workflow_execution.memo[k].CopyFrom(
                        data_converter.payload_converter.to_payloads([val])[0]
                    )

            # Encode search attributes
            if cmd.search_attributes:
                _encode_search_attributes(
                    cmd.search_attributes,
                    bridge_cmd.continue_as_new_workflow_execution.search_attributes,
                )

            # Apply headers
            temporalio.common._apply_headers(
                cmd.headers,
                bridge_cmd.continue_as_new_workflow_execution.headers,
            )

        elif isinstance(cmd, SetPatchMarkerCommand):
            # Convert SetPatchMarkerCommand to SetPatchMarker
            bridge_cmd.set_patch_marker.patch_id = cmd.patch_id
            bridge_cmd.set_patch_marker.deprecated = cmd.deprecated

        elif isinstance(cmd, UpdateResponseCommand):
            # Convert UpdateResponseCommand to UpdateResponse
            bridge_cmd.update_response.protocol_instance_id = (
                cmd.protocol_instance_id
            )
            if cmd.accepted:
                bridge_cmd.update_response.accepted.SetInParent()
            elif cmd.rejected_failure is not None:
                failure_converter = temporalio.converter.DefaultFailureConverter()
                failure = temporalio.api.failure.v1.Failure()
                failure_converter.to_failure(
                    cmd.rejected_failure,
                    data_converter.payload_converter,
                    failure,
                )
                bridge_cmd.update_response.rejected.CopyFrom(failure)
            elif cmd._is_completed:
                payload = data_converter.payload_converter.to_payload(
                    cmd.completed_result
                )
                bridge_cmd.update_response.completed.CopyFrom(payload)

        else:
            raise NotImplementedError(
                f"Command type {type(cmd).__name__} not yet supported"
            )

        bridge_comp.successful.commands.append(bridge_cmd)

    return bridge_comp


def _encode_search_attributes(
    attributes: temporalio.common.SearchAttributes
    | temporalio.common.TypedSearchAttributes,
    payloads: Any,
) -> None:
    """Encode search attributes into protobuf payloads map.

    Follows the same pattern as sdk-python's _encode_search_attributes.

    Args:
        attributes: Search attributes (typed or untyped).
        payloads: Protobuf map to write encoded payloads into.
    """
    if isinstance(attributes, temporalio.common.TypedSearchAttributes):
        for pair in attributes:
            payloads[pair.key.name].CopyFrom(
                temporalio.converter.encode_typed_search_attribute_value(
                    pair.key, pair.value
                )
            )
    else:
        for k, vals in attributes.items():
            payloads[k].CopyFrom(
                temporalio.converter.encode_search_attribute_values(vals)
            )
