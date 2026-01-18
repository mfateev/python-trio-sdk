"""Type converters between bridge protobuf types and POC activation types.

This module provides functions to convert between:
- Bridge WorkflowActivation (protobuf) → POC WorkflowActivation (dataclass)
- POC WorkflowActivationCompletion (dataclass) → Bridge WorkflowActivationCompletion (protobuf)

These converters allow the POC runtime to work with the Temporal bridge without
needing to know about protobuf details.
"""

from __future__ import annotations

import google.protobuf.duration_pb2
import temporalio.api.common.v1
import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb
import temporalio.bridge.proto.workflow_commands.workflow_commands_pb2 as cmd_pb
import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as comp_pb
import temporalio.converter

from temporalio_trio.worker._activation import (
    CancelWorkflowCommand,
    CancelWorkflowJob,
    CompleteWorkflowCommand,
    FailWorkflowCommand,
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
    poc_jobs: list[WorkflowStartedJob | TimerFiredJob | CancelWorkflowJob] = []
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
        elif job_type == "remove_from_cache":
            # Eviction jobs are handled separately in the bridge worker
            # They should not be passed to the workflow instance
            continue
        else:
            # Phase 1: Only support initialize_workflow, fire_timer, cancel_workflow
            raise NotImplementedError(
                f"Job type '{job_type}' not yet supported in Phase 1. "
                f"This will be added in Phase 2."
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

        else:
            raise NotImplementedError(
                f"Command type {type(cmd).__name__} not yet supported"
            )

        bridge_comp.successful.commands.append(bridge_cmd)

    return bridge_comp
