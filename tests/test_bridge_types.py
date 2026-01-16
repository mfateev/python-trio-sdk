"""Tests for bridge type conversion.

Tests the conversion between bridge protobuf types and POC activation types.
"""

import google.protobuf.duration_pb2
import google.protobuf.timestamp_pb2
import pytest
import temporalio.api.common.v1
import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb
import temporalio.bridge.proto.workflow_commands.workflow_commands_pb2 as cmd_pb
import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as comp_pb
import temporalio.converter

from temporalio_trio.worker._activation import (
    CompleteWorkflowCommand,
    FailWorkflowCommand,
    StartTimerCommand,
    TimerFiredJob,
    WorkflowActivationCompletion,
    WorkflowStartedJob,
)
from temporalio_trio.worker._bridge_types import (
    bridge_to_poc_activation,
    poc_to_bridge_completion,
)


def test_convert_initialize_workflow():
    """Test converting InitializeWorkflow to WorkflowStartedJob."""
    # Create bridge activation with initialize_workflow job
    bridge_act = act_pb.WorkflowActivation()
    bridge_act.run_id = "test-run-id"
    bridge_act.timestamp.seconds = 1000
    bridge_act.timestamp.nanos = 500_000_000

    job = bridge_act.jobs.add()
    job.initialize_workflow.workflow_type = "TestWorkflow"
    job.initialize_workflow.workflow_id = "test-workflow-id"

    # Add an argument
    arg_payload = temporalio.api.common.v1.Payload()
    arg_payload.data = b'"test-arg"'
    arg_payload.metadata["encoding"] = b"json/plain"
    job.initialize_workflow.arguments.append(arg_payload)

    # Convert to POC activation
    data_converter = temporalio.converter.DataConverter()
    poc_act = bridge_to_poc_activation(bridge_act, data_converter)

    # Verify conversion
    assert poc_act.timestamp_ns == 1_000_500_000_000
    assert len(poc_act.jobs) == 1
    assert isinstance(poc_act.jobs[0], WorkflowStartedJob)
    assert poc_act.jobs[0].workflow_type == "TestWorkflow"
    assert len(poc_act.jobs[0].args) == 1
    assert poc_act.jobs[0].args[0] == "test-arg"


def test_convert_fire_timer():
    """Test converting FireTimer to TimerFiredJob."""
    # Create bridge activation with fire_timer job
    bridge_act = act_pb.WorkflowActivation()
    bridge_act.run_id = "test-run-id"
    bridge_act.timestamp.seconds = 2000
    bridge_act.timestamp.nanos = 0

    job = bridge_act.jobs.add()
    job.fire_timer.seq = 42

    # Convert to POC activation
    data_converter = temporalio.converter.DataConverter()
    poc_act = bridge_to_poc_activation(bridge_act, data_converter)

    # Verify conversion
    assert poc_act.timestamp_ns == 2_000_000_000_000
    assert len(poc_act.jobs) == 1
    assert isinstance(poc_act.jobs[0], TimerFiredJob)
    assert poc_act.jobs[0].timer_id == 42


def test_convert_remove_from_cache():
    """Test that remove_from_cache jobs are filtered out."""
    # Create bridge activation with remove_from_cache job
    bridge_act = act_pb.WorkflowActivation()
    bridge_act.run_id = "test-run-id"
    bridge_act.timestamp.seconds = 1000
    bridge_act.timestamp.nanos = 0

    job = bridge_act.jobs.add()
    job.remove_from_cache.SetInParent()

    # Convert to POC activation
    data_converter = temporalio.converter.DataConverter()
    poc_act = bridge_to_poc_activation(bridge_act, data_converter)

    # Verify that the job was filtered out
    assert len(poc_act.jobs) == 0


def test_convert_unsupported_job_type():
    """Test that unsupported job types raise NotImplementedError."""
    # Create bridge activation with signal_workflow job (not supported in Phase 1)
    bridge_act = act_pb.WorkflowActivation()
    bridge_act.run_id = "test-run-id"
    bridge_act.timestamp.seconds = 1000
    bridge_act.timestamp.nanos = 0

    job = bridge_act.jobs.add()
    job.signal_workflow.signal_name = "test-signal"

    # Try to convert - should raise NotImplementedError
    data_converter = temporalio.converter.DataConverter()
    with pytest.raises(NotImplementedError, match="signal_workflow.*not yet supported"):
        bridge_to_poc_activation(bridge_act, data_converter)


def test_convert_start_timer_command():
    """Test converting StartTimerCommand to bridge command."""
    # Create POC completion with StartTimerCommand
    poc_comp = WorkflowActivationCompletion(
        commands=[
            StartTimerCommand(
                timer_id=5,
                duration_ms=10_000,  # 10 seconds
            )
        ]
    )

    # Convert to bridge completion
    data_converter = temporalio.converter.DataConverter()
    bridge_comp = poc_to_bridge_completion("test-run-id", poc_comp, data_converter)

    # Verify conversion
    assert bridge_comp.run_id == "test-run-id"
    assert bridge_comp.HasField("successful")
    assert len(bridge_comp.successful.commands) == 1

    cmd = bridge_comp.successful.commands[0]
    assert cmd.HasField("start_timer")
    assert cmd.start_timer.seq == 5
    assert cmd.start_timer.start_to_fire_timeout.seconds == 10
    assert cmd.start_timer.start_to_fire_timeout.nanos == 0


def test_convert_complete_workflow_command():
    """Test converting CompleteWorkflowCommand to bridge command."""
    # Create POC completion with CompleteWorkflowCommand
    poc_comp = WorkflowActivationCompletion(
        commands=[CompleteWorkflowCommand(result="workflow-result")]
    )

    # Convert to bridge completion
    data_converter = temporalio.converter.DataConverter()
    bridge_comp = poc_to_bridge_completion("test-run-id", poc_comp, data_converter)

    # Verify conversion
    assert bridge_comp.run_id == "test-run-id"
    assert bridge_comp.HasField("successful")
    assert len(bridge_comp.successful.commands) == 1

    cmd = bridge_comp.successful.commands[0]
    assert cmd.HasField("complete_workflow_execution")
    # Deserialize the result
    result = data_converter.payload_converter.from_payload(
        cmd.complete_workflow_execution.result
    )
    assert result == "workflow-result"


def test_convert_fail_workflow_command():
    """Test converting FailWorkflowCommand to bridge command."""
    # Create POC completion with FailWorkflowCommand
    exc = ValueError("workflow failed")
    poc_comp = WorkflowActivationCompletion(
        commands=[FailWorkflowCommand(exception=exc)]
    )

    # Convert to bridge completion
    data_converter = temporalio.converter.DataConverter()
    bridge_comp = poc_to_bridge_completion("test-run-id", poc_comp, data_converter)

    # Verify conversion
    assert bridge_comp.run_id == "test-run-id"
    assert bridge_comp.HasField("successful")
    assert len(bridge_comp.successful.commands) == 1

    cmd = bridge_comp.successful.commands[0]
    assert cmd.HasField("fail_workflow_execution")
    assert "workflow failed" in cmd.fail_workflow_execution.failure.message


def test_convert_multiple_commands():
    """Test converting completion with multiple commands."""
    # Create POC completion with multiple commands
    poc_comp = WorkflowActivationCompletion(
        commands=[
            StartTimerCommand(timer_id=1, duration_ms=1000),
            StartTimerCommand(timer_id=2, duration_ms=2000),
        ]
    )

    # Convert to bridge completion
    data_converter = temporalio.converter.DataConverter()
    bridge_comp = poc_to_bridge_completion("test-run-id", poc_comp, data_converter)

    # Verify conversion
    assert bridge_comp.run_id == "test-run-id"
    assert bridge_comp.HasField("successful")
    assert len(bridge_comp.successful.commands) == 2

    # Check first command
    cmd1 = bridge_comp.successful.commands[0]
    assert cmd1.HasField("start_timer")
    assert cmd1.start_timer.seq == 1
    assert cmd1.start_timer.start_to_fire_timeout.seconds == 1

    # Check second command
    cmd2 = bridge_comp.successful.commands[1]
    assert cmd2.HasField("start_timer")
    assert cmd2.start_timer.seq == 2
    assert cmd2.start_timer.start_to_fire_timeout.seconds == 2
