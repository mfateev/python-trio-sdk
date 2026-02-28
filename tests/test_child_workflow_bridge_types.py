"""Tests for child workflow bridge type converters.

These tests verify conversion between POC types and bridge protobuf types
for child workflow commands and jobs.
"""

from __future__ import annotations

from datetime import timedelta

import temporalio.api.common.v1.message_pb2
import temporalio.bridge.proto.child_workflow.child_workflow_pb2 as cw_pb
import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb
import temporalio.bridge.proto.workflow_commands.workflow_commands_pb2 as cmd_pb
import temporalio.common
import temporalio.converter

from temporalio_trio.worker._activation import (
    CancelChildWorkflowCommand,
    ChildWorkflowResolvedJob,
    ChildWorkflowStartedJob,
    ChildWorkflowStartFailedJob,
    StartChildWorkflowCommand,
    WorkflowActivationCompletion,
)
from temporalio_trio.worker._bridge_types import (
    _convert_resolve_child_workflow,
    _convert_resolve_child_workflow_start,
    poc_to_bridge_completion,
)


class TestConvertResolveChildWorkflowStart:
    """Tests for _convert_resolve_child_workflow_start."""

    def test_succeeded(self):
        """Test converting succeeded child workflow start."""
        data_converter = temporalio.converter.DataConverter()

        # Create bridge protobuf
        resolve = act_pb.ResolveChildWorkflowExecutionStart()
        resolve.seq = 1
        resolve.succeeded.run_id = "child-run-123"

        result = _convert_resolve_child_workflow_start(resolve, data_converter)

        assert isinstance(result, ChildWorkflowStartedJob)
        assert result.seq == 1
        assert result.run_id == "child-run-123"

    def test_failed(self):
        """Test converting failed child workflow start."""
        data_converter = temporalio.converter.DataConverter()

        # Create bridge protobuf
        resolve = act_pb.ResolveChildWorkflowExecutionStart()
        resolve.seq = 2
        resolve.failed.workflow_id = "child-wf-1"
        resolve.failed.workflow_type = "ChildWorkflow"
        resolve.failed.cause = (
            cw_pb.START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS
        )

        result = _convert_resolve_child_workflow_start(resolve, data_converter)

        assert isinstance(result, ChildWorkflowStartFailedJob)
        assert result.seq == 2
        assert result.workflow_id == "child-wf-1"
        assert result.workflow_type == "ChildWorkflow"
        assert result.cause == 1  # WORKFLOW_ALREADY_EXISTS enum value

    def test_cancelled(self):
        """Test converting cancelled child workflow start."""
        data_converter = temporalio.converter.DataConverter()

        # Create bridge protobuf
        resolve = act_pb.ResolveChildWorkflowExecutionStart()
        resolve.seq = 3
        resolve.cancelled.failure.message = "Start cancelled by user"

        result = _convert_resolve_child_workflow_start(resolve, data_converter)

        assert isinstance(result, ChildWorkflowStartFailedJob)
        assert result.seq == 3
        assert "CANCELLED" in result.cause
        assert "Start cancelled by user" in result.cause


class TestConvertResolveChildWorkflow:
    """Tests for _convert_resolve_child_workflow."""

    def test_completed(self):
        """Test converting completed child workflow.

        result_payload should be the raw protobuf Payload (not decoded),
        matching sdk-python where decoding happens at resolution time.
        """
        data_converter = temporalio.converter.DataConverter()

        # Create bridge protobuf
        resolve = act_pb.ResolveChildWorkflowExecution()
        resolve.seq = 1
        # Set completed result
        payload = data_converter.payload_converter.to_payload("hello world")
        resolve.result.completed.result.CopyFrom(payload)

        result = _convert_resolve_child_workflow(resolve, data_converter)

        assert isinstance(result, ChildWorkflowResolvedJob)
        assert result.seq == 1
        # result_payload is the raw Payload, not decoded
        assert result.result_payload is not None
        decoded = data_converter.payload_converter.from_payloads([result.result_payload])
        assert decoded[0] == "hello world"
        assert result.failure is None

    def test_completed_complex_result(self):
        """Test converting completed child workflow with complex result."""
        data_converter = temporalio.converter.DataConverter()

        # Create bridge protobuf
        resolve = act_pb.ResolveChildWorkflowExecution()
        resolve.seq = 2
        # Set completed result with dict
        payload = data_converter.payload_converter.to_payload(
            {"key": "value", "count": 42}
        )
        resolve.result.completed.result.CopyFrom(payload)

        result = _convert_resolve_child_workflow(resolve, data_converter)

        assert isinstance(result, ChildWorkflowResolvedJob)
        assert result.seq == 2
        # result_payload is the raw Payload, not decoded
        assert result.result_payload is not None
        decoded = data_converter.payload_converter.from_payloads([result.result_payload])
        assert decoded[0] == {"key": "value", "count": 42}
        assert result.failure is None

    def test_failed(self):
        """Test converting failed child workflow."""
        from temporalio.exceptions import FailureError

        data_converter = temporalio.converter.DataConverter()

        # Create bridge protobuf
        resolve = act_pb.ResolveChildWorkflowExecution()
        resolve.seq = 3
        resolve.result.failed.failure.message = "Child workflow error"

        result = _convert_resolve_child_workflow(resolve, data_converter)

        assert isinstance(result, ChildWorkflowResolvedJob)
        assert result.seq == 3
        assert result.result_payload is None
        assert result.failure is not None
        # Now uses proper FailureError type instead of RuntimeError
        assert isinstance(result.failure, FailureError)
        assert "Child workflow error" in str(result.failure)

    def test_cancelled(self):
        """Test converting cancelled child workflow."""
        data_converter = temporalio.converter.DataConverter()

        # Create bridge protobuf
        resolve = act_pb.ResolveChildWorkflowExecution()
        resolve.seq = 4
        resolve.result.cancelled.failure.message = "Cancelled by parent"

        result = _convert_resolve_child_workflow(resolve, data_converter)

        assert isinstance(result, ChildWorkflowResolvedJob)
        assert result.seq == 4
        assert result.result_payload is None
        assert result.failure is not None
        assert "cancelled" in str(result.failure).lower()
        assert "Cancelled by parent" in str(result.failure)


class TestPocToBridgeCompletionChildWorkflow:
    """Tests for child workflow command conversion in poc_to_bridge_completion."""

    def test_start_child_workflow_minimal(self):
        """Test converting StartChildWorkflowCommand with minimal fields."""
        data_converter = temporalio.converter.DataConverter()

        cmd = StartChildWorkflowCommand(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
        )
        completion = WorkflowActivationCompletion(commands=[cmd])

        result = poc_to_bridge_completion("run-123", completion, data_converter)

        assert result.run_id == "run-123"
        assert len(result.successful.commands) == 1

        bridge_cmd = result.successful.commands[0]
        assert bridge_cmd.HasField("start_child_workflow_execution")
        start_cmd = bridge_cmd.start_child_workflow_execution
        assert start_cmd.seq == 1
        assert start_cmd.workflow_id == "child-1"
        assert start_cmd.workflow_type == "ChildWorkflow"

    def test_start_child_workflow_with_args(self):
        """Test converting StartChildWorkflowCommand with pre-encoded arguments."""
        data_converter = temporalio.converter.DataConverter()

        # Pre-encode args (matching the new pattern)
        encoded_args = data_converter.payload_converter.to_payloads(
            ["arg1", 42, {"key": "value"}]
        )
        cmd = StartChildWorkflowCommand(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
            args=encoded_args,
        )
        completion = WorkflowActivationCompletion(commands=[cmd])

        result = poc_to_bridge_completion("run-123", completion, data_converter)

        bridge_cmd = result.successful.commands[0]
        start_cmd = bridge_cmd.start_child_workflow_execution
        assert len(start_cmd.input) == 3

        # Verify arguments are serialized
        arg0 = data_converter.payload_converter.from_payload(start_cmd.input[0])
        arg1 = data_converter.payload_converter.from_payload(start_cmd.input[1])
        arg2 = data_converter.payload_converter.from_payload(start_cmd.input[2])
        assert arg0 == "arg1"
        assert arg1 == 42
        assert arg2 == {"key": "value"}

    def test_start_child_workflow_with_task_queue(self):
        """Test converting StartChildWorkflowCommand with task queue."""
        data_converter = temporalio.converter.DataConverter()

        cmd = StartChildWorkflowCommand(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
            task_queue="child-queue",
        )
        completion = WorkflowActivationCompletion(commands=[cmd])

        result = poc_to_bridge_completion("run-123", completion, data_converter)

        bridge_cmd = result.successful.commands[0]
        start_cmd = bridge_cmd.start_child_workflow_execution
        assert start_cmd.task_queue == "child-queue"

    def test_start_child_workflow_with_timeouts(self):
        """Test converting StartChildWorkflowCommand with timeouts."""
        data_converter = temporalio.converter.DataConverter()

        cmd = StartChildWorkflowCommand(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
            execution_timeout=timedelta(hours=1),
            run_timeout=timedelta(minutes=30),
            task_timeout=timedelta(minutes=5),
        )
        completion = WorkflowActivationCompletion(commands=[cmd])

        result = poc_to_bridge_completion("run-123", completion, data_converter)

        bridge_cmd = result.successful.commands[0]
        start_cmd = bridge_cmd.start_child_workflow_execution

        # Verify timeouts
        assert start_cmd.workflow_execution_timeout.seconds == 3600
        assert start_cmd.workflow_run_timeout.seconds == 1800
        assert start_cmd.workflow_task_timeout.seconds == 300

    def test_start_child_workflow_with_policies(self):
        """Test converting StartChildWorkflowCommand with policies."""
        data_converter = temporalio.converter.DataConverter()

        cmd = StartChildWorkflowCommand(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
            parent_close_policy=2,  # ABANDON
            cancellation_type=1,  # TRY_CANCEL
            id_reuse_policy=2,  # ALLOW_DUPLICATE_FAILED_ONLY
        )
        completion = WorkflowActivationCompletion(commands=[cmd])

        result = poc_to_bridge_completion("run-123", completion, data_converter)

        bridge_cmd = result.successful.commands[0]
        start_cmd = bridge_cmd.start_child_workflow_execution

        assert start_cmd.parent_close_policy == 2
        assert start_cmd.cancellation_type == 1
        assert start_cmd.workflow_id_reuse_policy == 2

    def test_start_child_workflow_with_retry_policy(self):
        """Test converting StartChildWorkflowCommand with retry policy."""
        data_converter = temporalio.converter.DataConverter()

        retry_policy = temporalio.common.RetryPolicy(
            initial_interval=timedelta(seconds=1),
            maximum_interval=timedelta(seconds=60),
            backoff_coefficient=2.0,
            maximum_attempts=5,
            non_retryable_error_types=["ValueError", "RuntimeError"],
        )
        cmd = StartChildWorkflowCommand(
            seq=1,
            workflow_id="child-1",
            workflow_type="ChildWorkflow",
            retry_policy=retry_policy,
        )
        completion = WorkflowActivationCompletion(commands=[cmd])

        result = poc_to_bridge_completion("run-123", completion, data_converter)

        bridge_cmd = result.successful.commands[0]
        start_cmd = bridge_cmd.start_child_workflow_execution

        assert start_cmd.retry_policy.initial_interval.seconds == 1
        assert start_cmd.retry_policy.maximum_interval.seconds == 60
        assert start_cmd.retry_policy.backoff_coefficient == 2.0
        assert start_cmd.retry_policy.maximum_attempts == 5
        assert list(start_cmd.retry_policy.non_retryable_error_types) == [
            "ValueError",
            "RuntimeError",
        ]

    def test_cancel_child_workflow(self):
        """Test converting CancelChildWorkflowCommand."""
        data_converter = temporalio.converter.DataConverter()

        cmd = CancelChildWorkflowCommand(seq=42)
        completion = WorkflowActivationCompletion(commands=[cmd])

        result = poc_to_bridge_completion("run-123", completion, data_converter)

        assert result.run_id == "run-123"
        assert len(result.successful.commands) == 1

        bridge_cmd = result.successful.commands[0]
        assert bridge_cmd.HasField("cancel_child_workflow_execution")
        cancel_cmd = bridge_cmd.cancel_child_workflow_execution
        assert cancel_cmd.child_workflow_seq == 42

    def test_multiple_child_workflow_commands(self):
        """Test converting multiple child workflow commands."""
        data_converter = temporalio.converter.DataConverter()

        commands = [
            StartChildWorkflowCommand(
                seq=1, workflow_id="child-1", workflow_type="Child1"
            ),
            StartChildWorkflowCommand(
                seq=2, workflow_id="child-2", workflow_type="Child2"
            ),
            CancelChildWorkflowCommand(seq=1),
        ]
        completion = WorkflowActivationCompletion(commands=commands)

        result = poc_to_bridge_completion("run-123", completion, data_converter)

        assert len(result.successful.commands) == 3
        assert result.successful.commands[0].HasField("start_child_workflow_execution")
        assert result.successful.commands[1].HasField("start_child_workflow_execution")
        assert result.successful.commands[2].HasField("cancel_child_workflow_execution")
