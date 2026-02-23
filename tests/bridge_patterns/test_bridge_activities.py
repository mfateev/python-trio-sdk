"""Bridge pattern tests for activities.

Pattern 8: Activity Execution (Success)
Pattern 9: Activity Failure
Pattern 10: Activity Cancellation

These tests verify SDK-Core behavior for activity scheduling and resolution.
"""

from __future__ import annotations

import json
import time
from datetime import timedelta
from uuid import uuid4

import pytest
import temporalio.bridge.proto
import temporalio.bridge.proto.activity_result.activity_result_pb2 as activity_result_pb
import temporalio.bridge.proto.activity_task.activity_task_pb2 as activity_task_pb
import trio

from temporalio_trio._async_bridge import TrioBridgeWrapper

from .conftest import (
    DEFAULT_TIMEOUT,
    ActivationParser,
    CompletionBuilder,
    get_workflow_status_via_cli,
    poll_and_handle_eviction,
    safe_shutdown,
    start_workflow_via_cli,
)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_8_activity_execution_success(unique_task_queue: str) -> None:
    """Test Pattern 8: Activity Execution (Success).

    Flow:
    1. Start workflow via CLI
    2. poll -> [initialize_workflow]
    3. complete([ScheduleActivity(seq=1, ...)])
    4. Poll activity task, complete activity
    5. poll -> [resolve_activity(seq=1, completed=result)]
    6. complete([CompleteWorkflowExecution])

    Verifies:
    - ScheduleActivity command structure
    - Activity task polling
    - Activity completion
    - resolve_activity job structure with completed result
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-activity-success-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # 1. Start workflow via CLI
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="ActivityWorkflow",
            task_queue=unique_task_queue,
            args=["test_input"],
        )

        # 2. Poll for workflow activation (initialize_workflow)
        activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)

        assert activation.has_job_type("initialize_workflow"), (
            f"Expected initialize_workflow job, got: "
            f"{[j.WhichOneof('variant') for j in activation.jobs]}"
        )

        run_id = activation.run_id
        init_job = activation.get_job("initialize_workflow")
        print(f"Pattern 8: Received initialize_workflow for {init_job.workflow_type}")

        # 3. Complete with ScheduleActivity command
        def build_schedule_activity(rid: str) -> bytes:
            return (
                CompletionBuilder(rid)
                .schedule_activity(
                    seq=1,
                    activity_type="TestActivity",
                    task_queue=unique_task_queue,
                    args=("activity_arg",),
                    schedule_to_close_timeout=timedelta(seconds=60),
                    start_to_close_timeout=timedelta(seconds=30),
                )
                .build()
            )

        await bridge.complete_workflow_activation(
            build_schedule_activity(run_id), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 8: Sent ScheduleActivity(seq=1)")

        # 4. Poll for activity task
        activity_task_bytes = await bridge.poll_activity_task(timeout=DEFAULT_TIMEOUT)
        activity_task = activity_task_pb.ActivityTask()
        activity_task.ParseFromString(activity_task_bytes)

        print(f"Pattern 8: Received activity task: {activity_task.start.activity_type}")

        # Verify activity task fields
        assert activity_task.start.activity_type == "TestActivity"
        assert len(activity_task.start.input) > 0

        # 5. Complete the activity with a result
        activity_completion = activity_result_pb.ActivityExecutionResult()
        from temporalio.converter import DataConverter

        dc = DataConverter.default
        result_payload = dc.payload_converter.to_payload("activity_result_value")
        activity_completion.completed.result.CopyFrom(result_payload)

        activity_task_completion = temporalio.bridge.proto.ActivityTaskCompletion()
        activity_task_completion.task_token = activity_task.task_token
        activity_task_completion.result.CopyFrom(activity_completion)

        await bridge.complete_activity_task(
            activity_task_completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 8: Completed activity with result")

        # 6. Poll for workflow activation (resolve_activity)
        activation = await poll_and_handle_eviction(
            bridge, run_id, timeout=DEFAULT_TIMEOUT,
            replay_commands=[build_schedule_activity],
        )

        assert activation.has_job_type("resolve_activity"), (
            f"Expected resolve_activity job, got: "
            f"{[j.WhichOneof('variant') for j in activation.jobs]}"
        )

        resolve_job = activation.get_job("resolve_activity")
        print(f"Pattern 8: Received resolve_activity(seq={resolve_job.seq})")

        # Verify resolve_activity structure
        assert resolve_job.seq == 1
        result_status = resolve_job.result.WhichOneof("status")
        assert result_status == "completed", (
            f"Expected 'completed', got '{result_status}'"
        )

        # Decode and verify result
        if resolve_job.result.completed.result.ByteSize() > 0:
            result_value = activation.decode_payload(
                resolve_job.result.completed.result
            )
            print(f"Pattern 8: Activity result: {result_value}")
            assert result_value == "activity_result_value"

        # 7. Complete workflow
        completion = (
            CompletionBuilder(run_id).complete_workflow("workflow_done").build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)
        print("Pattern 8: Sent CompleteWorkflowExecution")

        # Give server time to process
        await trio.sleep(0.5)

        # Verify workflow completed
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "COMPLETED", f"Expected COMPLETED, got {status}"

        print("Pattern 8: Activity Execution Success - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_9_activity_failure(unique_task_queue: str) -> None:
    """Test Pattern 9: Activity Failure.

    Flow:
    1. Start workflow
    2. poll -> [initialize_workflow]
    3. complete([ScheduleActivity(seq=1, ...)])
    4. Execute activity, return failure
    5. poll -> [resolve_activity(seq=1, failed=failure)]
    6. complete([FailWorkflowExecution]) or handle error

    Verifies:
    - Activity failure reporting structure
    - Failure message and type in resolve_activity
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-activity-failure-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # 1. Start workflow via CLI
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="ActivityFailureWorkflow",
            task_queue=unique_task_queue,
        )

        # 2. Poll for workflow activation (initialize_workflow)
        activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)

        assert activation.has_job_type("initialize_workflow")
        run_id = activation.run_id
        print("Pattern 9: Received initialize_workflow")

        # 3. Complete with ScheduleActivity command
        def build_schedule_failing_activity(rid: str) -> bytes:
            return (
                CompletionBuilder(rid)
                .schedule_activity(
                    seq=1,
                    activity_type="FailingActivity",
                    task_queue=unique_task_queue,
                    schedule_to_close_timeout=timedelta(seconds=10),
                    start_to_close_timeout=timedelta(seconds=5),
                )
                .build()
            )

        await bridge.complete_workflow_activation(
            build_schedule_failing_activity(run_id), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 9: Sent ScheduleActivity(seq=1)")

        # 4. Poll for activity task
        activity_task_bytes = await bridge.poll_activity_task(timeout=DEFAULT_TIMEOUT)
        activity_task = activity_task_pb.ActivityTask()
        activity_task.ParseFromString(activity_task_bytes)

        print(f"Pattern 9: Received activity task: {activity_task.start.activity_type}")

        # 5. Complete activity with failure
        activity_completion = activity_result_pb.ActivityExecutionResult()
        activity_completion.failed.failure.message = "Activity intentionally failed"
        activity_completion.failed.failure.source = "PythonSDK"
        activity_completion.failed.failure.application_failure_info.type = (
            "RuntimeError"
        )
        activity_completion.failed.failure.application_failure_info.non_retryable = True

        activity_task_completion = temporalio.bridge.proto.ActivityTaskCompletion()
        activity_task_completion.task_token = activity_task.task_token
        activity_task_completion.result.CopyFrom(activity_completion)

        await bridge.complete_activity_task(
            activity_task_completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 9: Completed activity with failure")

        # 6. Poll for workflow activation (resolve_activity with failure)
        activation = await poll_and_handle_eviction(
            bridge, run_id, timeout=DEFAULT_TIMEOUT,
            replay_commands=[build_schedule_failing_activity],
        )

        assert activation.has_job_type("resolve_activity"), (
            f"Expected resolve_activity job, got: "
            f"{[j.WhichOneof('variant') for j in activation.jobs]}"
        )

        resolve_job = activation.get_job("resolve_activity")
        print(f"Pattern 9: Received resolve_activity(seq={resolve_job.seq})")

        # Verify failure structure
        assert resolve_job.seq == 1
        result_status = resolve_job.result.WhichOneof("status")
        assert result_status == "failed", f"Expected 'failed', got '{result_status}'"

        # Check failure details
        # Failure is wrapped: activity_failure_info with cause containing the actual error
        failure = resolve_job.result.failed.failure
        print(f"Pattern 9: Failure message: {failure.message}")
        print(f"Pattern 9: Failure source: {failure.source}")

        # The actual error message is in the cause
        if failure.cause.ByteSize() > 0:
            print(f"Pattern 9: Cause message: {failure.cause.message}")
            assert "intentionally failed" in failure.cause.message
        else:
            # Fallback to checking the outer message
            assert "Activity task failed" in failure.message

        # 7. Fail the workflow (propagate activity failure)
        completion = (
            CompletionBuilder(run_id)
            .fail_workflow(f"Activity failed: {failure.message}")
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)
        print("Pattern 9: Sent FailWorkflowExecution")

        # Give server time to process
        await trio.sleep(0.5)

        # Verify workflow failed
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "FAILED", f"Expected FAILED, got {status}"

        print("Pattern 9: Activity Failure - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_10_activity_cancellation(unique_task_queue: str) -> None:
    """Test Pattern 10: Activity Cancellation.

    Flow:
    1. Start workflow
    2. poll -> [initialize_workflow]
    3. complete([ScheduleActivity(seq=1) + StartTimer(seq=2, 100ms)])
    4. Poll activity task (activity is running)
    5. poll -> [fire_timer(seq=2)] (timer wakes up workflow)
    6. complete([RequestCancelActivity(seq=1)])
    7. Complete activity with cancelled status
    8. poll -> [resolve_activity(seq=1, cancelled=...)]
    9. complete([CompleteWorkflowExecution])

    Verifies:
    - RequestCancelActivity command structure
    - Activity cancellation flow
    - resolve_activity with cancelled status
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-activity-cancel-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # 1. Start workflow via CLI
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="ActivityCancelWorkflow",
            task_queue=unique_task_queue,
        )

        # 2. Poll for workflow activation (initialize_workflow)
        activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)

        assert activation.has_job_type("initialize_workflow")
        run_id = activation.run_id
        print("Pattern 10: Received initialize_workflow")

        # 3. Schedule activity + short timer (to wake up workflow for cancellation)
        def build_schedule_activity_and_timer(rid: str) -> bytes:
            return (
                CompletionBuilder(rid)
                .schedule_activity(
                    seq=1,
                    activity_type="LongRunningActivity",
                    task_queue=unique_task_queue,
                    schedule_to_close_timeout=timedelta(seconds=60),
                    start_to_close_timeout=timedelta(seconds=60),
                    heartbeat_timeout=timedelta(seconds=5),
                )
                .start_timer(seq=2, duration=timedelta(milliseconds=100))
                .build()
            )

        def build_request_cancel_activity(rid: str) -> bytes:
            return CompletionBuilder(rid).request_cancel_activity(seq=1).build()

        await bridge.complete_workflow_activation(
            build_schedule_activity_and_timer(run_id), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 10: Sent ScheduleActivity(seq=1) + StartTimer(seq=2)")

        # 4. Poll for activity task (activity starts running)
        activity_task_bytes = await bridge.poll_activity_task(timeout=DEFAULT_TIMEOUT)
        activity_task = activity_task_pb.ActivityTask()
        activity_task.ParseFromString(activity_task_bytes)
        print("Pattern 10: Received activity task (activity running)")

        # 5. Poll for timer to fire (wakes up workflow)
        activation = await poll_and_handle_eviction(
            bridge, run_id, timeout=DEFAULT_TIMEOUT,
            replay_commands=[build_schedule_activity_and_timer],
        )

        assert activation.has_job_type("fire_timer"), (
            f"Expected fire_timer job, got: "
            f"{[j.WhichOneof('variant') for j in activation.jobs]}"
        )
        print("Pattern 10: Timer fired, workflow woke up")

        # 6. Send RequestCancelActivity
        await bridge.complete_workflow_activation(
            build_request_cancel_activity(run_id), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 10: Sent RequestCancelActivity(seq=1)")

        # 7. Complete activity with cancelled status
        activity_completion = activity_result_pb.ActivityExecutionResult()
        activity_completion.cancelled.failure.message = "Activity cancelled by workflow"
        activity_completion.cancelled.failure.source = "PythonSDK"

        activity_task_completion = temporalio.bridge.proto.ActivityTaskCompletion()
        activity_task_completion.task_token = activity_task.task_token
        activity_task_completion.result.CopyFrom(activity_completion)

        await bridge.complete_activity_task(
            activity_task_completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 10: Completed activity with cancelled status")

        # 8. Poll for workflow activation (resolve_activity with cancelled)
        activation = await poll_and_handle_eviction(
            bridge, run_id, timeout=DEFAULT_TIMEOUT,
            replay_commands=[
                build_schedule_activity_and_timer,
                build_request_cancel_activity,
            ],
        )

        assert activation.has_job_type("resolve_activity"), (
            f"Expected resolve_activity job, got: "
            f"{[j.WhichOneof('variant') for j in activation.jobs]}"
        )

        resolve_job = activation.get_job("resolve_activity")
        print(f"Pattern 10: Received resolve_activity(seq={resolve_job.seq})")

        # Verify cancelled structure
        assert resolve_job.seq == 1
        result_status = resolve_job.result.WhichOneof("status")
        assert result_status == "cancelled", (
            f"Expected 'cancelled', got '{result_status}'"
        )

        # Check cancellation details
        cancel_failure = resolve_job.result.cancelled.failure
        print(f"Pattern 10: Cancellation message: {cancel_failure.message}")

        # 9. Complete workflow (handling cancellation gracefully)
        completion = (
            CompletionBuilder(run_id).complete_workflow("cancelled_gracefully").build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)
        print("Pattern 10: Sent CompleteWorkflowExecution")

        # Give server time to process
        await trio.sleep(0.5)

        # Verify workflow completed
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "COMPLETED", f"Expected COMPLETED, got {status}"

        print("Pattern 10: Activity Cancellation - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_8_activity_with_retry(unique_task_queue: str) -> None:
    """Test Pattern 8 variant: Activity with retry policy.

    Verifies that activity retry policies are properly serialized
    and that retries work correctly.
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-activity-retry-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # Start workflow
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="ActivityRetryWorkflow",
            task_queue=unique_task_queue,
        )

        # Poll and get run_id
        activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)
        run_id = activation.run_id

        # Schedule activity with retry policy
        # Using low-level protobuf to set retry policy
        import google.protobuf.duration_pb2
        import temporalio.bridge.proto.workflow_commands.workflow_commands_pb2 as cmd_pb
        import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as comp_pb

        def build_schedule_retryable_activity(rid: str) -> bytes:
            comp = comp_pb.WorkflowActivationCompletion()
            comp.run_id = rid
            comp.successful.SetInParent()

            cmd = cmd_pb.WorkflowCommand()
            cmd.schedule_activity.seq = 1
            cmd.schedule_activity.activity_id = "1"
            cmd.schedule_activity.activity_type = "RetryableActivity"
            cmd.schedule_activity.task_queue = unique_task_queue

            # Set timeout
            cmd.schedule_activity.schedule_to_close_timeout.seconds = 30

            # Set retry policy
            cmd.schedule_activity.retry_policy.initial_interval.seconds = 1
            cmd.schedule_activity.retry_policy.backoff_coefficient = 2.0
            cmd.schedule_activity.retry_policy.maximum_interval.seconds = 10
            cmd.schedule_activity.retry_policy.maximum_attempts = 3

            comp.successful.commands.append(cmd)
            return comp.SerializeToString()

        await bridge.complete_workflow_activation(
            build_schedule_retryable_activity(run_id), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 8 (retry): Scheduled activity with retry policy")

        # Poll activity, fail it, and verify retry happens
        # First attempt
        activity_task_bytes = await bridge.poll_activity_task(timeout=DEFAULT_TIMEOUT)
        activity_task = activity_task_pb.ActivityTask()
        activity_task.ParseFromString(activity_task_bytes)
        print(f"Pattern 8 (retry): Attempt {activity_task.start.attempt}")

        # Fail first attempt
        activity_completion = activity_result_pb.ActivityExecutionResult()
        activity_completion.failed.failure.message = "Temporary failure"
        activity_completion.failed.failure.application_failure_info.non_retryable = (
            False
        )

        activity_task_completion = temporalio.bridge.proto.ActivityTaskCompletion()
        activity_task_completion.task_token = activity_task.task_token
        activity_task_completion.result.CopyFrom(activity_completion)

        await bridge.complete_activity_task(
            activity_task_completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )

        # Wait for retry (poll again)
        activity_task_bytes = await bridge.poll_activity_task(timeout=DEFAULT_TIMEOUT)
        activity_task = activity_task_pb.ActivityTask()
        activity_task.ParseFromString(activity_task_bytes)
        print(f"Pattern 8 (retry): Retry attempt {activity_task.start.attempt}")

        # This time succeed
        activity_completion = activity_result_pb.ActivityExecutionResult()
        from temporalio.converter import DataConverter

        dc = DataConverter.default
        result_payload = dc.payload_converter.to_payload("success_after_retry")
        activity_completion.completed.result.CopyFrom(result_payload)

        activity_task_completion = temporalio.bridge.proto.ActivityTaskCompletion()
        activity_task_completion.task_token = activity_task.task_token
        activity_task_completion.result.CopyFrom(activity_completion)

        await bridge.complete_activity_task(
            activity_task_completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )

        # Poll for resolve_activity
        activation = await poll_and_handle_eviction(
            bridge, run_id, timeout=DEFAULT_TIMEOUT,
            replay_commands=[build_schedule_retryable_activity],
        )

        assert activation.has_job_type("resolve_activity")
        resolve_job = activation.get_job("resolve_activity")
        assert resolve_job.result.WhichOneof("status") == "completed"

        # Complete workflow
        completion = (
            CompletionBuilder(run_id).complete_workflow("retry_success").build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        await trio.sleep(0.5)
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "COMPLETED"

        print("Pattern 8 (retry): Activity with retry - PASSED")

    finally:
        await safe_shutdown(bridge)
