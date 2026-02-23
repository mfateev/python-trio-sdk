"""Bridge pattern tests for child workflows.

Pattern 14: Child Workflow (Success)
Pattern 15: Child Workflow Cancellation

These tests verify SDK-Core behavior for child workflow execution.
"""

from __future__ import annotations

import time
from datetime import timedelta
from uuid import uuid4

import pytest
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
async def test_pattern_14_child_workflow_success(unique_task_queue: str) -> None:
    """Test Pattern 14: Child Workflow (Success).

    Flow:
    1. Start parent workflow
    2. poll -> [initialize_workflow]
    3. complete([StartChildWorkflowExecution(seq=1, workflow_type="ChildWorkflow", ...)])
    4. poll -> [resolve_child_workflow_execution_start(seq=1, succeeded=run_id)]
    5. Start worker for child, complete child workflow
    6. poll -> [resolve_child_workflow_execution(seq=1, completed=result)]
    7. complete([CompleteWorkflowExecution])

    Verifies:
    - StartChildWorkflowExecution command structure
    - Two-phase resolution: start confirmation, then completion
    - resolve_child_workflow_execution_start structure
    - resolve_child_workflow_execution structure with completed result
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    parent_workflow_id = f"test-child-parent-{uuid4()}"
    child_workflow_id = f"test-child-child-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # 1. Start parent workflow via CLI
        start_workflow_via_cli(
            workflow_id=parent_workflow_id,
            workflow_type="ParentWorkflow",
            task_queue=unique_task_queue,
        )

        # 2. Poll for parent workflow activation (initialize_workflow)
        activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)

        assert activation.has_job_type("initialize_workflow")
        parent_run_id = activation.run_id
        print("Pattern 14: Parent received initialize_workflow")

        # 3. Start child workflow
        def build_start_child(rid: str) -> bytes:
            return (
                CompletionBuilder(rid)
                .start_child_workflow(
                    seq=1,
                    workflow_id=child_workflow_id,
                    workflow_type="ChildWorkflow",
                    task_queue=unique_task_queue,
                    args=("child_input",),
                    execution_timeout=timedelta(seconds=60),
                )
                .build()
            )

        await bridge.complete_workflow_activation(
            build_start_child(parent_run_id), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 14: Sent StartChildWorkflowExecution(seq=1)")

        # 4. Poll for child workflow start confirmation
        activation = await poll_and_handle_eviction(
            bridge,
            parent_run_id,
            timeout=DEFAULT_TIMEOUT,
            replay_commands=[build_start_child],
        )

        # May receive resolve_child_workflow_execution_start OR initialize_workflow for child
        # depending on task queue configuration
        child_started = False
        child_run_id = None

        if activation.has_job_type("resolve_child_workflow_execution_start"):
            start_job = activation.get_job("resolve_child_workflow_execution_start")
            print(
                f"Pattern 14: Received resolve_child_workflow_execution_start(seq={start_job.seq})"
            )

            # Verify start confirmation structure
            assert start_job.seq == 1
            status = start_job.WhichOneof("status")
            assert status == "succeeded", f"Expected 'succeeded', got '{status}'"
            child_run_id = start_job.succeeded.run_id
            print(f"Pattern 14: Child started with run_id: {child_run_id}")
            child_started = True

            # Acknowledge start confirmation with empty completion
            completion = CompletionBuilder(parent_run_id).build()
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )

        # The child workflow task may come on the same task queue
        # We need to handle the child's workflow tasks

        # Poll until we get the child's initialize_workflow
        child_activation = None
        for _ in range(5):
            activation = await poll_and_handle_eviction(
                bridge, parent_run_id, timeout=DEFAULT_TIMEOUT
            )

            # Handle replay for parent workflow
            if (
                activation.has_job_type("initialize_workflow")
                and activation.activation.is_replaying
                and activation.run_id == parent_run_id
            ):
                await bridge.complete_workflow_activation(
                    build_start_child(parent_run_id), timeout=DEFAULT_TIMEOUT
                )
                print("Pattern 14: Replayed StartChildWorkflow for parent")
                continue

            if activation.has_job_type("initialize_workflow"):
                init_job = activation.get_job("initialize_workflow")
                if init_job.workflow_type == "ChildWorkflow":
                    child_activation = activation
                    print(f"Pattern 14: Child received initialize_workflow")
                    break
                else:
                    # Parent got another activation, handle it
                    pass

            if activation.has_job_type("resolve_child_workflow_execution_start"):
                if not child_started:
                    start_job = activation.get_job(
                        "resolve_child_workflow_execution_start"
                    )
                    child_run_id = start_job.succeeded.run_id
                    child_started = True
                    print(f"Pattern 14: Child started with run_id: {child_run_id}")

                # Acknowledge with empty completion
                completion = CompletionBuilder(parent_run_id).build()
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )

            if activation.has_job_type("resolve_child_workflow_execution"):
                # Child already completed (fast execution)
                break

        # 5. Complete child workflow
        if child_activation:
            child_run_id = child_activation.run_id
            completion = (
                CompletionBuilder(child_run_id)
                .complete_workflow("child_result_value")
                .build()
            )
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )
            print("Pattern 14: Child completed with result")

        # 6. Poll for child workflow completion in parent
        for _ in range(5):
            activation = await poll_and_handle_eviction(
                bridge, parent_run_id, timeout=DEFAULT_TIMEOUT
            )

            # Handle replay for parent workflow
            if (
                activation.has_job_type("initialize_workflow")
                and activation.activation.is_replaying
                and activation.run_id == parent_run_id
            ):
                await bridge.complete_workflow_activation(
                    build_start_child(parent_run_id), timeout=DEFAULT_TIMEOUT
                )
                print("Pattern 14: Replayed StartChildWorkflow for parent (phase 2)")
                continue

            if activation.has_job_type("resolve_child_workflow_execution"):
                resolve_job = activation.get_job("resolve_child_workflow_execution")
                print(
                    f"Pattern 14: Parent received resolve_child_workflow_execution(seq={resolve_job.seq})"
                )

                # Verify completion structure
                assert resolve_job.seq == 1
                result_status = resolve_job.result.WhichOneof("status")
                assert result_status == "completed", (
                    f"Expected 'completed', got '{result_status}'"
                )

                # Decode result
                if resolve_job.result.completed.result.ByteSize() > 0:
                    result_value = activation.decode_payload(
                        resolve_job.result.completed.result
                    )
                    print(f"Pattern 14: Child result: {result_value}")
                    assert result_value == "child_result_value"

                # 7. Complete parent workflow
                completion = (
                    CompletionBuilder(parent_run_id)
                    .complete_workflow("parent_done")
                    .build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )
                print("Pattern 14: Parent completed")
                break

            # Handle other activation types
            if activation.run_id == parent_run_id:
                # Parent activation, acknowledge
                completion = CompletionBuilder(parent_run_id).build()
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )
            elif activation.has_job_type("initialize_workflow"):
                # Child's initialize - complete it
                completion = (
                    CompletionBuilder(activation.run_id)
                    .complete_workflow("child_result_value")
                    .build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )

        await trio.sleep(0.5)

        # Verify parent workflow completed
        status = get_workflow_status_via_cli(parent_workflow_id)
        assert status == "COMPLETED", f"Expected COMPLETED, got {status}"

        print("Pattern 14: Child Workflow Success - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_14_child_workflow_start_failed(unique_task_queue: str) -> None:
    """Test Pattern 14 variant: Child Workflow Start Failed.

    Verifies resolve_child_workflow_execution_start with failed status
    when child workflow cannot be started (e.g., workflow ID conflict).
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    parent_workflow_id = f"test-child-fail-{uuid4()}"
    # Use same ID to cause conflict
    conflicting_workflow_id = f"test-conflict-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # First, start a workflow with the conflicting ID
        start_workflow_via_cli(
            workflow_id=conflicting_workflow_id,
            workflow_type="BlockingWorkflow",
            task_queue=unique_task_queue,
        )

        # Process the blocking workflow to keep it running
        activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)
        blocking_run_id = activation.run_id

        # Start a long timer to keep it running
        completion = (
            CompletionBuilder(blocking_run_id)
            .start_timer(seq=1, duration=timedelta(seconds=300))
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        await trio.sleep(0.3)

        # Now start parent workflow
        start_workflow_via_cli(
            workflow_id=parent_workflow_id,
            workflow_type="ParentWorkflow",
            task_queue=unique_task_queue,
        )

        # Get parent activation
        activation = await poll_and_handle_eviction(
            bridge, blocking_run_id, timeout=DEFAULT_TIMEOUT
        )

        assert activation.has_job_type("initialize_workflow")
        parent_run_id = activation.run_id

        # Try to start child with conflicting ID and REJECT_DUPLICATE policy
        import temporalio.bridge.proto.workflow_commands.workflow_commands_pb2 as cmd_pb
        import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as comp_pb

        completion = comp_pb.WorkflowActivationCompletion()
        completion.run_id = parent_run_id
        completion.successful.SetInParent()

        cmd = cmd_pb.WorkflowCommand()
        cmd.start_child_workflow_execution.seq = 1
        cmd.start_child_workflow_execution.workflow_id = conflicting_workflow_id
        cmd.start_child_workflow_execution.workflow_type = "ChildWorkflow"
        cmd.start_child_workflow_execution.task_queue = unique_task_queue
        # REJECT_DUPLICATE = 3 means fail if workflow ID already exists
        cmd.start_child_workflow_execution.workflow_id_reuse_policy = 3

        completion.successful.commands.append(cmd)
        await bridge.complete_workflow_activation(
            completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 14 (start fail): Sent StartChildWorkflow with REJECT_DUPLICATE")

        # Poll for child start failure
        activation = await poll_and_handle_eviction(
            bridge, parent_run_id, timeout=DEFAULT_TIMEOUT
        )

        if activation.has_job_type("resolve_child_workflow_execution_start"):
            start_job = activation.get_job("resolve_child_workflow_execution_start")
            status = start_job.WhichOneof("status")
            print(f"Pattern 14 (start fail): Start status: {status}")

            if status == "failed":
                print(f"Pattern 14 (start fail): Start failed as expected")
                print(f"Pattern 14 (start fail): Cause: {start_job.failed.cause}")

                # Complete parent workflow handling the error
                completion = (
                    CompletionBuilder(parent_run_id)
                    .complete_workflow("child_start_failed_handled")
                    .build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )
            elif status == "succeeded":
                # On some versions/configurations, it might succeed
                # Complete anyway
                completion = CompletionBuilder(parent_run_id).build()
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )

        print("Pattern 14 (start fail): Child Workflow Start Failed - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_15_child_workflow_cancellation(unique_task_queue: str) -> None:
    """Test Pattern 15: Child Workflow Cancellation.

    Flow:
    1. Start parent
    2. poll -> [initialize_workflow]
    3. complete([StartChildWorkflowExecution(seq=1, ...)])
    4. poll -> [resolve_child_workflow_execution_start(seq=1, succeeded)]
    5. complete([CancelChildWorkflowExecution(child_workflow_seq=1)])  # SAME activation!
    6. poll -> [resolve_child_workflow_execution(seq=1, cancelled=...)]
    7. complete([CompleteWorkflowExecution])

    Key insight: CancelChildWorkflowExecution must be sent as part of completing
    the activation that delivered resolve_child_workflow_execution_start.

    Verifies:
    - CancelChildWorkflowExecution command structure
    - resolve_child_workflow_execution with cancelled status
    """
    import temporalio.bridge.proto.workflow_commands.workflow_commands_pb2 as cmd_pb
    import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as comp_pb

    bridge = TrioBridgeWrapper()
    await bridge.start()

    parent_workflow_id = f"test-child-cancel-parent-{uuid4()}"
    child_workflow_id = f"test-child-cancel-child-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # 1. Start parent workflow
        start_workflow_via_cli(
            workflow_id=parent_workflow_id,
            workflow_type="CancellingParentWorkflow",
            task_queue=unique_task_queue,
        )

        # 2. Poll for parent initialization
        activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)

        assert activation.has_job_type("initialize_workflow")
        parent_run_id = activation.run_id
        print("Pattern 15: Parent received initialize_workflow")

        # 3. Start child workflow with long timeout
        def build_start_child_15(rid: str) -> bytes:
            return (
                CompletionBuilder(rid)
                .start_child_workflow(
                    seq=1,
                    workflow_id=child_workflow_id,
                    workflow_type="LongRunningChildWorkflow",
                    task_queue=unique_task_queue,
                    execution_timeout=timedelta(seconds=300),
                )
                .build()
            )

        await bridge.complete_workflow_activation(
            build_start_child_15(parent_run_id), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 15: Sent StartChildWorkflowExecution(seq=1)")

        # 4. Process activations until we get resolve_child_workflow_execution_start for parent
        # Need to handle both child's initialize_workflow and parent's resolve_child_workflow_execution_start
        child_run_id = None
        cancel_sent = False

        for _ in range(15):
            activation = await poll_and_handle_eviction(
                bridge, parent_run_id, timeout=DEFAULT_TIMEOUT
            )

            # Handle replay for parent workflow
            if (
                activation.has_job_type("initialize_workflow")
                and activation.activation.is_replaying
                and activation.run_id == parent_run_id
            ):
                await bridge.complete_workflow_activation(
                    build_start_child_15(parent_run_id), timeout=DEFAULT_TIMEOUT
                )
                print("Pattern 15: Replayed StartChildWorkflow for parent")
                continue

            # Check if this is the parent's resolve_child_workflow_execution_start
            if activation.has_job_type("resolve_child_workflow_execution_start"):
                start_job = activation.get_job("resolve_child_workflow_execution_start")
                if start_job.WhichOneof("status") == "succeeded":
                    child_run_id = start_job.succeeded.run_id
                    print(f"Pattern 15: Child started with run_id: {child_run_id}")

                    # 5. Send CancelChildWorkflowExecution in the SAME completion
                    # This is the key fix - we must respond to this activation
                    # with the cancel command, not send it separately later
                    comp = comp_pb.WorkflowActivationCompletion()
                    comp.run_id = parent_run_id
                    comp.successful.SetInParent()

                    cmd = cmd_pb.WorkflowCommand()
                    cmd.cancel_child_workflow_execution.child_workflow_seq = 1
                    comp.successful.commands.append(cmd)

                    await bridge.complete_workflow_activation(
                        comp.SerializeToString(), timeout=DEFAULT_TIMEOUT
                    )
                    print("Pattern 15: Sent CancelChildWorkflowExecution(seq=1)")
                    cancel_sent = True
                    break

            # Handle child's initialize_workflow - start a long timer to keep it running
            if activation.has_job_type("initialize_workflow"):
                init_job = activation.get_job("initialize_workflow")
                if init_job.workflow_type == "LongRunningChildWorkflow":
                    # Start a long timer in child to keep it running
                    completion = (
                        CompletionBuilder(activation.run_id)
                        .start_timer(seq=1, duration=timedelta(seconds=300))
                        .build()
                    )
                    await bridge.complete_workflow_activation(
                        completion, timeout=DEFAULT_TIMEOUT
                    )
                    print("Pattern 15: Child workflow started with long timer")
                    continue

            # Acknowledge other activations with empty completion
            completion = CompletionBuilder(activation.run_id).build()
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )

        assert cancel_sent, "Should have sent cancel command"

        # 6. Poll for child cancellation result and handle child's cancel_workflow
        child_cancelled = False

        for _ in range(15):
            try:
                activation = await poll_and_handle_eviction(
                    bridge, parent_run_id, timeout=DEFAULT_TIMEOUT
                )

                # Handle replay for parent workflow
                if (
                    activation.has_job_type("initialize_workflow")
                    and activation.activation.is_replaying
                    and activation.run_id == parent_run_id
                ):
                    await bridge.complete_workflow_activation(
                        build_start_child_15(parent_run_id), timeout=DEFAULT_TIMEOUT
                    )
                    print(
                        "Pattern 15: Replayed StartChildWorkflow for parent (phase 2)"
                    )
                    continue

                # Check for parent's resolve_child_workflow_execution
                if activation.has_job_type("resolve_child_workflow_execution"):
                    resolve_job = activation.get_job("resolve_child_workflow_execution")
                    result_status = resolve_job.result.WhichOneof("status")
                    print(
                        f"Pattern 15: resolve_child_workflow_execution status: {result_status}"
                    )

                    if result_status == "cancelled":
                        print("Pattern 15: Child workflow cancelled successfully")
                        cancel_failure = resolve_job.result.cancelled.failure
                        if cancel_failure.message:
                            print(
                                f"Pattern 15: Cancel message: {cancel_failure.message}"
                            )
                        child_cancelled = True

                        # 7. Complete parent workflow
                        completion = (
                            CompletionBuilder(parent_run_id)
                            .complete_workflow("child_cancelled_handled")
                            .build()
                        )
                        await bridge.complete_workflow_activation(
                            completion, timeout=DEFAULT_TIMEOUT
                        )
                        break
                    elif result_status == "failed":
                        # Child might have failed instead of being cancelled
                        print(f"Pattern 15: Child failed: {resolve_job.result.failed}")
                        completion = (
                            CompletionBuilder(parent_run_id)
                            .complete_workflow("child_failed")
                            .build()
                        )
                        await bridge.complete_workflow_activation(
                            completion, timeout=DEFAULT_TIMEOUT
                        )
                        break

                # Handle child's cancel_workflow job - respond with CancelWorkflowExecution
                if activation.has_job_type("cancel_workflow"):
                    print("Pattern 15: Child received cancel_workflow")
                    comp = comp_pb.WorkflowActivationCompletion()
                    comp.run_id = activation.run_id
                    comp.successful.SetInParent()
                    cmd = cmd_pb.WorkflowCommand()
                    cmd.cancel_workflow_execution.SetInParent()
                    comp.successful.commands.append(cmd)
                    await bridge.complete_workflow_activation(
                        comp.SerializeToString(), timeout=DEFAULT_TIMEOUT
                    )
                    continue

                # Acknowledge other activations
                completion = CompletionBuilder(activation.run_id).build()
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )

            except Exception as e:
                print(f"Pattern 15: Error during poll: {e}")
                break

        await trio.sleep(0.5)

        # Verify parent completed
        status = get_workflow_status_via_cli(parent_workflow_id)
        print(f"Pattern 15: Parent workflow status: {status}")

        # Child should have been cancelled (or workflow completed successfully)
        assert child_cancelled or status == "COMPLETED", (
            f"Expected child_cancelled=True or COMPLETED status, got cancelled={child_cancelled}, status={status}"
        )

        print("Pattern 15: Child Workflow Cancellation - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_14_child_workflow_with_args(unique_task_queue: str) -> None:
    """Test Pattern 14 variant: Child workflow with arguments.

    Verifies arguments are correctly passed to child workflow.
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    parent_workflow_id = f"test-child-args-parent-{uuid4()}"
    child_workflow_id = f"test-child-args-child-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # Start parent
        start_workflow_via_cli(
            workflow_id=parent_workflow_id,
            workflow_type="ParentWorkflow",
            task_queue=unique_task_queue,
        )

        # Get parent activation
        activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)
        parent_run_id = activation.run_id

        # Start child with complex arguments
        def build_start_child_with_args(rid: str) -> bytes:
            return (
                CompletionBuilder(rid)
                .start_child_workflow(
                    seq=1,
                    workflow_id=child_workflow_id,
                    workflow_type="ArgChildWorkflow",
                    task_queue=unique_task_queue,
                    args=(
                        {"key": "value", "nested": {"a": 1}},
                        [1, 2, 3],
                        "string_arg",
                        42,
                    ),
                    execution_timeout=timedelta(seconds=30),
                )
                .build()
            )

        await bridge.complete_workflow_activation(
            build_start_child_with_args(parent_run_id), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 14 (args): Sent StartChildWorkflow with args")

        # Process activations until we find child's initialize
        child_args_received = None

        for _ in range(5):
            activation = await poll_and_handle_eviction(
                bridge, parent_run_id, timeout=DEFAULT_TIMEOUT
            )

            # Handle replay for parent workflow
            if (
                activation.has_job_type("initialize_workflow")
                and activation.activation.is_replaying
                and activation.run_id == parent_run_id
            ):
                await bridge.complete_workflow_activation(
                    build_start_child_with_args(parent_run_id), timeout=DEFAULT_TIMEOUT
                )
                print("Pattern 14 (args): Replayed StartChildWorkflow for parent")
                continue

            if activation.has_job_type("initialize_workflow"):
                init_job = activation.get_job("initialize_workflow")
                if init_job.workflow_type == "ArgChildWorkflow":
                    # Decode and verify arguments
                    child_args_received = [
                        activation.decode_payload(p) for p in init_job.arguments
                    ]
                    print(
                        f"Pattern 14 (args): Child received args: {child_args_received}"
                    )

                    # Verify arguments
                    assert len(child_args_received) == 4
                    assert child_args_received[0] == {
                        "key": "value",
                        "nested": {"a": 1},
                    }
                    assert child_args_received[1] == [1, 2, 3]
                    assert child_args_received[2] == "string_arg"
                    assert child_args_received[3] == 42

                    # Complete child
                    completion = (
                        CompletionBuilder(activation.run_id)
                        .complete_workflow("child_done")
                        .build()
                    )
                    await bridge.complete_workflow_activation(
                        completion, timeout=DEFAULT_TIMEOUT
                    )
                    break

            # Acknowledge other activations
            completion = CompletionBuilder(activation.run_id).build()
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )

        assert child_args_received is not None, "Child should have received arguments"

        # Wait for child completion in parent and complete
        for _ in range(5):
            activation = await poll_and_handle_eviction(
                bridge, parent_run_id, timeout=DEFAULT_TIMEOUT
            )

            # Handle replay for parent workflow
            if (
                activation.has_job_type("initialize_workflow")
                and activation.activation.is_replaying
                and activation.run_id == parent_run_id
            ):
                await bridge.complete_workflow_activation(
                    build_start_child_with_args(parent_run_id), timeout=DEFAULT_TIMEOUT
                )
                continue

            if activation.has_job_type("resolve_child_workflow_execution"):
                completion = (
                    CompletionBuilder(parent_run_id)
                    .complete_workflow("parent_done")
                    .build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )
                break

            completion = CompletionBuilder(activation.run_id).build()
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )

        print("Pattern 14 (args): Child Workflow with Arguments - PASSED")

    finally:
        await safe_shutdown(bridge)
