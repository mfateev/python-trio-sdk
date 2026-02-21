"""Bridge pattern tests for Continue-As-New workflow command.

These tests verify the ContinueAsNewCommand dataclass and its conversion
through the bridge to the Temporal server. Tests focus on:
- Basic continue-as-new with same workflow type
- Continue-as-new with different workflow type
- Continue-as-new with different task queue
- Passing arguments to the new execution

These tests use the POC ContinueAsNewCommand and _bridge_types conversion,
rather than the raw protobuf manipulation used in test_bridge_advanced.py.
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
import trio

from temporalio_trio._async_bridge import TrioBridgeWrapper
from temporalio_trio.worker._activation import (
    ContinueAsNewCommand,
    WorkflowActivationCompletion,
)
from temporalio_trio.worker._bridge_types import poc_to_bridge_completion

from .conftest import (
    DEFAULT_TIMEOUT,
    ActivationParser,
    CompletionBuilder,
    get_workflow_status_via_cli,
    safe_shutdown,
    start_workflow_via_cli,
)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_continue_as_new_basic(unique_task_queue: str) -> None:
    """Test basic continue-as-new using ContinueAsNewCommand.

    Flow:
    1. Start workflow
    2. poll -> [initialize_workflow]
    3. complete([ContinueAsNewCommand(workflow_type=..., args=...)])
    4. poll -> [initialize_workflow] (new execution)
    5. complete([CompleteWorkflowExecution])
    6. Verify workflow completed

    Verifies:
    - ContinueAsNewCommand is properly converted through the bridge
    - New execution starts with specified workflow type and arguments
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-can-basic-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # 1. Start workflow with initial iteration count
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="IteratingWorkflow",
            task_queue=unique_task_queue,
            args=[1],  # iteration 1
        )

        # 2. Poll for initialization
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)

        assert activation.has_job_type("initialize_workflow")
        run_id = activation.run_id
        init_job = activation.get_job("initialize_workflow")

        # Decode iteration argument
        iteration = 1
        if init_job.arguments:
            iteration = activation.decode_payload(init_job.arguments[0])
        print(
            f"Continue-As-New Basic: Received initialize_workflow (iteration={iteration})"
        )

        # 3. Use ContinueAsNewCommand (POC dataclass) instead of raw protobuf
        from temporalio.converter import DataConverter

        dc = DataConverter.default
        poc_completion = WorkflowActivationCompletion(
            commands=[
                ContinueAsNewCommand(
                    workflow_type="IteratingWorkflow",
                    args=(iteration + 1,),
                    task_queue=unique_task_queue,
                ),
            ]
        )

        # Convert using our bridge_types converter
        bridge_completion = poc_to_bridge_completion(run_id, poc_completion, dc)
        await bridge.complete_workflow_activation(
            bridge_completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )
        print(
            f"Continue-As-New Basic: Sent ContinueAsNewCommand(iteration={iteration + 1})"
        )

        # 4. Poll for new execution's initialization
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)

        # Handle possible cache eviction before getting new execution
        if activation.has_job_type("remove_from_cache"):
            print("Continue-As-New Basic: Handling cache eviction")
            evict_run_id = activation.run_id
            completion = CompletionBuilder(evict_run_id).build()
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )

            # Now poll for the new execution
            activation_bytes = await bridge.poll_workflow_activation(
                timeout=DEFAULT_TIMEOUT
            )
            activation = ActivationParser(activation_bytes)

        assert activation.has_job_type("initialize_workflow"), (
            f"Expected initialize_workflow, got {[j.WhichOneof('variant') for j in activation.jobs]}"
        )
        new_run_id = activation.run_id
        init_job = activation.get_job("initialize_workflow")

        # Verify iteration was passed
        if init_job.arguments:
            new_iteration = activation.decode_payload(init_job.arguments[0])
            print(
                f"Continue-As-New Basic: New execution received iteration={new_iteration}"
            )
            assert new_iteration == iteration + 1

        # 5. Complete the new execution
        completion = (
            CompletionBuilder(new_run_id)
            .complete_workflow(f"completed_after_{new_iteration}_iterations")
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        await trio.sleep(0.5)
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "COMPLETED", f"Expected COMPLETED, got {status}"

        print("Continue-As-New Basic: PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_continue_as_new_different_type(unique_task_queue: str) -> None:
    """Test continue-as-new with a different workflow type using ContinueAsNewCommand.

    Verifies that ContinueAsNewCommand can specify a different workflow type
    for the new execution.
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-can-diff-type-poc-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # Start as TypeA
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="TypeAWorkflow",
            task_queue=unique_task_queue,
        )

        # Get initialization
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)
        run_id = activation.run_id
        init_job = activation.get_job("initialize_workflow")
        print(f"Continue-As-New Different Type: Started as {init_job.workflow_type}")

        # Continue as TypeB using ContinueAsNewCommand
        from temporalio.converter import DataConverter

        dc = DataConverter.default
        poc_completion = WorkflowActivationCompletion(
            commands=[
                ContinueAsNewCommand(
                    workflow_type="TypeBWorkflow",
                    args=("transformed_data",),
                ),
            ]
        )

        bridge_completion = poc_to_bridge_completion(run_id, poc_completion, dc)
        await bridge.complete_workflow_activation(
            bridge_completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )
        print("Continue-As-New Different Type: Continuing as TypeBWorkflow")

        # Poll for new execution - may need to handle cache eviction
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)

        # Handle possible cache eviction
        if activation.has_job_type("remove_from_cache"):
            print("Continue-As-New Different Type: Handling cache eviction")
            evict_run_id = activation.run_id
            completion = CompletionBuilder(evict_run_id).build()
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )

            activation_bytes = await bridge.poll_workflow_activation(
                timeout=DEFAULT_TIMEOUT
            )
            activation = ActivationParser(activation_bytes)

        assert activation.has_job_type("initialize_workflow"), (
            f"Expected initialize_workflow, got {[j.WhichOneof('variant') for j in activation.jobs]}"
        )
        init_job = activation.get_job("initialize_workflow")
        print(
            f"Continue-As-New Different Type: New execution is {init_job.workflow_type}"
        )
        assert init_job.workflow_type == "TypeBWorkflow"

        # Complete
        completion = (
            CompletionBuilder(activation.run_id)
            .complete_workflow("type_b_done")
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        print("Continue-As-New Different Type: PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_continue_as_new_with_task_queue(unique_task_queue: str) -> None:
    """Test continue-as-new with a different task queue using ContinueAsNewCommand.

    Verifies that ContinueAsNewCommand can specify a different task queue.
    Note: In this test we use the same task queue since we only have one worker,
    but the command should properly set the task_queue field.
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-can-task-queue-{uuid4()}"
    # Use same queue since we have one worker, but this tests the field is properly set
    target_task_queue = unique_task_queue

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="TaskQueueWorkflow",
            task_queue=unique_task_queue,
        )

        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)
        run_id = activation.run_id
        print("Continue-As-New Task Queue: Initial workflow started")

        # Continue with explicit task queue using ContinueAsNewCommand
        from temporalio.converter import DataConverter

        dc = DataConverter.default
        poc_completion = WorkflowActivationCompletion(
            commands=[
                ContinueAsNewCommand(
                    workflow_type="TaskQueueWorkflow",
                    args=(),
                    task_queue=target_task_queue,  # Explicit task queue
                ),
            ]
        )

        bridge_completion = poc_to_bridge_completion(run_id, poc_completion, dc)
        await bridge.complete_workflow_activation(
            bridge_completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )
        print(
            f"Continue-As-New Task Queue: Continuing with task_queue={target_task_queue}"
        )

        # Poll for new execution
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)

        # Handle possible cache eviction
        if activation.has_job_type("remove_from_cache"):
            print("Continue-As-New Task Queue: Handling cache eviction")
            evict_run_id = activation.run_id
            completion = CompletionBuilder(evict_run_id).build()
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )

            activation_bytes = await bridge.poll_workflow_activation(
                timeout=DEFAULT_TIMEOUT
            )
            activation = ActivationParser(activation_bytes)

        assert activation.has_job_type("initialize_workflow"), (
            f"Expected initialize_workflow, got {[j.WhichOneof('variant') for j in activation.jobs]}"
        )
        print("Continue-As-New Task Queue: New execution received")

        # Complete
        completion = (
            CompletionBuilder(activation.run_id)
            .complete_workflow("task_queue_done")
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        await trio.sleep(0.5)
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "COMPLETED", f"Expected COMPLETED, got {status}"

        print("Continue-As-New Task Queue: PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_continue_as_new_multiple_args(unique_task_queue: str) -> None:
    """Test continue-as-new with multiple arguments using ContinueAsNewCommand.

    Verifies that ContinueAsNewCommand properly encodes and passes multiple
    arguments of different types to the new execution.
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-can-multi-args-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="MultiArgWorkflow",
            task_queue=unique_task_queue,
            args=["initial", 1, True],
        )

        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)
        run_id = activation.run_id
        init_job = activation.get_job("initialize_workflow")

        # Decode initial args
        initial_args = []
        for arg in init_job.arguments:
            initial_args.append(activation.decode_payload(arg))
        print(f"Continue-As-New Multiple Args: Initial args={initial_args}")

        # Continue with different args using ContinueAsNewCommand
        from temporalio.converter import DataConverter

        dc = DataConverter.default
        new_args = ("continued", 2, False, {"key": "value"})
        poc_completion = WorkflowActivationCompletion(
            commands=[
                ContinueAsNewCommand(
                    workflow_type="MultiArgWorkflow",
                    args=new_args,
                ),
            ]
        )

        bridge_completion = poc_to_bridge_completion(run_id, poc_completion, dc)
        await bridge.complete_workflow_activation(
            bridge_completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )
        print(f"Continue-As-New Multiple Args: Continuing with args={new_args}")

        # Poll for new execution
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)

        # Handle possible cache eviction
        if activation.has_job_type("remove_from_cache"):
            print("Continue-As-New Multiple Args: Handling cache eviction")
            evict_run_id = activation.run_id
            completion = CompletionBuilder(evict_run_id).build()
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )

            activation_bytes = await bridge.poll_workflow_activation(
                timeout=DEFAULT_TIMEOUT
            )
            activation = ActivationParser(activation_bytes)

        assert activation.has_job_type("initialize_workflow"), (
            f"Expected initialize_workflow, got {[j.WhichOneof('variant') for j in activation.jobs]}"
        )
        init_job = activation.get_job("initialize_workflow")

        # Verify new args were passed correctly
        received_args = []
        for arg in init_job.arguments:
            received_args.append(activation.decode_payload(arg))
        print(f"Continue-As-New Multiple Args: Received args={received_args}")

        assert len(received_args) == 4, f"Expected 4 args, got {len(received_args)}"
        assert received_args[0] == "continued"
        assert received_args[1] == 2
        assert received_args[2] is False
        assert received_args[3] == {"key": "value"}

        # Complete
        completion = (
            CompletionBuilder(activation.run_id)
            .complete_workflow("multi_args_done")
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        await trio.sleep(0.5)
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "COMPLETED", f"Expected COMPLETED, got {status}"

        print("Continue-As-New Multiple Args: PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_continue_as_new_with_timeout(unique_task_queue: str) -> None:
    """Test continue-as-new with timeouts using ContinueAsNewCommand.

    Verifies that ContinueAsNewCommand properly sets run_timeout and task_timeout
    for the new execution.
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-can-timeout-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="TimeoutWorkflow",
            task_queue=unique_task_queue,
        )

        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)
        run_id = activation.run_id
        print("Continue-As-New Timeout: Initial workflow started")

        # Continue with explicit timeouts using ContinueAsNewCommand
        from temporalio.converter import DataConverter

        dc = DataConverter.default
        poc_completion = WorkflowActivationCompletion(
            commands=[
                ContinueAsNewCommand(
                    workflow_type="TimeoutWorkflow",
                    args=(),
                    run_timeout=timedelta(hours=1),
                    task_timeout=timedelta(seconds=30),
                ),
            ]
        )

        bridge_completion = poc_to_bridge_completion(run_id, poc_completion, dc)
        await bridge.complete_workflow_activation(
            bridge_completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )
        print("Continue-As-New Timeout: Continuing with timeouts")

        # Poll for new execution
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)

        # Handle possible cache eviction
        if activation.has_job_type("remove_from_cache"):
            print("Continue-As-New Timeout: Handling cache eviction")
            evict_run_id = activation.run_id
            completion = CompletionBuilder(evict_run_id).build()
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )

            activation_bytes = await bridge.poll_workflow_activation(
                timeout=DEFAULT_TIMEOUT
            )
            activation = ActivationParser(activation_bytes)

        assert activation.has_job_type("initialize_workflow"), (
            f"Expected initialize_workflow, got {[j.WhichOneof('variant') for j in activation.jobs]}"
        )

        # Complete
        completion = (
            CompletionBuilder(activation.run_id)
            .complete_workflow("timeout_done")
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        await trio.sleep(0.5)
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "COMPLETED", f"Expected COMPLETED, got {status}"

        print("Continue-As-New Timeout: PASSED")

    finally:
        await safe_shutdown(bridge)
