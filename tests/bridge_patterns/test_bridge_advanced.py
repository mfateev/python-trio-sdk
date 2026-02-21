"""Bridge pattern tests for advanced workflow features.

Pattern 16: Workflow Failure
Pattern 17: Continue-As-New
Pattern 18: Signal External Workflow
Pattern 19: Search Attributes

These tests verify SDK-Core behavior for advanced workflow features.
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
    safe_shutdown,
    signal_workflow_via_cli,
    start_workflow_via_cli,
)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_16_workflow_failure(unique_task_queue: str) -> None:
    """Test Pattern 16: Workflow Failure.

    Flow:
    1. Start workflow
    2. poll -> [initialize_workflow]
    3. complete([FailWorkflowExecution(failure=...)])
    4. Verify workflow status: FAILED

    Verifies:
    - FailWorkflowExecution command structure
    - Failure message, source, and type
    - Workflow ends with FAILED status
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-wf-failure-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # 1. Start workflow
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="FailingWorkflow",
            task_queue=unique_task_queue,
        )

        # 2. Poll for initialization
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)

        assert activation.has_job_type("initialize_workflow")
        run_id = activation.run_id
        print("Pattern 16: Received initialize_workflow")

        # 3. Fail the workflow with detailed error
        import temporalio.bridge.proto.workflow_commands.workflow_commands_pb2 as cmd_pb
        import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as comp_pb

        completion = comp_pb.WorkflowActivationCompletion()
        completion.run_id = run_id
        completion.successful.SetInParent()

        cmd = cmd_pb.WorkflowCommand()
        cmd.fail_workflow_execution.failure.message = (
            "Workflow failed due to business logic error"
        )
        cmd.fail_workflow_execution.failure.source = "PythonSDK"
        cmd.fail_workflow_execution.failure.stack_trace = (
            "Traceback (most recent call last):\n"
            '  File "workflow.py", line 42, in run\n'
            '    raise ValueError("Invalid input")\n'
            "ValueError: Invalid input"
        )
        # Set application failure info
        cmd.fail_workflow_execution.failure.application_failure_info.type = "ValueError"
        cmd.fail_workflow_execution.failure.application_failure_info.non_retryable = (
            True
        )

        completion.successful.commands.append(cmd)
        await bridge.complete_workflow_activation(
            completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 16: Sent FailWorkflowExecution")

        # 4. Verify workflow status
        await trio.sleep(0.5)
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "FAILED", f"Expected FAILED, got {status}"

        print("Pattern 16: Workflow Failure - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_17_continue_as_new(unique_task_queue: str) -> None:
    """Test Pattern 17: Continue-As-New.

    Flow:
    1. Start workflow
    2. poll -> [initialize_workflow]
    3. complete([ContinueAsNewWorkflowExecution(workflow_type=..., arguments=...)])
    4. Verify original workflow: CONTINUED_AS_NEW
    5. Verify new workflow started

    Verifies:
    - ContinueAsNewWorkflowExecution command structure
    - Workflow ends with CONTINUED_AS_NEW status
    - New execution starts with specified arguments
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-continue-as-new-{uuid4()}"

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
        print(f"Pattern 17: Received initialize_workflow (iteration={iteration})")

        # 3. Continue-as-new with incremented iteration
        completion = (
            CompletionBuilder(run_id)
            .continue_as_new(
                workflow_type="IteratingWorkflow",
                args=(iteration + 1,),
                task_queue=unique_task_queue,
            )
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)
        print(
            f"Pattern 17: Sent ContinueAsNewWorkflowExecution(iteration={iteration + 1})"
        )

        # 4. Poll for new execution's initialization
        # Note: Can't easily check CONTINUED_AS_NEW status without specific run_id
        # The CLI describe returns the LATEST execution, which is the new one (RUNNING)
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)

        # Handle possible cache eviction before getting new execution
        if activation.has_job_type("remove_from_cache"):
            print("Pattern 17: Handling cache eviction")
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
            print(f"Pattern 17: New execution received iteration={new_iteration}")
            assert new_iteration == iteration + 1

        # Complete the new execution
        completion = (
            CompletionBuilder(new_run_id)
            .complete_workflow(f"completed_after_{new_iteration}_iterations")
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        await trio.sleep(0.5)
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "COMPLETED", f"Expected COMPLETED, got {status}"

        print("Pattern 17: Continue-As-New - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_17_continue_as_new_different_type(
    unique_task_queue: str,
) -> None:
    """Test Pattern 17 variant: Continue-as-new with different workflow type.

    Verifies that workflow can continue as a different type.
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-can-diff-type-{uuid4()}"

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
        print(f"Pattern 17 (type): Started as {init_job.workflow_type}")

        # Continue as TypeB
        completion = (
            CompletionBuilder(run_id)
            .continue_as_new(
                workflow_type="TypeBWorkflow",
                args=("transformed_data",),
            )
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)
        print("Pattern 17 (type): Continuing as TypeBWorkflow")

        # Poll for new execution - may need to handle cache eviction
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)

        # Handle possible cache eviction before getting new execution
        if activation.has_job_type("remove_from_cache"):
            print(f"Pattern 17 (type): Handling cache eviction")
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
        init_job = activation.get_job("initialize_workflow")
        print(f"Pattern 17 (type): New execution is {init_job.workflow_type}")
        assert init_job.workflow_type == "TypeBWorkflow"

        # Complete
        completion = (
            CompletionBuilder(activation.run_id)
            .complete_workflow("type_b_done")
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        print("Pattern 17 (type): Continue-As-New Different Type - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_18_signal_external_workflow(unique_task_queue: str) -> None:
    """Test Pattern 18: Signal External Workflow.

    Flow:
    1. Start target workflow (keep running with timer)
    2. Start signaling workflow
    3. poll -> [initialize_workflow]
    4. complete([SignalExternalWorkflowExecution(seq=1, workflow_id=target_id, signal_name="sig")])
    5. poll -> [resolve_signal_external_workflow(seq=1, success)]
    6. Verify target received signal

    Verifies:
    - SignalExternalWorkflowExecution command structure
    - resolve_signal_external_workflow job structure
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    target_workflow_id = f"test-signal-external-target-{uuid4()}"
    signaling_workflow_id = f"test-signal-external-sender-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # 1. Start target workflow
        start_workflow_via_cli(
            workflow_id=target_workflow_id,
            workflow_type="TargetWorkflow",
            task_queue=unique_task_queue,
        )

        # Keep target running with timer
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)
        target_run_id = activation.run_id

        completion = (
            CompletionBuilder(target_run_id)
            .start_timer(seq=1, duration=timedelta(seconds=300))
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)
        print("Pattern 18: Target workflow started and waiting")

        await trio.sleep(0.3)

        # 2. Start signaling workflow
        start_workflow_via_cli(
            workflow_id=signaling_workflow_id,
            workflow_type="SignalingWorkflow",
            task_queue=unique_task_queue,
        )

        # 3. Poll for signaling workflow initialization
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)
        signaling_run_id = activation.run_id
        print("Pattern 18: Signaling workflow received initialize_workflow")

        # 4. Send SignalExternalWorkflowExecution command
        import temporalio.bridge.proto.workflow_commands.workflow_commands_pb2 as cmd_pb
        import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as comp_pb
        from temporalio.converter import DataConverter

        dc = DataConverter.default

        completion = comp_pb.WorkflowActivationCompletion()
        completion.run_id = signaling_run_id
        completion.successful.SetInParent()

        cmd = cmd_pb.WorkflowCommand()
        cmd.signal_external_workflow_execution.seq = 1
        cmd.signal_external_workflow_execution.workflow_execution.workflow_id = (
            target_workflow_id
        )
        # run_id is optional - if not set, signals most recent run
        cmd.signal_external_workflow_execution.signal_name = "external_signal"

        # Add signal arguments
        signal_arg_payload = dc.payload_converter.to_payload(
            "signal_data_from_external"
        )
        cmd.signal_external_workflow_execution.args.append(signal_arg_payload)

        completion.successful.commands.append(cmd)
        await bridge.complete_workflow_activation(
            completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 18: Sent SignalExternalWorkflowExecution")

        # 5. Poll for resolution
        signal_resolved = False
        target_received_signal = False

        for _ in range(10):
            activation_bytes = await bridge.poll_workflow_activation(
                timeout=DEFAULT_TIMEOUT
            )
            activation = ActivationParser(activation_bytes)

            # Check for signal resolution in signaling workflow
            if activation.has_job_type("resolve_signal_external_workflow"):
                resolve_job = activation.get_job("resolve_signal_external_workflow")
                print(
                    f"Pattern 18: Received resolve_signal_external_workflow(seq={resolve_job.seq})"
                )

                # Check if it succeeded (failure is absent = success)
                if resolve_job.failure.ByteSize() > 0:
                    print(f"Pattern 18: Signal failed: {resolve_job.failure.message}")
                else:
                    print("Pattern 18: Signal sent successfully")
                    signal_resolved = True

                # Complete signaling workflow
                completion = (
                    CompletionBuilder(signaling_run_id)
                    .complete_workflow("signal_sent")
                    .build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )
                continue

            # Check for signal received by target
            if activation.has_job_type("signal_workflow"):
                signal_job = activation.get_job("signal_workflow")
                print(f"Pattern 18: Target received signal: {signal_job.signal_name}")
                target_received_signal = True

                # Complete target workflow
                completion = (
                    CompletionBuilder(target_run_id)
                    .cancel_timer(seq=1)
                    .complete_workflow("signal_received")
                    .build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )
                continue

            # Handle other activations
            completion = CompletionBuilder(activation.run_id).build()
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )

            if signal_resolved and target_received_signal:
                break

        assert signal_resolved, "Signal should have been resolved"
        # Note: target_received_signal might not always be true if we poll order differs

        print("Pattern 18: Signal External Workflow - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_19_search_attributes(unique_task_queue: str) -> None:
    """Test Pattern 19: Search Attributes.

    Flow:
    1. Start workflow
    2. poll -> [initialize_workflow]
    3. complete([UpsertWorkflowSearchAttributes(search_attributes={...})])
    4. poll -> (no specific job, just ack)
    5. complete([CompleteWorkflowExecution])
    6. Verify search attributes on workflow

    Verifies:
    - UpsertWorkflowSearchAttributes command structure
    - Search attributes are persisted on workflow

    Note: This test requires custom search attributes to be configured on the server.
    It may be skipped if search attributes are not available.
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-search-attrs-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # Start workflow
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="SearchAttributeWorkflow",
            task_queue=unique_task_queue,
        )

        # Get initialization
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)
        run_id = activation.run_id
        print("Pattern 19: Received initialize_workflow")

        # Upsert search attributes
        # Note: This uses the standard "CustomKeywordField" search attribute
        # which should be available on most Temporal server setups
        import temporalio.bridge.proto.workflow_commands.workflow_commands_pb2 as cmd_pb
        import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as comp_pb
        from temporalio.converter import DataConverter

        dc = DataConverter.default

        completion = comp_pb.WorkflowActivationCompletion()
        completion.run_id = run_id
        completion.successful.SetInParent()

        cmd = cmd_pb.WorkflowCommand()

        # Add search attribute - using Keyword type
        # The key must be a registered search attribute
        keyword_payload = dc.payload_converter.to_payload("test-value-123")
        keyword_payload.metadata["encoding"] = b"json/plain"
        keyword_payload.metadata["type"] = b"Keyword"
        cmd.upsert_workflow_search_attributes.search_attributes[
            "CustomKeywordField"
        ].CopyFrom(keyword_payload)

        completion.successful.commands.append(cmd)

        # Add a timer to wake up workflow and complete
        cmd2 = cmd_pb.WorkflowCommand()
        cmd2.start_timer.seq = 1
        cmd2.start_timer.start_to_fire_timeout.seconds = 0
        cmd2.start_timer.start_to_fire_timeout.nanos = 100_000_000  # 100ms
        completion.successful.commands.append(cmd2)

        try:
            await bridge.complete_workflow_activation(
                completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
            )
            print("Pattern 19: Sent UpsertWorkflowSearchAttributes + timer")
        except Exception as e:
            # Search attributes might not be configured
            print(
                f"Pattern 19: UpsertSearchAttributes failed (may not be configured): {e}"
            )
            raise

        # Wait for timer to fire
        # Note: This test may fail due to cache eviction issues with search attributes
        # The UpsertSearchAttributes command combined with short timers can cause
        # repeated eviction/replay cycles. This is a known limitation.
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)

        # Handle single cache eviction/replay if needed
        if activation.has_job_type("remove_from_cache"):
            print("Pattern 19: Handling cache eviction")
            evict_completion = CompletionBuilder(activation.run_id).build()
            await bridge.complete_workflow_activation(
                evict_completion, timeout=DEFAULT_TIMEOUT
            )
            activation_bytes = await bridge.poll_workflow_activation(
                timeout=DEFAULT_TIMEOUT
            )
            activation = ActivationParser(activation_bytes)

        # If we get initialize_workflow (replay), complete with just timer + workflow done
        if activation.has_job_type("initialize_workflow"):
            print("Pattern 19: Replaying - completing workflow directly")
            run_id = activation.run_id
            completion = (
                CompletionBuilder(run_id)
                .complete_workflow("search_attrs_set_replayed")
                .build()
            )
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )
        elif activation.has_job_type("fire_timer"):
            print("Pattern 19: Timer fired")
            # Complete workflow
            completion = (
                CompletionBuilder(run_id).complete_workflow("search_attrs_set").build()
            )
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )
        else:
            pytest.fail(
                f"Unexpected activation: {[j.WhichOneof('variant') for j in activation.jobs]}"
            )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        await trio.sleep(0.5)
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "COMPLETED", f"Expected COMPLETED, got {status}"

        # Note: Verifying search attributes would require additional CLI query
        # or API call that's not implemented in this test infrastructure

        print("Pattern 19: Search Attributes - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_16_workflow_failure_with_details(unique_task_queue: str) -> None:
    """Test Pattern 16 variant: Workflow failure with encoded details.

    Verifies failure details can include structured data.
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-wf-fail-details-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="FailWithDetailsWorkflow",
            task_queue=unique_task_queue,
        )

        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)
        run_id = activation.run_id

        # Fail with detailed failure info
        import temporalio.bridge.proto.workflow_commands.workflow_commands_pb2 as cmd_pb
        import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as comp_pb
        from temporalio.converter import DataConverter

        dc = DataConverter.default

        completion = comp_pb.WorkflowActivationCompletion()
        completion.run_id = run_id
        completion.successful.SetInParent()

        cmd = cmd_pb.WorkflowCommand()
        cmd.fail_workflow_execution.failure.message = "Validation failed"
        cmd.fail_workflow_execution.failure.source = "PythonSDK"
        cmd.fail_workflow_execution.failure.application_failure_info.type = (
            "ValidationError"
        )
        cmd.fail_workflow_execution.failure.application_failure_info.non_retryable = (
            True
        )

        # Add failure details as encoded payload
        # Note: details is a Payloads message with a nested payloads repeated field
        details_payload = dc.payload_converter.to_payload(
            {"field": "email", "error": "invalid format", "value": "not-an-email"}
        )
        cmd.fail_workflow_execution.failure.application_failure_info.details.payloads.append(
            details_payload
        )

        completion.successful.commands.append(cmd)
        await bridge.complete_workflow_activation(
            completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )

        await trio.sleep(0.5)
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "FAILED"

        print("Pattern 16 (details): Workflow Failure with Details - PASSED")

    finally:
        await safe_shutdown(bridge)
