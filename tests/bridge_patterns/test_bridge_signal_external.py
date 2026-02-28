"""Bridge pattern tests for Signal External Workflow.

Pattern 18: Signal External Workflow

This test verifies that:
1. SignalExternalWorkflowExecution command can be sent to the bridge
2. resolve_signal_external_workflow job is received with success (no failure)
3. Target workflow receives the signal
"""

from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest
import trio

from temporalio_trio._async_bridge import TrioBridgeWrapper
from temporalio_trio.worker._activation import (
    SignalExternalResolvedJob,
    SignalExternalWorkflowCommand,
)
from temporalio_trio.worker._bridge_types import (
    bridge_to_poc_activation,
    poc_to_bridge_completion,
)

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
async def test_signal_external_workflow_success(unique_task_queue: str) -> None:
    """Test Pattern 18: Signal External Workflow - Success case.

    Flow:
    1. Start target workflow (keep running with timer)
    2. Start signaling workflow
    3. poll -> [initialize_workflow] for signaling workflow
    4. complete([SignalExternalWorkflowExecution(seq=1, workflow_id=target_id, signal_name="sig")])
    5. poll -> [resolve_signal_external_workflow(seq=1, success)]
    6. Verify target received signal

    Verifies:
    - SignalExternalWorkflowExecution command structure
    - resolve_signal_external_workflow job structure
    - Success indicated by absence of failure field
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
        activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)
        target_run_id = activation.run_id

        def build_target_timer(rid: str) -> bytes:
            return (
                CompletionBuilder(rid)
                .start_timer(seq=1, duration=timedelta(seconds=300))
                .build()
            )

        await bridge.complete_workflow_activation(
            build_target_timer(target_run_id), timeout=DEFAULT_TIMEOUT
        )
        print("Test: Target workflow started and waiting")

        await trio.sleep(0.3)

        # 2. Start signaling workflow
        start_workflow_via_cli(
            workflow_id=signaling_workflow_id,
            workflow_type="SignalingWorkflow",
            task_queue=unique_task_queue,
        )

        # 3. Poll for signaling workflow initialization
        activation = await poll_and_handle_eviction(
            bridge,
            target_run_id,
            timeout=DEFAULT_TIMEOUT,
            replay_commands=[build_target_timer],
        )
        signaling_run_id = activation.run_id
        print("Test: Signaling workflow received initialize_workflow")

        # 4. Send SignalExternalWorkflowExecution command using our POC types
        import temporalio.converter

        dc = temporalio.converter.DataConverter.default

        from temporalio_trio.worker._activation import (
            SignalExternalWorkflowCommand,
            WorkflowActivationCompletion,
        )

        def build_signal_external(rid: str) -> bytes:
            encoded_args = dc.payload_converter.to_payloads(["signal_data_from_external"])
            signal_cmd = SignalExternalWorkflowCommand(
                seq=1,
                workflow_id=target_workflow_id,
                signal_name="external_signal",
                args=encoded_args,
            )
            poc_completion = WorkflowActivationCompletion(commands=[signal_cmd])
            bridge_comp = poc_to_bridge_completion(rid, poc_completion, dc)
            return bridge_comp.SerializeToString()

        await bridge.complete_workflow_activation(
            build_signal_external(signaling_run_id), timeout=DEFAULT_TIMEOUT
        )
        print("Test: Sent SignalExternalWorkflowExecution via POC types")

        # 5. Poll for resolution and signal delivery
        signal_resolved = False
        target_received_signal = False

        for _ in range(10):
            if signal_resolved and target_received_signal:
                break

            activation = await poll_and_handle_eviction(
                bridge, "", timeout=DEFAULT_TIMEOUT
            )

            # Handle replay for either workflow
            if (
                activation.has_job_type("initialize_workflow")
                and activation.activation.is_replaying
            ):
                if activation.run_id == target_run_id:
                    await bridge.complete_workflow_activation(
                        build_target_timer(activation.run_id), timeout=DEFAULT_TIMEOUT
                    )
                elif activation.run_id == signaling_run_id:
                    await bridge.complete_workflow_activation(
                        build_signal_external(activation.run_id),
                        timeout=DEFAULT_TIMEOUT,
                    )
                else:
                    completion = CompletionBuilder(activation.run_id).build()
                    await bridge.complete_workflow_activation(
                        completion, timeout=DEFAULT_TIMEOUT
                    )
                print(f"Test: Replayed commands for run_id={activation.run_id[:8]}...")
                continue

            # Check for signal resolution in signaling workflow
            if activation.has_job_type("resolve_signal_external_workflow"):
                resolve_job = activation.get_job("resolve_signal_external_workflow")
                print(
                    f"Test: Received resolve_signal_external_workflow(seq={resolve_job.seq})"
                )

                # Verify success is indicated by absence of failure
                if resolve_job.failure.ByteSize() > 0:
                    pytest.fail(
                        f"Signal should have succeeded, but got failure: {resolve_job.failure.message}"
                    )
                else:
                    print("Test: Signal sent successfully (no failure field)")
                    signal_resolved = True

                # Also test POC conversion
                import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb

                bridge_act = act_pb.WorkflowActivation()
                bridge_act.ParseFromString(activation.activation.SerializeToString())
                poc_activation = bridge_to_poc_activation(bridge_act, dc)

                # Find the SignalExternalResolvedJob
                for job in poc_activation.jobs:
                    if isinstance(job, SignalExternalResolvedJob):
                        assert job.seq == 1
                        assert job.failure is None, (
                            "POC job should have no failure for success"
                        )
                        print("Test: POC SignalExternalResolvedJob correctly parsed")

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
                print(f"Test: Target received signal: {signal_job.signal_name}")

                # Verify signal name and args
                assert signal_job.signal_name == "external_signal"
                if signal_job.input:
                    arg_value = dc.payload_converter.from_payload(signal_job.input[0])
                    print(f"Test: Signal arg value: {arg_value}")
                    assert arg_value == "signal_data_from_external"

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

            # Handle other activations (cache eviction, etc.)
            completion = CompletionBuilder(activation.run_id).build()
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )

            if signal_resolved and target_received_signal:
                break

        assert signal_resolved, "Signal should have been resolved"
        assert target_received_signal, "Target should have received the signal"

        # Verify workflows completed
        await trio.sleep(0.5)
        signaling_status = get_workflow_status_via_cli(signaling_workflow_id)
        target_status = get_workflow_status_via_cli(target_workflow_id)

        assert signaling_status == "COMPLETED", (
            f"Expected signaling COMPLETED, got {signaling_status}"
        )
        assert target_status == "COMPLETED", (
            f"Expected target COMPLETED, got {target_status}"
        )

        print("Test: Signal External Workflow - SUCCESS")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_signal_external_workflow_with_run_id(unique_task_queue: str) -> None:
    """Test Pattern 18: Signal External Workflow with specific run_id.

    Verifies that we can signal a specific run of a workflow.
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    target_workflow_id = f"test-signal-external-runid-{uuid4()}"
    signaling_workflow_id = f"test-signal-external-sender-runid-{uuid4()}"

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

        # Get target's run_id
        activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)
        target_run_id = activation.run_id
        print(f"Test: Target run_id = {target_run_id}")

        # Keep target running
        def build_target_timer_runid(rid: str) -> bytes:
            return (
                CompletionBuilder(rid)
                .start_timer(seq=1, duration=timedelta(seconds=300))
                .build()
            )

        await bridge.complete_workflow_activation(
            build_target_timer_runid(target_run_id), timeout=DEFAULT_TIMEOUT
        )

        await trio.sleep(0.3)

        # 2. Start signaling workflow
        start_workflow_via_cli(
            workflow_id=signaling_workflow_id,
            workflow_type="SignalingWorkflow",
            task_queue=unique_task_queue,
        )

        activation = await poll_and_handle_eviction(
            bridge,
            target_run_id,
            timeout=DEFAULT_TIMEOUT,
            replay_commands=[build_target_timer_runid],
        )
        signaling_run_id = activation.run_id

        # 3. Send signal with specific run_id
        import temporalio.converter

        dc = temporalio.converter.DataConverter.default

        from temporalio_trio.worker._activation import (
            SignalExternalWorkflowCommand,
            WorkflowActivationCompletion,
        )

        def build_signal_with_runid(rid: str) -> bytes:
            encoded_args = dc.payload_converter.to_payloads(["data_for_specific_run"])
            signal_cmd = SignalExternalWorkflowCommand(
                seq=1,
                workflow_id=target_workflow_id,
                run_id=target_run_id,
                signal_name="specific_run_signal",
                args=encoded_args,
            )
            poc_completion = WorkflowActivationCompletion(commands=[signal_cmd])
            bridge_comp = poc_to_bridge_completion(rid, poc_completion, dc)
            return bridge_comp.SerializeToString()

        await bridge.complete_workflow_activation(
            build_signal_with_runid(signaling_run_id), timeout=DEFAULT_TIMEOUT
        )
        print("Test: Sent signal with specific run_id")

        # 4. Wait for resolution and signal
        signal_resolved = False
        target_received_signal = False

        for _ in range(10):
            if signal_resolved and target_received_signal:
                break

            activation = await poll_and_handle_eviction(
                bridge, "", timeout=DEFAULT_TIMEOUT
            )

            # Handle replay
            if (
                activation.has_job_type("initialize_workflow")
                and activation.activation.is_replaying
            ):
                if activation.run_id == target_run_id:
                    await bridge.complete_workflow_activation(
                        build_target_timer_runid(activation.run_id),
                        timeout=DEFAULT_TIMEOUT,
                    )
                elif activation.run_id == signaling_run_id:
                    await bridge.complete_workflow_activation(
                        build_signal_with_runid(activation.run_id),
                        timeout=DEFAULT_TIMEOUT,
                    )
                else:
                    completion = CompletionBuilder(activation.run_id).build()
                    await bridge.complete_workflow_activation(
                        completion, timeout=DEFAULT_TIMEOUT
                    )
                continue

            if activation.has_job_type("resolve_signal_external_workflow"):
                resolve_job = activation.get_job("resolve_signal_external_workflow")
                if resolve_job.failure.ByteSize() == 0:
                    signal_resolved = True
                    print("Test: Signal with run_id resolved successfully")

                completion = (
                    CompletionBuilder(signaling_run_id)
                    .complete_workflow("signal_sent_with_run_id")
                    .build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )
                continue

            if activation.has_job_type("signal_workflow"):
                signal_job = activation.get_job("signal_workflow")
                assert signal_job.signal_name == "specific_run_signal"
                target_received_signal = True
                print("Test: Target received signal with specific run_id")

                completion = (
                    CompletionBuilder(target_run_id)
                    .cancel_timer(seq=1)
                    .complete_workflow("signal_with_run_id_received")
                    .build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )
                continue

            completion = CompletionBuilder(activation.run_id).build()
            await bridge.complete_workflow_activation(
                completion, timeout=DEFAULT_TIMEOUT
            )

            if signal_resolved and target_received_signal:
                break

        assert signal_resolved, "Signal with run_id should have resolved"
        assert target_received_signal, "Target should have received signal with run_id"

        print("Test: Signal External Workflow with run_id - SUCCESS")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_signal_external_workflow_nonexistent_target(
    unique_task_queue: str,
) -> None:
    """Test Pattern 18: Signal External Workflow - Target not found.

    Verifies that signaling a non-existent workflow fails with appropriate error.
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    signaling_workflow_id = f"test-signal-external-fail-{uuid4()}"
    nonexistent_workflow_id = f"nonexistent-workflow-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # Start signaling workflow
        start_workflow_via_cli(
            workflow_id=signaling_workflow_id,
            workflow_type="SignalingWorkflow",
            task_queue=unique_task_queue,
        )

        activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)
        signaling_run_id = activation.run_id

        # Send signal to non-existent workflow
        import temporalio.converter

        dc = temporalio.converter.DataConverter.default

        from temporalio_trio.worker._activation import (
            SignalExternalWorkflowCommand,
            WorkflowActivationCompletion,
        )

        def build_signal_nonexistent(rid: str) -> bytes:
            signal_cmd = SignalExternalWorkflowCommand(
                seq=1,
                workflow_id=nonexistent_workflow_id,
                signal_name="test_signal",
                args=(),
            )
            poc_completion = WorkflowActivationCompletion(commands=[signal_cmd])
            bridge_comp = poc_to_bridge_completion(rid, poc_completion, dc)
            return bridge_comp.SerializeToString()

        await bridge.complete_workflow_activation(
            build_signal_nonexistent(signaling_run_id), timeout=DEFAULT_TIMEOUT
        )
        print("Test: Sent signal to non-existent workflow")

        # Wait for resolution - should fail
        signal_failed = False

        for _ in range(5):
            activation = await poll_and_handle_eviction(
                bridge,
                signaling_run_id,
                timeout=DEFAULT_TIMEOUT,
                replay_commands=[build_signal_nonexistent],
            )

            if activation.has_job_type("resolve_signal_external_workflow"):
                resolve_job = activation.get_job("resolve_signal_external_workflow")

                # Check that failure IS present
                if resolve_job.failure.ByteSize() > 0:
                    signal_failed = True
                    print(
                        f"Test: Signal failed as expected: {resolve_job.failure.message}"
                    )

                    # Also verify POC conversion captures the failure
                    import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb

                    bridge_act = act_pb.WorkflowActivation()
                    bridge_act.ParseFromString(
                        activation.activation.SerializeToString()
                    )
                    poc_activation = bridge_to_poc_activation(bridge_act, dc)

                    for job in poc_activation.jobs:
                        if isinstance(job, SignalExternalResolvedJob):
                            assert job.failure is not None, (
                                "POC job should have failure"
                            )
                            print(f"Test: POC failure message: {job.failure}")
                else:
                    pytest.fail("Signal to non-existent workflow should have failed")

                # Complete signaling workflow
                completion = (
                    CompletionBuilder(signaling_run_id)
                    .complete_workflow("signal_failed")
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

        assert signal_failed, "Signal to non-existent workflow should have failed"

        print("Test: Signal External Workflow to non-existent target - SUCCESS")

    finally:
        await safe_shutdown(bridge)


def test_signal_external_command_dataclass() -> None:
    """Test SignalExternalWorkflowCommand dataclass structure."""
    import temporalio.converter

    converter = temporalio.converter.DataConverter.default.payload_converter
    encoded_args = converter.to_payloads(["arg1", 42, {"key": "value"}])
    cmd = SignalExternalWorkflowCommand(
        seq=1,
        workflow_id="target-workflow",
        signal_name="my_signal",
        run_id="specific-run-id",
        args=encoded_args,
    )

    assert cmd.seq == 1
    assert cmd.workflow_id == "target-workflow"
    assert cmd.signal_name == "my_signal"
    assert cmd.run_id == "specific-run-id"
    assert len(cmd.args) == 3


def test_signal_external_command_defaults() -> None:
    """Test SignalExternalWorkflowCommand default values."""
    cmd = SignalExternalWorkflowCommand(
        seq=1,
        workflow_id="target",
        signal_name="signal",
    )

    assert cmd.run_id is None
    assert cmd.args == []


def test_signal_external_resolved_job_success() -> None:
    """Test SignalExternalResolvedJob for success case."""
    job = SignalExternalResolvedJob(seq=1, failure=None)

    assert job.seq == 1
    assert job.failure is None  # Success indicated by absence of failure


def test_signal_external_resolved_job_failure() -> None:
    """Test SignalExternalResolvedJob for failure case."""
    error = RuntimeError("Target workflow not found")
    job = SignalExternalResolvedJob(seq=1, failure=error)

    assert job.seq == 1
    assert job.failure is not None
    assert "not found" in str(job.failure)
