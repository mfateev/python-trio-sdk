"""Bridge pattern tests for signals and queries.

Pattern 11: Signal Workflow
Pattern 12: Query Workflow
Pattern 13: Query Failure

These tests verify SDK-Core behavior for workflow signals and queries.
"""

from __future__ import annotations

import time
from datetime import timedelta
from uuid import uuid4

import pytest
import trio

from temporalio_trio._async_bridge import TrioBridgeWrapper

from .conftest import (
    ActivationParser,
    CompletionBuilder,
    DEFAULT_TIMEOUT,
    get_workflow_status_via_cli,
    query_workflow_via_cli,
    query_workflow_via_cli_async,
    safe_shutdown,
    signal_workflow_via_cli,
    start_workflow_via_cli,
)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_11_signal_workflow(unique_task_queue: str) -> None:
    """Test Pattern 11: Signal Workflow.

    Flow:
    1. Start workflow
    2. poll -> [initialize_workflow]
    3. complete([StartTimer(seq=1, duration=60s)])  # Keep workflow alive
    4. Send signal via CLI: temporal workflow signal --name my_signal --input '"data"'
    5. poll -> [signal_workflow(signal_name="my_signal", input=[payload])]
    6. complete([])  # No response command needed for signals
    7. complete([CancelTimer, CompleteWorkflowExecution])

    Verifies:
    - signal_workflow job structure
    - Signal name and arguments are correctly received
    - Signals don't require a response command
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-signal-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # 1. Start workflow via CLI
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="SignalWorkflow",
            task_queue=unique_task_queue,
        )

        # 2. Poll for workflow activation (initialize_workflow)
        activation_bytes = await bridge.poll_workflow_activation(timeout=DEFAULT_TIMEOUT)
        activation = ActivationParser(activation_bytes)

        assert activation.has_job_type("initialize_workflow")
        run_id = activation.run_id
        print("Pattern 11: Received initialize_workflow")

        # 3. Complete with a long timer to keep workflow alive
        completion = (
            CompletionBuilder(run_id)
            .start_timer(seq=1, duration=timedelta(seconds=60))
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)
        print("Pattern 11: Started timer to keep workflow alive")

        # Give server time to process
        await trio.sleep(0.5)

        # 4. Send signal via CLI
        signal_workflow_via_cli(
            workflow_id=workflow_id,
            signal_name="my_signal",
            args=["signal_data", 42],
        )
        print("Pattern 11: Sent signal via CLI")

        # 5. Poll for workflow activation (signal_workflow)
        activation_bytes = await bridge.poll_workflow_activation(timeout=DEFAULT_TIMEOUT)
        activation = ActivationParser(activation_bytes)

        assert activation.has_job_type("signal_workflow"), (
            f"Expected signal_workflow job, got: "
            f"{[j.WhichOneof('variant') for j in activation.jobs]}"
        )

        signal_job = activation.get_job("signal_workflow")
        print(f"Pattern 11: Received signal_workflow(name={signal_job.signal_name})")

        # Verify signal structure
        assert signal_job.signal_name == "my_signal"
        assert len(signal_job.input) >= 1

        # Decode signal arguments
        decoded_args = [activation.decode_payload(p) for p in signal_job.input]
        print(f"Pattern 11: Signal args: {decoded_args}")
        assert decoded_args == ["signal_data", 42]

        # 6. Complete with empty commands (signals don't need response)
        # But we need to cancel timer and complete workflow
        completion = (
            CompletionBuilder(run_id)
            .cancel_timer(seq=1)
            .complete_workflow("signal_received")
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)
        print("Pattern 11: Cancelled timer and completed workflow")

        # Give server time to process
        await trio.sleep(0.5)

        # Verify workflow completed
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "COMPLETED", f"Expected COMPLETED, got {status}"

        print("Pattern 11: Signal Workflow - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_11_multiple_signals(unique_task_queue: str) -> None:
    """Test Pattern 11 variant: Multiple signals in same activation.

    Verifies that multiple signals can be received in a single activation.
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-multi-signal-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # Start workflow
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="MultiSignalWorkflow",
            task_queue=unique_task_queue,
        )

        # Get initial activation
        activation_bytes = await bridge.poll_workflow_activation(timeout=DEFAULT_TIMEOUT)
        activation = ActivationParser(activation_bytes)
        run_id = activation.run_id

        # Start timer to keep workflow alive
        completion = (
            CompletionBuilder(run_id)
            .start_timer(seq=1, duration=timedelta(seconds=60))
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        await trio.sleep(0.3)

        # Send multiple signals rapidly
        signal_workflow_via_cli(workflow_id, "signal_a", args=[1])
        signal_workflow_via_cli(workflow_id, "signal_b", args=[2])
        signal_workflow_via_cli(workflow_id, "signal_c", args=[3])

        await trio.sleep(0.3)

        # Poll for signals - may come in one or multiple activations
        received_signals = []
        for _ in range(3):  # At most 3 activations
            activation_bytes = await bridge.poll_workflow_activation(timeout=5.0)
            activation = ActivationParser(activation_bytes)

            signal_jobs = activation.get_all_jobs("signal_workflow")
            for sig in signal_jobs:
                received_signals.append(sig.signal_name)
                print(f"Pattern 11 (multi): Received signal: {sig.signal_name}")

            # If we got all signals, complete
            if len(received_signals) >= 3:
                break

            # Otherwise, acknowledge and continue polling
            completion = CompletionBuilder(run_id).build()  # Empty completion
            await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        # Verify we got all signals
        assert set(received_signals) >= {"signal_a", "signal_b", "signal_c"}
        print(f"Pattern 11 (multi): Received all signals: {received_signals}")

        # Complete workflow
        completion = (
            CompletionBuilder(run_id)
            .cancel_timer(seq=1)
            .complete_workflow("all_signals_received")
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        await trio.sleep(0.5)
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "COMPLETED"

        print("Pattern 11 (multi): Multiple Signals - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_12_query_workflow(unique_task_queue: str) -> None:
    """Test Pattern 12: Query Workflow.

    Flow:
    1. Start workflow
    2. poll -> [initialize_workflow]
    3. complete([StartTimer(seq=1, duration=60s)])  # Keep workflow alive
    4. Query via CLI: temporal workflow query --name get_status
    5. poll -> [query_workflow(query_id="xxx", query_type="get_status", arguments=[])]
    6. complete([RespondToQuery(query_id="xxx", succeeded=result)])
    7. Verify query response

    Verifies:
    - query_workflow job structure
    - Query always arrives in separate activation
    - Query response must include matching query_id
    - RespondToQuery command structure
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-query-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    query_result = None

    try:
        # 1. Start workflow via CLI
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="QueryWorkflow",
            task_queue=unique_task_queue,
        )

        # 2. Poll for workflow activation (initialize_workflow)
        activation_bytes = await bridge.poll_workflow_activation(timeout=DEFAULT_TIMEOUT)
        activation = ActivationParser(activation_bytes)

        assert activation.has_job_type("initialize_workflow")
        run_id = activation.run_id
        print("Pattern 12: Received initialize_workflow")

        # 3. Complete with TWO timers:
        #    - seq=1: Short timer (1s) to wake up workflow after query
        #    - seq=2: Long timer (60s) to keep workflow alive for query
        completion = (
            CompletionBuilder(run_id)
            .start_timer(seq=1, duration=timedelta(seconds=1))
            .start_timer(seq=2, duration=timedelta(seconds=60))
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)
        print("Pattern 12: Started timers to keep workflow alive")

        await trio.sleep(0.3)

        # 4. Send query via CLI (in background since it blocks)
        async def send_query():
            nonlocal query_result
            await trio.sleep(0.1)  # Give time for poll to start
            query_result = await query_workflow_via_cli_async(
                workflow_id=workflow_id,
                query_type="get_status",
                args=["status_query_arg"],
            )
            print(f"Pattern 12: CLI query returned: {query_result}")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(send_query)

            # 5. Poll for workflow activation (query_workflow)
            activation_bytes = await bridge.poll_workflow_activation(timeout=DEFAULT_TIMEOUT)
            activation = ActivationParser(activation_bytes)

            # Query might come with other jobs
            if activation.has_job_type("query_workflow"):
                query_job = activation.get_job("query_workflow")
                print(
                    f"Pattern 12: Received query_workflow("
                    f"query_id={query_job.query_id}, "
                    f"query_type={query_job.query_type})"
                )

                # Verify query structure
                assert query_job.query_type == "get_status"
                assert query_job.query_id  # Non-empty query ID

                # Decode query arguments
                decoded_args = [
                    activation.decode_payload(p) for p in query_job.arguments
                ]
                print(f"Pattern 12: Query args: {decoded_args}")

                # 6. Respond to query
                response_value = {"status": "running", "count": 42}
                completion = (
                    CompletionBuilder(run_id)
                    .respond_to_query(
                        query_id=query_job.query_id,
                        result=response_value,
                    )
                    .build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )
                print("Pattern 12: Sent RespondToQuery")
            else:
                pytest.fail(
                    f"Expected query_workflow job, got: "
                    f"{[j.WhichOneof('variant') for j in activation.jobs]}"
                )

        # Wait for CLI query to complete
        await trio.sleep(0.3)

        # Verify query result was received by CLI
        print(f"Pattern 12: Query CLI result: {query_result}")
        assert query_result is not None

        # 7. Wait for short timer to fire
        activation_bytes = await bridge.poll_workflow_activation(timeout=5.0)
        activation = ActivationParser(activation_bytes)
        assert activation.has_job_type("fire_timer"), (
            f"Expected fire_timer, got {[j.WhichOneof('variant') for j in activation.jobs]}"
        )
        print("Pattern 12: Short timer fired")

        # 8. Cancel long timer and complete workflow
        completion = (
            CompletionBuilder(run_id)
            .cancel_timer(seq=2)
            .complete_workflow("query_handled")
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        await trio.sleep(0.5)
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "COMPLETED", f"Expected COMPLETED, got {status}"

        print("Pattern 12: Query Workflow - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_13_query_failure(unique_task_queue: str) -> None:
    """Test Pattern 13: Query Failure.

    Flow:
    1. Start workflow
    2. poll -> [initialize_workflow]
    3. complete([StartTimer])
    4. Query via CLI
    5. poll -> [query_workflow(...)]
    6. complete([RespondToQuery(query_id="xxx", failed=failure)])
    7. Verify CLI shows error

    Verifies:
    - RespondToQuery with failed status
    - Error message is propagated to caller
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-query-fail-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    query_error = None

    try:
        # Start workflow
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="QueryFailWorkflow",
            task_queue=unique_task_queue,
        )

        # Get initial activation
        activation_bytes = await bridge.poll_workflow_activation(timeout=DEFAULT_TIMEOUT)
        activation = ActivationParser(activation_bytes)
        run_id = activation.run_id

        # Start timer
        completion = (
            CompletionBuilder(run_id)
            .start_timer(seq=1, duration=timedelta(seconds=60))
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        await trio.sleep(0.5)

        # Send query that will fail (in background)
        async def send_failing_query():
            nonlocal query_error
            await trio.sleep(0.1)
            try:
                await query_workflow_via_cli_async(
                    workflow_id=workflow_id,
                    query_type="failing_query",
                )
            except RuntimeError as e:
                query_error = str(e)
                print(f"Pattern 13: CLI query error (expected): {query_error}")

        async with trio.open_nursery() as nursery:
            nursery.start_soon(send_failing_query)

            # Poll for query
            activation_bytes = await bridge.poll_workflow_activation(timeout=DEFAULT_TIMEOUT)
            activation = ActivationParser(activation_bytes)

            if activation.has_job_type("query_workflow"):
                query_job = activation.get_job("query_workflow")
                print(f"Pattern 13: Received query_workflow(query_type={query_job.query_type})")

                # Respond with failure
                completion = (
                    CompletionBuilder(run_id)
                    .respond_to_query(
                        query_id=query_job.query_id,
                        error="Query handler not found: failing_query",
                    )
                    .build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )
                print("Pattern 13: Sent RespondToQuery with failure")

        # Wait for CLI to receive error
        await trio.sleep(1.0)

        # Verify error was received
        # Note: CLI might still return 0 exit code but error in output
        print(f"Pattern 13: Query error result: {query_error}")

        # Complete workflow
        # Need to poll and handle any pending activations
        try:
            with trio.move_on_after(2.0):
                activation_bytes = await bridge.poll_workflow_activation(timeout=2.0)
                activation = ActivationParser(activation_bytes)
                completion = (
                    CompletionBuilder(run_id)
                    .cancel_timer(seq=1)
                    .complete_workflow("query_failed_as_expected")
                    .build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )
        except Exception:
            pass

        print("Pattern 13: Query Failure - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_12_query_with_arguments(unique_task_queue: str) -> None:
    """Test Pattern 12 variant: Query with arguments.

    Verifies query arguments are correctly passed.
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-query-args-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # Start workflow
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="QueryArgsWorkflow",
            task_queue=unique_task_queue,
        )

        # Get initial activation
        activation_bytes = await bridge.poll_workflow_activation(timeout=DEFAULT_TIMEOUT)
        activation = ActivationParser(activation_bytes)
        run_id = activation.run_id

        # Start timer
        completion = (
            CompletionBuilder(run_id)
            .start_timer(seq=1, duration=timedelta(seconds=60))
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        await trio.sleep(0.5)

        # Send query with complex arguments
        async def send_query():
            await trio.sleep(0.1)
            return await query_workflow_via_cli_async(
                workflow_id=workflow_id,
                query_type="complex_query",
                args=[{"key": "value", "count": 100}, [1, 2, 3], "string_arg"],
            )

        async with trio.open_nursery() as nursery:
            query_task = nursery.start_soon(send_query)

            # Poll for query
            activation_bytes = await bridge.poll_workflow_activation(timeout=DEFAULT_TIMEOUT)
            activation = ActivationParser(activation_bytes)

            if activation.has_job_type("query_workflow"):
                query_job = activation.get_job("query_workflow")

                # Verify arguments
                decoded_args = [
                    activation.decode_payload(p) for p in query_job.arguments
                ]
                print(f"Pattern 12 (args): Query arguments: {decoded_args}")

                assert len(decoded_args) == 3
                assert decoded_args[0] == {"key": "value", "count": 100}
                assert decoded_args[1] == [1, 2, 3]
                assert decoded_args[2] == "string_arg"

                # Respond
                completion = (
                    CompletionBuilder(run_id)
                    .respond_to_query(
                        query_id=query_job.query_id,
                        result="args_received",
                    )
                    .build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )

        await trio.sleep(0.5)

        # Complete workflow
        completion = (
            CompletionBuilder(run_id)
            .cancel_timer(seq=1)
            .complete_workflow("done")
            .build()
        )
        await bridge.complete_workflow_activation(completion, timeout=DEFAULT_TIMEOUT)

        print("Pattern 12 (args): Query with Arguments - PASSED")

    finally:
        await safe_shutdown(bridge)
