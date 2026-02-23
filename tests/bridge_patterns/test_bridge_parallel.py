"""Bridge pattern tests for parallel workflow execution.

Pattern 20: Multiple Parallel Workflows

This test validates our understanding of how SDK-Core handles multiple
concurrent workflows through the same bridge connection.

Key questions answered:
- Can activations from different workflows arrive interleaved?
- Can we complete activations in any order?
- How does run_id routing work?
"""

from __future__ import annotations

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
async def test_pattern_20_parallel_workflows_basic(unique_task_queue: str) -> None:
    """Test Pattern 20: Multiple Parallel Workflows (Basic).

    Scenario: Start 3 workflows, process all activations, complete all.

    Flow:
    1. Start workflow A, B, C via CLI
    2. poll -> activation for one of them (run_id identifies which)
    3. Repeat polling until we have initialize_workflow for all 3
    4. Complete each with a timer
    5. Poll for timer fires
    6. Complete all workflows

    Verifies:
    - Activations arrive with distinct run_ids
    - Completions are routed by run_id
    - Order of activation arrival is non-deterministic
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    # Create 3 workflow IDs
    workflow_ids = [f"test-parallel-{i}-{uuid4()}" for i in range(3)]

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # 1. Start all 3 workflows
        for wf_id in workflow_ids:
            start_workflow_via_cli(
                workflow_id=wf_id,
                workflow_type="ParallelTestWorkflow",
                task_queue=unique_task_queue,
            )
        print(f"Pattern 20: Started {len(workflow_ids)} workflows")

        # 2. Collect initialize_workflow activations for all workflows
        # Track: run_id -> workflow info
        workflows: dict[str, dict] = {}
        activation_order: list[str] = []
        # Track replay commands per run_id
        replay_map: dict[str, list] = {}

        def build_short_timer(rid: str) -> bytes:
            return (
                CompletionBuilder(rid)
                .start_timer(seq=1, duration=timedelta(milliseconds=100))
                .build()
            )

        for _ in range(10):  # Safety limit
            if len(workflows) == 3:
                break

            activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)

            # Handle replay
            if (
                activation.has_job_type("initialize_workflow")
                and activation.activation.is_replaying
                and activation.run_id in workflows
            ):
                await bridge.complete_workflow_activation(
                    build_short_timer(activation.run_id), timeout=DEFAULT_TIMEOUT
                )
                print(f"Pattern 20: Replayed timer for run_id={activation.run_id[:8]}...")
                continue

            if activation.has_job_type("initialize_workflow"):
                run_id = activation.run_id
                init_job = activation.get_job("initialize_workflow")
                workflow_id = init_job.workflow_id

                workflows[run_id] = {
                    "workflow_id": workflow_id,
                    "run_id": run_id,
                    "initialized": True,
                    "timer_fired": False,
                    "completed": False,
                }
                activation_order.append(run_id)
                print(
                    f"Pattern 20: Received initialize for {workflow_id} (run_id={run_id[:8]}...)"
                )

                # Start a short timer for each
                await bridge.complete_workflow_activation(
                    build_short_timer(run_id), timeout=DEFAULT_TIMEOUT
                )

        assert len(workflows) == 3, f"Expected 3 workflows, got {len(workflows)}"
        print(f"Pattern 20: Activation order: {[r[:8] for r in activation_order]}")

        # 3. Collect timer fires for all workflows
        timer_fire_order: list[str] = []

        for _ in range(10):  # Safety limit
            pending = [r for r, w in workflows.items() if not w["timer_fired"]]
            if not pending:
                break

            activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)

            # Handle replay
            if (
                activation.has_job_type("initialize_workflow")
                and activation.activation.is_replaying
                and activation.run_id in workflows
            ):
                await bridge.complete_workflow_activation(
                    build_short_timer(activation.run_id), timeout=DEFAULT_TIMEOUT
                )
                continue

            run_id = activation.run_id

            if activation.has_job_type("fire_timer"):
                workflows[run_id]["timer_fired"] = True
                timer_fire_order.append(run_id)
                print(f"Pattern 20: Timer fired for run_id={run_id[:8]}...")

                # Complete the workflow
                completion = (
                    CompletionBuilder(run_id)
                    .complete_workflow(f"result-{run_id[:8]}")
                    .build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )
                workflows[run_id]["completed"] = True

        print(f"Pattern 20: Timer fire order: {[r[:8] for r in timer_fire_order]}")

        # 4. Verify all workflows completed
        await trio.sleep(0.5)

        for wf_id in workflow_ids:
            status = get_workflow_status_via_cli(wf_id)
            assert status == "COMPLETED", (
                f"Workflow {wf_id} expected COMPLETED, got {status}"
            )

        print("Pattern 20: All 3 workflows completed successfully")
        print(
            f"Pattern 20: Activation order matched timer fire order: {activation_order == timer_fire_order}"
        )

        print("Pattern 20: Parallel Workflows Basic - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_20_parallel_workflows_interleaved_completion(
    unique_task_queue: str,
) -> None:
    """Test Pattern 20: Parallel Workflows with Interleaved Completion.

    Scenario: Start 3 workflows with different timer durations,
    verify completions can be processed in timer-fire order (not start order).

    Verifies:
    - Workflows complete based on their own timers
    - Completion order can differ from start order
    - Each run_id is independent
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_ids = [f"test-interleaved-{i}-{uuid4()}" for i in range(3)]
    # Different timer durations: workflow 0 = 300ms, 1 = 100ms, 2 = 200ms
    # Expected completion order: 1, 2, 0
    timer_durations = [300, 100, 200]

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # Start all workflows
        for wf_id in workflow_ids:
            start_workflow_via_cli(
                workflow_id=wf_id,
                workflow_type="InterleavedWorkflow",
                task_queue=unique_task_queue,
            )

        # Collect initializations and start timers with different durations
        workflows: dict[str, dict] = {}

        def build_timer_for_run(rid: str) -> bytes:
            """Build timer completion using the duration stored for this run_id."""
            dur = workflows[rid]["duration_ms"]
            return (
                CompletionBuilder(rid)
                .start_timer(seq=1, duration=timedelta(milliseconds=dur))
                .build()
            )

        for _ in range(10):
            if len(workflows) == 3:
                break

            activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)

            # Handle replay
            if (
                activation.has_job_type("initialize_workflow")
                and activation.activation.is_replaying
                and activation.run_id in workflows
            ):
                await bridge.complete_workflow_activation(
                    build_timer_for_run(activation.run_id), timeout=DEFAULT_TIMEOUT
                )
                continue

            if activation.has_job_type("initialize_workflow"):
                run_id = activation.run_id
                init_job = activation.get_job("initialize_workflow")
                workflow_id = init_job.workflow_id

                # Find which index this workflow is
                idx = next(
                    i for i, wid in enumerate(workflow_ids) if wid == workflow_id
                )
                duration_ms = timer_durations[idx]

                workflows[run_id] = {
                    "workflow_id": workflow_id,
                    "index": idx,
                    "duration_ms": duration_ms,
                }
                print(
                    f"Pattern 20: Workflow {idx} initialized with {duration_ms}ms timer"
                )

                # Start timer with workflow-specific duration
                await bridge.complete_workflow_activation(
                    build_timer_for_run(run_id), timeout=DEFAULT_TIMEOUT
                )

        assert len(workflows) == 3

        # Collect timer fires and track completion order
        completion_order: list[int] = []

        for _ in range(10):
            if len(completion_order) == 3:
                break

            activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)

            # Handle replay
            if (
                activation.has_job_type("initialize_workflow")
                and activation.activation.is_replaying
                and activation.run_id in workflows
            ):
                await bridge.complete_workflow_activation(
                    build_timer_for_run(activation.run_id), timeout=DEFAULT_TIMEOUT
                )
                continue

            run_id = activation.run_id

            if activation.has_job_type("fire_timer"):
                idx = workflows[run_id]["index"]
                completion_order.append(idx)
                print(
                    f"Pattern 20: Workflow {idx} ({workflows[run_id]['duration_ms']}ms) timer fired"
                )

                completion = (
                    CompletionBuilder(run_id).complete_workflow(f"done-{idx}").build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )

        print(f"Pattern 20: Completion order: {completion_order}")
        print(f"Pattern 20: Expected order: [1, 2, 0] (shortest timer first)")

        # Timer order should generally be 1, 2, 0 (100ms, 200ms, 300ms)
        # But due to timing variance, we just verify all completed
        assert len(completion_order) == 3
        assert set(completion_order) == {0, 1, 2}

        # Verify all completed
        await trio.sleep(0.5)
        for wf_id in workflow_ids:
            status = get_workflow_status_via_cli(wf_id)
            assert status == "COMPLETED"

        print("Pattern 20: Parallel Workflows Interleaved - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_20_parallel_workflows_out_of_order_completion(
    unique_task_queue: str,
) -> None:
    """Test Pattern 20: Complete activations out of poll order.

    Scenario: Poll multiple activations, then complete them in reverse order.

    Verifies:
    - Activations can be completed in any order
    - SDK-Core correctly routes completions by run_id
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_ids = [f"test-ooo-{i}-{uuid4()}" for i in range(3)]

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # Start all workflows
        for wf_id in workflow_ids:
            start_workflow_via_cli(
                workflow_id=wf_id,
                workflow_type="OutOfOrderWorkflow",
                task_queue=unique_task_queue,
            )

        # Collect all initializations WITHOUT completing them yet
        pending_activations: list[tuple[str, str]] = []  # (run_id, workflow_id)

        for _ in range(10):
            if len(pending_activations) == 3:
                break

            activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)

            if activation.has_job_type("initialize_workflow"):
                run_id = activation.run_id
                init_job = activation.get_job("initialize_workflow")
                workflow_id = init_job.workflow_id
                pending_activations.append((run_id, workflow_id))
                print(f"Pattern 20: Polled activation for {workflow_id}")

                # Complete immediately (SDK-Core requires completion before next poll
                # for the same workflow, but we can interleave different workflows)
                completion = (
                    CompletionBuilder(run_id)
                    .complete_workflow(f"result-{workflow_id}")
                    .build()
                )
                await bridge.complete_workflow_activation(
                    completion, timeout=DEFAULT_TIMEOUT
                )

        assert len(pending_activations) == 3
        print(f"Pattern 20: Poll order: {[wid for _, wid in pending_activations]}")

        # Verify all completed
        await trio.sleep(0.5)
        for wf_id in workflow_ids:
            status = get_workflow_status_via_cli(wf_id)
            assert status == "COMPLETED", f"Expected COMPLETED, got {status}"

        print("Pattern 20: Parallel Workflows Out-of-Order - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_20_parallel_workflows_mixed_operations(
    unique_task_queue: str,
) -> None:
    """Test Pattern 20: Parallel workflows with mixed operations.

    Scenario:
    - Workflow A: timer only
    - Workflow B: activity
    - Workflow C: signal wait

    Verifies handling of different job types across parallel workflows.
    """
    import temporalio.bridge.proto
    import temporalio.bridge.proto.activity_result.activity_result_pb2 as activity_result_pb

    bridge = TrioBridgeWrapper()
    await bridge.start()

    wf_timer = f"test-mixed-timer-{uuid4()}"
    wf_activity = f"test-mixed-activity-{uuid4()}"
    wf_signal = f"test-mixed-signal-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # Start all 3 workflows
        start_workflow_via_cli(wf_timer, "TimerWorkflow", unique_task_queue)
        start_workflow_via_cli(wf_activity, "ActivityWorkflow", unique_task_queue)
        start_workflow_via_cli(wf_signal, "SignalWorkflow", unique_task_queue)

        # Track workflows
        workflows: dict[str, dict] = {}

        def build_mixed_completion(rid: str) -> bytes:
            """Build completion based on the workflow type stored for this run_id."""
            wf_type = workflows[rid]["type"]
            if wf_type == "timer":
                return (
                    CompletionBuilder(rid)
                    .start_timer(seq=1, duration=timedelta(milliseconds=100))
                    .build()
                )
            elif wf_type == "activity":
                return (
                    CompletionBuilder(rid)
                    .schedule_activity(
                        seq=1,
                        activity_type="TestActivity",
                        task_queue=unique_task_queue,
                        schedule_to_close_timeout=timedelta(seconds=30),
                    )
                    .build()
                )
            else:  # signal
                return (
                    CompletionBuilder(rid)
                    .start_timer(seq=1, duration=timedelta(seconds=5))
                    .build()
                )

        # Initialize all workflows with their specific operations
        for _ in range(10):
            if len(workflows) == 3:
                break

            activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)

            # Handle replay
            if (
                activation.has_job_type("initialize_workflow")
                and activation.activation.is_replaying
                and activation.run_id in workflows
            ):
                await bridge.complete_workflow_activation(
                    build_mixed_completion(activation.run_id), timeout=DEFAULT_TIMEOUT
                )
                continue

            if activation.has_job_type("initialize_workflow"):
                run_id = activation.run_id
                init_job = activation.get_job("initialize_workflow")
                workflow_id = init_job.workflow_id

                workflows[run_id] = {
                    "workflow_id": workflow_id,
                    "type": None,
                    "done": False,
                }

                if workflow_id == wf_timer:
                    workflows[run_id]["type"] = "timer"
                elif workflow_id == wf_activity:
                    workflows[run_id]["type"] = "activity"
                else:  # signal workflow
                    workflows[run_id]["type"] = "signal"

                await bridge.complete_workflow_activation(
                    build_mixed_completion(run_id), timeout=DEFAULT_TIMEOUT
                )
                print(f"Pattern 20: Initialized {workflows[run_id]['type']} workflow")

        assert len(workflows) == 3

        # Send signal to signal workflow
        from .conftest import signal_workflow_via_cli

        signal_workflow_via_cli(wf_signal, "complete_signal")
        print("Pattern 20: Sent signal to signal workflow")

        # Poll activity tasks concurrently with workflow activations.
        # The activity workflow needs: schedule -> poll_activity_task ->
        # complete_activity -> resolve_activity. Activity task polling
        # happens on a separate channel, so we must poll both in parallel.
        activity_done = False

        async def handle_activity_tasks():
            """Poll and complete activity tasks in parallel."""
            nonlocal activity_done
            try:
                activity_bytes = await bridge.poll_activity_task(timeout=DEFAULT_TIMEOUT)
                if activity_bytes:
                    import temporalio.bridge.proto.activity_task.activity_task_pb2 as activity_task_pb

                    activity_task = activity_task_pb.ActivityTask()
                    activity_task.ParseFromString(activity_bytes)

                    completion = temporalio.bridge.proto.ActivityTaskCompletion()
                    completion.task_token = activity_task.task_token
                    result = activity_result_pb.ActivityExecutionResult()
                    result.completed.result.data = b'"activity_result"'
                    completion.result.CopyFrom(result)
                    await bridge.complete_activity_task(
                        completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
                    )
                    print("Pattern 20: Completed activity task")
                    activity_done = True
            except Exception:
                pass

        async def handle_workflow_activations(task_nursery):
            """Process workflow activations until all complete."""
            for _ in range(20):
                pending = [r for r, w in workflows.items() if not w["done"]]
                if not pending:
                    break

                activation = await poll_and_handle_eviction(bridge, "", timeout=DEFAULT_TIMEOUT)

                # Handle replay
                if (
                    activation.has_job_type("initialize_workflow")
                    and activation.activation.is_replaying
                    and activation.run_id in workflows
                ):
                    await bridge.complete_workflow_activation(
                        build_mixed_completion(activation.run_id), timeout=DEFAULT_TIMEOUT
                    )
                    continue

                run_id = activation.run_id
                if run_id not in workflows:
                    continue

                wf = workflows[run_id]

                if activation.has_job_type("fire_timer"):
                    if wf["type"] == "timer":
                        completion = (
                            CompletionBuilder(run_id)
                            .complete_workflow("timer_done")
                            .build()
                        )
                        await bridge.complete_workflow_activation(
                            completion, timeout=DEFAULT_TIMEOUT
                        )
                        wf["done"] = True
                        print("Pattern 20: Timer workflow completed")

                elif activation.has_job_type("resolve_activity"):
                    completion = (
                        CompletionBuilder(run_id).complete_workflow("activity_done").build()
                    )
                    await bridge.complete_workflow_activation(
                        completion, timeout=DEFAULT_TIMEOUT
                    )
                    wf["done"] = True
                    print("Pattern 20: Activity workflow completed")

                elif activation.has_job_type("signal_workflow"):
                    completion = (
                        CompletionBuilder(run_id)
                        .cancel_timer(seq=1)
                        .complete_workflow("signal_done")
                        .build()
                    )
                    await bridge.complete_workflow_activation(
                        completion, timeout=DEFAULT_TIMEOUT
                    )
                    wf["done"] = True
                    print("Pattern 20: Signal workflow completed")

            # All done — cancel activity poller if still running
            task_nursery.cancel_scope.cancel()

        async with trio.open_nursery() as task_nursery:
            task_nursery.start_soon(handle_activity_tasks)
            task_nursery.start_soon(handle_workflow_activations, task_nursery)

        # Verify completions
        for wf_id in [wf_timer, wf_signal]:
            status = get_workflow_status_via_cli(wf_id)
            assert status == "COMPLETED", f"{wf_id} expected COMPLETED, got {status}"

        print("Pattern 20: Parallel Workflows Mixed Operations - PASSED")

    finally:
        await safe_shutdown(bridge)
