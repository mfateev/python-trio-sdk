"""Unit tests for start_child_workflow behavior.

These tests validate that start_child_workflow returns immediately after
the child starts (not after completion), allowing the parent to signal
the child before it completes.

This test file was added after fixing a bug where start_child_workflow
was waiting for the child to complete instead of just waiting for it to start.

NOTE: The most comprehensive validation is in the E2E tests
(test_e2e_child_workflow_signal*.py) which test the complete workflow
with a real Temporal server.
"""

import pytest
import trio

from temporalio_trio.worker._runtime import WorkflowRuntime


def _setup_outbound(runtime: WorkflowRuntime) -> None:
    """Set up a minimal terminal outbound interceptor on the runtime."""
    from temporalio_trio.worker._single_thread_worker import _WorkflowOutboundImpl

    runtime.outbound_interceptor = _WorkflowOutboundImpl(runtime)


class TestStartChildWorkflowBehavior:
    """Tests for start_child_workflow returning immediately after start."""

    @pytest.mark.trio
    async def test_handle_first_execution_run_id_set_after_start(self) -> None:
        """Test that handle has first_execution_run_id set after child starts."""
        import random as random_module

        runtime = WorkflowRuntime(
            workflow_id="parent",
            workflow_type="Parent",
            run_id="run-1",
            task_queue="queue",
            random=random_module.Random(42),
            time_ns=0,
        )
        _setup_outbound(runtime)

        async def parent():
            handle = await runtime.workflow_start_child_workflow(
                "Child",
                id="child-1",
                task_queue="queue",
                result_type=None,
                cancellation_type=None,
                parent_close_policy=None,
                execution_timeout=None,
                run_timeout=None,
                task_timeout=None,
                id_reuse_policy=None,
                retry_policy=None,
            )

            # Verify run_id is set immediately after start
            assert handle.first_execution_run_id == "child-run-1"
            return "done"

        async def simulate():
            await trio.sleep(0.05)
            runtime.apply_child_workflow_started(seq=1, run_id="child-run-1")
            await trio.sleep(0.05)
            runtime.apply_child_workflow_resolved(seq=1, result="child-done")

        runtime.on_suspend = lambda: None

        async with trio.open_nursery() as nursery:
            nursery.start_soon(parent)
            nursery.start_soon(simulate)

    @pytest.mark.trio
    async def test_replay_path_handle_has_run_id_immediately(self) -> None:
        """Test that during replay, handle has run_id set immediately."""
        import random as random_module

        runtime = WorkflowRuntime(
            workflow_id="parent",
            workflow_type="Parent",
            run_id="run-1",
            task_queue="queue",
            random=random_module.Random(42),
            time_ns=0,
        )
        _setup_outbound(runtime)

        # Simulate replay: child already started and completed
        runtime.started_children[1] = "child-run-replay"
        runtime.completed_children[1] = "replay_result"

        handle = await runtime.workflow_start_child_workflow(
            "Child",
            id="child-1",
            task_queue="queue",
            result_type=None,
            cancellation_type=None,
            parent_close_policy=None,
            execution_timeout=None,
            run_timeout=None,
            task_timeout=None,
            id_reuse_policy=None,
            retry_policy=None,
        )

        # In replay, handle should have run_id immediately
        assert handle.first_execution_run_id == "child-run-replay"

    @pytest.mark.trio
    async def test_start_child_creates_start_event_not_completion_event(self) -> None:
        """Test that start_child_workflow waits for start event, not completion."""
        import random as random_module

        runtime = WorkflowRuntime(
            workflow_id="parent",
            workflow_type="Parent",
            run_id="run-1",
            task_queue="queue",
            random=random_module.Random(42),
            time_ns=0,
        )
        _setup_outbound(runtime)

        async def parent():
            # Start child should create a start event
            handle = await runtime.workflow_start_child_workflow(
                "Child",
                id="child-1",
                task_queue="queue",
                result_type=None,
                cancellation_type=None,
                parent_close_policy=None,
                execution_timeout=None,
                run_timeout=None,
                task_timeout=None,
                id_reuse_policy=None,
                retry_policy=None,
            )

            # After starting, the child should NOT be in pending_child_starts anymore
            assert 1 not in runtime.pending_child_starts, (
                "Child should not be in pending_child_starts after starting"
            )

            # But child should still be in pending_children (waiting for completion)
            assert 1 in runtime.pending_children, (
                "Child should be in pending_children (waiting for completion)"
            )

            return handle

        async def simulate():
            await trio.sleep(0.05)
            # Simulate child starting
            runtime.apply_child_workflow_started(seq=1, run_id="child-run-1")
            await trio.sleep(0.05)
            # Don't complete yet - just verify parent got the handle

        runtime.on_suspend = lambda: None

        async with trio.open_nursery() as nursery:
            nursery.start_soon(parent)
            nursery.start_soon(simulate)
