"""E2E tests for @workflow.update with real Temporal server.

Ported from sdk-python's tests/worker/test_workflow.py update tests.
"""

from __future__ import annotations

import uuid
from typing import Optional

import pytest
import trio

from temporalio_trio import workflow
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker

# ============================================================================
# Workflow definitions for E2E tests
# ============================================================================


@workflow.defn
class SimpleUpdateWorkflow:
    """Workflow with a simple sync update handler."""

    def __init__(self) -> None:
        self._value = 0
        self._done = False

    @workflow.run
    async def run(self) -> int:
        await workflow.wait_condition(lambda: self._done)
        return self._value

    @workflow.update
    def set_value(self, value: int) -> int:
        old = self._value
        self._value = value
        return old

    @workflow.signal
    def finish(self) -> None:
        self._done = True

    @workflow.query
    def get_value(self) -> int:
        return self._value


@workflow.defn
class AsyncUpdateWorkflow:
    """Workflow with an async update handler."""

    def __init__(self) -> None:
        self._value = ""
        self._done = False

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._done)
        return self._value

    @workflow.update
    async def append_value(self, suffix: str) -> str:
        self._value += suffix
        return self._value

    @workflow.signal
    def finish(self) -> None:
        self._done = True


@workflow.defn
class ValidatedUpdateWorkflow:
    """Workflow with update validator."""

    def __init__(self) -> None:
        self._value = 0
        self._done = False

    @workflow.run
    async def run(self) -> int:
        await workflow.wait_condition(lambda: self._done)
        return self._value

    @workflow.update
    def set_value(self, value: int) -> int:
        old = self._value
        self._value = value
        return old

    @set_value.validator
    def validate_set_value(self, value: int) -> None:
        if value < 0:
            raise ValueError(f"Value must be non-negative, got {value}")

    @workflow.signal
    def finish(self) -> None:
        self._done = True


@workflow.defn
class MultiUpdateWorkflow:
    """Workflow with multiple update handlers (sync, async, named).

    Ported from sdk-python's UpdateHandlersWorkflow.
    """

    def __init__(self) -> None:
        self._last_event: Optional[str] = None
        self._done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._done)

    @workflow.update
    def last_event(self, an_arg: str) -> str:
        """Sync update handler that tracks last event."""
        if an_arg == "fail":
            raise ValueError("SyncFail")
        le = self._last_event or "<no event>"
        self._last_event = an_arg
        return le

    @last_event.validator
    def last_event_validator(self, an_arg: str) -> None:
        if an_arg == "reject_me":
            raise ValueError("Rejected")

    @workflow.update
    async def last_event_async(self, an_arg: str) -> str:
        """Async update handler that tracks last event."""
        if an_arg == "fail":
            raise ValueError("AsyncFail")
        le = self._last_event or "<no event>"
        self._last_event = an_arg
        return le

    @workflow.update(name="renamed")
    async def async_named(self) -> str:
        """Named update handler."""
        return "named"

    @workflow.signal
    def finish(self) -> None:
        self._done = True


@workflow.defn
class UpdateInfoWorkflow:
    """Workflow that verifies current_update_info() in handlers.

    Ported from sdk-python's CurrentUpdateWorkflow.
    """

    def __init__(self) -> None:
        self._update_ids: list[str] = []
        self._done = False

    @workflow.run
    async def run(self) -> list[str]:
        # Confirm no update info outside handler
        assert workflow.current_update_info() is None
        await workflow.wait_condition(lambda: self._done)
        return self._update_ids

    @workflow.update
    def do_update(self) -> str:
        """Returns the update ID from current_update_info()."""
        info = workflow.current_update_info()
        assert info is not None
        assert info.name == "do_update"
        self._update_ids.append(info.id)
        return info.id

    @do_update.validator
    def do_update_validator(self) -> None:
        info = workflow.current_update_info()
        assert info is not None
        assert info.name == "do_update"

    @workflow.signal
    def finish(self) -> None:
        self._done = True


@workflow.defn
class HandlerFailureWorkflow:
    """Workflow with update handlers that raise errors.

    Ported from sdk-python's UpdateHandlersWorkflow unhappy paths.
    """

    def __init__(self) -> None:
        self._done = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._done)

    @workflow.update
    def sync_fail(self, msg: str) -> str:
        raise ValueError(msg)

    @workflow.update
    async def async_fail(self, msg: str) -> str:
        raise ValueError(msg)

    @workflow.signal
    def finish(self) -> None:
        self._done = True


@workflow.defn
class AllHandlersFinishedWorkflow:
    """Workflow that uses all_handlers_finished().

    Simplified from sdk-python's UnfinishedHandlersWarningsWorkflow.
    Tests that all_handlers_finished() returns True after sync update completes.
    """

    def __init__(self) -> None:
        self._update_count = 0
        self._done = False

    @workflow.run
    async def run(self) -> int:
        await workflow.wait_condition(lambda: self._done)
        # By the time we get here, all sync updates have completed
        assert workflow.all_handlers_finished()
        return self._update_count

    @workflow.update
    def my_update(self, value: int) -> int:
        self._update_count += 1
        return value * 2

    @workflow.signal
    def finish(self) -> None:
        self._done = True


@workflow.defn
class UpdateRespectsRunIdWorkflow:
    """Workflow for testing first_execution_run_id targeting.

    Ported from sdk-python's UpdateRespectsFirstExecutionRunIdWorkflow.
    """

    def __init__(self) -> None:
        self._update_received = False

    @workflow.run
    async def run(self) -> None:
        await workflow.wait_condition(lambda: self._update_received)

    @workflow.update
    def complete_me(self) -> str:
        self._update_received = True
        return "done"


# ============================================================================
# Helper
# ============================================================================


async def _run_with_worker(
    client: Client,
    task_queue: str,
    workflows: list[type],
    test_fn,
) -> None:
    """Run a test function with a worker."""
    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=workflows,
    )
    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)
        await trio.sleep(0)
        try:
            await test_fn()
        finally:
            await worker.shutdown()
            await trio.sleep(0.3)
            nursery.cancel_scope.cancel()


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
async def client():
    """Create a Trio client for testing."""
    client = await Client.connect("localhost:7233", namespace="default")
    yield client
    await client.close()


# ============================================================================
# E2E Tests - Happy Path (ported from test_workflow_update_handlers_happy)
# ============================================================================


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_execute_update(client: Client) -> None:
    """Test sync update handler returns correct values."""
    task_queue = f"test-update-{uuid.uuid4().hex[:8]}"

    async def test():
        handle = await client.start_workflow(
            SimpleUpdateWorkflow,
            id=f"update-test-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )
        old_value = await handle.execute_update("set_value", arg=42)
        assert old_value == 0

        current = await handle.query("get_value")
        assert current == 42

        old_value = await handle.execute_update("set_value", arg=100)
        assert old_value == 42

        await handle.signal("finish")
        result = await handle.result()
        assert result == 100

    await _run_with_worker(client, task_queue, [SimpleUpdateWorkflow], test)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_async_update_handler(client: Client) -> None:
    """Test async update handler returns correct values."""
    task_queue = f"test-async-update-{uuid.uuid4().hex[:8]}"

    async def test():
        handle = await client.start_workflow(
            AsyncUpdateWorkflow,
            id=f"async-update-test-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )
        result1 = await handle.execute_update("append_value", arg="hello")
        assert result1 == "hello"

        result2 = await handle.execute_update("append_value", arg=" world")
        assert result2 == "hello world"

        await handle.signal("finish")
        result = await handle.result()
        assert result == "hello world"

    await _run_with_worker(client, task_queue, [AsyncUpdateWorkflow], test)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_named_update_handler(client: Client) -> None:
    """Test @workflow.update(name='renamed') uses custom name.

    Ported from sdk-python's test_workflow_update_handlers_happy (name overload).
    """
    task_queue = f"test-named-update-{uuid.uuid4().hex[:8]}"

    async def test():
        handle = await client.start_workflow(
            MultiUpdateWorkflow,
            id=f"named-update-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )
        result = await handle.execute_update("renamed")
        assert result == "named"

        await handle.signal("finish")
        await handle.result()

    await _run_with_worker(client, task_queue, [MultiUpdateWorkflow], test)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_multiple_update_handlers(client: Client) -> None:
    """Test workflow with multiple sync and async update handlers.

    Ported from sdk-python's test_workflow_update_handlers_happy.
    """
    task_queue = f"test-multi-update-{uuid.uuid4().hex[:8]}"

    async def test():
        handle = await client.start_workflow(
            MultiUpdateWorkflow,
            id=f"multi-update-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )

        # Sync handler
        last = await handle.execute_update("last_event", arg="val1")
        assert last == "<no event>"

        # Async handler sees previous state
        last = await handle.execute_update("last_event_async", arg="val2")
        assert last == "val1"

        # Sync handler sees async handler's state
        last = await handle.execute_update("last_event", arg="val3")
        assert last == "val2"

        await handle.signal("finish")
        await handle.result()

    await _run_with_worker(client, task_queue, [MultiUpdateWorkflow], test)


# ============================================================================
# E2E Tests - Unhappy Path (ported from test_workflow_update_handlers_unhappy)
# ============================================================================


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_validated_update_accepts(client: Client) -> None:
    """Test update with validator that accepts."""
    task_queue = f"test-valid-update-{uuid.uuid4().hex[:8]}"

    async def test():
        handle = await client.start_workflow(
            ValidatedUpdateWorkflow,
            id=f"valid-update-test-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )
        old_value = await handle.execute_update("set_value", arg=42)
        assert old_value == 0

        await handle.signal("finish")
        result = await handle.result()
        assert result == 42

    await _run_with_worker(client, task_queue, [ValidatedUpdateWorkflow], test)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_validated_update_rejects(client: Client) -> None:
    """Test update with validator that rejects.

    Ported from sdk-python's test_workflow_update_handlers_unhappy (rejection).
    """
    task_queue = f"test-reject-update-{uuid.uuid4().hex[:8]}"

    async def test():
        handle = await client.start_workflow(
            ValidatedUpdateWorkflow,
            id=f"reject-update-test-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )

        with pytest.raises(RuntimeError, match="non-negative"):
            await handle.execute_update("set_value", arg=-1)

        # Valid update should still work after rejection
        old_value = await handle.execute_update("set_value", arg=42)
        assert old_value == 0

        await handle.signal("finish")
        result = await handle.result()
        assert result == 42

    await _run_with_worker(client, task_queue, [ValidatedUpdateWorkflow], test)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_validator_rejection_with_message(client: Client) -> None:
    """Test validator rejection includes the error message.

    Ported from sdk-python's test_workflow_update_handlers_unhappy (reject_me).
    """
    task_queue = f"test-reject-msg-{uuid.uuid4().hex[:8]}"

    async def test():
        handle = await client.start_workflow(
            MultiUpdateWorkflow,
            id=f"reject-msg-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )

        with pytest.raises(RuntimeError, match="Rejected"):
            await handle.execute_update("last_event", arg="reject_me")

        # Workflow is still running, valid updates work
        result = await handle.execute_update("last_event", arg="ok")
        assert result == "<no event>"

        await handle.signal("finish")
        await handle.result()

    await _run_with_worker(client, task_queue, [MultiUpdateWorkflow], test)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_sync_handler_failure(client: Client) -> None:
    """Test sync update handler failure propagates to client.

    Ported from sdk-python's test_workflow_update_handlers_unhappy (SyncFail).
    """
    task_queue = f"test-sync-fail-{uuid.uuid4().hex[:8]}"

    async def test():
        handle = await client.start_workflow(
            HandlerFailureWorkflow,
            id=f"sync-fail-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )

        with pytest.raises(RuntimeError, match="sync error"):
            await handle.execute_update("sync_fail", arg="sync error")

        await handle.signal("finish")
        await handle.result()

    await _run_with_worker(client, task_queue, [HandlerFailureWorkflow], test)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_async_handler_failure(client: Client) -> None:
    """Test async update handler failure propagates to client.

    Ported from sdk-python's test_workflow_update_handlers_unhappy (AsyncFail).
    """
    task_queue = f"test-async-fail-{uuid.uuid4().hex[:8]}"

    async def test():
        handle = await client.start_workflow(
            HandlerFailureWorkflow,
            id=f"async-fail-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )

        with pytest.raises(RuntimeError, match="async error"):
            await handle.execute_update("async_fail", arg="async error")

        await handle.signal("finish")
        await handle.result()

    await _run_with_worker(client, task_queue, [HandlerFailureWorkflow], test)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_undefined_update_handler(client: Client) -> None:
    """Test calling an undefined update handler fails.

    Ported from sdk-python's test_workflow_update_handlers_unhappy (undefined).
    """
    task_queue = f"test-undef-update-{uuid.uuid4().hex[:8]}"

    async def test():
        handle = await client.start_workflow(
            SimpleUpdateWorkflow,
            id=f"undef-update-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )

        with pytest.raises(RuntimeError, match="not found"):
            await handle.execute_update("nonexistent_update", arg="whatever")

        # Workflow still works
        await handle.signal("finish")
        result = await handle.result()
        assert result == 0

    await _run_with_worker(client, task_queue, [SimpleUpdateWorkflow], test)


# ============================================================================
# E2E Tests - current_update_info (ported from test_workflow_current_update)
# ============================================================================


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_current_update_info(client: Client) -> None:
    """Test current_update_info() returns correct metadata in handlers.

    Ported from sdk-python's test_workflow_current_update.
    Verifies that update handlers can access their own update info
    (name and id) via current_update_info().
    """
    task_queue = f"test-update-info-{uuid.uuid4().hex[:8]}"

    async def test():
        handle = await client.start_workflow(
            UpdateInfoWorkflow,
            id=f"update-info-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )

        # Send updates - the handler returns the update ID from current_update_info()
        update_id1 = await handle.execute_update("do_update")
        assert update_id1  # Should be a non-empty string (server-generated ID)

        update_id2 = await handle.execute_update("do_update")
        assert update_id2
        assert update_id1 != update_id2  # Different updates get different IDs

        await handle.signal("finish")
        collected_ids = await handle.result()
        assert len(collected_ids) == 2
        assert update_id1 in collected_ids
        assert update_id2 in collected_ids

    await _run_with_worker(client, task_queue, [UpdateInfoWorkflow], test)


# ============================================================================
# E2E Tests - all_handlers_finished (ported from test_unfinished_update_handler)
# ============================================================================


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_all_handlers_finished(client: Client) -> None:
    """Test all_handlers_finished() returns True after sync updates complete.

    Ported from sdk-python's test_unfinished_update_handler (simplified).
    Verifies that after sync update handlers complete, all_handlers_finished()
    returns True in the workflow run method.
    """
    task_queue = f"test-handlers-finished-{uuid.uuid4().hex[:8]}"

    async def test():
        handle = await client.start_workflow(
            AllHandlersFinishedWorkflow,
            id=f"handlers-finished-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )

        # Send updates
        result1 = await handle.execute_update("my_update", arg=5)
        assert result1 == 10

        result2 = await handle.execute_update("my_update", arg=7)
        assert result2 == 14

        # Finish - the workflow asserts all_handlers_finished() before returning
        await handle.signal("finish")
        count = await handle.result()
        assert count == 2

    await _run_with_worker(client, task_queue, [AllHandlersFinishedWorkflow], test)


# ============================================================================
# E2E Tests - run ID targeting
# (ported from test_workflow_update_respects_first_execution_run_id)
# ============================================================================


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_update_targets_correct_execution(client: Client) -> None:
    """Test that update correctly completes a workflow via update.

    Ported from sdk-python's test_workflow_update_respects_first_execution_run_id.
    Verifies the basic flow: start workflow, send update to complete it,
    get result.
    """
    task_queue = f"test-run-id-{uuid.uuid4().hex[:8]}"

    async def test():
        wf_id = f"run-id-test-{uuid.uuid4().hex[:8]}"
        handle = await client.start_workflow(
            UpdateRespectsRunIdWorkflow,
            id=wf_id,
            task_queue=task_queue,
        )

        # Update completes the workflow
        result = await handle.execute_update("complete_me")
        assert result == "done"

        await handle.result()

    await _run_with_worker(client, task_queue, [UpdateRespectsRunIdWorkflow], test)


# ============================================================================
# Workflow definitions for concurrent update handler tests
# ============================================================================


@workflow.defn
class UpdateAwaitingSignalWorkflow:
    """Workflow where async update handler awaits wait_condition set by signal.

    This is the core deadlock scenario: the update handler calls
    wait_condition(lambda: self._signal_received), which can only become true
    when the main workflow processes a signal. If the update handler blocks
    the _run_workflow loop, the signal can never arrive and the system
    deadlocks.

    Ported from sdk-python's UpdateCompletionIsHonoredWhenAfterWorkflowReturn
    patterns.
    """

    def __init__(self) -> None:
        self._signal_received = False
        self._done = False

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: self._done)
        return "workflow-done"

    @workflow.update
    async def my_update(self) -> str:
        # This wait_condition depends on state set by a signal.
        # If this handler blocks the _run_workflow loop, the signal can never
        # arrive and we deadlock.
        await workflow.wait_condition(lambda: self._signal_received)
        return "update-result"

    @workflow.signal
    def unblock_update(self) -> None:
        self._signal_received = True

    @workflow.signal
    def finish(self) -> None:
        self._done = True


@workflow.defn
class UpdateCompletionAfterWorkflowReturnWorkflow:
    """Update handler completes after main workflow returns.

    The update handler awaits wait_condition(workflow_returned), which becomes
    true when the main workflow's run method returns. The completed command for
    the update must be sent AFTER the workflow completion command, and the
    protocol allows this since accepted and completed responses can span
    different activation completions.

    Ported from sdk-python's
    UpdateCompletionIsHonoredWhenAfterWorkflowReturn1Workflow.
    """

    def __init__(self) -> None:
        self._workflow_returned = False

    @workflow.run
    async def run(self) -> str:
        self._workflow_returned = True
        return "workflow-result"

    @workflow.update
    async def my_update(self) -> str:
        await workflow.wait_condition(lambda: self._workflow_returned)
        return "update-result"


@workflow.defn
class ConcurrentUpdatesWorkflow:
    """Workflow with multiple concurrent async update handlers.

    Demonstrates that multiple async update handlers can run concurrently,
    each waiting on different signals to unblock them.
    """

    def __init__(self) -> None:
        self._results: list[str] = []
        self._unblock_first = False
        self._unblock_second = False
        self._done = False

    @workflow.run
    async def run(self) -> list[str]:
        await workflow.wait_condition(lambda: self._done)
        return self._results

    @workflow.update
    async def first_update(self) -> str:
        await workflow.wait_condition(lambda: self._unblock_first)
        self._results.append("first")
        return "first-done"

    @workflow.update
    async def second_update(self) -> str:
        await workflow.wait_condition(lambda: self._unblock_second)
        self._results.append("second")
        return "second-done"

    @workflow.signal
    def unblock_first(self) -> None:
        self._unblock_first = True

    @workflow.signal
    def unblock_second(self) -> None:
        self._unblock_second = True

    @workflow.signal
    def finish(self) -> None:
        self._done = True


# ============================================================================
# E2E Tests - Concurrent update handlers
# ============================================================================


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_update_handler_awaits_signal(client: Client) -> None:
    """Test async update handler that awaits wait_condition set by a signal.

    This is the core deadlock scenario: previously, the update handler would
    block the _run_workflow loop, preventing the signal from arriving. With
    concurrent update handlers, the handler runs as a nursery task and the
    main workflow loop can process signals.
    """
    task_queue = f"test-update-signal-{uuid.uuid4().hex[:8]}"

    async def test():
        handle = await client.start_workflow(
            UpdateAwaitingSignalWorkflow,
            id=f"update-signal-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )

        # Start the update — it will await wait_condition(signal_received)
        # Use start_update so we don't block waiting for the result
        update_handle = await handle.start_update(
            "my_update",
        )

        # Send signal to unblock the update handler
        await handle.signal("unblock_update")

        # Now get the update result — should succeed without deadlock
        update_result = await update_handle.result()
        assert update_result == "update-result"

        # Finish the workflow
        await handle.signal("finish")
        wf_result = await handle.result()
        assert wf_result == "workflow-done"

    await _run_with_worker(client, task_queue, [UpdateAwaitingSignalWorkflow], test)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_update_completion_after_workflow_return(client: Client) -> None:
    """Test update handler completes after main workflow returns.

    Ported from sdk-python's
    test_update_completion_is_honored_when_after_workflow_return_1.

    The update handler awaits wait_condition(workflow_returned), which becomes
    true only when the main workflow's run method returns. We send the update
    before starting the worker so it's queued server-side and delivered in the
    same workflow task as the start — matching the sdk-python pattern.
    """
    task_queue = f"test-update-after-return-{uuid.uuid4().hex[:8]}"
    wf_id = f"update-after-return-{uuid.uuid4().hex[:8]}"

    # 1. Start the workflow (no worker yet, so it just queues)
    handle = await client.start_workflow(
        UpdateCompletionAfterWorkflowReturnWorkflow,
        id=wf_id,
        task_queue=task_queue,
    )

    # 2. Send the update (queued on server since no worker is processing yet)
    #    Use start_update so we don't block — we'll get the result after starting the worker
    update_handle = await handle.start_update("my_update")

    # 3. Now start the worker — it picks up both the workflow start AND the update
    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[UpdateCompletionAfterWorkflowReturnWorkflow],
    )
    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)
        await trio.sleep(0)
        try:
            # 4. Get the update result
            update_result = await update_handle.result()
            assert update_result == "update-result"

            # 5. Get the workflow result
            wf_result = await handle.result()
            assert wf_result == "workflow-result"
        finally:
            await worker.shutdown()
            await trio.sleep(0.3)
            nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_concurrent_update_handlers(client: Client) -> None:
    """Test multiple concurrent async update handlers.

    Two update handlers are started concurrently, each waiting on different
    signals. They are unblocked in reverse order to verify true concurrency
    (not serialized execution).
    """
    task_queue = f"test-concurrent-updates-{uuid.uuid4().hex[:8]}"

    async def test():
        handle = await client.start_workflow(
            ConcurrentUpdatesWorkflow,
            id=f"concurrent-updates-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )

        # Start both updates — they each await their own wait_condition
        update1_handle = await handle.start_update("first_update")
        update2_handle = await handle.start_update("second_update")

        # Unblock in reverse order — second first, then first
        await handle.signal("unblock_second")
        result2 = await update2_handle.result()
        assert result2 == "second-done"

        await handle.signal("unblock_first")
        result1 = await update1_handle.result()
        assert result1 == "first-done"

        # Finish workflow
        await handle.signal("finish")
        results = await handle.result()
        # Both handlers completed
        assert "first" in results
        assert "second" in results

    await _run_with_worker(client, task_queue, [ConcurrentUpdatesWorkflow], test)
