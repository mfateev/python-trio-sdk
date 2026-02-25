"""E2E tests for @workflow.update with real Temporal server."""

from __future__ import annotations

import time
import uuid

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


# ============================================================================
# E2E Tests
# ============================================================================


@pytest.fixture
async def client():
    """Create a Trio client for testing."""
    client = await Client.connect("localhost:7233", namespace="default")
    yield client
    await client.close()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_execute_update(client: Client) -> None:
    """Test sending an update and getting a result."""
    task_queue = f"test-update-{uuid.uuid4().hex[:8]}"

    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[SimpleUpdateWorkflow],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)
        await trio.sleep(0)

        # Start workflow
        handle = await client.start_workflow(
            SimpleUpdateWorkflow,
            id=f"update-test-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )

        # Send update and get result
        old_value = await handle.execute_update("set_value", arg=42)
        assert old_value == 0

        # Verify state changed
        current = await handle.query("get_value")
        assert current == 42

        # Send another update
        old_value = await handle.execute_update("set_value", arg=100)
        assert old_value == 42

        # Finish workflow
        await handle.signal("finish")
        result = await handle.result()
        assert result == 100

        # Shutdown
        await worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_async_update_handler(client: Client) -> None:
    """Test async update handler."""
    task_queue = f"test-async-update-{uuid.uuid4().hex[:8]}"

    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[AsyncUpdateWorkflow],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)
        await trio.sleep(0)

        handle = await client.start_workflow(
            AsyncUpdateWorkflow,
            id=f"async-update-test-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )

        # Send updates
        result1 = await handle.execute_update("append_value", arg="hello")
        assert result1 == "hello"

        result2 = await handle.execute_update("append_value", arg=" world")
        assert result2 == "hello world"

        # Finish
        await handle.signal("finish")
        result = await handle.result()
        assert result == "hello world"

        await worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_validated_update_accepts(client: Client) -> None:
    """Test update with validator that accepts."""
    task_queue = f"test-valid-update-{uuid.uuid4().hex[:8]}"

    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[ValidatedUpdateWorkflow],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)
        await trio.sleep(0)

        handle = await client.start_workflow(
            ValidatedUpdateWorkflow,
            id=f"valid-update-test-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )

        # Valid update
        old_value = await handle.execute_update("set_value", arg=42)
        assert old_value == 0

        # Finish
        await handle.signal("finish")
        result = await handle.result()
        assert result == 42

        await worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_validated_update_rejects(client: Client) -> None:
    """Test update with validator that rejects."""
    task_queue = f"test-reject-update-{uuid.uuid4().hex[:8]}"

    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[ValidatedUpdateWorkflow],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)
        await trio.sleep(0)

        handle = await client.start_workflow(
            ValidatedUpdateWorkflow,
            id=f"reject-update-test-{uuid.uuid4().hex[:8]}",
            task_queue=task_queue,
        )

        # Invalid update should raise
        with pytest.raises(RuntimeError, match="non-negative"):
            await handle.execute_update("set_value", arg=-1)

        # Valid update should still work
        old_value = await handle.execute_update("set_value", arg=42)
        assert old_value == 0

        # Finish
        await handle.signal("finish")
        result = await handle.result()
        assert result == 42

        await worker.shutdown()
        await trio.sleep(0.3)
        nursery.cancel_scope.cancel()
