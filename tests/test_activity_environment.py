"""Tests for ActivityEnvironment testing utility."""

import pytest
import trio

from temporalio_trio import activity
from temporalio_trio.testing import ActivityEnvironment


@pytest.mark.trio
async def test_activity_env_async():
    """Test ActivityEnvironment with async activity that handles cancellation.

    Ported from sdk-python: tests/testing/test_activity.py:test_activity_env_async
    """
    heartbeats: list[Any] = []

    @activity.defn
    async def my_activity() -> str:
        activity.heartbeat("beat1")
        try:
            await activity.wait_for_cancelled()
        except trio.Cancelled:
            pass
        activity.heartbeat("cancelled")
        return "done"

    env = ActivityEnvironment()
    env.on_heartbeat = lambda *args: heartbeats.append(args[0] if args else None)

    # We need to run the activity and cancel it after it starts heartbeating
    async with trio.open_nursery() as nursery:

        async def run_and_cancel():
            # Wait a tiny bit for the activity to start and send first heartbeat
            await trio.sleep(0.05)
            env.cancel()

        result_holder: list[str] = []

        async def run_activity():
            result = await env.run(my_activity)
            result_holder.append(result)

        nursery.start_soon(run_and_cancel)
        nursery.start_soon(run_activity)

    assert result_holder[0] == "done"
    assert heartbeats == ["beat1", "cancelled"]


@pytest.mark.trio
async def test_activity_env_info():
    """Test that ActivityEnvironment provides correct info."""

    @activity.defn
    async def info_activity() -> str:
        info = activity.info()
        return f"{info.activity_id}:{info.task_queue}"

    env = ActivityEnvironment()
    result = await env.run(info_activity)
    assert result == "test:test"


@pytest.mark.trio
async def test_activity_env_heartbeat():
    """Test that heartbeats are routed to on_heartbeat callback."""
    heartbeats: list[tuple] = []

    @activity.defn
    async def heartbeat_activity() -> str:
        activity.heartbeat("a", 1)
        activity.heartbeat("b", 2)
        return "ok"

    env = ActivityEnvironment()
    env.on_heartbeat = lambda *args: heartbeats.append(args)

    result = await env.run(heartbeat_activity)
    assert result == "ok"
    assert heartbeats == [("a", 1), ("b", 2)]


@pytest.mark.trio
async def test_activity_env_worker_shutdown():
    """Test worker_shutdown notification."""

    @activity.defn
    async def shutdown_activity() -> bool:
        return activity.is_worker_shutdown()

    env = ActivityEnvironment()

    # Not shutdown yet
    result = await env.run(shutdown_activity)
    assert result is False

    # After shutdown
    env.worker_shutdown()
    result = await env.run(shutdown_activity)
    assert result is True


@pytest.mark.trio
async def test_activity_env_cancel_before_run():
    """Test cancellation that happens before run starts."""

    @activity.defn
    async def check_cancel() -> bool:
        return activity.is_cancelled()

    env = ActivityEnvironment()
    env.cancel()

    # Activity should see cancellation immediately
    # Since cancel_scope.cancel() is called, the activity will get Cancelled
    # Let's test with an activity that catches it
    @activity.defn
    async def catch_cancel() -> str:
        try:
            await trio.sleep(10)
            return "not cancelled"
        except trio.Cancelled:
            return "caught cancel"

    result = await env.run(catch_cancel)
    assert result == "caught cancel"


# Need this for the heartbeats list type hint
from typing import Any  # noqa: E402
