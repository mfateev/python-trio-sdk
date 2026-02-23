"""Unit tests for activity cancellation logic.

These tests use ActivityEnvironment and don't require a Temporal server.
"""

import pytest
import trio

from temporalio_trio import activity
from temporalio_trio.testing import ActivityEnvironment
from temporalio_trio.worker._activity import _RunningActivity

# =============================================================================
# Activity definitions for unit tests
# =============================================================================


@activity.defn
async def catch_cancel_activity() -> str:
    """Activity that catches cancel and returns a value."""
    try:
        while True:
            activity.heartbeat("alive")
            await trio.sleep(1)
    except trio.Cancelled:
        return f"cancelled, is_cancelled={activity.is_cancelled()}"


@activity.defn
async def wait_cancel_activity() -> str:
    """Activity that uses wait_for_cancelled() API."""
    activity.heartbeat("started")
    await activity.wait_for_cancelled()
    return f"wait_done, is_cancelled={activity.is_cancelled()}"


@activity.defn
async def check_is_cancelled_activity() -> bool:
    """Activity that checks is_cancelled flag."""
    await activity.wait_for_cancelled()
    return activity.is_cancelled()


@activity.defn
async def check_worker_shutdown_activity() -> bool:
    """Activity that checks worker shutdown."""
    await activity.wait_for_worker_shutdown()
    return activity.is_worker_shutdown()


@activity.defn
async def heartbeat_collector_activity() -> str:
    """Activity that heartbeats and then returns."""
    activity.heartbeat("beat1")
    activity.heartbeat("beat2")
    return "done"


# =============================================================================
# Unit tests
# =============================================================================


@pytest.mark.trio
async def test_activity_env_cancel_returns_value():
    """Test that cancelled activity can catch and return a value."""
    env = ActivityEnvironment()

    async with trio.open_nursery() as nursery:

        async def cancel_after_start():
            await trio.sleep(0.1)
            env.cancel()

        nursery.start_soon(cancel_after_start)
        result = await env.run(catch_cancel_activity)

    assert result == "cancelled, is_cancelled=True"


@pytest.mark.trio
async def test_activity_env_wait_for_cancelled():
    """Test that wait_for_cancelled() unblocks on cancel."""
    env = ActivityEnvironment()

    async with trio.open_nursery() as nursery:

        async def cancel_after_start():
            await trio.sleep(0.1)
            env.cancel()

        nursery.start_soon(cancel_after_start)
        result = await env.run(wait_cancel_activity)

    assert result == "wait_done, is_cancelled=True"


@pytest.mark.trio
async def test_activity_env_is_cancelled_flag():
    """Test that is_cancelled() returns True after cancel."""
    env = ActivityEnvironment()

    async with trio.open_nursery() as nursery:

        async def cancel_after_start():
            await trio.sleep(0.1)
            env.cancel()

        nursery.start_soon(cancel_after_start)
        result = await env.run(check_is_cancelled_activity)

    assert result is True


@pytest.mark.trio
async def test_activity_env_worker_shutdown_event():
    """Test that wait_for_worker_shutdown() unblocks on shutdown."""
    env = ActivityEnvironment()

    async with trio.open_nursery() as nursery:

        async def shutdown_after_start():
            await trio.sleep(0.1)
            env.worker_shutdown()

        nursery.start_soon(shutdown_after_start)
        result = await env.run(check_worker_shutdown_activity)

    assert result is True


@pytest.mark.trio
async def test_activity_env_heartbeat_callback():
    """Test that heartbeat callback is invoked with details."""
    env = ActivityEnvironment()
    heartbeats: list = []
    env.on_heartbeat = lambda *args: heartbeats.append(args)

    result = await env.run(heartbeat_collector_activity)

    assert result == "done"
    assert len(heartbeats) == 2
    assert heartbeats[0] == ("beat1",)
    assert heartbeats[1] == ("beat2",)


@pytest.mark.trio
async def test_running_activity_cancel_sets_flags():
    """Test that _RunningActivity.cancel() sets cancelled_event and cancels scope."""
    send, receive = trio.open_memory_channel[tuple](100)
    cancelled_event = activity._TrioEvent(trio.Event())
    scope = trio.CancelScope()

    running = _RunningActivity(
        task_token=b"test",
        info=None,  # type: ignore[arg-type]
        cancelled_event=cancelled_event,
        heartbeat_send=send,
        heartbeat_receive=receive,
        cancel_scope=scope,
    )

    assert not cancelled_event.is_set()
    assert not scope.cancel_called

    running.cancel(cancelled_by_request=True)

    assert cancelled_event.is_set()
    assert scope.cancel_called
    assert running.cancelled_by_request is True


@pytest.mark.trio
async def test_running_activity_cancel_noop_when_done():
    """Test that cancel() doesn't cancel scope when done=True."""
    send, receive = trio.open_memory_channel[tuple](100)
    cancelled_event = activity._TrioEvent(trio.Event())
    scope = trio.CancelScope()

    running = _RunningActivity(
        task_token=b"test",
        info=None,  # type: ignore[arg-type]
        cancelled_event=cancelled_event,
        heartbeat_send=send,
        heartbeat_receive=receive,
        cancel_scope=scope,
        done=True,
    )

    running.cancel(cancelled_by_request=True)

    # Event is still set (to update is_cancelled flag)
    assert cancelled_event.is_set()
    # But scope is NOT cancelled because done=True
    assert not scope.cancel_called
