"""E2E tests for schedule operations."""

import uuid
from datetime import timedelta

import pytest
import trio

from temporalio_trio import workflow
from temporalio_trio.client import (
    Client,
    Schedule,
    ScheduleActionStartWorkflow,
    ScheduleAlreadyRunningError,
    ScheduleHandle,
    ScheduleIntervalSpec,
    ScheduleOverlapPolicy,
    SchedulePolicy,
    ScheduleSpec,
    ScheduleState,
    ScheduleUpdate,
    ScheduleUpdateInput,
)
from temporalio_trio.worker import Worker


@workflow.defn
class ScheduleTargetWorkflow:
    """Simple workflow that schedules can trigger."""

    @workflow.run
    async def run(self) -> str:
        return "scheduled-result"


@pytest.fixture
async def client():
    c = await Client.connect("localhost:7233", namespace="default")
    yield c
    await c.close()


@pytest.fixture
async def worker(client):
    task_queue = f"schedule-test-{uuid.uuid4()}"

    w = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[ScheduleTargetWorkflow],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(w.run)
        yield task_queue
        await w.shutdown()
        nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_schedule_create_describe_delete(client, worker):
    """Test creating, describing, and deleting a schedule."""
    task_queue = worker
    schedule_id = f"test-sched-{uuid.uuid4()}"

    handle = await client.create_schedule(
        schedule_id,
        Schedule(
            action=ScheduleActionStartWorkflow(
                workflow="ScheduleTargetWorkflow",
                id=f"sched-wf-{uuid.uuid4()}",
                task_queue=task_queue,
            ),
            spec=ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(hours=1))],
            ),
            state=ScheduleState(paused=True),
        ),
    )

    assert isinstance(handle, ScheduleHandle)
    assert handle.id == schedule_id

    # Describe
    desc = await handle.describe()
    assert desc.id == schedule_id
    assert desc.schedule.state.paused is True
    assert isinstance(desc.schedule.action, ScheduleActionStartWorkflow)
    assert desc.schedule.action.workflow == "ScheduleTargetWorkflow"

    # Delete
    await handle.delete()

    # Verify deleted - describe should fail
    with pytest.raises(RuntimeError):
        await handle.describe()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_schedule_pause_unpause(client, worker):
    """Test pausing and unpausing a schedule."""
    task_queue = worker
    schedule_id = f"test-pause-{uuid.uuid4()}"

    handle = await client.create_schedule(
        schedule_id,
        Schedule(
            action=ScheduleActionStartWorkflow(
                workflow="ScheduleTargetWorkflow",
                id=f"sched-wf-{uuid.uuid4()}",
                task_queue=task_queue,
            ),
            spec=ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(hours=1))],
            ),
        ),
    )

    try:
        # Pause
        await handle.pause(note="Test pause")
        desc = await handle.describe()
        assert desc.schedule.state.paused is True

        # Unpause
        await handle.unpause(note="Test unpause")
        desc = await handle.describe()
        assert desc.schedule.state.paused is False
    finally:
        await handle.delete()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_schedule_update(client, worker):
    """Test updating a schedule."""
    task_queue = worker
    schedule_id = f"test-update-{uuid.uuid4()}"

    handle = await client.create_schedule(
        schedule_id,
        Schedule(
            action=ScheduleActionStartWorkflow(
                workflow="ScheduleTargetWorkflow",
                id=f"sched-wf-{uuid.uuid4()}",
                task_queue=task_queue,
            ),
            spec=ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(hours=1))],
            ),
        ),
    )

    try:
        # Update the spec
        def updater(input: ScheduleUpdateInput) -> ScheduleUpdate:
            sched = input.description.schedule
            sched.spec = ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(hours=2))],
            )
            return ScheduleUpdate(schedule=sched)

        await handle.update(updater)

        desc = await handle.describe()
        assert len(desc.schedule.spec.intervals) == 1
        assert desc.schedule.spec.intervals[0].every == timedelta(hours=2)
    finally:
        await handle.delete()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_schedule_trigger(client, worker):
    """Test triggering a schedule immediately."""
    task_queue = worker
    schedule_id = f"test-trigger-{uuid.uuid4()}"

    handle = await client.create_schedule(
        schedule_id,
        Schedule(
            action=ScheduleActionStartWorkflow(
                workflow="ScheduleTargetWorkflow",
                id=f"sched-wf-{uuid.uuid4()}",
                task_queue=task_queue,
            ),
            spec=ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(hours=24))],
            ),
        ),
    )

    try:
        await handle.trigger()
        # Give it a moment to execute
        await trio.sleep(2.0)

        desc = await handle.describe()
        assert desc.info.num_actions >= 1
    finally:
        await handle.delete()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_schedule_already_running(client, worker):
    """Test that creating a duplicate schedule raises ScheduleAlreadyRunningError."""
    task_queue = worker
    schedule_id = f"test-dup-{uuid.uuid4()}"

    schedule = Schedule(
        action=ScheduleActionStartWorkflow(
            workflow="ScheduleTargetWorkflow",
            id=f"sched-wf-{uuid.uuid4()}",
            task_queue=task_queue,
        ),
        spec=ScheduleSpec(
            intervals=[ScheduleIntervalSpec(every=timedelta(hours=1))],
        ),
        state=ScheduleState(paused=True),
    )

    handle = await client.create_schedule(schedule_id, schedule)

    try:
        with pytest.raises(ScheduleAlreadyRunningError):
            await client.create_schedule(schedule_id, schedule)
    finally:
        await handle.delete()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_list_schedules(client, worker):
    """Test listing schedules."""
    task_queue = worker
    schedule_id = f"test-list-{uuid.uuid4()}"

    handle = await client.create_schedule(
        schedule_id,
        Schedule(
            action=ScheduleActionStartWorkflow(
                workflow="ScheduleTargetWorkflow",
                id=f"sched-wf-{uuid.uuid4()}",
                task_queue=task_queue,
            ),
            spec=ScheduleSpec(
                intervals=[ScheduleIntervalSpec(every=timedelta(hours=1))],
            ),
            state=ScheduleState(paused=True),
        ),
    )

    try:
        # Give server a moment to index
        await trio.sleep(1.0)

        entries = client.list_schedules()
        # At least our schedule should be in the list
        ids = [e.id async for e in entries]
        assert schedule_id in ids
    finally:
        await handle.delete()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_get_schedule_handle(client):
    """Test getting a handle to an existing schedule."""
    handle = client.get_schedule_handle("some-id")
    assert isinstance(handle, ScheduleHandle)
    assert handle.id == "some-id"
