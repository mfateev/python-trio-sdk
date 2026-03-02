"""Schedule types and handle for Temporal schedules."""

from __future__ import annotations

import inspect
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Awaitable, Optional, Union

import temporalio.common
import temporalio.converter
import temporalio.exceptions
from google.protobuf.duration_pb2 import Duration
from google.protobuf.timestamp_pb2 import Timestamp
from temporalio.api.common.v1 import Payload, Payloads, WorkflowExecution, WorkflowType
from temporalio.api.enums.v1 import ScheduleOverlapPolicy as _ProtoScheduleOverlapPolicy
from temporalio.api.schedule.v1 import (
    BackfillRequest,
    IntervalSpec,
    Range,
    SchedulePatch,
    SchedulePolicies,
    StructuredCalendarSpec,
    TriggerImmediatelyRequest,
)
from temporalio.api.schedule.v1 import Schedule as _ProtoSchedule
from temporalio.api.schedule.v1 import (
    ScheduleAction as _ProtoScheduleAction,
)
from temporalio.api.schedule.v1 import (
    ScheduleSpec as _ProtoScheduleSpec,
)
from temporalio.api.schedule.v1 import (
    ScheduleState as _ProtoScheduleState,
)
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflow.v1 import NewWorkflowExecutionInfo
from temporalio.api.workflowservice.v1 import (
    CreateScheduleRequest,
    CreateScheduleResponse,
    DeleteScheduleRequest,
    DescribeScheduleRequest,
    DescribeScheduleResponse,
    ListSchedulesRequest,
    ListSchedulesResponse,
    PatchScheduleRequest,
    UpdateScheduleRequest,
)
from temporalio.converter import DataConverter

if TYPE_CHECKING:
    from ._client import Client


# --- Enums ---


class ScheduleOverlapPolicy(IntEnum):
    """How to handle overlapping schedule actions."""

    SKIP = int(_ProtoScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_SKIP)
    BUFFER_ONE = int(_ProtoScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_BUFFER_ONE)
    BUFFER_ALL = int(_ProtoScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_BUFFER_ALL)
    CANCEL_OTHER = int(_ProtoScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_CANCEL_OTHER)
    TERMINATE_OTHER = int(
        _ProtoScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_TERMINATE_OTHER
    )
    ALLOW_ALL = int(_ProtoScheduleOverlapPolicy.SCHEDULE_OVERLAP_POLICY_ALLOW_ALL)


# --- Spec types ---


@dataclass(frozen=True)
class ScheduleRange:
    """Range for schedule calendar fields."""

    start: int
    end: int = 0
    step: int = 0

    def _to_proto(self) -> Range:
        return Range(start=self.start, end=self.end, step=self.step)

    @staticmethod
    def _from_proto(r: Range) -> ScheduleRange:
        return ScheduleRange(start=r.start, end=r.end, step=r.step)


@dataclass
class ScheduleCalendarSpec:
    """Calendar-based schedule specification."""

    second: Sequence[ScheduleRange] = (ScheduleRange(0),)
    minute: Sequence[ScheduleRange] = (ScheduleRange(0),)
    hour: Sequence[ScheduleRange] = (ScheduleRange(0),)
    day_of_month: Sequence[ScheduleRange] = (ScheduleRange(1, 31),)
    month: Sequence[ScheduleRange] = (ScheduleRange(1, 12),)
    year: Sequence[ScheduleRange] = ()
    day_of_week: Sequence[ScheduleRange] = (ScheduleRange(0, 6),)
    comment: str | None = None

    def _to_proto(self) -> StructuredCalendarSpec:
        return StructuredCalendarSpec(
            second=[r._to_proto() for r in self.second],
            minute=[r._to_proto() for r in self.minute],
            hour=[r._to_proto() for r in self.hour],
            day_of_month=[r._to_proto() for r in self.day_of_month],
            month=[r._to_proto() for r in self.month],
            year=[r._to_proto() for r in self.year],
            day_of_week=[r._to_proto() for r in self.day_of_week],
            comment=self.comment or "",
        )

    @staticmethod
    def _from_proto(s: StructuredCalendarSpec) -> ScheduleCalendarSpec:
        return ScheduleCalendarSpec(
            second=tuple(ScheduleRange._from_proto(r) for r in s.second),
            minute=tuple(ScheduleRange._from_proto(r) for r in s.minute),
            hour=tuple(ScheduleRange._from_proto(r) for r in s.hour),
            day_of_month=tuple(ScheduleRange._from_proto(r) for r in s.day_of_month),
            month=tuple(ScheduleRange._from_proto(r) for r in s.month),
            year=tuple(ScheduleRange._from_proto(r) for r in s.year),
            day_of_week=tuple(ScheduleRange._from_proto(r) for r in s.day_of_week),
            comment=s.comment or None,
        )


@dataclass
class ScheduleIntervalSpec:
    """Interval-based schedule specification."""

    every: timedelta
    offset: timedelta | None = None

    def _to_proto(self) -> IntervalSpec:
        interval = Duration()
        interval.FromTimedelta(self.every)
        phase = None
        if self.offset is not None:
            phase = Duration()
            phase.FromTimedelta(self.offset)
        return IntervalSpec(interval=interval, phase=phase)

    @staticmethod
    def _from_proto(s: IntervalSpec) -> ScheduleIntervalSpec:
        return ScheduleIntervalSpec(
            every=s.interval.ToTimedelta(),
            offset=s.phase.ToTimedelta() if s.HasField("phase") else None,
        )


@dataclass
class ScheduleSpec:
    """When a schedule should trigger."""

    calendars: Sequence[ScheduleCalendarSpec] = ()
    intervals: Sequence[ScheduleIntervalSpec] = ()
    cron_expressions: Sequence[str] = ()
    skip: Sequence[ScheduleCalendarSpec] = ()
    start_at: datetime | None = None
    end_at: datetime | None = None
    jitter: timedelta | None = None
    time_zone_name: str | None = None

    def _to_proto(self) -> _ProtoScheduleSpec:
        start_time = None
        if self.start_at is not None:
            start_time = Timestamp()
            start_time.FromDatetime(self.start_at)
        end_time = None
        if self.end_at is not None:
            end_time = Timestamp()
            end_time.FromDatetime(self.end_at)
        jitter = None
        if self.jitter is not None:
            jitter = Duration()
            jitter.FromTimedelta(self.jitter)
        return _ProtoScheduleSpec(
            structured_calendar=[c._to_proto() for c in self.calendars],
            interval=[i._to_proto() for i in self.intervals],
            cron_string=list(self.cron_expressions),
            exclude_structured_calendar=[s._to_proto() for s in self.skip],
            start_time=start_time,
            end_time=end_time,
            jitter=jitter,
            timezone_name=self.time_zone_name or "",
        )

    @staticmethod
    def _from_proto(s: _ProtoScheduleSpec) -> ScheduleSpec:
        start_at = None
        if s.HasField("start_time") and s.start_time.seconds > 0:
            start_at = s.start_time.ToDatetime().replace(tzinfo=timezone.utc)
        end_at = None
        if s.HasField("end_time") and s.end_time.seconds > 0:
            end_at = s.end_time.ToDatetime().replace(tzinfo=timezone.utc)
        jitter = None
        if s.HasField("jitter") and (s.jitter.seconds > 0 or s.jitter.nanos > 0):
            jitter = s.jitter.ToTimedelta()
        return ScheduleSpec(
            calendars=tuple(
                ScheduleCalendarSpec._from_proto(c) for c in s.structured_calendar
            ),
            intervals=tuple(ScheduleIntervalSpec._from_proto(i) for i in s.interval),
            cron_expressions=tuple(s.cron_string),
            skip=tuple(
                ScheduleCalendarSpec._from_proto(c)
                for c in s.exclude_structured_calendar
            ),
            start_at=start_at,
            end_at=end_at,
            jitter=jitter,
            time_zone_name=s.timezone_name or None,
        )


# --- Action types ---


class ScheduleAction(ABC):
    """Base class for schedule actions."""

    @abstractmethod
    async def _to_proto(self, client: Client) -> _ProtoScheduleAction: ...

    @staticmethod
    def _from_proto(action: _ProtoScheduleAction) -> ScheduleAction:
        if action.HasField("start_workflow"):
            return ScheduleActionStartWorkflow._from_raw_proto(action.start_workflow)
        raise ValueError("Unknown schedule action type")


@dataclass
class ScheduleActionStartWorkflow(ScheduleAction):
    """Schedule action that starts a workflow."""

    workflow: str
    args: Sequence[Any] = ()
    id: str = ""
    task_queue: str = ""
    execution_timeout: timedelta | None = None
    run_timeout: timedelta | None = None
    task_timeout: timedelta | None = None
    retry_policy: temporalio.common.RetryPolicy | None = None
    memo: Mapping[str, Any] | None = None
    search_attributes: temporalio.common.SearchAttributes | dict[str, Any] | None = None

    async def _to_proto(self, client: Client) -> _ProtoScheduleAction:
        dc = client.data_converter

        # Encode args
        input_payloads = None
        if self.args:
            payloads_list = await dc.encode(list(self.args))
            input_payloads = Payloads(payloads=payloads_list)

        wf_info = NewWorkflowExecutionInfo(
            workflow_id=self.id,
            workflow_type=WorkflowType(name=self.workflow),
            task_queue=TaskQueue(name=self.task_queue),
            input=input_payloads,
        )

        # Timeouts
        if self.execution_timeout is not None:
            d = Duration()
            d.FromTimedelta(self.execution_timeout)
            wf_info.workflow_execution_timeout.CopyFrom(d)
        if self.run_timeout is not None:
            d = Duration()
            d.FromTimedelta(self.run_timeout)
            wf_info.workflow_run_timeout.CopyFrom(d)
        if self.task_timeout is not None:
            d = Duration()
            d.FromTimedelta(self.task_timeout)
            wf_info.workflow_task_timeout.CopyFrom(d)

        # Retry policy
        if self.retry_policy is not None:
            self.retry_policy.apply_to_proto(wf_info.retry_policy)

        # Memo
        if self.memo is not None:
            for k, v in self.memo.items():
                if isinstance(v, Payload):
                    wf_info.memo.fields[k].CopyFrom(v)
                else:
                    wf_info.memo.fields[k].CopyFrom((await dc.encode([v]))[0])

        # Search attributes
        if self.search_attributes is not None:
            temporalio.converter.encode_search_attributes(
                self.search_attributes, wf_info.search_attributes
            )

        return _ProtoScheduleAction(start_workflow=wf_info)

    @staticmethod
    def _from_raw_proto(
        raw_info: NewWorkflowExecutionInfo,
    ) -> ScheduleActionStartWorkflow:
        execution_timeout = None
        if raw_info.HasField("workflow_execution_timeout"):
            execution_timeout = raw_info.workflow_execution_timeout.ToTimedelta()
        run_timeout = None
        if raw_info.HasField("workflow_run_timeout"):
            run_timeout = raw_info.workflow_run_timeout.ToTimedelta()
        task_timeout = None
        if raw_info.HasField("workflow_task_timeout"):
            task_timeout = raw_info.workflow_task_timeout.ToTimedelta()

        retry_policy = None
        if raw_info.HasField("retry_policy"):
            retry_policy = temporalio.common.RetryPolicy.from_proto(
                raw_info.retry_policy
            )

        # Args are stored as raw payloads
        args: list[Any] = []
        if raw_info.HasField("input") and raw_info.input.payloads:
            args = list(raw_info.input.payloads)

        # Memo as raw payloads
        memo: dict[str, Any] | None = None
        if raw_info.memo.fields:
            memo = dict(raw_info.memo.fields)

        return ScheduleActionStartWorkflow(
            workflow=raw_info.workflow_type.name,
            args=args,
            id=raw_info.workflow_id,
            task_queue=raw_info.task_queue.name,
            execution_timeout=execution_timeout,
            run_timeout=run_timeout,
            task_timeout=task_timeout,
            retry_policy=retry_policy,
            memo=memo,
        )


# --- Policy / State ---


@dataclass
class SchedulePolicy:
    """Schedule execution policy."""

    overlap: ScheduleOverlapPolicy = ScheduleOverlapPolicy.SKIP
    catchup_window: timedelta = field(default_factory=lambda: timedelta(days=365))
    pause_on_failure: bool = False

    def _to_proto(self) -> SchedulePolicies:
        cw = Duration()
        cw.FromTimedelta(self.catchup_window)
        return SchedulePolicies(
            overlap_policy=int(self.overlap),
            catchup_window=cw,
            pause_on_failure=self.pause_on_failure,
        )

    @staticmethod
    def _from_proto(p: SchedulePolicies) -> SchedulePolicy:
        return SchedulePolicy(
            overlap=ScheduleOverlapPolicy(p.overlap_policy)
            if p.overlap_policy
            else ScheduleOverlapPolicy.SKIP,
            catchup_window=p.catchup_window.ToTimedelta()
            if p.HasField("catchup_window")
            else timedelta(days=365),
            pause_on_failure=p.pause_on_failure,
        )


@dataclass
class ScheduleState:
    """Mutable state of a schedule."""

    note: str | None = None
    paused: bool = False
    limited_actions: bool = False
    remaining_actions: int = 0

    def _to_proto(self) -> _ProtoScheduleState:
        return _ProtoScheduleState(
            notes=self.note or "",
            paused=self.paused,
            limited_actions=self.limited_actions,
            remaining_actions=self.remaining_actions,
        )

    @staticmethod
    def _from_proto(s: _ProtoScheduleState) -> ScheduleState:
        return ScheduleState(
            note=s.notes or None,
            paused=s.paused,
            limited_actions=s.limited_actions,
            remaining_actions=s.remaining_actions,
        )


# --- Schedule ---


@dataclass
class Schedule:
    """A schedule definition."""

    action: ScheduleAction
    spec: ScheduleSpec
    policy: SchedulePolicy = field(default_factory=SchedulePolicy)
    state: ScheduleState = field(default_factory=ScheduleState)

    async def _to_proto(self, client: Client) -> _ProtoSchedule:
        return _ProtoSchedule(
            spec=self.spec._to_proto(),
            action=await self.action._to_proto(client),
            policies=self.policy._to_proto(),
            state=self.state._to_proto(),
        )

    @staticmethod
    def _from_proto(s: _ProtoSchedule) -> Schedule:
        return Schedule(
            action=ScheduleAction._from_proto(s.action),
            spec=ScheduleSpec._from_proto(s.spec),
            policy=SchedulePolicy._from_proto(s.policies),
            state=ScheduleState._from_proto(s.state),
        )


# --- Backfill ---


@dataclass
class ScheduleBackfill:
    """Backfill request for a schedule."""

    start_at: datetime
    end_at: datetime
    overlap: ScheduleOverlapPolicy | None = None

    def _to_proto(self) -> BackfillRequest:
        st = Timestamp()
        st.FromDatetime(self.start_at)
        et = Timestamp()
        et.FromDatetime(self.end_at)
        return BackfillRequest(
            start_time=st,
            end_time=et,
            overlap_policy=int(self.overlap) if self.overlap is not None else 0,
        )


# --- Description types ---


@dataclass
class ScheduleActionExecutionStartWorkflow:
    """Info about a running/recent schedule action execution."""

    workflow_id: str
    first_execution_run_id: str


@dataclass
class ScheduleActionResult:
    """Result of a schedule action execution."""

    scheduled_at: datetime
    started_at: datetime
    action: ScheduleActionExecutionStartWorkflow


@dataclass
class ScheduleInfo:
    """Info about a schedule's execution history."""

    num_actions: int
    num_actions_missed_catchup_window: int
    num_actions_skipped_overlap: int
    running_actions: Sequence[ScheduleActionExecutionStartWorkflow]
    recent_actions: Sequence[ScheduleActionResult]
    next_action_times: Sequence[datetime]
    created_at: datetime | None
    last_updated_at: datetime | None


@dataclass
class ScheduleDescription:
    """Full description of a schedule."""

    id: str
    schedule: Schedule
    info: ScheduleInfo
    raw_description: DescribeScheduleResponse

    @staticmethod
    def _from_proto(
        schedule_id: str,
        resp: DescribeScheduleResponse,
    ) -> ScheduleDescription:
        sched = Schedule._from_proto(resp.schedule)
        info_proto = resp.info

        running_actions = [
            ScheduleActionExecutionStartWorkflow(
                workflow_id=w.workflow_id,
                first_execution_run_id=w.run_id,
            )
            for w in info_proto.running_workflows
        ]

        recent_actions = []
        for ra in info_proto.recent_actions:
            sched_at = ra.schedule_time.ToDatetime().replace(tzinfo=timezone.utc)
            started_at = ra.actual_time.ToDatetime().replace(tzinfo=timezone.utc)
            exec_info = ScheduleActionExecutionStartWorkflow(
                workflow_id=ra.start_workflow_result.workflow_id,
                first_execution_run_id=ra.start_workflow_result.run_id,
            )
            recent_actions.append(
                ScheduleActionResult(
                    scheduled_at=sched_at,
                    started_at=started_at,
                    action=exec_info,
                )
            )

        next_times = [
            t.ToDatetime().replace(tzinfo=timezone.utc)
            for t in info_proto.future_action_times
        ]

        created_at = None
        if info_proto.HasField("create_time"):
            created_at = info_proto.create_time.ToDatetime().replace(
                tzinfo=timezone.utc
            )
        last_updated_at = None
        if info_proto.HasField("update_time"):
            last_updated_at = info_proto.update_time.ToDatetime().replace(
                tzinfo=timezone.utc
            )

        return ScheduleDescription(
            id=schedule_id,
            schedule=sched,
            info=ScheduleInfo(
                num_actions=info_proto.action_count,
                num_actions_missed_catchup_window=info_proto.missed_catchup_window,
                num_actions_skipped_overlap=info_proto.overlap_skipped,
                running_actions=running_actions,
                recent_actions=recent_actions,
                next_action_times=next_times,
                created_at=created_at,
                last_updated_at=last_updated_at,
            ),
            raw_description=resp,
        )


# --- Update types ---


@dataclass
class ScheduleUpdateInput:
    """Input to a schedule update callback."""

    description: ScheduleDescription


@dataclass
class ScheduleUpdate:
    """Result of a schedule update callback."""

    schedule: Schedule


# --- Errors ---


class ScheduleAlreadyRunningError(temporalio.exceptions.TemporalError):
    """Error when a schedule with the given ID already exists."""

    def __init__(self) -> None:
        super().__init__("Schedule already running")


# --- List types ---


@dataclass
class ScheduleListEntry:
    """Summary of a schedule from a list operation."""

    id: str
    workflow_type: str | None
    paused: bool
    note: str | None
    recent_actions: Sequence[ScheduleActionResult]
    next_action_times: Sequence[datetime]


# --- Handle ---


class ScheduleHandle:
    """Handle to an existing schedule."""

    def __init__(self, client: Client, id: str) -> None:
        self._client = client
        self._id = id

    @property
    def id(self) -> str:
        """Schedule ID."""
        return self._id

    async def describe(self) -> ScheduleDescription:
        """Describe this schedule."""
        req = DescribeScheduleRequest(
            namespace=self._client.namespace,
            schedule_id=self._id,
        )
        resp_bytes = await self._client._bridge.describe_schedule(
            req.SerializeToString()
        )
        resp = DescribeScheduleResponse()
        resp.ParseFromString(resp_bytes)
        return ScheduleDescription._from_proto(self._id, resp)

    async def delete(self) -> None:
        """Delete this schedule."""
        req = DeleteScheduleRequest(
            namespace=self._client.namespace,
            schedule_id=self._id,
            identity=self._client.identity,
        )
        await self._client._bridge.delete_schedule(req.SerializeToString())

    async def pause(self, *, note: str | None = None) -> None:
        """Pause this schedule.

        Args:
            note: Optional note about why the schedule was paused.
        """
        patch = SchedulePatch(pause=note or "Paused via Python SDK")
        req = PatchScheduleRequest(
            namespace=self._client.namespace,
            schedule_id=self._id,
            patch=patch,
            identity=self._client.identity,
        )
        await self._client._bridge.patch_schedule(req.SerializeToString())

    async def unpause(self, *, note: str | None = None) -> None:
        """Unpause this schedule.

        Args:
            note: Optional note about why the schedule was unpaused.
        """
        patch = SchedulePatch(unpause=note or "Unpaused via Python SDK")
        req = PatchScheduleRequest(
            namespace=self._client.namespace,
            schedule_id=self._id,
            patch=patch,
            identity=self._client.identity,
        )
        await self._client._bridge.patch_schedule(req.SerializeToString())

    async def trigger(
        self,
        *,
        overlap: ScheduleOverlapPolicy | None = None,
    ) -> None:
        """Trigger this schedule immediately.

        Args:
            overlap: Overlap policy for this trigger.
        """
        patch = SchedulePatch(
            trigger_immediately=TriggerImmediatelyRequest(
                overlap_policy=int(overlap) if overlap is not None else 0,
            ),
        )
        req = PatchScheduleRequest(
            namespace=self._client.namespace,
            schedule_id=self._id,
            patch=patch,
            identity=self._client.identity,
        )
        await self._client._bridge.patch_schedule(req.SerializeToString())

    async def backfill(
        self,
        *backfills: ScheduleBackfill,
    ) -> None:
        """Backfill this schedule.

        Args:
            backfills: One or more backfill requests.
        """
        patch = SchedulePatch(
            backfill_request=[b._to_proto() for b in backfills],
        )
        req = PatchScheduleRequest(
            namespace=self._client.namespace,
            schedule_id=self._id,
            patch=patch,
            identity=self._client.identity,
        )
        await self._client._bridge.patch_schedule(req.SerializeToString())

    async def update(
        self,
        updater: Callable[
            [ScheduleUpdateInput],
            ScheduleUpdate | None | Awaitable[ScheduleUpdate | None],
        ],
    ) -> None:
        """Update this schedule.

        Args:
            updater: Callback that receives the current schedule description
                and returns the updated schedule, or None to cancel the update.
        """
        desc = await self.describe()
        update_input = ScheduleUpdateInput(description=desc)
        result = updater(update_input)
        if inspect.isawaitable(result):
            result = await result
        if result is None:
            return

        schedule_proto = await result.schedule._to_proto(self._client)
        req = UpdateScheduleRequest(
            namespace=self._client.namespace,
            schedule_id=self._id,
            schedule=schedule_proto,
            identity=self._client.identity,
            request_id=str(uuid.uuid4()),
        )
        await self._client._bridge.update_schedule(req.SerializeToString())


__all__ = [
    "Schedule",
    "ScheduleAction",
    "ScheduleActionExecutionStartWorkflow",
    "ScheduleActionResult",
    "ScheduleActionStartWorkflow",
    "ScheduleAlreadyRunningError",
    "ScheduleBackfill",
    "ScheduleCalendarSpec",
    "ScheduleDescription",
    "ScheduleHandle",
    "ScheduleInfo",
    "ScheduleIntervalSpec",
    "ScheduleListEntry",
    "ScheduleOverlapPolicy",
    "SchedulePolicy",
    "ScheduleRange",
    "ScheduleSpec",
    "ScheduleState",
    "ScheduleUpdate",
    "ScheduleUpdateInput",
]
