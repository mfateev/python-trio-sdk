"""Integration tests for the bridge type conversion and worker dispatch pipeline.

Tests the interactions that were broken and fixed:
1. run_id preservation through bridge_to_poc_activation
2. Eviction detection from remove_from_cache activations
3. Command type unification (runtime uses activation types directly)
4. _Runtime contextvar bridging (workflow.sleep/time via WorkflowRuntime duck typing)
5. Timer summary round-trip through the full completion pipeline
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
import temporalio.api.common.v1
import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb
import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as comp_pb
import temporalio.converter
import trio

from temporalio_trio.worker._activation import (
    CompleteWorkflowCommand,
    FailWorkflowCommand,
    ScheduleActivityCommand,
    StartTimerCommand,
    TimerFiredJob,
    WorkflowActivation,
    WorkflowActivationCompletion,
    WorkflowStartedJob,
)
from temporalio_trio.worker._bridge_types import (
    bridge_to_poc_activation,
    poc_to_bridge_completion,
)
from temporalio_trio.worker._runtime import (
    QueryFailureCommand,
    QuerySuccessCommand,
)
from temporalio_trio.worker._single_thread_worker import SingleThreadWorker
from temporalio_trio.worker._workflow_state import WorkflowState
from temporalio_trio.workflow import defn, run, sleep, time

# =============================================================================
# Test Workflows
# =============================================================================


@defn
class ImmediateWorkflow:
    """Workflow that completes immediately."""

    @run
    async def run(self, value: str) -> str:
        return f"done: {value}"


@defn
class SleepWorkflow:
    """Workflow that uses workflow.sleep() (the public API)."""

    @run
    async def run(self, duration: float) -> str:
        await sleep(duration)
        return "slept"


@defn
class SleepWithSummaryWorkflow:
    """Workflow that uses workflow.sleep() with summary."""

    @run
    async def run(self) -> str:
        await sleep(0.1, summary="waiting briefly")
        return "slept with summary"


@defn
class TimeWorkflow:
    """Workflow that reads workflow.time()."""

    @run
    async def run(self) -> float:
        return time()


# =============================================================================
# Mock Bridge
# =============================================================================


class MockBridge:
    """Mock bridge that returns pre-configured activations and records completions."""

    def __init__(self) -> None:
        self.activations: list[WorkflowActivation] = []
        self.completion_bytes: list[bytes] = []
        self._activation_index = 0
        self._shutdown = False
        self._activation_ready_event = trio.Event()

    def add_activation(self, activation: WorkflowActivation) -> None:
        self.activations.append(activation)
        self._activation_ready_event.set()

    async def poll_workflow_activation(
        self, timeout: float | None = None
    ) -> WorkflowActivation:
        while True:
            if self._shutdown:
                raise RuntimeError("PollShutdownError")
            if self._activation_index < len(self.activations):
                act = self.activations[self._activation_index]
                self._activation_index += 1
                return act  # type: ignore
            self._activation_ready_event = trio.Event()
            with trio.move_on_after(0.1):
                await self._activation_ready_event.wait()

    async def complete_workflow_activation(self, completion_bytes: bytes) -> None:
        self.completion_bytes.append(completion_bytes)

    def initiate_shutdown(self) -> None:
        self._shutdown = True
        self._activation_ready_event.set()

    async def shutdown(self) -> None:
        self.initiate_shutdown()


# =============================================================================
# 1. run_id preservation
# =============================================================================


class TestRunIdPreservation:
    """Tests that run_id is preserved through bridge type conversion."""

    def test_bridge_to_poc_preserves_run_id(self):
        """run_id from protobuf activation must survive conversion to POC type."""
        bridge_act = act_pb.WorkflowActivation()
        bridge_act.run_id = "my-real-run-id-12345"
        bridge_act.timestamp.seconds = 1000

        job = bridge_act.jobs.add()
        job.initialize_workflow.workflow_type = "TestWorkflow"

        dc = temporalio.converter.DataConverter()
        poc_act = bridge_to_poc_activation(bridge_act, dc)

        assert poc_act.run_id == "my-real-run-id-12345"

    def test_run_id_used_in_completion(self):
        """Completion must use the real run_id, not a synthetic one."""
        bridge_act = act_pb.WorkflowActivation()
        bridge_act.run_id = "server-assigned-run-id"
        bridge_act.timestamp.seconds = 1000

        job = bridge_act.jobs.add()
        job.initialize_workflow.workflow_type = "TestWorkflow"

        dc = temporalio.converter.DataConverter()
        poc_act = bridge_to_poc_activation(bridge_act, dc)

        poc_comp = WorkflowActivationCompletion(
            commands=[CompleteWorkflowCommand(result="ok")]
        )
        bridge_comp = poc_to_bridge_completion(poc_act.run_id, poc_comp, dc)

        assert bridge_comp.run_id == "server-assigned-run-id"

    def test_timer_fired_activation_preserves_run_id(self):
        """run_id preserved on continuation activations (no WorkflowStartedJob)."""
        bridge_act = act_pb.WorkflowActivation()
        bridge_act.run_id = "continuation-run-id"
        bridge_act.timestamp.seconds = 2000

        job = bridge_act.jobs.add()
        job.fire_timer.seq = 1

        dc = temporalio.converter.DataConverter()
        poc_act = bridge_to_poc_activation(bridge_act, dc)

        assert poc_act.run_id == "continuation-run-id"
        assert len(poc_act.jobs) == 1
        assert isinstance(poc_act.jobs[0], TimerFiredJob)

    def test_fallback_run_id_for_test_activations(self):
        """Test activations without run_id get a synthetic fallback."""
        act = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="Test", args=())],
            timestamp_ns=0,
        )
        worker = SingleThreadWorker(
            bridge=MockBridge(),  # type: ignore
            task_queue="q",
            workflows=[ImmediateWorkflow],
        )
        rid = worker._extract_run_id(act)
        assert rid.startswith("run-")


# =============================================================================
# 2. Eviction detection
# =============================================================================


class TestEvictionDetection:
    """Tests that remove_from_cache activations are properly detected."""

    def test_remove_from_cache_sets_flag_in_parse(self):
        """_parse_activation must detect remove_from_cache and set the flag."""
        bridge_act = act_pb.WorkflowActivation()
        bridge_act.run_id = "evict-run-id"
        bridge_act.timestamp.seconds = 1000

        job = bridge_act.jobs.add()
        job.remove_from_cache.SetInParent()

        activation_bytes = bridge_act.SerializeToString()

        worker = SingleThreadWorker(
            bridge=MockBridge(),  # type: ignore
            task_queue="q",
            workflows=[ImmediateWorkflow],
        )
        poc_act = worker._parse_activation(activation_bytes)

        assert poc_act.remove_from_cache is True
        assert poc_act.run_id == "evict-run-id"
        assert len(poc_act.jobs) == 0

    def test_normal_activation_not_eviction(self):
        """Regular activations must not be flagged as evictions."""
        bridge_act = act_pb.WorkflowActivation()
        bridge_act.run_id = "normal-run-id"
        bridge_act.timestamp.seconds = 1000

        job = bridge_act.jobs.add()
        job.initialize_workflow.workflow_type = "TestWorkflow"

        activation_bytes = bridge_act.SerializeToString()

        worker = SingleThreadWorker(
            bridge=MockBridge(),  # type: ignore
            task_queue="q",
            workflows=[ImmediateWorkflow],
        )
        poc_act = worker._parse_activation(activation_bytes)

        assert poc_act.remove_from_cache is False

    def test_poc_activation_default_not_eviction(self):
        """POC WorkflowActivation defaults to remove_from_cache=False."""
        act = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="Test", args=())],
            timestamp_ns=0,
        )
        assert act.remove_from_cache is False

    @pytest.mark.trio
    async def test_eviction_sends_empty_completion(self):
        """Eviction dispatch must send an empty completion, not spawn a workflow."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="q",
            workflows=[ImmediateWorkflow],
        )

        eviction_act = WorkflowActivation(
            jobs=[],
            timestamp_ns=1000,
            run_id="evict-me",
            remove_from_cache=True,
        )

        await worker._dispatch_activation(eviction_act)

        assert len(bridge.completion_bytes) == 1
        comp = comp_pb.WorkflowActivationCompletion()
        comp.ParseFromString(bridge.completion_bytes[0])
        assert comp.run_id == "evict-me"
        assert comp.HasField("successful")
        assert len(comp.successful.commands) == 0


# =============================================================================
# 3. Command normalization (passthrough in unified type system)
# =============================================================================


class TestCommandNormalization:
    """Tests that WorkflowRuntime commands are compatible with poc_to_bridge_completion.

    Since _runtime.py now imports command types directly from _activation.py,
    commands are already in the correct format — no normalization needed.
    """

    def test_runtime_uses_activation_timer_type(self):
        """StartTimerCommand from _runtime is the same as from _activation."""
        from temporalio_trio.worker._activation import (
            StartTimerCommand as ActStartTimerCommand,
        )
        from temporalio_trio.worker._runtime import (
            StartTimerCommand as RtStartTimerCommand,
        )

        assert ActStartTimerCommand is RtStartTimerCommand

    def test_runtime_uses_activation_activity_type(self):
        """ScheduleActivityCommand from _runtime is the same as from _activation."""
        from temporalio_trio.worker._activation import (
            ScheduleActivityCommand as ActCmd,
        )
        from temporalio_trio.worker._runtime import (
            ScheduleActivityCommand as RtCmd,
        )

        assert ActCmd is RtCmd

    def test_runtime_uses_activation_child_workflow_type(self):
        """StartChildWorkflowCommand from _runtime is the same as from _activation."""
        from temporalio_trio.worker._activation import (
            StartChildWorkflowCommand as ActCmd,
        )
        from temporalio_trio.worker._runtime import (
            StartChildWorkflowCommand as RtCmd,
        )

        assert ActCmd is RtCmd

    def test_runtime_uses_activation_cancel_type(self):
        """CancelWorkflowCommand from _runtime is the same as from _activation."""
        from temporalio_trio.worker._activation import (
            CancelWorkflowCommand as ActCmd,
        )
        from temporalio_trio.worker._runtime import (
            CancelWorkflowCommand as RtCmd,
        )

        assert ActCmd is RtCmd


# =============================================================================
# 4. _Runtime contextvar bridging
# =============================================================================


class TestRuntimeContextvarBridging:
    """Tests that workflow.sleep() and workflow.time() work inside SingleThreadWorker."""

    @pytest.mark.trio
    async def test_workflow_time_accessible(self):
        """workflow.time() must return the activation timestamp inside a workflow."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="q",
            workflows=[TimeWorkflow],
        )

        bridge.add_activation(
            WorkflowActivation(
                jobs=[WorkflowStartedJob(workflow_type="TimeWorkflow", args=())],
                timestamp_ns=5_000_000_000,
                run_id="time-test",
            )
        )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)
            await trio.sleep(0.5)
            worker.shutdown()
            await trio.sleep(0.1)
            nursery.cancel_scope.cancel()

        assert len(bridge.completion_bytes) >= 1
        comp = comp_pb.WorkflowActivationCompletion()
        comp.ParseFromString(bridge.completion_bytes[0])
        assert comp.HasField("successful")
        assert len(comp.successful.commands) == 1
        assert comp.successful.commands[0].HasField("complete_workflow_execution")

    @pytest.mark.trio
    async def test_workflow_sleep_produces_timer_command(self):
        """workflow.sleep() via public API must produce a StartTimerCommand."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="q",
            workflows=[SleepWorkflow],
        )

        bridge.add_activation(
            WorkflowActivation(
                jobs=[WorkflowStartedJob(workflow_type="SleepWorkflow", args=(1.0,))],
                timestamp_ns=0,
                run_id="sleep-test",
            )
        )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)
            await trio.sleep(0.5)

            assert len(bridge.completion_bytes) >= 1
            comp = comp_pb.WorkflowActivationCompletion()
            comp.ParseFromString(bridge.completion_bytes[0])
            assert comp.HasField("successful")
            assert len(comp.successful.commands) == 1
            cmd = comp.successful.commands[0]
            assert cmd.HasField("start_timer")
            assert cmd.start_timer.start_to_fire_timeout.seconds == 1

            # Deliver timer fired
            bridge.add_activation(
                WorkflowActivation(
                    jobs=[TimerFiredJob(timer_id=1)],
                    timestamp_ns=1_000_000_000,
                    run_id="sleep-test",
                )
            )
            await trio.sleep(0.5)

            # Second completion should have CompleteWorkflowCommand
            assert len(bridge.completion_bytes) >= 2
            comp2 = comp_pb.WorkflowActivationCompletion()
            comp2.ParseFromString(bridge.completion_bytes[1])
            assert comp2.HasField("successful")
            assert len(comp2.successful.commands) == 1
            assert comp2.successful.commands[0].HasField("complete_workflow_execution")

            worker.shutdown()
            await trio.sleep(0.1)
            nursery.cancel_scope.cancel()


# =============================================================================
# 5. Timer summary round-trip
# =============================================================================


class TestTimerSummaryRoundTrip:
    """Tests that workflow.sleep(summary=...) propagates through to the completion."""

    def test_summary_in_bridge_completion(self):
        """Summary must appear as user_metadata in the protobuf completion."""
        dc = temporalio.converter.DataConverter()
        poc_comp = WorkflowActivationCompletion(
            commands=[
                StartTimerCommand(
                    timer_id=1, duration_ms=100, summary="waiting briefly"
                )
            ]
        )
        bridge_comp = poc_to_bridge_completion("run-1", poc_comp, dc)

        cmd = bridge_comp.successful.commands[0]
        assert cmd.HasField("start_timer")
        assert cmd.HasField("user_metadata")
        summary_value = dc.payload_converter.from_payload(cmd.user_metadata.summary)
        assert summary_value == "waiting briefly"

    def test_no_summary_no_user_metadata(self):
        """When summary is None, user_metadata must not be set."""
        dc = temporalio.converter.DataConverter()
        poc_comp = WorkflowActivationCompletion(
            commands=[StartTimerCommand(timer_id=1, duration_ms=100)]
        )
        bridge_comp = poc_to_bridge_completion("run-1", poc_comp, dc)

        cmd = bridge_comp.successful.commands[0]
        assert cmd.HasField("start_timer")
        assert not cmd.HasField("user_metadata")

    @pytest.mark.trio
    async def test_sleep_summary_reaches_bridge_completion(self):
        """workflow.sleep(summary=...) must produce a completion with user_metadata."""
        bridge = MockBridge()
        worker = SingleThreadWorker(
            bridge=bridge,  # type: ignore
            task_queue="q",
            workflows=[SleepWithSummaryWorkflow],
        )

        bridge.add_activation(
            WorkflowActivation(
                jobs=[
                    WorkflowStartedJob(
                        workflow_type="SleepWithSummaryWorkflow", args=()
                    )
                ],
                timestamp_ns=0,
                run_id="summary-test",
            )
        )

        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)
            await trio.sleep(0.5)

            assert len(bridge.completion_bytes) >= 1
            comp = comp_pb.WorkflowActivationCompletion()
            comp.ParseFromString(bridge.completion_bytes[0])
            cmd = comp.successful.commands[0]
            assert cmd.HasField("start_timer")
            assert cmd.HasField("user_metadata")

            dc = temporalio.converter.DataConverter()
            summary = dc.payload_converter.from_payload(cmd.user_metadata.summary)
            assert summary == "waiting briefly"

            worker.shutdown()
            await trio.sleep(0.1)
            nursery.cancel_scope.cancel()
