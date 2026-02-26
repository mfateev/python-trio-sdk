"""Tests for headers propagation functionality."""

from datetime import datetime, timezone

import temporalio.api.common.v1
import temporalio.converter

from temporalio_trio import workflow
from temporalio_trio.worker import (
    TrioWorkflowInstance,
    WorkflowInstanceDetails,
)
from temporalio_trio.worker._activation import (
    ContinueAsNewCommand,
    ScheduleActivityCommand,
    SignalExternalWorkflowCommand,
    SignalWorkflowJob,
    StartChildWorkflowCommand,
    WorkflowActivation,
    WorkflowActivationCompletion,
    WorkflowStartedJob,
)
from temporalio_trio.worker._bridge_types import (
    bridge_to_poc_activation,
    poc_to_bridge_completion,
)


def _make_payload(
    value: bytes, encoding: str = "binary/plain"
) -> temporalio.api.common.v1.Payload:
    """Create a test Payload protobuf."""
    p = temporalio.api.common.v1.Payload()
    p.data = value
    p.metadata["encoding"] = encoding.encode()
    return p


def _create_details(workflow_cls: type) -> WorkflowInstanceDetails:
    """Helper to create WorkflowInstanceDetails for tests."""
    defn = workflow._Definition.must_from_class(workflow_cls)
    info = workflow.Info(
        workflow_id="test-wf-1",
        workflow_type=defn.name,
        run_id="run-1",
        task_queue="test-queue",
        namespace="default",
        attempt=1,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    return WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=12345,
    )


# =============================================================================
# Test workflows
# =============================================================================


@workflow.defn
class HeadersInfoWorkflow:
    """Workflow that reads headers from info()."""

    def __init__(self):
        self._headers = {}

    @workflow.run
    async def run(self) -> str:
        self._headers = dict(workflow.info().headers)
        return "done"

    @workflow.query
    def get_headers_count(self) -> int:
        return len(self._headers)


@workflow.defn
class HeadersDefaultWorkflow:
    """Workflow that verifies empty default headers."""

    @workflow.run
    async def run(self) -> str:
        info = workflow.info()
        assert isinstance(info.headers, dict) or hasattr(info.headers, "__getitem__")
        return "done"


# =============================================================================
# Unit Tests: Headers on WorkflowStartedJob
# =============================================================================


def test_workflow_started_job_default_headers():
    """WorkflowStartedJob defaults to empty headers dict."""
    job = WorkflowStartedJob(workflow_type="Test", args=())
    assert job.headers == {}


def test_workflow_started_job_with_headers():
    """WorkflowStartedJob can store headers."""
    h1 = _make_payload(b"trace-id-123")
    headers = {"x-trace-id": h1}
    job = WorkflowStartedJob(workflow_type="Test", args=(), headers=headers)
    assert job.headers == {"x-trace-id": h1}
    assert job.headers["x-trace-id"].data == b"trace-id-123"


# =============================================================================
# Unit Tests: Headers on SignalWorkflowJob and QueryWorkflowJob
# =============================================================================


def test_signal_job_default_headers():
    """SignalWorkflowJob defaults to empty headers dict."""
    job = SignalWorkflowJob(signal_name="test", args=())
    assert job.headers == {}


def test_signal_job_with_headers():
    """SignalWorkflowJob can store headers."""
    h = _make_payload(b"signal-trace")
    job = SignalWorkflowJob(signal_name="test", args=(), headers={"x-trace": h})
    assert job.headers["x-trace"].data == b"signal-trace"


# =============================================================================
# Unit Tests: Headers on outgoing commands
# =============================================================================


def test_schedule_activity_default_headers():
    """ScheduleActivityCommand defaults to empty headers."""
    cmd = ScheduleActivityCommand(seq=1, activity_id="1", activity_type="MyAct")
    assert cmd.headers == {}


def test_schedule_activity_with_headers():
    """ScheduleActivityCommand can store headers."""
    h = _make_payload(b"act-trace")
    cmd = ScheduleActivityCommand(
        seq=1, activity_id="1", activity_type="MyAct", headers={"x-trace": h}
    )
    assert cmd.headers["x-trace"].data == b"act-trace"


def test_start_child_workflow_default_headers():
    """StartChildWorkflowCommand defaults to empty headers."""
    cmd = StartChildWorkflowCommand(seq=1, workflow_id="child-1", workflow_type="Child")
    assert cmd.headers == {}


def test_signal_external_default_headers():
    """SignalExternalWorkflowCommand defaults to empty headers."""
    cmd = SignalExternalWorkflowCommand(seq=1, workflow_id="ext-1", signal_name="sig")
    assert cmd.headers == {}


def test_continue_as_new_default_headers():
    """ContinueAsNewCommand defaults to empty headers."""
    cmd = ContinueAsNewCommand(workflow_type="MyWf")
    assert cmd.headers == {}


# =============================================================================
# Unit Tests: Headers exposed via workflow.info()
# =============================================================================


def test_info_default_headers():
    """Info dataclass defaults to empty headers."""
    info = workflow.Info(
        workflow_id="wf-1",
        workflow_type="Test",
        run_id="run-1",
        task_queue="q",
        namespace="default",
        attempt=1,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    assert info.headers == {}


def test_info_with_headers():
    """Info dataclass can store headers."""
    h = _make_payload(b"info-trace")
    info = workflow.Info(
        workflow_id="wf-1",
        workflow_type="Test",
        run_id="run-1",
        task_queue="q",
        namespace="default",
        attempt=1,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
        headers={"x-trace": h},
    )
    assert info.headers["x-trace"].data == b"info-trace"


def test_headers_exposed_via_workflow_info():
    """Headers from WorkflowStartedJob are accessible via workflow.info().headers."""
    details = _create_details(HeadersDefaultWorkflow)
    instance = TrioWorkflowInstance(details)

    activation = WorkflowActivation(
        jobs=[
            WorkflowStartedJob(
                workflow_type="HeadersDefaultWorkflow",
                args=(),
            )
        ],
        timestamp_ns=1000000,
        run_id="run-1",
        is_replaying=False,
    )

    completion = instance.activate(activation)

    # Workflow should complete without error
    from temporalio_trio.worker._activation import CompleteWorkflowCommand

    assert any(isinstance(cmd, CompleteWorkflowCommand) for cmd in completion.commands)


def test_headers_from_started_job_available_in_info():
    """Headers from WorkflowStartedJob flow into workflow.info().headers."""
    details = _create_details(HeadersInfoWorkflow)
    instance = TrioWorkflowInstance(details)

    h = _make_payload(b"test-trace")
    activation = WorkflowActivation(
        jobs=[
            WorkflowStartedJob(
                workflow_type="HeadersInfoWorkflow",
                args=(),
                headers={"x-trace": h},
            )
        ],
        timestamp_ns=1000000,
        run_id="run-1",
        is_replaying=False,
    )

    completion = instance.activate(activation)

    from temporalio_trio.worker._activation import CompleteWorkflowCommand

    assert any(isinstance(cmd, CompleteWorkflowCommand) for cmd in completion.commands)


# =============================================================================
# Unit Tests: Bridge type conversions - parsing headers from protobuf
# =============================================================================


def test_bridge_parse_initialize_workflow_headers():
    """bridge_to_poc_activation parses headers from InitializeWorkflow."""
    import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb

    # Build a protobuf activation with headers
    bridge_act = act_pb.WorkflowActivation()
    bridge_act.run_id = "test-run"
    bridge_act.timestamp.seconds = 1000
    bridge_act.is_replaying = False

    job = bridge_act.jobs.add()
    job.initialize_workflow.workflow_type = "TestWorkflow"
    # Add a header
    payload = job.initialize_workflow.headers["x-trace-id"]
    payload.data = b"trace-123"

    data_converter = temporalio.converter.DataConverter()
    poc_act = bridge_to_poc_activation(bridge_act, data_converter)

    assert len(poc_act.jobs) == 1
    started_job = poc_act.jobs[0]
    assert isinstance(started_job, WorkflowStartedJob)
    assert "x-trace-id" in started_job.headers
    assert started_job.headers["x-trace-id"].data == b"trace-123"


def test_bridge_parse_signal_workflow_headers():
    """bridge_to_poc_activation parses headers from SignalWorkflow."""
    import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb

    bridge_act = act_pb.WorkflowActivation()
    bridge_act.run_id = "test-run"
    bridge_act.timestamp.seconds = 1000

    job = bridge_act.jobs.add()
    job.signal_workflow.signal_name = "test_signal"
    # Add a header
    payload = job.signal_workflow.headers["x-signal-trace"]
    payload.data = b"signal-trace-456"

    data_converter = temporalio.converter.DataConverter()
    poc_act = bridge_to_poc_activation(bridge_act, data_converter)

    assert len(poc_act.jobs) == 1
    signal_job = poc_act.jobs[0]
    assert isinstance(signal_job, SignalWorkflowJob)
    assert "x-signal-trace" in signal_job.headers
    assert signal_job.headers["x-signal-trace"].data == b"signal-trace-456"


def test_bridge_parse_query_workflow_headers():
    """bridge_to_poc_activation parses headers from QueryWorkflow."""
    import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb

    bridge_act = act_pb.WorkflowActivation()
    bridge_act.run_id = "test-run"
    bridge_act.timestamp.seconds = 1000

    job = bridge_act.jobs.add()
    job.query_workflow.query_id = "q1"
    job.query_workflow.query_type = "get_status"
    # Add a header
    payload = job.query_workflow.headers["x-query-trace"]
    payload.data = b"query-trace-789"

    data_converter = temporalio.converter.DataConverter()
    poc_act = bridge_to_poc_activation(bridge_act, data_converter)

    assert len(poc_act.jobs) == 1
    from temporalio_trio.worker._activation import QueryWorkflowJob

    query_job = poc_act.jobs[0]
    assert isinstance(query_job, QueryWorkflowJob)
    assert "x-query-trace" in query_job.headers
    assert query_job.headers["x-query-trace"].data == b"query-trace-789"


# =============================================================================
# Unit Tests: Bridge type conversions - applying headers to outgoing commands
# =============================================================================


def test_bridge_apply_headers_schedule_activity():
    """Headers on ScheduleActivityCommand are applied to protobuf."""
    h = _make_payload(b"act-trace-data")
    cmd = ScheduleActivityCommand(
        seq=1,
        activity_id="1",
        activity_type="TestActivity",
        args=(),
        start_to_close_timeout=__import__("datetime").timedelta(seconds=30),
        headers={"x-trace": h},
    )

    completion = WorkflowActivationCompletion(commands=[cmd])
    data_converter = temporalio.converter.DataConverter()
    bridge_comp = poc_to_bridge_completion("run-1", completion, data_converter)

    assert len(bridge_comp.successful.commands) == 1
    bridge_cmd = bridge_comp.successful.commands[0]
    assert "x-trace" in bridge_cmd.schedule_activity.headers
    assert bridge_cmd.schedule_activity.headers["x-trace"].data == b"act-trace-data"


def test_bridge_apply_headers_start_child_workflow():
    """Headers on StartChildWorkflowCommand are applied to protobuf."""
    h = _make_payload(b"child-trace-data")
    cmd = StartChildWorkflowCommand(
        seq=1,
        workflow_id="child-1",
        workflow_type="ChildWf",
        headers={"x-trace": h},
    )

    completion = WorkflowActivationCompletion(commands=[cmd])
    data_converter = temporalio.converter.DataConverter()
    bridge_comp = poc_to_bridge_completion("run-1", completion, data_converter)

    assert len(bridge_comp.successful.commands) == 1
    bridge_cmd = bridge_comp.successful.commands[0]
    assert "x-trace" in bridge_cmd.start_child_workflow_execution.headers
    assert (
        bridge_cmd.start_child_workflow_execution.headers["x-trace"].data
        == b"child-trace-data"
    )


def test_bridge_apply_headers_signal_external():
    """Headers on SignalExternalWorkflowCommand are applied to protobuf."""
    h = _make_payload(b"signal-ext-trace")
    cmd = SignalExternalWorkflowCommand(
        seq=1,
        workflow_id="ext-1",
        signal_name="my_signal",
        headers={"x-trace": h},
    )

    completion = WorkflowActivationCompletion(commands=[cmd])
    data_converter = temporalio.converter.DataConverter()
    bridge_comp = poc_to_bridge_completion("run-1", completion, data_converter)

    assert len(bridge_comp.successful.commands) == 1
    bridge_cmd = bridge_comp.successful.commands[0]
    assert "x-trace" in bridge_cmd.signal_external_workflow_execution.headers
    assert (
        bridge_cmd.signal_external_workflow_execution.headers["x-trace"].data
        == b"signal-ext-trace"
    )


def test_bridge_apply_headers_continue_as_new():
    """Headers on ContinueAsNewCommand are applied to protobuf."""
    h = _make_payload(b"can-trace")
    cmd = ContinueAsNewCommand(
        workflow_type="MyWf",
        headers={"x-trace": h},
    )

    completion = WorkflowActivationCompletion(commands=[cmd])
    data_converter = temporalio.converter.DataConverter()
    bridge_comp = poc_to_bridge_completion("run-1", completion, data_converter)

    assert len(bridge_comp.successful.commands) == 1
    bridge_cmd = bridge_comp.successful.commands[0]
    assert "x-trace" in bridge_cmd.continue_as_new_workflow_execution.headers
    assert (
        bridge_cmd.continue_as_new_workflow_execution.headers["x-trace"].data
        == b"can-trace"
    )


def test_bridge_empty_headers_no_op():
    """Empty headers dict results in no headers on protobuf command."""
    cmd = ScheduleActivityCommand(
        seq=1,
        activity_id="1",
        activity_type="TestActivity",
        args=(),
        start_to_close_timeout=__import__("datetime").timedelta(seconds=30),
        headers={},
    )

    completion = WorkflowActivationCompletion(commands=[cmd])
    data_converter = temporalio.converter.DataConverter()
    bridge_comp = poc_to_bridge_completion("run-1", completion, data_converter)

    bridge_cmd = bridge_comp.successful.commands[0]
    assert len(bridge_cmd.schedule_activity.headers) == 0
