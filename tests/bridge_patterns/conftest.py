"""Common fixtures for bridge pattern tests.

These fixtures provide utilities for direct bridge testing, including:
- Bridge wrapper setup/teardown
- Protobuf message builders
- CLI helpers for starting workflows
- Common timeout values
- Custom search attribute setup
"""

from __future__ import annotations

import json
import subprocess
import time
from datetime import timedelta
from typing import Any, Callable
from uuid import uuid4

import google.protobuf.duration_pb2
import pytest
import temporalio.api.common.v1
import temporalio.bridge.proto.activity_result.activity_result_pb2 as act_result_pb
import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb
import temporalio.bridge.proto.workflow_commands.workflow_commands_pb2 as cmd_pb
import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as comp_pb
import temporalio.converter
import trio

from temporalio_trio._async_bridge import TrioBridgeWrapper

TEMPORAL_CLI_PATH = "/home/dev/.temporalio/bin/temporal"
DEFAULT_NAMESPACE = "default"
DEFAULT_TIMEOUT = 30.0


def _ensure_custom_search_attributes_exist() -> None:
    """Ensure custom search attributes exist on the Temporal server.

    Creates CustomKeywordField and CustomIntField if they don't already exist.
    This is idempotent - creating an existing attribute returns an error that we ignore.

    This is called at module load time to ensure search attributes are available
    for all bridge pattern tests that need them.
    """
    attributes = [
        ("CustomKeywordField", "Keyword"),
        ("CustomIntField", "Int"),
    ]

    for name, attr_type in attributes:
        try:
            subprocess.run(
                [
                    TEMPORAL_CLI_PATH,
                    "operator",
                    "search-attribute",
                    "create",
                    "--namespace",
                    DEFAULT_NAMESPACE,
                    "--name",
                    name,
                    "--type",
                    attr_type,
                ],
                capture_output=True,
                text=True,
                timeout=10,
            )
            # Ignore errors - attribute may already exist
        except Exception:
            pass  # Ignore errors during attribute creation


# Ensure custom search attributes exist before any tests run
_ensure_custom_search_attributes_exist()


@pytest.fixture
def data_converter() -> temporalio.converter.DataConverter:
    """Create a data converter for payload encoding/decoding."""
    return temporalio.converter.DataConverter.default


@pytest.fixture
def unique_task_queue() -> str:
    """Generate a unique task queue name for each test."""
    return f"bridge-pattern-test-{uuid4()}"


async def safe_shutdown(bridge: TrioBridgeWrapper, timeout: float = 5.0) -> None:
    """Safely shutdown a bridge with timeout.

    Args:
        bridge: The bridge wrapper to shutdown
        timeout: Maximum time to wait for shutdown
    """
    try:
        bridge.initiate_shutdown()
        with trio.move_on_after(timeout):
            await bridge.finalize_shutdown()
    except Exception:
        pass  # Ignore shutdown errors in cleanup


class CompletionBuilder:
    """Builder for creating workflow activation completion protobufs."""

    def __init__(
        self,
        run_id: str,
        data_converter: temporalio.converter.DataConverter | None = None,
    ):
        self.run_id = run_id
        self.data_converter = (
            data_converter or temporalio.converter.DataConverter.default
        )
        self.completion = comp_pb.WorkflowActivationCompletion()
        self.completion.run_id = run_id
        self.completion.successful.SetInParent()

    def start_timer(
        self,
        seq: int,
        duration: timedelta,
        summary: str | None = None,
    ) -> "CompletionBuilder":
        """Add a StartTimer command."""
        cmd = cmd_pb.WorkflowCommand()
        cmd.start_timer.seq = seq
        duration_proto = google.protobuf.duration_pb2.Duration()
        total_seconds = duration.total_seconds()
        duration_proto.seconds = int(total_seconds)
        duration_proto.nanos = int((total_seconds - int(total_seconds)) * 1_000_000_000)
        cmd.start_timer.start_to_fire_timeout.CopyFrom(duration_proto)
        self.completion.successful.commands.append(cmd)
        return self

    def cancel_timer(self, seq: int) -> "CompletionBuilder":
        """Add a CancelTimer command."""
        cmd = cmd_pb.WorkflowCommand()
        cmd.cancel_timer.seq = seq
        self.completion.successful.commands.append(cmd)
        return self

    def complete_workflow(self, result: Any) -> "CompletionBuilder":
        """Add a CompleteWorkflowExecution command."""
        cmd = cmd_pb.WorkflowCommand()
        payload = self.data_converter.payload_converter.to_payload(result)
        cmd.complete_workflow_execution.result.CopyFrom(payload)
        self.completion.successful.commands.append(cmd)
        return self

    def fail_workflow(self, message: str, stack_trace: str = "") -> "CompletionBuilder":
        """Add a FailWorkflowExecution command."""
        cmd = cmd_pb.WorkflowCommand()
        cmd.fail_workflow_execution.failure.message = message
        cmd.fail_workflow_execution.failure.stack_trace = stack_trace
        self.completion.successful.commands.append(cmd)
        return self

    def schedule_activity(
        self,
        seq: int,
        activity_type: str,
        task_queue: str,
        args: tuple[Any, ...] = (),
        activity_id: str | None = None,
        schedule_to_close_timeout: timedelta | None = None,
        schedule_to_start_timeout: timedelta | None = None,
        start_to_close_timeout: timedelta | None = None,
        heartbeat_timeout: timedelta | None = None,
    ) -> "CompletionBuilder":
        """Add a ScheduleActivity command."""
        cmd = cmd_pb.WorkflowCommand()
        cmd.schedule_activity.seq = seq
        cmd.schedule_activity.activity_id = activity_id or str(seq)
        cmd.schedule_activity.activity_type = activity_type
        cmd.schedule_activity.task_queue = task_queue

        for arg in args:
            payload = self.data_converter.payload_converter.to_payload(arg)
            cmd.schedule_activity.arguments.append(payload)

        def set_duration(
            proto: google.protobuf.duration_pb2.Duration, td: timedelta
        ) -> None:
            total_seconds = td.total_seconds()
            proto.seconds = int(total_seconds)
            proto.nanos = int((total_seconds - int(total_seconds)) * 1_000_000_000)

        if schedule_to_close_timeout:
            set_duration(
                cmd.schedule_activity.schedule_to_close_timeout,
                schedule_to_close_timeout,
            )
        if schedule_to_start_timeout:
            set_duration(
                cmd.schedule_activity.schedule_to_start_timeout,
                schedule_to_start_timeout,
            )
        if start_to_close_timeout:
            set_duration(
                cmd.schedule_activity.start_to_close_timeout, start_to_close_timeout
            )
        if heartbeat_timeout:
            set_duration(cmd.schedule_activity.heartbeat_timeout, heartbeat_timeout)

        self.completion.successful.commands.append(cmd)
        return self

    def request_cancel_activity(self, seq: int) -> "CompletionBuilder":
        """Add a RequestCancelActivity command."""
        cmd = cmd_pb.WorkflowCommand()
        cmd.request_cancel_activity.seq = seq
        self.completion.successful.commands.append(cmd)
        return self

    def respond_to_query(
        self,
        query_id: str,
        result: Any | None = None,
        error: str | None = None,
    ) -> "CompletionBuilder":
        """Add a RespondToQuery command."""
        cmd = cmd_pb.WorkflowCommand()
        cmd.respond_to_query.query_id = query_id
        if error:
            cmd.respond_to_query.failed.message = error
        else:
            payload = self.data_converter.payload_converter.to_payload(result)
            cmd.respond_to_query.succeeded.response.CopyFrom(payload)
        self.completion.successful.commands.append(cmd)
        return self

    def start_child_workflow(
        self,
        seq: int,
        workflow_id: str,
        workflow_type: str,
        task_queue: str,
        args: tuple[Any, ...] = (),
        execution_timeout: timedelta | None = None,
        run_timeout: timedelta | None = None,
        task_timeout: timedelta | None = None,
    ) -> "CompletionBuilder":
        """Add a StartChildWorkflowExecution command."""
        cmd = cmd_pb.WorkflowCommand()
        cmd.start_child_workflow_execution.seq = seq
        cmd.start_child_workflow_execution.workflow_id = workflow_id
        cmd.start_child_workflow_execution.workflow_type = workflow_type
        cmd.start_child_workflow_execution.task_queue = task_queue

        for arg in args:
            payload = self.data_converter.payload_converter.to_payload(arg)
            cmd.start_child_workflow_execution.input.append(payload)

        def set_duration(
            proto: google.protobuf.duration_pb2.Duration, td: timedelta
        ) -> None:
            total_seconds = td.total_seconds()
            proto.seconds = int(total_seconds)
            proto.nanos = int((total_seconds - int(total_seconds)) * 1_000_000_000)

        if execution_timeout:
            set_duration(
                cmd.start_child_workflow_execution.workflow_execution_timeout,
                execution_timeout,
            )
        if run_timeout:
            set_duration(
                cmd.start_child_workflow_execution.workflow_run_timeout, run_timeout
            )
        if task_timeout:
            set_duration(
                cmd.start_child_workflow_execution.workflow_task_timeout, task_timeout
            )

        self.completion.successful.commands.append(cmd)
        return self

    def cancel_child_workflow(self, seq: int) -> "CompletionBuilder":
        """Add a CancelChildWorkflowExecution command."""
        cmd = cmd_pb.WorkflowCommand()
        cmd.cancel_child_workflow_execution.child_workflow_seq = seq
        self.completion.successful.commands.append(cmd)
        return self

    def continue_as_new(
        self,
        workflow_type: str,
        args: tuple[Any, ...] = (),
        task_queue: str | None = None,
    ) -> "CompletionBuilder":
        """Add a ContinueAsNewWorkflowExecution command."""
        cmd = cmd_pb.WorkflowCommand()
        cmd.continue_as_new_workflow_execution.workflow_type = workflow_type
        if task_queue:
            cmd.continue_as_new_workflow_execution.task_queue = task_queue

        for arg in args:
            payload = self.data_converter.payload_converter.to_payload(arg)
            cmd.continue_as_new_workflow_execution.arguments.append(payload)

        self.completion.successful.commands.append(cmd)
        return self

    def build(self) -> bytes:
        """Serialize the completion to bytes."""
        return self.completion.SerializeToString()


class ActivationParser:
    """Helper for parsing workflow activation protobufs."""

    def __init__(
        self,
        activation_bytes: bytes,
        data_converter: temporalio.converter.DataConverter | None = None,
    ):
        self.activation = act_pb.WorkflowActivation()
        self.activation.ParseFromString(activation_bytes)
        self.data_converter = (
            data_converter or temporalio.converter.DataConverter.default
        )

    @property
    def run_id(self) -> str:
        return self.activation.run_id

    @property
    def timestamp_ns(self) -> int:
        return (
            self.activation.timestamp.seconds * 1_000_000_000
            + self.activation.timestamp.nanos
        )

    @property
    def jobs(self) -> list:
        return list(self.activation.jobs)

    def has_job_type(self, job_type: str) -> bool:
        """Check if activation contains a job of the given type."""
        for job in self.activation.jobs:
            if job.WhichOneof("variant") == job_type:
                return True
        return False

    def get_job(self, job_type: str) -> Any:
        """Get the first job of the given type."""
        for job in self.activation.jobs:
            if job.WhichOneof("variant") == job_type:
                return getattr(job, job_type)
        raise ValueError(f"No job of type '{job_type}' found")

    def get_all_jobs(self, job_type: str) -> list:
        """Get all jobs of the given type."""
        result = []
        for job in self.activation.jobs:
            if job.WhichOneof("variant") == job_type:
                result.append(getattr(job, job_type))
        return result

    def decode_payload(self, payload: temporalio.api.common.v1.Payload) -> Any:
        """Decode a payload to Python value."""
        return self.data_converter.payload_converter.from_payload(payload)


def start_workflow_via_cli(
    workflow_id: str,
    workflow_type: str,
    task_queue: str,
    namespace: str = DEFAULT_NAMESPACE,
    args: list[Any] | None = None,
) -> None:
    """Start a workflow using Temporal CLI.

    Args:
        workflow_id: The workflow ID to use
        workflow_type: The workflow type name
        task_queue: Task queue to run workflow on
        namespace: Temporal namespace
        args: List of arguments to pass to the workflow

    Raises:
        RuntimeError: If CLI command fails
    """
    cmd = [
        TEMPORAL_CLI_PATH,
        "workflow",
        "start",
        "--workflow-id",
        workflow_id,
        "--type",
        workflow_type,
        "--task-queue",
        task_queue,
        "--namespace",
        namespace,
    ]

    for arg in args or []:
        cmd.extend(["--input", json.dumps(arg)])

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to start workflow: {result.stderr or result.stdout}"
        )


def signal_workflow_via_cli(
    workflow_id: str,
    signal_name: str,
    args: list[Any] | None = None,
    namespace: str = DEFAULT_NAMESPACE,
) -> None:
    """Send a signal to a workflow using Temporal CLI.

    Args:
        workflow_id: The workflow ID to signal
        signal_name: The signal name
        args: Signal arguments
        namespace: Temporal namespace
    """
    cmd = [
        TEMPORAL_CLI_PATH,
        "workflow",
        "signal",
        "--workflow-id",
        workflow_id,
        "--name",
        signal_name,
        "--namespace",
        namespace,
    ]

    for arg in args or []:
        cmd.extend(["--input", json.dumps(arg)])

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to signal workflow: {result.stderr or result.stdout}"
        )


def query_workflow_via_cli(
    workflow_id: str,
    query_type: str,
    args: list[Any] | None = None,
    namespace: str = DEFAULT_NAMESPACE,
    timeout: int = 30,
) -> dict[str, Any]:
    """Query a workflow using Temporal CLI.

    Args:
        workflow_id: The workflow ID to query
        query_type: The query name
        args: Query arguments
        namespace: Temporal namespace
        timeout: Command timeout in seconds

    Returns:
        Query result as dict
    """
    cmd = [
        TEMPORAL_CLI_PATH,
        "workflow",
        "query",
        "--workflow-id",
        workflow_id,
        "--type",
        query_type,
        "--namespace",
        namespace,
    ]

    for arg in args or []:
        cmd.extend(["--input", json.dumps(arg)])

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to query workflow: {result.stderr or result.stdout}"
        )

    # Parse output - CLI returns the result directly
    try:
        return {"result": json.loads(result.stdout)}
    except json.JSONDecodeError:
        return {"result": result.stdout.strip()}


async def query_workflow_via_cli_async(
    workflow_id: str,
    query_type: str,
    args: list[Any] | None = None,
    namespace: str = DEFAULT_NAMESPACE,
    timeout: int = 30,
) -> dict[str, Any]:
    """Query a workflow using Temporal CLI (async version).

    Args:
        workflow_id: The workflow ID to query
        query_type: The query name
        args: Query arguments
        namespace: Temporal namespace
        timeout: Command timeout in seconds

    Returns:
        Query result as dict
    """
    return await trio.to_thread.run_sync(
        lambda: query_workflow_via_cli(
            workflow_id, query_type, args, namespace, timeout
        )
    )


def cancel_workflow_via_cli(
    workflow_id: str,
    namespace: str = DEFAULT_NAMESPACE,
) -> None:
    """Cancel a workflow using Temporal CLI.

    Args:
        workflow_id: The workflow ID to cancel
        namespace: Temporal namespace
    """
    cmd = [
        TEMPORAL_CLI_PATH,
        "workflow",
        "cancel",
        "--workflow-id",
        workflow_id,
        "--namespace",
        namespace,
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to cancel workflow: {result.stderr or result.stdout}"
        )


class CompletionHistory:
    """Tracks completion history per run_id for replay handling.

    When SDK-Core evicts a workflow and replays it, we need to re-send
    the same completion commands that were originally sent. This class
    records all completions and can replay them automatically.
    """

    def __init__(self) -> None:
        # Map from run_id to list of completion-builder callables
        self._history: dict[str, list[Callable[[str], bytes]]] = {}

    def record(self, run_id: str, build_fn: "Callable[[str], bytes]") -> None:
        """Record a completion for a run_id."""
        if run_id not in self._history:
            self._history[run_id] = []
        self._history[run_id].append(build_fn)

    def get_replay_commands(self, run_id: str) -> "list[Callable[[str], bytes]]":
        """Get all recorded completions for a run_id."""
        return self._history.get(run_id, [])

    def has_history(self, run_id: str) -> bool:
        """Check if any history exists for a run_id."""
        return run_id in self._history and len(self._history[run_id]) > 0


async def poll_and_handle_eviction(
    bridge: TrioBridgeWrapper,
    run_id: str,
    timeout: float = DEFAULT_TIMEOUT,
    replay_commands: "list[Callable[[str], bytes]] | None" = None,
    history: "CompletionHistory | None" = None,
) -> "ActivationParser":
    """Poll for workflow activation, handling cache eviction and replay if needed.

    If a remove_from_cache activation is received, complete it and poll again.
    If replay_commands is provided (or history has commands for the run_id) and
    we get an initialize_workflow with is_replaying=True, we replay each command
    sequence and poll again until we get a non-replay, non-eviction activation.

    Args:
        bridge: The bridge wrapper
        run_id: The expected run_id (used for completing eviction)
        timeout: Poll timeout
        replay_commands: Optional list of callables, each taking run_id and
            returning completion bytes. When a replay initialize_workflow is
            received, these are sent in order (each followed by a poll) to
            replay the workflow history before returning the final activation.
        history: Optional CompletionHistory to look up replay commands by run_id.
            If both replay_commands and history are provided, replay_commands
            takes precedence.

    Returns:
        ActivationParser for the non-eviction, non-replay activation
    """
    while True:
        activation_bytes = await bridge.poll_workflow_activation(timeout=timeout)
        activation = ActivationParser(activation_bytes)

        if activation.has_job_type("remove_from_cache"):
            # Handle cache eviction - complete with empty commands
            evict_run_id = activation.run_id
            completion = CompletionBuilder(evict_run_id).build()
            await bridge.complete_workflow_activation(completion, timeout=timeout)
            print(f"Handled cache eviction for run_id={evict_run_id}")
            continue

        # Determine replay commands to use
        cmds = replay_commands
        if (
            cmds is None
            and history is not None
            and activation.has_job_type("initialize_workflow")
            and activation.activation.is_replaying
        ):
            cmds = history.get_replay_commands(activation.run_id)

        if (
            cmds
            and activation.has_job_type("initialize_workflow")
            and activation.activation.is_replaying
        ):
            # Handle replay by re-sending the original commands
            replay_run_id = activation.run_id
            print(f"Handling replay for run_id={replay_run_id[:8]}...")
            for i, build_completion in enumerate(cmds):
                completion_bytes = build_completion(replay_run_id)
                await bridge.complete_workflow_activation(
                    completion_bytes, timeout=timeout
                )
                print(f"  Replayed command {i + 1}/{len(cmds)}")
                if i < len(cmds) - 1:
                    # Poll for next replay activation between commands
                    next_bytes = await bridge.poll_workflow_activation(timeout=timeout)
                    next_act = ActivationParser(next_bytes)
                    if next_act.has_job_type("remove_from_cache"):
                        evict_completion = CompletionBuilder(next_act.run_id).build()
                        await bridge.complete_workflow_activation(
                            evict_completion, timeout=timeout
                        )
            continue

        return activation


def get_workflow_status_via_cli(
    workflow_id: str,
    namespace: str = DEFAULT_NAMESPACE,
) -> str:
    """Get workflow status using Temporal CLI.

    Args:
        workflow_id: The workflow ID to check
        namespace: Temporal namespace

    Returns:
        Workflow status string (e.g., "COMPLETED", "RUNNING", "FAILED")
    """
    cmd = [
        TEMPORAL_CLI_PATH,
        "workflow",
        "describe",
        "--workflow-id",
        workflow_id,
        "--namespace",
        namespace,
        "--output",
        "json",
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to describe workflow: {result.stderr or result.stdout}"
        )

    data = json.loads(result.stdout)
    status = data.get("workflowExecutionInfo", {}).get("status", "UNKNOWN")
    if status.startswith("WORKFLOW_EXECUTION_STATUS_"):
        status = status.replace("WORKFLOW_EXECUTION_STATUS_", "")
    return status
