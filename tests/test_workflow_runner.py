"""Tests for workflow runner classes (Phase 4)."""

from datetime import datetime, timezone

import pytest

from temporalio_trio import workflow
from temporalio_trio.worker import (
    CompleteWorkflowCommand,
    StartTimerCommand,
    TimerFiredJob,
    TrioWorkflowInstance,
    TrioWorkflowRunner,
    WorkflowActivation,
    WorkflowActivationCompletion,
    WorkflowInstance,
    WorkflowInstanceDetails,
    WorkflowRunner,
    WorkflowStartedJob,
)


# Test workflows
@workflow.defn
class SimpleWorkflow:
    """A simple workflow for testing."""

    @workflow.run
    async def run(self) -> str:
        return "done"


@workflow.defn
class SleepWorkflow:
    """A workflow that sleeps."""

    @workflow.run
    async def run(self, sleep_seconds: float) -> str:
        start = workflow.time()
        await workflow.sleep(sleep_seconds)
        end = workflow.time()
        return f"Slept from {start} to {end}"


@workflow.defn
class InfoWorkflow:
    """A workflow that returns its info."""

    @workflow.run
    async def run(self) -> dict[str, str]:
        wf_info = workflow.info()
        return {
            "workflow_id": wf_info.workflow_id,
            "workflow_type": wf_info.workflow_type,
            "run_id": wf_info.run_id,
            "task_queue": wf_info.task_queue,
        }


def _create_details(
    workflow_cls: type,
    workflow_id: str = "test-wf-1",
    run_id: str = "run-1",
    task_queue: str = "test-queue",
    randomness_seed: int = 12345,
) -> WorkflowInstanceDetails:
    """Helper to create WorkflowInstanceDetails."""
    defn = workflow._Definition.must_from_class(workflow_cls)
    info = workflow.Info(
        workflow_id=workflow_id,
        workflow_type=defn.name or "",
        run_id=run_id,
        task_queue=task_queue,
        namespace="default",
        attempt=1,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    return WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=randomness_seed,
    )


class TestWorkflowRunnerAbstract:
    """Tests for WorkflowRunner abstract base class."""

    def test_is_abstract(self) -> None:
        """Test WorkflowRunner is abstract and cannot be instantiated."""
        with pytest.raises(TypeError, match="abstract"):
            WorkflowRunner()  # type: ignore[abstract]

    def test_has_prepare_workflow_method(self) -> None:
        """Test WorkflowRunner defines prepare_workflow abstract method."""
        assert hasattr(WorkflowRunner, "prepare_workflow")
        assert getattr(WorkflowRunner.prepare_workflow, "__isabstractmethod__", False)

    def test_has_create_instance_method(self) -> None:
        """Test WorkflowRunner defines create_instance abstract method."""
        assert hasattr(WorkflowRunner, "create_instance")
        assert getattr(WorkflowRunner.create_instance, "__isabstractmethod__", False)


class TestTrioWorkflowRunner:
    """Tests for TrioWorkflowRunner implementation."""

    def test_creation(self) -> None:
        """Test TrioWorkflowRunner can be created."""
        runner = TrioWorkflowRunner()
        assert runner is not None
        assert isinstance(runner, WorkflowRunner)

    def test_prepare_workflow(self) -> None:
        """Test prepare_workflow validates and registers workflow."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(SimpleWorkflow)

        runner.prepare_workflow(defn)

        assert defn.name in runner._prepared

    def test_prepare_workflow_multiple(self) -> None:
        """Test prepare_workflow can prepare multiple workflows."""
        runner = TrioWorkflowRunner()
        defn1 = workflow._Definition.must_from_class(SimpleWorkflow)
        defn2 = workflow._Definition.must_from_class(SleepWorkflow)

        runner.prepare_workflow(defn1)
        runner.prepare_workflow(defn2)

        assert defn1.name in runner._prepared
        assert defn2.name in runner._prepared

    def test_prepare_workflow_idempotent(self) -> None:
        """Test prepare_workflow can be called multiple times for same workflow."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(SimpleWorkflow)

        runner.prepare_workflow(defn)
        runner.prepare_workflow(defn)  # Should not raise

        assert defn.name in runner._prepared

    def test_create_instance(self) -> None:
        """Test create_instance returns TrioWorkflowInstance."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(SimpleWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(SimpleWorkflow)
        instance = runner.create_instance(details)

        assert instance is not None
        assert isinstance(instance, WorkflowInstance)
        assert isinstance(instance, TrioWorkflowInstance)

    def test_create_instance_requires_preparation(self) -> None:
        """Test create_instance raises if workflow not prepared."""
        runner = TrioWorkflowRunner()
        details = _create_details(SimpleWorkflow)

        with pytest.raises(ValueError, match="not prepared"):
            runner.create_instance(details)

    def test_create_instance_multiple(self) -> None:
        """Test create_instance can create multiple instances."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(SimpleWorkflow)
        runner.prepare_workflow(defn)

        details1 = _create_details(SimpleWorkflow, workflow_id="wf-1")
        details2 = _create_details(SimpleWorkflow, workflow_id="wf-2")

        instance1 = runner.create_instance(details1)
        instance2 = runner.create_instance(details2)

        assert instance1 is not instance2
        # Cast to TrioWorkflowInstance to access info property
        assert isinstance(instance1, TrioWorkflowInstance)
        assert isinstance(instance2, TrioWorkflowInstance)
        assert instance1.info.workflow_id == "wf-1"
        assert instance2.info.workflow_id == "wf-2"


class TestWorkflowRunnerExecution:
    """Tests for workflow execution through the runner."""

    def test_simple_workflow_execution(self) -> None:
        """Test simple workflow executes through runner."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(SimpleWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(SimpleWorkflow)
        instance = runner.create_instance(details)

        activation = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="SimpleWorkflow", args=())],
            timestamp_ns=0,
        )
        completion = instance.activate(activation)

        assert len(completion.commands) == 1
        assert isinstance(completion.commands[0], CompleteWorkflowCommand)
        assert completion.commands[0].result == "done"

    def test_sleep_workflow_creates_timer(self) -> None:
        """Test workflow.sleep() creates timer command through runner."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(SleepWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(SleepWorkflow)
        instance = runner.create_instance(details)

        activation = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="SleepWorkflow", args=(10.0,))],
            timestamp_ns=0,
        )
        completion = instance.activate(activation)

        # Should have a StartTimerCommand (workflow blocked on sleep)
        assert len(completion.commands) == 1
        assert isinstance(completion.commands[0], StartTimerCommand)
        assert completion.commands[0].duration_ms == 10000

    def test_sleep_workflow_completes_after_timer(self) -> None:
        """Test workflow completes after timer fires."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(SleepWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(SleepWorkflow)
        instance = runner.create_instance(details)

        # Start workflow - will create timer
        activation1 = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="SleepWorkflow", args=(10.0,))],
            timestamp_ns=0,
        )
        completion1 = instance.activate(activation1)

        timer_cmd = completion1.commands[0]
        assert isinstance(timer_cmd, StartTimerCommand)

        # Fire timer - workflow should complete
        activation2 = WorkflowActivation(
            jobs=[TimerFiredJob(timer_id=timer_cmd.timer_id)],
            timestamp_ns=10_000_000_000,  # 10 seconds later
        )
        completion2 = instance.activate(activation2)

        # Should have completion
        assert any(
            isinstance(cmd, CompleteWorkflowCommand) for cmd in completion2.commands
        )
        complete_cmd = next(
            c for c in completion2.commands if isinstance(c, CompleteWorkflowCommand)
        )
        # Note: workflow re-executes from beginning on each activation, so
        # workflow.time() returns the activation timestamp (10.0) during replay.
        # This is expected behavior for this simplified POC.
        assert complete_cmd.result == "Slept from 10.0 to 10.0"


class TestWorkflowInfoAPI:
    """Tests for workflow.info() API."""

    def test_info_raises_outside_workflow(self) -> None:
        """Test workflow.info() raises outside workflow context."""
        with pytest.raises(workflow._NotInWorkflowContextError):
            workflow.info()

    def test_info_workflow_returns_correct_info(self) -> None:
        """Test workflow.info() returns correct information."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(InfoWorkflow)
        runner.prepare_workflow(defn)

        details = _create_details(
            InfoWorkflow,
            workflow_id="my-workflow-123",
            run_id="run-456",
            task_queue="my-queue",
        )
        instance = runner.create_instance(details)

        activation = WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type="InfoWorkflow", args=())],
            timestamp_ns=0,
        )
        completion = instance.activate(activation)

        assert len(completion.commands) == 1
        assert isinstance(completion.commands[0], CompleteWorkflowCommand)
        result = completion.commands[0].result
        assert result == {
            "workflow_id": "my-workflow-123",
            "workflow_type": "InfoWorkflow",
            "run_id": "run-456",
            "task_queue": "my-queue",
        }


class TestMultipleWorkflowTypes:
    """Tests for running multiple workflow types."""

    def test_multiple_workflow_types_isolated(self) -> None:
        """Test multiple workflow types can run independently."""
        runner = TrioWorkflowRunner()

        # Prepare both workflows
        defn1 = workflow._Definition.must_from_class(SimpleWorkflow)
        defn2 = workflow._Definition.must_from_class(InfoWorkflow)
        runner.prepare_workflow(defn1)
        runner.prepare_workflow(defn2)

        # Create instances
        details1 = _create_details(SimpleWorkflow, workflow_id="simple-1")
        details2 = _create_details(InfoWorkflow, workflow_id="info-1")
        instance1 = runner.create_instance(details1)
        instance2 = runner.create_instance(details2)

        # Execute both
        completion1 = instance1.activate(
            WorkflowActivation(
                jobs=[WorkflowStartedJob(workflow_type="SimpleWorkflow", args=())],
                timestamp_ns=0,
            )
        )
        completion2 = instance2.activate(
            WorkflowActivation(
                jobs=[WorkflowStartedJob(workflow_type="InfoWorkflow", args=())],
                timestamp_ns=0,
            )
        )

        # Verify results
        assert completion1.commands[0].result == "done"  # type: ignore
        assert completion2.commands[0].result["workflow_id"] == "info-1"  # type: ignore
