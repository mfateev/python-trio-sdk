"""Tests for upsert_search_attributes functionality."""

import pytest

from temporalio_trio import workflow
from temporalio_trio.worker import (
    TrioWorkflowInstance,
    WorkflowInstanceDetails,
)
from temporalio_trio.worker._activation import (
    UpsertSearchAttributesCommand,
    WorkflowActivation,
    WorkflowStartedJob,
)


@workflow.defn
class SearchAttributeWorkflow:
    """Test workflow that upserts search attributes."""

    @workflow.run
    async def run(self) -> str:
        # Upsert search attributes
        workflow.upsert_search_attributes({
            "CustomKeywordField": "test-value",
            "CustomIntField": 42,
        })
        return "done"


@workflow.defn
class MultipleUpsertWorkflow:
    """Test workflow that upserts search attributes multiple times."""

    @workflow.run
    async def run(self) -> str:
        # First upsert
        workflow.upsert_search_attributes({
            "CustomKeywordField": "initial",
        })

        # Second upsert (should update)
        workflow.upsert_search_attributes({
            "CustomKeywordField": "updated",
            "CustomIntField": 100,
        })

        return "done"


@workflow.defn
class EmptyUpsertWorkflow:
    """Test workflow that calls upsert with empty dict."""

    @workflow.run
    async def run(self) -> str:
        # Empty upsert should be a no-op
        workflow.upsert_search_attributes({})
        return "done"


def _create_details(workflow_cls: type) -> WorkflowInstanceDetails:
    """Helper to create WorkflowInstanceDetails for tests."""
    defn = workflow._Definition.must_from_class(workflow_cls)
    info = workflow.Info(
        workflow_id="test-wf-1",
        workflow_type=defn.name,
        run_id="run-1",
        task_queue="test-queue",
    )
    return WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=12345,
    )


def test_upsert_search_attributes_basic():
    """Test basic upsert_search_attributes functionality."""
    details = _create_details(SearchAttributeWorkflow)
    instance = TrioWorkflowInstance(details)

    # Start workflow
    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="SearchAttributeWorkflow", args=())],
        timestamp_ns=1000000,  # 1ms in nanoseconds
        run_id="run-1",
        is_replaying=False,
    )

    completion = instance.activate(activation)

    # Check completion has UpsertSearchAttributes command
    assert len(completion.commands) == 2  # Upsert + Complete

    # First command should be UpsertSearchAttributes
    upsert_cmd = completion.commands[0]
    assert isinstance(upsert_cmd, UpsertSearchAttributesCommand)
    assert upsert_cmd.search_attributes == {
        "CustomKeywordField": "test-value",
        "CustomIntField": 42,
    }


def test_upsert_search_attributes_multiple():
    """Test multiple upsert_search_attributes calls in one workflow."""
    details = _create_details(MultipleUpsertWorkflow)
    instance = TrioWorkflowInstance(details)

    # Start workflow
    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="MultipleUpsertWorkflow", args=())],
        timestamp_ns=1000000,  # 1ms in nanoseconds
        run_id="run-1",
        is_replaying=False,
    )

    completion = instance.activate(activation)

    # Check completion has both UpsertSearchAttributes commands + Complete
    assert len(completion.commands) == 3  # Upsert + Upsert + Complete

    # First upsert
    upsert_cmd1 = completion.commands[0]
    assert isinstance(upsert_cmd1, UpsertSearchAttributesCommand)
    assert upsert_cmd1.search_attributes == {
        "CustomKeywordField": "initial",
    }

    # Second upsert
    upsert_cmd2 = completion.commands[1]
    assert isinstance(upsert_cmd2, UpsertSearchAttributesCommand)
    assert upsert_cmd2.search_attributes == {
        "CustomKeywordField": "updated",
        "CustomIntField": 100,
    }


def test_upsert_search_attributes_empty():
    """Test upsert_search_attributes with empty dict is a no-op."""
    details = _create_details(EmptyUpsertWorkflow)
    instance = TrioWorkflowInstance(details)

    # Start workflow
    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="EmptyUpsertWorkflow", args=())],
        timestamp_ns=1000000,  # 1ms in nanoseconds
        run_id="run-1",
        is_replaying=False,
    )

    completion = instance.activate(activation)

    # Empty dict should not create a command
    assert len(completion.commands) == 1  # Only Complete
    assert not any(
        isinstance(cmd, UpsertSearchAttributesCommand)
        for cmd in completion.commands
    )


def test_upsert_search_attributes_outside_workflow():
    """Test upsert_search_attributes raises error outside workflow context."""
    with pytest.raises(workflow._NotInWorkflowContextError):
        workflow.upsert_search_attributes({"key": "value"})


def test_upsert_search_attributes_various_types():
    """Test upsert_search_attributes with various value types."""
    from datetime import datetime

    @workflow.defn
    class VariousTypesWorkflow:
        @workflow.run
        async def run(self) -> str:
            workflow.upsert_search_attributes({
                "StringField": "text",
                "IntField": 123,
                "FloatField": 45.67,
                "BoolField": True,
                "DatetimeField": datetime(2024, 1, 1, 12, 0, 0),
                "ListField": ["item1", "item2"],
            })
            return "done"

    details = _create_details(VariousTypesWorkflow)
    instance = TrioWorkflowInstance(details)

    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="VariousTypesWorkflow", args=())],
        timestamp_ns=1000000,  # 1ms in nanoseconds
        run_id="run-1",
        is_replaying=False,
    )

    completion = instance.activate(activation)

    # Check command was created with all types
    assert len(completion.commands) == 2  # Upsert + Complete
    upsert_cmd = completion.commands[0]
    assert isinstance(upsert_cmd, UpsertSearchAttributesCommand)

    attrs = upsert_cmd.search_attributes
    assert attrs["StringField"] == "text"
    assert attrs["IntField"] == 123
    assert attrs["FloatField"] == 45.67
    assert attrs["BoolField"] is True
    assert attrs["DatetimeField"] == datetime(2024, 1, 1, 12, 0, 0)
    assert attrs["ListField"] == ["item1", "item2"]


def test_upsert_search_attributes_replay():
    """Test upsert_search_attributes during replay doesn't cause issues."""
    details = _create_details(SearchAttributeWorkflow)
    instance = TrioWorkflowInstance(details)

    # First activation (initial run)
    activation1 = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="SearchAttributeWorkflow", args=())],
        timestamp_ns=1000000,  # 1ms in nanoseconds
        run_id="run-1",
        is_replaying=False,
    )

    completion1 = instance.activate(activation1)
    assert len(completion1.commands) == 2  # Upsert + Complete

    # Create new instance for replay
    details_replay = _create_details(SearchAttributeWorkflow)
    instance_replay = TrioWorkflowInstance(details_replay)

    # Replay the same activation
    activation2 = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="SearchAttributeWorkflow", args=())],
        timestamp_ns=1000000,  # 1ms in nanoseconds
        run_id="run-1",
        is_replaying=True,  # Replaying
    )

    completion2 = instance_replay.activate(activation2)

    # Should produce same commands during replay
    assert len(completion2.commands) == 2  # Upsert + Complete
    assert isinstance(completion2.commands[0], UpsertSearchAttributesCommand)
    assert completion2.commands[0].search_attributes == {
        "CustomKeywordField": "test-value",
        "CustomIntField": 42,
    }
