"""Tests for upsert_search_attributes functionality."""

from datetime import datetime, timezone

import pytest
from temporalio.common import SearchAttributeKey

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

# Typed search attribute keys used across tests
KW_KEY = SearchAttributeKey.for_keyword("CustomKeywordField")
INT_KEY = SearchAttributeKey.for_int("CustomIntField")


@workflow.defn
class SearchAttributeWorkflow:
    """Test workflow that upserts search attributes."""

    @workflow.run
    async def run(self) -> str:
        workflow.upsert_search_attributes(
            [
                KW_KEY.value_set("test-value"),
                INT_KEY.value_set(42),
            ]
        )
        return "done"


@workflow.defn
class MultipleUpsertWorkflow:
    """Test workflow that upserts search attributes multiple times."""

    @workflow.run
    async def run(self) -> str:
        # First upsert
        workflow.upsert_search_attributes(
            [
                KW_KEY.value_set("initial"),
            ]
        )

        # Second upsert (should update)
        workflow.upsert_search_attributes(
            [
                KW_KEY.value_set("updated"),
                INT_KEY.value_set(100),
            ]
        )

        return "done"


@workflow.defn
class EmptyUpsertWorkflow:
    """Test workflow that calls upsert with empty sequence."""

    @workflow.run
    async def run(self) -> str:
        # Empty upsert should be a no-op
        workflow.upsert_search_attributes([])
        return "done"


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


def _updates_to_dict(updates):
    """Convert typed search attribute updates to a dict for easy assertion."""
    return {u.key.name: u.value for u in updates}


def test_upsert_search_attributes_basic():
    """Test basic upsert_search_attributes functionality."""
    details = _create_details(SearchAttributeWorkflow)
    instance = TrioWorkflowInstance(details)

    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="SearchAttributeWorkflow", args=())],
        timestamp_ns=1000000,
        run_id="run-1",
        is_replaying=False,
    )

    completion = instance.activate(activation)

    # Check completion has UpsertSearchAttributes command
    assert len(completion.commands) == 2  # Upsert + Complete

    upsert_cmd = completion.commands[0]
    assert isinstance(upsert_cmd, UpsertSearchAttributesCommand)
    assert _updates_to_dict(upsert_cmd.search_attributes) == {
        "CustomKeywordField": "test-value",
        "CustomIntField": 42,
    }


def test_upsert_search_attributes_multiple():
    """Test multiple upsert_search_attributes calls in one workflow."""
    details = _create_details(MultipleUpsertWorkflow)
    instance = TrioWorkflowInstance(details)

    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="MultipleUpsertWorkflow", args=())],
        timestamp_ns=1000000,
        run_id="run-1",
        is_replaying=False,
    )

    completion = instance.activate(activation)

    # Check completion has both UpsertSearchAttributes commands + Complete
    assert len(completion.commands) == 3  # Upsert + Upsert + Complete

    upsert_cmd1 = completion.commands[0]
    assert isinstance(upsert_cmd1, UpsertSearchAttributesCommand)
    assert _updates_to_dict(upsert_cmd1.search_attributes) == {
        "CustomKeywordField": "initial",
    }

    upsert_cmd2 = completion.commands[1]
    assert isinstance(upsert_cmd2, UpsertSearchAttributesCommand)
    assert _updates_to_dict(upsert_cmd2.search_attributes) == {
        "CustomKeywordField": "updated",
        "CustomIntField": 100,
    }


def test_upsert_search_attributes_empty():
    """Test upsert_search_attributes with empty sequence is a no-op."""
    details = _create_details(EmptyUpsertWorkflow)
    instance = TrioWorkflowInstance(details)

    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="EmptyUpsertWorkflow", args=())],
        timestamp_ns=1000000,
        run_id="run-1",
        is_replaying=False,
    )

    completion = instance.activate(activation)

    # Empty sequence should not create a command
    assert len(completion.commands) == 1  # Only Complete
    assert not any(
        isinstance(cmd, UpsertSearchAttributesCommand) for cmd in completion.commands
    )


def test_upsert_search_attributes_outside_workflow():
    """Test upsert_search_attributes raises error outside workflow context."""
    with pytest.raises(workflow._NotInWorkflowContextError):
        workflow.upsert_search_attributes([KW_KEY.value_set("value")])


def test_upsert_search_attributes_various_types():
    """Test upsert_search_attributes with all 7 key types."""
    text_key = SearchAttributeKey.for_text("TextField")
    kw_key = SearchAttributeKey.for_keyword("KeywordField")
    int_key = SearchAttributeKey.for_int("IntField")
    float_key = SearchAttributeKey.for_float("FloatField")
    bool_key = SearchAttributeKey.for_bool("BoolField")
    dt_key = SearchAttributeKey.for_datetime("DatetimeField")
    kwlist_key = SearchAttributeKey.for_keyword_list("KeywordListField")

    dt_val = datetime(2024, 1, 1, 12, 0, 0)

    @workflow.defn
    class AllTypesWorkflow:
        @workflow.run
        async def run(self) -> str:
            workflow.upsert_search_attributes(
                [
                    text_key.value_set("some text"),
                    kw_key.value_set("keyword"),
                    int_key.value_set(123),
                    float_key.value_set(45.67),
                    bool_key.value_set(True),
                    dt_key.value_set(dt_val),
                    kwlist_key.value_set(["item1", "item2"]),
                ]
            )
            return "done"

    details = _create_details(AllTypesWorkflow)
    instance = TrioWorkflowInstance(details)

    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="AllTypesWorkflow", args=())],
        timestamp_ns=1000000,
        run_id="run-1",
        is_replaying=False,
    )

    completion = instance.activate(activation)

    assert len(completion.commands) == 2  # Upsert + Complete
    upsert_cmd = completion.commands[0]
    assert isinstance(upsert_cmd, UpsertSearchAttributesCommand)

    attrs = _updates_to_dict(upsert_cmd.search_attributes)
    assert attrs["TextField"] == "some text"
    assert attrs["KeywordField"] == "keyword"
    assert attrs["IntField"] == 123
    assert attrs["FloatField"] == 45.67
    assert attrs["BoolField"] is True
    assert attrs["DatetimeField"] == dt_val
    assert attrs["KeywordListField"] == ["item1", "item2"]


def test_upsert_search_attributes_replay():
    """Test upsert_search_attributes during replay doesn't cause issues."""
    details = _create_details(SearchAttributeWorkflow)
    instance = TrioWorkflowInstance(details)

    # First activation (initial run)
    activation1 = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="SearchAttributeWorkflow", args=())],
        timestamp_ns=1000000,
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
        timestamp_ns=1000000,
        run_id="run-1",
        is_replaying=True,
    )

    completion2 = instance_replay.activate(activation2)

    # Should produce same commands during replay
    assert len(completion2.commands) == 2  # Upsert + Complete
    assert isinstance(completion2.commands[0], UpsertSearchAttributesCommand)
    assert _updates_to_dict(completion2.commands[0].search_attributes) == {
        "CustomKeywordField": "test-value",
        "CustomIntField": 42,
    }


def test_upsert_search_attributes_value_unset():
    """Test value_unset() produces None in update."""

    @workflow.defn
    class UnsetWorkflow:
        @workflow.run
        async def run(self) -> str:
            workflow.upsert_search_attributes(
                [
                    KW_KEY.value_unset(),
                ]
            )
            return "done"

    details = _create_details(UnsetWorkflow)
    instance = TrioWorkflowInstance(details)

    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="UnsetWorkflow", args=())],
        timestamp_ns=1000000,
        run_id="run-1",
        is_replaying=False,
    )

    completion = instance.activate(activation)

    assert len(completion.commands) == 2  # Upsert + Complete
    upsert_cmd = completion.commands[0]
    assert isinstance(upsert_cmd, UpsertSearchAttributesCommand)
    # value_unset() should produce None value
    assert _updates_to_dict(upsert_cmd.search_attributes) == {
        "CustomKeywordField": None,
    }


def test_upsert_search_attributes_mixed_set_and_unset():
    """Test mixed value_set() + value_unset() in one call."""

    @workflow.defn
    class MixedWorkflow:
        @workflow.run
        async def run(self) -> str:
            workflow.upsert_search_attributes(
                [
                    KW_KEY.value_set("keep-this"),
                    INT_KEY.value_unset(),
                ]
            )
            return "done"

    details = _create_details(MixedWorkflow)
    instance = TrioWorkflowInstance(details)

    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="MixedWorkflow", args=())],
        timestamp_ns=1000000,
        run_id="run-1",
        is_replaying=False,
    )

    completion = instance.activate(activation)

    assert len(completion.commands) == 2  # Upsert + Complete
    upsert_cmd = completion.commands[0]
    assert isinstance(upsert_cmd, UpsertSearchAttributesCommand)
    assert _updates_to_dict(upsert_cmd.search_attributes) == {
        "CustomKeywordField": "keep-this",
        "CustomIntField": None,
    }
