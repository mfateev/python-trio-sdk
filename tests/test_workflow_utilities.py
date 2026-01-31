"""Tests for workflow utilities (uuid4, random, memo, now) - Phase 2."""

import uuid
from datetime import datetime, timezone
from random import Random

import pytest

from temporalio_trio import workflow
from temporalio_trio.worker import (
    TrioWorkflowInstance,
    WorkflowInstanceDetails,
)


# Test workflows
@workflow.defn
class SimpleWorkflow:
    """A simple workflow for testing."""

    @workflow.run
    async def run(self) -> str:
        return "done"


def _create_simple_details(
    workflow_cls: type = SimpleWorkflow,
    workflow_id: str = "test-wf-1",
    run_id: str = "run-1",
    task_queue: str = "test-queue",
    randomness_seed: int = 12345,
    raw_memo: dict | None = None,
) -> WorkflowInstanceDetails:
    """Helper to create WorkflowInstanceDetails for tests."""
    defn = workflow._Definition.must_from_class(workflow_cls)
    info = workflow.Info(
        workflow_id=workflow_id,
        workflow_type=defn.name,
        run_id=run_id,
        task_queue=task_queue,
        raw_memo=raw_memo or {},
    )
    return WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=randomness_seed,
    )


class TestWorkflowRandom:
    """Tests for workflow.random() function."""

    def test_returns_random_instance(self) -> None:
        """Test random() returns a Random instance."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            rand = workflow.random()
            assert isinstance(rand, Random)
        finally:
            workflow._Runtime.reset_current(token)

    def test_deterministic_with_same_seed(self) -> None:
        """Test random() returns deterministic values with same seed."""
        details1 = _create_simple_details(randomness_seed=12345)
        details2 = _create_simple_details(randomness_seed=12345)

        instance1 = TrioWorkflowInstance(details1)
        instance2 = TrioWorkflowInstance(details2)

        token1 = workflow._Runtime.set_current(instance1)
        try:
            values1 = [workflow.random().randint(0, 1000000) for _ in range(5)]
        finally:
            workflow._Runtime.reset_current(token1)

        token2 = workflow._Runtime.set_current(instance2)
        try:
            values2 = [workflow.random().randint(0, 1000000) for _ in range(5)]
        finally:
            workflow._Runtime.reset_current(token2)

        assert values1 == values2

    def test_different_with_different_seeds(self) -> None:
        """Test random() returns different values with different seeds."""
        details1 = _create_simple_details(randomness_seed=12345)
        details2 = _create_simple_details(randomness_seed=67890)

        instance1 = TrioWorkflowInstance(details1)
        instance2 = TrioWorkflowInstance(details2)

        token1 = workflow._Runtime.set_current(instance1)
        try:
            value1 = workflow.random().randint(0, 1000000)
        finally:
            workflow._Runtime.reset_current(token1)

        token2 = workflow._Runtime.set_current(instance2)
        try:
            value2 = workflow.random().randint(0, 1000000)
        finally:
            workflow._Runtime.reset_current(token2)

        assert value1 != value2

    def test_same_instance_across_calls(self) -> None:
        """Test random() returns the same Random instance."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            rand1 = workflow.random()
            rand2 = workflow.random()
            assert rand1 is rand2
        finally:
            workflow._Runtime.reset_current(token)

    def test_raises_outside_workflow_context(self) -> None:
        """Test random() raises when not in workflow context."""
        with pytest.raises(workflow._NotInWorkflowContextError):
            workflow.random()


class TestWorkflowUuid4:
    """Tests for workflow.uuid4() function."""

    def test_returns_uuid(self) -> None:
        """Test uuid4() returns a UUID instance."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            result = workflow.uuid4()
            assert isinstance(result, uuid.UUID)
        finally:
            workflow._Runtime.reset_current(token)

    def test_returns_v4_uuid(self) -> None:
        """Test uuid4() returns a version 4 UUID."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            result = workflow.uuid4()
            assert result.version == 4
        finally:
            workflow._Runtime.reset_current(token)

    def test_deterministic_with_same_seed(self) -> None:
        """Test uuid4() returns deterministic UUIDs with same seed."""
        details1 = _create_simple_details(randomness_seed=12345)
        details2 = _create_simple_details(randomness_seed=12345)

        instance1 = TrioWorkflowInstance(details1)
        instance2 = TrioWorkflowInstance(details2)

        token1 = workflow._Runtime.set_current(instance1)
        try:
            uuid1 = workflow.uuid4()
        finally:
            workflow._Runtime.reset_current(token1)

        token2 = workflow._Runtime.set_current(instance2)
        try:
            uuid2 = workflow.uuid4()
        finally:
            workflow._Runtime.reset_current(token2)

        assert uuid1 == uuid2

    def test_different_with_different_seeds(self) -> None:
        """Test uuid4() returns different UUIDs with different seeds."""
        details1 = _create_simple_details(randomness_seed=12345)
        details2 = _create_simple_details(randomness_seed=67890)

        instance1 = TrioWorkflowInstance(details1)
        instance2 = TrioWorkflowInstance(details2)

        token1 = workflow._Runtime.set_current(instance1)
        try:
            uuid1 = workflow.uuid4()
        finally:
            workflow._Runtime.reset_current(token1)

        token2 = workflow._Runtime.set_current(instance2)
        try:
            uuid2 = workflow.uuid4()
        finally:
            workflow._Runtime.reset_current(token2)

        assert uuid1 != uuid2

    def test_sequential_calls_different(self) -> None:
        """Test sequential uuid4() calls return different UUIDs."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            uuid1 = workflow.uuid4()
            uuid2 = workflow.uuid4()
            assert uuid1 != uuid2
        finally:
            workflow._Runtime.reset_current(token)

    def test_raises_outside_workflow_context(self) -> None:
        """Test uuid4() raises when not in workflow context."""
        with pytest.raises(workflow._NotInWorkflowContextError):
            workflow.uuid4()


class TestWorkflowNow:
    """Tests for workflow.now() function."""

    def test_returns_datetime(self) -> None:
        """Test now() returns a datetime instance."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)
        instance._time_ns = 1_000_000_000_000_000_000  # 1 billion seconds (from epoch)

        token = workflow._Runtime.set_current(instance)
        try:
            result = workflow.now()
            assert isinstance(result, datetime)
        finally:
            workflow._Runtime.reset_current(token)

    def test_returns_utc_datetime(self) -> None:
        """Test now() returns a UTC timezone-aware datetime."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)
        instance._time_ns = 1_000_000_000_000_000_000

        token = workflow._Runtime.set_current(instance)
        try:
            result = workflow.now()
            assert result.tzinfo == timezone.utc
        finally:
            workflow._Runtime.reset_current(token)

    def test_matches_workflow_time(self) -> None:
        """Test now() matches workflow time from time()."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)
        instance._time_ns = 1_609_459_200_000_000_000  # 2021-01-01 00:00:00 UTC

        token = workflow._Runtime.set_current(instance)
        try:
            now_result = workflow.now()
            time_result = workflow.time()
            expected = datetime.fromtimestamp(time_result, timezone.utc)
            assert now_result == expected
        finally:
            workflow._Runtime.reset_current(token)

    def test_specific_timestamp(self) -> None:
        """Test now() returns correct datetime for specific timestamp."""
        details = _create_simple_details()
        instance = TrioWorkflowInstance(details)
        # 2021-01-01 00:00:00 UTC
        instance._time_ns = 1_609_459_200_000_000_000

        token = workflow._Runtime.set_current(instance)
        try:
            result = workflow.now()
            assert result.year == 2021
            assert result.month == 1
            assert result.day == 1
            assert result.hour == 0
            assert result.minute == 0
            assert result.second == 0
        finally:
            workflow._Runtime.reset_current(token)

    def test_raises_outside_workflow_context(self) -> None:
        """Test now() raises when not in workflow context."""
        with pytest.raises(workflow._NotInWorkflowContextError):
            workflow.now()


class TestWorkflowMemo:
    """Tests for workflow.memo() function."""

    def test_returns_empty_mapping_when_no_memo(self) -> None:
        """Test memo() returns empty mapping when no memo set."""
        details = _create_simple_details(raw_memo={})
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            result = workflow.memo()
            assert result == {}
            assert len(result) == 0
        finally:
            workflow._Runtime.reset_current(token)

    def test_returns_memo_values(self) -> None:
        """Test memo() returns memo values."""
        memo_data = {"key1": "value1", "key2": 42, "key3": [1, 2, 3]}
        details = _create_simple_details(raw_memo=memo_data)
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            result = workflow.memo()
            assert result["key1"] == "value1"
            assert result["key2"] == 42
            assert result["key3"] == [1, 2, 3]
        finally:
            workflow._Runtime.reset_current(token)

    def test_returns_same_mapping(self) -> None:
        """Test memo() returns the same mapping object."""
        memo_data = {"key": "value"}
        details = _create_simple_details(raw_memo=memo_data)
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            memo1 = workflow.memo()
            memo2 = workflow.memo()
            assert memo1 is memo2
        finally:
            workflow._Runtime.reset_current(token)

    def test_raises_outside_workflow_context(self) -> None:
        """Test memo() raises when not in workflow context."""
        with pytest.raises(workflow._NotInWorkflowContextError):
            workflow.memo()


class TestWorkflowMemoValue:
    """Tests for workflow.memo_value() function."""

    def test_returns_value_for_existing_key(self) -> None:
        """Test memo_value() returns value for existing key."""
        memo_data = {"key1": "value1", "key2": 42}
        details = _create_simple_details(raw_memo=memo_data)
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            result = workflow.memo_value("key1")
            assert result == "value1"

            result2 = workflow.memo_value("key2")
            assert result2 == 42
        finally:
            workflow._Runtime.reset_current(token)

    def test_raises_keyerror_for_missing_key(self) -> None:
        """Test memo_value() raises KeyError for missing key without default."""
        details = _create_simple_details(raw_memo={"existing": "value"})
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            with pytest.raises(KeyError, match="missing"):
                workflow.memo_value("missing")
        finally:
            workflow._Runtime.reset_current(token)

    def test_returns_default_for_missing_key(self) -> None:
        """Test memo_value() returns default for missing key."""
        details = _create_simple_details(raw_memo={"existing": "value"})
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            result = workflow.memo_value("missing", "default_value")
            assert result == "default_value"
        finally:
            workflow._Runtime.reset_current(token)

    def test_returns_none_as_default(self) -> None:
        """Test memo_value() can use None as explicit default."""
        details = _create_simple_details(raw_memo={"existing": "value"})
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            result = workflow.memo_value("missing", None)
            assert result is None
        finally:
            workflow._Runtime.reset_current(token)

    def test_returns_none_memo_value(self) -> None:
        """Test memo_value() returns None if that's the actual stored value."""
        details = _create_simple_details(raw_memo={"key": None})
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            result = workflow.memo_value("key")
            assert result is None
        finally:
            workflow._Runtime.reset_current(token)

    def test_type_hint_accepted(self) -> None:
        """Test memo_value() accepts type_hint parameter."""
        details = _create_simple_details(raw_memo={"key": "value"})
        instance = TrioWorkflowInstance(details)

        token = workflow._Runtime.set_current(instance)
        try:
            # Type hint is accepted but not used in POC
            result = workflow.memo_value("key", type_hint=str)
            assert result == "value"
        finally:
            workflow._Runtime.reset_current(token)

    def test_raises_outside_workflow_context(self) -> None:
        """Test memo_value() raises when not in workflow context."""
        with pytest.raises(workflow._NotInWorkflowContextError):
            workflow.memo_value("key")


class TestInfoRawMemo:
    """Tests for Info.raw_memo field."""

    def test_default_empty_dict(self) -> None:
        """Test Info.raw_memo defaults to empty dict."""
        info = workflow.Info(
            workflow_id="wf-1",
            workflow_type="TestWorkflow",
            run_id="run-1",
            task_queue="queue-1",
        )
        assert info.raw_memo == {}

    def test_custom_memo(self) -> None:
        """Test Info.raw_memo can be set to custom values."""
        memo_data = {"key1": "value1", "key2": 42}
        info = workflow.Info(
            workflow_id="wf-1",
            workflow_type="TestWorkflow",
            run_id="run-1",
            task_queue="queue-1",
            raw_memo=memo_data,
        )
        assert info.raw_memo == memo_data

    def test_memo_preserved_in_details(self) -> None:
        """Test raw_memo is preserved when creating WorkflowInstanceDetails."""
        memo_data = {"key": "value"}
        details = _create_simple_details(raw_memo=memo_data)
        assert details.info.raw_memo == memo_data


class TestDeterminismSuite:
    """Tests to verify determinism of workflow utilities."""

    def test_uuid4_sequence_deterministic(self) -> None:
        """Test a sequence of uuid4() calls is deterministic."""
        details1 = _create_simple_details(randomness_seed=42)
        details2 = _create_simple_details(randomness_seed=42)

        instance1 = TrioWorkflowInstance(details1)
        instance2 = TrioWorkflowInstance(details2)

        token1 = workflow._Runtime.set_current(instance1)
        try:
            sequence1 = [workflow.uuid4() for _ in range(10)]
        finally:
            workflow._Runtime.reset_current(token1)

        token2 = workflow._Runtime.set_current(instance2)
        try:
            sequence2 = [workflow.uuid4() for _ in range(10)]
        finally:
            workflow._Runtime.reset_current(token2)

        assert sequence1 == sequence2

    def test_mixed_random_and_uuid_deterministic(self) -> None:
        """Test mixed random() and uuid4() calls are deterministic."""
        details1 = _create_simple_details(randomness_seed=42)
        details2 = _create_simple_details(randomness_seed=42)

        instance1 = TrioWorkflowInstance(details1)
        instance2 = TrioWorkflowInstance(details2)

        results1 = []
        token1 = workflow._Runtime.set_current(instance1)
        try:
            results1.append(workflow.uuid4())
            results1.append(workflow.random().randint(0, 1000))
            results1.append(workflow.uuid4())
            results1.append(workflow.random().random())
        finally:
            workflow._Runtime.reset_current(token1)

        results2 = []
        token2 = workflow._Runtime.set_current(instance2)
        try:
            results2.append(workflow.uuid4())
            results2.append(workflow.random().randint(0, 1000))
            results2.append(workflow.uuid4())
            results2.append(workflow.random().random())
        finally:
            workflow._Runtime.reset_current(token2)

        assert results1 == results2
