"""Tests for deterministic random and UUID generation in workflows.

This tests Phase 1 of the feature parity plan:
- workflow.random() returns a seeded random.Random instance
- workflow.uuid4() generates deterministic UUIDs
- Both functions are deterministic across replays
"""

import random
import uuid
from datetime import datetime, timezone

import pytest

from temporalio_trio.worker._workflow_instance import (
    TrioWorkflowInstance,
    TrioWorkflowRunner,
    WorkflowInstanceDetails,
)
from temporalio_trio.workflow import (
    Info,
    _Definition,
    _NotInWorkflowContextError,
    _Runtime,
    defn,
    run,
)
from temporalio_trio.workflow import random as workflow_random
from temporalio_trio.workflow import uuid4 as workflow_uuid4


def _create_workflow_definition():
    """Create a test workflow definition."""

    @defn
    class TestWorkflow:
        @run
        async def run(self) -> str:
            return "test"

    return _Definition.must_from_class(TestWorkflow)


def _create_test_instance(
    seed: int = 12345,
    workflow_id: str = "wf-1",
    run_id: str = "run-1",
    workflow_type: str = "TestWorkflow",
    task_queue: str = "test-queue",
) -> TrioWorkflowInstance:
    """Create a TrioWorkflowInstance for testing."""
    defn = _create_workflow_definition()
    info = Info(
        workflow_id=workflow_id,
        run_id=run_id,
        workflow_type=workflow_type,
        task_queue=task_queue,
        namespace="default",
        attempt=1,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    det = WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=seed,
    )
    return TrioWorkflowInstance(det)


class TestWorkflowRandom:
    """Tests for workflow.random() function."""

    def test_random_raises_outside_workflow_context(self) -> None:
        """workflow.random() raises if not in workflow context."""
        with pytest.raises(_NotInWorkflowContextError):
            workflow_random()

    def test_random_returns_random_instance(self) -> None:
        """workflow.random() returns a random.Random instance."""
        instance = _create_test_instance()
        token = _Runtime.set_current(instance)
        try:
            rng = workflow_random()
            assert isinstance(rng, random.Random)
        finally:
            _Runtime.reset_current(token)

    def test_random_is_seeded_consistently(self) -> None:
        """Same seed produces same random values."""
        instance1 = _create_test_instance(seed=12345)
        instance2 = _create_test_instance(seed=12345)

        token1 = _Runtime.set_current(instance1)
        try:
            values1 = [workflow_random().randint(0, 1000000) for _ in range(5)]
        finally:
            _Runtime.reset_current(token1)

        token2 = _Runtime.set_current(instance2)
        try:
            values2 = [workflow_random().randint(0, 1000000) for _ in range(5)]
        finally:
            _Runtime.reset_current(token2)

        assert values1 == values2

    def test_random_different_seeds_different_values(self) -> None:
        """Different seeds produce different random values."""
        instance1 = _create_test_instance(seed=12345)
        instance2 = _create_test_instance(seed=67890)

        token1 = _Runtime.set_current(instance1)
        try:
            values1 = [workflow_random().randint(0, 1000000) for _ in range(5)]
        finally:
            _Runtime.reset_current(token1)

        token2 = _Runtime.set_current(instance2)
        try:
            values2 = [workflow_random().randint(0, 1000000) for _ in range(5)]
        finally:
            _Runtime.reset_current(token2)

        assert values1 != values2

    def test_random_returns_same_instance_on_multiple_calls(self) -> None:
        """Multiple calls to workflow.random() return the same instance."""
        instance = _create_test_instance()
        token = _Runtime.set_current(instance)
        try:
            rng1 = workflow_random()
            rng2 = workflow_random()
            assert rng1 is rng2
        finally:
            _Runtime.reset_current(token)

    def test_random_supports_all_standard_methods(self) -> None:
        """workflow.random() supports all standard random.Random methods."""
        instance = _create_test_instance()
        token = _Runtime.set_current(instance)
        try:
            rng = workflow_random()

            # Test common methods
            _ = rng.random()
            _ = rng.randint(1, 100)
            _ = rng.randrange(1, 100)
            _ = rng.uniform(0.0, 1.0)
            _ = rng.choice([1, 2, 3, 4, 5])
            items = [1, 2, 3, 4, 5]
            rng.shuffle(items)
            _ = rng.sample([1, 2, 3, 4, 5], 3)
            _ = rng.gauss(0, 1)
        finally:
            _Runtime.reset_current(token)


class TestWorkflowUuid4:
    """Tests for workflow.uuid4() function."""

    def test_uuid4_raises_outside_workflow_context(self) -> None:
        """workflow.uuid4() raises if not in workflow context."""
        with pytest.raises(_NotInWorkflowContextError):
            workflow_uuid4()

    def test_uuid4_returns_uuid_instance(self) -> None:
        """workflow.uuid4() returns a uuid.UUID instance."""
        instance = _create_test_instance()
        token = _Runtime.set_current(instance)
        try:
            result = workflow_uuid4()
            assert isinstance(result, uuid.UUID)
        finally:
            _Runtime.reset_current(token)

    def test_uuid4_is_version_4(self) -> None:
        """workflow.uuid4() returns a version 4 UUID."""
        instance = _create_test_instance()
        token = _Runtime.set_current(instance)
        try:
            result = workflow_uuid4()
            assert result.version == 4
        finally:
            _Runtime.reset_current(token)

    def test_uuid4_is_deterministic(self) -> None:
        """Same seed produces same UUIDs."""
        instance1 = _create_test_instance(seed=12345)
        instance2 = _create_test_instance(seed=12345)

        token1 = _Runtime.set_current(instance1)
        try:
            uuid1 = workflow_uuid4()
        finally:
            _Runtime.reset_current(token1)

        token2 = _Runtime.set_current(instance2)
        try:
            uuid2 = workflow_uuid4()
        finally:
            _Runtime.reset_current(token2)

        assert uuid1 == uuid2

    def test_uuid4_different_seeds_different_uuids(self) -> None:
        """Different seeds produce different UUIDs."""
        instance1 = _create_test_instance(seed=12345)
        instance2 = _create_test_instance(seed=67890)

        token1 = _Runtime.set_current(instance1)
        try:
            uuid1 = workflow_uuid4()
        finally:
            _Runtime.reset_current(token1)

        token2 = _Runtime.set_current(instance2)
        try:
            uuid2 = workflow_uuid4()
        finally:
            _Runtime.reset_current(token2)

        assert uuid1 != uuid2

    def test_uuid4_multiple_calls_unique(self) -> None:
        """Multiple calls to workflow.uuid4() return unique UUIDs."""
        instance = _create_test_instance()
        token = _Runtime.set_current(instance)
        try:
            uuids = [workflow_uuid4() for _ in range(10)]
            # All should be unique
            assert len(set(uuids)) == 10
        finally:
            _Runtime.reset_current(token)

    def test_uuid4_is_valid_string_format(self) -> None:
        """workflow.uuid4() can be converted to standard UUID string format."""
        instance = _create_test_instance()
        token = _Runtime.set_current(instance)
        try:
            result = workflow_uuid4()
            uuid_str = str(result)
            # Validate format: 8-4-4-4-12 hexadecimal digits
            parts = uuid_str.split("-")
            assert len(parts) == 5
            assert len(parts[0]) == 8
            assert len(parts[1]) == 4
            assert len(parts[2]) == 4
            assert len(parts[3]) == 4
            assert len(parts[4]) == 12
            # All should be hex digits
            for part in parts:
                int(part, 16)
        finally:
            _Runtime.reset_current(token)

    def test_uuid4_variant_is_rfc4122(self) -> None:
        """workflow.uuid4() produces RFC 4122 compliant UUIDs."""
        instance = _create_test_instance()
        token = _Runtime.set_current(instance)
        try:
            result = workflow_uuid4()
            # RFC 4122 variant - variant bits should be 10xx
            # This is indicated by the variant property being RFC_4122
            assert result.variant == uuid.RFC_4122
        finally:
            _Runtime.reset_current(token)


class TestRandomReplayDeterminism:
    """Tests that random values are deterministic across simulated replays."""

    def test_replay_produces_same_random_sequence(self) -> None:
        """Simulating replay produces the same random sequence."""
        seed = 99999

        # First "execution"
        instance1 = _create_test_instance(seed=seed)
        token1 = _Runtime.set_current(instance1)
        try:
            values1 = []
            for _ in range(10):
                values1.append(workflow_random().randint(0, 1000000))
        finally:
            _Runtime.reset_current(token1)

        # Second "execution" with same seed (simulating replay)
        instance2 = _create_test_instance(seed=seed)
        token2 = _Runtime.set_current(instance2)
        try:
            values2 = []
            for _ in range(10):
                values2.append(workflow_random().randint(0, 1000000))
        finally:
            _Runtime.reset_current(token2)

        assert values1 == values2

    def test_replay_produces_same_uuid_sequence(self) -> None:
        """Simulating replay produces the same UUID sequence."""
        seed = 99999

        # First "execution"
        instance1 = _create_test_instance(seed=seed)
        token1 = _Runtime.set_current(instance1)
        try:
            uuids1 = [workflow_uuid4() for _ in range(5)]
        finally:
            _Runtime.reset_current(token1)

        # Second "execution" with same seed (simulating replay)
        instance2 = _create_test_instance(seed=seed)
        token2 = _Runtime.set_current(instance2)
        try:
            uuids2 = [workflow_uuid4() for _ in range(5)]
        finally:
            _Runtime.reset_current(token2)

        assert uuids1 == uuids2


class TestTrioWorkflowInstanceRandom:
    """Tests for workflow_random() method on TrioWorkflowInstance."""

    def test_workflow_random_method_exists(self) -> None:
        """TrioWorkflowInstance has workflow_random() method."""
        instance = _create_test_instance()
        assert hasattr(instance, "workflow_random")
        assert callable(instance.workflow_random)

    def test_workflow_random_returns_seeded_random(self) -> None:
        """workflow_random() returns the seeded Random instance."""
        instance = _create_test_instance(seed=12345)
        rng = instance.workflow_random()
        assert isinstance(rng, random.Random)

    def test_workflow_random_same_seed_same_values(self) -> None:
        """Same seed produces same values via workflow_random()."""
        instance1 = _create_test_instance(seed=12345)
        instance2 = _create_test_instance(seed=12345)

        val1 = instance1.workflow_random().randint(0, 1000000)
        val2 = instance2.workflow_random().randint(0, 1000000)

        assert val1 == val2
