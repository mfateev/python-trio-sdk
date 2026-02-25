"""Tests for workflow patching/versioning (Phase 2 of feature parity plan).

This tests:
- workflow.patched() returns correct value based on replay state and history
- workflow.deprecate_patch() marks patches as deprecated
- Patch markers are correctly recorded in history
- Memoization ensures consistent results within an activation
"""

import random
from datetime import datetime, timezone

import pytest

from temporalio_trio.worker._activation import (
    NotifyHasPatchJob,
    SetPatchMarkerCommand,
    WorkflowActivation,
    WorkflowActivationCompletion,
    WorkflowStartedJob,
)
from temporalio_trio.worker._workflow_instance import (
    TrioWorkflowInstance,
    WorkflowInstanceDetails,
)
from temporalio_trio.workflow import (
    Info,
    _Definition,
    _NotInWorkflowContextError,
    _Runtime,
    defn,
    deprecate_patch,
    patched,
    run,
)


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


class TestPatchedFunction:
    """Tests for workflow.patched() function."""

    def test_patched_raises_outside_workflow_context(self) -> None:
        """patched() raises if not in workflow context."""
        with pytest.raises(_NotInWorkflowContextError):
            patched("my-patch")

    def test_patched_returns_true_on_new_execution(self) -> None:
        """patched() returns True on new (non-replay) execution."""
        instance = _create_test_instance()
        instance._is_replaying = False  # Explicitly not replaying

        token = _Runtime.set_current(instance)
        try:
            result = patched("my-patch-v1")
            assert result is True
        finally:
            _Runtime.reset_current(token)

    def test_patched_emits_set_patch_marker_command(self) -> None:
        """patched() emits SetPatchMarkerCommand on new execution."""
        instance = _create_test_instance()
        instance._is_replaying = False
        instance._commands = []

        token = _Runtime.set_current(instance)
        try:
            patched("my-patch-v1")
        finally:
            _Runtime.reset_current(token)

        # Check that a SetPatchMarkerCommand was emitted
        patch_commands = [
            cmd for cmd in instance._commands if isinstance(cmd, SetPatchMarkerCommand)
        ]
        assert len(patch_commands) == 1
        assert patch_commands[0].patch_id == "my-patch-v1"
        assert patch_commands[0].deprecated is False

    def test_patched_returns_true_during_replay_with_marker(self) -> None:
        """patched() returns True during replay if patch was recorded."""
        instance = _create_test_instance()
        instance._is_replaying = True
        instance._patches_notified.add("my-patch-v1")

        token = _Runtime.set_current(instance)
        try:
            result = patched("my-patch-v1")
            assert result is True
        finally:
            _Runtime.reset_current(token)

    def test_patched_returns_false_during_replay_without_marker(self) -> None:
        """patched() returns False during replay if patch was not recorded."""
        instance = _create_test_instance()
        instance._is_replaying = True
        # Note: my-patch-v1 is NOT in _patches_notified

        token = _Runtime.set_current(instance)
        try:
            result = patched("my-patch-v1")
            assert result is False
        finally:
            _Runtime.reset_current(token)

    def test_patched_does_not_emit_command_during_replay(self) -> None:
        """patched() does not emit command during replay."""
        instance = _create_test_instance()
        instance._is_replaying = True
        instance._patches_notified.add("my-patch-v1")
        instance._commands = []

        token = _Runtime.set_current(instance)
        try:
            patched("my-patch-v1")
        finally:
            _Runtime.reset_current(token)

        # No commands should be emitted during replay
        patch_commands = [
            cmd for cmd in instance._commands if isinstance(cmd, SetPatchMarkerCommand)
        ]
        assert len(patch_commands) == 0

    def test_patched_is_memoized(self) -> None:
        """Multiple calls to patched() with same ID return same result."""
        instance = _create_test_instance()
        instance._is_replaying = False
        instance._commands = []

        token = _Runtime.set_current(instance)
        try:
            result1 = patched("my-patch-v1")
            result2 = patched("my-patch-v1")
            result3 = patched("my-patch-v1")

            assert result1 is True
            assert result2 is True
            assert result3 is True
        finally:
            _Runtime.reset_current(token)

        # Only one command should be emitted (memoization prevents duplicates)
        patch_commands = [
            cmd for cmd in instance._commands if isinstance(cmd, SetPatchMarkerCommand)
        ]
        assert len(patch_commands) == 1

    def test_patched_different_ids_independent(self) -> None:
        """Different patch IDs are tracked independently."""
        instance = _create_test_instance()
        instance._is_replaying = True
        instance._patches_notified.add("patch-a")
        # Note: patch-b is NOT in _patches_notified

        token = _Runtime.set_current(instance)
        try:
            result_a = patched("patch-a")
            result_b = patched("patch-b")

            assert result_a is True
            assert result_b is False
        finally:
            _Runtime.reset_current(token)


class TestDeprecatePatchFunction:
    """Tests for workflow.deprecate_patch() function."""

    def test_deprecate_patch_raises_outside_workflow_context(self) -> None:
        """deprecate_patch() raises if not in workflow context."""
        with pytest.raises(_NotInWorkflowContextError):
            deprecate_patch("my-patch")

    def test_deprecate_patch_emits_deprecated_marker(self) -> None:
        """deprecate_patch() emits SetPatchMarkerCommand with deprecated=True."""
        instance = _create_test_instance()
        instance._is_replaying = False
        instance._commands = []

        token = _Runtime.set_current(instance)
        try:
            deprecate_patch("my-patch-v1")
        finally:
            _Runtime.reset_current(token)

        patch_commands = [
            cmd for cmd in instance._commands if isinstance(cmd, SetPatchMarkerCommand)
        ]
        assert len(patch_commands) == 1
        assert patch_commands[0].patch_id == "my-patch-v1"
        assert patch_commands[0].deprecated is True


class TestNotifyHasPatchJob:
    """Tests for NotifyHasPatchJob handling."""

    def test_notify_has_patch_job_recorded(self) -> None:
        """NotifyHasPatchJob is recorded in _patches_notified."""
        instance = _create_test_instance()

        # Create activation with NotifyHasPatchJob
        activation = WorkflowActivation(
            jobs=[
                NotifyHasPatchJob(patch_id="my-patch-v1"),
                WorkflowStartedJob(workflow_type="TestWorkflow", args=()),
            ],
            timestamp_ns=1000000000,
        )

        # Apply activation
        instance.activate(activation)

        # Check that patch was recorded
        assert "my-patch-v1" in instance._patches_notified

    def test_multiple_notify_has_patch_jobs(self) -> None:
        """Multiple NotifyHasPatchJobs are all recorded."""
        instance = _create_test_instance()

        activation = WorkflowActivation(
            jobs=[
                NotifyHasPatchJob(patch_id="patch-a"),
                NotifyHasPatchJob(patch_id="patch-b"),
                NotifyHasPatchJob(patch_id="patch-c"),
                WorkflowStartedJob(workflow_type="TestWorkflow", args=()),
            ],
            timestamp_ns=1000000000,
        )

        instance.activate(activation)

        assert "patch-a" in instance._patches_notified
        assert "patch-b" in instance._patches_notified
        assert "patch-c" in instance._patches_notified


class TestWorkflowPatchMethod:
    """Tests for TrioWorkflowInstance.workflow_patch() method."""

    def test_workflow_patch_method_exists(self) -> None:
        """TrioWorkflowInstance has workflow_patch() method."""
        instance = _create_test_instance()
        assert hasattr(instance, "workflow_patch")
        assert callable(instance.workflow_patch)

    def test_workflow_patch_new_execution(self) -> None:
        """workflow_patch() returns True on new execution."""
        instance = _create_test_instance()
        instance._is_replaying = False
        result = instance.workflow_patch("my-patch")
        assert result is True

    def test_workflow_patch_replay_with_marker(self) -> None:
        """workflow_patch() returns True during replay with marker."""
        instance = _create_test_instance()
        instance._is_replaying = True
        instance._patches_notified.add("my-patch")
        result = instance.workflow_patch("my-patch")
        assert result is True

    def test_workflow_patch_replay_without_marker(self) -> None:
        """workflow_patch() returns False during replay without marker."""
        instance = _create_test_instance()
        instance._is_replaying = True
        result = instance.workflow_patch("my-patch")
        assert result is False

    def test_workflow_patch_deprecated_flag(self) -> None:
        """workflow_patch() respects deprecated flag."""
        instance = _create_test_instance()
        instance._is_replaying = False
        instance._commands = []

        instance.workflow_patch("my-patch", deprecated=True)

        patch_commands = [
            cmd for cmd in instance._commands if isinstance(cmd, SetPatchMarkerCommand)
        ]
        assert len(patch_commands) == 1
        assert patch_commands[0].deprecated is True


class TestPatchingE2EScenarios:
    """End-to-end scenario tests for patching."""

    def test_evolution_scenario_new_code(self) -> None:
        """New execution always takes new code path."""
        instance = _create_test_instance()
        instance._is_replaying = False
        instance._commands = []

        token = _Runtime.set_current(instance)
        try:
            if patched("my-change-v1"):
                code_path = "new"
            else:
                code_path = "old"
        finally:
            _Runtime.reset_current(token)

        assert code_path == "new"
        # Marker should be recorded
        assert any(
            isinstance(cmd, SetPatchMarkerCommand) and cmd.patch_id == "my-change-v1"
            for cmd in instance._commands
        )

    def test_evolution_scenario_replay_old(self) -> None:
        """Replay of old execution takes old code path."""
        instance = _create_test_instance()
        instance._is_replaying = True
        # No patch marker in history

        token = _Runtime.set_current(instance)
        try:
            if patched("my-change-v1"):
                code_path = "new"
            else:
                code_path = "old"
        finally:
            _Runtime.reset_current(token)

        assert code_path == "old"

    def test_evolution_scenario_replay_new(self) -> None:
        """Replay of new execution takes new code path."""
        instance = _create_test_instance()
        instance._is_replaying = True
        instance._patches_notified.add("my-change-v1")  # Patch marker in history

        token = _Runtime.set_current(instance)
        try:
            if patched("my-change-v1"):
                code_path = "new"
            else:
                code_path = "old"
        finally:
            _Runtime.reset_current(token)

        assert code_path == "new"

    def test_multiple_patches_independent(self) -> None:
        """Multiple patches work independently."""
        instance = _create_test_instance()
        instance._is_replaying = True
        instance._patches_notified.add("patch-1")
        # patch-2 is NOT notified

        token = _Runtime.set_current(instance)
        try:
            result_1 = patched("patch-1")
            result_2 = patched("patch-2")
        finally:
            _Runtime.reset_current(token)

        assert result_1 is True
        assert result_2 is False
