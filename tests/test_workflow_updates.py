"""Unit tests for @workflow.update decorator and update handling."""

from __future__ import annotations

import inspect
from unittest.mock import MagicMock

import pytest
import trio
from temporalio_trio import workflow
from temporalio_trio.worker._activation import (
    UpdateResponseCommand,
    UpdateWorkflowJob,
    WorkflowActivation,
    WorkflowStartedJob,
)
from temporalio_trio.workflow import (
    HandlerUnfinishedPolicy,
    UpdateInfo,
    _Definition,
    _UpdateDefinition,
    current_update_info,
)


# ============================================================================
# Decorator tests
# ============================================================================


class TestUpdateDecorator:
    """Tests for @workflow.update decorator."""

    def test_basic_update(self) -> None:
        """Test basic @workflow.update without arguments."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.update
            async def my_update(self, value: int) -> str:
                return f"updated {value}"

        defn = _Definition.must_from_class(MyWorkflow)
        assert "my_update" in defn.updates
        update_defn = defn.updates["my_update"]
        assert update_defn.name == "my_update"
        assert update_defn.is_method is True
        assert update_defn.unfinished_policy == HandlerUnfinishedPolicy.WARN_AND_ABANDON

    def test_named_update(self) -> None:
        """Test @workflow.update with custom name."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.update(name="custom-update")
            async def my_update(self, value: int) -> str:
                return f"updated {value}"

        defn = _Definition.must_from_class(MyWorkflow)
        assert "custom-update" in defn.updates
        assert "my_update" not in defn.updates

    def test_dynamic_update(self) -> None:
        """Test @workflow.update(dynamic=True)."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.update(dynamic=True)
            async def handle_any_update(self, name: str, args: list) -> str:
                return f"handled {name}"

        defn = _Definition.must_from_class(MyWorkflow)
        assert None in defn.updates
        assert defn.updates[None].name is None

    def test_sync_update_handler(self) -> None:
        """Test sync update handler (not async)."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.update
            def my_update(self, value: int) -> str:
                return f"updated {value}"

        defn = _Definition.must_from_class(MyWorkflow)
        assert "my_update" in defn.updates

    def test_update_with_unfinished_policy(self) -> None:
        """Test @workflow.update with custom unfinished policy."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.update(unfinished_policy=HandlerUnfinishedPolicy.ABANDON)
            async def my_update(self) -> None:
                pass

        defn = _Definition.must_from_class(MyWorkflow)
        update_defn = defn.updates["my_update"]
        assert update_defn.unfinished_policy == HandlerUnfinishedPolicy.ABANDON

    def test_update_with_description(self) -> None:
        """Test @workflow.update with description."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.update(description="My update handler")
            async def my_update(self) -> None:
                pass

        defn = _Definition.must_from_class(MyWorkflow)
        assert defn.updates["my_update"].description == "My update handler"


class TestUpdateValidator:
    """Tests for update validator decorator."""

    def test_validator_attachment(self) -> None:
        """Test attaching a validator to an update handler."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.update
            async def my_update(self, value: int) -> str:
                return f"updated {value}"

            @my_update.validator
            def validate_my_update(self, value: int) -> None:
                if value < 0:
                    raise ValueError("Value must be non-negative")

        defn = _Definition.must_from_class(MyWorkflow)
        update_defn = defn.updates["my_update"]
        assert update_defn.validator is not None

    def test_validator_invocation(self) -> None:
        """Test that validator works when invoked."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.update
            async def my_update(self, value: int) -> str:
                return f"updated {value}"

            @my_update.validator
            def validate_my_update(self, value: int) -> None:
                if value < 0:
                    raise ValueError("Value must be non-negative")

        obj = MyWorkflow()
        defn = _Definition.must_from_class(MyWorkflow)
        validator = defn.updates["my_update"].validator
        bound_validator = validator.__get__(obj, type(obj))

        # Should not raise for valid input
        bound_validator(42)

        # Should raise for invalid input
        with pytest.raises(ValueError, match="non-negative"):
            bound_validator(-1)

    def test_duplicate_validator_raises(self) -> None:
        """Test that setting validator twice raises."""
        defn = _UpdateDefinition(
            name="test",
            fn=lambda: None,
            is_method=False,
        )
        defn.set_validator(lambda: None)
        with pytest.raises(RuntimeError, match="Validator already set"):
            defn.set_validator(lambda: None)


class TestUpdateDefinition:
    """Tests for _UpdateDefinition."""

    def test_creation(self) -> None:
        """Test basic creation."""
        defn = _UpdateDefinition(
            name="test",
            fn=lambda x: x,
            is_method=False,
        )
        assert defn.name == "test"
        assert defn.is_method is False
        assert defn.validator is None
        assert defn.unfinished_policy == HandlerUnfinishedPolicy.WARN_AND_ABANDON

    def test_from_fn(self) -> None:
        """Test from_fn for decorated function."""

        @workflow.update
        async def my_update(self, value: int) -> str:
            return str(value)

        defn = _UpdateDefinition.from_fn(my_update)
        assert defn is not None
        assert defn.name == "my_update"

    def test_from_fn_undecorated(self) -> None:
        """Test from_fn for undecorated function."""

        async def my_func():
            pass

        assert _UpdateDefinition.from_fn(my_func) is None

    def test_type_hints(self) -> None:
        """Test type hint extraction."""

        @workflow.update
        async def my_update(self, value: int, name: str) -> bool:
            return True

        defn = _UpdateDefinition.from_fn(my_update)
        assert defn is not None
        # arg_types are extracted from type hints (excluding self)
        assert defn.arg_types is not None


class TestUpdateInfo:
    """Tests for UpdateInfo and current_update_info."""

    def test_update_info_creation(self) -> None:
        """Test UpdateInfo dataclass."""
        info = UpdateInfo(id="update-1", name="my_update")
        assert info.id == "update-1"
        assert info.name == "my_update"

    def test_update_info_frozen(self) -> None:
        """Test UpdateInfo is immutable."""
        info = UpdateInfo(id="update-1", name="my_update")
        with pytest.raises(AttributeError):
            info.id = "changed"

    def test_current_update_info_outside_handler(self) -> None:
        """Test current_update_info returns None outside handler."""
        assert current_update_info() is None


class TestAllHandlersFinished:
    """Tests for all_handlers_finished."""

    def test_all_handlers_finished_no_runtime(self) -> None:
        """Test all_handlers_finished raises outside workflow context."""
        with pytest.raises(Exception):
            workflow.all_handlers_finished()


class TestDefinitionWithUpdates:
    """Tests for _Definition with updates field."""

    def test_definition_has_updates(self) -> None:
        """Test that _Definition includes updates dict."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.update
            async def update1(self) -> None:
                pass

            @workflow.update(name="custom")
            async def update2(self) -> None:
                pass

        defn = _Definition.must_from_class(MyWorkflow)
        assert len(defn.updates) == 2
        assert "update1" in defn.updates
        assert "custom" in defn.updates

    def test_duplicate_update_raises(self) -> None:
        """Test that duplicate update handlers raise."""
        with pytest.raises(ValueError, match="Duplicate update handler"):

            @workflow.defn
            class MyWorkflow:
                @workflow.run
                async def run(self) -> None:
                    pass

                @workflow.update(name="same")
                async def update1(self) -> None:
                    pass

                @workflow.update(name="same")
                async def update2(self) -> None:
                    pass


# ============================================================================
# Activation type tests
# ============================================================================


class TestUpdateActivationTypes:
    """Tests for UpdateWorkflowJob and UpdateResponseCommand."""

    def test_update_workflow_job(self) -> None:
        """Test UpdateWorkflowJob creation."""
        job = UpdateWorkflowJob(
            id="update-1",
            protocol_instance_id="proto-1",
            name="my_update",
            args=(42,),
            run_validator=True,
        )
        assert job.id == "update-1"
        assert job.protocol_instance_id == "proto-1"
        assert job.name == "my_update"
        assert job.args == (42,)
        assert job.run_validator is True
        assert job.headers == {}

    def test_update_response_accepted(self) -> None:
        """Test UpdateResponseCommand accepted."""
        cmd = UpdateResponseCommand(
            protocol_instance_id="proto-1",
            accepted=True,
        )
        assert cmd.protocol_instance_id == "proto-1"
        assert cmd.accepted is True
        assert cmd.rejected_failure is None
        assert cmd.completed_result is None
        assert cmd._is_completed is False

    def test_update_response_completed(self) -> None:
        """Test UpdateResponseCommand completed."""
        cmd = UpdateResponseCommand(
            protocol_instance_id="proto-1",
            completed_result="result-value",
            _is_completed=True,
        )
        assert cmd._is_completed is True
        assert cmd.completed_result == "result-value"

    def test_update_response_rejected(self) -> None:
        """Test UpdateResponseCommand rejected."""
        err = ValueError("Invalid input")
        cmd = UpdateResponseCommand(
            protocol_instance_id="proto-1",
            rejected_failure=err,
        )
        assert cmd.rejected_failure is err
