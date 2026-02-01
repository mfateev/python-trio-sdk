"""Tests for workflow update decorator and types - Phase 2."""

import inspect

import pytest

from temporalio_trio import workflow
from temporalio_trio.worker import (
    DoUpdateJob,
    UpdateAcceptedCommand,
    UpdateCompletedCommand,
    UpdateRejectedCommand,
)


class TestHandlerUnfinishedPolicy:
    """Tests for HandlerUnfinishedPolicy enum."""

    def test_warn_and_abandon_value(self) -> None:
        """Test WARN_AND_ABANDON has value 1."""
        assert workflow.HandlerUnfinishedPolicy.WARN_AND_ABANDON.value == 1

    def test_abandon_value(self) -> None:
        """Test ABANDON has value 2."""
        assert workflow.HandlerUnfinishedPolicy.ABANDON.value == 2


class TestUpdateDefinition:
    """Tests for _UpdateDefinition class."""

    def test_create_with_required_fields(self) -> None:
        """Test _UpdateDefinition with required fields."""

        def handler(self, arg: str) -> str:
            return arg

        defn = workflow._UpdateDefinition(
            name="my_update",
            fn=handler,
            is_method=True,
        )
        assert defn.name == "my_update"
        assert defn.fn is handler
        assert defn.is_method is True
        assert (
            defn.unfinished_policy == workflow.HandlerUnfinishedPolicy.WARN_AND_ABANDON
        )
        assert defn.description is None
        assert defn.validator is None

    def test_create_with_all_fields(self) -> None:
        """Test _UpdateDefinition with all fields."""

        def handler(self, arg: str) -> str:
            return arg

        def validator(self, arg: str) -> None:
            pass

        defn = workflow._UpdateDefinition(
            name="my_update",
            fn=handler,
            is_method=True,
            unfinished_policy=workflow.HandlerUnfinishedPolicy.ABANDON,
            description="My update description",
            validator=validator,
        )
        assert defn.name == "my_update"
        assert defn.unfinished_policy == workflow.HandlerUnfinishedPolicy.ABANDON
        assert defn.description == "My update description"
        assert defn.validator is validator

    def test_dynamic_handler_name_is_none(self) -> None:
        """Test dynamic handler has name=None."""

        def handler(self, name: str, args: list) -> None:
            pass

        defn = workflow._UpdateDefinition(
            name=None,
            fn=handler,
            is_method=True,
        )
        assert defn.name is None

    def test_set_validator(self) -> None:
        """Test set_validator method."""

        def handler(self, arg: str) -> str:
            return arg

        def validator(self, arg: str) -> None:
            pass

        defn = workflow._UpdateDefinition(
            name="my_update",
            fn=handler,
            is_method=True,
        )
        assert defn.validator is None

        defn.set_validator(validator)
        assert defn.validator is validator


class TestUpdateDecorator:
    """Tests for @workflow.update decorator."""

    def test_basic_decorator(self) -> None:
        """Test basic @workflow.update decorator."""

        @workflow.update
        async def my_update(self, value: str) -> str:
            return f"Updated: {value}"

        assert hasattr(my_update, "_update_defn")
        defn = my_update._update_defn
        assert defn.name == "my_update"
        assert defn.is_method is True
        assert (
            defn.unfinished_policy == workflow.HandlerUnfinishedPolicy.WARN_AND_ABANDON
        )

    def test_decorator_with_custom_name(self) -> None:
        """Test @workflow.update with custom name."""

        @workflow.update(name="custom_update_name")
        async def my_update(self, value: str) -> str:
            return value

        defn = my_update._update_defn
        assert defn.name == "custom_update_name"

    def test_decorator_with_description(self) -> None:
        """Test @workflow.update with description."""

        @workflow.update(description="Updates the workflow state")
        async def my_update(self, value: str) -> str:
            return value

        defn = my_update._update_defn
        assert defn.description == "Updates the workflow state"

    def test_decorator_with_abandon_policy(self) -> None:
        """Test @workflow.update with ABANDON policy."""

        @workflow.update(unfinished_policy=workflow.HandlerUnfinishedPolicy.ABANDON)
        async def my_update(self, value: str) -> str:
            return value

        defn = my_update._update_defn
        assert defn.unfinished_policy == workflow.HandlerUnfinishedPolicy.ABANDON

    def test_decorator_dynamic_handler(self) -> None:
        """Test @workflow.update with dynamic=True."""

        @workflow.update(dynamic=True)
        async def handle_all(self, name: str, args: list) -> None:
            pass

        defn = handle_all._update_defn
        assert defn.name is None  # Dynamic handlers have name=None

    def test_decorator_raises_on_name_and_dynamic(self) -> None:
        """Test decorator raises if both name and dynamic are specified."""
        with pytest.raises(ValueError, match="Cannot provide both name and dynamic"):

            @workflow.update(name="my_update", dynamic=True)
            async def my_update(self, value: str) -> str:
                return value

    def test_sync_handler_allowed(self) -> None:
        """Test sync update handlers are allowed."""

        @workflow.update
        def my_update(self, value: str) -> str:
            return f"Updated: {value}"

        assert hasattr(my_update, "_update_defn")

    def test_has_validator_attribute(self) -> None:
        """Test decorated function has validator attribute."""

        @workflow.update
        async def my_update(self, value: str) -> str:
            return value

        assert hasattr(my_update, "validator")
        assert callable(my_update.validator)


class TestUpdateValidator:
    """Tests for update validator pattern."""

    def test_validator_decorator(self) -> None:
        """Test @handler.validator decorator."""

        @workflow.update
        async def my_update(self, value: str) -> str:
            return value

        @my_update.validator
        def validate_my_update(self, value: str) -> None:
            if not value:
                raise ValueError("value required")

        defn = my_update._update_defn
        assert defn.validator is validate_my_update

    def test_validator_returns_none(self) -> None:
        """Test validator should return None."""

        @workflow.update
        async def my_update(self, value: str) -> str:
            return value

        @my_update.validator
        def validate_my_update(self, value: str) -> None:
            pass

        # Validator decorator returns the function
        defn = my_update._update_defn
        assert defn.validator is not None


class TestWorkflowDefnWithUpdates:
    """Tests for @workflow.defn collecting update handlers."""

    def test_workflow_collects_updates(self) -> None:
        """Test @workflow.defn collects @workflow.update methods."""

        @workflow.defn
        class MyWorkflow:
            @workflow.update
            async def my_update(self, value: str) -> str:
                return value

            @workflow.run
            async def run(self) -> None:
                pass

        defn = workflow._Definition.must_from_class(MyWorkflow)
        assert "my_update" in defn.updates
        assert defn.updates["my_update"].name == "my_update"

    def test_workflow_collects_multiple_updates(self) -> None:
        """Test workflow collects multiple update handlers."""

        @workflow.defn
        class MyWorkflow:
            @workflow.update
            async def update_a(self, value: str) -> str:
                return value

            @workflow.update
            async def update_b(self, value: int) -> int:
                return value

            @workflow.run
            async def run(self) -> None:
                pass

        defn = workflow._Definition.must_from_class(MyWorkflow)
        assert len(defn.updates) == 2
        assert "update_a" in defn.updates
        assert "update_b" in defn.updates

    def test_workflow_collects_dynamic_update(self) -> None:
        """Test workflow collects dynamic update handler."""

        @workflow.defn
        class MyWorkflow:
            @workflow.update(dynamic=True)
            async def handle_updates(self, name: str, args: list) -> None:
                pass

            @workflow.run
            async def run(self) -> None:
                pass

        defn = workflow._Definition.must_from_class(MyWorkflow)
        assert None in defn.updates  # Dynamic handler has None key

    def test_workflow_rejects_duplicate_update_names(self) -> None:
        """Test workflow rejects duplicate update names."""
        with pytest.raises(ValueError, match="multiple @workflow.update"):

            @workflow.defn
            class MyWorkflow:
                @workflow.update(name="same_name")
                async def update_a(self, value: str) -> str:
                    return value

                @workflow.update(name="same_name")
                async def update_b(self, value: str) -> str:
                    return value

                @workflow.run
                async def run(self) -> None:
                    pass

    def test_workflow_update_with_custom_name(self) -> None:
        """Test workflow collects update with custom name."""

        @workflow.defn
        class MyWorkflow:
            @workflow.update(name="CustomUpdateName")
            async def my_update(self, value: str) -> str:
                return value

            @workflow.run
            async def run(self) -> None:
                pass

        defn = workflow._Definition.must_from_class(MyWorkflow)
        assert "CustomUpdateName" in defn.updates
        assert "my_update" not in defn.updates


class TestDoUpdateJob:
    """Tests for DoUpdateJob dataclass."""

    def test_create_with_all_fields(self) -> None:
        """Test DoUpdateJob with all fields."""
        job = DoUpdateJob(
            id="update-123",
            protocol_instance_id="proto-456",
            name="my_update",
            args=("arg1", "arg2"),
            run_validator=True,
        )
        assert job.id == "update-123"
        assert job.protocol_instance_id == "proto-456"
        assert job.name == "my_update"
        assert job.args == ("arg1", "arg2")
        assert job.run_validator is True

    def test_default_run_validator(self) -> None:
        """Test DoUpdateJob defaults run_validator to True."""
        job = DoUpdateJob(
            id="update-123",
            protocol_instance_id="proto-456",
            name="my_update",
            args=(),
        )
        assert job.run_validator is True

    def test_run_validator_false(self) -> None:
        """Test DoUpdateJob with run_validator=False."""
        job = DoUpdateJob(
            id="update-123",
            protocol_instance_id="proto-456",
            name="my_update",
            args=(),
            run_validator=False,
        )
        assert job.run_validator is False


class TestUpdateCommands:
    """Tests for update command dataclasses."""

    def test_update_accepted_command(self) -> None:
        """Test UpdateAcceptedCommand."""
        cmd = UpdateAcceptedCommand(protocol_instance_id="proto-123")
        assert cmd.protocol_instance_id == "proto-123"

    def test_update_completed_command(self) -> None:
        """Test UpdateCompletedCommand."""
        cmd = UpdateCompletedCommand(
            protocol_instance_id="proto-123",
            result="success",
        )
        assert cmd.protocol_instance_id == "proto-123"
        assert cmd.result == "success"

    def test_update_rejected_command(self) -> None:
        """Test UpdateRejectedCommand."""
        err = ValueError("invalid input")
        cmd = UpdateRejectedCommand(
            protocol_instance_id="proto-123",
            exception=err,
        )
        assert cmd.protocol_instance_id == "proto-123"
        assert cmd.exception is err


class TestUpdateDefinitionBinding:
    """Tests for _UpdateDefinition bind methods."""

    def test_bind_fn(self) -> None:
        """Test bind_fn returns bound method."""

        @workflow.defn
        class MyWorkflow:
            def __init__(self) -> None:
                self.value = "test"

            @workflow.update
            def my_update(self, arg: str) -> str:
                return f"{self.value}: {arg}"

            @workflow.run
            async def run(self) -> None:
                pass

        defn = workflow._Definition.must_from_class(MyWorkflow)
        update_defn = defn.updates["my_update"]

        instance = MyWorkflow()
        bound_fn = update_defn.bind_fn(instance)

        # Call the bound method
        result = bound_fn("hello")
        assert result == "test: hello"

    def test_bind_validator(self) -> None:
        """Test bind_validator returns bound method."""

        @workflow.defn
        class MyWorkflow:
            def __init__(self) -> None:
                self.min_length = 3

            @workflow.update
            def my_update(self, arg: str) -> str:
                return arg

            @my_update.validator
            def validate_my_update(self, arg: str) -> None:
                if len(arg) < self.min_length:
                    raise ValueError("too short")

            @workflow.run
            async def run(self) -> None:
                pass

        defn = workflow._Definition.must_from_class(MyWorkflow)
        update_defn = defn.updates["my_update"]

        instance = MyWorkflow()
        bound_validator = update_defn.bind_validator(instance)
        assert bound_validator is not None  # Has a validator

        # Should not raise for valid input
        bound_validator("hello")

        # Should raise for invalid input
        with pytest.raises(ValueError, match="too short"):
            bound_validator("ab")

    def test_bind_validator_none_when_no_validator(self) -> None:
        """Test bind_validator returns None when no validator set."""

        @workflow.defn
        class MyWorkflow:
            @workflow.update
            def my_update(self, arg: str) -> str:
                return arg

            @workflow.run
            async def run(self) -> None:
                pass

        defn = workflow._Definition.must_from_class(MyWorkflow)
        update_defn = defn.updates["my_update"]

        instance = MyWorkflow()
        bound_validator = update_defn.bind_validator(instance)
        assert bound_validator is None
