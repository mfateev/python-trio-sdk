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
        assert validator is not None
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
            setattr(info, "id", "changed")

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


# ============================================================================
# Additional tests ported from sdk-python
# ============================================================================


class TestMultipleUpdateHandlers:
    """Tests for workflows with multiple update handler types.

    Ported from sdk-python's UpdateHandlersWorkflow definition patterns.
    """

    def test_sync_and_async_handlers(self) -> None:
        """Test workflow can have both sync and async update handlers."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.update
            def sync_update(self) -> str:
                return "sync"

            @workflow.update
            async def async_update(self) -> str:
                return "async"

        defn = _Definition.must_from_class(MyWorkflow)
        assert "sync_update" in defn.updates
        assert "async_update" in defn.updates

    def test_named_and_default_handlers(self) -> None:
        """Test workflow with named and default-named update handlers."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.update
            def default_name(self) -> str:
                return "default"

            @workflow.update(name="custom-name")
            def custom(self) -> str:
                return "custom"

        defn = _Definition.must_from_class(MyWorkflow)
        assert "default_name" in defn.updates
        assert "custom-name" in defn.updates
        assert "custom" not in defn.updates

    def test_update_with_signals_and_queries(self) -> None:
        """Test workflow with updates, signals, and queries together."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.update
            def my_update(self) -> str:
                return "updated"

            @workflow.signal
            def my_signal(self) -> None:
                pass

            @workflow.query
            def my_query(self) -> str:
                return "query"

        defn = _Definition.must_from_class(MyWorkflow)
        assert "my_update" in defn.updates
        assert "my_signal" in defn.signals
        assert "my_query" in defn.queries

    def test_validator_on_named_update(self) -> None:
        """Test validator attached to a named update handler."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> None:
                pass

            @workflow.update(name="set-value")
            def set_value(self, v: int) -> int:
                return v

            @set_value.validator
            def validate(self, v: int) -> None:
                if v < 0:
                    raise ValueError("negative")

        defn = _Definition.must_from_class(MyWorkflow)
        assert "set-value" in defn.updates
        assert defn.updates["set-value"].validator is not None


class TestUpdateDefinitionRetType:
    """Tests for update return type extraction."""

    def test_ret_type_extracted(self) -> None:
        """Test that return type is extracted from type hints."""

        @workflow.update
        async def my_update(self, value: int) -> str:
            return str(value)

        defn = _UpdateDefinition.from_fn(my_update)
        assert defn is not None
        assert defn.ret_type is str

    def test_arg_types_extracted(self) -> None:
        """Test that argument types are extracted from type hints."""

        @workflow.update
        def my_update(self, a: int, b: str) -> bool:
            return True

        defn = _UpdateDefinition.from_fn(my_update)
        assert defn is not None
        assert defn.arg_types is not None
        # Should have types for a and b (excluding self)
        assert len(defn.arg_types) == 2


class TestBridgeTypeConversion:
    """Tests for UpdateWorkflowJob bridge conversion."""

    def test_convert_do_update(self) -> None:
        """Test converting a bridge DoUpdate to UpdateWorkflowJob."""
        import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb
        import temporalio.converter

        from temporalio_trio.worker._bridge_types import bridge_to_poc_activation

        # Build a minimal activation with DoUpdate job
        bridge_act = act_pb.WorkflowActivation()
        bridge_act.run_id = "test-run-id"
        bridge_act.timestamp.seconds = 1000

        job = bridge_act.jobs.add()
        job.do_update.id = "update-123"
        job.do_update.protocol_instance_id = "proto-456"
        job.do_update.name = "my_update"
        job.do_update.run_validator = True

        # Add an input payload
        dc = temporalio.converter.DataConverter.default
        payload = dc.payload_converter.to_payload(42)
        job.do_update.input.append(payload)

        # Convert
        poc_act = bridge_to_poc_activation(bridge_act, dc)

        assert len(poc_act.jobs) == 1
        update_job = poc_act.jobs[0]
        assert isinstance(update_job, UpdateWorkflowJob)
        assert update_job.id == "update-123"
        assert update_job.protocol_instance_id == "proto-456"
        assert update_job.name == "my_update"
        assert update_job.run_validator is True
        assert update_job.args == (42,)

    def test_convert_update_response_accepted(self) -> None:
        """Test converting UpdateResponseCommand (accepted) to bridge."""
        import temporalio.converter

        from temporalio_trio.worker._activation import (
            WorkflowActivationCompletion,
        )
        from temporalio_trio.worker._bridge_types import poc_to_bridge_completion

        dc = temporalio.converter.DataConverter.default
        cmd = UpdateResponseCommand(
            protocol_instance_id="proto-1",
            accepted=True,
        )
        poc_comp = WorkflowActivationCompletion(commands=[cmd])
        bridge_comp = poc_to_bridge_completion("run-1", poc_comp, dc)

        assert len(bridge_comp.successful.commands) == 1
        bridge_cmd = bridge_comp.successful.commands[0]
        assert bridge_cmd.update_response.protocol_instance_id == "proto-1"
        assert bridge_cmd.update_response.HasField("accepted")

    def test_convert_update_response_completed(self) -> None:
        """Test converting UpdateResponseCommand (completed) to bridge."""
        import temporalio.converter

        from temporalio_trio.worker._activation import (
            WorkflowActivationCompletion,
        )
        from temporalio_trio.worker._bridge_types import poc_to_bridge_completion

        dc = temporalio.converter.DataConverter.default
        cmd = UpdateResponseCommand(
            protocol_instance_id="proto-1",
            completed_result="hello",
            _is_completed=True,
        )
        poc_comp = WorkflowActivationCompletion(commands=[cmd])
        bridge_comp = poc_to_bridge_completion("run-1", poc_comp, dc)

        assert len(bridge_comp.successful.commands) == 1
        bridge_cmd = bridge_comp.successful.commands[0]
        assert bridge_cmd.update_response.protocol_instance_id == "proto-1"
        assert bridge_cmd.update_response.HasField("completed")
        # Decode the result
        result = dc.payload_converter.from_payload(bridge_cmd.update_response.completed)
        assert result == "hello"

    def test_convert_update_response_rejected(self) -> None:
        """Test converting UpdateResponseCommand (rejected) to bridge."""
        import temporalio.converter

        from temporalio_trio.worker._activation import (
            WorkflowActivationCompletion,
        )
        from temporalio_trio.worker._bridge_types import poc_to_bridge_completion

        dc = temporalio.converter.DataConverter.default
        cmd = UpdateResponseCommand(
            protocol_instance_id="proto-1",
            rejected_failure=ValueError("bad input"),
        )
        poc_comp = WorkflowActivationCompletion(commands=[cmd])
        bridge_comp = poc_to_bridge_completion("run-1", poc_comp, dc)

        assert len(bridge_comp.successful.commands) == 1
        bridge_cmd = bridge_comp.successful.commands[0]
        assert bridge_cmd.update_response.protocol_instance_id == "proto-1"
        assert bridge_cmd.update_response.HasField("rejected")
        assert "bad input" in bridge_cmd.update_response.rejected.message
