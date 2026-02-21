"""Tests for workflow definition and decorators (Phase 1)."""

import random
from datetime import timedelta
from typing import Any, Callable, NoReturn, Sequence

import pytest
import temporalio.common

from temporalio_trio import workflow
from temporalio_trio.workflow import (
    ChildWorkflowCancellationType,
    ChildWorkflowHandle,
    ParentClosePolicy,
)


class TestWorkflowRunDecorator:
    """Tests for @workflow.run decorator."""

    def test_run_decorator_marks_method(self) -> None:
        """Test @workflow.run marks method with attribute."""

        @workflow.run
        async def my_run() -> str:
            return "done"

        assert getattr(my_run, "__temporal_workflow_run", False) is True

    def test_run_decorator_requires_async(self) -> None:
        """Test @workflow.run requires async function."""
        with pytest.raises(ValueError, match="must be async"):

            @workflow.run
            def not_async() -> str:
                return "done"

    def test_run_decorator_preserves_function(self) -> None:
        """Test @workflow.run preserves the original function."""

        @workflow.run
        async def my_run(x: int) -> int:
            return x * 2

        assert my_run.__name__ == "my_run"


class TestWorkflowDefnDecorator:
    """Tests for @workflow.defn decorator."""

    def test_defn_creates_definition(self) -> None:
        """Test @workflow.defn creates definition."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> str:
                return "done"

        defn = workflow._Definition.from_class(MyWorkflow)
        assert defn is not None
        assert defn.name == "MyWorkflow"
        assert defn.cls is MyWorkflow
        assert defn.run_fn is not None

    def test_defn_with_custom_name(self) -> None:
        """Test @workflow.defn with custom name."""

        @workflow.defn(name="CustomName")
        class MyWorkflow:
            @workflow.run
            async def run(self) -> str:
                return "done"

        defn = workflow._Definition.from_class(MyWorkflow)
        assert defn is not None
        assert defn.name == "CustomName"

    def test_defn_requires_run_method(self) -> None:
        """Test @workflow.defn requires @workflow.run method."""
        with pytest.raises(ValueError, match="must have a @workflow.run method"):

            @workflow.defn
            class BadWorkflow:
                async def run(self) -> str:  # Missing @workflow.run
                    return "done"

    def test_defn_rejects_multiple_run_methods(self) -> None:
        """Test @workflow.defn rejects multiple @workflow.run methods."""
        with pytest.raises(ValueError, match="multiple @workflow.run methods"):

            @workflow.defn
            class BadWorkflow:
                @workflow.run
                async def run1(self) -> str:
                    return "done"

                @workflow.run
                async def run2(self) -> str:
                    return "done"

    def test_defn_without_parentheses(self) -> None:
        """Test @workflow.defn works without parentheses."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> str:
                return "done"

        defn = workflow._Definition.from_class(MyWorkflow)
        assert defn is not None

    def test_defn_with_parentheses_no_args(self) -> None:
        """Test @workflow.defn() works with empty parentheses."""

        @workflow.defn()
        class MyWorkflow:
            @workflow.run
            async def run(self) -> str:
                return "done"

        defn = workflow._Definition.from_class(MyWorkflow)
        assert defn is not None
        assert defn.name == "MyWorkflow"


class TestDefinitionClass:
    """Tests for _Definition dataclass."""

    def test_from_class_returns_none_for_non_workflow(self) -> None:
        """Test from_class returns None for non-workflow class."""

        class NotAWorkflow:
            pass

        assert workflow._Definition.from_class(NotAWorkflow) is None

    def test_must_from_class_raises_for_non_workflow(self) -> None:
        """Test must_from_class raises for non-workflow class."""

        class NotAWorkflow:
            pass

        with pytest.raises(ValueError, match="is not a workflow class"):
            workflow._Definition.must_from_class(NotAWorkflow)

    def test_must_from_class_returns_definition(self) -> None:
        """Test must_from_class returns definition for workflow class."""

        @workflow.defn
        class MyWorkflow:
            @workflow.run
            async def run(self) -> str:
                return "done"

        defn = workflow._Definition.must_from_class(MyWorkflow)
        assert defn.name == "MyWorkflow"


class TestRuntimeClass:
    """Tests for _Runtime abstract class."""

    def test_current_raises_outside_workflow(self) -> None:
        """Test _Runtime.current() raises outside workflow context."""
        with pytest.raises(
            workflow._NotInWorkflowContextError, match="Not in workflow context"
        ):
            workflow._Runtime.current()

    def test_maybe_current_returns_none_outside_workflow(self) -> None:
        """Test _Runtime.maybe_current() returns None outside workflow."""
        assert workflow._Runtime.maybe_current() is None

    def test_set_and_reset_current(self) -> None:
        """Test set_current and reset_current work correctly."""

        # Create a concrete implementation for testing
        class MockRuntime(workflow._Runtime):
            def workflow_time_ns(self) -> int:
                return 12345

            async def workflow_sleep(
                self, duration: float, summary: str | None
            ) -> None:
                pass

            def workflow_info(self) -> workflow.Info:
                return workflow.Info(
                    workflow_id="mock",
                    workflow_type="MockWorkflow",
                    run_id="mock-run",
                    task_queue="mock-queue",
                )

            async def workflow_execute_activity(
                self,
                activity: str | Callable[..., Any],
                *args: Any,
                task_queue: str | None = None,
                schedule_to_close_timeout: timedelta | None = None,
                schedule_to_start_timeout: timedelta | None = None,
                start_to_close_timeout: timedelta | None = None,
                heartbeat_timeout: timedelta | None = None,
                retry_policy: temporalio.common.RetryPolicy | None = None,
                activity_id: str | None = None,
            ) -> Any:
                pass

            async def workflow_start_child_workflow(
                self,
                workflow: str | type,
                *args: Any,
                id: str,
                task_queue: str | None,
                cancellation_type: ChildWorkflowCancellationType,
                parent_close_policy: ParentClosePolicy,
                execution_timeout: timedelta | None,
                run_timeout: timedelta | None,
                task_timeout: timedelta | None,
                id_reuse_policy: temporalio.common.WorkflowIDReusePolicy,
                retry_policy: temporalio.common.RetryPolicy | None,
            ) -> ChildWorkflowHandle[Any, Any]:
                raise NotImplementedError()

            async def workflow_wait_child_workflow(
                self,
                handle: ChildWorkflowHandle[Any, Any],
            ) -> Any:
                raise NotImplementedError()

            async def workflow_wait_condition(
                self,
                fn: Callable[[], bool],
                *,
                timeout: float | None = None,
                timeout_summary: str | None = None,
            ) -> None:
                pass

            def workflow_continue_as_new(
                self,
                *args: Any,
                workflow: str | type | None,
                task_queue: str | None,
                run_timeout: timedelta | None,
                task_timeout: timedelta | None,
                retry_policy: temporalio.common.RetryPolicy | None,
            ) -> NoReturn:
                raise NotImplementedError()

            def workflow_get_external_workflow_handle(
                self,
                workflow_id: str,
                *,
                run_id: str | None,
            ) -> workflow.ExternalWorkflowHandle[Any]:
                raise NotImplementedError()

            async def workflow_signal_external_workflow(
                self,
                workflow_id: str,
                signal_name: str,
                args: Sequence[Any],
                *,
                run_id: str | None,
            ) -> None:
                raise NotImplementedError()

            def workflow_random(self) -> random.Random:
                return random.Random(12345)

            def workflow_patch(
                self, patch_id: str, *, deprecated: bool = False
            ) -> bool:
                return True

        mock = MockRuntime()

        # Initially None
        assert workflow._Runtime.maybe_current() is None

        # Set runtime
        token = workflow._Runtime.set_current(mock)
        assert workflow._Runtime.current() is mock
        assert workflow._Runtime.maybe_current() is mock

        # Reset runtime
        workflow._Runtime.reset_current(token)
        assert workflow._Runtime.maybe_current() is None

    def test_runtime_context_isolation(self) -> None:
        """Test runtime context is isolated per context."""

        class MockRuntime(workflow._Runtime):
            def __init__(self, name: str) -> None:
                self.name = name

            def workflow_time_ns(self) -> int:
                return 0

            async def workflow_sleep(
                self, duration: float, summary: str | None
            ) -> None:
                pass

            def workflow_info(self) -> workflow.Info:
                return workflow.Info(
                    workflow_id="mock",
                    workflow_type="MockWorkflow",
                    run_id="mock-run",
                    task_queue="mock-queue",
                )

            async def workflow_execute_activity(
                self,
                activity: str | Callable[..., Any],
                *args: Any,
                task_queue: str | None = None,
                schedule_to_close_timeout: timedelta | None = None,
                schedule_to_start_timeout: timedelta | None = None,
                start_to_close_timeout: timedelta | None = None,
                heartbeat_timeout: timedelta | None = None,
                retry_policy: temporalio.common.RetryPolicy | None = None,
                activity_id: str | None = None,
            ) -> Any:
                pass

            async def workflow_start_child_workflow(
                self,
                workflow: str | type,
                *args: Any,
                id: str,
                task_queue: str | None,
                cancellation_type: ChildWorkflowCancellationType,
                parent_close_policy: ParentClosePolicy,
                execution_timeout: timedelta | None,
                run_timeout: timedelta | None,
                task_timeout: timedelta | None,
                id_reuse_policy: temporalio.common.WorkflowIDReusePolicy,
                retry_policy: temporalio.common.RetryPolicy | None,
            ) -> ChildWorkflowHandle[Any, Any]:
                raise NotImplementedError()

            async def workflow_wait_child_workflow(
                self,
                handle: ChildWorkflowHandle[Any, Any],
            ) -> Any:
                raise NotImplementedError()

            async def workflow_wait_condition(
                self,
                fn: Callable[[], bool],
                *,
                timeout: float | None = None,
                timeout_summary: str | None = None,
            ) -> None:
                pass

            def workflow_continue_as_new(
                self,
                *args: Any,
                workflow: str | type | None,
                task_queue: str | None,
                run_timeout: timedelta | None,
                task_timeout: timedelta | None,
                retry_policy: temporalio.common.RetryPolicy | None,
            ) -> NoReturn:
                raise NotImplementedError()

            def workflow_get_external_workflow_handle(
                self,
                workflow_id: str,
                *,
                run_id: str | None,
            ) -> workflow.ExternalWorkflowHandle[Any]:
                raise NotImplementedError()

            async def workflow_signal_external_workflow(
                self,
                workflow_id: str,
                signal_name: str,
                args: Sequence[Any],
                *,
                run_id: str | None,
            ) -> None:
                raise NotImplementedError()

            def workflow_random(self) -> random.Random:
                return random.Random(12345)

            def workflow_patch(
                self, patch_id: str, *, deprecated: bool = False
            ) -> bool:
                return True

        mock1 = MockRuntime("mock1")
        mock2 = MockRuntime("mock2")

        # Set mock1
        token1 = workflow._Runtime.set_current(mock1)
        assert workflow._Runtime.current().name == "mock1"  # type: ignore

        # Set mock2 (nested)
        token2 = workflow._Runtime.set_current(mock2)
        assert workflow._Runtime.current().name == "mock2"  # type: ignore

        # Reset to mock1
        workflow._Runtime.reset_current(token2)
        assert workflow._Runtime.current().name == "mock1"  # type: ignore

        # Reset to None
        workflow._Runtime.reset_current(token1)
        assert workflow._Runtime.maybe_current() is None


class TestPublicAPI:
    """Tests for public workflow API functions."""

    @pytest.mark.trio
    async def test_sleep_raises_outside_workflow(self) -> None:
        """Test workflow.sleep() raises outside workflow context."""
        with pytest.raises(workflow._NotInWorkflowContextError):
            await workflow.sleep(1.0)

    @pytest.mark.trio
    async def test_sleep_with_mock_runtime(self) -> None:
        """Test workflow.sleep() delegates to runtime."""
        sleep_calls: list[tuple[float, str | None]] = []

        class MockRuntime(workflow._Runtime):
            def workflow_time_ns(self) -> int:
                return 0

            async def workflow_sleep(
                self, duration: float, summary: str | None
            ) -> None:
                sleep_calls.append((duration, summary))

            def workflow_info(self) -> workflow.Info:
                return workflow.Info(
                    workflow_id="mock",
                    workflow_type="MockWorkflow",
                    run_id="mock-run",
                    task_queue="mock-queue",
                )

            async def workflow_execute_activity(
                self,
                activity: str | Callable[..., Any],
                *args: Any,
                task_queue: str | None = None,
                schedule_to_close_timeout: timedelta | None = None,
                schedule_to_start_timeout: timedelta | None = None,
                start_to_close_timeout: timedelta | None = None,
                heartbeat_timeout: timedelta | None = None,
                retry_policy: temporalio.common.RetryPolicy | None = None,
                activity_id: str | None = None,
            ) -> Any:
                pass

            async def workflow_start_child_workflow(
                self,
                workflow: str | type,
                *args: Any,
                id: str,
                task_queue: str | None,
                cancellation_type: ChildWorkflowCancellationType,
                parent_close_policy: ParentClosePolicy,
                execution_timeout: timedelta | None,
                run_timeout: timedelta | None,
                task_timeout: timedelta | None,
                id_reuse_policy: temporalio.common.WorkflowIDReusePolicy,
                retry_policy: temporalio.common.RetryPolicy | None,
            ) -> ChildWorkflowHandle[Any, Any]:
                raise NotImplementedError()

            async def workflow_wait_child_workflow(
                self,
                handle: ChildWorkflowHandle[Any, Any],
            ) -> Any:
                raise NotImplementedError()

            async def workflow_wait_condition(
                self,
                fn: Callable[[], bool],
                *,
                timeout: float | None = None,
                timeout_summary: str | None = None,
            ) -> None:
                pass

            def workflow_continue_as_new(
                self,
                *args: Any,
                workflow: str | type | None,
                task_queue: str | None,
                run_timeout: timedelta | None,
                task_timeout: timedelta | None,
                retry_policy: temporalio.common.RetryPolicy | None,
            ) -> NoReturn:
                raise NotImplementedError()

            def workflow_get_external_workflow_handle(
                self,
                workflow_id: str,
                *,
                run_id: str | None,
            ) -> workflow.ExternalWorkflowHandle[Any]:
                raise NotImplementedError()

            async def workflow_signal_external_workflow(
                self,
                workflow_id: str,
                signal_name: str,
                args: Sequence[Any],
                *,
                run_id: str | None,
            ) -> None:
                raise NotImplementedError()

            def workflow_random(self) -> random.Random:
                return random.Random(12345)

            def workflow_patch(
                self, patch_id: str, *, deprecated: bool = False
            ) -> bool:
                return True

        mock = MockRuntime()
        token = workflow._Runtime.set_current(mock)
        try:
            await workflow.sleep(5.0)
            await workflow.sleep(10.0, summary="waiting for something")
            assert sleep_calls == [(5.0, None), (10.0, "waiting for something")]
        finally:
            workflow._Runtime.reset_current(token)

    def test_time_raises_outside_workflow(self) -> None:
        """Test workflow.time() raises outside workflow context."""
        with pytest.raises(workflow._NotInWorkflowContextError):
            workflow.time()

    def test_time_ns_raises_outside_workflow(self) -> None:
        """Test workflow.time_ns() raises outside workflow context."""
        with pytest.raises(workflow._NotInWorkflowContextError):
            workflow.time_ns()

    def test_time_with_mock_runtime(self) -> None:
        """Test workflow.time() with mock runtime."""

        class MockRuntime(workflow._Runtime):
            def workflow_time_ns(self) -> int:
                return 5_000_000_000  # 5 seconds in nanoseconds

            async def workflow_sleep(
                self, duration: float, summary: str | None
            ) -> None:
                pass

            def workflow_info(self) -> workflow.Info:
                return workflow.Info(
                    workflow_id="mock",
                    workflow_type="MockWorkflow",
                    run_id="mock-run",
                    task_queue="mock-queue",
                )

            async def workflow_execute_activity(
                self,
                activity: str | Callable[..., Any],
                *args: Any,
                task_queue: str | None = None,
                schedule_to_close_timeout: timedelta | None = None,
                schedule_to_start_timeout: timedelta | None = None,
                start_to_close_timeout: timedelta | None = None,
                heartbeat_timeout: timedelta | None = None,
                retry_policy: temporalio.common.RetryPolicy | None = None,
                activity_id: str | None = None,
            ) -> Any:
                pass

            async def workflow_start_child_workflow(
                self,
                workflow: str | type,
                *args: Any,
                id: str,
                task_queue: str | None,
                cancellation_type: ChildWorkflowCancellationType,
                parent_close_policy: ParentClosePolicy,
                execution_timeout: timedelta | None,
                run_timeout: timedelta | None,
                task_timeout: timedelta | None,
                id_reuse_policy: temporalio.common.WorkflowIDReusePolicy,
                retry_policy: temporalio.common.RetryPolicy | None,
            ) -> ChildWorkflowHandle[Any, Any]:
                raise NotImplementedError()

            async def workflow_wait_child_workflow(
                self,
                handle: ChildWorkflowHandle[Any, Any],
            ) -> Any:
                raise NotImplementedError()

            async def workflow_wait_condition(
                self,
                fn: Callable[[], bool],
                *,
                timeout: float | None = None,
                timeout_summary: str | None = None,
            ) -> None:
                pass

            def workflow_continue_as_new(
                self,
                *args: Any,
                workflow: str | type | None,
                task_queue: str | None,
                run_timeout: timedelta | None,
                task_timeout: timedelta | None,
                retry_policy: temporalio.common.RetryPolicy | None,
            ) -> NoReturn:
                raise NotImplementedError()

            def workflow_get_external_workflow_handle(
                self,
                workflow_id: str,
                *,
                run_id: str | None,
            ) -> workflow.ExternalWorkflowHandle[Any]:
                raise NotImplementedError()

            async def workflow_signal_external_workflow(
                self,
                workflow_id: str,
                signal_name: str,
                args: Sequence[Any],
                *,
                run_id: str | None,
            ) -> None:
                raise NotImplementedError()

            def workflow_random(self) -> random.Random:
                return random.Random(12345)

            def workflow_patch(
                self, patch_id: str, *, deprecated: bool = False
            ) -> bool:
                return True

        mock = MockRuntime()
        token = workflow._Runtime.set_current(mock)
        try:
            assert workflow.time() == 5.0
            assert workflow.time_ns() == 5_000_000_000
        finally:
            workflow._Runtime.reset_current(token)


class TestInfoDataclass:
    """Tests for Info dataclass."""

    def test_info_creation(self) -> None:
        """Test Info can be created with all fields."""
        info = workflow.Info(
            workflow_id="wf-123",
            workflow_type="MyWorkflow",
            run_id="run-456",
            task_queue="my-queue",
        )
        assert info.workflow_id == "wf-123"
        assert info.workflow_type == "MyWorkflow"
        assert info.run_id == "run-456"
        assert info.task_queue == "my-queue"

    def test_info_equality(self) -> None:
        """Test Info equality comparison."""
        info1 = workflow.Info(
            workflow_id="wf-123",
            workflow_type="MyWorkflow",
            run_id="run-456",
            task_queue="my-queue",
        )
        info2 = workflow.Info(
            workflow_id="wf-123",
            workflow_type="MyWorkflow",
            run_id="run-456",
            task_queue="my-queue",
        )
        assert info1 == info2
