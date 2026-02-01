"""Tests for workflow interceptor framework - Phase 2."""

from datetime import timedelta

import pytest

from temporalio_trio.worker import (
    ContinueAsNewInput,
    ExecuteWorkflowInput,
    HandleQueryInput,
    HandleSignalInput,
    HandleUpdateInput,
    Interceptor,
    StartActivityInput,
    StartChildWorkflowInput,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)


class TestInputDataclasses:
    """Tests for interceptor input dataclasses."""

    def test_execute_workflow_input(self) -> None:
        """Test ExecuteWorkflowInput fields."""

        async def run_fn(self) -> str:
            return "done"

        class MyWorkflow:
            pass

        inp = ExecuteWorkflowInput(
            type=MyWorkflow,
            run_fn=run_fn,
            args=("arg1", "arg2"),
            headers={"key": "value"},
        )
        assert inp.type is MyWorkflow
        assert inp.run_fn is run_fn
        assert inp.args == ("arg1", "arg2")
        assert inp.headers == {"key": "value"}

    def test_handle_signal_input(self) -> None:
        """Test HandleSignalInput fields."""
        inp = HandleSignalInput(
            signal="my_signal",
            args=("arg1",),
            headers={"key": "value"},
        )
        assert inp.signal == "my_signal"
        assert inp.args == ("arg1",)
        assert inp.headers == {"key": "value"}

    def test_handle_query_input(self) -> None:
        """Test HandleQueryInput fields."""
        inp = HandleQueryInput(
            id="query-123",
            query="my_query",
            args=(42,),
            headers={"key": "value"},
        )
        assert inp.id == "query-123"
        assert inp.query == "my_query"
        assert inp.args == (42,)
        assert inp.headers == {"key": "value"}

    def test_handle_update_input(self) -> None:
        """Test HandleUpdateInput fields."""
        inp = HandleUpdateInput(
            id="update-123",
            update="my_update",
            args=("new_value",),
            headers={"key": "value"},
        )
        assert inp.id == "update-123"
        assert inp.update == "my_update"
        assert inp.args == ("new_value",)
        assert inp.headers == {"key": "value"}

    def test_continue_as_new_input(self) -> None:
        """Test ContinueAsNewInput fields."""
        inp = ContinueAsNewInput(
            workflow="OtherWorkflow",
            args=("arg1", "arg2"),
            task_queue="new-queue",
            run_timeout=timedelta(hours=1),
            task_timeout=timedelta(minutes=5),
            memo={"key": "value"},
            headers={"header": "data"},
        )
        assert inp.workflow == "OtherWorkflow"
        assert inp.args == ("arg1", "arg2")
        assert inp.task_queue == "new-queue"
        assert inp.run_timeout == timedelta(hours=1)
        assert inp.task_timeout == timedelta(minutes=5)
        assert inp.memo == {"key": "value"}
        assert inp.headers == {"header": "data"}

    def test_continue_as_new_input_defaults(self) -> None:
        """Test ContinueAsNewInput with None values."""
        inp = ContinueAsNewInput(
            workflow=None,
            args=[],
            task_queue=None,
            run_timeout=None,
            task_timeout=None,
            memo=None,
            headers={},
        )
        assert inp.workflow is None
        assert inp.args == []
        assert inp.task_queue is None
        assert inp.run_timeout is None
        assert inp.task_timeout is None
        assert inp.memo is None
        assert inp.headers == {}

    def test_start_activity_input(self) -> None:
        """Test StartActivityInput fields."""
        inp = StartActivityInput(
            activity="my_activity",
            args=("arg1",),
            activity_id="act-123",
            task_queue="activity-queue",
            schedule_to_close_timeout=timedelta(minutes=10),
            schedule_to_start_timeout=timedelta(seconds=30),
            start_to_close_timeout=timedelta(minutes=5),
            heartbeat_timeout=timedelta(seconds=10),
            headers={"key": "value"},
        )
        assert inp.activity == "my_activity"
        assert inp.args == ("arg1",)
        assert inp.activity_id == "act-123"
        assert inp.task_queue == "activity-queue"
        assert inp.schedule_to_close_timeout == timedelta(minutes=10)
        assert inp.schedule_to_start_timeout == timedelta(seconds=30)
        assert inp.start_to_close_timeout == timedelta(minutes=5)
        assert inp.heartbeat_timeout == timedelta(seconds=10)
        assert inp.headers == {"key": "value"}

    def test_start_child_workflow_input(self) -> None:
        """Test StartChildWorkflowInput fields."""
        inp = StartChildWorkflowInput(
            workflow="ChildWorkflow",
            args=("arg1", "arg2"),
            id="child-wf-123",
            task_queue="child-queue",
            execution_timeout=timedelta(hours=2),
            run_timeout=timedelta(hours=1),
            task_timeout=timedelta(minutes=5),
            memo={"key": "value"},
            headers={"header": "data"},
        )
        assert inp.workflow == "ChildWorkflow"
        assert inp.args == ("arg1", "arg2")
        assert inp.id == "child-wf-123"
        assert inp.task_queue == "child-queue"
        assert inp.execution_timeout == timedelta(hours=2)
        assert inp.run_timeout == timedelta(hours=1)
        assert inp.task_timeout == timedelta(minutes=5)
        assert inp.memo == {"key": "value"}
        assert inp.headers == {"header": "data"}


class TestWorkflowInterceptorClassInput:
    """Tests for WorkflowInterceptorClassInput."""

    def test_unsafe_extern_functions_mutable(self) -> None:
        """Test unsafe_extern_functions is mutable."""

        def my_func() -> str:
            return "hello"

        inp = WorkflowInterceptorClassInput(
            unsafe_extern_functions={"my_func": my_func},
        )
        assert inp.unsafe_extern_functions["my_func"] is my_func

        # Should be mutable
        def another_func() -> int:
            return 42

        inp.unsafe_extern_functions["another"] = another_func
        assert "another" in inp.unsafe_extern_functions


class TestInterceptor:
    """Tests for base Interceptor class."""

    def test_workflow_interceptor_class_returns_none(self) -> None:
        """Test default workflow_interceptor_class returns None."""
        interceptor = Interceptor()
        inp = WorkflowInterceptorClassInput(unsafe_extern_functions={})
        result = interceptor.workflow_interceptor_class(inp)
        assert result is None

    def test_custom_interceptor_returns_class(self) -> None:
        """Test custom interceptor can return a class."""

        class MyInboundInterceptor(WorkflowInboundInterceptor):
            pass

        class MyInterceptor(Interceptor):
            def workflow_interceptor_class(
                self,
                input: WorkflowInterceptorClassInput,
            ) -> type[WorkflowInboundInterceptor] | None:
                return MyInboundInterceptor

        interceptor = MyInterceptor()
        inp = WorkflowInterceptorClassInput(unsafe_extern_functions={})
        result = interceptor.workflow_interceptor_class(inp)
        assert result is MyInboundInterceptor


class TestWorkflowInboundInterceptor:
    """Tests for WorkflowInboundInterceptor."""

    def test_init_stores_next(self) -> None:
        """Test __init__ stores next interceptor."""

        class RootInterceptor(WorkflowInboundInterceptor):
            def __init__(self) -> None:
                pass  # Root has no next

        root = RootInterceptor()
        interceptor = WorkflowInboundInterceptor(root)
        assert interceptor.next is root

    def test_init_passes_to_next(self) -> None:
        """Test init() passes outbound to next."""
        init_calls: list[WorkflowOutboundInterceptor] = []

        class RootInterceptor(WorkflowInboundInterceptor):
            def __init__(self) -> None:
                pass

            def init(self, outbound: WorkflowOutboundInterceptor) -> None:
                init_calls.append(outbound)

        class RootOutbound(WorkflowOutboundInterceptor):
            def __init__(self) -> None:
                pass

        root = RootInterceptor()
        interceptor = WorkflowInboundInterceptor(root)
        outbound = RootOutbound()

        interceptor.init(outbound)
        assert len(init_calls) == 1
        assert init_calls[0] is outbound

    @pytest.mark.trio
    async def test_execute_workflow_delegates(self) -> None:
        """Test execute_workflow delegates to next."""
        call_log: list[str] = []

        class RootInterceptor(WorkflowInboundInterceptor):
            def __init__(self) -> None:
                pass

            async def execute_workflow(self, input: ExecuteWorkflowInput) -> str:
                call_log.append("root")
                return "result"

        async def run_fn(self) -> str:
            return "done"

        root = RootInterceptor()
        interceptor = WorkflowInboundInterceptor(root)

        inp = ExecuteWorkflowInput(
            type=object,
            run_fn=run_fn,
            args=(),
            headers={},
        )
        result = await interceptor.execute_workflow(inp)

        assert result == "result"
        assert call_log == ["root"]

    @pytest.mark.trio
    async def test_handle_signal_delegates(self) -> None:
        """Test handle_signal delegates to next."""
        call_log: list[str] = []

        class RootInterceptor(WorkflowInboundInterceptor):
            def __init__(self) -> None:
                pass

            async def handle_signal(self, input: HandleSignalInput) -> None:
                call_log.append(f"signal:{input.signal}")

        root = RootInterceptor()
        interceptor = WorkflowInboundInterceptor(root)

        inp = HandleSignalInput(signal="test_signal", args=(), headers={})
        await interceptor.handle_signal(inp)

        assert call_log == ["signal:test_signal"]

    @pytest.mark.trio
    async def test_handle_query_delegates(self) -> None:
        """Test handle_query delegates to next."""
        call_log: list[str] = []

        class RootInterceptor(WorkflowInboundInterceptor):
            def __init__(self) -> None:
                pass

            async def handle_query(self, input: HandleQueryInput) -> str:
                call_log.append(f"query:{input.query}")
                return "query_result"

        root = RootInterceptor()
        interceptor = WorkflowInboundInterceptor(root)

        inp = HandleQueryInput(id="q-1", query="test_query", args=(), headers={})
        result = await interceptor.handle_query(inp)

        assert result == "query_result"
        assert call_log == ["query:test_query"]

    def test_handle_update_validator_delegates(self) -> None:
        """Test handle_update_validator delegates to next."""
        call_log: list[str] = []

        class RootInterceptor(WorkflowInboundInterceptor):
            def __init__(self) -> None:
                pass

            def handle_update_validator(self, input: HandleUpdateInput) -> None:
                call_log.append(f"validate:{input.update}")

        root = RootInterceptor()
        interceptor = WorkflowInboundInterceptor(root)

        inp = HandleUpdateInput(id="u-1", update="test_update", args=(), headers={})
        interceptor.handle_update_validator(inp)

        assert call_log == ["validate:test_update"]

    @pytest.mark.trio
    async def test_handle_update_handler_delegates(self) -> None:
        """Test handle_update_handler delegates to next."""
        call_log: list[str] = []

        class RootInterceptor(WorkflowInboundInterceptor):
            def __init__(self) -> None:
                pass

            async def handle_update_handler(self, input: HandleUpdateInput) -> str:
                call_log.append(f"handle:{input.update}")
                return "update_result"

        root = RootInterceptor()
        interceptor = WorkflowInboundInterceptor(root)

        inp = HandleUpdateInput(id="u-1", update="test_update", args=(), headers={})
        result = await interceptor.handle_update_handler(inp)

        assert result == "update_result"
        assert call_log == ["handle:test_update"]


class TestWorkflowOutboundInterceptor:
    """Tests for WorkflowOutboundInterceptor."""

    def test_init_stores_next(self) -> None:
        """Test __init__ stores next interceptor."""

        class RootOutbound(WorkflowOutboundInterceptor):
            def __init__(self) -> None:
                pass

        root = RootOutbound()
        interceptor = WorkflowOutboundInterceptor(root)
        assert interceptor.next is root

    def test_start_activity_delegates(self) -> None:
        """Test start_activity delegates to next."""
        call_log: list[str] = []

        class RootOutbound(WorkflowOutboundInterceptor):
            def __init__(self) -> None:
                pass

            def start_activity(self, input: StartActivityInput) -> str:
                call_log.append(f"activity:{input.activity}")
                return "activity_handle"

        root = RootOutbound()
        interceptor = WorkflowOutboundInterceptor(root)

        inp = StartActivityInput(
            activity="test_activity",
            args=(),
            activity_id=None,
            task_queue=None,
            schedule_to_close_timeout=None,
            schedule_to_start_timeout=None,
            start_to_close_timeout=None,
            heartbeat_timeout=None,
            headers={},
        )
        result = interceptor.start_activity(inp)

        assert result == "activity_handle"
        assert call_log == ["activity:test_activity"]

    @pytest.mark.trio
    async def test_start_child_workflow_delegates(self) -> None:
        """Test start_child_workflow delegates to next."""
        call_log: list[str] = []

        class RootOutbound(WorkflowOutboundInterceptor):
            def __init__(self) -> None:
                pass

            async def start_child_workflow(self, input: StartChildWorkflowInput) -> str:
                call_log.append(f"child:{input.workflow}")
                return "child_handle"

        root = RootOutbound()
        interceptor = WorkflowOutboundInterceptor(root)

        inp = StartChildWorkflowInput(
            workflow="ChildWorkflow",
            args=(),
            id="child-1",
            task_queue=None,
            execution_timeout=None,
            run_timeout=None,
            task_timeout=None,
            memo=None,
            headers={},
        )
        result = await interceptor.start_child_workflow(inp)

        assert result == "child_handle"
        assert call_log == ["child:ChildWorkflow"]


class TestInterceptorChain:
    """Tests for interceptor chain behavior."""

    @pytest.mark.trio
    async def test_chain_executes_in_order(self) -> None:
        """Test interceptor chain executes in correct order."""
        call_log: list[str] = []

        class RootInterceptor(WorkflowInboundInterceptor):
            def __init__(self) -> None:
                pass

            async def execute_workflow(self, input: ExecuteWorkflowInput) -> str:
                call_log.append("root")
                return "result"

        class InterceptorA(WorkflowInboundInterceptor):
            async def execute_workflow(self, input: ExecuteWorkflowInput) -> str:
                call_log.append("A-before")
                result = await self.next.execute_workflow(input)
                call_log.append("A-after")
                return result

        class InterceptorB(WorkflowInboundInterceptor):
            async def execute_workflow(self, input: ExecuteWorkflowInput) -> str:
                call_log.append("B-before")
                result = await self.next.execute_workflow(input)
                call_log.append("B-after")
                return result

        # Build chain: A -> B -> Root
        root = RootInterceptor()
        b = InterceptorB(root)
        a = InterceptorA(b)

        async def run_fn(self) -> str:
            return "done"

        inp = ExecuteWorkflowInput(type=object, run_fn=run_fn, args=(), headers={})
        result = await a.execute_workflow(inp)

        assert result == "result"
        assert call_log == ["A-before", "B-before", "root", "B-after", "A-after"]

    @pytest.mark.trio
    async def test_chain_can_short_circuit(self) -> None:
        """Test interceptor can short-circuit the chain."""
        call_log: list[str] = []

        class RootInterceptor(WorkflowInboundInterceptor):
            def __init__(self) -> None:
                pass

            async def execute_workflow(self, input: ExecuteWorkflowInput) -> str:
                call_log.append("root")
                return "root_result"

        class ShortCircuitInterceptor(WorkflowInboundInterceptor):
            async def execute_workflow(self, input: ExecuteWorkflowInput) -> str:
                call_log.append("short-circuit")
                return "intercepted"

        root = RootInterceptor()
        interceptor = ShortCircuitInterceptor(root)

        async def run_fn(self) -> str:
            return "done"

        inp = ExecuteWorkflowInput(type=object, run_fn=run_fn, args=(), headers={})
        result = await interceptor.execute_workflow(inp)

        assert result == "intercepted"
        assert call_log == ["short-circuit"]  # Root never called

    @pytest.mark.trio
    async def test_chain_can_modify_input(self) -> None:
        """Test interceptor can modify input before passing to next."""
        received_args: list[tuple] = []

        class RootInterceptor(WorkflowInboundInterceptor):
            def __init__(self) -> None:
                pass

            async def handle_signal(self, input: HandleSignalInput) -> None:
                received_args.append(tuple(input.args))

        class ModifyingInterceptor(WorkflowInboundInterceptor):
            async def handle_signal(self, input: HandleSignalInput) -> None:
                # Create modified input
                modified = HandleSignalInput(
                    signal=input.signal,
                    args=(*input.args, "added_by_interceptor"),
                    headers=input.headers,
                )
                await self.next.handle_signal(modified)

        root = RootInterceptor()
        interceptor = ModifyingInterceptor(root)

        inp = HandleSignalInput(signal="test", args=("original",), headers={})
        await interceptor.handle_signal(inp)

        assert received_args == [("original", "added_by_interceptor")]

    def test_build_chain_from_classes(self) -> None:
        """Test building interceptor chain from list of classes."""

        class RootInterceptor(WorkflowInboundInterceptor):
            def __init__(self) -> None:
                pass

        class InterceptorA(WorkflowInboundInterceptor):
            pass

        class InterceptorB(WorkflowInboundInterceptor):
            pass

        # Build chain in reversed order: [A, B] → A -> B -> Root
        interceptor_classes = [InterceptorA, InterceptorB]
        root = RootInterceptor()
        current: WorkflowInboundInterceptor = root

        for cls in reversed(interceptor_classes):
            current = cls(current)

        # Verify chain structure
        assert isinstance(current, InterceptorA)
        assert isinstance(current.next, InterceptorB)
        assert isinstance(current.next.next, RootInterceptor)


class TestLoggingInterceptorExample:
    """Example of a logging interceptor to demonstrate the pattern."""

    @pytest.mark.trio
    async def test_logging_interceptor(self) -> None:
        """Test example logging interceptor."""
        log_messages: list[str] = []

        class RootInterceptor(WorkflowInboundInterceptor):
            def __init__(self) -> None:
                pass

            async def execute_workflow(self, input: ExecuteWorkflowInput) -> str:
                return "workflow_done"

            async def handle_signal(self, input: HandleSignalInput) -> None:
                pass

            async def handle_query(self, input: HandleQueryInput) -> str:
                return "query_result"

        class LoggingInterceptor(WorkflowInboundInterceptor):
            async def execute_workflow(self, input: ExecuteWorkflowInput) -> str:
                log_messages.append(f"Executing workflow {input.type.__name__}")
                try:
                    result = await self.next.execute_workflow(input)
                    log_messages.append(f"Workflow completed with: {result}")
                    return result
                except Exception as e:
                    log_messages.append(f"Workflow failed: {e}")
                    raise

            async def handle_signal(self, input: HandleSignalInput) -> None:
                log_messages.append(f"Handling signal: {input.signal}")
                await self.next.handle_signal(input)

            async def handle_query(self, input: HandleQueryInput) -> str:
                log_messages.append(f"Handling query: {input.query}")
                return await self.next.handle_query(input)

        class MyWorkflow:
            pass

        async def run_fn(self) -> str:
            return "done"

        root = RootInterceptor()
        interceptor = LoggingInterceptor(root)

        # Execute workflow
        wf_input = ExecuteWorkflowInput(
            type=MyWorkflow, run_fn=run_fn, args=(), headers={}
        )
        await interceptor.execute_workflow(wf_input)

        # Handle signal
        signal_input = HandleSignalInput(signal="my_signal", args=(), headers={})
        await interceptor.handle_signal(signal_input)

        # Handle query
        query_input = HandleQueryInput(id="q-1", query="my_query", args=(), headers={})
        await interceptor.handle_query(query_input)

        assert log_messages == [
            "Executing workflow MyWorkflow",
            "Workflow completed with: workflow_done",
            "Handling signal: my_signal",
            "Handling query: my_query",
        ]
