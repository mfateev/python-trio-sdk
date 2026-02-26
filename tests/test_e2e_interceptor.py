"""End-to-end tests for the interceptor framework.

Ported from sdk-python/tests/worker/test_interceptor.py.

These tests require a running Temporal server.

Run these tests with:
    pytest -v -m temporal_server tests/test_e2e_interceptor.py
"""

import uuid
from datetime import timedelta
from typing import Any, Callable, List, NoReturn, Optional, Tuple, Type

import pytest
import trio
from temporalio.exceptions import ApplicationError

from temporalio_trio import activity, workflow
from temporalio_trio.client import Client
from temporalio_trio.worker import (
    ActivityInboundInterceptor,
    ActivityOutboundInterceptor,
    ContinueAsNewInput,
    ExecuteActivityInput,
    ExecuteWorkflowInput,
    HandleQueryInput,
    HandleSignalInput,
    HandleUpdateInput,
    Interceptor,
    SignalChildWorkflowInput,
    SignalExternalWorkflowInput,
    StartActivityInput,
    StartChildWorkflowInput,
    StartLocalActivityInput,
    Worker,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
)

interceptor_traces: List[Tuple[str, Any]] = []


class TracingWorkerInterceptor(Interceptor):
    def intercept_activity(
        self, next: ActivityInboundInterceptor
    ) -> ActivityInboundInterceptor:
        return TracingActivityInboundInterceptor(super().intercept_activity(next))

    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> Optional[Type[WorkflowInboundInterceptor]]:
        return TracingWorkflowInboundInterceptor


class TracingActivityInboundInterceptor(ActivityInboundInterceptor):
    def init(self, outbound: ActivityOutboundInterceptor) -> None:
        super().init(TracingActivityOutboundInterceptor(outbound))

    async def execute_activity(self, input: ExecuteActivityInput) -> Any:
        interceptor_traces.append(("activity.execute", input))
        return await super().execute_activity(input)


class TracingActivityOutboundInterceptor(ActivityOutboundInterceptor):
    def info(self) -> activity.Info:
        interceptor_traces.append(("activity.info", super().info()))
        return super().info()

    def heartbeat(self, *details: Any) -> None:
        interceptor_traces.append(("activity.heartbeat", details))
        super().heartbeat(*details)


class TracingWorkflowInboundInterceptor(WorkflowInboundInterceptor):
    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        super().init(TracingWorkflowOutboundInterceptor(outbound))

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        interceptor_traces.append(("workflow.execute", input))
        return await super().execute_workflow(input)

    async def handle_signal(self, input: HandleSignalInput) -> None:
        interceptor_traces.append(("workflow.signal", input))
        return await super().handle_signal(input)

    async def handle_query(self, input: HandleQueryInput) -> Any:
        interceptor_traces.append(("workflow.query", input))
        return await super().handle_query(input)

    def handle_update_validator(self, input: HandleUpdateInput) -> None:
        interceptor_traces.append(("workflow.update.validator", input))
        return super().handle_update_validator(input)

    async def handle_update_handler(self, input: HandleUpdateInput) -> Any:
        interceptor_traces.append(("workflow.update.handler", input))
        return await super().handle_update_handler(input)


class TracingWorkflowOutboundInterceptor(WorkflowOutboundInterceptor):
    def continue_as_new(self, input: ContinueAsNewInput) -> NoReturn:
        interceptor_traces.append(("workflow.continue_as_new", input))
        super().continue_as_new(input)

    def info(self) -> workflow.Info:
        interceptor_traces.append(("workflow.info", super().info()))
        return super().info()

    async def signal_child_workflow(self, input: SignalChildWorkflowInput) -> None:
        interceptor_traces.append(("workflow.signal_child_workflow", input))
        await super().signal_child_workflow(input)

    async def signal_external_workflow(
        self, input: SignalExternalWorkflowInput
    ) -> None:
        interceptor_traces.append(("workflow.signal_external_workflow", input))
        await super().signal_external_workflow(input)

    async def start_activity(self, input: StartActivityInput) -> Any:
        interceptor_traces.append(("workflow.start_activity", input))
        return await super().start_activity(input)

    async def start_child_workflow(
        self, input: StartChildWorkflowInput
    ) -> workflow.ChildWorkflowHandle:
        interceptor_traces.append(("workflow.start_child_workflow", input))
        return await super().start_child_workflow(input)

    async def start_local_activity(self, input: StartLocalActivityInput) -> Any:
        interceptor_traces.append(("workflow.start_local_activity", input))
        return await super().start_local_activity(input)


@activity.defn
async def intercepted_activity(param: str) -> str:
    if not activity.info().is_local:
        activity.heartbeat("details")
    return f"param: {param}"


@workflow.defn
class InterceptedWorkflow:
    def __init__(self) -> None:
        self._finish = False

    @workflow.run
    async def run(self, style: str) -> str:
        if style == "activity-test":
            # Test activity interceptor
            r1 = await workflow.execute_activity(
                intercepted_activity,
                "val1",
                schedule_to_close_timeout=timedelta(seconds=5),
            )
            # Call info() to trigger outbound interceptor
            my_id = workflow.info().workflow_id
            await workflow.wait_condition(lambda: self._finish)
            return f"{r1},{my_id}"
        return "done"

    @workflow.query
    def query(self, param: str) -> str:
        return f"query: {param}"

    @workflow.signal
    def signal(self, param: str) -> None:
        self._finish = True


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_worker_interceptor():
    """Test the full interceptor chain with tracing interceptor."""
    # Clear any previous traces
    interceptor_traces.clear()

    client = await Client.connect("localhost:7233", namespace="default")
    try:
        task_queue = f"task_queue_{uuid.uuid4()}"
        worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[InterceptedWorkflow],
            activities=[intercepted_activity],
            interceptors=[TracingWorkerInterceptor()],
        )
        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)
            await trio.sleep(0)
            try:
                # Run workflow
                handle = await client.start_workflow(
                    InterceptedWorkflow,
                    "activity-test",
                    id=f"workflow_{uuid.uuid4()}",
                    task_queue=task_queue,
                )
                # Test query interceptor
                assert "query: query-val" == await handle.query("query", "query-val")
                # Test signal interceptor (this completes the workflow)
                await handle.signal("signal", "signal-val")
                result = await handle.result()
                assert result is not None

                # Check traces
                def pop_trace(
                    name: str, filter: Optional[Callable[[Any], bool]] = None
                ) -> Any:
                    index = next(
                        (
                            i
                            for i, v in enumerate(interceptor_traces)
                            if v[0] == name and (not filter or filter(v[1]))
                        ),
                        None,
                    )
                    if index is None:
                        return None
                    return interceptor_traces.pop(index)[1]

                # Activity interceptor traces
                assert pop_trace("activity.execute", lambda v: v.args[0] == "val1")
                # Check activity info is called at least once
                activity_infos = 0
                while pop_trace("activity.info"):
                    activity_infos += 1
                assert activity_infos >= 1
                assert pop_trace("activity.heartbeat", lambda v: v[0] == "details")

                # Workflow execute interceptor trace
                assert pop_trace(
                    "workflow.execute", lambda v: v.args[0] == "activity-test"
                )

                # Query interceptor trace
                assert pop_trace("workflow.query", lambda v: v.args[0] == "query-val")

                # Outbound interceptor traces
                assert pop_trace("workflow.info")
                assert pop_trace(
                    "workflow.start_activity", lambda v: v.args[0] == "val1"
                )

            finally:
                await worker.shutdown()
                await trio.sleep(0.3)
                nursery.cancel_scope.cancel()
    finally:
        await client.close()


class WorkflowInstanceAccessInterceptor(Interceptor):
    def workflow_interceptor_class(
        self, input: WorkflowInterceptorClassInput
    ) -> Optional[Type[WorkflowInboundInterceptor]]:
        return WorkflowInstanceAccessInboundInterceptor


class WorkflowInstanceAccessInboundInterceptor(WorkflowInboundInterceptor):
    async def execute_workflow(self, input: ExecuteWorkflowInput) -> int:
        from_workflow_instance_api = workflow.instance()
        assert from_workflow_instance_api is not None
        id_from_workflow_instance_api = id(from_workflow_instance_api)
        id_from_workflow_run_method = await super().execute_workflow(input)
        return id_from_workflow_run_method - id_from_workflow_instance_api


@workflow.defn
class WorkflowInstanceAccessWorkflow:
    @workflow.run
    async def run(self) -> int:
        return id(self)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_workflow_instance_access_from_interceptor():
    """Test that workflow.instance() returns correct object inside interceptor."""
    client = await Client.connect("localhost:7233", namespace="default")
    try:
        task_queue = f"task_queue_{uuid.uuid4()}"
        worker = Worker(
            client,
            task_queue=task_queue,
            workflows=[WorkflowInstanceAccessWorkflow],
            interceptors=[WorkflowInstanceAccessInterceptor()],
        )
        async with trio.open_nursery() as nursery:
            nursery.start_soon(worker.run)
            await trio.sleep(0)
            try:
                handle = await client.start_workflow(
                    WorkflowInstanceAccessWorkflow,
                    id=f"workflow_{uuid.uuid4()}",
                    task_queue=task_queue,
                )
                difference = await handle.result()
                assert difference == 0
            finally:
                await worker.shutdown()
                await trio.sleep(0.3)
                nursery.cancel_scope.cancel()
    finally:
        await client.close()
