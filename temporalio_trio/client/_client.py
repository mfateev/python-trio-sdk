"""Trio-based Temporal client implementation.

This module provides a pure Trio client for interacting with Temporal workflows.
It uses the TrioBridgeWrapper to communicate with SDK Core via the Rust bridge.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Sequence, Type, TypeVar, Union

import temporalio.common
import temporalio.converter
import trio
from temporalio.api.common.v1 import Payloads, WorkflowExecution
from temporalio.api.enums.v1 import WorkflowIdConflictPolicy, WorkflowIdReusePolicy
from temporalio.api.sdk.v1 import UserMetadata
from temporalio.api.workflowservice.v1 import (
    CountWorkflowExecutionsRequest,
    CountWorkflowExecutionsResponse,
    ListWorkflowExecutionsRequest,
    ListWorkflowExecutionsResponse,
    SignalWithStartWorkflowExecutionRequest,
    SignalWithStartWorkflowExecutionResponse,
    StartWorkflowExecutionRequest,
    StartWorkflowExecutionResponse,
)
from temporalio.common import RetryPolicy, SearchAttributes
from temporalio.converter import DataConverter

from .._async_bridge import TrioBridgeWrapper
from ._workflow_handle import WorkflowExecutionStatus, WorkflowHandle


@dataclass
class ClientConfig:
    """Configuration for connecting to Temporal server."""

    target_url: str
    namespace: str = "default"
    identity: Optional[str] = None
    data_converter: Optional[DataConverter] = None
    tls: bool = False
    rpc_metadata: Mapping[str, str] = field(default_factory=dict)
    default_workflow_query_reject_condition: Optional[
        temporalio.common.QueryRejectCondition
    ] = None
    retry_config: Any = None
    lazy: bool = False


@dataclass
class WorkflowExecutionInfo:
    """Summary information about a workflow execution.

    Returned by :py:meth:`Client.list_workflows`.
    """

    workflow_id: str
    """Workflow ID."""

    run_id: str
    """Run ID for this workflow run."""

    status: Optional[WorkflowExecutionStatus]
    """Current status of the workflow execution."""

    workflow_type: str
    """Type name for the workflow."""

    start_time: Optional[datetime]
    """When the workflow was created."""

    close_time: Optional[datetime]
    """When the workflow was closed, if closed."""


class Client:
    """Temporal client for Trio async runtime.

    This client provides a Trio-native interface to Temporal, matching the
    official SDK's API but using pure Trio for all async operations.

    Example:
        async def main():
            client = await Client.connect("localhost:7233")
            try:
                # Start a workflow
                handle = await client.start_workflow(
                    "MyWorkflow",
                    "arg1",
                    id="workflow-123",
                    task_queue="my-queue",
                )
                result = await handle.result()
                print(f"Result: {result}")
            finally:
                await client.close()

        trio.run(main)
    """

    def __init__(
        self,
        bridge: TrioBridgeWrapper,
        config: ClientConfig,
    ) -> None:
        """Initialize client (internal - use connect() instead)."""
        self._bridge = bridge
        self._config = config
        self._data_converter = config.data_converter or DataConverter.default

    @classmethod
    async def connect(
        cls,
        target_url: str,
        *,
        namespace: str = "default",
        identity: Optional[str] = None,
        data_converter: Optional[DataConverter] = None,
        tls: bool = False,
        rpc_metadata: Mapping[str, str] = {},
        default_workflow_query_reject_condition: Optional[
            temporalio.common.QueryRejectCondition
        ] = None,
        retry_config: Any = None,
        lazy: bool = False,
    ) -> "Client":
        """Connect to Temporal server.

        Args:
            target_url: Temporal server URL (e.g., "localhost:7233")
            namespace: Temporal namespace (default: "default")
            identity: Client identity (default: auto-generated)
            data_converter: Data converter for payload serialization
            tls: If True, use ``https://`` scheme instead of ``http://``.
                Full TLS configuration is not yet implemented.
            rpc_metadata: Headers to include on every RPC call. Not yet
                propagated to the bridge (stored for future use).
            default_workflow_query_reject_condition: Default rejection
                condition for workflow queries when not set per-query.
                See :py:meth:`WorkflowHandle.query` for details.
            retry_config: Retry configuration for service calls. Accepted
                and stored but not yet propagated to the bridge.
            lazy: If True, delay actual connection until the first call.
                For now the parameter is accepted but eager connection is
                always performed.

        Returns:
            Connected Client instance

        Example:
            client = await Client.connect("localhost:7233", namespace="default")
        """
        # Ensure URL has scheme - target_url might be just "localhost:7233"
        if not target_url.startswith(("http://", "https://")):
            scheme = "https" if tls else "http"
            target_url = f"{scheme}://{target_url}"

        config = ClientConfig(
            target_url=target_url,
            namespace=namespace,
            identity=identity or f"trio-client-{uuid.uuid4()}",
            data_converter=data_converter,
            tls=tls,
            rpc_metadata=rpc_metadata,
            default_workflow_query_reject_condition=default_workflow_query_reject_condition,
            retry_config=retry_config,
            lazy=lazy,
        )

        bridge = TrioBridgeWrapper()
        await bridge.start()
        await bridge.initialize_client(
            target_url=config.target_url,
            namespace=config.namespace,
            identity=config.identity,
        )

        return cls(bridge=bridge, config=config)

    async def start_workflow(
        self,
        workflow: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: Optional[Type] = None,
        execution_timeout: Union[timedelta, float, None] = None,
        run_timeout: Union[timedelta, float, None] = None,
        task_timeout: Union[timedelta, float, None] = None,
        id_reuse_policy: WorkflowIdReusePolicy.ValueType = WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[RetryPolicy] = None,
        cron_schedule: Optional[str] = None,
        memo: Optional[dict[str, Any]] = None,
        search_attributes: Optional[Union[SearchAttributes, dict[str, Any]]] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Union[timedelta, float, None] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> WorkflowHandle:
        """Start a workflow execution.

        Args:
            workflow: Workflow type name or class
            arg: Single argument to the workflow
            args: Multiple arguments to the workflow. Cannot be set if arg is.
            id: Unique workflow ID
            task_queue: Task queue name
            result_type: For string workflows, this can set the specific result
                type hint to deserialize into.
            execution_timeout: Total workflow execution timeout
            run_timeout: Single run timeout
            task_timeout: Workflow task timeout
            id_reuse_policy: Workflow ID reuse policy
            id_conflict_policy: How already-running workflows of the same ID
                are treated. Default is unspecified which effectively means fail
                the start attempt. This cannot be set if ``id_reuse_policy`` is
                set to terminate if running.
            retry_policy: Retry policy for workflow
            cron_schedule: Cron schedule (if applicable)
            memo: Memo for workflow
            search_attributes: Search attributes
            static_summary: A single-line fixed summary for this workflow
                execution that may appear in the UI/CLI.
            static_details: General fixed details for this workflow execution
                that may appear in UI/CLI. This can be in Temporal markdown
                format and can span multiple lines.
            start_delay: Delay before starting workflow
            start_signal: If present, this signal is sent as
                signal-with-start instead of traditional workflow start.
            start_signal_args: Arguments for start_signal if start_signal
                present.
            request_eager_start: Request eager workflow start
            priority: Priority of the workflow execution.

        Returns:
            WorkflowHandle for the started workflow

        Example:
            handle = await client.start_workflow(
                "MyWorkflow",
                "arg1",
                id="workflow-123",
                task_queue="my-queue",
            )
            print(f"Started: {handle.workflow_id}")
        """
        # Convert workflow name
        workflow_type = workflow if isinstance(workflow, str) else workflow.__name__

        # Resolve arg/args mutual exclusion
        resolved_args = temporalio.common._arg_or_args(arg, args)

        # Encode arguments
        input_payloads = None
        if resolved_args:
            payloads_list = await self._data_converter.encode(resolved_args)
            input_payloads = Payloads(payloads=payloads_list)

        # Build request - use SignalWithStart if start_signal is set
        if start_signal is not None:
            request: Union[
                StartWorkflowExecutionRequest,
                SignalWithStartWorkflowExecutionRequest,
            ] = SignalWithStartWorkflowExecutionRequest(
                namespace=self._config.namespace,
                workflow_id=id,
                workflow_type={"name": workflow_type},
                task_queue={"name": task_queue},
                input=input_payloads,
                workflow_execution_timeout=self._duration_to_proto(execution_timeout),
                workflow_run_timeout=self._duration_to_proto(run_timeout),
                workflow_task_timeout=self._duration_to_proto(task_timeout),
                identity=self._config.identity,
                workflow_id_reuse_policy=id_reuse_policy,
                workflow_id_conflict_policy=int(id_conflict_policy),
                signal_name=start_signal,
            )
            # Encode signal args
            if start_signal_args:
                signal_payloads = await self._data_converter.encode(
                    list(start_signal_args)
                )
                request.signal_input.payloads.extend(signal_payloads)
        else:
            request = StartWorkflowExecutionRequest(
                namespace=self._config.namespace,
                workflow_id=id,
                workflow_type={"name": workflow_type},
                task_queue={"name": task_queue},
                input=input_payloads,
                workflow_execution_timeout=self._duration_to_proto(execution_timeout),
                workflow_run_timeout=self._duration_to_proto(run_timeout),
                workflow_task_timeout=self._duration_to_proto(task_timeout),
                identity=self._config.identity,
                workflow_id_reuse_policy=id_reuse_policy,
                workflow_id_conflict_policy=int(id_conflict_policy),
                request_eager_execution=request_eager_start,
            )

        # Encode retry policy
        if retry_policy is not None:
            retry_policy.apply_to_proto(request.retry_policy)

        # Set cron schedule
        if cron_schedule:
            request.cron_schedule = cron_schedule

        # Encode memo
        if memo is not None:
            for k, v in memo.items():
                request.memo.fields[k].CopyFrom(
                    (await self._data_converter.encode([v]))[0]
                )

        # Encode search attributes
        if search_attributes is not None:
            temporalio.converter.encode_search_attributes(
                search_attributes, request.search_attributes
            )

        # Encode user metadata (static_summary and static_details)
        if static_summary is not None or static_details is not None:
            enc_summary = None
            enc_details = None
            if static_summary is not None:
                enc_summary = (await self._data_converter.encode([static_summary]))[0]
            if static_details is not None:
                enc_details = (await self._data_converter.encode([static_details]))[0]
            metadata = UserMetadata(summary=enc_summary, details=enc_details)
            request.user_metadata.CopyFrom(metadata)

        # Encode start delay
        if start_delay is not None:
            if isinstance(start_delay, timedelta):
                request.workflow_start_delay.FromTimedelta(start_delay)
            else:
                request.workflow_start_delay.FromTimedelta(
                    timedelta(seconds=start_delay)
                )

        # Encode priority
        if priority is not None:
            request.priority.CopyFrom(priority._to_proto())

        # Serialize request
        request_bytes = request.SerializeToString()

        # Call bridge
        if start_signal is not None:
            response_bytes = await self._bridge.signal_with_start_workflow_execution(
                request_bytes
            )
            # Parse response
            swsr = SignalWithStartWorkflowExecutionResponse()
            swsr.ParseFromString(response_bytes)
            run_id = swsr.run_id
        else:
            response_bytes = await self._bridge.start_workflow_execution(request_bytes)
            # Parse response
            swr = StartWorkflowExecutionResponse()
            swr.ParseFromString(response_bytes)
            run_id = swr.run_id

        # Return handle - run_id intentionally left as None so the handle
        # tracks the latest run (matching sdk-python behavior)
        return WorkflowHandle(
            client=self,
            workflow_id=id,
            run_id=None,
            result_run_id=run_id,
        )

    async def execute_workflow(
        self,
        workflow: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        result_type: Optional[Type] = None,
        execution_timeout: Union[timedelta, float, None] = None,
        run_timeout: Union[timedelta, float, None] = None,
        task_timeout: Union[timedelta, float, None] = None,
        id_reuse_policy: WorkflowIdReusePolicy.ValueType = WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[RetryPolicy] = None,
        cron_schedule: Optional[str] = None,
        memo: Optional[dict[str, Any]] = None,
        search_attributes: Optional[Union[SearchAttributes, dict[str, Any]]] = None,
        static_summary: Optional[str] = None,
        static_details: Optional[str] = None,
        start_delay: Union[timedelta, float, None] = None,
        start_signal: Optional[str] = None,
        start_signal_args: Sequence[Any] = [],
        request_eager_start: bool = False,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> Any:
        """Start a workflow and wait for its result.

        This is a convenience method that combines start_workflow() and handle.result().

        Args:
            Same as start_workflow()

        Returns:
            The workflow result

        Example:
            result = await client.execute_workflow(
                "GreetingWorkflow",
                "World",
                id="greeting-123",
                task_queue="my-queue",
            )
            print(f"Result: {result}")
        """
        handle = await self.start_workflow(
            workflow,
            arg=arg,
            args=args,
            id=id,
            task_queue=task_queue,
            result_type=result_type,
            execution_timeout=execution_timeout,
            run_timeout=run_timeout,
            task_timeout=task_timeout,
            id_reuse_policy=id_reuse_policy,
            id_conflict_policy=id_conflict_policy,
            retry_policy=retry_policy,
            cron_schedule=cron_schedule,
            memo=memo,
            search_attributes=search_attributes,
            static_summary=static_summary,
            static_details=static_details,
            start_delay=start_delay,
            start_signal=start_signal,
            start_signal_args=start_signal_args,
            request_eager_start=request_eager_start,
            priority=priority,
        )
        return await handle.result()

    def get_workflow_handle(
        self,
        workflow_id: str,
        *,
        run_id: Optional[str] = None,
    ) -> WorkflowHandle:
        """Get a handle to an existing workflow.

        Args:
            workflow_id: Workflow ID
            run_id: Optional run ID (if not provided, uses latest run)

        Returns:
            WorkflowHandle for the workflow

        Example:
            handle = client.get_workflow_handle("workflow-123")
            result = await handle.result()
        """
        return WorkflowHandle(
            client=self,
            workflow_id=workflow_id,
            run_id=run_id,
            result_run_id=run_id,
        )

    async def list_workflows(
        self,
        query: str = "",
    ) -> list[WorkflowExecutionInfo]:
        """List workflow executions matching a query.

        Args:
            query: A Temporal visibility query string (e.g.
                ``'WorkflowType="MyWorkflow"'``). An empty string returns all
                workflows.

        Returns:
            List of :py:class:`WorkflowExecutionInfo` for matching workflows.

        Example:
            workflows = await client.list_workflows()
            for wf in workflows:
                print(f"{wf.workflow_id}: {wf.status}")
        """
        request = ListWorkflowExecutionsRequest(
            namespace=self._config.namespace,
            query=query,
        )
        request_bytes = request.SerializeToString()

        response_bytes = await self._bridge.list_workflows(request_bytes)

        response = ListWorkflowExecutionsResponse()
        response.ParseFromString(response_bytes)

        results: list[WorkflowExecutionInfo] = []
        for info in response.executions:
            # Parse timestamps
            start_time: Optional[datetime] = None
            if info.HasField("start_time"):
                start_time = info.start_time.ToDatetime().replace(tzinfo=timezone.utc)

            close_time: Optional[datetime] = None
            if info.HasField("close_time"):
                close_time = info.close_time.ToDatetime().replace(tzinfo=timezone.utc)

            # Parse status
            status: Optional[WorkflowExecutionStatus] = None
            if info.status:
                status = WorkflowExecutionStatus(info.status)

            results.append(
                WorkflowExecutionInfo(
                    workflow_id=info.execution.workflow_id,
                    run_id=info.execution.run_id,
                    status=status,
                    workflow_type=info.type.name,
                    start_time=start_time,
                    close_time=close_time,
                )
            )

        return results

    async def count_workflows(
        self,
        query: str = "",
    ) -> int:
        """Count workflow executions matching a query.

        Args:
            query: A Temporal visibility query string (e.g.
                ``'WorkflowType="MyWorkflow"'``). An empty string counts all
                workflows.

        Returns:
            Number of matching workflows.

        Example:
            count = await client.count_workflows('WorkflowType="MyWorkflow"')
            print(f"Found {count} workflows")
        """
        request = CountWorkflowExecutionsRequest(
            namespace=self._config.namespace,
            query=query,
        )
        request_bytes = request.SerializeToString()

        response_bytes = await self._bridge.count_workflows(request_bytes)

        response = CountWorkflowExecutionsResponse()
        response.ParseFromString(response_bytes)

        return response.count

    async def close(self) -> None:
        """Close the client and release resources.

        Example:
            await client.close()
        """
        await self._bridge.shutdown()

    @property
    def data_converter(self) -> DataConverter:
        """Get the data converter used by this client."""
        return self._data_converter

    @property
    def namespace(self) -> str:
        """Get the namespace for this client."""
        return self._config.namespace

    @property
    def identity(self) -> str:
        """Get the identity for this client."""
        return self._config.identity or ""

    @staticmethod
    def _duration_to_proto(duration: Union[timedelta, float, None]) -> Optional[dict]:
        """Convert duration to protobuf duration dict.

        Accepts timedelta, float (seconds), or None.
        """
        if duration is None:
            return None
        if isinstance(duration, timedelta):
            total_seconds = duration.total_seconds()
        else:
            total_seconds = duration
        seconds = int(total_seconds)
        nanos = int((total_seconds - seconds) * 1e9)
        return {"seconds": seconds, "nanos": nanos}


__all__ = ["Client", "ClientConfig", "WorkflowExecutionInfo"]
