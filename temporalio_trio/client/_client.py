"""Trio-based Temporal client implementation.

This module provides a pure Trio client for interacting with Temporal workflows.
It uses the TrioBridgeWrapper to communicate with SDK Core via the Rust bridge.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Optional, Sequence, Type, TypeVar, Union

import trio
from temporalio.api.common.v1 import Payloads, WorkflowExecution
from temporalio.api.enums.v1 import WorkflowIdReusePolicy
from temporalio.api.workflowservice.v1 import (
    StartWorkflowExecutionRequest,
    StartWorkflowExecutionResponse,
)
from temporalio.common import RetryPolicy, SearchAttributes
from temporalio.converter import DataConverter

from .._async_bridge import TrioBridgeWrapper
from ._workflow_handle import WorkflowHandle


@dataclass
class ClientConfig:
    """Configuration for connecting to Temporal server."""

    target_url: str
    namespace: str = "default"
    identity: Optional[str] = None
    data_converter: Optional[DataConverter] = None


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
    ) -> "Client":
        """Connect to Temporal server.

        Args:
            target_url: Temporal server URL (e.g., "localhost:7233")
            namespace: Temporal namespace (default: "default")
            identity: Client identity (default: auto-generated)
            data_converter: Data converter for payload serialization

        Returns:
            Connected Client instance

        Example:
            client = await Client.connect("localhost:7233", namespace="default")
        """
        # Ensure URL has scheme - target_url might be just "localhost:7233"
        if not target_url.startswith(("http://", "https://")):
            target_url = f"http://{target_url}"

        config = ClientConfig(
            target_url=target_url,
            namespace=namespace,
            identity=identity or f"trio-client-{uuid.uuid4()}",
            data_converter=data_converter,
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
        *args: Any,
        id: str,
        task_queue: str,
        execution_timeout: Optional[float] = None,
        run_timeout: Optional[float] = None,
        task_timeout: Optional[float] = None,
        id_reuse_policy: WorkflowIdReusePolicy.ValueType = WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
        retry_policy: Optional[RetryPolicy] = None,
        cron_schedule: Optional[str] = None,
        memo: Optional[dict[str, Any]] = None,
        search_attributes: Optional[Union[SearchAttributes, dict[str, Any]]] = None,
        start_delay: Optional[float] = None,
        request_eager_start: bool = False,
    ) -> WorkflowHandle:
        """Start a workflow execution.

        Args:
            workflow: Workflow type name or class
            *args: Positional arguments for the workflow
            id: Unique workflow ID
            task_queue: Task queue name
            execution_timeout: Total workflow execution timeout
            run_timeout: Single run timeout
            task_timeout: Workflow task timeout
            id_reuse_policy: Workflow ID reuse policy
            retry_policy: Retry policy for workflow
            cron_schedule: Cron schedule (if applicable)
            memo: Memo for workflow
            search_attributes: Search attributes
            start_delay: Delay before starting workflow
            request_eager_start: Request eager workflow start

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

        # Encode arguments
        input_payloads = None
        if args:
            payloads_list = await self._data_converter.encode(args)
            input_payloads = Payloads(payloads=payloads_list)

        # Build request
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
            request_eager_execution=request_eager_start,
        )

        # Serialize request
        request_bytes = request.SerializeToString()

        # Call bridge
        response_bytes = await self._bridge.start_workflow_execution(request_bytes)

        # Parse response
        response = StartWorkflowExecutionResponse()
        response.ParseFromString(response_bytes)

        # Return handle
        return WorkflowHandle(
            client=self,
            workflow_id=id,
            run_id=response.run_id,
            result_run_id=response.run_id,
        )

    async def execute_workflow(
        self,
        workflow: str,
        *args: Any,
        id: str,
        task_queue: str,
        execution_timeout: Optional[float] = None,
        run_timeout: Optional[float] = None,
        task_timeout: Optional[float] = None,
        id_reuse_policy: WorkflowIdReusePolicy.ValueType = WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
        retry_policy: Optional[RetryPolicy] = None,
        cron_schedule: Optional[str] = None,
        memo: Optional[dict[str, Any]] = None,
        search_attributes: Optional[Union[SearchAttributes, dict[str, Any]]] = None,
        start_delay: Optional[float] = None,
        request_eager_start: bool = False,
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
            *args,
            id=id,
            task_queue=task_queue,
            execution_timeout=execution_timeout,
            run_timeout=run_timeout,
            task_timeout=task_timeout,
            id_reuse_policy=id_reuse_policy,
            retry_policy=retry_policy,
            cron_schedule=cron_schedule,
            memo=memo,
            search_attributes=search_attributes,
            start_delay=start_delay,
            request_eager_start=request_eager_start,
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

    @staticmethod
    def _duration_to_proto(duration: Optional[float]) -> Optional[dict]:
        """Convert duration in seconds to protobuf duration dict."""
        if duration is None:
            return None
        seconds = int(duration)
        nanos = int((duration - seconds) * 1e9)
        return {"seconds": seconds, "nanos": nanos}


__all__ = ["Client", "ClientConfig"]
