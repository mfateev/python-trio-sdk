"""Workflow handle for interacting with workflow executions."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any, Optional

import trio
from temporalio.api.common.v1 import Payloads
from temporalio.api.enums.v1 import EventType
from temporalio.api.workflowservice.v1 import GetWorkflowExecutionHistoryResponse
from temporalio.converter import DataConverter

if TYPE_CHECKING:
    from ._client import Client

logger = logging.getLogger(__name__)


class WorkflowHandle:
    """Handle to a workflow execution.

    Provides methods to interact with a running or completed workflow:
    - result(): Wait for workflow to complete and get result
    - query(): Query workflow state
    - signal(): Send signal to workflow
    - cancel(): Request workflow cancellation
    - terminate(): Forcefully terminate workflow

    Example:
        handle = await client.start_workflow(
            "MyWorkflow",
            "arg",
            id="wf-123",
            task_queue="queue",
        )

        # Wait for result
        result = await handle.result()

        # Or query workflow
        status = await handle.query("get_status")

        # Or signal workflow
        await handle.signal("update_value", 42)

        # Or cancel workflow
        await handle.cancel()
    """

    def __init__(
        self,
        client: "Client",
        workflow_id: str,
        run_id: Optional[str],
        result_run_id: Optional[str],
    ) -> None:
        """Initialize workflow handle (internal).

        Args:
            client: Client instance
            workflow_id: Workflow ID
            run_id: Current run ID (None for latest)
            result_run_id: Run ID to wait for result (None for latest)
        """
        self._client = client
        self._workflow_id = workflow_id
        self._run_id = run_id
        self._result_run_id = result_run_id

    @property
    def workflow_id(self) -> str:
        """Get workflow ID."""
        return self._workflow_id

    @property
    def run_id(self) -> Optional[str]:
        """Get run ID (None if tracking latest run)."""
        return self._run_id

    async def result(self, *, timeout: Optional[float] = None) -> Any:
        """Wait for workflow to complete and return result.

        This method uses server-side long polling - the server blocks until the
        workflow completes, then returns the close event. This matches the
        Python SDK behavior.

        Args:
            timeout: Optional timeout in seconds

        Returns:
            The workflow result (deserialized)

        Raises:
            WorkflowFailureError: If workflow failed
            RuntimeError: If workflow was canceled or terminated
            trio.TooSlowError: If timeout exceeded

        Example:
            result = await handle.result()
            print(f"Workflow completed with: {result}")
        """

        async def wait_for_result() -> Any:
            # Server-side long polling: The Rust bridge makes a GetWorkflowExecutionHistory
            # call with wait_new_event=true and history_event_filter_type=CLOSE_EVENT.
            # The server blocks until the workflow closes, then returns the close event.
            # If server times out, we retry (rare - server timeout is typically 60s).
            while True:
                # Get workflow close event via server-side long poll
                response_bytes = await self._client._bridge.get_workflow_result(
                    workflow_id=self._workflow_id,
                    run_id=self._result_run_id,
                    timeout=None,  # Let server-side timeout handle it
                )

                # Parse response
                response = GetWorkflowExecutionHistoryResponse()
                response.ParseFromString(response_bytes)

                # Try to extract result - returns _WorkflowResult if terminal event found
                wrapped_result = await self._try_extract_result_from_history(response)
                if wrapped_result is not None:
                    return wrapped_result.value

                # No close event yet (server timed out), retry
                logger.debug(
                    f"Workflow {self._workflow_id} not complete yet, retrying long poll"
                )

        if timeout is not None:
            with trio.move_on_after(timeout) as cancel_scope:
                return await wait_for_result()

            if cancel_scope.cancelled_caught:
                raise trio.TooSlowError(
                    f"Workflow {self._workflow_id} did not complete within {timeout}s"
                )
        else:
            return await wait_for_result()

    async def query(
        self,
        query_type: str,
        *args: Any,
        timeout: Optional[float] = None,
    ) -> Any:
        """Query workflow state.

        Args:
            query_type: Query name
            *args: Query arguments
            timeout: Optional timeout in seconds

        Returns:
            Query result (deserialized)

        Example:
            status = await handle.query("get_status")
            count = await handle.query("get_count")
        """
        # Encode query arguments
        args_bytes = b""
        if args:
            payloads_list = await self._client.data_converter.encode(args)
            payloads = Payloads(payloads=payloads_list)
            args_bytes = payloads.SerializeToString()

        # Call bridge
        response_bytes = await self._client._bridge.query_workflow(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            query_type=query_type,
            args_bytes=args_bytes,
            timeout=timeout,
        )

        # Parse response and extract result
        # TODO: Parse QueryWorkflowResponse and deserialize result
        return None  # Placeholder

    async def signal(
        self,
        signal_name: str,
        *args: Any,
        timeout: Optional[float] = None,
    ) -> None:
        """Send signal to workflow.

        Args:
            signal_name: Signal name
            *args: Signal arguments
            timeout: Optional timeout in seconds

        Example:
            await handle.signal("update_value", 42)
            await handle.signal("pause")
        """
        # Encode signal arguments
        args_bytes = b""
        if args:
            payloads_list = await self._client.data_converter.encode(args)
            payloads = Payloads(payloads=payloads_list)
            args_bytes = payloads.SerializeToString()

        # Call bridge
        await self._client._bridge.signal_workflow(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            signal_name=signal_name,
            args_bytes=args_bytes,
            timeout=timeout,
        )

    async def cancel(self, *, timeout: Optional[float] = None) -> None:
        """Request workflow cancellation.

        This sends a cancellation request to the workflow. The workflow can
        handle the cancellation gracefully.

        Args:
            timeout: Optional timeout in seconds

        Example:
            await handle.cancel()
        """
        await self._client._bridge.cancel_workflow_execution(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            timeout=timeout,
        )

    async def terminate(
        self,
        *,
        reason: str = "Terminated by client",
        timeout: Optional[float] = None,
    ) -> None:
        """Forcefully terminate workflow.

        This immediately terminates the workflow without allowing cleanup.

        Args:
            reason: Termination reason
            timeout: Optional timeout in seconds

        Example:
            await handle.terminate(reason="User requested termination")
        """
        await self._client._bridge.terminate_workflow_execution(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            reason=reason,
            timeout=timeout,
        )

    async def _try_extract_result_from_history(
        self, response: GetWorkflowExecutionHistoryResponse
    ) -> Optional["_WorkflowResult"]:
        """Try to extract workflow result from history response.

        Args:
            response: GetWorkflowExecutionHistoryResponse

        Returns:
            _WorkflowResult wrapper if terminal event found, None if still running

        Raises:
            RuntimeError: If workflow failed, was canceled, or terminated
        """
        # Find terminal event in history
        for event in response.history.events:
            if event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
                # Extract result payload
                completed = event.workflow_execution_completed_event_attributes
                if completed.result and completed.result.payloads:
                    # Deserialize payloads
                    results = await self._client.data_converter.decode(
                        completed.result.payloads
                    )
                    return _WorkflowResult(results[0] if results else None)
                return _WorkflowResult(None)

            elif event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
                # Workflow failed
                failed = event.workflow_execution_failed_event_attributes
                raise RuntimeError(
                    f"Workflow failed: {failed.failure.message if failed.failure else 'Unknown error'}"
                )

            elif event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
                raise RuntimeError("Workflow was canceled")

            elif event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
                terminated = event.workflow_execution_terminated_event_attributes
                raise RuntimeError(f"Workflow was terminated: {terminated.reason}")

        # No terminal event found - workflow still running
        return None

    async def _extract_result_from_history(
        self, response: GetWorkflowExecutionHistoryResponse
    ) -> Any:
        """Extract workflow result from history response.

        Args:
            response: GetWorkflowExecutionHistoryResponse

        Returns:
            Deserialized workflow result

        Raises:
            RuntimeError: If workflow failed, canceled, terminated, or not complete
        """
        result = await self._try_extract_result_from_history(response)
        if result is None:
            raise RuntimeError("Workflow result not found in history")
        return result.value


class _WorkflowResult:
    """Wrapper to distinguish between 'no result yet' and 'result is None'."""

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value


__all__ = ["WorkflowHandle"]
