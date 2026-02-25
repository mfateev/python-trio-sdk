"""Workflow handle for interacting with workflow executions."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import IntEnum
from typing import TYPE_CHECKING, Any, Optional

import temporalio.api.update.v1
import temporalio.common
import temporalio.exceptions
import trio
from temporalio.api.common.v1 import Payloads
from temporalio.api.enums.v1 import EventType
from temporalio.api.enums.v1 import (
    WorkflowExecutionStatus as _ProtoWorkflowExecutionStatus,
)
from temporalio.api.workflowservice.v1 import (
    DescribeWorkflowExecutionResponse,
    GetWorkflowExecutionHistoryResponse,
    QueryWorkflowResponse,
)
from temporalio.converter import DataConverter

from temporalio_trio.workflow import (
    _QueryDefinition,
    _SignalDefinition,
    _UpdateDefinition,
)

if TYPE_CHECKING:
    from ._client import Client

logger = logging.getLogger(__name__)


class WorkflowExecutionStatus(IntEnum):
    """Status of a workflow execution.

    Mirrors ``temporalio.api.enums.v1.WorkflowExecutionStatus`` as a
    friendly :class:`IntEnum`.
    """

    RUNNING = int(
        _ProtoWorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_RUNNING
    )
    COMPLETED = int(
        _ProtoWorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_COMPLETED
    )
    FAILED = int(
        _ProtoWorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_FAILED
    )
    CANCELED = int(
        _ProtoWorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CANCELED
    )
    TERMINATED = int(
        _ProtoWorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TERMINATED
    )
    CONTINUED_AS_NEW = int(
        _ProtoWorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW
    )
    TIMED_OUT = int(
        _ProtoWorkflowExecutionStatus.WORKFLOW_EXECUTION_STATUS_TIMED_OUT
    )


@dataclass
class WorkflowExecutionDescription:
    """Description for a single workflow execution run.

    Returned by :py:meth:`WorkflowHandle.describe`.
    """

    workflow_id: str
    """ID for the workflow."""

    run_id: str
    """Run ID for this workflow run."""

    status: Optional[WorkflowExecutionStatus]
    """Current status of the workflow execution."""

    workflow_type: str
    """Type name for the workflow."""

    task_queue: str
    """Task queue for the workflow."""

    start_time: Optional[datetime]
    """When the workflow was created."""

    close_time: Optional[datetime]
    """When the workflow was closed if closed."""

    execution_time: Optional[datetime]
    """When this workflow run started or should start."""

    history_length: int
    """Number of events in the history."""

    raw_description: DescribeWorkflowExecutionResponse
    """Underlying protobuf description response."""


@dataclass
class WorkflowHistory:
    """History for a single workflow execution.

    Returned by :py:meth:`WorkflowHandle.fetch_history`.
    """

    workflow_id: str
    """ID for the workflow."""

    events: list[Any]
    """List of history events (protobuf HistoryEvent objects)."""


class WorkflowFailureError(temporalio.exceptions.TemporalError):
    """Error that occurs when a workflow is unsuccessful.

    This is raised by :py:meth:`WorkflowHandle.result` when the workflow
    fails, is cancelled, or is terminated. The :py:attr:`cause` attribute
    contains the underlying error.
    """

    def __init__(self, *, cause: BaseException) -> None:
        """Create workflow failure error."""
        super().__init__("Workflow execution failed")
        self.__cause__ = cause

    @property
    def cause(self) -> BaseException:
        """Cause of the workflow failure."""
        assert self.__cause__
        return self.__cause__


class WorkflowQueryRejectedError(temporalio.exceptions.TemporalError):
    """Error that occurs when a query was rejected."""

    def __init__(self, status: Optional[_ProtoWorkflowExecutionStatus]) -> None:
        """Create workflow query rejected error."""
        super().__init__(f"Query rejected, status: {status}")
        self._status = status

    @property
    def status(self) -> Optional[_ProtoWorkflowExecutionStatus]:
        """Get workflow execution status causing rejection."""
        return self._status


class WorkflowContinuedAsNewError(temporalio.exceptions.TemporalError):
    """Raised when a workflow continues as new and follow_runs is False."""

    def __init__(self, new_execution_run_id: str) -> None:
        super().__init__(
            f"Workflow continued as new with run ID: {new_execution_run_id}"
        )
        self.new_execution_run_id = new_execution_run_id


class WorkflowUpdateStage(IntEnum):
    """Stage of a workflow update to wait for."""

    ADMITTED = 1
    """Update has been admitted by the server."""

    ACCEPTED = 2
    """Update has been accepted (validator passed)."""

    COMPLETED = 3
    """Update handler has completed."""


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

    async def result(
        self, *, follow_runs: bool = True, timeout: Optional[float] = None
    ) -> Any:
        """Wait for workflow to complete and return result.

        This method uses server-side long polling - the server blocks until the
        workflow completes, then returns the close event. This matches the
        Python SDK behavior.

        Args:
            follow_runs: If true (default), follows continue-as-new chains to
                get the final result. If false, raises an error when the
                workflow continues as new.
            timeout: Optional timeout in seconds

        Returns:
            The workflow result (deserialized)

        Raises:
            WorkflowFailureError: If workflow failed, was cancelled, or terminated
            trio.TooSlowError: If timeout exceeded

        Example:
            result = await handle.result()
            print(f"Workflow completed with: {result}")
        """

        async def wait_for_result() -> Any:
            run_id = self._result_run_id
            while True:
                # Get workflow close event via server-side long poll
                response_bytes = await self._client._bridge.get_workflow_result(
                    workflow_id=self._workflow_id,
                    run_id=run_id,
                    timeout=None,  # Let server-side timeout handle it
                )

                # Parse response
                response = GetWorkflowExecutionHistoryResponse()
                response.ParseFromString(response_bytes)

                # Try to extract result - returns _WorkflowResult if terminal event found
                wrapped_result = await self._try_extract_result_from_history(
                    response, follow_runs=follow_runs
                )
                if wrapped_result is not None:
                    if wrapped_result.follow_run_id is not None:
                        # Continue-as-new: follow to next run
                        run_id = wrapped_result.follow_run_id
                        continue
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
        query: str | Callable,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        result_type: type | None = None,
        reject_condition: temporalio.common.QueryRejectCondition | None = None,
        timeout: float | None = None,
    ) -> Any:
        """Query the workflow.

        Args:
            query: Query function or name on the workflow.
            arg: Single argument to the query.
            args: Multiple arguments to the query. Cannot be set if arg is.
            result_type: For string queries, this can set the specific result
                type hint to deserialize into.
            reject_condition: Condition for rejecting the query. If unset/None,
                the client's ``default_workflow_query_reject_condition`` is
                used. If that is also None, no rejection condition is applied.
            timeout: Optional timeout in seconds.

        Returns:
            Result of the query.

        Raises:
            WorkflowQueryRejectedError: A query reject condition was satisfied.

        Example:
            status = await handle.query("get_status")
            count = await handle.query(MyWorkflow.get_count)
        """
        # Resolve query name from callable or string
        query_name: str
        ret_type = result_type
        if callable(query):
            defn = _QueryDefinition.from_fn(query)
            if not defn:
                raise RuntimeError(
                    f"Query definition not found on {query.__qualname__}, "
                    "is it decorated with @workflow.query?"
                )
            elif not defn.name:
                raise RuntimeError("Cannot invoke dynamic query definition")
            query_name = defn.name
        else:
            query_name = str(query)

        # Use client default reject condition if not explicitly provided
        if reject_condition is None:
            reject_condition = (
                self._client._config.default_workflow_query_reject_condition
            )

        # Normalize args
        resolved_args = temporalio.common._arg_or_args(arg, args)

        # Encode query arguments
        args_bytes = b""
        if resolved_args:
            payloads_list = await self._client.data_converter.encode(resolved_args)
            payloads = Payloads(payloads=payloads_list)
            args_bytes = payloads.SerializeToString()

        # Call bridge
        response_bytes = await self._client._bridge.query_workflow(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            query_type=query_name,
            args_bytes=args_bytes,
            reject_condition=reject_condition.value if reject_condition is not None else None,
            timeout=timeout,
        )

        # Parse response
        resp = QueryWorkflowResponse()
        resp.ParseFromString(response_bytes)

        # Check for rejection
        if resp.HasField("query_rejected"):
            raise WorkflowQueryRejectedError(
                _ProtoWorkflowExecutionStatus(resp.query_rejected.status)
                if resp.query_rejected.status
                else None
            )

        # Decode result
        if not resp.query_result.payloads:
            return None
        type_hints = [ret_type] if ret_type else None
        results = await self._client.data_converter.decode(
            resp.query_result.payloads, type_hints
        )
        if not results:
            return None
        return results[0]

    async def signal(
        self,
        signal: str | Callable,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        timeout: Optional[float] = None,
    ) -> None:
        """Send signal to workflow.

        Args:
            signal: Signal name or signal-decorated method reference.
            arg: Single argument to the signal.
            args: Multiple arguments to the signal. Cannot be set if arg is.
            timeout: Optional timeout in seconds.

        Example:
            await handle.signal("update_value", 42)
            await handle.signal(MyWorkflow.my_signal, "data")
        """
        # Resolve signal name from callable or string
        if callable(signal):
            defn = _SignalDefinition.from_fn(signal)
            if not defn:
                raise RuntimeError(
                    f"Signal definition not found on {signal.__qualname__}, "
                    "is it decorated with @workflow.signal?"
                )
            elif not defn.name:
                raise RuntimeError("Cannot invoke dynamic signal definition")
            signal_name = defn.name
        else:
            signal_name = str(signal)

        # Normalize args
        resolved_args = temporalio.common._arg_or_args(arg, args)

        # Encode signal arguments
        args_bytes = b""
        if resolved_args:
            payloads_list = await self._client.data_converter.encode(resolved_args)
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

    async def execute_update(
        self,
        update: str | Callable[..., Any],
        arg: Any = None,
        *,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> Any:
        """Send an update request and wait for it to complete.

        Args:
            update: Update name or handler reference.
            arg: Single argument to the update.
            args: Multiple arguments to the update.
            id: Optional update ID. Generated if not provided.
            timeout: Optional timeout in seconds.

        Returns:
            The result of the update handler.

        Raises:
            RuntimeError: If the update fails.
        """
        handle = await self.start_update(
            update,
            arg=arg,
            args=args,
            id=id,
            wait_for_stage=WorkflowUpdateStage.COMPLETED,
            timeout=timeout,
        )
        return handle.result_value

    async def start_update(
        self,
        update: str | Callable[..., Any],
        arg: Any = None,
        *,
        args: Sequence[Any] = [],
        id: Optional[str] = None,
        wait_for_stage: "WorkflowUpdateStage" = WorkflowUpdateStage.ACCEPTED,
        timeout: Optional[float] = None,
    ) -> "WorkflowUpdateHandle":
        """Send an update request and wait for it to reach the specified stage.

        Args:
            update: Update name or handler reference.
            arg: Single argument to the update.
            args: Multiple arguments to the update.
            id: Optional update ID. Generated if not provided.
            wait_for_stage: Stage to wait for (ACCEPTED or COMPLETED).
            timeout: Optional timeout in seconds.

        Returns:
            Handle to the update.

        Raises:
            RuntimeError: If the update fails validation or handler fails.
        """
        import uuid

        from temporalio.api.common.v1 import Payloads, WorkflowExecution
        from temporalio.api.enums.v1 import UpdateWorkflowExecutionLifecycleStage
        from temporalio.api.update.v1 import (
            Input as UpdateInput,
            Meta as UpdateMeta,
            Request as UpdateRequest,
            WaitPolicy,
        )
        from temporalio.api.workflowservice.v1 import (
            UpdateWorkflowExecutionRequest,
            UpdateWorkflowExecutionResponse,
        )

        # Resolve update name
        if callable(update):
            defn = _UpdateDefinition.from_fn(update)
            if defn and defn.name:
                update_name = defn.name
            else:
                update_name = getattr(update, "__name__", str(update))
        else:
            update_name = update

        # Handle arg/args
        if arg is not None and args:
            raise ValueError("Cannot specify both arg and args")
        if arg is not None:
            actual_args = [arg]
        else:
            actual_args = list(args)

        # Generate update ID if not provided
        update_id = id or str(uuid.uuid4())

        # Encode arguments
        data_converter = self._client.data_converter
        input_payloads = None
        if actual_args:
            encoded = data_converter.payload_converter.to_payloads(actual_args)
            input_payloads = Payloads(payloads=encoded)

        # Map stage
        if wait_for_stage == WorkflowUpdateStage.COMPLETED:
            lifecycle_stage = (
                UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED
            )
        else:
            lifecycle_stage = (
                UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ACCEPTED
            )

        # Build the UpdateWorkflowExecutionRequest
        request = UpdateWorkflowExecutionRequest(
            namespace=self._client.namespace,
            workflow_execution=WorkflowExecution(
                workflow_id=self._workflow_id,
                run_id=self._run_id or "",
            ),
            request=UpdateRequest(
                meta=UpdateMeta(
                    update_id=update_id,
                    identity=self._client.identity,
                ),
                input=UpdateInput(
                    name=update_name,
                    args=input_payloads,
                ),
            ),
            wait_policy=WaitPolicy(
                lifecycle_stage=lifecycle_stage,
            ),
        )

        # Send via bridge
        response_bytes = await self._client._bridge.update_workflow(
            request.SerializeToString(),
            timeout=timeout,
        )

        # Parse response
        response = UpdateWorkflowExecutionResponse()
        response.ParseFromString(response_bytes)

        # Extract result if completed
        result_value = None
        if response.HasField("outcome"):
            outcome = response.outcome
            if outcome.HasField("success"):
                if outcome.success.payloads:
                    decoded = data_converter.payload_converter.from_payloads(
                        outcome.success.payloads
                    )
                    result_value = decoded[0] if decoded else None
            elif outcome.HasField("failure"):
                # Convert failure to exception
                from temporalio_trio.worker._failure_converter import (
                    failure_to_exception,
                )

                cause = failure_to_exception(
                    outcome.failure,
                    data_converter.payload_converter,
                )
                raise RuntimeError(f"Update failed: {cause}")

        return WorkflowUpdateHandle(
            id=update_id,
            workflow_id=self._workflow_id,
            result_value=result_value,
            _client=self._client,
        )

    async def describe(
        self, *, timeout: Optional[float] = None
    ) -> WorkflowExecutionDescription:
        """Get workflow execution details.

        This will get details for :py:attr:`run_id` if present. To use a
        different run ID, create a new handle via
        :py:meth:`Client.get_workflow_handle`.

        Args:
            timeout: Optional timeout in seconds.

        Returns:
            Workflow execution description with status, type, times, etc.

        Raises:
            RuntimeError: If the bridge call fails.

        Example:
            desc = await handle.describe()
            print(f"Status: {desc.status}, Type: {desc.workflow_type}")
        """
        response_bytes = await self._client._bridge.describe_workflow(
            workflow_id=self._workflow_id,
            run_id=self._run_id,
            timeout=timeout,
        )

        # Parse protobuf response
        resp = DescribeWorkflowExecutionResponse()
        resp.ParseFromString(response_bytes)

        # Extract info from response
        info = resp.workflow_execution_info

        # Parse timestamps
        start_time: Optional[datetime] = None
        if info.HasField("start_time"):
            start_time = info.start_time.ToDatetime().replace(tzinfo=timezone.utc)

        close_time: Optional[datetime] = None
        if info.HasField("close_time"):
            close_time = info.close_time.ToDatetime().replace(tzinfo=timezone.utc)

        execution_time: Optional[datetime] = None
        if info.HasField("execution_time"):
            execution_time = info.execution_time.ToDatetime().replace(
                tzinfo=timezone.utc
            )

        # Parse status
        status: Optional[WorkflowExecutionStatus] = None
        if info.status:
            status = WorkflowExecutionStatus(info.status)

        return WorkflowExecutionDescription(
            workflow_id=info.execution.workflow_id,
            run_id=info.execution.run_id,
            status=status,
            workflow_type=info.type.name,
            task_queue=info.task_queue,
            start_time=start_time,
            close_time=close_time,
            execution_time=execution_time,
            history_length=info.history_length,
            raw_description=resp,
        )

    async def fetch_history(self) -> WorkflowHistory:
        """Get workflow history.

        Returns all history events for the workflow execution. If the workflow
        has a large history, this will automatically paginate through all pages
        to collect all events.

        Returns:
            WorkflowHistory with the workflow ID and all history events.

        Example:
            history = await handle.fetch_history()
            for event in history.events:
                print(f"Event: {event.event_type}")
        """
        events = await self.fetch_history_events()
        return WorkflowHistory(
            workflow_id=self._workflow_id,
            events=events,
        )

    async def fetch_history_events(self) -> list[Any]:
        """Get workflow history events.

        Returns all history events for the workflow execution. If the workflow
        has a large history, this will automatically paginate through all pages
        to collect all events.

        Returns:
            List of history events (protobuf HistoryEvent objects).

        Example:
            events = await handle.fetch_history_events()
            for event in events:
                print(f"Event: {event.event_type}")
        """
        all_events: list[Any] = []
        next_page_token = b""

        while True:
            response_bytes = (
                await self._client._bridge.get_workflow_execution_history(
                    workflow_id=self._workflow_id,
                    run_id=self._run_id,
                    next_page_token=next_page_token,
                )
            )

            response = GetWorkflowExecutionHistoryResponse()
            response.ParseFromString(response_bytes)

            all_events.extend(response.history.events)

            # Check for more pages
            if response.next_page_token:
                next_page_token = response.next_page_token
            else:
                break

        return all_events

    async def _try_extract_result_from_history(
        self,
        response: GetWorkflowExecutionHistoryResponse,
        *,
        follow_runs: bool = True,
    ) -> Optional["_WorkflowResult"]:
        """Try to extract workflow result from history response.

        Args:
            response: GetWorkflowExecutionHistoryResponse
            follow_runs: If true, return follow_run_id for continue-as-new

        Returns:
            _WorkflowResult wrapper if terminal event found, None if still running

        Raises:
            WorkflowFailureError: If workflow failed, was cancelled, or terminated
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
                # Workflow failed - decode the failure chain
                failed = event.workflow_execution_failed_event_attributes
                if failed.failure and failed.failure.ByteSize():
                    cause = await self._client.data_converter.decode_failure(
                        failed.failure
                    )
                else:
                    cause = temporalio.exceptions.ApplicationError("Unknown error")
                raise WorkflowFailureError(cause=cause)

            elif event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
                raise WorkflowFailureError(
                    cause=temporalio.exceptions.CancelledError("Workflow cancelled")
                )

            elif event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
                terminated = event.workflow_execution_terminated_event_attributes
                raise WorkflowFailureError(
                    cause=temporalio.exceptions.TerminatedError(
                        terminated.reason or "Workflow terminated"
                    )
                )

            elif event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
                raise WorkflowFailureError(
                    cause=temporalio.exceptions.TimeoutError("Workflow timed out")
                )

            elif event.event_type == EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
                continued = event.workflow_execution_continued_as_new_event_attributes
                new_run_id = continued.new_execution_run_id
                if follow_runs:
                    # Follow to the new run
                    return _WorkflowResult(None, follow_run_id=new_run_id)
                else:
                    raise WorkflowContinuedAsNewError(new_run_id)

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
            WorkflowFailureError: If workflow failed, cancelled, or terminated
            RuntimeError: If workflow not complete
        """
        result = await self._try_extract_result_from_history(response)
        if result is None:
            raise RuntimeError("Workflow result not found in history")
        return result.value


class _WorkflowResult:
    """Wrapper to distinguish between 'no result yet' and 'result is None'.

    Also supports follow_run_id for continue-as-new chains.
    """

    __slots__ = ("value", "follow_run_id")

    def __init__(
        self, value: Any, follow_run_id: Optional[str] = None
    ) -> None:
        self.value = value
        self.follow_run_id = follow_run_id


@dataclass
class WorkflowUpdateHandle:
    """Handle to a workflow update.

    Returned by :py:meth:`WorkflowHandle.start_update`.
    """

    id: str
    """Update ID."""

    workflow_id: str
    """Workflow ID."""

    result_value: Any = None
    """Result value if the update has completed."""

    _client: Any = None
    """Client reference for future polling."""

    async def result(self, *, timeout: Optional[float] = None) -> Any:
        """Get the result of the update.

        If the update has already completed, returns the stored result.
        Otherwise, polls for the result.

        Args:
            timeout: Optional timeout in seconds.

        Returns:
            The result of the update handler.
        """
        if self.result_value is not None:
            return self.result_value

        # Poll for result
        from temporalio.api.update.v1 import WaitPolicy
        from temporalio.api.enums.v1 import UpdateWorkflowExecutionLifecycleStage
        from temporalio.api.workflowservice.v1 import (
            PollWorkflowExecutionUpdateRequest,
            PollWorkflowExecutionUpdateResponse,
        )
        from temporalio.api.update.v1 import UpdateRef
        from temporalio.api.common.v1 import WorkflowExecution

        request = PollWorkflowExecutionUpdateRequest(
            namespace=self._client.namespace,
            update_ref=UpdateRef(
                workflow_execution=WorkflowExecution(
                    workflow_id=self.workflow_id,
                ),
                update_id=self.id,
            ),
            identity=self._client.identity,
            wait_policy=WaitPolicy(
                lifecycle_stage=UpdateWorkflowExecutionLifecycleStage.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_COMPLETED,
            ),
        )

        response_bytes = await self._client._bridge.poll_workflow_execution_update(
            request.SerializeToString(),
            timeout=timeout,
        )

        response = PollWorkflowExecutionUpdateResponse()
        response.ParseFromString(response_bytes)

        if response.HasField("outcome"):
            outcome = response.outcome
            if outcome.HasField("success"):
                if outcome.success.payloads:
                    decoded = self._client.data_converter.payload_converter.from_payloads(
                        outcome.success.payloads
                    )
                    self.result_value = decoded[0] if decoded else None
                    return self.result_value
            elif outcome.HasField("failure"):
                cause = await self._client.data_converter.decode_failure(
                    outcome.failure
                )
                raise RuntimeError(f"Update failed: {cause}")

        return self.result_value


__all__ = [
    "WorkflowContinuedAsNewError",
    "WorkflowExecutionDescription",
    "WorkflowExecutionStatus",
    "WorkflowFailureError",
    "WorkflowHandle",
    "WorkflowHistory",
    "WorkflowQueryRejectedError",
    "WorkflowUpdateHandle",
    "WorkflowUpdateStage",
]
