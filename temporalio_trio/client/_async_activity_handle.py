"""Async activity handle for external activity completion."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Optional, Sequence

import temporalio.common
import temporalio.exceptions
from temporalio.api.common.v1 import Payloads
from temporalio.api.failure.v1 import Failure
from temporalio.api.workflowservice.v1 import (
    RecordActivityTaskHeartbeatByIdRequest,
    RecordActivityTaskHeartbeatByIdResponse,
    RecordActivityTaskHeartbeatRequest,
    RecordActivityTaskHeartbeatResponse,
    RespondActivityTaskCanceledByIdRequest,
    RespondActivityTaskCanceledRequest,
    RespondActivityTaskCompletedByIdRequest,
    RespondActivityTaskCompletedRequest,
    RespondActivityTaskFailedByIdRequest,
    RespondActivityTaskFailedRequest,
)

if TYPE_CHECKING:
    from ._client import Client


@dataclass(frozen=True)
class AsyncActivityIDReference:
    """Reference to an async activity by workflow ID and activity ID."""

    workflow_id: str | None
    run_id: str | None
    activity_id: str


class AsyncActivityCancelledError(temporalio.exceptions.TemporalError):
    """Error raised when an async activity heartbeat discovers cancellation."""

    def __init__(self) -> None:
        super().__init__("Activity cancelled")


class AsyncActivityHandle:
    """Handle for completing, failing, or heartbeating an async activity.

    Obtained via :py:meth:`Client.get_async_activity_handle`.
    """

    def __init__(
        self,
        client: Client,
        id_or_token: AsyncActivityIDReference | bytes,
    ) -> None:
        self._client = client
        self._id_or_token = id_or_token

    async def heartbeat(
        self,
        *details: Any,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Record a heartbeat for the async activity.

        Args:
            details: Heartbeat detail values.

        Raises:
            AsyncActivityCancelledError: If the activity has been cancelled.
        """
        dc = self._client.data_converter

        # Encode details
        encoded_details: Payloads | None = None
        if details:
            payloads_list = await dc.encode(list(details))
            encoded_details = Payloads(payloads=payloads_list)

        if isinstance(self._id_or_token, AsyncActivityIDReference):
            ref = self._id_or_token
            req = RecordActivityTaskHeartbeatByIdRequest(
                workflow_id=ref.workflow_id or "",
                run_id=ref.run_id or "",
                activity_id=ref.activity_id,
                namespace=self._client.namespace,
                identity=self._client.identity,
                details=encoded_details,
            )
            resp_bytes = (
                await self._client._bridge.record_activity_task_heartbeat_by_id(
                    req.SerializeToString()
                )
            )
            resp = RecordActivityTaskHeartbeatByIdResponse()
            resp.ParseFromString(resp_bytes)
            if resp.cancel_requested:
                raise AsyncActivityCancelledError()
        else:
            req_tok = RecordActivityTaskHeartbeatRequest(
                task_token=self._id_or_token,
                namespace=self._client.namespace,
                identity=self._client.identity,
                details=encoded_details,
            )
            resp_bytes = await self._client._bridge.record_activity_task_heartbeat(
                req_tok.SerializeToString()
            )
            resp_tok = RecordActivityTaskHeartbeatResponse()
            resp_tok.ParseFromString(resp_bytes)
            if resp_tok.cancel_requested:
                raise AsyncActivityCancelledError()

    async def complete(
        self,
        result: Any = temporalio.common._arg_unset,
        *,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Complete the async activity with a result.

        Args:
            result: The result value. Use the default (unset) sentinel
                to complete without a result.
        """
        dc = self._client.data_converter

        # Encode result
        encoded_result: Payloads | None = None
        if result is not temporalio.common._arg_unset:
            payloads_list = await dc.encode([result])
            encoded_result = Payloads(payloads=payloads_list)

        if isinstance(self._id_or_token, AsyncActivityIDReference):
            ref = self._id_or_token
            req = RespondActivityTaskCompletedByIdRequest(
                workflow_id=ref.workflow_id or "",
                run_id=ref.run_id or "",
                activity_id=ref.activity_id,
                namespace=self._client.namespace,
                identity=self._client.identity,
                result=encoded_result,
            )
            await self._client._bridge.respond_activity_task_completed_by_id(
                req.SerializeToString()
            )
        else:
            req_tok = RespondActivityTaskCompletedRequest(
                task_token=self._id_or_token,
                namespace=self._client.namespace,
                identity=self._client.identity,
                result=encoded_result,
            )
            await self._client._bridge.respond_activity_task_completed(
                req_tok.SerializeToString()
            )

    async def fail(
        self,
        error: Exception,
        *,
        last_heartbeat_details: Sequence[Any] = [],
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Fail the async activity with an error.

        Args:
            error: The error that caused the failure.
            last_heartbeat_details: Details from the last heartbeat.
        """
        dc = self._client.data_converter

        # Encode failure
        failure = Failure()
        await dc.encode_failure(error, failure)

        # Encode last heartbeat details
        encoded_details: Payloads | None = None
        if last_heartbeat_details:
            payloads_list = await dc.encode(list(last_heartbeat_details))
            encoded_details = Payloads(payloads=payloads_list)

        if isinstance(self._id_or_token, AsyncActivityIDReference):
            ref = self._id_or_token
            req = RespondActivityTaskFailedByIdRequest(
                workflow_id=ref.workflow_id or "",
                run_id=ref.run_id or "",
                activity_id=ref.activity_id,
                namespace=self._client.namespace,
                identity=self._client.identity,
                failure=failure,
                last_heartbeat_details=encoded_details,
            )
            await self._client._bridge.respond_activity_task_failed_by_id(
                req.SerializeToString()
            )
        else:
            req_tok = RespondActivityTaskFailedRequest(
                task_token=self._id_or_token,
                namespace=self._client.namespace,
                identity=self._client.identity,
                failure=failure,
                last_heartbeat_details=encoded_details,
            )
            await self._client._bridge.respond_activity_task_failed(
                req_tok.SerializeToString()
            )

    async def report_cancellation(
        self,
        *details: Any,
        rpc_metadata: Mapping[str, str] = {},
        rpc_timeout: Optional[timedelta] = None,
    ) -> None:
        """Report that the async activity has been cancelled.

        Args:
            details: Cancellation detail values.
        """
        dc = self._client.data_converter

        # Encode details
        encoded_details: Payloads | None = None
        if details:
            payloads_list = await dc.encode(list(details))
            encoded_details = Payloads(payloads=payloads_list)

        if isinstance(self._id_or_token, AsyncActivityIDReference):
            ref = self._id_or_token
            req = RespondActivityTaskCanceledByIdRequest(
                workflow_id=ref.workflow_id or "",
                run_id=ref.run_id or "",
                activity_id=ref.activity_id,
                namespace=self._client.namespace,
                identity=self._client.identity,
                details=encoded_details,
            )
            await self._client._bridge.respond_activity_task_canceled_by_id(
                req.SerializeToString()
            )
        else:
            req_tok = RespondActivityTaskCanceledRequest(
                task_token=self._id_or_token,
                namespace=self._client.namespace,
                identity=self._client.identity,
                details=encoded_details,
            )
            await self._client._bridge.respond_activity_task_canceled(
                req_tok.SerializeToString()
            )


__all__ = [
    "AsyncActivityCancelledError",
    "AsyncActivityHandle",
    "AsyncActivityIDReference",
]
