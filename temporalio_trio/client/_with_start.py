"""Update-with-start support."""

from __future__ import annotations

import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Optional, Union

import temporalio.common
import temporalio.converter
from temporalio.api.common.v1 import Payloads, WorkflowType
from temporalio.api.enums.v1 import (
    WorkflowIdConflictPolicy,
    WorkflowIdReusePolicy,
)
from temporalio.api.taskqueue.v1 import TaskQueue
from temporalio.api.workflowservice.v1 import (
    StartWorkflowExecutionRequest,
)
from temporalio.common import RetryPolicy, SearchAttributes

if TYPE_CHECKING:
    from ._client import Client


class WithStartWorkflowOperation:
    """Operation to start a workflow as part of update-with-start.

    This encapsulates workflow start parameters for use with
    :py:meth:`Client.execute_update_with_start_workflow` or
    :py:meth:`Client.start_update_with_start_workflow`.

    Must not be reused across multiple calls.
    """

    def __init__(
        self,
        workflow: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        args: Sequence[Any] = [],
        id: str,
        task_queue: str,
        execution_timeout: Union[timedelta, float, None] = None,
        run_timeout: Union[timedelta, float, None] = None,
        task_timeout: Union[timedelta, float, None] = None,
        id_reuse_policy: WorkflowIdReusePolicy.ValueType = WorkflowIdReusePolicy.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
        id_conflict_policy: temporalio.common.WorkflowIDConflictPolicy = temporalio.common.WorkflowIDConflictPolicy.UNSPECIFIED,
        retry_policy: Optional[RetryPolicy] = None,
        cron_schedule: Optional[str] = None,
        memo: Optional[dict[str, Any]] = None,
        search_attributes: Optional[Union[SearchAttributes, dict[str, Any]]] = None,
        start_delay: Union[timedelta, float, None] = None,
        priority: temporalio.common.Priority = temporalio.common.Priority.default,
    ) -> None:
        if int(id_conflict_policy) == 0:
            raise ValueError(
                "id_conflict_policy is required for update-with-start "
                "and must not be UNSPECIFIED"
            )

        # Resolve workflow name
        self._workflow = workflow if isinstance(workflow, str) else workflow.__name__
        self._args = temporalio.common._arg_or_args(arg, args)
        self._id = id
        self._task_queue = task_queue
        self._execution_timeout = execution_timeout
        self._run_timeout = run_timeout
        self._task_timeout = task_timeout
        self._id_reuse_policy = id_reuse_policy
        self._id_conflict_policy = id_conflict_policy
        self._retry_policy = retry_policy
        self._cron_schedule = cron_schedule
        self._memo = memo
        self._search_attributes = search_attributes
        self._start_delay = start_delay
        self._priority = priority
        self._used = False

    @property
    def workflow_id(self) -> str:
        """The workflow ID for this operation."""
        return self._id


__all__ = ["WithStartWorkflowOperation"]
