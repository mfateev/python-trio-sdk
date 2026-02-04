"""Failure converter for converting protobuf Failure messages to Python exceptions.

This module provides utilities to convert Temporal Failure protobuf messages
to appropriate Python exception types (ActivityError, ChildWorkflowError, etc.).

The actual conversion logic is delegated to temporalio.converter.DefaultFailureConverter
from the official SDK, which we reuse since temporalio is already a dependency.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import temporalio.api.failure.v1
import temporalio.converter
import temporalio.exceptions

if TYPE_CHECKING:
    pass

__all__ = [
    "failure_to_exception",
]

# Use the SDK's default failure converter singleton
_default_failure_converter = temporalio.converter.DefaultFailureConverter()


def failure_to_exception(
    failure: temporalio.api.failure.v1.Failure,
    payload_converter: temporalio.converter.PayloadConverter,
) -> BaseException:
    """Convert a Temporal Failure protobuf to the appropriate Python exception.

    This function converts Temporal Failure messages to proper exception types
    like ActivityError, ChildWorkflowError, ApplicationError, etc. The conversion
    also handles cause chains - if the failure has a cause, it will be recursively
    converted and set as the __cause__ of the returned exception.

    Args:
        failure: The Temporal Failure protobuf message to convert.
        payload_converter: The payload converter for deserializing failure details.

    Returns:
        The appropriate exception type based on the failure_info field:
        - application_failure_info → ApplicationError
        - timeout_failure_info → TimeoutError
        - canceled_failure_info → CancelledError
        - terminated_failure_info → TerminatedError
        - server_failure_info → ServerError
        - activity_failure_info → ActivityError
        - child_workflow_execution_failure_info → ChildWorkflowError
        - (none) → FailureError

        If the failure has a cause field, the returned exception's __cause__
        will be set to the converted cause exception.

    Example:
        SDK-Core wraps activity failures like this:
        ```
        ActivityError (wrapper with metadata like activity_id, retry_state)
          └── __cause__ → ApplicationError (actual exception from activity)
        ```

        Workflows can catch and inspect:
        ```python
        try:
            await workflow.execute_activity(my_activity, ...)
        except ActivityError as e:
            if isinstance(e.__cause__, ApplicationError):
                print(f"Activity failed with type: {e.__cause__.type}")
        ```
    """
    return _default_failure_converter.from_failure(failure, payload_converter)
