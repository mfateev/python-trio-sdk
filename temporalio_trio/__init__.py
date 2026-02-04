"""Temporal Python SDK with Trio support.

This is an experimental implementation of the Temporal Python SDK using Trio
instead of asyncio for the async runtime.

Warning:
    This package is experimental and not ready for production use.
"""

from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    CancelledError,
    ChildWorkflowError,
    FailureError,
    ServerError,
    TemporalError,
    TerminatedError,
    TimeoutError,
)

from temporalio_trio import activity, client, testing, worker, workflow

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "activity",
    "client",
    "testing",
    "workflow",
    "worker",
    # Re-exported exceptions from temporalio.exceptions
    "ActivityError",
    "ApplicationError",
    "CancelledError",
    "ChildWorkflowError",
    "FailureError",
    "ServerError",
    "TemporalError",
    "TerminatedError",
    "TimeoutError",
]
