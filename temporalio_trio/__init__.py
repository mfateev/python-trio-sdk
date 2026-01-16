"""Temporal Python SDK with Trio support.

This is an experimental implementation of the Temporal Python SDK using Trio
instead of asyncio for the async runtime.

Warning:
    This package is experimental and not ready for production use.
"""

from temporalio_trio import workflow, worker

__version__ = "0.1.0"

__all__ = [
    "__version__",
    "workflow",
    "worker",
]
