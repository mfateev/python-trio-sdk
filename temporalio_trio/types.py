"""Type definitions for temporalio_trio.

This module mirrors temporalio.types from the SDK.
"""

from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

CallableType = TypeVar("CallableType", bound=Callable[..., Any])
"""TypeVar for any callable."""

CallableAsyncType = TypeVar("CallableAsyncType", bound=Callable[..., Awaitable[Any]])
"""TypeVar for async callables."""
