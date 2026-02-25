"""Workflow test environment for Trio.

This module provides a testing environment for Temporal workflows using Trio.
"""

from __future__ import annotations

import logging
import os
import shutil
import signal
import socket
import subprocess
import warnings
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncIterator, Optional, Sequence

import trio

import temporalio.common
import temporalio.converter

if TYPE_CHECKING:
    from temporalio_trio.client import Client

logger = logging.getLogger(__name__)


def _find_free_port() -> int:
    """Find a free port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.listen(1)
        port = s.getsockname()[1]
    return port


class WorkflowEnvironment:
    """Workflow environment for testing workflows with Trio.

    This environment provides a way to run workflows in a test context.
    It can either connect to an existing Temporal server or start a local
    dev server for testing.

    Example:
        async with await WorkflowEnvironment.start_local() as env:
            # Use env.client to interact with Temporal
            handle = await env.client.start_workflow(
                "MyWorkflow",
                id="test-1",
                task_queue="test-queue",
            )
            result = await handle.result()

    The environment is an async context manager that handles cleanup
    automatically when exiting the context.
    """

    def __init__(
        self,
        client: "Client",
        _server_process: Optional[subprocess.Popen] = None,
    ) -> None:
        """Initialize workflow environment (internal - use factory methods).

        Args:
            client: The connected Temporal client.
            _server_process: Optional subprocess for local server (internal).
        """
        self._client = client
        self._server_process = _server_process

    @property
    def client(self) -> "Client":
        """Get the Temporal client for this environment.

        Returns:
            The connected Temporal client.
        """
        return self._client

    @classmethod
    def from_client(cls, client: "Client") -> "WorkflowEnvironment":
        """Create a workflow environment from an existing client.

        Use this when you want to test against an already running Temporal
        server. The environment will not manage the server lifecycle.

        Args:
            client: An already connected Temporal client.

        Returns:
            A workflow environment wrapping the client.

        Example:
            client = await Client.connect("localhost:7233")
            env = WorkflowEnvironment.from_client(client)
        """
        return cls(client)

    @classmethod
    async def start_local(
        cls,
        *,
        namespace: str = "default",
        data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
        ip: str = "127.0.0.1",
        port: Optional[int] = None,
        ui: bool = False,
        search_attributes: Sequence[temporalio.common.SearchAttributeKey] = (),
        dev_server_existing_path: Optional[str] = None,
        dev_server_database_filename: Optional[str] = None,
        dev_server_log_level: Optional[str] = "warn",
        dev_server_extra_args: Sequence[str] = [],
        **kwargs: object,
    ) -> "WorkflowEnvironment":
        """Start a local Temporal dev server for testing.

        This starts the Temporal CLI dev server and connects a client to it.
        The server runs in a subprocess and is automatically shut down when
        the environment is closed.

        Args:
            namespace: Namespace to use (default: "default").
            data_converter: Data converter for payload serialization. Passed
                to the client connection.
            ip: IP address to bind to (default: "127.0.0.1").
            port: Port to use. If not specified, a free port is selected.
            ui: If True, start the Temporal UI with the dev server.
            search_attributes: Search attributes to register with the dev
                server.
            dev_server_existing_path: Path to temporal CLI binary. If not
                specified, searches PATH for "temporal".
            dev_server_database_filename: Path to the Sqlite database to use
                for the dev server. Unset default means only in-memory Sqlite
                will be used.
            dev_server_log_level: Log level to use for the dev server. Default
                is ``warn``, but if set to ``None`` this will translate the
                Python logger's level to a dev server log level.
            dev_server_extra_args: Extra arguments for the CLI binary.

        Returns:
            A workflow environment with a running local server.

        Raises:
            RuntimeError: If the Temporal CLI is not found or fails to start.

        Example:
            async with await WorkflowEnvironment.start_local() as env:
                # env.client is connected to the local dev server
                ...

        .. note::
            The following legacy parameter names are still accepted for
            backward compatibility but are deprecated:

            - ``temporal_cli_path`` -- use ``dev_server_existing_path``
            - ``log_level`` -- use ``dev_server_log_level``
            - ``extra_args`` -- use ``dev_server_extra_args``
        """
        # Handle backward-compatible renamed parameters
        if "temporal_cli_path" in kwargs:
            warnings.warn(
                "temporal_cli_path is deprecated, use dev_server_existing_path",
                DeprecationWarning,
                stacklevel=2,
            )
            if dev_server_existing_path is None:
                dev_server_existing_path = kwargs.pop("temporal_cli_path")  # type: ignore[assignment]
            else:
                kwargs.pop("temporal_cli_path")

        if "log_level" in kwargs:
            warnings.warn(
                "log_level is deprecated, use dev_server_log_level",
                DeprecationWarning,
                stacklevel=2,
            )
            if dev_server_log_level == "warn":
                dev_server_log_level = kwargs.pop("log_level")  # type: ignore[assignment]
            else:
                kwargs.pop("log_level")

        if "extra_args" in kwargs:
            warnings.warn(
                "extra_args is deprecated, use dev_server_extra_args",
                DeprecationWarning,
                stacklevel=2,
            )
            if not dev_server_extra_args:
                dev_server_extra_args = kwargs.pop("extra_args")  # type: ignore[assignment]
            else:
                kwargs.pop("extra_args")

        if kwargs:
            raise TypeError(
                f"start_local() got unexpected keyword arguments: "
                f"{', '.join(kwargs.keys())}"
            )

        # Use the logger's configured level if none given
        if not dev_server_log_level:
            if logger.isEnabledFor(logging.DEBUG):
                dev_server_log_level = "debug"
            elif logger.isEnabledFor(logging.INFO):
                dev_server_log_level = "info"
            elif logger.isEnabledFor(logging.WARNING):
                dev_server_log_level = "warn"
            elif logger.isEnabledFor(logging.ERROR):
                dev_server_log_level = "error"
            else:
                dev_server_log_level = "fatal"

        # Find temporal CLI
        if dev_server_existing_path:
            cli_path = dev_server_existing_path
        else:
            cli_path = shutil.which("temporal")
            if cli_path is None:
                raise RuntimeError(
                    "Temporal CLI not found in PATH. "
                    "Install it from https://docs.temporal.io/cli or "
                    "specify dev_server_existing_path parameter."
                )

        # Select port
        if port is None:
            port = _find_free_port()

        # Build extra args from search attributes
        extra_args_list: list[str] = []
        if search_attributes:
            for attr in search_attributes:
                extra_args_list.append("--search-attribute")
                extra_args_list.append(f"{attr.name}={attr._metadata_type}")
        extra_args_list.extend(dev_server_extra_args)

        # Build command
        cmd = [
            cli_path,
            "server",
            "start-dev",
            "--namespace",
            namespace,
            "--ip",
            ip,
            "--port",
            str(port),
            "--log-level",
            dev_server_log_level,
        ]
        if not ui:
            cmd.append("--headless")
        if dev_server_database_filename:
            cmd.extend(["--db-filename", dev_server_database_filename])
        if extra_args_list:
            cmd.extend(extra_args_list)

        logger.debug(f"Starting Temporal dev server: {' '.join(cmd)}")

        # Start server process
        # Use DEVNULL for stdin/stdout to avoid blocking
        server_process = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )

        # Wait for server to be ready
        target_url = f"{ip}:{port}"
        max_attempts = 50  # 5 seconds total
        connected = False

        for attempt in range(max_attempts):
            # Check if process died
            if server_process.poll() is not None:
                stderr_output = (
                    server_process.stderr.read() if server_process.stderr else ""
                )
                raise RuntimeError(
                    f"Temporal dev server exited unexpectedly. "
                    f"Exit code: {server_process.returncode}. "
                    f"Stderr: {stderr_output}"
                )

            # Try to connect
            try:
                # Import here to avoid circular imports
                from temporalio_trio.client import Client

                client = await Client.connect(
                    target_url,
                    namespace=namespace,
                    data_converter=data_converter
                    if data_converter is not temporalio.converter.DataConverter.default
                    else None,
                )
                connected = True
                break
            except Exception:
                # Server not ready yet
                await trio.sleep(0.1)

        if not connected:
            # Kill the server if we couldn't connect
            server_process.terminate()
            try:
                server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                server_process.kill()
            raise RuntimeError(
                f"Could not connect to Temporal dev server at {target_url} "
                f"after {max_attempts} attempts"
            )

        logger.info(f"Connected to Temporal dev server at {target_url}")
        return cls(client, _server_process=server_process)

    async def shutdown(self) -> None:
        """Shut down the environment.

        If a local server was started, this stops it. This is called
        automatically when using the environment as an async context manager.
        """
        # Close the client
        if self._client:
            await self._client.close()

        # Stop the server if we started it
        if self._server_process is not None:
            logger.debug("Stopping Temporal dev server")
            self._server_process.terminate()
            try:
                self._server_process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                logger.warning("Dev server did not stop gracefully, killing")
                self._server_process.kill()
                self._server_process.wait()
            self._server_process = None
            logger.info("Temporal dev server stopped")

    async def __aenter__(self) -> "WorkflowEnvironment":
        """Enter async context manager.

        Returns:
            This environment instance.
        """
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit async context manager, shutting down the environment."""
        await self.shutdown()


@asynccontextmanager
async def workflow_environment(
    target_url: str = "localhost:7233",
    namespace: str = "default",
) -> AsyncIterator[WorkflowEnvironment]:
    """Async context manager for creating a workflow environment.

    This is a convenience function for creating a workflow environment
    that connects to an existing server.

    Args:
        target_url: URL of the Temporal server.
        namespace: Namespace to use.

    Yields:
        A workflow environment connected to the specified server.

    Example:
        async with workflow_environment("localhost:7233") as env:
            handle = await env.client.start_workflow(...)
    """
    from temporalio_trio.client import Client

    client = await Client.connect(target_url, namespace=namespace)
    env = WorkflowEnvironment.from_client(client)
    try:
        yield env
    finally:
        await env.shutdown()
