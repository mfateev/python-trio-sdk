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
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncIterator, Optional

import trio

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

    Mirrors temporalio.testing.WorkflowEnvironment from the SDK.

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
        ip: str = "127.0.0.1",
        port: Optional[int] = None,
        log_level: str = "warn",
        temporal_cli_path: Optional[str] = None,
        extra_args: list[str] | None = None,
    ) -> "WorkflowEnvironment":
        """Start a local Temporal dev server for testing.

        This starts the Temporal CLI dev server and connects a client to it.
        The server runs in a subprocess and is automatically shut down when
        the environment is closed.

        Args:
            namespace: Namespace to use (default: "default").
            ip: IP address to bind to (default: "127.0.0.1").
            port: Port to use. If not specified, a free port is selected.
            log_level: Log level for the dev server (default: "warn").
            temporal_cli_path: Path to temporal CLI binary. If not specified,
                searches PATH for "temporal".
            extra_args: Extra arguments to pass to the dev server.

        Returns:
            A workflow environment with a running local server.

        Raises:
            RuntimeError: If the Temporal CLI is not found or fails to start.

        Example:
            async with await WorkflowEnvironment.start_local() as env:
                # env.client is connected to the local dev server
                ...
        """
        # Find temporal CLI
        if temporal_cli_path:
            cli_path = temporal_cli_path
        else:
            cli_path = shutil.which("temporal")
            if cli_path is None:
                raise RuntimeError(
                    "Temporal CLI not found in PATH. "
                    "Install it from https://docs.temporal.io/cli or "
                    "specify temporal_cli_path parameter."
                )

        # Select port
        if port is None:
            port = _find_free_port()

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
            log_level,
            "--headless",
        ]
        if extra_args:
            cmd.extend(extra_args)

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

                client = await Client.connect(target_url, namespace=namespace)
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
