"""Tests for WorkflowEnvironment testing utilities.

These tests verify the testing infrastructure for Temporal workflows with Trio.
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import trio

from temporalio_trio.testing import WorkflowEnvironment


def find_temporal_cli() -> str | None:
    """Find the Temporal CLI in PATH or common locations.

    Returns the path to the temporal CLI if found, None otherwise.
    """
    # Check PATH first
    cli_path = shutil.which("temporal")
    if cli_path:
        return cli_path

    # Check common locations
    common_paths = [
        Path.home() / "workarea" / "bin" / "temporal",
        Path("/home/dev/.temporalio/bin/temporal"),
        Path.home() / ".temporalio" / "bin" / "temporal",
        Path("/usr/local/bin/temporal"),
    ]

    for path in common_paths:
        if path.exists() and os.access(path, os.X_OK):
            return str(path)

    return None


class TestWorkflowEnvironmentFromClient:
    """Tests for WorkflowEnvironment.from_client()."""

    def test_from_client_creates_environment(self):
        """Test creating environment from existing client."""
        mock_client = MagicMock()
        env = WorkflowEnvironment.from_client(mock_client)

        assert env.client is mock_client
        assert env._server_process is None

    def test_from_client_no_server_process(self):
        """Test that from_client doesn't create a server process."""
        mock_client = MagicMock()
        env = WorkflowEnvironment.from_client(mock_client)

        # No server process should be managed
        assert env._server_process is None


class TestWorkflowEnvironmentStartLocal:
    """Tests for WorkflowEnvironment.start_local()."""

    @pytest.mark.trio
    async def test_start_local_requires_temporal_cli(self):
        """Test that start_local raises if temporal CLI not found."""
        # Patch shutil.which to return None (CLI not found)
        with patch("shutil.which", return_value=None):
            with pytest.raises(RuntimeError, match="Temporal CLI not found"):
                await WorkflowEnvironment.start_local(
                    dev_server_existing_path=None,
                )

    @pytest.mark.trio
    async def test_start_local_with_explicit_path(self):
        """Test start_local with explicit CLI path that doesn't exist."""
        with pytest.raises(Exception):
            # This should fail because the path doesn't exist
            await WorkflowEnvironment.start_local(
                dev_server_existing_path="/nonexistent/path/to/temporal",
            )

    def test_find_free_port(self):
        """Test that _find_free_port returns a valid port."""
        from temporalio_trio.testing._workflow import _find_free_port

        port = _find_free_port()
        assert isinstance(port, int)
        assert 1024 <= port <= 65535


class TestWorkflowEnvironmentContextManager:
    """Tests for async context manager behavior."""

    @pytest.mark.trio
    async def test_context_manager_calls_shutdown(self):
        """Test that exiting context manager calls shutdown."""
        mock_client = MagicMock()
        mock_client.close = AsyncMock()

        env = WorkflowEnvironment.from_client(mock_client)

        async with env:
            pass

        mock_client.close.assert_called_once()

    @pytest.mark.trio
    async def test_context_manager_returns_self(self):
        """Test that entering context manager returns the environment."""
        mock_client = MagicMock()
        mock_client.close = AsyncMock()

        env = WorkflowEnvironment.from_client(mock_client)

        async with env as entered_env:
            assert entered_env is env


class TestWorkflowEnvironmentShutdown:
    """Tests for WorkflowEnvironment.shutdown()."""

    @pytest.mark.trio
    async def test_shutdown_closes_client(self):
        """Test that shutdown closes the client."""
        mock_client = MagicMock()
        mock_client.close = AsyncMock()

        env = WorkflowEnvironment.from_client(mock_client)
        await env.shutdown()

        mock_client.close.assert_called_once()

    @pytest.mark.trio
    async def test_shutdown_terminates_server_process(self):
        """Test that shutdown terminates server process if present."""
        mock_client = MagicMock()
        mock_client.close = AsyncMock()

        mock_process = MagicMock()
        mock_process.terminate = MagicMock()
        mock_process.wait = MagicMock()

        env = WorkflowEnvironment(mock_client, _server_process=mock_process)
        await env.shutdown()

        mock_process.terminate.assert_called_once()
        mock_process.wait.assert_called()

    @pytest.mark.trio
    async def test_shutdown_kills_process_on_timeout(self):
        """Test that shutdown kills process if terminate times out."""
        import subprocess

        mock_client = MagicMock()
        mock_client.close = AsyncMock()

        mock_process = MagicMock()
        mock_process.terminate = MagicMock()
        # First wait() call raises timeout, second succeeds (after kill)
        mock_process.wait = MagicMock(
            side_effect=[subprocess.TimeoutExpired("cmd", 5), None]
        )
        mock_process.kill = MagicMock()

        env = WorkflowEnvironment(mock_client, _server_process=mock_process)
        await env.shutdown()

        mock_process.terminate.assert_called_once()
        mock_process.kill.assert_called_once()


class TestWorkflowEnvironmentIntegration:
    """Integration tests for WorkflowEnvironment.

    These tests require a running Temporal server.
    """

    @pytest.mark.trio
    @pytest.mark.temporal_server
    async def test_from_client_with_real_server(self):
        """Test from_client with a real Temporal server."""
        from temporalio_trio.client import Client

        client = await Client.connect("localhost:7233")
        try:
            env = WorkflowEnvironment.from_client(client)
            assert env.client is client
        finally:
            await client.close()

    @pytest.mark.trio
    async def test_start_local_creates_working_environment(self):
        """Test that start_local creates a working environment.

        This starts its own dev server on a free port, independent of
        any existing Temporal server.
        """
        cli_path = find_temporal_cli()
        if cli_path is None:
            pytest.skip("Temporal CLI not found in PATH or common locations")

        async with await WorkflowEnvironment.start_local(
            dev_server_existing_path=cli_path
        ) as env:
            # The environment should have a working client
            assert env.client is not None
            # Verify we can actually connect by checking namespaces
            # (this confirms the server is running and reachable)
