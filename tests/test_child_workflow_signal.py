"""Tests for child workflow signaling.

Tests the ChildWorkflowHandle.signal() functionality.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import temporalio.common

from temporalio_trio.workflow import ChildWorkflowHandle, _Runtime, _SignalDefinition


class TestChildWorkflowHandleSignal:
    """Tests for ChildWorkflowHandle.signal() method."""

    @pytest.mark.trio
    async def test_signal_with_string_name(self) -> None:
        """Test signaling child workflow with string signal name."""
        # Create mock runtime
        mock_runtime = MagicMock(spec=_Runtime)
        mock_runtime.workflow_signal_external_workflow = AsyncMock()

        # Create child workflow handle
        handle = ChildWorkflowHandle[None, str](
            seq=1,
            id="test-child-workflow-id",
            workflow_type="TestChildWorkflow",
            first_execution_run_id="test-run-id-123",
        )

        # Mock _Runtime.current() to return our mock
        import contextvars

        from temporalio_trio import workflow

        runtime_var = workflow._current_runtime
        token = runtime_var.set(mock_runtime)

        try:
            # Signal with string name
            await handle.signal("my_signal", "test_value")

            # Verify runtime method was called correctly
            mock_runtime.workflow_signal_external_workflow.assert_called_once()
            call_args = mock_runtime.workflow_signal_external_workflow.call_args

            assert call_args[0][0] == "test-child-workflow-id"  # workflow_id
            assert call_args[0][1] == "my_signal"  # signal_name
            assert call_args[0][2] == ["test_value"]  # args list
            assert call_args[1]["run_id"] == "test-run-id-123"  # run_id kwarg

        finally:
            runtime_var.reset(token)

    @pytest.mark.trio
    async def test_signal_with_callable(self) -> None:
        """Test signaling child workflow with signal method reference."""

        # Create mock signal method
        def my_signal_method(value: str) -> None:
            pass

        # Mock the signal definition
        signal_def = _SignalDefinition(
            name="my_custom_signal", fn=my_signal_method, is_method=False
        )

        # Create mock runtime
        mock_runtime = MagicMock(spec=_Runtime)
        mock_runtime.workflow_signal_external_workflow = AsyncMock()

        # Create child workflow handle
        handle = ChildWorkflowHandle[None, str](
            seq=2,
            id="test-child-id-2",
            workflow_type="TestWorkflow",
            first_execution_run_id="run-id-456",
        )

        # Mock runtime context
        import contextvars

        from temporalio_trio import workflow

        runtime_var = workflow._current_runtime
        token = runtime_var.set(mock_runtime)

        try:
            # Mock _SignalDefinition.from_fn to return our signal def
            with patch.object(_SignalDefinition, "from_fn", return_value=signal_def):
                # Signal with callable
                await handle.signal(my_signal_method, args=["arg1", "arg2"])

                # Verify runtime method was called
                mock_runtime.workflow_signal_external_workflow.assert_called_once()
                call_args = mock_runtime.workflow_signal_external_workflow.call_args

                assert call_args[0][0] == "test-child-id-2"
                assert call_args[0][1] == "my_custom_signal"  # Uses signal def name
                assert call_args[0][2] == ["arg1", "arg2"]  # args as list
                assert call_args[1]["run_id"] == "run-id-456"

        finally:
            runtime_var.reset(token)

    @pytest.mark.trio
    async def test_signal_with_multiple_args(self) -> None:
        """Test signaling with multiple arguments."""
        mock_runtime = MagicMock(spec=_Runtime)
        mock_runtime.workflow_signal_external_workflow = AsyncMock()

        handle = ChildWorkflowHandle[None, int](
            seq=3,
            id="child-id-3",
            workflow_type="CounterWorkflow",
            first_execution_run_id="run-id-789",
        )

        import contextvars

        from temporalio_trio import workflow

        runtime_var = workflow._current_runtime
        token = runtime_var.set(mock_runtime)

        try:
            # Signal with args parameter (multiple values)
            await handle.signal("increment", args=[10, 20, 30])

            mock_runtime.workflow_signal_external_workflow.assert_called_once()
            call_args = mock_runtime.workflow_signal_external_workflow.call_args

            assert call_args[0][0] == "child-id-3"
            assert call_args[0][1] == "increment"
            assert call_args[0][2] == [10, 20, 30]  # Multiple args as list

        finally:
            runtime_var.reset(token)

    @pytest.mark.trio
    async def test_signal_uses_first_execution_run_id(self) -> None:
        """Test that signal uses first_execution_run_id for targeting."""
        mock_runtime = MagicMock(spec=_Runtime)
        mock_runtime.workflow_signal_external_workflow = AsyncMock()

        # Create handle with specific run ID
        specific_run_id = "specific-run-id-abc"
        handle = ChildWorkflowHandle[None, str](
            seq=4,
            id="child-id-4",
            workflow_type="TestWorkflow",
            first_execution_run_id=specific_run_id,
        )

        import contextvars

        from temporalio_trio import workflow

        runtime_var = workflow._current_runtime
        token = runtime_var.set(mock_runtime)

        try:
            await handle.signal("test_signal")

            # Verify run_id is passed correctly
            call_args = mock_runtime.workflow_signal_external_workflow.call_args
            assert call_args[1]["run_id"] == specific_run_id

        finally:
            runtime_var.reset(token)

    @pytest.mark.trio
    async def test_signal_without_run_id(self) -> None:
        """Test signaling when child hasn't been started yet (no run_id)."""
        mock_runtime = MagicMock(spec=_Runtime)
        mock_runtime.workflow_signal_external_workflow = AsyncMock()

        # Create handle without run ID (child not started yet)
        handle = ChildWorkflowHandle[None, str](
            seq=5,
            id="child-id-5",
            workflow_type="TestWorkflow",
            first_execution_run_id=None,  # Not started yet
        )

        import contextvars

        from temporalio_trio import workflow

        runtime_var = workflow._current_runtime
        token = runtime_var.set(mock_runtime)

        try:
            await handle.signal("early_signal", "value")

            # Verify run_id is None (signals current run)
            call_args = mock_runtime.workflow_signal_external_workflow.call_args
            assert call_args[1]["run_id"] is None

        finally:
            runtime_var.reset(token)


class TestChildWorkflowSignalIntegration:
    """Integration test verifying signal() delegates to external workflow signaling."""

    @pytest.mark.trio
    async def test_child_signal_uses_same_mechanism_as_external(self) -> None:
        """Verify child workflow signal uses the same underlying mechanism as external workflow signal."""
        from temporalio_trio.workflow import ExternalWorkflowHandle

        mock_runtime = MagicMock(spec=_Runtime)
        mock_runtime.workflow_signal_external_workflow = AsyncMock()

        workflow_id = "shared-workflow-id"
        run_id = "shared-run-id"
        signal_name = "shared_signal"
        signal_args = ("arg1", "arg2")

        import contextvars

        from temporalio_trio import workflow

        runtime_var = workflow._current_runtime
        token = runtime_var.set(mock_runtime)

        try:
            # Signal via ChildWorkflowHandle
            child_handle = ChildWorkflowHandle[None, str](
                seq=10,
                id=workflow_id,
                workflow_type="TestWorkflow",
                first_execution_run_id=run_id,
            )
            await child_handle.signal(signal_name, args=signal_args)

            child_call_args = mock_runtime.workflow_signal_external_workflow.call_args

            # Reset mock
            mock_runtime.workflow_signal_external_workflow.reset_mock()

            # Signal via ExternalWorkflowHandle
            external_handle = ExternalWorkflowHandle[str](
                runtime=mock_runtime,
                workflow_id=workflow_id,
                run_id=run_id,
            )
            await external_handle.signal(signal_name, args=signal_args)

            external_call_args = (
                mock_runtime.workflow_signal_external_workflow.call_args
            )

            # Verify both use identical calls
            assert child_call_args == external_call_args

        finally:
            runtime_var.reset(token)
