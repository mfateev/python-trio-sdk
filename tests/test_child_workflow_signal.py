"""Tests for child workflow signaling.

Tests the ChildWorkflowHandle.signal() functionality.
Signal routing goes through the outbound interceptor's
signal_child_workflow method (matching sdk-python pattern).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import temporalio.common

from temporalio_trio.workflow import ChildWorkflowHandle, _Runtime, _SignalDefinition


def _make_mock_runtime() -> MagicMock:
    """Create a mock runtime with outbound interceptor for signal tests."""
    mock_runtime = MagicMock(spec=_Runtime)
    # The _workflow_runtime() returns an object with _assert_not_read_only
    mock_wf_runtime = MagicMock()
    mock_runtime._workflow_runtime.return_value = mock_wf_runtime
    # The outbound_interceptor has signal_child_workflow
    mock_runtime.outbound_interceptor = MagicMock()
    mock_runtime.outbound_interceptor.signal_child_workflow = AsyncMock()
    return mock_runtime


def _set_runtime(mock_runtime: MagicMock):
    """Set mock runtime as current and return reset token."""
    from temporalio_trio import workflow

    return workflow._current_runtime.set(mock_runtime)


class TestChildWorkflowHandleSignal:
    """Tests for ChildWorkflowHandle.signal() method."""

    @pytest.mark.trio
    async def test_signal_with_string_name(self) -> None:
        """Test signaling child workflow with string signal name."""
        mock_runtime = _make_mock_runtime()
        handle = ChildWorkflowHandle[None, str](
            seq=1,
            id="test-child-workflow-id",
            first_execution_run_id="test-run-id-123",
        )

        from temporalio_trio import workflow

        token = _set_runtime(mock_runtime)
        try:
            await handle.signal("my_signal", "test_value")

            # Verify outbound interceptor was called
            mock_runtime.outbound_interceptor.signal_child_workflow.assert_called_once()
            call_args = (
                mock_runtime.outbound_interceptor.signal_child_workflow.call_args
            )
            signal_input = call_args[0][0]
            assert signal_input.signal == "my_signal"
            assert signal_input.args == ["test_value"]
            assert signal_input.child_workflow_id == "test-child-workflow-id"
        finally:
            workflow._current_runtime.reset(token)

    @pytest.mark.trio
    async def test_signal_with_callable(self) -> None:
        """Test signaling child workflow with signal method reference."""

        def my_signal_method(value: str) -> None:
            pass

        signal_def = _SignalDefinition(
            name="my_custom_signal", fn=my_signal_method, is_method=False
        )

        mock_runtime = _make_mock_runtime()
        handle = ChildWorkflowHandle[None, str](
            seq=2,
            id="test-child-id-2",
            first_execution_run_id="run-id-456",
        )

        from temporalio_trio import workflow

        token = _set_runtime(mock_runtime)
        try:
            with patch.object(_SignalDefinition, "from_fn", return_value=signal_def):
                await handle.signal(my_signal_method, args=["arg1", "arg2"])

                call_args = (
                    mock_runtime.outbound_interceptor.signal_child_workflow.call_args
                )
                signal_input = call_args[0][0]
                assert signal_input.signal == "my_custom_signal"
                assert signal_input.args == ["arg1", "arg2"]
        finally:
            workflow._current_runtime.reset(token)

    @pytest.mark.trio
    async def test_signal_with_multiple_args(self) -> None:
        """Test signaling with multiple arguments."""
        mock_runtime = _make_mock_runtime()
        handle = ChildWorkflowHandle[None, int](
            seq=3,
            id="child-id-3",
            first_execution_run_id="run-id-789",
        )

        from temporalio_trio import workflow

        token = _set_runtime(mock_runtime)
        try:
            await handle.signal("increment", args=[10, 20, 30])

            call_args = (
                mock_runtime.outbound_interceptor.signal_child_workflow.call_args
            )
            signal_input = call_args[0][0]
            assert signal_input.signal == "increment"
            assert signal_input.args == [10, 20, 30]
            assert signal_input.child_workflow_id == "child-id-3"
        finally:
            workflow._current_runtime.reset(token)

    @pytest.mark.trio
    async def test_signal_child_workflow_id_passed(self) -> None:
        """Test that the child_workflow_id is passed correctly."""
        mock_runtime = _make_mock_runtime()
        handle = ChildWorkflowHandle[None, str](
            seq=4,
            id="child-id-4",
            first_execution_run_id="run-id-abc",
        )

        from temporalio_trio import workflow

        token = _set_runtime(mock_runtime)
        try:
            await handle.signal("test_signal")

            call_args = (
                mock_runtime.outbound_interceptor.signal_child_workflow.call_args
            )
            signal_input = call_args[0][0]
            assert signal_input.child_workflow_id == "child-id-4"
        finally:
            workflow._current_runtime.reset(token)
