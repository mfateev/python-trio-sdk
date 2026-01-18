"""Tests for the Trio client implementation."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import trio
from temporalio.api.enums.v1 import EventType
from temporalio.api.workflowservice.v1 import (
    GetWorkflowExecutionHistoryResponse,
    StartWorkflowExecutionResponse,
)
from temporalio.converter import DataConverter

from temporalio_trio.client import Client, WorkflowHandle


@pytest.fixture
async def mock_bridge():
    """Create a mocked TrioBridgeWrapper."""
    with patch("temporalio_trio.client._client.TrioBridgeWrapper") as mock:
        bridge_instance = AsyncMock()
        mock.return_value = bridge_instance
        yield bridge_instance


@pytest.mark.trio
async def test_client_connect(mock_bridge):
    """Test client connection."""
    client = await Client.connect(
        "localhost:7233",
        namespace="test-namespace",
        identity="test-client",
    )

    assert client.namespace == "test-namespace"
    assert client.data_converter is not None

    # Verify bridge was initialized
    mock_bridge.start.assert_called_once()
    mock_bridge.initialize_client.assert_called_once_with(
        target_url="http://localhost:7233",
        namespace="test-namespace",
        identity="test-client",
    )


@pytest.mark.trio
async def test_client_close(mock_bridge):
    """Test client close."""
    client = await Client.connect("localhost:7233")
    await client.close()

    mock_bridge.shutdown.assert_called_once()


@pytest.mark.trio
async def test_start_workflow(mock_bridge):
    """Test starting a workflow."""
    # Mock the start_workflow_execution response
    response = StartWorkflowExecutionResponse(run_id="run-123")
    mock_bridge.start_workflow_execution.return_value = response.SerializeToString()

    client = await Client.connect("localhost:7233")
    handle = await client.start_workflow(
        "MyWorkflow",
        "arg1",
        "arg2",
        id="wf-123",
        task_queue="my-queue",
    )

    assert isinstance(handle, WorkflowHandle)
    assert handle.workflow_id == "wf-123"
    assert handle.run_id == "run-123"

    # Verify bridge was called
    mock_bridge.start_workflow_execution.assert_called_once()


@pytest.mark.trio
async def test_execute_workflow(mock_bridge):
    """Test executing a workflow (start + wait for result)."""
    # Mock start response
    start_response = StartWorkflowExecutionResponse(run_id="run-123")
    mock_bridge.start_workflow_execution.return_value = (
        start_response.SerializeToString()
    )

    # Mock result response with completed event
    history_response = GetWorkflowExecutionHistoryResponse()
    event = history_response.history.events.add()
    event.event_type = EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    completed_attrs = event.workflow_execution_completed_event_attributes

    # Add result payload
    import json

    from temporalio.api.common.v1 import Payload

    payload = Payload()
    payload.metadata["encoding"] = b"json/plain"
    payload.data = json.dumps("Hello, World!").encode("utf-8")
    completed_attrs.result.payloads.append(payload)

    mock_bridge.get_workflow_result.return_value = history_response.SerializeToString()

    client = await Client.connect("localhost:7233")
    result = await client.execute_workflow(
        "GreetingWorkflow",
        "World",
        id="wf-123",
        task_queue="my-queue",
    )

    assert result == "Hello, World!"


@pytest.mark.trio
async def test_get_workflow_handle(mock_bridge):
    """Test getting a handle to an existing workflow."""
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle("wf-123", run_id="run-456")

    assert isinstance(handle, WorkflowHandle)
    assert handle.workflow_id == "wf-123"
    assert handle.run_id == "run-456"


@pytest.mark.trio
async def test_workflow_handle_result(mock_bridge):
    """Test getting workflow result via handle."""
    # Mock result response
    history_response = GetWorkflowExecutionHistoryResponse()
    event = history_response.history.events.add()
    event.event_type = EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    completed_attrs = event.workflow_execution_completed_event_attributes

    import json

    from temporalio.api.common.v1 import Payload

    payload = Payload()
    payload.metadata["encoding"] = b"json/plain"
    payload.data = json.dumps(42).encode("utf-8")
    completed_attrs.result.payloads.append(payload)

    mock_bridge.get_workflow_result.return_value = history_response.SerializeToString()

    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle("wf-123")

    result = await handle.result()
    assert result == 42

    mock_bridge.get_workflow_result.assert_called_once_with(
        workflow_id="wf-123",
        run_id=None,
        timeout=None,
    )


@pytest.mark.trio
async def test_workflow_handle_cancel(mock_bridge):
    """Test canceling a workflow."""
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle("wf-123", run_id="run-456")

    await handle.cancel()

    mock_bridge.cancel_workflow_execution.assert_called_once_with(
        workflow_id="wf-123",
        run_id="run-456",
        timeout=None,
    )


@pytest.mark.trio
async def test_workflow_handle_terminate(mock_bridge):
    """Test terminating a workflow."""
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle("wf-123", run_id="run-456")

    await handle.terminate(reason="Test termination")

    mock_bridge.terminate_workflow_execution.assert_called_once_with(
        workflow_id="wf-123",
        run_id="run-456",
        reason="Test termination",
        timeout=None,
    )


@pytest.mark.trio
async def test_workflow_handle_signal(mock_bridge):
    """Test signaling a workflow."""
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle("wf-123")

    await handle.signal("update_value", 100)

    mock_bridge.signal_workflow.assert_called_once()
    # Verify workflow_id and signal_name in the call
    call_args = mock_bridge.signal_workflow.call_args
    assert call_args.kwargs["workflow_id"] == "wf-123"
    assert call_args.kwargs["signal_name"] == "update_value"


@pytest.mark.trio
async def test_workflow_failed(mock_bridge):
    """Test handling workflow failure."""
    # Mock failed workflow response
    history_response = GetWorkflowExecutionHistoryResponse()
    event = history_response.history.events.add()
    event.event_type = EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
    failed_attrs = event.workflow_execution_failed_event_attributes
    failed_attrs.failure.message = "Workflow failed!"

    mock_bridge.get_workflow_result.return_value = history_response.SerializeToString()

    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle("wf-123")

    with pytest.raises(RuntimeError, match="Workflow failed"):
        await handle.result()
