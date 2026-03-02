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

from temporalio_trio.client import Client, TLSConfig, WorkflowFailureError, WorkflowHandle


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
        api_key=None,
        tls_config=None,
        rpc_metadata=None,
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
        args=["arg1", "arg2"],
        id="wf-123",
        task_queue="my-queue",
    )

    assert isinstance(handle, WorkflowHandle)
    assert handle.workflow_id == "wf-123"
    # run_id is intentionally None on handles from start_workflow (tracks latest run)
    assert handle.run_id is None

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

    # Note: Uses server-side long poll (timeout=None lets server handle it)
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
        first_execution_run_id=None,
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
        first_execution_run_id=None,
        details_payloads_bytes=None,
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

    with pytest.raises(WorkflowFailureError):
        await handle.result()


@pytest.mark.trio
async def test_terminate_default_reason(mock_bridge):
    """Test terminate with no reason defaults to empty string."""
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle("wf-123")

    await handle.terminate()

    mock_bridge.terminate_workflow_execution.assert_called_once_with(
        workflow_id="wf-123",
        run_id=None,
        reason="",
        first_execution_run_id=None,
        details_payloads_bytes=None,
        timeout=None,
    )


@pytest.mark.trio
async def test_terminate_with_args(mock_bridge):
    """Test terminate with detail args encodes payloads."""
    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle("wf-123")

    await handle.terminate("detail1", reason="test reason")

    call_kwargs = mock_bridge.terminate_workflow_execution.call_args.kwargs
    assert call_kwargs["reason"] == "test reason"
    assert call_kwargs["workflow_id"] == "wf-123"
    # details_payloads_bytes should be non-None bytes
    assert call_kwargs["details_payloads_bytes"] is not None
    assert isinstance(call_kwargs["details_payloads_bytes"], bytes)
    assert len(call_kwargs["details_payloads_bytes"]) > 0


@pytest.mark.trio
async def test_result_follows_completed_run(mock_bridge):
    """Test result() follows new_execution_run_id on COMPLETED event."""
    import json

    from temporalio.api.common.v1 import Payload

    # First response: COMPLETED with new_execution_run_id (follows to new run)
    resp1 = GetWorkflowExecutionHistoryResponse()
    ev1 = resp1.history.events.add()
    ev1.event_type = EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    ev1.workflow_execution_completed_event_attributes.new_execution_run_id = (
        "follow-run-1"
    )

    # Second response: COMPLETED with actual result
    resp2 = GetWorkflowExecutionHistoryResponse()
    ev2 = resp2.history.events.add()
    ev2.event_type = EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    payload = Payload()
    payload.metadata["encoding"] = b"json/plain"
    payload.data = json.dumps("final-result").encode("utf-8")
    ev2.workflow_execution_completed_event_attributes.result.payloads.append(payload)

    mock_bridge.get_workflow_result.side_effect = [
        resp1.SerializeToString(),
        resp2.SerializeToString(),
    ]

    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle("wf-123")
    result = await handle.result()
    assert result == "final-result"

    # Should have been called twice (once for original, once for follow)
    assert mock_bridge.get_workflow_result.call_count == 2
    second_call = mock_bridge.get_workflow_result.call_args_list[1]
    assert second_call.kwargs["run_id"] == "follow-run-1"


@pytest.mark.trio
async def test_result_follows_failed_run(mock_bridge):
    """Test result() follows new_execution_run_id on FAILED event."""
    import json

    from temporalio.api.common.v1 import Payload

    # First response: FAILED with new_execution_run_id
    resp1 = GetWorkflowExecutionHistoryResponse()
    ev1 = resp1.history.events.add()
    ev1.event_type = EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED
    ev1.workflow_execution_failed_event_attributes.new_execution_run_id = "follow-run-2"

    # Second response: COMPLETED with result
    resp2 = GetWorkflowExecutionHistoryResponse()
    ev2 = resp2.history.events.add()
    ev2.event_type = EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    payload = Payload()
    payload.metadata["encoding"] = b"json/plain"
    payload.data = json.dumps("recovered").encode("utf-8")
    ev2.workflow_execution_completed_event_attributes.result.payloads.append(payload)

    mock_bridge.get_workflow_result.side_effect = [
        resp1.SerializeToString(),
        resp2.SerializeToString(),
    ]

    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle("wf-123")
    result = await handle.result()
    assert result == "recovered"

    assert mock_bridge.get_workflow_result.call_count == 2
    second_call = mock_bridge.get_workflow_result.call_args_list[1]
    assert second_call.kwargs["run_id"] == "follow-run-2"


@pytest.mark.trio
async def test_result_follows_timed_out_run(mock_bridge):
    """Test result() follows new_execution_run_id on TIMED_OUT event."""
    import json

    from temporalio.api.common.v1 import Payload

    # First response: TIMED_OUT with new_execution_run_id
    resp1 = GetWorkflowExecutionHistoryResponse()
    ev1 = resp1.history.events.add()
    ev1.event_type = EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT
    ev1.workflow_execution_timed_out_event_attributes.new_execution_run_id = (
        "follow-run-3"
    )

    # Second response: COMPLETED with result
    resp2 = GetWorkflowExecutionHistoryResponse()
    ev2 = resp2.history.events.add()
    ev2.event_type = EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED
    payload = Payload()
    payload.metadata["encoding"] = b"json/plain"
    payload.data = json.dumps("after-timeout").encode("utf-8")
    ev2.workflow_execution_completed_event_attributes.result.payloads.append(payload)

    mock_bridge.get_workflow_result.side_effect = [
        resp1.SerializeToString(),
        resp2.SerializeToString(),
    ]

    client = await Client.connect("localhost:7233")
    handle = client.get_workflow_handle("wf-123")
    result = await handle.result()
    assert result == "after-timeout"

    assert mock_bridge.get_workflow_result.call_count == 2
    second_call = mock_bridge.get_workflow_result.call_args_list[1]
    assert second_call.kwargs["run_id"] == "follow-run-3"


@pytest.mark.trio
async def test_start_workflow_sets_first_execution_run_id(mock_bridge):
    """Test that start_workflow sets first_execution_run_id on the handle."""
    response = StartWorkflowExecutionResponse(run_id="run-abc")
    mock_bridge.start_workflow_execution.return_value = response.SerializeToString()

    client = await Client.connect("localhost:7233")
    handle = await client.start_workflow("MyWorkflow", id="wf-123", task_queue="q")

    assert handle._first_execution_run_id == "run-abc"


@pytest.mark.trio
async def test_cancel_passes_first_execution_run_id(mock_bridge):
    """Test that cancel passes first_execution_run_id to the bridge."""
    response = StartWorkflowExecutionResponse(run_id="run-xyz")
    mock_bridge.start_workflow_execution.return_value = response.SerializeToString()

    client = await Client.connect("localhost:7233")
    handle = await client.start_workflow("MyWorkflow", id="wf-123", task_queue="q")

    await handle.cancel()

    mock_bridge.cancel_workflow_execution.assert_called_once_with(
        workflow_id="wf-123",
        run_id=None,
        first_execution_run_id="run-xyz",
        timeout=None,
    )


@pytest.mark.trio
async def test_tls_enabled_by_default_when_api_key_provided(mock_bridge):
    """Test that TLS is enabled by default when API key is provided and tls is not configured."""
    client = await Client.connect(
        "localhost:7233",
        api_key="test-api-key",
    )
    # TLS should be auto-enabled when api_key is provided and tls not explicitly set
    mock_bridge.initialize_client.assert_called_once()
    call_kwargs = mock_bridge.initialize_client.call_args.kwargs
    assert call_kwargs["target_url"] == "https://localhost:7233"
    assert call_kwargs["tls_config"] is not None
    assert call_kwargs["api_key"] == "test-api-key"


@pytest.mark.trio
async def test_tls_can_be_explicitly_disabled_with_api_key(mock_bridge):
    """Test that TLS can be explicitly disabled even when API key is provided."""
    client = await Client.connect(
        "localhost:7233",
        api_key="test-api-key",
        tls=False,
    )
    # TLS should remain disabled when explicitly set to False
    call_kwargs = mock_bridge.initialize_client.call_args.kwargs
    assert call_kwargs["target_url"] == "http://localhost:7233"
    assert call_kwargs["tls_config"] is None
    assert call_kwargs["api_key"] == "test-api-key"


@pytest.mark.trio
async def test_tls_disabled_by_default_when_no_api_key(mock_bridge):
    """Test that TLS is disabled by default when no API key is provided."""
    client = await Client.connect("localhost:7233")
    call_kwargs = mock_bridge.initialize_client.call_args.kwargs
    assert call_kwargs["target_url"] == "http://localhost:7233"
    assert call_kwargs["tls_config"] is None


@pytest.mark.trio
async def test_tls_explicit_config_preserved(mock_bridge):
    """Test that explicit TLS configuration is preserved regardless of API key."""
    import base64

    tls_config = TLSConfig(
        server_root_ca_cert=b"test-cert",
        domain="test-domain",
    )
    client = await Client.connect(
        "localhost:7233",
        api_key="test-api-key",
        tls=tls_config,
    )
    call_kwargs = mock_bridge.initialize_client.call_args.kwargs
    assert call_kwargs["target_url"] == "https://localhost:7233"
    assert call_kwargs["tls_config"] is not None
    assert call_kwargs["tls_config"]["server_root_ca_cert"] == base64.b64encode(
        b"test-cert"
    ).decode("ascii")
    assert call_kwargs["tls_config"]["domain"] == "test-domain"
    assert call_kwargs["api_key"] == "test-api-key"


@pytest.mark.trio
async def test_rpc_metadata_propagated(mock_bridge):
    """Test that rpc_metadata is forwarded to the bridge."""
    client = await Client.connect(
        "localhost:7233",
        rpc_metadata={"x-custom-header": "value1"},
    )
    call_kwargs = mock_bridge.initialize_client.call_args.kwargs
    assert call_kwargs["rpc_metadata"] == {"x-custom-header": "value1"}


@pytest.mark.trio
async def test_api_key_property(mock_bridge):
    """Test that Client.api_key returns the configured value."""
    client = await Client.connect(
        "localhost:7233",
        api_key="my-secret-key",
    )
    assert client.api_key == "my-secret-key"

    # Without api_key
    client2 = await Client.connect("localhost:7233")
    assert client2.api_key is None
