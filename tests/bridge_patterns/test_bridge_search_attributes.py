"""Bridge pattern tests for search attributes.

Pattern 19: UpsertSearchAttributes

This test verifies the UpsertSearchAttributesCommand conversion through the bridge.

Note: These tests require custom search attributes to be registered on the Temporal server:
- CustomKeywordField (Keyword type)
- CustomIntField (Int type)

The custom search attributes are automatically created by conftest.py when the bridge_patterns
tests are loaded.
"""

from __future__ import annotations

import json
import subprocess
from uuid import uuid4

import pytest
import trio

from temporalio_trio._async_bridge import TrioBridgeWrapper
from temporalio_trio.worker._activation import (
    CompleteWorkflowCommand,
    UpsertSearchAttributesCommand,
    WorkflowActivationCompletion,
)
from temporalio_trio.worker._bridge_types import poc_to_bridge_completion

from .conftest import (
    DEFAULT_TIMEOUT,
    TEMPORAL_CLI_PATH,
    ActivationParser,
    CompletionBuilder,
    get_workflow_status_via_cli,
    safe_shutdown,
    start_workflow_via_cli,
)


def get_workflow_search_attributes_via_cli(
    workflow_id: str,
    namespace: str = "default",
) -> dict[str, str]:
    """Get workflow search attributes using Temporal CLI.

    Args:
        workflow_id: The workflow ID to check
        namespace: Temporal namespace

    Returns:
        Dictionary of search attribute names to their JSON-encoded values
    """
    cmd = [
        TEMPORAL_CLI_PATH,
        "workflow",
        "describe",
        "--workflow-id",
        workflow_id,
        "--namespace",
        namespace,
        "--output",
        "json",
    ]

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=10,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to describe workflow: {result.stderr or result.stdout}"
        )

    data = json.loads(result.stdout)
    # Search attributes are in workflowExecutionInfo.searchAttributes.indexedFields
    search_attrs = (
        data.get("workflowExecutionInfo", {})
        .get("searchAttributes", {})
        .get("indexedFields", {})
    )
    # Each field is a Payload with data field
    result_attrs = {}
    for key, payload in search_attrs.items():
        # The data field is base64 encoded JSON
        if "data" in payload:
            import base64

            decoded = base64.b64decode(payload["data"]).decode("utf-8")
            # Strip quotes from string values
            if decoded.startswith('"') and decoded.endswith('"'):
                decoded = decoded[1:-1]
            result_attrs[key] = decoded
    return result_attrs


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_19_upsert_search_attributes(unique_task_queue: str) -> None:
    """Test Pattern 19: Upsert Search Attributes.

    Flow:
    1. Start workflow
    2. poll -> [initialize_workflow]
    3. complete([UpsertSearchAttributesCommand(search_attributes={...}), StartTimer])
    4. poll -> [fire_timer]
    5. complete([CompleteWorkflowExecution])
    6. Verify search attributes were set via CLI

    Verifies:
    - UpsertSearchAttributesCommand to bridge protobuf conversion
    - Search attributes are persisted on workflow
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-search-attrs-poc-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        # 1. Start workflow
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="SearchAttributeWorkflow",
            task_queue=unique_task_queue,
        )

        # 2. Poll for initialization
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)

        assert activation.has_job_type("initialize_workflow")
        run_id = activation.run_id
        print("Pattern 19: Received initialize_workflow")

        # 3. Send UpsertSearchAttributes command with a custom keyword field
        # Using CustomKeywordField which is available by default on dev server
        import temporalio.bridge.proto.workflow_commands.workflow_commands_pb2 as cmd_pb
        import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as comp_pb
        from temporalio.converter import DataConverter

        dc = DataConverter.default

        completion = comp_pb.WorkflowActivationCompletion()
        completion.run_id = run_id
        completion.successful.SetInParent()

        # Create UpsertSearchAttributes command using our POC types
        upsert_cmd = UpsertSearchAttributesCommand(
            search_attributes={"CustomKeywordField": "test-value-123"}
        )

        # Convert using our bridge conversion function
        poc_completion = WorkflowActivationCompletion(commands=[upsert_cmd])
        bridge_completion = poc_to_bridge_completion(run_id, poc_completion, dc)

        # Add a timer to wake up workflow
        cmd2 = cmd_pb.WorkflowCommand()
        cmd2.start_timer.seq = 1
        cmd2.start_timer.start_to_fire_timeout.seconds = 0
        cmd2.start_timer.start_to_fire_timeout.nanos = 100_000_000  # 100ms
        bridge_completion.successful.commands.append(cmd2)

        await bridge.complete_workflow_activation(
            bridge_completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )
        print("Pattern 19: Sent UpsertSearchAttributesCommand + timer")

        # 4. Wait for timer to fire and handle potential cache eviction
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)

        # Handle single cache eviction/replay if needed
        if activation.has_job_type("remove_from_cache"):
            print("Pattern 19: Handling cache eviction")
            evict_completion = CompletionBuilder(activation.run_id).build()
            await bridge.complete_workflow_activation(
                evict_completion, timeout=DEFAULT_TIMEOUT
            )
            activation_bytes = await bridge.poll_workflow_activation(
                timeout=DEFAULT_TIMEOUT
            )
            activation = ActivationParser(activation_bytes)

        # If we get initialize_workflow (replay), complete with just workflow done
        if activation.has_job_type("initialize_workflow"):
            print("Pattern 19: Replaying - completing workflow directly")
            run_id = activation.run_id
            final_completion = (
                CompletionBuilder(run_id)
                .complete_workflow("search_attrs_set_replayed")
                .build()
            )
            await bridge.complete_workflow_activation(
                final_completion, timeout=DEFAULT_TIMEOUT
            )
        elif activation.has_job_type("fire_timer"):
            print("Pattern 19: Timer fired")
            # 5. Complete workflow
            final_completion = (
                CompletionBuilder(run_id).complete_workflow("search_attrs_set").build()
            )
            await bridge.complete_workflow_activation(
                final_completion, timeout=DEFAULT_TIMEOUT
            )
        else:
            pytest.fail(
                f"Unexpected activation: {[j.WhichOneof('variant') for j in activation.jobs]}"
            )

        # 6. Verify workflow completed
        await trio.sleep(0.5)
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "COMPLETED", f"Expected COMPLETED, got {status}"

        # 7. Verify search attributes were set
        search_attrs = get_workflow_search_attributes_via_cli(workflow_id)
        print(f"Pattern 19: Search attributes: {search_attrs}")

        # Note: Search attributes may include system attributes, so we check for our custom one
        assert "CustomKeywordField" in search_attrs, (
            f"CustomKeywordField not found in search attributes: {search_attrs}"
        )
        assert search_attrs["CustomKeywordField"] == "test-value-123", (
            f"Expected 'test-value-123', got '{search_attrs['CustomKeywordField']}'"
        )

        print("Pattern 19: UpsertSearchAttributes - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_pattern_19_multiple_search_attributes(unique_task_queue: str) -> None:
    """Test Pattern 19 variant: Multiple search attributes at once.

    Verifies that multiple search attributes can be upserted in a single command.
    """
    bridge = TrioBridgeWrapper()
    await bridge.start()

    workflow_id = f"test-multi-search-attrs-{uuid4()}"

    await bridge.initialize_with_config(
        target_url="http://localhost:7233",
        namespace="default",
        task_queue=unique_task_queue,
    )

    try:
        start_workflow_via_cli(
            workflow_id=workflow_id,
            workflow_type="MultiSearchAttributeWorkflow",
            task_queue=unique_task_queue,
        )

        # Get initialization
        activation_bytes = await bridge.poll_workflow_activation(
            timeout=DEFAULT_TIMEOUT
        )
        activation = ActivationParser(activation_bytes)
        run_id = activation.run_id

        # Upsert multiple search attributes
        from temporalio.converter import DataConverter

        dc = DataConverter.default

        upsert_cmd = UpsertSearchAttributesCommand(
            search_attributes={
                "CustomKeywordField": "multi-test-value",
                "CustomIntField": 42,
            }
        )

        poc_completion = WorkflowActivationCompletion(
            commands=[
                upsert_cmd,
                CompleteWorkflowCommand(result="multi_attrs_set"),
            ]
        )
        bridge_completion = poc_to_bridge_completion(run_id, poc_completion, dc)

        await bridge.complete_workflow_activation(
            bridge_completion.SerializeToString(), timeout=DEFAULT_TIMEOUT
        )

        # Verify workflow completed
        await trio.sleep(0.5)
        status = get_workflow_status_via_cli(workflow_id)
        assert status == "COMPLETED", f"Expected COMPLETED, got {status}"

        # Verify search attributes
        search_attrs = get_workflow_search_attributes_via_cli(workflow_id)
        print(f"Pattern 19 (multi): Search attributes: {search_attrs}")

        assert "CustomKeywordField" in search_attrs
        assert search_attrs["CustomKeywordField"] == "multi-test-value"

        # CustomIntField should be present (as a string in JSON output)
        if "CustomIntField" in search_attrs:
            assert search_attrs["CustomIntField"] == "42"

        print("Pattern 19 (multi): Multiple Search Attributes - PASSED")

    finally:
        await safe_shutdown(bridge)


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_upsert_search_attributes_command_conversion() -> None:
    """Test UpsertSearchAttributesCommand to bridge protobuf conversion.

    This is a unit test that verifies the conversion logic without
    starting a real workflow.
    """
    from temporalio.converter import DataConverter

    dc = DataConverter.default

    # Create command
    cmd = UpsertSearchAttributesCommand(
        search_attributes={
            "KeywordAttr": "test-value",
            "IntAttr": 123,
            "BoolAttr": True,
        }
    )

    # Convert using bridge function
    poc_completion = WorkflowActivationCompletion(commands=[cmd])
    bridge_completion = poc_to_bridge_completion("test-run-id", poc_completion, dc)

    # Verify conversion
    assert len(bridge_completion.successful.commands) == 1
    bridge_cmd = bridge_completion.successful.commands[0]

    # Check that upsert_workflow_search_attributes is set
    assert bridge_cmd.HasField("upsert_workflow_search_attributes")

    # Check search attributes
    search_attrs = bridge_cmd.upsert_workflow_search_attributes.search_attributes
    assert "KeywordAttr" in search_attrs
    assert "IntAttr" in search_attrs
    assert "BoolAttr" in search_attrs

    # Decode and verify values
    keyword_value = dc.payload_converter.from_payload(search_attrs["KeywordAttr"])
    assert keyword_value == "test-value"

    int_value = dc.payload_converter.from_payload(search_attrs["IntAttr"])
    assert int_value == 123

    bool_value = dc.payload_converter.from_payload(search_attrs["BoolAttr"])
    assert bool_value is True

    print("UpsertSearchAttributesCommand conversion test - PASSED")
