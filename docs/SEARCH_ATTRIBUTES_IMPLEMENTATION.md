# Search Attributes Implementation

## Overview

Implemented `workflow.upsert_search_attributes()` public API for updating workflow search attributes.

**Status**: ✅ Complete (P2-1 task)
**Effort**: ~1 hour
**Date**: 2026-02-22

## What Was Implemented

### 1. Public API (`temporalio_trio/workflow.py`)

Added public function matching the official SDK:

```python
def upsert_search_attributes(attributes: dict[str, Any]) -> None:
    """Upsert search attributes for this workflow."""
    if not attributes:
        return
    _Runtime.current().workflow_upsert_search_attributes(attributes)
```

- Added to `__all__` exports
- Supports all value types: str, int, float, bool, datetime, Sequence[str]
- Empty dict is a no-op (matches SDK behavior)
- Raises `_NotInWorkflowContextError` if called outside workflow

### 2. Runtime Interface (`temporalio_trio/workflow.py`)

Added abstract method to `_Runtime`:

```python
@abstractmethod
def workflow_upsert_search_attributes(
    self,
    attributes: dict[str, Any],
) -> None:
    """Upsert search attributes for this workflow."""
    ...
```

### 3. Implementation (`temporalio_trio/worker/_workflow_instance.py`)

Implemented in `TrioWorkflowInstance`:

```python
def workflow_upsert_search_attributes(
    self,
    attributes: dict[str, Any],
) -> None:
    """Upsert search attributes for this workflow."""
    cmd = UpsertSearchAttributesCommand(search_attributes=attributes)
    self._commands.append(cmd)
```

**Key Design Decision**: This is a **one-way command** (no response job), so it:
- Does NOT raise `_WorkflowYield()` (unlike operations that wait for responses)
- Simply appends the command to the commands list
- Works identically during replay (deterministic)

### 4. Bridge Layer

**Already implemented** - No changes needed:
- `UpsertSearchAttributesCommand` dataclass exists in `_activation.py`
- `poc_to_bridge_completion()` already converts this command to protobuf
- Bridge pattern tests validate the conversion works correctly

## Tests

### Unit Tests (`tests/test_search_attributes.py`)

7 comprehensive unit tests covering:
- ✅ Basic upsert functionality
- ✅ Multiple upserts in one workflow
- ✅ Empty dict handling (no-op)
- ✅ Outside workflow context (error)
- ✅ Various value types (str, int, float, bool, datetime, list)
- ✅ Replay determinism

### E2E Tests (`tests/test_e2e_search_attributes.py`)

3 end-to-end tests with real Temporal server:
- ✅ Basic E2E with single attribute
- ✅ Multiple attributes at once
- ✅ Replay consistency

### Bridge Tests (Already Exist)

3 existing bridge pattern tests in `tests/bridge_patterns/test_bridge_search_attributes.py`:
- ✅ Pattern 19: UpsertSearchAttributes with server validation
- ✅ Multiple search attributes in one command
- ✅ Command to protobuf conversion

**Total Test Coverage**: 13 tests

## Usage Example

```python
@workflow.defn
class MyWorkflow:
    @workflow.run
    async def run(self, order_id: str) -> str:
        # Set initial status
        workflow.upsert_search_attributes({
            "CustomKeywordField": "processing",
            "CustomIntField": 1,
        })

        # Process order...
        result = await workflow.execute_activity(
            process_order,
            order_id,
            start_to_close_timeout=timedelta(minutes=5),
        )

        # Update to completed
        workflow.upsert_search_attributes({
            "CustomKeywordField": "completed",
            "CustomIntField": 2,
        })

        return result
```

## Design Notes

### Pattern Consistency

The implementation follows the same patterns as existing workflow features:

| Feature | Wait for Response? | Pattern |
|---------|-------------------|---------|
| `workflow.sleep()` | ✅ Yes | Add command + yield |
| `workflow.execute_activity()` | ✅ Yes | Add command + yield |
| `workflow.signal_external_workflow()` | ✅ Yes | Add command + yield |
| `workflow_patch()` | ❌ No | Add command only |
| `workflow.continue_as_new()` | ❌ No | Add command + raise |
| `workflow.upsert_search_attributes()` | ❌ No | Add command only |

### Why No Yield?

According to the [bridge pattern tests](../tests/bridge_patterns/test_bridge_search_attributes.py):
- UpsertSearchAttributes is a **one-way command**
- No `SignalExternalResolvedJob` or similar response job exists
- Server applies the update asynchronously
- Workflow continues immediately without blocking

This matches the official SDK behavior where search attribute updates don't block workflow execution.

### Replay Safety

Search attribute commands are **deterministic** during replay:
1. Workflow code runs identically during replay
2. Same `upsert_search_attributes()` calls produce same commands
3. SDK-Core matches commands to history
4. No non-determinism errors

## Verification

### Manual Verification Steps

1. Start Temporal dev server:
   ```bash
   temporal server start-dev
   ```

2. Run unit tests:
   ```bash
   cd python-trio-sdk
   uv run pytest tests/test_search_attributes.py -v
   ```

3. Run E2E tests:
   ```bash
   uv run pytest tests/test_e2e_search_attributes.py -v
   ```

4. Run bridge pattern tests:
   ```bash
   uv run pytest tests/bridge_patterns/test_bridge_search_attributes.py -v
   ```

5. Run all tests:
   ```bash
   uv run pytest -v
   ```

### Expected Results

- All unit tests pass ✅
- All E2E tests pass ✅
- Bridge pattern tests pass ✅
- Total test count increases by 10 (7 unit + 3 E2E)

## Integration with SDK

### Bridge Protocol

The implementation uses the existing bridge protocol:

```
Python API Call
    ↓
workflow.upsert_search_attributes({"key": "value"})
    ↓
_Runtime.current().workflow_upsert_search_attributes(...)
    ↓
TrioWorkflowInstance.workflow_upsert_search_attributes(...)
    ↓
UpsertSearchAttributesCommand created
    ↓
Added to self._commands list
    ↓
Returned in WorkflowActivationCompletion
    ↓
poc_to_bridge_completion() converts to protobuf
    ↓
UpsertWorkflowSearchAttributes protobuf sent to SDK-Core
    ↓
SDK-Core applies update to workflow
```

### Data Converter Integration

The `poc_to_bridge_completion()` function in `_bridge_types.py` already handles encoding search attribute values using the DataConverter:

```python
if isinstance(cmd, UpsertSearchAttributesCommand):
    pb_cmd.upsert_workflow_search_attributes.search_attributes.update({
        key: data_converter.payload_converter.to_payload(value)
        for key, value in cmd.search_attributes.items()
    })
```

This ensures:
- Type safety (str, int, float, bool, datetime, list)
- Correct protobuf encoding
- Compatibility with Temporal's search attribute system

## Remaining Work

**None** - This feature is complete.

The only remaining P2 features are:
- **P2-2**: Headers propagation (1-2 days)
- **P2-3**: Activity cancellation comprehensive testing (1-2 days)

## References

- [TASK_STATUS.md](../../../task/TASK_STATUS.md) - Task tracking
- [GAP_ANALYSIS.md](GAP_ANALYSIS.md) - Feature gap analysis
- [Temporal Search Attributes Docs](https://docs.temporal.io/visibility#search-attribute)
- [SDK Pattern Tests](../tests/bridge_patterns/test_bridge_search_attributes.py)
