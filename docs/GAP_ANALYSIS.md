# Gap Analysis: Bridge Tests vs SDK Implementation

**Date:** 2026-02-03 (Updated)
**Author:** Claude Code Analysis
**Scope:** Comparison of patterns learned from bridge tests vs current SDK worker implementation

---

## Executive Summary

The bridge pattern tests reveal the full scope of Temporal's workflow activation/completion protocol. This document tracks the gaps between bridge-level capabilities and the SDK's public API.

**Current Status:**
- **3 workflow commands** fully implemented at bridge and SDK level
- **2 commands** implemented at bridge level, awaiting public API
- **2 architectural gaps** remaining (error types, headers)

---

## 1. Workflow Commands Status

### 1.1 ContinueAsNewWorkflowExecution ✅ IMPLEMENTED

**Status:** Fully implemented with public API and tests.

**Implementation:**
- `ContinueAsNewCommand` in `_activation.py` with full field support
- `workflow.continue_as_new()` public API in `workflow.py`
- `ContinueAsNewError` exception class for control flow
- Bridge conversion in `poc_to_bridge_completion()`
- Runtime method `workflow_continue_as_new()` in `_runtime.py`

**Tests:**
- `tests/test_continue_as_new.py` - Unit tests for API and error handling
- `tests/test_e2e_continue_as_new.py` - E2E tests with real server
- `tests/bridge_patterns/test_bridge_continue_as_new.py` - Bridge protocol tests

**API:**
```python
def continue_as_new(
    *args: Any,
    workflow: str | type | None = None,
    task_queue: str | None = None,
    run_timeout: timedelta | None = None,
    task_timeout: timedelta | None = None,
    retry_policy: RetryPolicy | None = None,
) -> NoReturn:
    """Continue workflow as new execution."""
```

---

### 1.2 SignalExternalWorkflowExecution ⚠️ BRIDGE ONLY

**Status:** Bridge-level implementation complete. Needs public API.

**What's done:**
- `SignalExternalWorkflowCommand` in `_activation.py`
- `SignalExternalResolvedJob` for handling resolution
- Bridge conversion in `poc_to_bridge_completion()`
- Job handler in `bridge_to_poc_activation()`
- Bridge tests in `test_bridge_signal_external.py`

**What's needed:**
- Public API: `workflow.signal_external_workflow()`
- Or: `workflow.get_external_workflow_handle()` pattern

**Needed API:**
```python
async def signal_external_workflow(
    workflow_id: str,
    signal: str,
    *args: Any,
    run_id: str | None = None,
) -> None:
    """Signal another workflow."""
```

---

### 1.3 UpsertSearchAttributes ⚠️ BRIDGE ONLY

**Status:** Bridge-level implementation complete. Needs public API.

**What's done:**
- `UpsertSearchAttributesCommand` in `_activation.py`
- Bridge conversion in `poc_to_bridge_completion()`
- Bridge tests in `test_bridge_search_attributes.py`

**What's needed:**
- Public API: `workflow.upsert_search_attributes()`

**Needed API:**
```python
def upsert_search_attributes(
    attributes: dict[str, Any],
) -> None:
    """Update search attributes."""
```

---

## 2. Job Handlers Status

### 2.1 resolve_signal_external_workflow ✅ IMPLEMENTED

**Status:** Implemented at bridge level.

- `SignalExternalResolvedJob` defined in `_activation.py`
- Conversion in `bridge_to_poc_activation()` handles success/failure
- Needs integration with public API (when signal_external_workflow is added)

---

### 2.2 ActivityCancelledJob (Clarified)

**Status:** Not needed as separate job type.

**Finding:** Bridge tests show `resolve_activity` with `cancelled` status, not a separate `activity_cancelled` job. The SDK correctly handles activity cancellation through the existing `ActivityResolvedJob` with a cancelled result.

The `ActivityCancelledJob` type may be removed or repurposed if not needed.

---

## 3. Architectural Gaps

### 3.1 Cache Eviction Handling ✅ WORKING

**Status:** Implemented and tested.

**Implementation** (`_single_thread_worker.py`):
```python
if activation.is_eviction:
    await self._send_commands(run_id, [])
    continue  # polls again
```

**Tests:**
- `test_single_thread_worker.py` - Eviction and reactivation tests
- Bridge tests verify eviction behavior

---

### 3.2 Workflow Replay and Determinism ✅ CLARIFIED

**Key Finding:** SDK-Core handles command matching during replay, not the SDK.

**How it works:**
1. SDK-Core tracks history from server
2. SDK re-runs workflow code from start
3. SDK sends same commands (deterministic code)
4. SDK-Core matches commands to history events
5. Mismatches → Nondeterminism error → Eviction

**SDK responsibilities:**
- ✅ Create fresh instances on replay (done)
- ✅ Re-run workflow code from start (done)
- ✅ Send same commands (deterministic code)
- ✅ Handle `is_replaying` flag (done via `WorkflowInfo`)

**Assessment:** No gap - SDK-Core handles the complexity.

---

### 3.3 Query on Completed Workflow ✅ FIXED

**Status:** Fixed in commit `65ebf5c`.

**Implementation:**
- Added `sticky_queue_schedule_to_start_timeout_millis` to Rust bridge
- Worker passes sticky timeout to bridge initialization
- `QuerySuccessCommand`/`QueryFailureCommand` support in completion
- Eviction and query handling in `SingleThreadWorker`

**Tests:**
- `test_e2e_integration.py::test_e2e_query_triggers_replay`
- Bridge tests for query on completed workflow

---

### 3.4 Error Type Preservation ❌ NOT IMPLEMENTED

**Current behavior:**
```python
# Creates generic RuntimeError:
error = RuntimeError(
    f"Activity {resolved.activity_id} failed: {resolved.failure}"
)
```

**Gap:** Original exception type is lost. Applications cannot:
- Catch specific exception types (`ValueError` vs `TimeoutError`)
- Preserve exception chains
- Access non-retryable flag

**Needed:**
- Exception type reconstruction from `application_failure_info.type`
- Proper exception chaining with `__cause__`
- Non-retryable flag preservation

**Priority: P1** - Important for proper error handling in production.

---

### 3.5 Headers Not Propagated ❌ NOT IMPLEMENTED

**Current state:**
- Headers captured in job dataclasses (`signal_job.headers`, `query_job.headers`)
- Never passed to signal/query handlers
- No API to access headers in workflow code

**Gap:** Cannot implement:
- Distributed tracing (trace IDs in headers)
- Authentication/authorization context
- Custom metadata propagation

**Priority: P2** - Important for observability and security.

---

### 3.6 Parallel Workflow Handling ✅ WORKING

**Status:** Correctly implemented.

- Uses `run_id` as key in `workflow_states` dict
- Proper isolation between concurrent workflows
- Bridge tests verify parallel workflow behavior (`test_bridge_parallel.py`)

---

## 4. Test Coverage Summary

| Feature | Bridge Tests | SDK Tests | Status |
|---------|-------------|-----------|--------|
| Initialize workflow | Pattern 8-20 | test_worker.py | ✅ |
| Fire timer | Patterns 10-12, 20 | test_worker.py | ✅ |
| Cancel workflow | Pattern 15 | test_worker.py | ✅ |
| Signal workflow | Patterns 11, 11-multi | test_worker.py | ✅ |
| Query workflow | Patterns 12, 12-args, 13 | test_worker.py | ✅ |
| Query after completion | Bridge tests | test_e2e_integration | ✅ Fixed |
| Schedule activity | Patterns 8, 9, 10 | test_worker.py | ✅ |
| Activity result | Patterns 8, 9 | test_worker.py | ✅ |
| Activity failure | Pattern 9 | Limited | ⚠️ Error type |
| Start child workflow | Pattern 14 | test_worker.py | ✅ |
| Child workflow result | Pattern 14 | test_worker.py | ✅ |
| Continue-as-new | Pattern 17, CAN tests | test_continue_as_new.py, test_e2e_continue_as_new.py | ✅ |
| Signal external | Pattern 18, external tests | Bridge only | ⚠️ Needs API |
| Upsert search attrs | Pattern 19, SA tests | Bridge only | ⚠️ Needs API |
| Cache eviction | Patterns 17, CAN | test_single_thread_worker.py | ✅ |
| Parallel workflows | Pattern 20 | test_bridge_parallel.py | ✅ |
| Workflow failure | Pattern 16 | test_worker.py | ✅ |

---

## 5. Priority Recommendations

### P0 - Critical ✅ DONE
1. ~~**ContinueAsNewWorkflowExecution**~~ - ✅ Implemented

### P1 - High Priority
1. **SignalExternalWorkflowExecution** - Bridge done, needs public API
2. **Error type preservation** - Required for proper error handling

### P2 - Medium Priority
3. **UpsertSearchAttributes** - Bridge done, needs public API
4. **Headers propagation** - Required for distributed tracing
5. **Activity cancellation cleanup** - Verify/remove unused `ActivityCancelledJob`

### P3 - Low Priority
6. **Custom payload codecs** - Advanced feature

---

## 6. Summary

| Category | Count | Status |
|----------|-------|--------|
| Workflow commands | 3 | 1 done, 2 need API |
| Job handlers | 2 | All done |
| Architectural | 6 | 4 done, 2 remaining |
| Public APIs | 4 | 1 done, 2 need implementation, 1 optional |

**Overall assessment:** The SDK has progressed significantly. Core workflow operations are complete including continue-as-new. The remaining work is:
1. Add public APIs for signal-external and upsert-search-attributes (bridge work done)
2. Implement error type preservation for activity failures
3. Add headers propagation for observability

The foundation is solid and production-readiness is within reach.
