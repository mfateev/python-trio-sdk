# Gap Analysis: Bridge Tests vs SDK Implementation

**Date:** 2026-02-02
**Author:** Claude Code Analysis
**Scope:** Comparison of patterns learned from bridge tests vs current SDK worker implementation

---

## Executive Summary

The bridge pattern tests reveal the full scope of Temporal's workflow activation/completion protocol. Comparing these patterns against the current `temporalio_trio` SDK implementation reveals **3 major gaps** (missing commands), **2 incomplete job handlers**, and **several architectural considerations** that will need addressing for production readiness.

---

## 1. Missing Workflow Commands

These commands are tested and working in bridge tests but **not implemented** in the SDK.

### 1.1 ContinueAsNewWorkflowExecution (Critical)

**Bridge Test Coverage:**
- `test_bridge_continue_as_new.py` - 4 test variants
- `test_bridge_advanced.py` - Pattern 17

**What it does:**
- Completes current workflow and starts a fresh execution
- Preserves workflow ID but gets new run ID
- Can change workflow type, arguments, task queue

**Bridge test pattern:**
```python
# POC type used in tests
@dataclass
class ContinueAsNewCommand:
    workflow_type: str
    args: tuple = ()
    task_queue: str | None = None

# Converts to bridge proto:
bridge_cmd.continue_as_new_workflow_execution.workflow_type = cmd.workflow_type
bridge_cmd.continue_as_new_workflow_execution.task_queue = cmd.task_queue or ""
for arg in cmd.args:
    payload = dc.payload_converter.to_payload(arg)
    bridge_cmd.continue_as_new_workflow_execution.arguments.append(payload)
```

**SDK Gap:**
- No `ContinueAsNewCommand` defined in `_activation.py`
- No `workflow.continue_as_new()` API in `_workflow.py`
- No conversion in `poc_to_bridge_completion()`

**Impact:** Workflows cannot implement patterns like:
- Pagination/batching with state reset
- Long-running workflows that need periodic cleanup
- Version migration patterns

---

### 1.2 SignalExternalWorkflowExecution (High)

**Bridge Test Coverage:**
- `test_bridge_signal_external.py` - 3 test variants
- `test_bridge_advanced.py` - Pattern 18

**What it does:**
- Sends a signal from one workflow to another
- Supports targeting by workflow_id or workflow_id+run_id
- Returns resolution (success/failure)

**Bridge test pattern:**
```python
@dataclass
class SignalExternalWorkflowCommand:
    seq: int
    workflow_id: str
    signal_name: str
    args: tuple = ()
    run_id: str | None = None
    child_workflow_only: bool = False

# Corresponding job for resolution:
# resolve_signal_external_workflow with seq and status
```

**SDK Gap:**
- No `SignalExternalWorkflowCommand` in `_activation.py`
- No `workflow.signal_external_workflow()` API
- No `SignalExternalResolvedJob` to handle resolution
- Missing job type `resolve_signal_external_workflow` in `bridge_to_poc_activation()`

**Impact:** Workflows cannot coordinate with other workflows via signals from within workflow code.

---

### 1.3 UpsertSearchAttributes (Medium)

**Bridge Test Coverage:**
- `test_bridge_search_attributes.py` - 3 test variants
- `test_bridge_advanced.py` - Pattern 19

**What it does:**
- Updates workflow's search attributes during execution
- Enables dynamic visibility queries
- Supports typed search attribute fields

**Bridge test pattern:**
```python
@dataclass
class UpsertSearchAttributesCommand:
    search_attributes: dict[str, Any]

# Converts to bridge with payload encoding per attribute
```

**SDK Gap:**
- No `UpsertSearchAttributesCommand` in `_activation.py`
- No `workflow.upsert_search_attributes()` API
- POC conversion exists in `_bridge_types.py` but not integrated

**Impact:** Workflows cannot update search attributes for visibility/filtering during execution.

---

## 2. Missing/Incomplete Job Handlers

### 2.1 resolve_signal_external_workflow (Missing)

**Bridge behavior:**
- Sent after `SignalExternalWorkflowExecution` command completes
- Contains `seq` and status (succeeded/failed)
- Failure includes reason (workflow not found, etc.)

**SDK status:**
- Not handled in `bridge_to_poc_activation()`
- Will raise `NotImplementedError` if encountered

---

### 2.2 ActivityCancelledJob (Defined but Unused)

**SDK Definition** (`_activation.py:310-322`):
```python
@dataclass
class ActivityCancelledJob:
    """Activity was cancelled."""
    seq: int
    details: Any = None
```

**Gap:**
- Defined in types but never processed in `_apply_activation()`
- No corresponding handling in `WorkflowRuntime`
- Activity cancellation flow incomplete

**Bridge test pattern (Pattern 10):**
```python
# 1. Schedule activity
# 2. Send RequestCancelActivity command
# 3. Receive resolve_activity with cancelled status
# 4. Handle cancellation in workflow
```

The bridge tests show `resolve_activity` with `cancelled` status, not a separate `activity_cancelled` job. This suggests the SDK's `ActivityCancelledJob` may be unnecessary or the bridge behavior differs from expectation.

---

## 3. Architectural Gaps

### 3.1 Cache Eviction Handling

**What bridge tests show:**
```python
# Cache eviction can happen between any activations
if activation.has_job_type("remove_from_cache"):
    # Must send empty completion
    evict_completion = CompletionBuilder(activation.run_id).build()
    await bridge.complete_workflow_activation(evict_completion)
    # Then poll again for real work
```

**SDK implementation** (`_single_thread_worker.py:227-232`):
```python
if activation.is_eviction:
    await self._send_commands(run_id, [])
    continue  # polls again
```

**Assessment:** SDK handles this, but tests show eviction can happen:
- After continue-as-new
- During long-running timers
- When cache pressure is high

**Potential gap:** No mechanism to preserve local workflow state through eviction for deterministic replay.

---

### 3.2 Workflow Replay and Determinism ✅ CLARIFIED

**Key Finding: SDK-Core Handles Command Deduplication**

Investigation of sdk-python and sdk-core revealed that **SDK-Core handles command matching during replay**, not the SDK. The SDK's responsibility is simpler than initially assumed.

**How Replay Actually Works:**

1. **SDK-Core tracks history**: When a workflow replays, SDK-Core has the complete event history from the server.

2. **SDK re-runs workflow code**: The SDK creates a fresh workflow instance and re-executes from the beginning.

3. **SDK sends same commands**: Because workflow code is deterministic, it generates the same commands with same sequence numbers.

4. **SDK-Core matches commands to history**:
   - `StartTimer(seq=1)` sent → SDK-Core finds `TimerStarted(seq=1)` in history → Match!
   - Already-resolved events (timer fired, activity completed) are delivered as jobs in the same activation
   - If commands don't match history → **Nondeterminism error** → Workflow evicted

**Evidence from sdk-core tests** (`replay.rs:145-183`):
```rust
// History had StartTimer, but SDK sends ScheduleActivity instead
core.complete_workflow_activation(WorkflowActivationCompletion::from_cmds(
    task.run_id,
    vec![ScheduleActivity { seq: 0, ... }.into()],  // Wrong command!
)).await.unwrap();

// SDK-Core detects mismatch and evicts with nondeterminism
let task = core.poll_workflow_activation().await.unwrap();
assert_eq!(task.eviction_reason(), Some(EvictionReason::Nondeterminism));
```

**Evidence from sdk-python** (`_workflow_instance.py`):
- `_curr_seqs` starts fresh for each workflow instance (line 230)
- `_add_command()` just appends commands, no memoization (line 1638)
- No tracking of previously sent commands

**What the Trio SDK Actually Needs:**

The current `SingleThreadWorker` uses event-based suspension where workflows stay alive between activations. On eviction, the workflow state is deleted. The fix is straightforward:

1. ✅ On `initialize_workflow` after eviction, create a **fresh workflow instance** (already done)
2. ✅ Re-run workflow code from start (already done for new workflows)
3. ✅ Send same commands (deterministic code produces same commands)
4. ⚠️ **Gap**: Ensure `is_replaying` flag is respected (skip side effects)
5. ⚠️ **Gap**: Handle multiple jobs in single activation (e.g., `initialize_workflow` + `fire_timer` together)

**Assessment:** The replay mechanism is **simpler than initially thought**. SDK-Core does the heavy lifting. The trio SDK just needs to ensure it creates fresh instances on replay and handles the `is_replaying` flag correctly.

**Priority: Downgraded from P0 to P2** - The current implementation likely works for most cases. Edge cases around multi-job activations may need attention.

---

### 3.3 Error Type Preservation

**What bridge tests show:**
```python
# Activity failure contains rich error info:
failure = resolve_activity.result.failed.failure
failure.message  # Human readable
failure.source   # "PythonSDK"
failure.application_failure_info.type  # "ValueError"
failure.application_failure_info.non_retryable  # bool
failure.cause  # Nested failure
```

**SDK implementation:**
```python
# Creates generic RuntimeError:
error = RuntimeError(
    f"Activity {resolved.activity_id} failed: {resolved.failure}"
)
```

**Gap:** Original exception type is lost. Applications cannot:
- Catch specific exception types
- Distinguish between `ValueError` vs `TimeoutError` vs custom errors
- Preserve exception chains

**Needed:**
- Exception type reconstruction from `application_failure_info.type`
- Proper exception chaining with `__cause__`
- Non-retryable flag preservation

---

### 3.4 Headers Not Propagated

**Bridge test shows:**
```python
signal_job.headers  # Dict of header name -> payload
query_job.headers   # Same structure
```

**SDK implementation:**
- Headers captured in job dataclasses
- But never passed to signal/query handlers
- No API to access headers in workflow code

**Gap:** Cannot implement:
- Distributed tracing (trace IDs in headers)
- Authentication/authorization context
- Custom metadata propagation

---

### 3.5 Parallel Workflow Handling

**What bridge tests show (Pattern 20):**
```python
# Multiple workflows interleave on same task queue
# Activations arrive in non-deterministic order
# Must track state per run_id, not per workflow_id
```

**SDK implementation:**
- Uses `run_id` as key in `workflow_states` dict
- Correct approach for isolation

**Assessment:** SDK handles this correctly. No gap identified.

---

### 3.6 Query on Completed Workflow (Replay-Based Query)

**What happens:**
- Workflow completes and is removed from worker's cache (`_workflow_states`)
- Client sends a query to the completed workflow
- Server re-delivers history to worker with `initialize_workflow` + `query` jobs
- Worker must replay workflow from start to answer the query

**SDK implementation:**
- `SingleThreadWorker` removes workflows from `_workflow_states` after completion
- When a query activation arrives for a completed workflow, the worker receives:
  - `initialize_workflow` job (replay starts fresh)
  - `query` job in the same activation
- **Gap**: The worker doesn't handle queries arriving during replay properly

**Evidence from E2E test `test_e2e_query_triggers_replay`:**
```
# Workflow completes successfully
# Query sent via CLI after completion
# Query times out - worker doesn't respond
# Log shows: "Empty activation for completed workflow... sending empty completion"
```

**Root cause:**
- After replaying and completing, the worker sends empty completion
- The query job gets lost because workflow is immediately completed again
- No mechanism to answer queries during the "empty activation after completion" phase

**How SDK-Python handles this:**
- Workflow instance stays in memory longer, or
- Replay activation processes query before completion, or
- Multi-pass activation handling: complete workflow, then answer query

**Required fix:**
1. When activation contains both `initialize_workflow` and `query` jobs:
   - Replay workflow to restore state
   - Process query and generate `RespondToQuery` response
   - Include query response in completion (even if workflow completes)
2. Ensure query handling occurs before workflow removal from cache

**Priority: P2** - Queries on completed workflows are a real-world pattern (e.g., checking final state).

---

## 4. Missing Convenience APIs

These aren't protocol gaps but missing developer-facing APIs:

### 4.1 No `workflow.continue_as_new()` helper

**Needed:**
```python
async def continue_as_new(
    *args,
    workflow: str | None = None,
    task_queue: str | None = None,
) -> NoReturn:
    """Continue workflow as new execution."""
```

### 4.2 No `workflow.signal_external_workflow()` helper

**Needed:**
```python
async def signal_external_workflow(
    workflow_id: str,
    signal: str,
    *args,
    run_id: str | None = None,
) -> None:
    """Signal another workflow."""
```

### 4.3 No `workflow.upsert_search_attributes()` helper

**Needed:**
```python
def upsert_search_attributes(
    attributes: dict[str, Any],
) -> None:
    """Update search attributes."""
```

### 4.4 No `workflow.get_external_workflow_handle()` for external signals

**Needed:**
```python
def get_external_workflow_handle(
    workflow_id: str,
    run_id: str | None = None,
) -> ExternalWorkflowHandle:
    """Get handle to signal/cancel external workflow."""
```

---

## 5. Test Coverage Comparison

| Feature | Bridge Tests | SDK Tests | Gap |
|---------|-------------|-----------|-----|
| Initialize workflow | Pattern 8-20 | test_worker.py | None |
| Fire timer | Patterns 10-12, 20 | test_worker.py | None |
| Cancel workflow | Pattern 15 | test_worker.py | None |
| Signal workflow | Patterns 11, 11-multi | test_worker.py | None |
| Query workflow | Patterns 12, 12-args, 13 | test_worker.py | None |
| Query after completion | - | test_e2e_integration | Replay-based query |
| Schedule activity | Patterns 8, 9, 10 | test_worker.py | None |
| Activity result | Patterns 8, 9 | test_worker.py | None |
| Activity failure | Pattern 9 | Limited | Error type |
| Activity cancellation | Pattern 10 | Missing | Full flow |
| Start child workflow | Pattern 14 | test_worker.py | None |
| Child workflow result | Pattern 14 | test_worker.py | None |
| Child start failure | Pattern 14-start-fail | Missing | Test needed |
| Cancel child workflow | Pattern 15 | Limited | Full flow |
| Continue-as-new | Pattern 17, CAN tests | Missing | Command + tests |
| Signal external | Pattern 18, external tests | Missing | Command + tests |
| Upsert search attrs | Pattern 19, SA tests | Missing | Command + tests |
| Cache eviction | Patterns 17, CAN | Implicit | Explicit tests |
| Parallel workflows | Pattern 20 | Missing | Tests needed |
| Workflow failure | Pattern 16 | test_worker.py | None |

---

## 6. Priority Recommendations

### P0 - Critical for Production
1. **ContinueAsNewWorkflowExecution** - Required for long-running workflows

### P1 - High Priority
2. **SignalExternalWorkflowExecution** - Required for workflow coordination
3. **Error type preservation** - Required for proper error handling
4. **Activity cancellation flow** - Required for graceful shutdown

### P2 - Medium Priority
5. **Query on completed workflow** - Required for checking final state after completion
6. **Replay edge cases** - Multi-job activations, `is_replaying` flag handling (SDK-Core handles most of it)
7. **UpsertSearchAttributes** - Required for visibility
8. **Headers propagation** - Required for distributed tracing
9. **Child workflow start failure handling** - Edge case but important

### P3 - Low Priority
9. **Parallel workflow tests** - SDK likely handles correctly
10. **Custom payload codecs** - Advanced feature

---

## 7. Summary of Findings

| Category | Count | Examples |
|----------|-------|----------|
| Missing commands | 3 | ContinueAsNew, SignalExternal, UpsertSearchAttrs |
| Missing job handlers | 1 | resolve_signal_external_workflow |
| Incomplete handlers | 1 | ActivityCancelledJob defined but unused |
| Architectural gaps | 4 | Error types, headers, multi-job activations, query-on-completed |
| Missing APIs | 4 | continue_as_new(), signal_external_workflow(), etc. |

**Key Insight: SDK-Core Handles Replay**

Investigation revealed that SDK-Core handles command deduplication during replay, not the SDK. The SDK's job is to:
1. Re-run workflow code deterministically
2. Send the same commands (SDK-Core matches them to history)
3. Respect `is_replaying` flag for side effects

This significantly simplifies the replay gap - the trio SDK's current architecture is closer to correct than initially assessed.

**Overall assessment:** The SDK has a solid foundation with core workflow operations working. The missing commands (ContinueAsNew, SignalExternal, UpsertSearchAttrs) are the primary gaps for real-world workflows. Replay handling is largely handled by SDK-Core; the trio SDK just needs minor adjustments for edge cases.
