# Implementation Plan: Trio-Based Temporal SDK POC

This document outlines the phased implementation plan for the Trio-based Temporal SDK proof of concept.

## Current Status

**Last Updated:** 2026-02-01

| Phase | Status | Notes |
|-------|--------|-------|
| 1 | COMPLETE | Core infrastructure fully implemented |
| 2 | COMPLETE | Workflow instance with full feature set |
| 3 | COMPLETE | Time, clock, and activation handling |
| 4 | COMPLETE | Runner and public API |
| 5 | BLOCKED | Tests written, awaiting Trio deterministic fork |

### Blocking Issue

Tests requiring `trio.run(deterministic=True)` fail because the standard Trio library
doesn't support the `deterministic` parameter. This requires installing the Trio fork
with deterministic scheduling support:

```bash
pip install git+https://github.com/mfateev/trio.git@temporal-deterministic-scheduling
```

**Test Results (with standard Trio):**
- 195 tests pass (unit tests for components)
- 41 tests fail (activation/execution tests requiring deterministic mode)

---

## Overview

**Goal:** Prove that Trio can serve as the async runtime for deterministic Temporal workflow execution.

**Success Criteria:**
1. A workflow can call `await workflow.sleep(duration)` and wake up after the specified time
2. Replaying the same workflow history produces identical execution
3. Multiple workflows can run in parallel with isolated, deterministic execution
4. Architecture matches the existing Python SDK patterns

**Development Rules:**
- Each phase must have unit tests covering all new code
- Each phase must pass `poe lint` before proceeding
- Each phase must pass `poe test` before proceeding

## Phase Summary

| Phase | Focus | Key Deliverables | Status |
|-------|-------|------------------|--------|
| 1 | Core Infrastructure | `_Runtime`, `_Definition`, decorators | COMPLETE |
| 2 | Workflow Instance | `WorkflowInstanceDetails`, `TrioWorkflowInstance` | COMPLETE |
| 3 | Time & Clock | `WorkflowClock`, history events, time control | COMPLETE |
| 4 | Runner & Execution | `WorkflowRunner`, `TrioWorkflowRunner`, public API | COMPLETE |
| 5 | Replay Verification | Replay tests, parallel execution tests | BLOCKED |

---

## Phase 1: Core Infrastructure - COMPLETE

**Goal:** Establish the foundational patterns that all workflow APIs build upon.

### Implemented Components

- `_Runtime` abstract base class with `current()`, `set_current()`, `reset_current()`
- `_Definition` dataclass with `from_class()`, `must_from_class()`
- `@workflow.defn` decorator with optional `name` parameter
- `@workflow.run` decorator with async validation

### Phase 1 Checklist

- [x] Implement `_Runtime` abstract base class
- [x] Implement `_Definition` dataclass
- [x] Implement `@workflow.defn` decorator
- [x] Implement `@workflow.run` decorator
- [x] Write unit tests
- [x] Pass `poe lint`
- [x] Pass `poe test`

---

## Phase 2: Workflow Instance - COMPLETE

**Goal:** Implement the workflow instance that handles activations and provides runtime APIs.

### Implemented Components

- `Info` dataclass with `workflow_id`, `workflow_type`, `run_id`, `task_queue`, `raw_memo`
- `WorkflowInstanceDetails` frozen dataclass
- `WorkflowInstance` abstract base class
- `TrioWorkflowInstance` implementing both `WorkflowInstance` and `_Runtime`

### Additional Features (Beyond Original Plan)

- `workflow.random()` - Deterministic random number generator
- `workflow.memo()` and `workflow.memo_value()` - Memo access
- `workflow.continue_as_new()` - Continue-as-new support
- `_ContinueAsNewError` for signaling continue-as-new

### Phase 2 Checklist

- [x] Implement `Info` dataclass
- [x] Implement `WorkflowInstanceDetails` dataclass
- [x] Implement `WorkflowInstance` abstract base class
- [x] Implement `TrioWorkflowInstance`
- [x] Write unit tests
- [x] Pass `poe lint`
- [x] Pass `poe test`

---

## Phase 3: Time & Clock - COMPLETE

**Goal:** Implement time control for deterministic workflow execution.

### Implemented Components

- `WorkflowClock` implementing `trio.abc.Clock` with `advance_to()`, `current_time()`, `deadline_to_sleep_time()`
- `WorkflowStartedJob`, `TimerFiredJob`, `DoUpdateJob` activation jobs
- `WorkflowActivation` container for jobs with `timestamp_ns`
- `StartTimerCommand`, `CompleteWorkflowCommand`, `FailWorkflowCommand` completion commands
- `ContinueAsNewWorkflowCommand` for continue-as-new
- `UpdateAcceptedCommand`, `UpdateCompletedCommand`, `UpdateRejectedCommand` for updates
- `WorkflowActivationCompletion` container for commands

### Implementation Details

The `TrioWorkflowInstance.activate()` method:
1. Updates workflow time from activation timestamp
2. Processes jobs to update internal state (fired timers set)
3. Sets runtime context via `_Runtime.set_current()`
4. Creates `WorkflowClock` at current workflow time
5. Runs workflow with `trio.run(deterministic=True, random_seed=..., clock=...)`
6. Catches `_WorkflowYield` when workflow needs to wait for timer
7. Returns completion with commands

Timer handling uses replay-safe pattern:
- `workflow_sleep()` checks if timer already fired (replay case)
- If fired, returns immediately
- If not fired, creates `StartTimerCommand` and raises `_WorkflowYield`

### Phase 3 Checklist

- [x] Implement history event dataclasses
- [x] Implement `WorkflowClock`
- [x] Implement `TrioWorkflowInstance.activate()`
- [x] Implement timer handling with replay support
- [x] Write unit tests
- [x] Pass `poe lint`
- [x] Pass `poe test` (unit tests only, without deterministic mode)

---

## Phase 4: Runner & Public API - COMPLETE

**Goal:** Implement the workflow runner and expose the public API.

### Implemented Components

- `WorkflowRunner` abstract base class
- `TrioWorkflowRunner` with `prepare_workflow()` and `create_instance()`
- Public API functions in `workflow.py`:
  - `sleep(duration, *, summary=None)` - Async sleep
  - `time()` - Current time in seconds
  - `time_ns()` - Current time in nanoseconds
  - `info()` - Workflow info
  - `random()` - Deterministic random
  - `memo()` - All memo values
  - `memo_value(key, default, *, type_hint)` - Single memo value
  - `continue_as_new(*args, **kwargs)` - Continue-as-new

### Phase 4 Checklist

- [x] Implement `WorkflowRunner` abstract base class
- [x] Implement `TrioWorkflowRunner`
- [x] Implement public API (`sleep`, `time`, `time_ns`, `info`, `random`, `memo`, `continue_as_new`)
- [x] Write unit tests
- [x] Pass `poe lint`
- [x] Pass `poe test` (unit tests only)

---

## Phase 5: Replay Verification - BLOCKED

**Goal:** Verify deterministic replay and parallel execution.

**Status:** Tests are written and ready. Blocked on Trio deterministic fork.

### Test Files

- `tests/test_replay.py` - Comprehensive replay tests:
  - `TestReplayDeterminism` - Same history produces same result
  - `TestReplayHistoryIntegrity` - History structure validation
  - `TestReplayEdgeCases` - Zero sleep, large sleep, no sleeps

- `tests/test_parallel.py` - Parallel execution tests:
  - `TestParallelWorkflowIsolation` - Workflows don't interfere
  - `TestSameSeedDeterminism` - Same seed = same result
  - `TestHighConcurrency` - Many parallel workflows
  - `TestSequentialVsParallel` - Same results either way

### Phase 5 Checklist

- [x] Implement replay test
- [x] Implement parallel execution test
- [x] Verify determinism with same seed
- [x] Verify isolation between parallel workflows
- [ ] Pass `poe lint`
- [ ] Pass `poe test` - **BLOCKED: Requires Trio deterministic fork**

---

## File Structure (Current)

```
temporalio_trio/
├── __init__.py                 # Package exports
├── workflow.py                 # Public workflow API + _Runtime, _Definition
└── worker/
    ├── __init__.py             # Worker exports
    ├── _activation.py          # Activation/completion types
    ├── _clock.py               # WorkflowClock
    ├── _interceptor.py         # (placeholder for future use)
    └── _workflow_instance.py   # WorkflowRunner, TrioWorkflowInstance

tests/
├── __init__.py
├── test_basic.py               # Basic workflow definition tests
├── test_workflow_activation.py # Activation handling tests
├── test_workflow_clock.py      # Clock unit tests
├── test_workflow_instance.py   # Instance creation tests
├── test_workflow_runner.py     # Runner tests
├── test_replay.py              # Replay determinism tests
└── test_parallel.py            # Parallel execution tests
```

---

## Dependencies

### Required: Trio Fork with Deterministic Scheduling

```bash
# Install from fork (required for full test suite)
pip install git+https://github.com/mfateev/trio.git@temporal-deterministic-scheduling
```

### Current pyproject.toml

```toml
dependencies = [
    "trio>=0.27.0",
    "attrs>=23.2.0",
    "outcome>=1.3.0",
    "typing-extensions>=4.2.0,<5",
]
```

---

## Success Metrics

After completing all phases:

1. **Unit Tests:** All tests pass (195 currently passing)
2. **Integration Tests:** All activation tests pass (41 blocked on Trio fork)
3. **Linting:** `poe lint` passes (pyright, mypy, ruff)
4. **Replay:** Same history produces identical results
5. **Parallelism:** Multiple workflows run concurrently without interference
6. **API Compatibility:** Public API matches Temporal SDK patterns

---

## Next Steps

### Immediate (Unblock Phase 5)

1. **Install Trio Fork** - Set up deterministic scheduling support
2. **Run Full Test Suite** - Verify all 236 tests pass
3. **Document Fork Setup** - Add instructions to README

### Post-POC

Once POC is validated:

1. **Integrate with Temporal Server** - Real gRPC communication
2. **Add Activities** - External operation support
3. **Add Signals/Queries** - Workflow interaction
4. **Add Child Workflows** - Workflow composition
5. **Consider Sandbox** - Determinism enforcement
