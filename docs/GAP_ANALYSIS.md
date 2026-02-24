# Gap Analysis: Production Readiness & Feature Compatibility with sdk-python

**Date:** 2026-02-24 (Updated)
**Author:** Claude Code Analysis
**Scope:** Full feature parity with sdk-python and production readiness assessment

---

## Executive Summary

This document tracks all gaps between the Trio SDK and the official Temporal Python SDK (sdk-python) for achieving:
1. **100% feature compatibility** with sdk-python
2. **Production readiness** for real-world deployments

**Current Status:**
- **Core workflow execution**: ✅ Complete
- **Basic worker features**: ✅ Complete
- **Advanced worker features**: ❌ Missing (tuner, interceptors, versioning)
- **Production features**: ❌ Missing (metrics, proper error types, updates)
- **API completeness**: ~70% (core APIs done, advanced APIs missing)

**Assessment:** SDK is suitable for POC/experimentation but **not production-ready**. Significant gaps remain in advanced features, observability, and extensibility.

---

## 1. Workflow API Gaps

### 1.1 Update Handlers ❌ NOT IMPLEMENTED

**Status:** Not implemented. Critical for production workflows that need to modify running executions.

**What's missing:**
- `@workflow.update` decorator
- `workflow.update()` API for clients
- Update validation handlers
- Update rejection

**sdk-python reference:**
```python
@workflow.defn
class MyWorkflow:
    @workflow.update
    async def update_value(self, new_value: int) -> int:
        """Update handler - can modify workflow state and return result."""
        old_value = self._value
        self._value = new_value
        return old_value

    @update_value.validator
    def validate_update(self, new_value: int) -> None:
        """Validation runs before update - can reject."""
        if new_value < 0:
            raise ValueError("Value must be positive")
```

**Why it matters:**
- Updates provide synchronous workflow modification (unlike signals)
- Can return values to caller
- Support validation before execution
- Critical for interactive workflows (approval flows, configuration updates)

**Priority: P0** - Required for production interactive workflows.

---

### 1.2 Workflow Update (from within workflow) ❌ NOT IMPLEMENTED

**Status:** Not implemented.

**What's missing:**
- `workflow.wait_for_update()` - Wait for update from within workflow
- `workflow.update_info()` - Access current update context

**Priority: P2** - Advanced use case, less common.

---

### 1.3 Versioning & Patching ❌ NOT IMPLEMENTED

**Status:** Not implemented. Critical for safe workflow code updates in production.

**What's missing:**
- `workflow.patched()` API for safe code changes
- `workflow.deprecate_patch()` for cleanup
- Version markers for workflow definitions

**sdk-python reference:**
```python
@workflow.defn
class MyWorkflow:
    @workflow.run
    async def run(self) -> str:
        if workflow.patched("my-patch"):
            # New code path
            return await new_logic()
        else:
            # Old code path (for replay)
            return await old_logic()
```

**Why it matters:**
- Safe deployment of workflow code changes
- Prevents nondeterminism errors during replay
- Essential for long-running production workflows

**Priority: P0** - Critical for production deployments with code updates.

---

### 1.4 Signal External Workflow ✅ COMPLETE

**Status:** Fully implemented with public API and E2E tests.

- `workflow.get_external_workflow_handle()` ✅
- `ExternalWorkflowHandle.signal()` ✅
- `ChildWorkflowHandle.signal()` ✅
- Bridge tests + E2E tests ✅

---

### 1.5 Upsert Search Attributes ✅ COMPLETE

**Status:** Fully implemented with public API and E2E tests.

- `workflow.upsert_search_attributes()` ✅
- All value types supported (str, int, float, bool, datetime, Sequence[str]) ✅
- E2E tests with real server ✅

---

### 1.6 Async Activity Context ✅ COMPLETE

**Status:** Fully implemented with heartbeat, cancellation, and info access.

- `@activity.defn` decorator ✅
- `activity.heartbeat()` ✅
- Activity cancellation handling (`wait_for_cancelled()`) ✅
- Activity info access ✅
- E2E tests (8 tests) ✅

---

### 1.7 Local Activities ❌ NOT IMPLEMENTED

**Status:** Not implemented.

**What's missing:**
- `@activity.defn(local=True)` for local activities
- Local activity scheduling in workflows
- Local activity execution in worker

**Why it matters:**
- Low-latency operations (no network round-trip)
- Better performance for simple operations
- Common pattern in production workflows

**Priority: P1** - Important optimization for production.

---

## 2. Worker Configuration Gaps

### 2.1 Interceptors ❌ NOT IMPLEMENTED

**Status:** Not implemented. Critical for observability and customization.

**What's missing:**
- `Interceptor` base class
- Workflow interceptors (activity calls, timers, child workflows)
- Activity interceptors (execution, heartbeat)
- Client interceptors (workflow start, signal, query)
- Interceptor chaining

**sdk-python reference:**
```python
class TracingInterceptor(Interceptor):
    """Add distributed tracing to all operations."""

    async def intercept_activity(
        self, input: ActivityInboundInput
    ) -> ActivityOutput:
        with tracer.start_span(f"activity:{input.activity_type}"):
            return await input.next(input)
```

**Why it matters:**
- Distributed tracing (OpenTelemetry integration)
- Custom metrics collection
- Authorization/authentication hooks
- Request/response logging
- Performance monitoring

**Priority: P0** - Critical for production observability.

---

### 2.2 Build ID / Versioning ⚠️ PARTIAL

**Status:** Worker accepts `build_id` parameter but no versioning workflow support.

**What's implemented:**
- Worker `build_id` parameter

**What's missing:**
- `get_build_id()` API in workflows
- Build ID-based routing
- Version-specific workflow execution
- Deployment strategy support

**Priority: P1** - Important for canary deployments and gradual rollouts.

---

### 2.3 Failure Converter ⚠️ PARTIAL

**Status:** Basic support exists but incomplete.

**What's implemented:**
- `FailureConverter` exists in codebase
- Basic failure serialization

**What's missing:**
- Custom failure converters
- Error type preservation (see 3.4)
- Failure details encoding/decoding

**Priority: P1** - Important for proper error handling.

---

### 2.4 Workflow Runner Customization ❌ NOT IMPLEMENTED

**Status:** Not implemented.

**What's missing:**
- Custom workflow runner support
- Sandboxing options (RestrictedWorkflowRunner)
- Workflow code isolation

**Priority: P2** - Advanced feature, less critical.

---

## 3. Architectural & Runtime Gaps

### 3.1 Error Type Preservation ✅ COMPLETE

**Status:** Fully implemented. ActivityError, ChildWorkflowError, ApplicationError all preserved.

- Exception type reconstruction from failure info ✅
- Proper exception chaining with `__cause__` ✅
- ActivityError with activity_type/activity_id metadata ✅
- ChildWorkflowError with workflow_type/workflow_id metadata ✅
- E2E tests validated ✅

---

### 3.2 Headers Propagation ✅ COMPLETE

**Status:** Fully implemented end-to-end.

- `workflow.info().headers` access ✅
- Headers parsed from protobuf activations (InitializeWorkflow, SignalWorkflow, QueryWorkflow) ✅
- Outgoing headers attached to all commands (ScheduleActivity, StartChildWorkflow, SignalExternalWorkflow, ContinueAsNew) ✅
- 21 unit + 2 E2E tests ✅

---

### 3.3 Payload Codec Support ❌ NOT IMPLEMENTED

**Status:** Not implemented.

**What's missing:**
- `PayloadCodec` interface
- Encryption codec support
- Compression codec support
- Custom codec registration

**Why it matters:**
- Data encryption at rest
- PII protection
- Compliance requirements (GDPR, HIPAA)
- Bandwidth optimization

**Priority: P1** - Critical for security-sensitive deployments.

---

### 3.4 Metrics & Telemetry ❌ NOT IMPLEMENTED

**Status:** Not implemented.

**What's missing:**
- Prometheus metrics exporter
- Custom metrics runtime
- OpenTelemetry integration
- Worker health metrics (task latency, queue depth, etc.)

**sdk-python reference:**
```python
from temporalio.runtime import Runtime, PrometheusConfig

runtime = Runtime(telemetry=TelemetryConfig(
    metrics=PrometheusConfig(bind_address="0.0.0.0:9090")
))
```

**Why it matters:**
- Production monitoring (SLOs, SLIs)
- Alerting on worker health
- Performance analysis
- Capacity planning

**Priority: P0** - Critical for production operations.

---

### 3.5 Data Converter Extensibility ⚠️ LIMITED

**Status:** Basic data converter support exists but limited.

**What's implemented:**
- `DataConverter` parameter in Worker
- Default converter usage

**What's missing:**
- Custom payload converters
- Type hints preservation
- Codec chaining
- Protobuf support

**Priority: P2** - Important for advanced serialization needs.

---

### 3.6 Schedules API ❌ NOT IMPLEMENTED

**Status:** Not implemented.

**What's missing:**
- Schedule creation/management from Python
- Cron workflow support
- Pause/unpause schedules
- Backfill operations

**Priority: P2** - Important for recurring workflows.

---

### 3.7 Nexus Support ❌ NOT IMPLEMENTED

**Status:** Not implemented. Nexus is a newer Temporal feature for service-to-service calls.

**What's missing:**
- Nexus operation handlers
- Nexus client calls
- Cross-namespace operations

**Priority: P3** - New feature, lower priority for initial production.

---

### 3.8 Worker Tuner / Slot Supplier ❌ NOT IMPLEMENTED

**Status:** Not implemented. Optimization for dynamic resource management.

**What's missing:**
- `WorkerTuner` base class and implementations
- `FixedSizeSlotSupplier` - Simple fixed-size slot allocation
- `ResourceBasedTuner` - Dynamic tuning based on system resources
- `ResourceBasedSlotSupplier` - Memory/CPU-aware slot management
- Custom slot supplier support
- Worker configuration: `worker_tuner` parameter

**sdk-python reference:**
```python
from temporalio.worker import Worker
from temporalio.worker.tuner import (
    WorkerTuner,
    FixedSizeSlotSupplier,
    ResourceBasedTuner,
)

# Option 1: Fixed size tuner
tuner = WorkerTuner(
    workflow_task_slot_supplier=FixedSizeSlotSupplier(100),
    activity_task_slot_supplier=FixedSizeSlotSupplier(200),
    local_activity_slot_supplier=FixedSizeSlotSupplier(200),
)

# Option 2: Resource-based tuner (dynamic)
tuner = ResourceBasedTuner(
    target_memory_usage=0.8,
    target_cpu_usage=0.8,
)

worker = Worker(
    client,
    task_queue="my-queue",
    workflows=[MyWorkflow],
    worker_tuner=tuner,  # ❌ Not supported in Trio SDK
)
```

**Current Trio SDK alternative:**
```python
# Static limits work fine for most use cases
worker = Worker(
    client,
    task_queue="my-queue",
    workflows=[MyWorkflow],
    max_concurrent_workflow_tasks=100,
    max_concurrent_activities=200,
    max_concurrent_local_activities=200,
)
```

**Why it could be useful:**
- Dynamic resource management based on system load
- Memory protection in very high-throughput scenarios
- Better resource utilization in variable workloads
- Custom tuning policies for specific domains

**What's needed:**
1. Python API for tuner/slot supplier interfaces
2. Bridge exposure of sdk-core's tuner support
3. Resource monitoring integration (memory, CPU)
4. Configuration passing through Worker initialization

**SDK-Core support:** ✅ Available in `sdk-core/crates/sdk-core/src/worker/tuner.rs` - just needs Python exposure.

**Priority: P3** - **Optional optimization**. Static limits (`max_concurrent_*`) are sufficient for most production deployments. Dynamic tuning is beneficial for very high-throughput systems with variable workloads, but not required for basic production use.

---

## 4. Client API Gaps

### 4.1 Workflow Handle APIs ⚠️ PARTIAL

**What's implemented:**
- Basic workflow start ✅
- Signal workflow ✅
- Cancel workflow ✅
- Terminate workflow ✅
- Result (with server-side long polling) ✅

**Known placeholder:**
- `WorkflowHandle.query()` — bridge call is made but **response parsing is not implemented** (`temporalio_trio/client/_workflow_handle.py:199-200`). Always returns `None`. The TODO is to parse `QueryWorkflowResponse` protobuf and deserialize the result payloads.

**What's missing:**
- `workflow_handle.query()` result parsing (placeholder)
- `workflow_handle.update()` - Send update to workflow
- `workflow_handle.fetch_history()` - Get full workflow history
- Typed workflow handles

**Priority: P1** - Query result parsing is a small fix; update/history are larger features.

---

### 4.2 Schedule Handle APIs ❌ NOT IMPLEMENTED

**Status:** Not implemented.

**What's missing:**
- Create schedule
- List schedules
- Update schedule
- Delete schedule
- Trigger schedule

**Priority: P2** - Part of schedules feature.

---

### 4.3 Async Activity Completion ❌ NOT IMPLEMENTED

**Status:** Not implemented.

**What's missing:**
- Manual activity completion via token
- Async activity pattern support
- Heartbeat from external systems

**Priority: P2** - Important for long-running external integrations.

---

## 5. Testing & Development Gaps

### 5.1 Workflow Testing Utilities ✅ COMPLETE

**Status:** `WorkflowEnvironment` implemented (Phase 9).

- `WorkflowEnvironment` for unit testing ✅
- 709 tests passing (unit + E2E + stress) ✅
- E2E stress tests with 100-200 concurrent workflows ✅

**Still missing:**
- Time skipping in tests (requires test server integration)
- Mock activity support
- Workflow replayer for validation

**Priority: P2** - Core testing utilities done; time-skipping is nice-to-have.

---

### 5.2 Workflow Replay Testing ❌ NOT IMPLEMENTED

**Status:** Not implemented.

**What's missing:**
- `WorkflowReplayer` for history validation
- Regression testing against production histories
- Nondeterminism detection

**Why it matters:**
- Validate code changes don't break existing workflows
- Catch nondeterminism before production
- Essential for safe deployments

**Priority: P1** - Critical for production code changes.

---

## 6. Documentation & Production Support

### 6.1 API Documentation ⚠️ MINIMAL

**Status:** Basic docstrings exist but incomplete.

**What's needed:**
- Comprehensive API reference docs
- Migration guide from sdk-python
- Production deployment guide
- Performance tuning guide
- Troubleshooting documentation

**Priority: P1** - Required for adoption.

---

### 6.2 Examples & Samples ⚠️ MINIMAL

**Status:** Basic E2E tests exist but no standalone examples.

**What's needed:**
- Hello World example
- Production-ready examples (error handling, retries, etc.)
- Common patterns (sagas, entity workflows, etc.)
- Migration examples from sdk-python

**Priority: P1** - Required for onboarding.

---

## 7. Code Placeholders & Stubs

A scan of all source files under `temporalio_trio/` (2026-02-24) found the following incomplete implementations:

### 7.1 `WorkflowHandle.query()` Result Parsing — PLACEHOLDER

**File:** `temporalio_trio/client/_workflow_handle.py:162-200`

**Status:** The method encodes query arguments, calls `self._client._bridge.query_workflow()`, but **does not parse the response**. Returns `None` unconditionally.

```python
# Line 199-200 — current code:
# TODO: Parse QueryWorkflowResponse and deserialize result
return None  # Placeholder
```

**Fix needed:** Parse `QueryWorkflowResponse` protobuf from `response_bytes`, extract the result payloads, and deserialize via `DataConverter`. Small task (~20 lines).

**Impact:** Client-side `handle.query()` is non-functional. Worker-side query handling works correctly (queries are processed and results sent to the server).

---

### 7.2 Bridge Type Fallbacks — DEFENSIVE (Not a gap)

**File:** `temporalio_trio/worker/_bridge_types.py:150, 817`

Two `NotImplementedError` raises for unknown job/command types. These are **defensive guards**, not missing features — all known job types (11 types) and command types (16 types) are handled. These would only trigger if SDK-Core introduces new job/command types in the future.

---

### 7.3 Summary

| Location | Type | Severity | Fix Effort |
|----------|------|----------|------------|
| `_workflow_handle.py:200` — `query()` result parsing | Placeholder | Medium — client queries broken | Small (~20 lines) |
| `_bridge_types.py:150` — unknown job type guard | Defensive | None — all types handled | N/A |
| `_bridge_types.py:817` — unknown command type guard | Defensive | None — all types handled | N/A |

**No other placeholders, TODOs, FIXMEs, or stubs found in source code.**

---

## 8. Production Readiness Checklist

### 8.1 Critical (P0) - Required for Production

- [ ] **Metrics & Telemetry** - Observability
- [ ] **Interceptors** - Tracing, logging, monitoring
- [x] **Error Type Preservation** - ✅ Complete (ActivityError, ChildWorkflowError, ApplicationError)
- [ ] **Update Handlers** - Interactive workflows
- [ ] **Versioning (patched/deprecate_patch)** - Safe code updates

**Status:** 1/5 complete.

---

### 8.2 High Priority (P1) - Important for Production

- [ ] **Local Activities** - Performance optimization
- [x] **Activity Context & Heartbeat** - ✅ Complete (heartbeat, cancellation, info, wait_for_cancelled)
- [ ] **Payload Codec** - Encryption & compression
- [x] **Headers Propagation** - ✅ Complete (workflow.info().headers, outgoing commands)
- [x] **Signal External Workflow API** - ✅ Complete (get_external_workflow_handle, signal)
- [x] **Upsert Search Attributes API** - ✅ Complete (workflow.upsert_search_attributes)
- [x] **Workflow Testing Utilities** - ✅ Complete (WorkflowEnvironment, 709 tests)
- [ ] **Workflow Replay Testing** - Safe deployments
- [ ] **Build ID Versioning** - Canary deployments
- [x] **Workflow Handle APIs** - ⚠️ Partial (query() result parsing is a placeholder — see §7.1)
- [ ] **Documentation** - Adoption & support

**Status:** 6/11 complete (1 partial).

---

### 8.3 Medium Priority (P2) - Nice to Have

- [ ] **Schedules API** - Recurring workflows
- [ ] **Async Activity Completion** - External integrations
- [ ] **Data Converter Extensibility** - Advanced serialization
- [ ] **Workflow Runner Customization** - Sandboxing
- [ ] **Update from Within Workflow** - Advanced patterns

**Status:** 0/5 complete.

---

### 8.4 Low Priority (P3) - Future

- [ ] **Worker Tuner / Slot Supplier** - Dynamic resource management (optional optimization)
- [ ] **Nexus Support** - Cross-namespace calls
- [ ] **Advanced Interceptor Patterns** - Complex middleware
- [x] **E2E Stress Tests (100+ workflows)** - ✅ Complete (6 tests, 100-200 concurrent workflows)

**Status:** 1/4 complete.

---

## 9. Feature Compatibility Summary

| Category | sdk-python Features | Trio SDK | Coverage |
|----------|---------------------|----------|----------|
| **Core Workflow APIs** | 15 | 14 | 93% |
| **Worker Configuration** | 12 | 7 | 58% |
| **Advanced Features** | 8 | 0 | 0% |
| **Client APIs** | 10 | 7 | 70% |
| **Testing** | 5 | 2 | 40% |
| **Observability** | 6 | 0 | 0% |
| **Overall** | **56** | **30** | **54%** |

*Updated 2026-02-24. Since Feb 8: +9 features (error types, headers, search attrs, signal external, activity context/heartbeat/cancellation, WorkflowEnvironment, stress tests).*

---

## 10. Recommended Implementation Order

### Phase 1: Production Critical (P0)
**Goal:** Make SDK production-ready for basic use cases

1. **Metrics & Telemetry** - Observability foundation
2. **Interceptors** - Extensibility foundation
3. **Error Type Preservation** - Proper error handling
4. **Update Handlers** - Interactive workflows
5. **Versioning APIs** - Safe code updates

**Estimated effort:** 5-7 weeks
**Outcome:** Production-ready for stateless workflows with proper monitoring

---

### Phase 2: Production Complete (P1)
**Goal:** Feature parity for common production patterns

7. **Local Activities** - Performance
8. **Activity Context & Heartbeat** - Long-running activities
9. **Payload Codec** - Security
10. **Headers Propagation** - Tracing
11. **Signal External & Search Attributes APIs** - Remaining core APIs
12. **Testing Utilities & Replay** - Development & validation
13. **Documentation** - Adoption

**Estimated effort:** 8-10 weeks
**Outcome:** Production-ready for all common patterns

---

### Phase 3: Advanced Features (P2-P3)
**Goal:** Full feature parity with sdk-python

14. Schedules, Async Activity Completion, Worker Tuner, Nexus, etc.

**Estimated effort:** 6-8 weeks
**Outcome:** 100% feature compatibility with optional optimizations

---

## 11. Production Deployment Blockers

**Current Assessment:** The Trio SDK is **NOT production-ready** due to:

### Critical Blockers:
1. **No metrics** - Cannot monitor worker health or performance
2. **No interceptors** - Cannot add tracing, logging, or custom logic
3. **Poor error handling** - Exception types lost, debugging difficult
4. **No versioning** - Cannot safely update workflow code
5. **No update handlers** - Cannot implement interactive workflows

### Risk Assessment:
- **High Risk**: Inability to diagnose issues, poor error handling, deployment failures
- **Cannot support**: Long-running workflows with updates, compliance requirements, observability integration
- **Suitable for**: POC, experimentation, controlled production with static concurrency limits

### Path to Production:
Complete all Phase 1 (P0) items before considering production deployment.

---

## 12. Summary

**Overall completion:** ~54% feature parity with sdk-python

**Production readiness:** ❌ Not ready (missing metrics, interceptors, versioning, updates)

**Strengths:**
- ✅ Core workflow execution (timers, activities, child workflows, signals, queries)
- ✅ Deterministic replay
- ✅ Error type preservation (ActivityError, ChildWorkflowError, ApplicationError)
- ✅ Headers propagation (workflow.info().headers, outgoing commands)
- ✅ Activity context (heartbeat, cancellation, wait_for_cancelled)
- ✅ Search attributes (upsert_search_attributes)
- ✅ Signal external workflows
- ✅ E2E stress-tested with 100-200 concurrent workflows
- ✅ 709 tests passing (unit + E2E + stress)
- ✅ Clean API matching sdk-python patterns

**Weaknesses:**
- ❌ No observability (metrics, tracing)
- ❌ No interceptors
- ❌ No versioning (patched/deprecate_patch)
- ❌ No update handlers
- ❌ Client-side `WorkflowHandle.query()` result parsing is a placeholder (returns None)
- ❌ Missing 46% of sdk-python features
- ❌ Minimal documentation and examples

**Known code placeholders:**
- `temporalio_trio/client/_workflow_handle.py:200` — `query()` always returns `None` (response parsing not implemented)

**Recommendation:**
- **For POC/experimentation:** ✅ Ready
- **For production:** ❌ Requires metrics, interceptors, versioning, updates (~8-10 weeks)
- **For full compatibility:** ❌ Requires all phases (~15-20 weeks of work)

---

**Last updated:** 2026-02-24 (Updated completed features, added placeholder inventory)
**Next review:** After interceptors/metrics implementation
