# Gap Analysis: Feature Compatibility with sdk-python

**Date:** 2026-03-01 (Updated)
**Author:** Claude Code Analysis
**Scope:** Full feature parity with sdk-python

---

## Executive Summary

This document tracks all gaps between the Trio SDK and the official Temporal Python SDK (sdk-python).

**Current Status:**
- **Core workflow execution**: ✅ Complete
- **Worker features**: ✅ Complete (interceptors, replayer, error handling, local activities)
- **Workflow API**: ✅ Complete (updates, patching, signals, queries, child workflows)
- **Client API**: ⚠️ ~85% (core operations done, advanced subsystems missing)
- **Testing**: ⚠️ Partial (WorkflowEnvironment done, time-skipping missing)
- **Observability**: ⚠️ Basic (Runtime/TelemetryConfig exist, advanced features missing)

**Assessment:** SDK core features are production-ready. Remaining gaps are in client-side advanced subsystems (schedules, external activities, pagination) and advanced telemetry.

**Test count:** 773 tests passing (unit + E2E)

---

## Completed Since Last Review (2026-02-24)

All previously-P0/P1/P2 features are now implemented:

- ✅ **@workflow.update** - Full subsystem with concurrent async handlers, validators, `current_update_info()`, `all_handlers_finished()`
- ✅ **Interceptors** - Full framework: `Interceptor`, `WorkflowInboundInterceptor`, `WorkflowOutboundInterceptor`, `ActivityInboundInterceptor`, `ActivityOutboundInterceptor` with all `*Input` dataclasses
- ✅ **Replayer** - `replay_workflow()`, `replay_workflows()`, `workflow_replay_iterator()`, `WorkflowHistory.from_json()`
- ✅ **Versioning/Patching** - `workflow.patched()`, `workflow.deprecate_patch()`
- ✅ **Local activities** - `execute_local_activity()` with full bridge propagation
- ✅ **Comprehensive API audit** - All P0 silent drops fixed, all naming divergences aligned
- ✅ **WorkflowHandle.describe()** - With `WorkflowExecutionDescription`, `WorkflowExecutionStatus`
- ✅ **WorkflowHandle.fetch_history()** - Full history with pagination
- ✅ **WorkflowHandle.execute_update()/start_update()** - Full update client support
- ✅ **Client.list_workflows()** - With `WorkflowExecutionInfo`
- ✅ **Client.count_workflows()** - Query-based counting
- ✅ **workflow.Info** - All 17 fields including `ParentInfo`, `RootInfo`
- ✅ **WorkflowHandle.result(follow_runs=True)** - Follows continue-as-new chains
- ✅ **Error type preservation** - ActivityError, ChildWorkflowError, ApplicationError
- ✅ **Runtime class** - Basic `Runtime` with `TelemetryConfig`, `PrometheusConfig`, `OpenTelemetryConfig`
- ✅ **Worker** - `is_shutdown`, async `shutdown()`, `config()`, `workflow_failure_exception_types`
- ✅ **Shared client/worker bridge** - Worker reuses Client's bridge and gRPC connection

---

## 1. Client API Gaps

### 1.1 Schedule System ❌ NOT IMPLEMENTED

**Status:** Entire subsystem missing.

**What's missing:**
- `Client.create_schedule()` - Create a schedule
- `Client.get_schedule_handle()` - Get handle to existing schedule
- `Client.list_schedules()` - List schedules (returns `ScheduleAsyncIterator`)
- `ScheduleHandle` class with: `describe()`, `delete()`, `pause()`, `unpause()`, `trigger()`, `backfill()`, `update()`
- `Schedule`, `ScheduleSpec`, `ScheduleCalendarSpec`, `ScheduleIntervalSpec`, `ScheduleRange` dataclasses
- `ScheduleActionStartWorkflow`, `SchedulePolicy`, `ScheduleState`, `ScheduleDescription`, `ScheduleInfo` dataclasses
- `ScheduleBackfill`, `ScheduleUpdateInput`, `ScheduleUpdate` dataclasses
- `ScheduleOverlapPolicy` enum

**Priority: P2** - Important for recurring workflows but can be managed via Temporal UI or CLI.

---

### 1.2 External Activity API ❌ NOT IMPLEMENTED

**Status:** Entire subsystem missing. These are client-side APIs for starting and managing activities externally (outside of workflows).

**What's missing:**
- `Client.start_activity()` / `Client.execute_activity()` - Start activity from client
- `Client.list_activities()` / `Client.count_activities()` - List/count activities
- `Client.get_activity_handle()` - Get handle to external activity
- `ActivityHandle` class with: `result()`, `describe()`, `cancel()`, `terminate()`
- `ActivityExecutionDescription`, `ActivityExecutionAsyncIterator` types

**Priority: P3** - Rarely used; most activities are started from within workflows.

---

### 1.3 AsyncActivityHandle ❌ NOT IMPLEMENTED

**Status:** Missing. This is for manual (async) activity completion from external systems.

**What's missing:**
- `Client.get_async_activity_handle()` - Get handle by workflow_id/activity_id or task_token
- `AsyncActivityHandle` class with: `heartbeat()`, `complete()`, `fail()`, `report_cancellation()`

**Note:** `activity.raise_complete_async()` IS implemented on the workflow side. Only the client-side completion handle is missing.

**Priority: P2** - Important for long-running external integrations.

---

### 1.4 Update-with-Start ❌ NOT IMPLEMENTED

**Status:** Missing. A newer sdk-python feature that combines update and start in one RPC.

**What's missing:**
- `Client.execute_update_with_start_workflow()`
- `Client.start_update_with_start_workflow()`
- `WithStartWorkflowOperation` class

**Priority: P3** - Newer feature, less common pattern.

---

### 1.5 Client.connect() Missing Parameters ⚠️ PARTIAL

**What's implemented:** `target_url`, `namespace`, `identity`, `data_converter`, `tls` (bool), `rpc_metadata`, `default_workflow_query_reject_condition`, `retry_config`, `lazy`

**What's missing:**

| Missing Parameter | sdk-python Type | Severity | Notes |
|-------------------|----------------|----------|-------|
| `api_key` | `str \| None` | High | Required for Temporal Cloud |
| `keep_alive_config` | `KeepAliveConfig` | Medium | HTTP/2 keep-alive tuning |
| `runtime` | `Runtime \| None` | Medium | Share runtime across clients |
| `http_connect_proxy_config` | `HttpConnectProxyConfig` | Low | Proxy support |
| `plugins` | `Sequence[Plugin]` | Low | Plugin system |
| `interceptors` | `Sequence[Interceptor]` | Medium | Client-side interceptors (see §1.8) |
| `header_codec_behavior` | `HeaderCodecBehavior` | Low | Header codec control |

**Priority: P1** - `api_key` is needed for Temporal Cloud users.

---

### 1.6 TLS Configuration ⚠️ BOOLEAN ONLY

**What's implemented:** `tls: bool = False`

**What's missing:** Full `TLSConfig` dataclass:
- `server_root_ca_cert: bytes | None` - Custom CA certificate
- `domain: str | None` - TLS domain override
- `client_cert: bytes | None` - Client certificate for mTLS
- `client_private_key: bytes | None` - Client private key for mTLS

**Priority: P1** - mTLS is required for many production deployments and Temporal Cloud.

---

### 1.7 Per-Call RPC Control ❌ NOT IMPLEMENTED

**Status:** sdk-python allows `rpc_metadata` and `rpc_timeout` on every individual operation (start_workflow, signal, query, cancel, terminate, describe, etc.). Trio SDK only has a `timeout` parameter on some handle methods.

**What's missing on every handle/client operation:**
- `rpc_metadata: Mapping[str, str]` - Per-call metadata headers
- `rpc_timeout: timedelta | None` - Per-call RPC timeout

**Priority: P2** - Most users rely on client-level defaults.

---

### 1.8 Client-Side Interceptors ❌ NOT IMPLEMENTED

**Status:** Worker-side interceptors (workflow + activity) are implemented. Client-side `OutboundInterceptor` is not.

**What's missing:**
- `OutboundInterceptor` class - intercepts client operations (start_workflow, cancel_workflow, signal_workflow, query_workflow, etc.)
- `interceptors` parameter on `Client.connect()`

**Priority: P2** - Worker-side interceptors cover most use cases (tracing, logging).

---

### 1.9 Pagination / Async Iterators ⚠️ SIMPLIFIED

**Status:** List operations return plain lists instead of async iterators with pagination.

| Method | sdk-python Returns | Trio SDK Returns |
|--------|-------------------|-----------------|
| `list_workflows()` | `WorkflowExecutionAsyncIterator` (paginated) | `list[WorkflowExecutionInfo]` |
| `count_workflows()` | `WorkflowExecutionCount` (with groups) | `int` |
| `fetch_history_events()` | `WorkflowHistoryEventAsyncIterator` | `list[Any]` |

**What's missing:**
- `WorkflowExecutionAsyncIterator` with `current_page`, `next_page_token`, `fetch_next_page()`
- `WorkflowExecutionCount` dataclass with count groups
- `WorkflowHistoryEventAsyncIterator` for streaming
- Pagination params: `limit`, `page_size`, `next_page_token` on list operations

**Priority: P2** - Plain lists work for moderate-scale use; pagination needed for large namespaces.

---

### 1.10 Raw Service Access ❌ NOT IMPLEMENTED

**Status:** Missing. sdk-python exposes raw gRPC services for advanced use.

**What's missing:**
- `Client.service_client` property (`ServiceClient`)
- `Client.workflow_service` property (`WorkflowService`)
- `Client.operator_service` property (`OperatorService`)
- `Client.test_service` property (`TestService`)
- `ServiceClient.check_health()` method
- Mutable `Client.api_key` and `Client.rpc_metadata` properties

**Priority: P3** - Advanced use case for direct gRPC access.

---

### 1.11 Missing Client Methods

| Method | Description | Priority |
|--------|-------------|----------|
| `Client.config()` | Returns `ClientConfig` TypedDict | P3 |
| `Client.get_workflow_handle_for()` | Typed workflow handle from class | P3 |
| `WorkflowHandle.get_update_handle()` | Get handle to existing update by ID | P2 |
| `WorkflowHandle.get_update_handle_for()` | Typed update handle | P3 |

---

### 1.12 Build ID / Versioning Client APIs ❌ NOT IMPLEMENTED

**Status:** Worker accepts `build_id` parameter. Client-side versioning APIs missing.

**What's missing:**
- `Client.update_worker_build_id_compatibility()`
- `Client.get_worker_build_id_compatibility()`
- `Client.get_worker_task_reachability()`
- `start_workflow(versioning_override=...)` parameter

**Priority: P3** - Worker-level `build_id` is sufficient for basic versioning.

---

### 1.13 Missing Error Classes

| Error Class | Description | Priority |
|-------------|-------------|----------|
| `WorkflowQueryFailedError` | Query handler raised an exception | P2 |
| `WorkflowUpdateFailedError` | Update handler raised an exception | P2 |
| `WorkflowUpdateRPCTimeoutOrCancelledError` | Update RPC timed out | P3 |
| `ActivityFailureError` | Activity execution failed (client-side) | P3 |

**Note:** `WorkflowFailureError`, `WorkflowQueryRejectedError`, `WorkflowContinuedAsNewError` ARE implemented.

---

## 2. Worker Gaps

### 2.1 Worker Tuner / Slot Supplier ❌ NOT IMPLEMENTED

**Status:** Static concurrency limits work. Dynamic tuning not implemented.

**What's missing:**
- `WorkerTuner`, `FixedSizeSlotSupplier`, `ResourceBasedTuner`, `ResourceBasedSlotSupplier`
- `CustomSlotSupplier`, `SlotPermit`
- `worker_tuner` parameter on Worker

**Current alternative:** `max_concurrent_workflow_tasks`, `max_concurrent_activities`, `max_concurrent_local_activities` params (all working).

**Priority: P3** - Static limits are sufficient for most deployments.

---

### 2.2 Workflow Sandbox ❌ NOT IMPLEMENTED

**Status:** Not applicable in the same way as sdk-python. Trio's structured concurrency provides natural isolation.

**What's missing:**
- `RestrictedWorkflowRunner` / sandboxing
- Import restrictions for workflow code
- `@workflow.defn(sandboxed=True)` enforcement

**Priority: P3** - Trio's deterministic scheduling largely addresses the same concerns.

---

## 3. Observability Gaps

### 3.1 Telemetry ⚠️ BASIC

**What's implemented:**
- `Runtime` class with `default()`, `set_default()`, `telemetry` property
- `TelemetryConfig` with `metrics` field
- `PrometheusConfig` with `bind_address`
- `OpenTelemetryConfig` with `url`, `metric_periodicity`, `metric_temporality`
- `workflow.metric_meter()` and `activity.metric_meter()` (return noop meters)

**What's missing:**

| Feature | Description | Priority |
|---------|-------------|----------|
| `LoggingConfig` / `LogForwardingConfig` | Core log forwarding to Python | P2 |
| `TelemetryFilter` | Per-component log levels | P3 |
| `MetricBuffer` | Buffered metric collection | P3 |
| Functional `metric_meter()` | Currently returns noop meter | P2 |
| `OpenTelemetryMetricTemporality` enum | Proper enum vs bool | P3 |
| `_MetricMeter`, `_MetricCounter`, etc. | Internal metric implementations | P2 |

---

### 3.2 Payload Codec ❌ NOT IMPLEMENTED

**Status:** Not implemented.

**What's missing:**
- `PayloadCodec` interface
- Encryption codec support
- Compression codec support
- Codec integration with `DataConverter`

**Priority: P2** - Important for security-sensitive deployments (GDPR, HIPAA).

---

## 4. Testing Gaps

### 4.1 Time-Skipping Test Environment ❌ NOT IMPLEMENTED

**Status:** `WorkflowEnvironment.start_local()` starts a real dev server. Time-skipping not supported.

**What's missing:**
- `WorkflowEnvironment.start_time_skipping()` - Uses test server with time manipulation
- `env.sleep(duration)` - Advance time in test environment
- `env.get_current_time()` - Get simulated time
- `env.supports_time_skipping` property
- `auto_time_skipping_disabled()` context manager

**Priority: P2** - Real server works for E2E tests; time-skipping needed for fast timer-heavy tests.

---

## 5. Documentation & Examples

### 5.1 Documentation ⚠️ MINIMAL

**What exists:** Architecture docs, bridge design, POC results, E2E testing guide, implementation plan.

**What's needed:**
- Comprehensive API reference
- Migration guide from sdk-python
- Production deployment guide
- Troubleshooting documentation

**Priority: P2** - Required for community adoption.

---

## 6. Production Readiness Checklist

### 6.1 Critical (P0) - Required for Production ✅ ALL COMPLETE

- [x] **Error Type Preservation** - ActivityError, ChildWorkflowError, ApplicationError
- [x] **Interceptors** - Full workflow + activity interceptor framework
- [x] **Update Handlers** - @workflow.update with concurrent handlers, validators
- [x] **Versioning (patched/deprecate_patch)** - Safe code updates
- [x] **Replayer** - Workflow history replay and nondeterminism detection

**Status:** 5/5 complete.

---

### 6.2 High Priority (P1) - Important for Production

- [x] **Local Activities** - `execute_local_activity()` with full bridge propagation
- [x] **Activity Context & Heartbeat** - heartbeat, cancellation, info, wait_for_cancelled
- [x] **Headers Propagation** - workflow.info().headers, outgoing commands
- [x] **Signal External Workflow** - get_external_workflow_handle, signal
- [x] **Upsert Search Attributes** - workflow.upsert_search_attributes
- [x] **Workflow Testing Utilities** - WorkflowEnvironment
- [x] **Workflow Replay Testing** - Replayer with nondeterminism detection
- [x] **WorkflowHandle APIs** - query, describe, result(follow_runs), update, fetch_history
- [ ] **TLS/mTLS Configuration** - Full TLSConfig with client certs (§1.6)
- [ ] **Temporal Cloud support** - `api_key` parameter on Client.connect() (§1.5)
- [x] **Worker config propagation** - All params forwarded to bridge

**Status:** 10/12 complete.

---

### 6.3 Medium Priority (P2) - Nice to Have

- [ ] **Schedules API** - Recurring workflow management (§1.1)
- [ ] **Async Activity Completion** - Client-side AsyncActivityHandle (§1.3)
- [ ] **Payload Codec** - Encryption & compression (§3.2)
- [ ] **Client-side interceptors** - OutboundInterceptor (§1.8)
- [ ] **Pagination / async iterators** - Large-scale list operations (§1.9)
- [ ] **Per-call RPC control** - rpc_metadata/rpc_timeout per operation (§1.7)
- [ ] **Time-skipping tests** - start_time_skipping() (§4.1)
- [ ] **Functional metrics** - Non-noop metric_meter() (§3.1)
- [ ] **Missing error classes** - WorkflowQueryFailedError, WorkflowUpdateFailedError (§1.13)
- [ ] **WorkflowHandle.get_update_handle()** - Get existing update handle (§1.11)
- [ ] **Documentation** - API reference, migration guide (§5.1)

**Status:** 0/11 complete.

---

### 6.4 Low Priority (P3) - Future

- [ ] **Worker Tuner / Slot Supplier** - Dynamic resource management (§2.1)
- [ ] **Nexus Support** - Cross-namespace calls
- [ ] **External Activity API** - Client-side activity management (§1.2)
- [ ] **Update-with-start** - Combined update+start RPC (§1.4)
- [ ] **Build ID client APIs** - Versioning management from client (§1.12)
- [ ] **Raw service access** - gRPC service properties (§1.10)
- [ ] **Workflow sandbox** - RestrictedWorkflowRunner (§2.2)
- [ ] **Advanced telemetry** - LoggingConfig, MetricBuffer, TelemetryFilter (§3.1)
- [x] **E2E Stress Tests** - 100-200 concurrent workflows

**Status:** 1/9 complete.

---

## 7. Feature Compatibility Summary

| Category | sdk-python Features | Trio SDK | Coverage |
|----------|---------------------|----------|----------|
| **Core Workflow APIs** | 18 | 18 | 100% |
| **Worker Configuration** | 14 | 12 | 86% |
| **Client Core Operations** | 12 | 12 | 100% |
| **Client Advanced APIs** | 10 | 2 | 20% |
| **Interceptors** | 4 types | 4 types | 100% (worker-side) |
| **Testing** | 4 | 2 | 50% |
| **Observability** | 8 | 3 | 38% |
| **Overall** | **70** | **53** | **76%** |

*Updated 2026-03-01. Since Feb 24: +23 features (updates, interceptors, replayer, local activities, API audit fixes, shared bridge).*

---

## 8. Remaining Implementation Order

### Phase 1: Production Connectivity (P1)
**Goal:** Support Temporal Cloud and mTLS deployments

1. **Full TLSConfig** - mTLS with client certs
2. **api_key parameter** - Temporal Cloud authentication

**Outcome:** SDK works with all Temporal deployment types

---

### Phase 2: Client Completeness (P2)
**Goal:** Feature parity for common client patterns

3. **Schedules API** - Full schedule CRUD
4. **AsyncActivityHandle** - Manual activity completion
5. **Client-side interceptors** - OutboundInterceptor
6. **Pagination** - Async iterators for list operations
7. **Payload Codec** - Encryption/compression
8. **Functional metrics** - Non-noop metric meters
9. **Time-skipping tests** - Test server integration

**Outcome:** Full client-side feature parity for common patterns

---

### Phase 3: Advanced Features (P3)
**Goal:** 100% feature parity

10. Worker Tuner, Nexus, external activities, update-with-start, etc.

**Outcome:** Complete feature compatibility

---

## 9. Summary

**Overall completion:** ~76% feature parity with sdk-python

**Production readiness:** ✅ Ready for most use cases (with caveats below)

**Strengths:**
- ✅ Complete workflow execution (timers, activities, child workflows, signals, queries, updates)
- ✅ Deterministic replay with patched()/deprecate_patch()
- ✅ Full interceptor framework (workflow + activity)
- ✅ Replayer with nondeterminism detection
- ✅ Error type preservation (ActivityError, ChildWorkflowError, ApplicationError)
- ✅ Headers propagation and search attributes
- ✅ Local activities
- ✅ Shared client/worker bridge (matching sdk-python architecture)
- ✅ 773 tests passing (unit + E2E)
- ✅ Basic telemetry (Runtime, PrometheusConfig, OpenTelemetryConfig)

**Remaining gaps:**
- ❌ No schedule management from client (use Temporal UI/CLI)
- ❌ No mTLS / Temporal Cloud support (tls is boolean only)
- ❌ No client-side interceptors (worker-side interceptors work)
- ❌ No async activity completion handle
- ❌ No pagination on list APIs (plain lists only)
- ❌ No time-skipping test environment
- ❌ Metric meters are noop stubs

**Recommendation:**
- **For development/testing:** ✅ Ready
- **For self-hosted production:** ✅ Ready (all core features complete)
- **For Temporal Cloud:** ❌ Needs `api_key` and `TLSConfig` support
- **For full compatibility:** Remaining ~24% is advanced client features and polish

---

**Last updated:** 2026-03-01
**Next review:** After TLS/Cloud connectivity implementation
