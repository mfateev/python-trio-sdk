# Comprehensive Audit Report: python-trio-sdk vs sdk-python

**Date:** 2026-02-24 (updated 2026-02-25)
**Scope:** Public API gap analysis, argument/option structure comparison, implementation propagation audit

## Fixes Applied

### P0 -- Silently Dropped Parameters (all fixed)

- **P0 #1** - `Client.start_workflow()`: `retry_policy`, `cron_schedule`, `memo`, `search_attributes`, `start_delay` now encoded in protobuf
- **P0 #2** - `Worker` config: all 9 dropped params now forwarded through bridge to sdk-core (Rust bridge rebuilt)
- **P0 #3** - `WorkflowHandle.query()`: `reject_condition` now propagated to bridge (Rust bridge rebuilt)

### P1 -- Critical Missing Features (all fixed)

- **P1 #4** - `workflow.Info`: 13 missing fields added (`namespace`, `attempt`, `start_time`, `execution_timeout`, `run_timeout`, `task_timeout`, `retry_policy`, `continued_run_id`, `cron_schedule`, `parent`, `root`, `raw_memo`, `priority`). Added `ParentInfo` and `RootInfo` dataclasses.
- **P1 #5** - `WorkflowHandle.result(follow_runs=True)`: follows continue-as-new chains. Handles timed-out and continued-as-new events.
- **P1 #6** - `WorkflowHandle.describe()`: Added with `WorkflowExecutionDescription` and `WorkflowExecutionStatus` (Rust bridge rebuilt)
- **P1 #7** - `WorkflowHandle.signal()`: Now accepts `str | Callable` with `arg`/`args` pattern
- **P1 #8** - `start_workflow()` return: `run_id` now correctly `None` on handle (tracks latest run)
- **P1 #9** - `result()` handles `workflow_execution_timed_out` and `workflow_execution_continued_as_new`

### P2 -- Important Missing Features (fixed)

- **Client.connect()**: Added `tls`, `rpc_metadata`, `default_workflow_query_reject_condition`, `retry_config`, `lazy` params. Added `identity` property.
- **Client.start_workflow()/execute_workflow()**: Changed to `arg`/`args` pattern. Timeouts now accept `timedelta | float`.
- **Worker**: Added `is_shutdown` property, async `shutdown()`, `config()` method, `workflow_failure_exception_types` param.
- **`@workflow.defn`**: Added `sandboxed`, `dynamic`, `failure_exception_types` params. `_Definition.name` now `Optional[str]` for dynamic workflows.
- **`@workflow.init`**: New decorator for workflow constructors.
- **`@workflow.signal`**: Added `unfinished_policy` param with `HandlerUnfinishedPolicy` enum.
- **`workflow.now()`**: Added convenience UTC datetime function.
- **`workflow.in_workflow()`**: Added context detection function.
- **`workflow.sleep()`**: Now accepts `timedelta` in addition to `float`.
- **`workflow.all_handlers_finished()`**: Added stub (returns True).
- **`workflow.execute_activity()`**: Added `result_type` param.
- **`workflow.start_child_workflow()`**: Added `cron_schedule`, `memo`, `search_attributes` params (propagated to bridge).
- **`workflow.continue_as_new()`**: Added `memo`, `search_attributes` params (propagated to bridge).
- **`ExternalWorkflowHandle.cancel()`**: Added with full bridge propagation.
- **`VersioningIntent` enum**: Added.
- **`NondeterminismError`, `ReadOnlyContextError`**: Added.
- **`_Definition`**: Added `ret_type`, `arg_types`, `init_fn`, `from_run_fn()`.
- **`_QueryDefinition`**: Added `ret_type`, `arg_types`.
- **`_SignalDefinition`**: Added `arg_types`, `unfinished_policy`.
- **`activity.raise_complete_async()`**: Added with `_CompleteAsyncError`.
- **`activity.metric_meter()`**: Added (returns noop meter).
- **`@activity.defn(dynamic=True)`**: Added dynamic activity support.
- **Testing `start_local()`**: Added `data_converter`, `ui`, `dev_server_database_filename`, `search_attributes`. Fixed param names to match sdk-python with backward-compat deprecation warnings.
- **`WorkflowHandle.query()`**: Uses client `default_workflow_query_reject_condition` as fallback.

### Additional P2 Fixes (second pass)

- **Client.start_workflow()**: Added `id_conflict_policy`, `static_summary`, `static_details`, `priority`, `start_signal`, `start_signal_args`, `result_type` params (all encoded in protobuf).
- **Local activity support**: Added `execute_local_activity()` with `ScheduleLocalActivityCommand`, full bridge propagation, and `local_retry_threshold` param.
- **Workflow utility functions**: Added `memo()`, `instance()`, `payload_converter()`, `get_current_details()`, `set_current_details()`.
- **Workflow info functions**: Added `get_current_build_id()`, `get_current_history_length()`, `get_current_history_size()`, `is_continue_as_new_suggested()` (stubs).
- **Replay-safe logger**: Added `LoggerAdapter` class and module-level `logger` that suppresses log messages during replay.
- **WorkflowHandle.fetch_history()**: Added with `WorkflowHistory` dataclass and pagination support (Rust bridge rebuilt).
- **WorkflowHandle.fetch_history_events()**: Added with automatic pagination.
- **WorkflowContinuedAsNewError**: Added proper error class for continue-as-new when `follow_runs=False`.
- **Client.list_workflows()**: Added with `WorkflowExecutionInfo` dataclass (Rust bridge rebuilt).
- **Client.count_workflows()**: Added (Rust bridge rebuilt).
- **Runtime class**: Added basic `Runtime` class with `default()`, `set_default()`, and `telemetry` property.

### Bug Fixes

- `WorkflowRuntime.workflow_id` was incorrectly set to `run_id`. Now uses `workflow_id` from `InitializeWorkflow` activation.

## Feature Inventory (Updated)

| # | Feature | Status |
|---|---------|--------|
| 1 | Client API | Good |
| 2 | Worker API | Good |
| 3 | Workflow API | Good |
| 4 | Activity API | Good |
| 5 | Testing API | Partial |
| 6 | Runtime/Telemetry API | Minimal |

---

## CRITICAL: Silently Dropped Parameters

These are the most dangerous bugs -- the public API accepts parameters that are **never propagated** to the bridge/core, giving users a false sense of configuration.

### Client.start_workflow() -- 5 params silently dropped

| Parameter | Accepted? | Encoded in Protobuf? |
|-----------|-----------|---------------------|
| `retry_policy` | Yes | **NO -- DROPPED** |
| `cron_schedule` | Yes | **NO -- DROPPED** |
| `memo` | Yes | **NO -- DROPPED** |
| `search_attributes` | Yes | **NO -- DROPPED** |
| `start_delay` | Yes | **NO -- DROPPED** |

Users passing `retry_policy=RetryPolicy(maximum_attempts=3)` get **no retry policy**. Cron workflows silently don't schedule. Memos and search attributes are lost.

### WorkflowHandle.query() -- reject_condition dropped

The `reject_condition` parameter is accepted but never sent to the bridge. Query rejection is silently ignored.

### Worker -- 9 config params silently dropped

| Parameter | Stored? | Passed to Bridge? |
|-----------|---------|-------------------|
| `nonsticky_to_sticky_poll_ratio` | Yes | **NO** |
| `max_concurrent_activity_task_polls` | Yes | **NO** |
| `max_activities_per_second` | Yes | **NO** |
| `max_task_queue_activities_per_second` | Yes | **NO** |
| `max_concurrent_workflow_tasks` | Yes | **NO** |
| `max_concurrent_activities` | Yes | **NO** |
| `max_concurrent_local_activities` | Yes | **NO** |
| `build_id` | Yes | **NO** |
| `graceful_shutdown_timeout` | Yes | **NO** |

---

## Feature 1: Client API

### Client.connect() -- Missing Parameters

| Missing Parameter | Severity |
|-------------------|----------|
| `tls` | High (production) |
| `api_key` | High (Temporal Cloud) |
| `retry_config` | High |
| `rpc_metadata` | Medium |
| `keep_alive_config` | Medium |
| `default_workflow_query_reject_condition` | Medium |
| `lazy` | Low |
| `runtime` | Low |
| `http_connect_proxy_config` | Low |

### Naming Divergences

- `target_url` should be `target_host`
- `WorkflowHandle.workflow_id` should be `id`
- `signal(signal_name=...)` should be `signal(signal=...)` accepting str or Callable

### Client.start_workflow() -- Missing Parameters

- `id_conflict_policy`, `static_summary`, `static_details`, `priority`
- `start_signal` / `start_signal_args` (signal-with-start)
- `result_type`, `rpc_metadata`, `rpc_timeout`
- Uses `*args` instead of sdk-python's `arg`/`args` pattern
- Uses `float` seconds for timeouts instead of `timedelta`

### WorkflowHandle -- Missing Methods

- `describe()` -- entirely missing
- `fetch_history()` / `fetch_history_events()` -- missing
- `execute_update()` / `start_update()` -- missing (entire update feature)
- `result()` -- missing `follow_runs` parameter (breaks continue-as-new)

### Missing Entirely from Client

- `list_workflows()`, `count_workflows()`
- `get_async_activity_handle()` (async activity completion)
- Schedule system (`create_schedule()`, etc.)
- Worker versioning APIs
- Interceptor framework
- `WorkflowHandle` generics (no type safety)
- `get_workflow_handle_for()` (typed variant)
- `service_client`, `workflow_service`, `operator_service`, `test_service` properties
- `config()` method, `identity` property
- `rpc_metadata` get/set property, `api_key` get/set property

### Missing Error Classes

- `WorkflowContinuedAsNewError`
- `WorkflowQueryFailedError`
- `WorkflowUpdateFailedError`
- `WorkflowUpdateRPCTimeoutOrCancelledError`
- `RPCTimeoutOrCancelledError`
- `AsyncActivityCancelledError`

### Missing Types

- `WorkflowExecution`, `WorkflowExecutionDescription`, `WorkflowExecutionStatus` (IntEnum)
- `WorkflowExecutionCount`, `WorkflowExecutionAsyncIterator`
- `WorkflowHistory`, `WorkflowHistoryEventFilterType`, `WorkflowHistoryEventAsyncIterator`
- `WorkflowUpdateHandle`, `WorkflowUpdateStage`
- `WithStartWorkflowOperation`
- `AsyncActivityHandle`, `AsyncActivityIDReference`
- All `*Input` interceptor dataclasses

### Implementation Issues

- `start_workflow()` return: sets `run_id` on handle (sdk-python intentionally leaves it `None`)
- `result()` does not handle `workflow_execution_timed_out` events
- `result()` does not handle `workflow_execution_continued_as_new` events
- Query does not extract `ret_type` from callable definition
- `terminate()` reason defaults to `"Terminated by client"` instead of `None`
- `terminate()` does not accept `*args` for termination details
- No `first_execution_run_id` support on WorkflowHandle

---

## Feature 2: Worker API

### Worker.__init__() -- Missing Parameters

| Missing Parameter | Severity |
|-------------------|----------|
| `interceptors` | Critical |
| `workflow_failure_exception_types` | High |
| `tuner` (WorkerTuner) | Medium |
| `on_fatal_error` | Medium |
| `use_worker_versioning` | Medium |
| `disable_eager_activity_execution` | Medium |
| `debug_mode` | Low |
| `activity_executor` | Expected (async-only) |
| `workflow_task_executor` | Expected (single-threaded) |
| `shared_state_manager` | Expected (async-only) |
| `disable_safe_workflow_eviction` | Expected (different model) |

### Worker -- Missing Methods/Properties

- `config()` -- no `WorkerConfig` TypedDict
- `is_shutdown` property
- `client` setter (update client mid-run)
- `shutdown()` is synchronous (sdk-python's is async and waits for completion)

### Missing Subsystems

- **Interceptors** -- entire `_interceptor.py` module (14+ types: `Interceptor`, `ActivityInboundInterceptor`, `ActivityOutboundInterceptor`, `WorkflowInboundInterceptor`, `WorkflowOutboundInterceptor`, all `*Input` dataclasses)
- **Replayer** -- entire `_replayer.py` module (`Replayer`, `ReplayerConfig`, `WorkflowReplayResult`, `WorkflowReplayResults`)
- **Tuning/Slot suppliers** -- entire `_tuning.py` module (`WorkerTuner`, `FixedSizeSlotSupplier`, `ResourceBasedSlotSupplier`, `CustomSlotSupplier`, `SlotPermit`, etc.)

### Structural Issues

- `data_converter` and `namespace` passed as separate Worker params instead of being read from `client.config()` (inconsistency with sdk-python)
- Low-level bridge types (`WorkflowActivation`, `ScheduleActivityCommand`, etc.) exported in public `__all__` -- sdk-python keeps these internal
- `SingleThreadWorker` and `TrioActivityWorker` exported publicly (sdk-python keeps internal workers private)
- `run()` does not call `bridge_worker.validate()` (commented out)
- No `finalize_shutdown()` call in shutdown path
- No `drain_poll_queue()` for error recovery
- No `wait_all_completed()` for activity completions
- Uses `trio.sleep(0.1)` hack for shutdown delay instead of proper drain

### Activity Worker Issues

- No dynamic activity support (`self._dynamic_activity`)
- No interceptor chain
- No complete-async support (`_CompleteAsyncError`)
- No activity drain on error
- No headers passed to activities

---

## Feature 3: Workflow API

### Missing Decorators/Features

- `@workflow.update` -- entire update subsystem missing
- `@workflow.update.validator` -- missing
- `@workflow.init` -- missing (allows `__init__` to accept same params as `@workflow.run`)
- `@defn` missing `sandboxed` param
- `@defn` missing `dynamic` param
- `@defn` missing `failure_exception_types` param
- `@signal` missing `unfinished_policy` param (`HandlerUnfinishedPolicy`)
- `@signal`/`@query` definition classes missing `arg_types`, `ret_type`, `dynamic_vararg`, `bind_fn()`, `must_name_from_fn_or_str()`

### execute_activity() -- Missing Parameters

| Missing Parameter | Severity |
|-------------------|----------|
| `result_type` | Medium |
| `versioning_intent` | Medium |
| `summary` | Low |
| `priority` | Low |

Note: All accepted params ARE correctly propagated to the bridge (good).

### start_child_workflow() -- Missing Parameters

| Missing Parameter | Severity |
|-------------------|----------|
| `cron_schedule` | Medium |
| `memo` | Medium |
| `search_attributes` | Medium |
| `versioning_intent` | Medium |
| `static_summary` | Low |
| `static_details` | Low |
| `priority` | Low |
| `result_type` | Medium |

Note: All accepted params ARE correctly propagated (good).

### continue_as_new() -- Missing Parameters

- `memo`
- `search_attributes`
- `versioning_intent`

Note: All accepted params ARE correctly propagated (good).

### Missing Local Activity Support

- `execute_local_activity()` -- entirely missing
- `start_local_activity()` -- entirely missing
- All class/method variants (`execute_local_activity_class()`, etc.)

### Missing Activity Handle Pattern

- `start_activity()` (start without awaiting) -- missing
- `ActivityHandle` class -- missing (extends `asyncio.Task` in sdk-python; trio equivalent needed)
- All class/method variants (`start_activity_class()`, `execute_activity_class()`, etc.)

### Workflow Info -- 13 of 17 fields missing

| Missing Field | Severity |
|---------------|----------|
| `namespace` | High |
| `attempt` | High |
| `start_time` | High |
| `task_timeout` | Medium |
| `retry_policy` | Medium |
| `execution_timeout` | Medium |
| `run_timeout` | Medium |
| `continued_run_id` | Medium |
| `cron_schedule` | Medium |
| `parent` (ParentInfo) | Medium |
| `root` (RootInfo) | Medium |
| `search_attributes` / `typed_search_attributes` | Medium |
| `raw_memo` | Medium |
| `priority` | Low |

Also missing: `ParentInfo`, `RootInfo`, `UpdateInfo` dataclasses. Trio `Info` is mutable (`@dataclass`) while sdk-python is frozen (`@dataclass(frozen=True)`).

### Missing Info Utility Functions

- `get_current_build_id()`
- `get_current_history_length()`
- `get_current_history_size()`
- `is_continue_as_new_suggested()`

### Missing Dynamic Handler Functions (13 functions)

- `get_signal_handler(name)` / `set_signal_handler(name, handler)`
- `get_dynamic_signal_handler()` / `set_dynamic_signal_handler(handler)`
- `get_query_handler(name)` / `set_query_handler(name, handler)`
- `get_dynamic_query_handler()` / `set_dynamic_query_handler(handler)`
- `get_update_handler(name)` / `set_update_handler(name, handler, validator)`
- `get_dynamic_update_handler()` / `set_dynamic_update_handler(handler, validator)`
- `all_handlers_finished()`

### Missing Utility Functions

- `now()` -- convenience UTC datetime
- `in_workflow()` -- context detection
- `instance()` -- get workflow instance
- `memo()` / `memo_value()` -- memo access
- `payload_converter()` -- converter access
- `metric_meter()` -- custom metrics
- `current_update_info()` -- update context info
- `get_current_details()` / `set_current_details()` -- workflow details
- `logger` / `LoggerAdapter` -- replay-safe logging
- `unsafe` class -- sandbox/replay utilities

### Missing Enums

- `VersioningIntent` (COMPATIBLE=1, DEFAULT=2)
- `HandlerUnfinishedPolicy` (WARN_AND_ABANDON=1, ABANDON=2)

### Missing Error Classes

- `NondeterminismError`
- `ReadOnlyContextError`

### ExternalWorkflowHandle -- Missing `cancel()` method

### sleep() -- Does not accept `timedelta` (only `float`)

### Minor Divergences

- `patched()` / `deprecate_patch()` param named `patch_id` instead of `id`
- `defn` skips `__dunder__` attributes (sdk-python uses `inspect.getmembers()`)
- `defn` does not check base classes for overridden signals/queries (sdk-python has MRO checking)
- Query decorator raises `ValueError` for async handlers (sdk-python issues deprecation warning)
- No type overloads (single function signatures, no IDE type inference)
- No `ChildWorkflowConfig`, `ActivityConfig`, `LocalActivityConfig` TypedDicts

---

## Feature 4: Activity API

This is the **best-implemented feature** with the fewest gaps.

### Missing Parameters on @defn

- `dynamic` -- dynamic activity support missing (unexpected)
- `no_thread_cancel_exception` -- expected (async-only)

### Missing Functions

- `raise_complete_async()` / `_CompleteAsyncError` -- async activity completion (unexpected gap: this is a general Temporal feature, not sync-specific)
- `metric_meter()` -- custom metrics in activities (unexpected)
- `wait_for_cancelled_sync()` -- expected (async-only)
- `wait_for_worker_shutdown_sync()` -- expected (async-only)
- `shield_thread_cancel_exception()` -- expected (async-only)

### Reverse Gap (trio has MORE than sdk-python)

- `activity.Info.retry_policy` field exists in trio-sdk but NOT in sdk-python. Should be verified against latest upstream.

### Implementation Issues

- Does not check `fn.__call__` for coroutine detection (classes with async `__call__` rejected)
- `_Definition.name` is non-optional `str` (should be `Optional[str]` for dynamic activities)

---

## Feature 5: Testing API

### WorkflowEnvironment -- Missing Features

| Missing Feature | Severity |
|-----------------|----------|
| `start_time_skipping()` | High (major testing feature) |
| `sleep(duration)` | Medium |
| `get_current_time()` | Medium |
| `supports_time_skipping` property | Medium |
| `auto_time_skipping_disabled()` context manager | Medium |
| `_AssertionErrorInterceptor` | Medium (test assertions in workflows won't properly fail) |

### start_local() -- 15 Missing Parameters

| Missing Parameter | Severity |
|-------------------|----------|
| `data_converter` | Medium |
| `interceptors` | Medium |
| `default_workflow_query_reject_condition` | Low |
| `retry_config` | Low |
| `rpc_metadata` | Low |
| `identity` | Low |
| `tls` | Low |
| `download_dest_dir` | Low |
| `ui` | Low |
| `runtime` | Low |
| `search_attributes` | Low |
| `dev_server_database_filename` | Low |
| `dev_server_log_format` | Low |
| `dev_server_download_version` | Low |
| `dev_server_download_ttl` | Low |

### start_local() -- Parameter Naming Divergences

- `temporal_cli_path` should be `dev_server_existing_path`
- `log_level` should be `dev_server_log_level`
- `extra_args` should be `dev_server_extra_args`

### ActivityEnvironment

- `metric_meter` attribute missing (activity code calling `activity.metric_meter()` will fail)

### Implementation Differences

- Uses `subprocess.Popen` for server management instead of bridge's `EphemeralServer` (no auto-download, version management)
- `shutdown()` closes client (sdk-python base class does NOT close client)
- `from_client()` does not add `_AssertionErrorInterceptor`

---

## Feature 6: Runtime/Telemetry API

### Missing Entirely

- `Runtime` class -- the central runtime object does not exist
- `LoggingConfig` / `LogForwardingConfig` -- no Core log forwarding
- `TelemetryFilter` -- no per-component log levels
- `MetricBuffer` and all buffered metric types
- `MetricBufferDurationFormat` enum
- `BufferedMetric`, `BufferedMetricUpdate` protocols
- `OpenTelemetryMetricTemporality` enum
- All internal metric classes (`_MetricMeter`, `_MetricCounter`, `_MetricHistogram`, `_MetricGauge`, etc.)

### Config Divergences

- `OpenTelemetryConfig.metric_periodicity` uses raw `int` millis instead of `timedelta`
- `OpenTelemetryConfig.metric_temporality` uses `bool` instead of `OpenTelemetryMetricTemporality` enum
- `TelemetryConfig.metrics` does not accept `MetricBuffer` option
- `TelemetryConfig.logging` field entirely missing
- Telemetry configs serialize to JSON (`_to_json_dict()`) rather than bridge config objects (`_to_bridge_config()`)

---

## Correctly Implemented (No Issues)

These features are properly implemented with all parameters propagated to the bridge:

- `workflow.execute_activity()` -- all params reach the bridge protobuf
- `workflow.start_child_workflow()` -- all params reach the bridge protobuf
- `workflow.continue_as_new()` -- all params reach the bridge protobuf
- `workflow.upsert_search_attributes()` -- properly encoded via typed converter
- `WorkflowHandle.signal()` -- all params propagated
- `WorkflowHandle.cancel()` -- all params propagated
- `WorkflowHandle.terminate()` -- all params propagated
- Activity heartbeat/cancellation -- bidirectional, correct
- `activity.Info` fields -- all 17 common fields match (plus trio has extra `retry_policy`)
- `activity.LoggerAdapter` -- identical implementation
- Workflow `time()`, `time_ns()`, `random()`, `uuid4()` -- correct
- Workflow `wait_condition()` -- correct
- Workflow `patched()`, `deprecate_patch()` -- correct (minor param name difference)
- `ActivityCancellationType`, `ChildWorkflowCancellationType`, `ParentClosePolicy` enums -- correct values

---

## Priority Fix Recommendations

### P0 -- Fix Silent Drops (dangerous bugs)

1. Propagate `retry_policy`, `cron_schedule`, `memo`, `search_attributes`, `start_delay` in `Client.start_workflow()` to the protobuf request
2. Propagate all 9 Worker config params to the bridge (`nonsticky_to_sticky_poll_ratio`, `max_concurrent_activity_task_polls`, `max_activities_per_second`, `max_task_queue_activities_per_second`, `max_concurrent_workflow_tasks`, `max_concurrent_activities`, `max_concurrent_local_activities`, `build_id`, `graceful_shutdown_timeout`)
3. Propagate `reject_condition` in `WorkflowHandle.query()` to the bridge

### P1 -- Critical Missing Features

4. Add missing `workflow.Info` fields (at least `namespace`, `attempt`, `start_time`, `task_timeout`, `execution_timeout`, `run_timeout`)
5. Add `follow_runs` to `WorkflowHandle.result()`
6. Add `WorkflowHandle.describe()`
7. Fix naming divergences (`target_host`, `handle.id`, `signal` param)
8. Fix `start_workflow()` return handle: don't set `run_id` (should be `None`)
9. Handle `workflow_execution_timed_out` and `workflow_execution_continued_as_new` in `result()`

### P2 -- Important Missing Features

10. Local activity support (`execute_local_activity()`, `start_local_activity()`)
11. Workflow update support (`@workflow.update`, `execute_update()`, `start_update()`)
12. Interceptor framework
13. Time-skipping test environment (`start_time_skipping()`)
14. `workflow_failure_exception_types` on Worker
15. `raise_complete_async()` for async activity completion

### P3 -- Completeness

16. Replayer (`Replayer`, `ReplayerConfig`)
17. Tuning/slot suppliers (`WorkerTuner`, `FixedSizeSlotSupplier`, etc.)
18. `Runtime` class
19. Worker versioning (`use_worker_versioning`, `build_id` propagation)
20. Schedule support
21. `list_workflows()`, `count_workflows()`
22. Logging config, metric buffer
23. Dynamic workflows/activities
24. All remaining workflow utility functions
