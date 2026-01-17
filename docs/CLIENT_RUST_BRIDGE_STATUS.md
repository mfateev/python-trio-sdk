# Client Rust Bridge Implementation Status

**Date**: 2026-01-17
**Status**: In Progress - Rust bridge layer

## Goal

Implement a pure Trio client with **NO asyncio dependency**, using the Rust bridge for all SDK Core operations.

## Architecture

```
Client (Python/Trio)
    ↓
TrioBridgeWrapper (Python/Trio)
    ↓
TrioAsyncBridge (Rust/PyO3)
    ↓
CoreClientHandle (Rust)
    ↓
RetryClient (SDK Core)
    ↓
Temporal Server
```

## Current Progress

### ✅ Completed

1. **Created `core_client.rs`**: New Rust module for client operations
   - `CoreClientHandle` struct
   - Methods: `initialize()`, `start_workflow_execution()`, `get_workflow_result()`, `cancel_workflow()`, `terminate_workflow()`, `query_workflow()`, `signal_workflow()`
   - File: `temporalio_trio_bridge/src/core_client.rs`

2. **Added to lib.rs**: Exposed new module
   - Added `mod core_client;`
   - Export: `pub use core_client::CoreClientHandle;`

3. **Extended bridge.rs**: Added client operations to bridge
   - Updated `new()` to create both `CoreWorkerHandle` and `CoreClientHandle`
   - Updated `rust_event_loop()` to pass client handle
   - Updated `process_request_async()` to accept client handle
   - Added client operations to `handle_operation()` match:
     - `initialize_client`
     - `start_workflow`
     - `get_workflow_result`
     - `cancel_workflow`
     - `terminate_workflow`
     - `query_workflow`
     - `signal_workflow`

### 🔧 Needs Fixing - Compilation Errors

**Error 1: Type mismatch in RetryClient**
```rust
error[E0308]: mismatched types
  --> src/core_client.rs:78:30
   |
78 |         *client_guard = Some(retry_client);
   |                         ---- ^^^^^^^^^^^^ expected `RetryClient<Client>`, found `RetryClient<RetryClient<...>>`
```

**Issue**: `connect_no_namespace()` already returns a `RetryClient`, we're double-wrapping it.

**Fix**: Change line 71 in `core_client.rs`:
```rust
// Current (wrong):
let retry_client = RetryClient::new(client, Default::default());

// Should be:
let client_inner = client; // client is already a RetryClient
```

Actually, looking at worker code, we should store the raw client, not wrap it:
```rust
pub struct CoreClientHandle {
    client: Arc<Mutex<Option<temporalio_client::Client>>>,  // Not RetryClient!
    // ...
}
```

**Error 2: Missing trait imports**
```rust
error[E0599]: no method named `start_workflow` found for reference `&RetryClient<Client>`
```

**Fix**: Add trait imports at top of `core_client.rs`:
```rust
use temporalio_client::WorkflowClientTrait;
use temporalio_client::WorkflowService;
```

**Error 3: Wrong method names**
- `start_workflow` → `start_workflow_execution`
- `query_workflow` → `query_workflow_execution`
- etc.

**Fix**: Use correct method names from traits.

## Remaining Work

###  1. Fix Rust Compilation (2-3 hours)
- [ ] Fix `RetryClient` type issue
- [ ] Add necessary trait imports
- [ ] Use correct method names
- [ ] Handle protobuf conversions correctly
- [ ] Build successfully: `cargo build --release`
- [ ] Install bridge: `maturin develop --release`

### 2. Python Bridge Wrapper (2 hours)
- [ ] Add client methods to `TrioBridgeWrapper` in `_async_bridge.py`:
  - `initialize_client_with_config()`
  - `start_workflow_execution()`
  - `get_workflow_result()`
  - `cancel_workflow_execution()`
  - `terminate_workflow_execution()`
  - `query_workflow_execution()`
  - `signal_workflow_execution()`

### 3. Python Client Implementation (3-4 hours)
- [ ] Create `temporalio_trio/client/` directory
- [ ] Implement `Client` class:
  - `connect()`: Create bridge, initialize client
  - `start_workflow()`: Convert args to protobuf, call bridge
  - `execute_workflow()`: Start + wait for result
  - `get_workflow_handle()`: Return handle
  - Properties: `namespace`, `identity`, `data_converter`
- [ ] Implement `WorkflowHandle` class:
  - `result()`: Poll for workflow completion via bridge
  - `query()`: Call bridge query method
  - `signal()`: Call bridge signal method
  - `cancel()`: Call bridge cancel method
  - `terminate()`: Call bridge terminate method
- [ ] Handle protobuf serialization/deserialization
- [ ] Use `temporalio.converter.DataConverter` for payloads
- [ ] Properties: `workflow_id`, `run_id`

### 4. Testing (2-3 hours)
- [ ] Write unit tests mocking bridge
- [ ] Write integration tests with real server
- [ ] Test all client operations
- [ ] Test error handling
- [ ] Ensure all 166 existing tests still pass

### 5. Documentation (1 hour)
- [ ] Update README with client example
- [ ] Create `examples/client_example.py`
- [ ] Update `CLIENT_USAGE_COMPARISON.md`
- [ ] Document pure Trio architecture

## Key Design Decisions

1. **No asyncio dependency**: All operations go through Rust bridge
2. **Same API as SDK**: `Client.connect()`, `start_workflow()`, etc.
3. **Protobuf all the way**: Python ↔ bytes ↔ Rust ↔ SDK Core
4. **Data converter reuse**: Still use `temporalio.converter.DataConverter` for payload serialization (this is runtime-agnostic)

## References

- Worker implementation: `temporalio_trio/worker/_worker.py`
- Bridge wrapper: `temporalio_trio/_async_bridge.py`
- Rust bridge: `temporalio_trio_bridge/src/bridge.rs`
- SDK Core client API: `sdk-core/crates/client/src/lib.rs`

## Next Steps

1. Fix Rust compilation errors
2. Build and install bridge
3. Implement Python wrapper methods
4. Implement Client and WorkflowHandle
5. Test with real Temporal server

Estimated total remaining: **10-13 hours**
