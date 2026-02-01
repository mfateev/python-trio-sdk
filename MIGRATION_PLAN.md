# Migration Plan: Trio SDK to Fully Async Bridge

## Executive Summary

Migrate from trio-asyncio-based bridge to the fully async architecture proven in `poc_async_bridge.py`. This eliminates the asyncio dependency while maintaining true async semantics on both Rust and Trio sides.

**Scope:** 4 files, ~13 locations to modify
**Timeline:** 5 phases, estimated 2-3 weeks
**Risk:** Medium - Bridge is critical path for workflow execution

---

## Current State Analysis

### trio-asyncio Usage

**Total instances:** 13 across 4 files

| File | trio-asyncio Uses | Purpose |
|------|-------------------|---------|
| `worker/_worker.py` | 4 | Worker initialization & lifecycle |
| `bridge_worker.py` | 5 | Polling & completion operations |
| `bridge_worker_example.py` | 2 | Example client connection |
| `pyproject.toml` | 1 | Dependency declaration |

### Critical Operations Using trio-asyncio

1. **Bridge Worker Polling** (`bridge_worker.py:116`) - **CRITICAL PATH**
   ```python
   bridge_act = await trio_asyncio.run_aio_coroutine(
       self._bridge_worker.poll_workflow_activation()
   )
   ```

2. **Activation Completion** (`bridge_worker.py:165,244,258`)
   ```python
   await trio_asyncio.run_aio_coroutine(
       self._bridge_worker.complete_workflow_activation(comp)
   )
   ```

3. **Bridge Lifecycle** (`worker/_worker.py:278,306`)
   ```python
   await trio_asyncio.run_aio_coroutine(self._bridge_worker.validate())
   await trio_asyncio.run_aio_coroutine(self._bridge_worker.finalize_shutdown())
   ```

4. **Client Connection** (`bridge_worker_example.py:62-64`)
   ```python
   async with trio_asyncio.open_loop():
       client = await trio_asyncio.run_aio_coroutine(
           temporalio.client.Client.connect("localhost:7233")
       )
   ```

### Components NOT Requiring Changes ✅

- `_workflow_instance.py` - Pure Trio execution in threads
- `workflow.py` - Runtime context via ContextVar (not asyncio)
- `_clock.py` - WorkflowClock for deterministic time
- `_runtime.py` - Core runtime implementation
- All workflow decorator and execution logic

---

## Target Architecture

### New Bridge Design (from POC)

```
┌─────────────────────────────────────────┐
│ Trio Layer (Pure Trio)                  │
│   ├─ TrioBridgeWrapper                  │
│   │    └─ Uses Trio Events for waiting  │
│   └─ Worker / TrioBridgeWorker          │
└────────────┬────────────────────────────┘
             │ (Queue + Callbacks)
┌────────────▼────────────────────────────┐
│ Rust Bridge Thread (PyO3)               │
│   ├─ Tokio Runtime                      │
│   ├─ async_nursery                      │
│   ├─ temporalio-sdk-core                │
│   └─ trio.from_thread callbacks         │
└─────────────────────────────────────────┘
```

**Key Components:**

1. **TrioAsyncBridge (PyO3 Rust module)**
   - Single Rust thread with Tokio runtime
   - Request queue for Trio → Rust
   - Callback delivery via `trio.from_thread`

2. **TrioBridgeWrapper (Python)**
   - Wraps TrioAsyncBridge
   - Creates Trio Events for async waiting
   - Manages trio_token for callbacks

3. **Updated TrioBridgeWorker**
   - Uses TrioBridgeWrapper instead of raw bridge
   - No trio-asyncio calls
   - Pure Trio async operations

---

## Migration Phases

### Phase 1: Create New Async Bridge (PyO3 + Rust)

**Goal:** Implement the core async bridge proven in POC

**Tasks:**

1.1. **Create Rust bridge crate structure**
```bash
temporalio_trio_bridge/
├── Cargo.toml
├── src/
│   ├── lib.rs          # PyO3 module definition
│   ├── bridge.rs       # TrioAsyncBridge implementation
│   ├── request.rs      # Request types
│   └── worker.rs       # Worker wrapper
```

1.2. **Implement TrioAsyncBridge in Rust**
```rust
// src/bridge.rs
#[pyclass]
struct TrioAsyncBridge {
    request_tx: Arc<Mutex<mpsc::UnboundedSender<Request>>>,
}

impl TrioAsyncBridge {
    // Spawns Rust thread with Tokio runtime
    fn new() -> PyResult<Self>

    // Non-blocking request submission
    fn send_request(&self, operation: String, data: Vec<u8>, callback: PyObject)
}
```

1.3. **Implement request processing loop**
```rust
async fn process_requests(
    mut rx: mpsc::UnboundedReceiver<Request>,
    core_worker: Arc<CoreWorker>
) {
    while let Some(request) = rx.recv().await {
        tokio::spawn(async move {
            // Use async_nursery for structured concurrency
            let (nursery, mut stream) = Nursery::new();

            match request.operation.as_str() {
                "poll_activation" => { /* ... */ }
                "complete_activation" => { /* ... */ }
                // ...
            }
        });
    }
}
```

1.4. **Build and test PyO3 module**
```bash
cd temporalio_trio_bridge
cargo build --release
maturin develop
```

**Deliverable:** `temporalio_trio_bridge` module importable from Python

**Success Criteria:**
- ✅ Module imports successfully
- ✅ Can send requests from Python
- ✅ Callbacks fire correctly via trio.from_thread
- ✅ No memory leaks or panics

**Estimated Time:** 3-5 days

---

### Phase 2: Create Python Bridge Wrapper

**Goal:** Wrap Rust bridge with Trio-friendly Python API

**Tasks:**

2.1. **Create `temporalio_trio/_async_bridge.py`**
```python
class TrioBridgeWrapper:
    """Wraps TrioAsyncBridge with Trio async primitives."""

    def __init__(self):
        self._rust_bridge = TrioAsyncBridge()
        self._trio_token: Optional[trio.lowlevel.TrioToken] = None

    def set_trio_token(self, token: trio.lowlevel.TrioToken):
        """Set Trio token for from_thread callbacks."""
        self._trio_token = token

    async def poll_workflow_activation(self) -> bytes:
        """Poll for activation - fully async, no blocked threads."""
        event = trio.Event()
        result_container = []

        def deliver_result(result_bytes: bytes):
            result_container.append(result_bytes)
            trio.from_thread.run_sync(
                event.set,
                trio_token=self._trio_token
            )

        self._rust_bridge.send_request("poll_activation", b"", deliver_result)
        await event.wait()
        return result_container[0]

    async def complete_workflow_activation(self, completion_bytes: bytes):
        """Complete activation - fully async."""
        # Similar pattern
```

2.2. **Add error handling and timeout support**
```python
async def poll_workflow_activation(self, timeout: Optional[float] = None):
    event = trio.Event()

    # ... setup callback

    if timeout:
        with trio.move_on_after(timeout):
            await event.wait()
    else:
        await event.wait()
```

2.3. **Add lifecycle management**
```python
async def start(self):
    """Initialize bridge and capture Trio token."""
    self._trio_token = trio.lowlevel.current_trio_token()
    self._rust_bridge.start()

async def shutdown(self):
    """Shutdown bridge gracefully."""
    self._rust_bridge.shutdown()
```

**Deliverable:** `TrioBridgeWrapper` class in `temporalio_trio/_async_bridge.py`

**Success Criteria:**
- ✅ All bridge operations return Trio awaitables
- ✅ No trio-asyncio usage
- ✅ Error handling works correctly
- ✅ Callbacks fire on correct Trio context

**Estimated Time:** 2-3 days

---

### Phase 3: Migrate TrioBridgeWorker

**Goal:** Update `bridge_worker.py` to use new async bridge

**Tasks:**

3.1. **Update imports**
```python
# Remove
import trio_asyncio

# Add
from temporalio_trio._async_bridge import TrioBridgeWrapper
```

3.2. **Replace bridge initialization**
```python
# OLD (temporalio_trio/bridge_worker.py:54-68)
def __init__(
    self,
    bridge_worker: temporalio.bridge.worker.Worker,  # ← Asyncio-based
    namespace: str,
    task_queue: str,
    workflows: Sequence[Type],
    data_converter: temporalio.converter.DataConverter | None = None,
) -> None:
    self._bridge_worker = bridge_worker  # ← Direct asyncio bridge
    # ...

# NEW
def __init__(
    self,
    bridge_wrapper: TrioBridgeWrapper,  # ← Trio-compatible wrapper
    namespace: str,
    task_queue: str,
    workflows: Sequence[Type],
    data_converter: temporalio.converter.DataConverter | None = None,
) -> None:
    self._bridge = bridge_wrapper  # ← Uses new async bridge
    # ...
```

3.3. **Update polling loop (line 109-134)**
```python
# OLD
async def _poll_loop(self) -> None:
    try:
        while not self._shutdown_event.is_set():
            try:
                bridge_act = await trio_asyncio.run_aio_coroutine(  # ← Remove
                    self._bridge_worker.poll_workflow_activation()
                )
            except Exception as e:
                # ...

# NEW
async def _poll_loop(self) -> None:
    try:
        while not self._shutdown_event.is_set():
            try:
                # Fully async, no trio-asyncio!
                bridge_act_bytes = await self._bridge.poll_workflow_activation()
                bridge_act = WorkflowActivation()
                bridge_act.ParseFromString(bridge_act_bytes)
            except Exception as e:
                # ...
```

3.4. **Update completion calls (lines 165, 244, 258)**
```python
# OLD
await trio_asyncio.run_aio_coroutine(
    self._bridge_worker.complete_workflow_activation(comp)
)

# NEW
comp_bytes = comp.SerializeToString()
await self._bridge.complete_workflow_activation(comp_bytes)
```

3.5. **Update shutdown methods**
```python
def shutdown(self) -> None:
    logger.info("Initiating worker shutdown")
    self._shutdown_event.set()
    self._bridge.initiate_shutdown()  # Synchronous call

async def finalize_shutdown(self) -> None:
    logger.info("Finalizing worker shutdown")
    await self._bridge.finalize_shutdown()  # Now fully async
```

**Deliverable:** Updated `bridge_worker.py` with no trio-asyncio usage

**Success Criteria:**
- ✅ Polling loop works correctly
- ✅ Activations processed successfully
- ✅ Completions delivered to bridge
- ✅ Shutdown works cleanly
- ✅ All tests pass

**Estimated Time:** 2-3 days

---

### Phase 4: Migrate Worker Class

**Goal:** Update `worker/_worker.py` to create new bridge

**Tasks:**

4.1. **Update imports**
```python
# Remove
import trio_asyncio

# Add
from temporalio_trio._async_bridge import TrioBridgeWrapper
```

4.2. **Update bridge creation in `run()` method (lines 201-310)**
```python
# OLD
async def run(self) -> None:
    if self._started:
        raise RuntimeError("Worker has already been started")
    self._started = True

    try:
        # Initialize bridge worker inside trio-asyncio context
        async with trio_asyncio.open_loop():  # ← Remove this
            # Get bridge client
            bridge_client = self._client.service_client._bridge_client
            # ... create WorkerConfig
            # Create bridge worker
            self._bridge_worker = temporalio.bridge.worker.Worker.create(
                bridge_client, config
            )
            await trio_asyncio.run_aio_coroutine(self._bridge_worker.validate())
            # ...

# NEW
async def run(self) -> None:
    if self._started:
        raise RuntimeError("Worker has already been started")
    self._started = True

    try:
        # Create async bridge wrapper
        bridge_wrapper = TrioBridgeWrapper()
        await bridge_wrapper.start()

        # Initialize with Temporal client
        await bridge_wrapper.initialize_with_client(
            self._client,
            namespace=self._namespace,
            task_queue=self._task_queue,
            # ... other config
        )

        # Validate
        await bridge_wrapper.validate()
        # ...
```

4.3. **Update TrioBridgeWorker instantiation**
```python
# OLD
self._trio_worker = TrioBridgeWorker(
    bridge_worker=self._bridge_worker,  # ← Asyncio-based bridge
    namespace=self._namespace,
    task_queue=self._task_queue,
    workflows=self._workflows,
    data_converter=self._data_converter,
)

# NEW
self._trio_worker = TrioBridgeWorker(
    bridge_wrapper=bridge_wrapper,  # ← Trio-compatible wrapper
    namespace=self._namespace,
    task_queue=self._task_queue,
    workflows=self._workflows,
    data_converter=self._data_converter,
)
```

4.4. **Update shutdown handling**
```python
# OLD
if self._bridge_worker:
    await trio_asyncio.run_aio_coroutine(
        self._bridge_worker.finalize_shutdown()
    )

# NEW
if bridge_wrapper:
    await bridge_wrapper.shutdown()
```

**Deliverable:** Updated `worker/_worker.py` with no trio-asyncio usage

**Success Criteria:**
- ✅ Worker starts successfully
- ✅ Bridge initializes correctly
- ✅ Workflows execute properly
- ✅ Shutdown works cleanly
- ✅ High-level Worker API unchanged for users

**Estimated Time:** 2 days

---

### Phase 5: Update Examples & Documentation

**Goal:** Update example and remove trio-asyncio dependency

**Tasks:**

5.1. **Update `bridge_worker_example.py`**
```python
# OLD
import trio_asyncio

async def run_worker():
    async with trio_asyncio.open_loop():
        client = await trio_asyncio.run_aio_coroutine(
            temporalio.client.Client.connect("localhost:7233")
        )
        # ...

# NEW
async def run_worker():
    # Client connection now handled internally by bridge
    # Or use Trio-compatible client wrapper
    client = await trio_temporal_client.connect("localhost:7233")
    # ...
```

5.2. **Remove trio-asyncio from dependencies**
```toml
# pyproject.toml - Remove line 13:
# "trio-asyncio>=0.14.0",  ← DELETE
```

5.3. **Update documentation**
- Update README.md to remove trio-asyncio references
- Update CLAUDE.md if it mentions trio-asyncio
- Update any comments referencing trio-asyncio

5.4. **Create migration guide for users**
```markdown
# Migration Guide: trio-asyncio to Async Bridge

## Before
```python
import trio_asyncio
from temporalio_trio.worker import Worker

async with trio_asyncio.open_loop():
    client = await trio_asyncio.run_aio_coroutine(
        temporalio.client.Client.connect("localhost:7233")
    )
    worker = Worker(client, task_queue="queue", workflows=[...])
    await worker.run()
```

## After
```python
from temporalio_trio.worker import Worker

# No trio-asyncio needed!
client = await trio_temporal_client.connect("localhost:7233")
worker = Worker(client, task_queue="queue", workflows=[...])
await worker.run()
```

**Deliverable:** Updated example and docs, trio-asyncio dependency removed

**Success Criteria:**
- ✅ Example runs without trio-asyncio
- ✅ Documentation updated
- ✅ pyproject.toml cleaned up
- ✅ Migration guide written

**Estimated Time:** 1-2 days

---

## Testing Strategy

### Unit Tests

Create tests for each component:

```python
# tests/test_async_bridge.py
async def test_poll_activation():
    """Test polling returns activation without blocking."""
    bridge = TrioBridgeWrapper()
    await bridge.start()

    activation = await bridge.poll_workflow_activation()
    assert activation is not None

async def test_concurrent_polls():
    """Test many concurrent polls don't exhaust threads."""
    bridge = TrioBridgeWrapper()
    await bridge.start()

    async with trio.open_nursery() as nursery:
        for i in range(100):
            nursery.start_soon(bridge.poll_workflow_activation)

    # Should complete without thread exhaustion
```

### Integration Tests

```python
# tests/test_worker_integration.py
async def test_worker_with_new_bridge():
    """Test full worker lifecycle with async bridge."""
    worker = Worker(
        client=client,
        task_queue="test-queue",
        workflows=[TestWorkflow],
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)
        await trio.sleep(1)
        # Start workflow
        result = await start_and_wait_workflow()
        assert result == expected
        worker.shutdown()
```

### Performance Tests

```python
# tests/test_performance.py
async def test_scalability():
    """Verify single Rust thread handles many concurrent operations."""
    bridge = TrioBridgeWrapper()
    await bridge.start()

    start = time.time()
    async with trio.open_nursery() as nursery:
        for i in range(500):
            nursery.start_soon(poll_and_complete, bridge)
    elapsed = time.time() - start

    print(f"500 operations in {elapsed:.2f}s")
    assert elapsed < 60  # Should be fast
```

---

## Rollback Plan

If migration fails, rollback steps:

1. **Revert Git Commits**
   ```bash
   git revert HEAD~N  # Revert N commits
   git push origin task/trio-asyncio --force
   ```

2. **Restore trio-asyncio dependency**
   ```bash
   git checkout HEAD~N -- pyproject.toml
   pip install trio-asyncio
   ```

3. **Restore old bridge integration**
   ```bash
   git checkout HEAD~N -- temporalio_trio/bridge_worker.py
   git checkout HEAD~N -- temporalio_trio/worker/_worker.py
   ```

4. **Verify tests pass**
   ```bash
   pytest tests/
   ```

---

## Risk Assessment

| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| PyO3 bridge bugs | Medium | High | Thorough unit testing, POC already proven |
| Performance regression | Low | Medium | Benchmark before/after, POC shows improvement |
| Memory leaks in Rust | Low | High | Use valgrind, test long-running workers |
| Callback timing issues | Medium | High | Test with many concurrent operations |
| Client connection issues | Low | Medium | May need separate client wrapper |

---

## Success Metrics

**Technical Metrics:**
- ✅ Zero trio-asyncio imports in codebase
- ✅ All tests pass
- ✅ Memory usage: <10MB for 100 concurrent workflows
- ✅ Thread count: 1 Rust thread + Trio main thread
- ✅ Latency: <200μs overhead per operation

**Functional Metrics:**
- ✅ Workflows execute correctly
- ✅ Activations polled successfully
- ✅ Completions delivered to bridge
- ✅ Error handling works
- ✅ Shutdown works cleanly

**Quality Metrics:**
- ✅ Test coverage: >90%
- ✅ No memory leaks
- ✅ No panics or crashes
- ✅ Documentation updated

---

## Dependencies & Blockers

**Prerequisites:**
1. POC validated (✅ Done)
2. Rust toolchain installed
3. PyO3/maturin build working
4. Understanding of Temporal bridge protocol

**External Dependencies:**
- temporalio-sdk-core (Rust) - no changes needed
- async_nursery crate - already available
- Trio with deterministic scheduling - already using fork

**Potential Blockers:**
- Client connection may need separate async handling
- Bridge protocol changes (unlikely)
- PyO3 version compatibility

---

## Timeline

| Phase | Duration | Dependencies | Start After |
|-------|----------|--------------|-------------|
| Phase 1: Rust Bridge | 3-5 days | None | Immediate |
| Phase 2: Python Wrapper | 2-3 days | Phase 1 | Phase 1 complete |
| Phase 3: Migrate TrioBridgeWorker | 2-3 days | Phase 2 | Phase 2 complete |
| Phase 4: Migrate Worker | 2 days | Phase 3 | Phase 3 complete |
| Phase 5: Examples & Docs | 1-2 days | Phase 4 | Phase 4 complete |
| **Total** | **10-15 days** | | |

**With testing and buffer: 2-3 weeks**

---

## Conclusion

This migration plan provides a clear, phased approach to eliminating trio-asyncio while maintaining functionality. The POC proves the architecture works; now we systematically implement it across the codebase.

**Next Step:** Begin Phase 1 - Create Rust async bridge implementation.
