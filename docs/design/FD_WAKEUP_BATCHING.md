# FD Wakeup + Batching: Optimized Tokio ↔ Trio Communication

## Status: Future Enhancement

This document describes an optimization to the Tokio ↔ Trio bridge communication that reduces GIL contention and improves throughput for concurrent workflow execution.

## Problem Statement

### Current Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                 Python/Trio Thread                          │
│                                                             │
│    send_request(op, data, callback)  ──► channel            │
│                                                             │
│    await trio.Event.wait() ◄── trio.from_thread.run_sync()  │
└─────────────────────────────────────────────────────────────┘
                          │
                    per-result callback
                          │
┌─────────────────────────────────────────────────────────────┐
│              Rust/Tokio Thread                              │
│                                                             │
│    Python::with_gil(|py| {                                  │
│        callback.call1(py, (result,))                        │
│    });                                                      │
└─────────────────────────────────────────────────────────────┘
```

### Inefficiencies

1. **Per-Result GIL Acquisition**: Every operation completion (timer fired, activity resolved, poll returned) requires:
   - Tokio thread calling `Python::with_gil()`
   - Blocking if Python is executing workflow code
   - `trio.from_thread.run_sync()` overhead

2. **Scaling Issues**: With N concurrent workflows, each potentially waiting on timers/activities:
   - N completions = N GIL acquisitions
   - Contention increases linearly with concurrency

3. **Latency Variability**: GIL wait time depends on Python workload, causing unpredictable result delivery latency.

### Measured Impact (Theoretical)

| Concurrent Workflows | Current (GIL acquires/sec) | Proposed (GIL acquires/sec) |
|---------------------|---------------------------|----------------------------|
| 10                  | ~100 (10 ops × 10 wf)     | ~10 (batched)              |
| 100                 | ~1000                     | ~20                        |
| 500                 | ~5000                     | ~50                        |

## Proposed Architecture

### Overview

Replace per-result callbacks with:
1. **Lock-free result queue**: Tokio pushes results without GIL
2. **File descriptor wakeup**: Tokio signals Python via eventfd/pipe
3. **Batch draining**: Python drains queue in one GIL-holding period

```
┌─────────────────────────────────────────────────────────────┐
│                 Python/Trio Thread                          │
│                                                             │
│    # Native Trio I/O waiting - no GIL needed by Rust        │
│    await trio.lowlevel.wait_readable(wakeup_fd)             │
│                                                             │
│    # One GIL acquisition for entire batch                   │
│    results = bridge.drain_results()                         │
│                                                             │
│    # Fire all events                                        │
│    for result in results:                                   │
│        pending_events[result.request_id].set()              │
└─────────────────────────────────────────────────────────────┘
                          │
                    eventfd/pipe (wakeup only)
                          │
┌─────────────────────────────────────────────────────────────┐
│              Rust/Tokio Thread                              │
│                                                             │
│    // No GIL needed!                                        │
│    result_queue.push(result);  // lock-free                 │
│    wakeup_fd.write(1);         // signal Python             │
└─────────────────────────────────────────────────────────────┘
```

### Key Components

#### 1. Lock-Free Result Queue (Rust)

```rust
use crossbeam_queue::SegQueue;

pub struct ResultQueue {
    queue: SegQueue<RequestResult>,
    wakeup_fd: RawFd,
}

impl ResultQueue {
    /// Push result and signal Python (no GIL needed)
    pub fn push_and_signal(&self, result: RequestResult) {
        self.queue.push(result);

        // Write to eventfd to wake Python
        // This is async-signal-safe and doesn't need GIL
        let buf: u64 = 1;
        unsafe {
            libc::write(self.wakeup_fd, &buf as *const u64 as *const _, 8);
        }
    }

    /// Drain all pending results (called from Python with GIL)
    pub fn drain(&self) -> Vec<RequestResult> {
        let mut results = Vec::new();
        while let Some(result) = self.queue.pop() {
            results.push(result);
        }
        results
    }
}
```

#### 2. Wakeup File Descriptor

**Linux**: Use `eventfd` for efficient signaling
```rust
let wakeup_fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK) };
```

**macOS/BSD**: Use pipe
```rust
let mut fds = [0i32; 2];
unsafe { libc::pipe(fds.as_mut_ptr()) };
let (read_fd, write_fd) = (fds[0], fds[1]);
```

**Cross-platform**: Use `polling` or `mio` crate abstractions

#### 3. Python Bridge Changes

```python
class TrioBridgeWrapper:
    def __init__(self) -> None:
        self._rust_bridge = TrioAsyncBridge()
        self._wakeup_fd: int = self._rust_bridge.get_wakeup_fd()
        self._pending_events: dict[str, trio.Event] = {}

    async def start(self) -> None:
        """Start the bridge and result processor."""
        self._trio_token = trio.lowlevel.current_trio_token()
        self._state = BridgeState.RUNNING

    async def run_result_processor(self, task_status=trio.TASK_STATUS_IGNORED) -> None:
        """Background task that processes results from Rust.

        This replaces per-result callbacks with batched processing.
        """
        task_status.started()

        while self._state == BridgeState.RUNNING:
            # Wait for Rust to signal results are ready
            # This is native Trio I/O - very efficient
            await trio.lowlevel.wait_readable(self._wakeup_fd)

            # Clear the eventfd
            self._clear_wakeup_fd()

            # Drain all pending results (one GIL acquisition)
            results = self._rust_bridge.drain_results()

            # Fire events for all results
            for result in results:
                request_id = result.request_id
                if request_id in self._pending_events:
                    event, container = self._pending_events.pop(request_id)
                    container.append(result)
                    event.set()

    def _clear_wakeup_fd(self) -> None:
        """Clear the eventfd/pipe to reset for next wakeup."""
        try:
            os.read(self._wakeup_fd, 8)
        except BlockingIOError:
            pass  # Already cleared

    async def poll_workflow_activation(self, timeout: Optional[float] = None) -> bytes:
        """Poll for activation using new batched approach."""
        self._check_running()

        event = trio.Event()
        result_container: list[RequestResult] = []
        request_id = self._rust_bridge.send_request_no_callback(
            "poll_activation", b""
        )

        # Register for result delivery
        self._pending_events[request_id] = (event, result_container)

        try:
            if timeout is not None:
                with trio.move_on_after(timeout) as cancel_scope:
                    await event.wait()
                if cancel_scope.cancelled_caught:
                    self._pending_events.pop(request_id, None)
                    raise trio.TooSlowError("poll_workflow_activation timed out")
            else:
                await event.wait()

            result = result_container[0]
            if not result.success:
                raise RuntimeError(result.error)
            return result.get_data()
        except BaseException:
            self._pending_events.pop(request_id, None)
            raise
```

#### 4. Modified Rust Bridge

```rust
#[pyclass]
pub struct TrioAsyncBridge {
    request_tx: Arc<Mutex<mpsc::UnboundedSender<Request>>>,
    result_queue: Arc<ResultQueue>,
    shutdown: Arc<Mutex<bool>>,
}

#[pymethods]
impl TrioAsyncBridge {
    #[new]
    fn new() -> PyResult<Self> {
        // Create eventfd for wakeup
        let wakeup_fd = create_wakeup_fd()?;

        let result_queue = Arc::new(ResultQueue::new(wakeup_fd));
        let result_queue_clone = result_queue.clone();

        let (tx, rx) = mpsc::unbounded_channel::<Request>();

        // Spawn Tokio thread
        std::thread::spawn(move || {
            Self::rust_event_loop(rx, result_queue_clone);
        });

        Ok(Self {
            request_tx: Arc::new(Mutex::new(tx)),
            result_queue,
            shutdown: Arc::new(Mutex::new(false)),
        })
    }

    /// Get the wakeup fd for Python to monitor
    fn get_wakeup_fd(&self) -> i32 {
        self.result_queue.wakeup_fd
    }

    /// Send request without callback (new method)
    fn send_request_no_callback(
        &self,
        operation: String,
        data: Vec<u8>,
    ) -> PyResult<String> {
        let request_id = uuid::Uuid::new_v4().to_string();
        let request = Request::new_no_callback(request_id.clone(), operation, data);
        self.request_tx.lock().send(request)?;
        Ok(request_id)
    }

    /// Drain all pending results (called from Python)
    fn drain_results(&self) -> Vec<RequestResult> {
        self.result_queue.drain()
    }
}

impl TrioAsyncBridge {
    async fn process_request_async(
        request: Request,
        core_worker: Arc<CoreWorkerHandle>,
        result_queue: Arc<ResultQueue>,
    ) {
        let result = Self::handle_operation(&request, core_worker).await;

        // No GIL needed! Just push to queue and signal
        result_queue.push_and_signal(result);
    }
}
```

### Integration with SingleThreadWorker

```python
class SingleThreadWorker:
    async def _worker_main(self) -> None:
        """Main worker loop with result processor."""
        async with trio.open_nursery() as nursery:
            # Start result processor as background task
            await nursery.start(self._bridge.run_result_processor)

            # Run the main poll/dispatch loop
            await self._run_poll_loop(nursery)
```

## Migration Path

### Phase 1: Add Infrastructure (Non-Breaking)

1. Add `ResultQueue` to Rust bridge
2. Add `get_wakeup_fd()` method
3. Add `drain_results()` method
4. Add `send_request_no_callback()` method
5. Keep existing callback-based methods working

### Phase 2: Python-Side Changes

1. Add `run_result_processor()` coroutine
2. Add `_pending_events` tracking
3. Create new async methods using batched approach
4. Keep old methods as aliases initially

### Phase 3: Cutover

1. Update `SingleThreadWorker` to use result processor
2. Remove callback-based code paths
3. Clean up deprecated methods

### Phase 4: Optimization

1. Tune batch sizes if needed
2. Add metrics for queue depth monitoring
3. Consider adaptive batching based on load

## Platform Considerations

### Linux
- Use `eventfd` for optimal performance
- Single syscall for signaling
- Kernel coalesces multiple writes

### macOS
- Use pipe pair
- Slightly higher overhead than eventfd
- Well-supported by Trio

### Windows
- Use `socket` pair or named pipe
- May need platform-specific abstraction
- Consider using `polling` crate

### Cross-Platform Implementation

```rust
#[cfg(target_os = "linux")]
fn create_wakeup_fd() -> Result<RawFd, Error> {
    let fd = unsafe { libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC) };
    if fd < 0 {
        return Err(Error::last_os_error());
    }
    Ok(fd)
}

#[cfg(not(target_os = "linux"))]
fn create_wakeup_fd() -> Result<(RawFd, RawFd), Error> {
    let mut fds = [0i32; 2];
    if unsafe { libc::pipe(fds.as_mut_ptr()) } < 0 {
        return Err(Error::last_os_error());
    }
    // Set non-blocking
    set_nonblocking(fds[0])?;
    set_nonblocking(fds[1])?;
    Ok((fds[0], fds[1]))  // (read_fd, write_fd)
}
```

## Expected Benefits

### Quantitative

| Metric | Current | Proposed | Improvement |
|--------|---------|----------|-------------|
| GIL acquisitions per 100 results | 100 | 1-10 | 10-100x reduction |
| Result delivery latency (p99) | Variable (GIL wait) | Consistent | More predictable |
| Max sustainable ops/sec | ~5000 | ~50000 | ~10x throughput |

### Qualitative

1. **Predictable Latency**: Results delivered at natural Trio yield points
2. **Better Scaling**: Overhead doesn't increase linearly with workflow count
3. **Cleaner Integration**: Uses Trio's native I/O primitives
4. **Debuggability**: Single point for result processing, easier to trace

## Risks and Mitigations

### Risk: Increased Latency for Single Results

**Concern**: Batching might delay delivery of single results.

**Mitigation**:
- `wait_readable()` returns immediately when fd is signaled
- No artificial batching delay - just natural coalescing
- Single result still delivered promptly

### Risk: Queue Unbounded Growth

**Concern**: If Python falls behind, queue grows unbounded.

**Mitigation**:
- Monitor queue depth
- Add backpressure if needed (block Tokio push)
- Alert on sustained high queue depth

### Risk: Platform Compatibility

**Concern**: eventfd is Linux-only.

**Mitigation**:
- Fallback to pipe on other platforms
- Use `polling` crate for abstraction
- Test on all target platforms

## Future Considerations

### Single-Thread Tokio

This design is a stepping stone. The ultimate optimization would be running Tokio in the same thread as Trio, eliminating the separate thread entirely. The FD wakeup mechanism could be reused for that architecture.

### Metrics Integration

Add observability:
```python
class BridgeMetrics:
    results_delivered: Counter
    batch_sizes: Histogram
    queue_depth: Gauge
    delivery_latency: Histogram
```

## References

- [eventfd(2) man page](https://man7.org/linux/man-pages/man2/eventfd.2.html)
- [Trio I/O documentation](https://trio.readthedocs.io/en/stable/reference-lowlevel.html)
- [crossbeam-queue](https://docs.rs/crossbeam-queue/latest/crossbeam_queue/)
- [PyO3 GIL handling](https://pyo3.rs/main/python_from_rust.html#acquiring-the-gil)
