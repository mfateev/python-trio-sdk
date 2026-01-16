# Bridge Integration Analysis

This document analyzes how to integrate the Trio-based workflow runtime with the Temporal Python SDK's Rust bridge.

## Bridge Architecture Overview

### Location

The bridge is **built into the main SDK** at `temporalio/bridge/` - not a separate package. It's a Rust crate compiled with PyO3/maturin.

```
temporalio/bridge/
├── Cargo.toml                    # Rust crate configuration (maturin build)
├── src/
│   ├── lib.rs                    # PyModule exports all bridge APIs
│   ├── runtime.rs                # Rust runtime wrapping tokio
│   ├── worker.rs                 # WorkerRef: Core worker binding
│   ├── client.rs                 # ClientRef: gRPC client binding
│   └── ...
├── proto/                        # Generated protobuf files
├── __init__.py
├── runtime.py                    # RuntimeOptions, TelemetryConfig
├── worker.py                     # WorkerConfig, poll/complete methods
└── client.py                     # ClientConfig, connection
```

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│  Python (asyncio or Trio)                                   │
│  ┌─────────────────────────────────────────────────────────┐│
│  │ Worker._workflow_worker.run()                           ││
│  │   └── poll_workflow_activation()  ← awaitable           ││
│  │   └── complete_workflow_activation(bytes)               ││
│  └─────────────────────────────────────────────────────────┘│
│                           │                                  │
│                     FFI (PyO3)                               │
│                           │                                  │
│  ┌─────────────────────────────────────────────────────────┐│
│  │ Rust Bridge (temporalio/bridge/src/)                    ││
│  │   └── pyo3_async_runtimes::future_into_py()             ││
│  │   └── Tokio runtime (background thread)                 ││
│  └─────────────────────────────────────────────────────────┘│
│                           │                                  │
│              Temporal Core SDK (Rust)                        │
│                           │                                  │
│                    gRPC to Server                            │
└─────────────────────────────────────────────────────────────┘
```

## Key FFI Mechanism

### Python Side

```python
# temporalio/bridge/worker.py
async def poll_workflow_activation(self) -> WorkflowActivation:
    """Poll for a workflow activation."""
    return WorkflowActivation.FromString(
        await self._ref.poll_workflow_activation()  # Awaits Rust future
    )

async def complete_workflow_activation(
    self,
    comp: WorkflowActivationCompletion,
) -> None:
    """Complete a workflow activation."""
    await self._ref.complete_workflow_activation(comp.SerializeToString())
```

### Rust Side

```rust
// temporalio/bridge/src/worker.rs
fn poll_workflow_activation<'p>(&self, py: Python<'p>) -> PyResult<Bound<'p, PyAny>> {
    let task_locals = pyo3_async_runtimes::TaskLocals::with_running_loop(py)?
        .copy_context(py)?;

    self.runtime.future_into_py(py, async move {
        let bytes = worker.poll_workflow_activation().await?;
        Ok(PyBytes::new(py, &bytes).into())
    })
}
```

### Critical Insight

**The Rust bridge doesn't care about asyncio!**

The bridge uses `pyo3_async_runtimes` which converts Rust futures to Python coroutines. These coroutines are generic Python awaitables that work with **any** async runtime that can `await` them - including Trio.

```python
# This works with ANY Python async runtime
await self._ref.poll_workflow_activation()
```

## Activation & Completion Flow

### Workflow Activation (Server → Python)

```
Temporal Server
    │
    ▼ (gRPC)
Rust Core SDK
    │
    ▼ (protobuf bytes)
Bridge Worker (poll_workflow_activation)
    │
    ▼ (Python awaitable)
Python Worker
    │
    ▼ (deserialize)
WorkflowActivation protobuf
```

**WorkflowActivation** contains:
- `run_id: str`
- `jobs: List[WorkflowActivationJob]`
  - `InitializeWorkflow` - Start new workflow
  - `FireTimer` - Timer fired
  - `ResolveActivity` - Activity completed
  - `SignalWorkflow` - Signal received
  - `QueryWorkflow` - Query received
  - etc.

### Workflow Completion (Python → Server)

```
Python Workflow Instance
    │
    ▼ (commands)
WorkflowActivationCompletion protobuf
    │
    ▼ (serialize to bytes)
Bridge Worker (complete_workflow_activation)
    │
    ▼ (Rust future)
Rust Core SDK
    │
    ▼ (gRPC)
Temporal Server
```

**WorkflowActivationCompletion** contains:
- `run_id: str`
- `successful: Success` OR `failed: Failure`
  - `Success.commands: List[WorkflowCommand]`
    - `StartTimer`
    - `CompleteWorkflowExecution`
    - `FailWorkflowExecution`
    - `ScheduleActivity`
    - etc.

## What's Reusable vs Asyncio-Specific

| Component | Reusable with Trio? | Notes |
|-----------|---------------------|-------|
| **Protobuf messages** | ✅ YES | Pure data structures |
| **Bridge FFI calls** | ✅ YES | Returns generic Python awaitables |
| **Payload codecs** | ✅ YES | Generic `Awaitable` interface |
| **Client config** | ✅ YES | Pure dataclasses |
| **Data converters** | ✅ YES | Async codec interface |
| **Worker.py (Python)** | ❌ NO | Uses `asyncio.create_task`, `asyncio.wait` |
| **Activity worker** | ❌ NO | Uses `loop.run_in_executor` |
| **Workflow worker** | ❌ NO | Uses `asyncio.all_tasks()` |

## Asyncio Patterns to Replace

### 1. Task Creation

```python
# Asyncio
asyncio.create_task(self._handle_activation(...))

# Trio
nursery.start_soon(self._handle_activation, ...)
```

### 2. Queues

```python
# Asyncio
self._queue: asyncio.Queue = asyncio.Queue()
await self._queue.put(item)
item = await self._queue.get()

# Trio
send_channel, receive_channel = trio.open_memory_channel(max_buffer_size)
await send_channel.send(item)
item = await receive_channel.receive()
```

### 3. Wait for Multiple Tasks

```python
# Asyncio
done, pending = await asyncio.wait(
    [task1, task2],
    return_when=asyncio.FIRST_COMPLETED
)

# Trio
async with trio.open_nursery() as nursery:
    nursery.start_soon(task1_func)
    nursery.start_soon(task2_func)
    # Use cancel scopes for early exit
```

### 4. Run in Executor (for sync code)

```python
# Asyncio
loop = asyncio.get_running_loop()
result = await loop.run_in_executor(executor, func, *args)

# Trio
result = await trio.to_thread.run_sync(func, *args)
```

### 5. Events

```python
# Asyncio
event = asyncio.Event()
event.set()
await event.wait()

# Trio
event = trio.Event()
event.set()
await event.wait()
```

### 6. Locks

```python
# Asyncio
lock = asyncio.Lock()
async with lock:
    ...

# Trio
lock = trio.Lock()
async with lock:
    ...
```

## Integration Strategy

### Minimal Trio Worker

```python
import trio
from temporalio.bridge.client import Client
from temporalio.bridge.worker import Worker as BridgeWorker, WorkerConfig
from temporalio.bridge.proto.workflow_activation import WorkflowActivation
from temporalio.bridge.proto.workflow_completion import WorkflowActivationCompletion

from temporalio_trio.worker import TrioWorkflowRunner, TrioWorkflowInstance

class TrioWorker:
    def __init__(
        self,
        client: Client,
        task_queue: str,
        workflows: list[type],
    ):
        self._client = client
        self._task_queue = task_queue
        self._runner = TrioWorkflowRunner()
        self._instances: dict[str, TrioWorkflowInstance] = {}

        for wf_cls in workflows:
            defn = workflow._Definition.must_from_class(wf_cls)
            self._runner.prepare_workflow(defn)

    async def run(self):
        # Create bridge worker
        bridge_worker = BridgeWorker.create(
            self._client._bridge_client,
            WorkerConfig(
                task_queue=self._task_queue,
                # ... other config
            ),
        )

        async with trio.open_nursery() as nursery:
            # Start polling loop
            nursery.start_soon(self._poll_loop, bridge_worker)

    async def _poll_loop(self, bridge_worker: BridgeWorker):
        while True:
            # Poll for activation (works with Trio!)
            activation = await bridge_worker.poll_workflow_activation()

            # Handle activation
            completion = await self._handle_activation(activation)

            # Send completion back
            await bridge_worker.complete_workflow_activation(completion)

    async def _handle_activation(
        self,
        activation: WorkflowActivation,
    ) -> WorkflowActivationCompletion:
        run_id = activation.run_id

        # Get or create instance
        if run_id not in self._instances:
            # Create new instance from InitializeWorkflow job
            self._instances[run_id] = self._create_instance(activation)

        instance = self._instances[run_id]

        # Convert bridge activation to our activation format
        our_activation = self._convert_activation(activation)

        # Execute with our Trio-based instance
        our_completion = instance.activate(our_activation)

        # Convert back to bridge format
        return self._convert_completion(our_completion, run_id)
```

### Required Conversions

We need to convert between:

1. **Bridge Protobuf → Our Activation Types**
   - `WorkflowActivation` → `temporalio_trio.worker.WorkflowActivation`
   - `InitializeWorkflow` job → `WorkflowStartedJob`
   - `FireTimer` job → `TimerFiredJob`

2. **Our Completion Types → Bridge Protobuf**
   - `temporalio_trio.worker.WorkflowActivationCompletion` → `WorkflowActivationCompletion`
   - `StartTimerCommand` → `workflow_command.StartTimer`
   - `CompleteWorkflowCommand` → `workflow_command.CompleteWorkflowExecution`

## Files to Create

| File | Description |
|------|-------------|
| `temporalio_trio/worker/_bridge_worker.py` | Trio-based worker using bridge |
| `temporalio_trio/worker/_converter.py` | Activation/completion converters |
| `temporalio_trio/client.py` | Client wrapper (optional) |

## Dependencies

To use the bridge, we need the `temporalio` package installed:

```toml
[project]
dependencies = [
    "temporalio>=1.7.0",  # For bridge access
    "trio>=0.27.0",       # Trio runtime (from fork)
]
```

## Next Steps

1. **Add temporalio dependency** to pyproject.toml
2. **Create activation/completion converters** between our types and bridge protobufs
3. **Create TrioWorker class** that uses bridge for polling
4. **Test with local Temporal server** using `temporal server start-dev`
5. **Create example workflow** that sleeps and completes

## References

- [Temporal Python SDK](https://github.com/temporalio/sdk-python)
- [temporalio/bridge/worker.py](https://github.com/temporalio/sdk-python/blob/main/temporalio/bridge/worker.py)
- [temporalio/worker/_worker.py](https://github.com/temporalio/sdk-python/blob/main/temporalio/worker/_worker.py)
- [PyO3 Async Runtimes](https://docs.rs/pyo3-async-runtimes/latest/pyo3_async_runtimes/)
