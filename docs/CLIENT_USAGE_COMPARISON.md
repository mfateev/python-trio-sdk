# Client Usage Comparison: Asyncio SDK vs Trio SDK

This document shows side-by-side examples of using the Temporal Python SDK with asyncio (current) vs Trio (proposed).

## 1. Basic Connection

### Asyncio (Current SDK)
```python
import asyncio
from temporalio.client import Client

async def main():
    # Connect to Temporal server
    client = await Client.connect("localhost:7233")

    # Use the client...

    # Close when done
    await client.close()

asyncio.run(main())
```

### Trio (Proposed)
```python
import trio
from temporalio_trio.client import Client

async def main():
    # Connect to Temporal server
    client = await Client.connect("localhost:7233")

    # Use the client...

    # Close when done
    await client.close()

trio.run(main)
```

**Key Difference**: Runtime (asyncio.run vs trio.run), API is identical.

---

## 2. Starting a Workflow

### Asyncio (Current SDK)
```python
import asyncio
from temporalio.client import Client

async def main():
    client = await Client.connect("localhost:7233")

    # Start a workflow
    handle = await client.start_workflow(
        "MyWorkflow",
        "argument",
        id="workflow-123",
        task_queue="my-task-queue",
    )

    print(f"Started workflow: {handle.workflow_id}")
    print(f"Run ID: {handle.run_id}")

    await client.close()

asyncio.run(main())
```

### Trio (Proposed)
```python
import trio
from temporalio_trio.client import Client

async def main():
    client = await Client.connect("localhost:7233")

    # Start a workflow
    handle = await client.start_workflow(
        "MyWorkflow",
        "argument",
        id="workflow-123",
        task_queue="my-task-queue",
    )

    print(f"Started workflow: {handle.workflow_id}")
    print(f"Run ID: {handle.run_id}")

    await client.close()

trio.run(main)
```

**Key Difference**: Only the runtime, API is identical.

---

## 3. Getting Workflow Result

### Asyncio (Current SDK)
```python
import asyncio
from temporalio.client import Client

async def main():
    client = await Client.connect("localhost:7233")

    # Execute workflow and wait for result
    result = await client.execute_workflow(
        "GreetingWorkflow",
        "World",
        id="greeting-123",
        task_queue="my-task-queue",
    )

    print(f"Result: {result}")  # "Hello, World!"

    await client.close()

asyncio.run(main())
```

### Trio (Proposed)
```python
import trio
from temporalio_trio.client import Client

async def main():
    client = await Client.connect("localhost:7233")

    # Execute workflow and wait for result
    result = await client.execute_workflow(
        "GreetingWorkflow",
        "World",
        id="greeting-123",
        task_queue="my-task-queue",
    )

    print(f"Result: {result}")  # "Hello, World!"

    await client.close()

trio.run(main)
```

**Key Difference**: Only the runtime.

---

## 4. Workflow Handle Operations

### Asyncio (Current SDK)
```python
import asyncio
from temporalio.client import Client

async def main():
    client = await Client.connect("localhost:7233")

    # Start workflow
    handle = await client.start_workflow(
        "MyWorkflow",
        "arg",
        id="workflow-123",
        task_queue="my-task-queue",
    )

    # Query the workflow
    status = await handle.query("get_status")
    print(f"Status: {status}")

    # Signal the workflow
    await handle.signal("update_data", {"value": 42})

    # Cancel the workflow
    await handle.cancel()

    # Wait for result (will raise CancelledError)
    try:
        result = await handle.result()
    except asyncio.CancelledError:
        print("Workflow was cancelled")

    await client.close()

asyncio.run(main())
```

### Trio (Proposed)
```python
import trio
from temporalio_trio.client import Client

async def main():
    client = await Client.connect("localhost:7233")

    # Start workflow
    handle = await client.start_workflow(
        "MyWorkflow",
        "arg",
        id="workflow-123",
        task_queue="my-task-queue",
    )

    # Query the workflow
    status = await handle.query("get_status")
    print(f"Status: {status}")

    # Signal the workflow
    await handle.signal("update_data", {"value": 42})

    # Cancel the workflow
    await handle.cancel()

    # Wait for result (will raise Cancelled)
    try:
        result = await handle.result()
    except trio.Cancelled:
        print("Workflow was cancelled")

    await client.close()

trio.run(main)
```

**Key Differences**:
- Runtime (asyncio.run vs trio.run)
- Exception type (asyncio.CancelledError vs trio.Cancelled)

---

## 5. Worker Setup

### Asyncio (Current SDK)
```python
import asyncio
from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker

@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return f"Hello, {name}!"

async def main():
    # Connect client
    client = await Client.connect("localhost:7233")

    # Create worker
    worker = Worker(
        client,
        task_queue="greeting-queue",
        workflows=[GreetingWorkflow],
    )

    # Run worker (blocks until shutdown)
    await worker.run()

asyncio.run(main())
```

### Trio (Proposed)
```python
import trio
from temporalio_trio import workflow
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker

@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return f"Hello, {name}!"

async def main():
    # Connect client
    client = await Client.connect("localhost:7233")

    # Create worker
    worker = Worker(
        client,
        task_queue="greeting-queue",
        workflows=[GreetingWorkflow],
    )

    # Run worker (blocks until shutdown)
    await worker.run()

trio.run(main)
```

**Key Differences**:
- Import from `temporalio_trio` instead of `temporalio`
- Runtime (asyncio.run vs trio.run)
- Workflow execution uses Trio internally

---

## 6. Complete Example with Client and Worker

### Asyncio (Current SDK)
```python
import asyncio
from temporalio import workflow
from temporalio.client import Client
from temporalio.worker import Worker

@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        await asyncio.sleep(1)  # Simulate work
        return f"Hello, {name}!"

async def main():
    # Connect client
    client = await Client.connect("localhost:7233")

    # Start worker in background task
    async with asyncio.TaskGroup() as tg:
        # Create and start worker
        worker = Worker(
            client,
            task_queue="greeting-queue",
            workflows=[GreetingWorkflow],
        )
        worker_task = tg.create_task(worker.run())

        # Wait a bit for worker to start
        await asyncio.sleep(0.5)

        # Execute workflow
        result = await client.execute_workflow(
            GreetingWorkflow.run,
            "World",
            id="greeting-workflow-1",
            task_queue="greeting-queue",
        )
        print(f"Result: {result}")

        # Shutdown worker
        worker.shutdown()
        await worker_task

asyncio.run(main())
```

### Trio (Proposed)
```python
import trio
from temporalio_trio import workflow
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker

@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        await workflow.sleep(1)  # Deterministic sleep
        return f"Hello, {name}!"

async def main():
    # Connect client
    client = await Client.connect("localhost:7233")

    # Start worker in background nursery
    async with trio.open_nursery() as nursery:
        # Create and start worker
        worker = Worker(
            client,
            task_queue="greeting-queue",
            workflows=[GreetingWorkflow],
        )
        nursery.start_soon(worker.run)

        # Wait a bit for worker to start
        await trio.sleep(0.5)

        # Execute workflow
        result = await client.execute_workflow(
            GreetingWorkflow.run,
            "World",
            id="greeting-workflow-1",
            task_queue="greeting-queue",
        )
        print(f"Result: {result}")

        # Shutdown worker
        worker.shutdown()
        nursery.cancel_scope.cancel()

trio.run(main)
```

**Key Differences**:
- asyncio.TaskGroup vs trio.open_nursery (structured concurrency)
- asyncio.sleep vs workflow.sleep (deterministic in Trio workflows)
- nursery.start_soon vs tg.create_task
- nursery.cancel_scope.cancel() for cleanup

---

## 7. Current Workaround (Before Client Implementation)

### Current State (Worker Only)
```python
import asyncio
import trio
from temporalio.client import Client as AsyncioClient
from temporalio_trio import workflow
from temporalio_trio.worker import Worker

@workflow.defn
class TimerWorkflow:
    @workflow.run
    async def run(self, duration: float) -> str:
        await workflow.sleep(duration)
        return f"Slept for {duration} seconds"

async def run_worker():
    # WORKAROUND: Use asyncio client for connection
    client = asyncio.run(
        AsyncioClient.connect("localhost:7233")
    )

    # Trio worker handles workflow execution
    worker = Worker(
        client,
        task_queue="trio-queue",
        workflows=[TimerWorkflow],
    )

    try:
        await worker.run()
    except KeyboardInterrupt:
        worker.shutdown()

# Run with Trio
trio.run(run_worker)
```

**Issue**: Mixing asyncio (client) and Trio (worker) is awkward.

### After Client Implementation
```python
import trio
from temporalio_trio import workflow
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker

@workflow.defn
class TimerWorkflow:
    @workflow.run
    async def run(self, duration: float) -> str:
        await workflow.sleep(duration)
        return f"Slept for {duration} seconds"

async def main():
    # Pure Trio: client and worker
    client = await Client.connect("localhost:7233")

    worker = Worker(
        client,
        task_queue="trio-queue",
        workflows=[TimerWorkflow],
    )

    try:
        await worker.run()
    except KeyboardInterrupt:
        worker.shutdown()
    finally:
        await client.close()

# Run with Trio
trio.run(main)
```

**Benefit**: Pure Trio, no asyncio mixing required.

---

## 8. Advanced Features

### Asyncio: Workflow with Multiple Concurrent Operations
```python
import asyncio
from temporalio.client import Client

async def main():
    client = await Client.connect("localhost:7233")

    # Start multiple workflows concurrently
    handles = await asyncio.gather(
        client.start_workflow("Workflow1", id="wf-1", task_queue="queue"),
        client.start_workflow("Workflow2", id="wf-2", task_queue="queue"),
        client.start_workflow("Workflow3", id="wf-3", task_queue="queue"),
    )

    # Wait for all results
    results = await asyncio.gather(
        *[handle.result() for handle in handles]
    )

    print(f"Results: {results}")
    await client.close()

asyncio.run(main())
```

### Trio: Workflow with Multiple Concurrent Operations
```python
import trio
from temporalio_trio.client import Client

async def main():
    client = await Client.connect("localhost:7233")

    handles = []

    # Start multiple workflows concurrently
    async with trio.open_nursery() as nursery:
        async def start_workflow(name, id):
            handle = await client.start_workflow(
                name, id=id, task_queue="queue"
            )
            handles.append(handle)

        nursery.start_soon(start_workflow, "Workflow1", "wf-1")
        nursery.start_soon(start_workflow, "Workflow2", "wf-2")
        nursery.start_soon(start_workflow, "Workflow3", "wf-3")

    # Wait for all results
    results = []
    async with trio.open_nursery() as nursery:
        async def get_result(handle):
            result = await handle.result()
            results.append(result)

        for handle in handles:
            nursery.start_soon(get_result, handle)

    print(f"Results: {results}")
    await client.close()

trio.run(main)
```

**Key Difference**: Trio uses structured concurrency with nurseries instead of gather.

---

## Summary

| Aspect | Asyncio SDK | Trio SDK |
|--------|-------------|----------|
| **Runtime** | `asyncio.run()` | `trio.run()` |
| **Import** | `from temporalio.client import Client` | `from temporalio_trio.client import Client` |
| **Connection** | `await Client.connect()` | `await Client.connect()` (same) |
| **Start Workflow** | `await client.start_workflow()` | `await client.start_workflow()` (same) |
| **Execute Workflow** | `await client.execute_workflow()` | `await client.execute_workflow()` (same) |
| **Query/Signal** | `await handle.query()`, `await handle.signal()` | `await handle.query()`, `await handle.signal()` (same) |
| **Concurrency** | `asyncio.gather()`, `TaskGroup` | `trio.open_nursery()` |
| **Cancellation** | `asyncio.CancelledError` | `trio.Cancelled` |
| **Workflow Sleep** | `asyncio.sleep()` (non-deterministic) | `workflow.sleep()` (deterministic) |
| **Worker** | `from temporalio.worker import Worker` | `from temporalio_trio.worker import Worker` |

## Migration Path

For users migrating from asyncio SDK to Trio SDK:

1. **Replace imports**:
   - `temporalio` → `temporalio_trio`

2. **Replace runtime**:
   - `asyncio.run()` → `trio.run()`

3. **Replace concurrency patterns**:
   - `asyncio.gather()` → `trio.open_nursery()` with `nursery.start_soon()`
   - `asyncio.TaskGroup` → `trio.open_nursery()`

4. **Replace exceptions**:
   - `asyncio.CancelledError` → `trio.Cancelled`

5. **Update workflow code**:
   - `asyncio.sleep()` → `workflow.sleep()` (for determinism)

**API compatibility**: All Client APIs remain identical, only concurrency patterns differ.
