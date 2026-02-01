# Client Implementation Plan

**Status**: Planning
**Priority**: Medium (Worker currently uses asyncio Client)
**Estimated Effort**: 2-3 weeks

## Overview

This document outlines the plan to implement a native Trio-based `Client` class for the temporalio-trio SDK. Currently, the Worker accepts an asyncio-based `temporalio.client.Client`, which creates a dependency mismatch. A native Trio client would:

- Enable pure Trio applications (no asyncio required)
- Provide proper cancellation semantics
- Improve performance by avoiding asyncio/trio bridge overhead
- Allow full control over connection management

## Current State

### Current Usage Pattern

```python
from temporalio.client import Client  # asyncio-based
from temporalio_trio.worker import Worker

# User must use asyncio client
client = await Client.connect("localhost:7233")

# Worker only reads config
worker = Worker(
    client,  # Only accesses: client.service_client.config.target_host
    task_queue="my-queue",
    workflows=[MyWorkflow],
)
```

**Problem**: Mixing asyncio and trio in user code.

### What We Need From Client

Currently, Worker only accesses:
```python
target_host = self._client.service_client.config.target_host
```

**But for a full Client**, we need:
- Connection to Temporal server (gRPC)
- Workflow start/query/signal operations
- Activity operations
- Schedule operations
- Namespace operations

## Goals

### MVP (Phase 1): Basic Client
- [x] Connection to Temporal server
- [ ] Start workflow execution
- [ ] Query workflow
- [ ] Signal workflow
- [ ] Get workflow handle
- [ ] Terminate/cancel workflow

### Full Featured (Phase 2): Complete Client
- [ ] Schedule management
- [ ] Activity operations
- [ ] Batch operations
- [ ] Search attributes
- [ ] Custom data converters
- [ ] Interceptors

### Optional (Phase 3): Advanced Features
- [ ] Cloud client support
- [ ] mTLS configuration
- [ ] Advanced retry policies
- [ ] Connection pooling

## Architecture

### Component Structure

```
temporalio_trio/
└── client/
    ├── __init__.py
    ├── _client.py           # Main Client class
    ├── _connection.py       # Connection management
    ├── _workflow_handle.py  # WorkflowHandle, WorkflowExecutionHandle
    ├── _async_activity.py   # AsyncActivityHandle
    └── _interceptor.py      # Interceptor interfaces
```

### Class Hierarchy

```python
# temporalio_trio/client/_client.py
class Client:
    """Trio-based Temporal client."""

    @staticmethod
    async def connect(
        target_host: str,
        *,
        namespace: str = "default",
        # ... other params matching SDK
    ) -> Client:
        """Connect to Temporal server using Trio."""
        ...

    async def start_workflow(
        self,
        workflow: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        id: str,
        task_queue: str,
        # ... other params
    ) -> WorkflowHandle:
        """Start a workflow and return handle."""
        ...

    def get_workflow_handle(
        self,
        workflow_id: str,
        *,
        run_id: Optional[str] = None,
    ) -> WorkflowHandle:
        """Get handle to existing workflow."""
        ...

# temporalio_trio/client/_workflow_handle.py
class WorkflowHandle:
    """Handle to a workflow execution."""

    async def result(self) -> Any:
        """Wait for workflow to complete and return result."""
        ...

    async def query(self, query: str, *args: Any) -> Any:
        """Query the workflow."""
        ...

    async def signal(self, signal: str, *args: Any) -> None:
        """Send signal to workflow."""
        ...

    async def cancel(self) -> None:
        """Cancel the workflow."""
        ...
```

## Implementation Phases

### Phase 1: Connection & Basic Operations (1 week)

#### Step 1.1: Connection Management

**Goal**: Connect to Temporal server using Trio networking

**Files**:
- `temporalio_trio/client/_connection.py`

**Components**:
```python
@dataclass
class ConnectionConfig:
    """Configuration for Temporal connection."""
    target_host: str
    namespace: str = "default"
    tls: Optional[TLSConfig] = None
    identity: Optional[str] = None
    api_key: Optional[str] = None
    retry_config: Optional[RetryConfig] = None

class TrioConnection:
    """Manages gRPC connection using Trio."""

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self._channel: Optional[grpc.aio.Channel] = None

    async def connect(self) -> None:
        """Establish connection to Temporal server."""
        # Use grpc.aio with trio event loop
        ...

    async def close(self) -> None:
        """Close connection gracefully."""
        ...
```

**Challenges**:
- gRPC-python doesn't have native Trio support
- Options:
  1. Use `grpclib` (pure Python, trio-compatible)
  2. Use `grpc.aio` with sniffio bridge
  3. Use temporalio-sdk-core bridge (like Worker does)

**Recommended**: Option 3 - Use SDK Core bridge
- Consistent with Worker implementation
- Proven to work with Trio
- Maintains compatibility with SDK Core

#### Step 1.2: Client Class

**Goal**: Implement basic Client class

**Files**:
- `temporalio_trio/client/_client.py`
- `temporalio_trio/client/__init__.py`

**Implementation**:
```python
class Client:
    """Trio-based Temporal client."""

    @staticmethod
    async def connect(
        target_host: str,
        *,
        namespace: str = "default",
        tls: Union[bool, TLSConfig] = False,
        data_converter: Optional[DataConverter] = None,
        identity: Optional[str] = None,
    ) -> Client:
        """Connect to Temporal server.

        Uses SDK Core bridge for connection, same as Worker.
        """
        # Similar to Worker bridge setup
        bridge = TrioBridgeWrapper()
        await bridge.start()

        # Initialize bridge with connection config
        await bridge.initialize_with_client(
            target_url=f"http://{target_host}",
            namespace=namespace,
            identity=identity or "trio-client",
            # ... other config
        )

        return Client(bridge, namespace, data_converter or DataConverter())

    def __init__(
        self,
        bridge: TrioBridgeWrapper,
        namespace: str,
        data_converter: DataConverter,
    ):
        self._bridge = bridge
        self._namespace = namespace
        self._data_converter = data_converter

    async def close(self) -> None:
        """Close client connection."""
        await self._bridge.shutdown()
```

**Tests**:
- [ ] Connection to local Temporal server
- [ ] Connection with TLS
- [ ] Connection with API key
- [ ] Connection failure handling
- [ ] Client close/cleanup

#### Step 1.3: Start Workflow

**Goal**: Start workflow execution

**Implementation**:
```python
class Client:
    async def start_workflow(
        self,
        workflow: str,
        arg: Any = temporalio.common._arg_unset,
        *,
        id: str,
        task_queue: str,
        execution_timeout: Optional[timedelta] = None,
        run_timeout: Optional[timedelta] = None,
        task_timeout: Optional[timedelta] = None,
        id_reuse_policy: temporalio.common.WorkflowIDReusePolicy = ...,
        retry_policy: Optional[temporalio.common.RetryPolicy] = None,
        cron_schedule: str = "",
        memo: Optional[Mapping[str, Any]] = None,
        search_attributes: Optional[temporalio.common.TypedSearchAttributes] = None,
    ) -> WorkflowHandle:
        """Start workflow execution."""

        # Convert arguments to payloads
        input_payloads = []
        if arg is not temporalio.common._arg_unset:
            payload = self._data_converter.to_payload(arg)
            input_payloads.append(payload)

        # Build start workflow request (protobuf)
        request = temporalio.api.workflowservice.v1.StartWorkflowExecutionRequest(
            namespace=self._namespace,
            workflow_id=id,
            workflow_type=temporalio.api.common.v1.WorkflowType(name=workflow),
            task_queue=temporalio.api.taskqueue.v1.TaskQueue(name=task_queue),
            input=input_payloads,
            # ... other fields
        )

        # Send via bridge
        response_bytes = await self._bridge.start_workflow_execution(
            request.SerializeToString()
        )

        # Parse response
        response = temporalio.api.workflowservice.v1.StartWorkflowExecutionResponse()
        response.ParseFromString(response_bytes)

        # Return workflow handle
        return WorkflowHandle(
            client=self,
            workflow_id=id,
            run_id=response.run_id,
        )
```

**Bridge Support Needed**:
```rust
// In temporalio_trio_bridge/src/bridge.rs
impl TrioAsyncBridge {
    pub fn start_workflow_execution(
        &self,
        request: &[u8],
        callback: PyObject,
    ) -> PyResult<String> {
        // Send StartWorkflowExecutionRequest via SDK Core
        // Return response via callback
    }
}
```

**Tests**:
- [ ] Start simple workflow
- [ ] Start workflow with arguments
- [ ] Start workflow with all options
- [ ] Workflow ID already exists handling
- [ ] Invalid workflow type handling

### Phase 2: Workflow Handles (3-4 days)

#### Step 2.1: WorkflowHandle

**Goal**: Interact with running workflows

**Files**:
- `temporalio_trio/client/_workflow_handle.py`

**Implementation**:
```python
@dataclass
class WorkflowHandle:
    """Handle to a workflow execution."""

    _client: Client
    workflow_id: str
    run_id: Optional[str]
    result_run_id: Optional[str] = None
    first_execution_run_id: Optional[str] = None

    async def result(
        self,
        *,
        follow_runs: bool = True,
        rpc_timeout: Optional[timedelta] = None,
    ) -> Any:
        """Wait for workflow to complete and return result.

        Uses long polling on GetWorkflowExecutionHistory.
        """
        # Poll for workflow completion
        while True:
            history = await self._fetch_history()

            # Check if workflow completed
            for event in history.events:
                if event.HasField("workflow_execution_completed"):
                    # Extract result from completion event
                    result_payload = event.workflow_execution_completed.result
                    return self._client._data_converter.from_payload(result_payload)

                elif event.HasField("workflow_execution_failed"):
                    # Workflow failed, raise exception
                    raise WorkflowFailedException(...)

            # Wait before next poll (use trio.sleep)
            await trio.sleep(1.0)

    async def query(
        self,
        query: str,
        *args: Any,
        reject_condition: Optional[QueryRejectCondition] = None,
    ) -> Any:
        """Query the workflow."""
        # Convert args to payloads
        input_payloads = [
            self._client._data_converter.to_payload(arg)
            for arg in args
        ]

        # Build query request
        request = temporalio.api.workflowservice.v1.QueryWorkflowRequest(
            namespace=self._client._namespace,
            execution=temporalio.api.common.v1.WorkflowExecution(
                workflow_id=self.workflow_id,
                run_id=self.run_id or "",
            ),
            query=temporalio.api.query.v1.WorkflowQuery(
                query_type=query,
                query_args=input_payloads,
            ),
        )

        # Send via bridge
        response_bytes = await self._client._bridge.query_workflow(
            request.SerializeToString()
        )

        # Parse and return result
        response = temporalio.api.workflowservice.v1.QueryWorkflowResponse()
        response.ParseFromString(response_bytes)

        return self._client._data_converter.from_payload(response.query_result)

    async def signal(
        self,
        signal: str,
        *args: Any,
    ) -> None:
        """Send signal to workflow."""
        input_payloads = [
            self._client._data_converter.to_payload(arg)
            for arg in args
        ]

        request = temporalio.api.workflowservice.v1.SignalWorkflowExecutionRequest(
            namespace=self._client._namespace,
            workflow_execution=temporalio.api.common.v1.WorkflowExecution(
                workflow_id=self.workflow_id,
                run_id=self.run_id or "",
            ),
            signal_name=signal,
            input=input_payloads,
        )

        await self._client._bridge.signal_workflow(
            request.SerializeToString()
        )

    async def cancel(self) -> None:
        """Request cancellation of workflow."""
        request = temporalio.api.workflowservice.v1.RequestCancelWorkflowExecutionRequest(
            namespace=self._client._namespace,
            workflow_execution=temporalio.api.common.v1.WorkflowExecution(
                workflow_id=self.workflow_id,
                run_id=self.run_id or "",
            ),
        )

        await self._client._bridge.cancel_workflow(
            request.SerializeToString()
        )

    async def terminate(
        self,
        reason: Optional[str] = None,
        details: Sequence[Any] = [],
    ) -> None:
        """Terminate workflow execution."""
        ...
```

**Tests**:
- [ ] Get workflow result (success)
- [ ] Get workflow result (failure)
- [ ] Query workflow
- [ ] Signal workflow
- [ ] Cancel workflow
- [ ] Terminate workflow

### Phase 3: Bridge Integration (2-3 days)

#### Step 3.1: Extend Rust Bridge

**Goal**: Add client operations to Rust bridge

**Files**:
- `temporalio_trio_bridge/src/bridge.rs`
- `temporalio_trio_bridge/src/client_worker.rs` (new)

**New Operations**:
```rust
// In bridge.rs
impl TrioAsyncBridge {
    // Existing: poll_workflow_activation, complete_workflow_activation

    // New client operations:
    pub fn start_workflow_execution(
        &self,
        request: &[u8],
        callback: PyObject,
    ) -> PyResult<String> {
        let req_id = uuid::Uuid::new_v4().to_string();
        let request = Request::new(
            req_id.clone(),
            "start_workflow",
            request.to_vec(),
            callback,
        );
        self.request_tx.lock().send(request)?;
        Ok(req_id)
    }

    pub fn query_workflow(
        &self,
        request: &[u8],
        callback: PyObject,
    ) -> PyResult<String> {
        // Similar pattern
    }

    pub fn signal_workflow(
        &self,
        request: &[u8],
        callback: PyObject,
    ) -> PyResult<String> {
        // Similar pattern
    }

    pub fn cancel_workflow(
        &self,
        request: &[u8],
        callback: PyObject,
    ) -> PyResult<String> {
        // Similar pattern
    }

    pub fn get_workflow_history(
        &self,
        request: &[u8],
        callback: PyObject,
    ) -> PyResult<String> {
        // For workflow.result() polling
    }
}
```

**Handler in Request Loop**:
```rust
// In bridge.rs request handling loop
match request.operation.as_str() {
    "start_workflow" => {
        let req = StartWorkflowExecutionRequest::decode(request.data)?;
        let response = worker.start_workflow_execution(req).await?;
        let response_bytes = response.encode_to_vec();
        // Deliver via callback
    }
    "query_workflow" => {
        // ...
    }
    // ... other operations
}
```

#### Step 3.2: Python Bridge Wrapper

**Goal**: Add client operations to Python bridge wrapper

**Files**:
- `temporalio_trio/_async_bridge.py`

**New Methods**:
```python
class TrioBridgeWrapper:
    # Existing: poll_workflow_activation, complete_workflow_activation

    async def start_workflow_execution(
        self,
        request_bytes: bytes,
        timeout: Optional[float] = None,
    ) -> bytes:
        """Start workflow execution via bridge."""
        event = trio.Event()
        result_container = []
        error_container = []

        def deliver_result(result) -> None:
            try:
                if not result.success:
                    error_container.append(
                        RuntimeError(result.error or "Unknown error")
                    )
                else:
                    result_container.append(result.get_data())
            finally:
                trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._rust_bridge.start_workflow_execution(request_bytes, deliver_result)

        if timeout:
            with trio.move_on_after(timeout) as cancel_scope:
                await event.wait()
            if cancel_scope.cancelled_caught:
                raise TimeoutError("Start workflow timed out")
        else:
            await event.wait()

        if error_container:
            raise error_container[0]

        return result_container[0]

    # Similar methods for:
    # - query_workflow
    # - signal_workflow
    # - cancel_workflow
    # - get_workflow_history
```

### Phase 4: Testing & Documentation (2-3 days)

#### Step 4.1: Unit Tests

**Files**:
- `tests/test_client.py`
- `tests/test_workflow_handle.py`

**Test Cases**:
```python
# tests/test_client.py

async def test_client_connect():
    """Test client connection."""
    client = await Client.connect("localhost:7233")
    assert client._namespace == "default"
    await client.close()

async def test_client_connect_custom_namespace():
    """Test client with custom namespace."""
    client = await Client.connect(
        "localhost:7233",
        namespace="custom",
    )
    assert client._namespace == "custom"
    await client.close()

@pytest.mark.trio
async def test_start_workflow():
    """Test starting a workflow."""
    client = await Client.connect("localhost:7233")

    handle = await client.start_workflow(
        "MyWorkflow",
        id="test-workflow-id",
        task_queue="test-queue",
    )

    assert handle.workflow_id == "test-workflow-id"
    assert handle.run_id is not None

    await client.close()

@pytest.mark.trio
async def test_workflow_result():
    """Test getting workflow result."""
    client = await Client.connect("localhost:7233")

    handle = await client.start_workflow(
        "SimpleWorkflow",
        "Hello",
        id="test-result-workflow",
        task_queue="test-queue",
    )

    result = await handle.result()
    assert result == "Hello, World!"

    await client.close()
```

#### Step 4.2: Integration Tests

**Files**:
- `tests/test_client_e2e.py`

**Test Scenarios**:
- [ ] Full workflow lifecycle (start → query → signal → result)
- [ ] Workflow with activities
- [ ] Workflow cancellation
- [ ] Workflow failure handling
- [ ] Multiple concurrent workflows
- [ ] Client connection pooling

#### Step 4.3: Documentation

**Files**:
- `README.md` (update)
- `docs/CLIENT_API.md` (new)
- `examples/client_usage.py` (new)

**Example Usage**:
```python
# examples/client_usage.py
import trio
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker

# Define workflow
from temporalio_trio import workflow

@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        return f"Hello, {name}!"

async def main():
    # Connect client
    client = await Client.connect("localhost:7233")

    # Start worker in background
    async with trio.open_nursery() as nursery:
        worker = Worker(
            client,
            task_queue="greeting-queue",
            workflows=[GreetingWorkflow],
        )
        nursery.start_soon(worker.run)

        # Start workflow
        handle = await client.start_workflow(
            GreetingWorkflow.run,
            "World",
            id="greeting-workflow-1",
            task_queue="greeting-queue",
        )

        # Get result
        result = await handle.result()
        print(f"Workflow result: {result}")

        # Cleanup
        await client.close()
        worker.shutdown()

if __name__ == "__main__":
    trio.run(main)
```

## API Compatibility

### Match SDK Client API

The Trio client should match the asyncio SDK client API as closely as possible:

**SDK (asyncio)**:
```python
client = await Client.connect("localhost:7233")
handle = await client.start_workflow(MyWorkflow.run, "arg", ...)
result = await handle.result()
```

**Trio SDK**:
```python
client = await Client.connect("localhost:7233")  # Same
handle = await client.start_workflow(MyWorkflow.run, "arg", ...)  # Same
result = await handle.result()  # Same
```

**Differences**:
- Import path: `temporalio_trio.client` vs `temporalio.client`
- Internal implementation: Trio vs asyncio
- Everything else: Identical API

## Migration Path

### Current (Using asyncio Client)

```python
import asyncio
from temporalio.client import Client
from temporalio_trio.worker import Worker

# Must mix asyncio and trio
async def main():
    client = await Client.connect("localhost:7233")  # asyncio
    worker = Worker(client, ...)  # trio
    ...

asyncio.run(main())
```

### Future (Pure Trio)

```python
import trio
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker

# Pure trio
async def main():
    client = await Client.connect("localhost:7233")  # trio
    worker = Worker(client, ...)  # trio
    ...

trio.run(main)
```

## Open Questions

1. **gRPC Integration**: How to best integrate gRPC with Trio?
   - Option A: grpclib (pure Python, trio-compatible)
   - Option B: grpc.aio with bridge
   - Option C: SDK Core bridge (recommended)

2. **Connection Pooling**: Should we implement connection pooling?
   - Defer to Phase 3 (optional)

3. **Interceptors**: How to handle client interceptors?
   - Phase 2 feature
   - Need to define Trio-specific interceptor interface

4. **Backward Compatibility**: Should we support asyncio Client?
   - Keep for now, deprecate later
   - Users can choose based on their needs

## Dependencies

### New Dependencies
- None (reuses existing dependencies)

### Modified Components
- `TrioBridgeWrapper` - Add client operations
- `TrioAsyncBridge` (Rust) - Add client gRPC calls

## Success Criteria

- [ ] Client can connect to Temporal server
- [ ] Can start workflow executions
- [ ] Can query/signal running workflows
- [ ] Can wait for workflow results
- [ ] Can cancel/terminate workflows
- [ ] All operations use native Trio (no asyncio)
- [ ] 90%+ test coverage
- [ ] API matches SDK client
- [ ] Documentation complete
- [ ] Example code works

## Timeline

| Phase | Duration | Deliverable |
|-------|----------|-------------|
| Phase 1 | 1 week | Connection + start_workflow |
| Phase 2 | 3-4 days | WorkflowHandle (query/signal/result) |
| Phase 3 | 2-3 days | Bridge integration |
| Phase 4 | 2-3 days | Tests + docs |
| **Total** | **2-3 weeks** | **Full client implementation** |

## Future Enhancements

After MVP, consider:

1. **Advanced Features**:
   - Schedule management
   - Batch operations
   - Search attributes
   - Update workflow

2. **Performance**:
   - Connection pooling
   - Request batching
   - Caching

3. **Cloud Support**:
   - Cloud API key handling
   - Region-specific endpoints
   - mTLS configuration

4. **Developer Experience**:
   - Better type hints
   - Context managers for cleanup
   - Async iterators for history

## References

- SDK Client: `temporalio/client.py`
- SDK Service: `temporalio/service.py`
- gRPC Service Definitions: `temporalio/api/workflowservice/v1/`
- Trio Networking: https://trio.readthedocs.io/en/stable/reference-io.html
