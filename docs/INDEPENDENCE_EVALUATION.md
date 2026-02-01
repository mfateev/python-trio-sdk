# Evaluation: Making Trio SDK Independent from sdk-python

**Date**: 2026-01-17
**Status**: Analysis Complete
**Current Dependency**: `temporalio>=1.7.0` (sdk-python)

## Executive Summary

The trio SDK currently depends on sdk-python for 3 critical components:
1. **DataConverter** - Payload serialization (CRITICAL)
2. **Protobuf Definitions** - Wire protocol with Rust bridge (CRITICAL)
3. **Client Configuration** - Connection settings (MODERATE)

**Recommendation**: Pursue a **Hybrid Independence** approach that achieves 80% independence with 20% of the effort, keeping proven protobuf definitions while replacing the client interface.

---

## Current Dependencies

### Dependency Overview

| Module | Severity | Usage | Replace? |
|--------|----------|-------|----------|
| `temporalio.converter.DataConverter` | CRITICAL | Payload serialization | Optional |
| `temporalio.bridge.proto.*` | CRITICAL | Protobuf messages | Keep |
| `temporalio.client.Client` | MODERATE | Config access only | Yes |
| `temporalio.bridge.worker` | NONE | Unused import | Remove |
| `temporalio.workflow` | NONE | Unused import | Remove |

### Dependency Graph

```
Trio SDK
├── temporalio.client.Client (config only)
│   └── Used for: .service_client.config.target_host
├── temporalio.converter.DataConverter (serialization)
│   ├── .payload_converter.from_payload()
│   └── .payload_converter.to_payload()
└── temporalio.bridge.proto.* (wire protocol)
    ├── workflow_activation_pb2.WorkflowActivation
    ├── workflow_completion_pb2.WorkflowActivationCompletion
    └── workflow_commands_pb2.WorkflowCommand
```

---

## Detailed Analysis

### 1. Critical Dependency: DataConverter

**What It Does:**
- Converts Python objects ↔ Temporal Payload (protobuf)
- Handles serialization for workflow arguments and results
- Supports custom type codecs (JSON, msgpack, etc.)

**Current Usage:**

```python
# In _bridge_types.py:108
for payload in init.arguments:
    value = data_converter.payload_converter.from_payload(payload)
    values.append(value)

# In _bridge_types.py:167
payload = data_converter.payload_converter.to_payload(cmd.result)
bridge_cmd.complete_workflow_execution.result.CopyFrom(payload)
```

**Files Using It:**
- `temporalio_trio/worker/_worker.py` - Initialization
- `temporalio_trio/bridge_worker.py` - Passing to bridge
- `temporalio_trio/worker/_bridge_types.py` - Actual conversion

**Why It's Hard to Replace:**
- Complex type system (handles dataclasses, enums, etc.)
- Custom codec support
- Binary Payload format (protobuf with metadata)
- Edge cases for None, bytes, datetime, etc.

**Replacement Effort**: 2-3 weeks
- Need custom codec system
- Must handle all Python types
- Need to maintain compatibility with Temporal Payloads

---

### 2. Critical Dependency: Protobuf Definitions

**What It Does:**
- Defines message format between Python and Rust bridge
- Wire protocol for SDK Core communication
- Type-safe serialization/deserialization

**Current Usage:**

```python
# In bridge_worker.py:120
bridge_act = act_pb.WorkflowActivation()
bridge_act.ParseFromString(bridge_act_bytes)

# In _bridge_types.py:155
bridge_cmd = cmd_pb.WorkflowCommand()
bridge_cmd.start_timer.seq = cmd.timer_id
```

**Messages Used:**
- `WorkflowActivation` - Incoming activations from bridge
- `WorkflowActivationCompletion` - Outgoing completions
- `WorkflowCommand` - Commands (StartTimer, CompleteWorkflow, etc.)
- `InitializeWorkflow` - Workflow start event
- `FireTimer` - Timer fired event

**Why It's Hard to Replace:**
- SDK Core expects specific protobuf format
- Would require changes to Rust bridge
- Proven, efficient wire format
- Already generated from `.proto` files

**Replacement Effort**: 3-4 weeks (if changing wire protocol)
- Alternative: Keep protobuf, generate from upstream `.proto` files
- Better: Keep as-is, it's language-neutral and proven

---

### 3. Moderate Dependency: Client Configuration

**What It Does:**
- Provides connection configuration (target_host, namespace)
- Used only to extract server address

**Current Usage:**

```python
# In _worker.py:202
target_host = self._client.service_client.config.target_host
```

**Why It's Moderate:**
- Only reads config, doesn't use client functionality
- Simple to replace with custom config object
- No deep integration

**Replacement Effort**: 1-2 days

**Proposed Replacement:**

```python
@dataclass
class TemporalConnectionConfig:
    """Connection configuration for Temporal server."""
    target_url: str
    namespace: str
    identity: str = ""

class Worker:
    def __init__(
        self,
        connection: TemporalConnectionConfig,  # Instead of Client
        task_queue: str,
        ...
    ):
        self._connection = connection
```

---

## Independence Strategies

### Strategy A: Minimal Independence (Easy Path)

**Keep**: Protobuf definitions, DataConverter
**Replace**: Client config only
**Remove**: Unused imports

**Changes:**
1. Create `TemporalConnectionConfig` dataclass
2. Replace `Client` parameter with config
3. Remove unused imports (`temporalio.bridge.worker`, `temporalio.workflow`)

**Effort**: 1-2 days
**Result**: Still depends on temporalio for DataConverter and protobufs
**Benefit**: Simpler Worker API, no client dependency

**Pros:**
- Minimal code changes
- Keeps proven serialization
- Reduces dependency surface

**Cons:**
- Still depends on sdk-python package
- Can't ship independently

---

### Strategy B: Full Independence (Hard Path)

**Replace**: Everything
**Create**: Custom DataConverter, custom protobuf generation

**Changes:**
1. Implement custom Payload codec system
2. Generate protobuf from upstream `.proto` files
3. Replace Client with config
4. Maintain codec for all Python types

**Effort**: 3-4 weeks
**Result**: Zero dependency on sdk-python
**Benefit**: Complete independence

**Pros:**
- Truly independent package
- Full control over serialization
- Can ship without sdk-python

**Cons:**
- High maintenance burden
- Must handle codec edge cases
- Risk of bugs in serialization
- Needs comprehensive tests

---

### Strategy C: Hybrid Independence (Recommended)

**Keep**: Protobuf definitions (proven format)
**Replace**: DataConverter with simple wrapper, Client config
**Simplify**: Use JSON-only codec initially

**Changes:**
1. Create lightweight `PayloadCodec` for basic types
2. Keep protobuf definitions (copy from sdk-python or generate)
3. Replace Client with config
4. Document that advanced codecs require sdk-python

**Effort**: 3-5 days
**Result**: 80% independent, optional sdk-python for advanced features
**Benefit**: Pragmatic balance

**Implementation:**

```python
# temporalio_trio/converter.py
class SimplePayloadConverter:
    """Lightweight converter for basic Python types."""

    def to_payload(self, value: Any) -> temporalio.api.common.v1.Payload:
        if value is None:
            return Payload(metadata={"encoding": b"binary/null"})
        elif isinstance(value, (str, int, float, bool)):
            # JSON encoding
            data = json.dumps(value).encode()
            return Payload(
                metadata={"encoding": b"json/plain"},
                data=data
            )
        else:
            raise ValueError(f"Unsupported type: {type(value)}")

    def from_payload(self, payload: temporalio.api.common.v1.Payload) -> Any:
        encoding = payload.metadata.get("encoding", b"").decode()
        if encoding == "binary/null":
            return None
        elif encoding == "json/plain":
            return json.loads(payload.data)
        else:
            raise ValueError(f"Unsupported encoding: {encoding}")

# For advanced use cases, allow plugging in sdk-python's converter
class DataConverter:
    def __init__(self, payload_converter=None):
        self.payload_converter = payload_converter or SimplePayloadConverter()
```

**Pros:**
- Achieves reasonable independence
- Keeps proven wire protocol
- Simpler than full reimplementation
- Can still use sdk-python for advanced cases

**Cons:**
- Simple codec doesn't handle all types initially
- Protobuf definitions still come from upstream

---

## Effort Estimates

### Breakdown by Task

| Task | Effort | Impact |
|------|--------|--------|
| Remove unused imports | 30 min | Cleanup |
| Create ConnectionConfig | 2 hours | Removes Client dependency |
| Simple PayloadConverter | 1 day | Basic serialization |
| Protobuf generation | 1 day | Keep wire protocol |
| Test coverage | 1 day | Ensure correctness |
| Documentation | 1 day | Migration guide |
| **Total (Strategy C)** | **3-5 days** | **80% independence** |

### Comparison

| Strategy | Effort | Independence | Risk | Maintenance |
|----------|--------|--------------|------|-------------|
| A: Minimal | 1-2 days | 40% | Low | Low |
| B: Full | 3-4 weeks | 100% | High | High |
| C: Hybrid | 3-5 days | 80% | Medium | Medium |

---

## Recommended Path Forward

### Phase 1: Quick Wins (1-2 days)

1. **Remove unused imports**
   ```python
   # Delete these:
   import temporalio.bridge.worker
   import temporalio.workflow
   ```

2. **Create ConnectionConfig**
   ```python
   @dataclass
   class TemporalConnectionConfig:
       target_url: str
       namespace: str
       identity: str = ""
       tls_root_cas: bytes | None = None
   ```

3. **Update Worker constructor**
   ```python
   def __init__(
       self,
       connection: TemporalConnectionConfig,
       task_queue: str,
       workflows: Sequence[Type],
       data_converter: Optional[DataConverter] = None,
       ...
   ):
   ```

### Phase 2: Simple Codec (2-3 days)

1. **Implement SimplePayloadConverter** (JSON-based)
2. **Add tests for basic types** (str, int, float, bool, None, list, dict)
3. **Document limitations** (no dataclasses, custom types initially)

### Phase 3: Protobuf Independence (Optional, 1-2 days)

1. **Copy or generate protobuf definitions**
   - Option A: Copy `.py` files from sdk-python
   - Option B: Generate from `.proto` files in SDK Core
2. **Update imports** to use local protobufs
3. **Add CI step** to verify protobuf compatibility

### Phase 4: Advanced Codec (Optional, 1-2 weeks)

1. **Add dataclass support** to PayloadConverter
2. **Add datetime, UUID, bytes** support
3. **Add custom type registry**
4. **Maintain compatibility** with sdk-python format

---

## Migration Guide (for Users)

### Before (Current):

```python
from temporalio.client import Client
from temporalio_trio.worker import Worker

client = await Client.connect("localhost:7233")

worker = Worker(
    client,
    task_queue="my-queue",
    workflows=[MyWorkflow],
)
```

### After (Strategy C):

```python
from temporalio_trio.worker import Worker
from temporalio_trio.connection import TemporalConnectionConfig

connection = TemporalConnectionConfig(
    target_url="http://localhost:7233",
    namespace="default",
)

worker = Worker(
    connection,
    task_queue="my-queue",
    workflows=[MyWorkflow],
)
```

**Benefits:**
- No need to create asyncio Client
- Simpler API
- Pure Trio (no asyncio)

---

## Risk Analysis

### Risks of Full Independence

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| Codec bugs | High | High | Extensive test coverage |
| Type edge cases | High | Medium | Progressive enhancement |
| Payload incompatibility | Medium | High | Validate against sdk-python |
| Maintenance burden | High | Medium | Keep codec simple |
| Breaking changes in Core | Medium | High | Pin protobuf versions |

### Risks of Staying Dependent

| Risk | Probability | Impact | Mitigation |
|------|------------|--------|------------|
| sdk-python API changes | Low | Medium | Pin version |
| Trio incompatibility | Low | High | Integration tests |
| Can't ship independently | High | Low | Accept constraint |

---

## Recommendations

### Immediate Actions (Next Sprint)

1. ✅ **Remove unused imports** (30 min)
2. ✅ **Create ConnectionConfig** (2 hours)
3. ✅ **Update Worker API** (4 hours)
4. **Add migration guide** (2 hours)

### Short-Term (Next Month)

5. **Implement SimplePayloadConverter** (1 day)
6. **Add basic type tests** (1 day)
7. **Document codec limitations** (1 day)

### Medium-Term (Next Quarter)

8. **Evaluate protobuf generation** (1 day)
9. **Add advanced codec features** (1 week)
10. **Full test coverage** (1 week)

### Decision Points

**After Phase 1**: Can ship with simplified API, still depends on sdk-python
**After Phase 2**: Can handle basic workflows independently (80% use cases)
**After Phase 3**: Fully independent package (100% use cases)

---

## Conclusion

**Strategy C (Hybrid Independence)** offers the best balance:
- **3-5 days effort** for 80% independence
- Keeps proven wire protocol (protobuf)
- Simple codec handles most use cases
- Optional advanced features via sdk-python

This approach allows the trio SDK to ship as a mostly-independent package while maintaining stability and avoiding complex reimplementation of edge cases.

**Next Step**: Implement Phase 1 (Quick Wins) to remove Client dependency and simplify the API.
