# MVP Proposal Validation Against Temporal Python SDK

This document validates `MVP_PROPOSAL.md` against the existing Temporal Python SDK patterns.

## Validation Goal

**Ensure architectural patterns match the SDK so features can be added later without redesign.**

MVP scope is intentionally limited (just `workflow.sleep()`), but the underlying architecture must be extensible.

## Summary

| Aspect | MVP Proposal | SDK Pattern | Status | Priority |
|--------|--------------|-------------|--------|----------|
| **Architecture (Critical)** |
| WorkflowRunner | `run()` method | `prepare_workflow()` + `create_instance()` | ❌ Divergent | **Must fix** |
| WorkflowContext | Custom dataclass | `_WorkflowInstanceImpl` extending `_Runtime` | ❌ Divergent | **Must fix** |
| Runtime pattern | Not present | `_Runtime.current()` + context var | ❌ Missing | **Must fix** |
| **API Surface (Follow SDK)** |
| Workflow decorator | `@workflow.defn` | `@workflow.defn` | ✅ Matches | - |
| Run decorator | `@workflow.run` | `@workflow.run` | ✅ Matches | - |
| Sleep API | `workflow.sleep(duration)` | `workflow.sleep(duration, *, summary=None)` | ✅ OK for MVP | Add params later |
| **Intentionally Deferred** |
| Time APIs | `workflow.time()` only | `time()` / `time_ns()` / `now()` | ⏳ Deferred | Add later |
| Random/UUID | Not present | `random()` / `uuid4()` | ⏳ Deferred | Add later |
| `workflow.info()` | Not present | `info()` -> `Info` dataclass | ⏳ Deferred | Add later |
| History events | Simple dataclasses | Protobuf messages | ⏳ Deferred | OK for MVP |
| WorkflowClock | Custom `trio.abc.Clock` | Internal time management | ✅ OK | Trio adaptation |

## Critical Architecture Patterns (Must Follow)

These patterns are **required** to ensure features can be added without redesign.

### 1. Runtime Pattern ❌ MUST FIX

The SDK uses a `_Runtime` abstract class with a `current()` static method to get the current workflow context. All workflow APIs (`sleep`, `time`, `info`, etc.) delegate to this runtime.

**SDK Pattern:**
```python
# temporalio/workflow.py

class _Runtime(ABC):
    @staticmethod
    def current() -> _Runtime:
        """Get current runtime, raise if not in workflow."""
        loop = _Runtime.maybe_current()
        if not loop:
            raise _NotInWorkflowEventLoopError("Not in workflow event loop")
        return loop

    @staticmethod
    def maybe_current() -> _Runtime | None:
        """Get current runtime or None."""
        try:
            return getattr(
                asyncio.get_running_loop(), "__temporal_workflow_runtime", None
            )
        except RuntimeError:
            return None

    # Abstract methods that implementations must provide
    @abstractmethod
    def workflow_time_ns(self) -> int: ...

    @abstractmethod
    async def workflow_sleep(self, duration: float, summary: str | None) -> None: ...

    # ... more abstract methods for each workflow API

# Public API delegates to runtime
async def sleep(duration: float | timedelta, *, summary: str | None = None) -> None:
    await _Runtime.current().workflow_sleep(duration, summary)

def time() -> float:
    return _Runtime.current().workflow_time_ns() / 1e9
```

**For Trio:** Instead of attaching to `asyncio.get_running_loop()`, use a `contextvars.ContextVar`:
```python
_current_runtime: ContextVar[_Runtime | None] = ContextVar('_current_runtime', default=None)

class _Runtime(ABC):
    @staticmethod
    def current() -> _Runtime:
        runtime = _current_runtime.get()
        if not runtime:
            raise RuntimeError("Not in workflow context")
        return runtime
```

**Why this matters:** All future workflow APIs (signals, queries, activities, child workflows) will follow this same pattern.

---

### 2. WorkflowRunner / WorkflowInstance Pattern ❌ MUST FIX

The SDK separates concerns:
- `WorkflowRunner` - Abstract factory that creates instances
- `WorkflowInstance` - Handles individual workflow activations
- `WorkflowInstanceDetails` - Immutable config passed to create instance

**SDK Pattern:**
```python
# Abstract interfaces
class WorkflowRunner(ABC):
    @abstractmethod
    def prepare_workflow(self, defn: workflow._Definition) -> None:
        """Called once per workflow type when worker starts."""
        pass

    @abstractmethod
    def create_instance(self, det: WorkflowInstanceDetails) -> WorkflowInstance:
        """Create instance for a specific workflow execution."""
        pass

class WorkflowInstance(ABC):
    @abstractmethod
    def activate(self, act: WorkflowActivation) -> WorkflowActivationCompletion:
        """Handle an activation (may be called multiple times for replay)."""
        pass

@dataclass(frozen=True)
class WorkflowInstanceDetails:
    """Immutable details for creating a workflow instance."""
    payload_converter_class: type[PayloadConverter]
    defn: workflow._Definition
    info: workflow.Info
    randomness_seed: int
    # ... more fields
```

**MVP Proposal Issue:** Combines everything into a single `WorkflowRunner.run()` method.

**Recommended Fix:**
```python
class TrioWorkflowRunner(WorkflowRunner):
    def prepare_workflow(self, defn: workflow._Definition) -> None:
        # Validate workflow is compatible with Trio
        pass

    def create_instance(self, det: WorkflowInstanceDetails) -> WorkflowInstance:
        return TrioWorkflowInstance(det)

class TrioWorkflowInstance(WorkflowInstance, _Runtime):
    def __init__(self, det: WorkflowInstanceDetails) -> None:
        self._info = det.info
        self._random = random.Random(det.randomness_seed)
        self._time_ns = 0
        # ...

    def activate(self, act: WorkflowActivation) -> WorkflowActivationCompletion:
        # Run workflow code with trio.run(deterministic=True)
        pass

    # Implement _Runtime abstract methods
    def workflow_time_ns(self) -> int:
        return self._time_ns

    async def workflow_sleep(self, duration: float, summary: str | None) -> None:
        # Trio-based sleep implementation
        pass
```

**Why this matters:** This pattern allows:
- Swapping runners (sandboxed vs unsandboxed)
- Proper activation/replay handling
- Clean separation of concerns

---

### 3. Definition Pattern ✅ OK

The decorator pattern is correct. The SDK uses `_Definition` to store metadata:

```python
@dataclass
class _Definition:
    name: str | None
    cls: type
    run_fn: Callable[..., Awaitable]
    signals: Mapping[str | None, _SignalDefinition]
    queries: Mapping[str | None, _QueryDefinition]
    # ...
```

For MVP, a simplified version is fine as long as it can be extended.

---

## Deferred Features (OK for MVP)

These features can be added later without architectural changes if the above patterns are followed:

### Sleep API Parameters

MVP can start with just `workflow.sleep(duration)`. The `summary` and `timedelta` support can be added later:
```python
# MVP
async def sleep(duration: float) -> None:
    await _Runtime.current().workflow_sleep(duration, None)

# Later
async def sleep(duration: float | timedelta, *, summary: str | None = None) -> None:
    if isinstance(duration, timedelta):
        duration = duration.total_seconds()
    await _Runtime.current().workflow_sleep(duration, summary)
```

### Additional Time APIs

MVP can have just `time()`. Add `time_ns()` and `now()` later:
```python
# All delegate to the same runtime method
def time() -> float:
    return _Runtime.current().workflow_time_ns() / 1e9

def time_ns() -> int:
    return _Runtime.current().workflow_time_ns()

def now() -> datetime:
    return datetime.fromtimestamp(time(), timezone.utc)
```

### Random/UUID, Info, Signals, Queries, etc.

All follow the same pattern - add abstract method to `_Runtime`, implement in `TrioWorkflowInstance`, expose via module function.

---

## Legacy Analysis (Reference Only)

### 4. WorkflowContext ❌

**MVP Proposal:**
```python
@dataclass
class WorkflowContext:
    workflow_id: str
    run_id: str
    current_time: float
    pending_timers: dict[int, trio.Event]
    history: list[HistoryEvent]
    history_index: int
    random_seed: int
    _random: random.Random
```

**SDK Pattern:**
```python
class _WorkflowInstanceImpl(
    WorkflowInstance, temporalio.workflow._Runtime, asyncio.AbstractEventLoop
):
    def __init__(self, det: WorkflowInstanceDetails) -> None:
        self._defn = det.defn
        self._info = det.info
        self._time_ns = 0
        self._random = random.Random(det.randomness_seed)
        self._pending_timers: dict[int, _TimerHandle] = {}
        # ... many more fields
```

**Issues:**
1. SDK uses `WorkflowInstanceDetails` dataclass for initialization parameters
2. SDK has `_info` containing `workflow_id`, `run_id`, etc.
3. SDK extends `_Runtime` abstract class
4. SDK is much more complex with interceptors, patches, signals, queries, updates

**Recommendation:**
- Create `WorkflowInstanceDetails` dataclass matching SDK
- Use `workflow.info()` pattern instead of direct attributes
- Implement as class extending abstract base, not simple dataclass

---

### 5. WorkflowRunner ❌

**MVP Proposal:**
```python
class WorkflowRunner:
    def run(
        self,
        workflow_class: type,
        history: list[HistoryEvent],
    ) -> WorkflowResult:
        return trio.run(
            self._execute_workflow,
            workflow_class,
            history,
            deterministic=True,
            random_seed=seed,
            clock=WorkflowClock(history),
        )
```

**SDK Pattern:**
```python
class WorkflowRunner(ABC):
    @abstractmethod
    def prepare_workflow(self, defn: temporalio.workflow._Definition) -> None:
        """Prepare a workflow for future execution."""
        raise NotImplementedError

    @abstractmethod
    def create_instance(self, det: WorkflowInstanceDetails) -> WorkflowInstance:
        """Create a workflow instance that can handle activations."""
        raise NotImplementedError
```

**Issues:**
1. SDK `WorkflowRunner` is abstract and creates instances
2. SDK separates preparation from execution
3. SDK `WorkflowInstance.activate()` handles individual activations
4. MVP merges everything into a single `run()` method

**Recommendation:** Match SDK pattern:
```python
class TrioWorkflowRunner(WorkflowRunner):
    def prepare_workflow(self, defn: workflow._Definition) -> None:
        # Validate workflow can run with Trio
        pass

    def create_instance(self, det: WorkflowInstanceDetails) -> WorkflowInstance:
        return TrioWorkflowInstance(det)
```

---

### 6. History Events ⚠️

**MVP Proposal:**
```python
@dataclass
class WorkflowStartedEvent:
    workflow_type: str
    random_seed: int

@dataclass
class TimerStartedEvent:
    timer_id: int
    duration: float
```

**SDK Pattern:**
Uses protobuf messages from `temporalio.api.history.v1`:
- `WorkflowExecutionStartedEventAttributes`
- `TimerStartedEventAttributes`
- `TimerFiredEventAttributes`
- etc.

**Recommendation:**
- For MVP: OK to use simplified dataclasses for testing
- Document this as a simplification
- Plan to integrate with real protobuf history for production

---

### 7. workflow.info() Missing ⚠️

**SDK Pattern:**
```python
def info() -> Info:
    """Current workflow's info."""
    return _Runtime.current().workflow_info()

@dataclass
class Info:
    workflow_id: str
    workflow_type: str
    run_id: str
    task_queue: str
    namespace: str
    # ... many more fields
```

**Recommendation:** Add `workflow.info()` returning an `Info` dataclass.

---

### 8. Random/UUID APIs Missing ⚠️

**SDK Pattern:**
```python
def random() -> Random:
    """Get a deterministic pseudo-random number generator."""

def uuid4() -> uuid.UUID:
    """Get a new, determinism-safe v4 UUID based on random()."""
```

**Recommendation:** Add both functions for deterministic randomness.

---

## Revised MVP Architecture

Based on validation, here's the recommended architecture:

```python
# temporalio_trio/workflow.py

from dataclasses import dataclass
from datetime import datetime, timedelta
from random import Random
from typing import Any
import uuid

# Match SDK's Info dataclass
@dataclass
class Info:
    workflow_id: str
    workflow_type: str
    run_id: str
    task_queue: str
    namespace: str
    # Simplified for MVP

# Match SDK's _Runtime pattern
class _Runtime:
    @staticmethod
    def current() -> "_Runtime":
        ...

    def workflow_info(self) -> Info:
        ...

    def workflow_time_ns(self) -> int:
        ...

    def workflow_random(self) -> Random:
        ...

    async def workflow_sleep(self, duration: float, summary: str | None) -> None:
        ...

# Public API - match SDK signatures exactly
def info() -> Info:
    return _Runtime.current().workflow_info()

def time() -> float:
    return _Runtime.current().workflow_time_ns() / 1e9

def time_ns() -> int:
    return _Runtime.current().workflow_time_ns()

def now() -> datetime:
    from datetime import timezone
    return datetime.fromtimestamp(time(), timezone.utc)

def random() -> Random:
    return _Runtime.current().workflow_random()

def uuid4() -> uuid.UUID:
    return uuid.UUID(int=random().getrandbits(128), version=4)

async def sleep(duration: float | timedelta, *, summary: str | None = None) -> None:
    if isinstance(duration, timedelta):
        duration = duration.total_seconds()
    await _Runtime.current().workflow_sleep(duration, summary)

# Decorators - match SDK exactly
def defn(cls=None, *, name=None, sandboxed=True):
    ...

def run(fn):
    ...
```

---

## Action Items

1. **Update MVP_PROPOSAL.md** to reflect SDK patterns
2. **Add missing APIs**: `time_ns()`, `now()`, `random()`, `uuid4()`, `info()`
3. **Add missing parameters**: `summary` to `sleep()`, `timedelta` support
4. **Redesign WorkflowRunner** to match SDK's abstract pattern
5. **Redesign WorkflowContext** to use `WorkflowInstanceDetails` pattern
6. **Document simplifications** that are intentional for MVP scope
