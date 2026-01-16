# MVP Proposal: Trio-Based Workflow Execution

## Goal

Prove that Trio can serve as the async runtime for deterministic Temporal workflow execution by implementing a minimal proof of concept.

## Scope

**In Scope:**
- Workflow definition and execution
- `workflow.sleep()` with timer-based wake-up
- Deterministic replay of workflow history
- Multiple concurrent workflows in parallel threads

**Out of Scope (for MVP):**
- Temporal client/server communication
- Activities
- Signals and queries
- Child workflows
- Workflow cancellation
- Serialization/deserialization

## Success Criteria

1. A workflow can call `await workflow.sleep(duration)` and wake up after the specified time
2. Replaying the same workflow history produces identical execution
3. Multiple workflows can run in parallel with isolated, deterministic execution
4. The implementation uses Trio primitives (nurseries, cancel scopes) internally
5. **Architecture matches the existing Python SDK patterns** (enabling future features without redesign)

## Architecture

**Design Principle:** Follow the existing Temporal Python SDK architecture. The only difference is using Trio instead of asyncio for async handling.

### Components

```
┌─────────────────────────────────────────────────────────────────────┐
│                        TrioWorkflowRunner                            │
│  implements: WorkflowRunner                                          │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  prepare_workflow(defn) - validate workflow definition          │ │
│  │  create_instance(det) -> TrioWorkflowInstance                   │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      TrioWorkflowInstance                            │
│  implements: WorkflowInstance, _Runtime                              │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  activate(activation) -> completion                             │ │
│  │    └── trio.run(deterministic=True, random_seed=seed)          │ │
│  │        └── Sets _current_runtime context var                    │ │
│  │            └── Executes user workflow code                      │ │
│  │                                                                 │ │
│  │  # _Runtime implementation                                      │ │
│  │  workflow_time_ns() -> int                                      │ │
│  │  workflow_sleep(duration, summary) -> None                      │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                                   │
                                   ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        workflow module API                           │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │  sleep(duration) -> _Runtime.current().workflow_sleep(...)      │ │
│  │  time() -> _Runtime.current().workflow_time_ns() / 1e9          │ │
│  │  defn, run decorators                                           │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Classes (Following SDK Patterns)

#### 1. `_Runtime` (Abstract Base)

All workflow APIs delegate to `_Runtime.current()`. This pattern enables adding features without changing the public API.

```python
from abc import ABC, abstractmethod
from contextvars import ContextVar

# Context variable for current runtime (Trio doesn't have event loops like asyncio)
_current_runtime: ContextVar[_Runtime | None] = ContextVar(
    '_current_runtime', default=None
)

class _Runtime(ABC):
    """Abstract runtime that provides workflow APIs.

    Mirrors temporalio.workflow._Runtime from the SDK.
    """

    @staticmethod
    def current() -> _Runtime:
        """Get the current runtime, raising if not in a workflow context."""
        runtime = _current_runtime.get()
        if runtime is None:
            raise RuntimeError("Not in workflow context")
        return runtime

    @staticmethod
    def maybe_current() -> _Runtime | None:
        """Get the current runtime or None."""
        return _current_runtime.get()

    # Abstract methods - implement in TrioWorkflowInstance
    @abstractmethod
    def workflow_time_ns(self) -> int:
        """Get current workflow time in nanoseconds."""
        ...

    @abstractmethod
    async def workflow_sleep(self, duration: float, summary: str | None) -> None:
        """Sleep for the given duration."""
        ...
```

#### 2. `WorkflowRunner` (Abstract) and `TrioWorkflowRunner`

Factory pattern that creates workflow instances. Matches SDK's `WorkflowRunner` interface.

```python
from abc import ABC, abstractmethod

class WorkflowRunner(ABC):
    """Abstract runner for workflows.

    Mirrors temporalio.worker.WorkflowRunner from the SDK.
    """

    @abstractmethod
    def prepare_workflow(self, defn: _Definition) -> None:
        """Prepare a workflow definition for execution.

        Called once per workflow type when worker starts.
        """
        ...

    @abstractmethod
    def create_instance(self, det: WorkflowInstanceDetails) -> WorkflowInstance:
        """Create a workflow instance for execution.

        Called for each workflow execution.
        """
        ...


class TrioWorkflowRunner(WorkflowRunner):
    """Workflow runner that uses Trio for async execution."""

    def prepare_workflow(self, defn: _Definition) -> None:
        """Validate workflow is compatible with Trio execution."""
        # For MVP, just validate basic structure
        if not defn.run_fn:
            raise ValueError("Workflow must have a run method")

    def create_instance(self, det: WorkflowInstanceDetails) -> WorkflowInstance:
        """Create a Trio-based workflow instance."""
        return TrioWorkflowInstance(det)
```

#### 3. `WorkflowInstance` (Abstract) and `TrioWorkflowInstance`

Handles workflow activations. The implementation extends both `WorkflowInstance` and `_Runtime`.

```python
from abc import ABC, abstractmethod
from dataclasses import dataclass
import random

class WorkflowInstance(ABC):
    """Instance of a workflow that handles activations.

    Mirrors temporalio.worker.WorkflowInstance from the SDK.
    """

    @abstractmethod
    def activate(
        self,
        act: WorkflowActivation
    ) -> WorkflowActivationCompletion:
        """Handle an activation and return completion."""
        ...


@dataclass(frozen=True)
class WorkflowInstanceDetails:
    """Immutable details for creating a workflow instance.

    Mirrors temporalio.worker.WorkflowInstanceDetails from the SDK.
    """
    defn: _Definition
    info: Info
    randomness_seed: int
    # Simplified for MVP - SDK has more fields


class TrioWorkflowInstance(WorkflowInstance, _Runtime):
    """Trio-based workflow instance.

    Implements both WorkflowInstance (activation handling) and
    _Runtime (workflow API implementation).
    """

    def __init__(self, det: WorkflowInstanceDetails) -> None:
        self._defn = det.defn
        self._info = det.info
        self._random = random.Random(det.randomness_seed)
        self._time_ns = 0
        self._pending_timers: dict[int, trio.Event] = {}
        self._timer_seq = 0

    def activate(
        self,
        act: WorkflowActivation
    ) -> WorkflowActivationCompletion:
        """Handle activation using Trio's deterministic scheduling."""
        # Set up context and run with Trio
        token = _current_runtime.set(self)
        try:
            result = trio.run(
                self._run_activation,
                act,
                deterministic=True,
                random_seed=self._random.getrandbits(64),
                clock=WorkflowClock(act.history),
            )
            return result
        finally:
            _current_runtime.reset(token)

    async def _run_activation(
        self,
        act: WorkflowActivation
    ) -> WorkflowActivationCompletion:
        """Execute workflow code within Trio context."""
        # Process activation jobs (start workflow, fire timers, etc.)
        for job in act.jobs:
            await self._process_job(job)

        return self._build_completion()

    # _Runtime implementation
    def workflow_time_ns(self) -> int:
        return self._time_ns

    async def workflow_sleep(self, duration: float, summary: str | None) -> None:
        """Sleep using Trio, controlled by workflow clock."""
        await trio.sleep(duration)
        # Clock advances based on history, not real time
```

#### 4. `_Definition`

Stores workflow metadata from decorators. Simplified for MVP.

```python
@dataclass
class _Definition:
    """Workflow definition metadata.

    Mirrors temporalio.workflow._Definition from the SDK.
    """
    name: str
    cls: type
    run_fn: Callable[..., Awaitable]
    # Simplified for MVP - SDK has signals, queries, updates, etc.

    @staticmethod
    def from_class(cls: type) -> _Definition | None:
        """Get definition from a decorated class."""
        return getattr(cls, '__temporal_workflow_definition', None)
```

#### 5. `Info`

Workflow information. Simplified for MVP.

```python
@dataclass
class Info:
    """Information about a running workflow.

    Mirrors temporalio.workflow.Info from the SDK.
    """
    workflow_id: str
    workflow_type: str
    run_id: str
    task_queue: str
    # Simplified for MVP - SDK has many more fields
```

#### 6. `WorkflowClock`

Custom Trio clock that advances based on workflow history.

```python
import trio.abc

class WorkflowClock(trio.abc.Clock):
    """Clock controlled by workflow history.

    This is Trio-specific - the SDK manages time differently
    because it uses asyncio's event loop.
    """

    def __init__(self, history: list[HistoryEvent]) -> None:
        self._history = history
        self._current_time = 0.0
        self._history_index = 0

    def current_time(self) -> float:
        return self._current_time

    def deadline_to_sleep_time(self, deadline: float) -> float:
        """Return 0 if history says timer fires, else block."""
        # Check if next history event is a timer firing at this deadline
        if self._next_timer_fires_at(deadline):
            self._advance_to(deadline)
            return 0.0
        return float('inf')

    def _advance_to(self, time: float) -> None:
        self._current_time = time
```

### Public Workflow API

```python
# temporalio_trio/workflow.py

def defn(cls: type) -> type:
    """Decorator for workflow classes.

    Mirrors @temporalio.workflow.defn from the SDK.
    """
    # Find the run method
    run_fn = None
    for name, method in inspect.getmembers(cls, inspect.iscoroutinefunction):
        if getattr(method, '__temporal_workflow_run', False):
            run_fn = method
            break

    if not run_fn:
        raise ValueError("Workflow must have a @workflow.run method")

    # Attach definition to class
    defn = _Definition(
        name=cls.__name__,
        cls=cls,
        run_fn=run_fn,
    )
    setattr(cls, '__temporal_workflow_definition', defn)
    return cls


def run(fn: Callable) -> Callable:
    """Decorator for workflow run method.

    Mirrors @temporalio.workflow.run from the SDK.
    """
    if not inspect.iscoroutinefunction(fn):
        raise ValueError("Workflow run method must be async")
    setattr(fn, '__temporal_workflow_run', True)
    return fn


async def sleep(duration: float) -> None:
    """Sleep for the given duration.

    Mirrors temporalio.workflow.sleep from the SDK.
    MVP simplified - full version supports timedelta and summary parameter.
    """
    await _Runtime.current().workflow_sleep(duration, None)


def time() -> float:
    """Get current workflow time in seconds.

    Mirrors temporalio.workflow.time from the SDK.
    """
    return _Runtime.current().workflow_time_ns() / 1e9
```

### History Events (Simplified for MVP)

```python
@dataclass
class WorkflowStartedEvent:
    workflow_type: str
    random_seed: int

@dataclass
class TimerStartedEvent:
    timer_id: int
    duration: float

@dataclass
class TimerFiredEvent:
    timer_id: int

@dataclass
class WorkflowCompletedEvent:
    result: Any

HistoryEvent = Union[
    WorkflowStartedEvent,
    TimerStartedEvent,
    TimerFiredEvent,
    WorkflowCompletedEvent,
]
```

## Implementation Plan

### Phase 1: Core Infrastructure

1. **`_Runtime` abstract class** - Base class with `current()` pattern
2. **`_Definition` dataclass** - Store workflow metadata
3. **`@workflow.defn` and `@workflow.run`** - Decorators

### Phase 2: Workflow Instance

4. **`WorkflowInstanceDetails`** - Immutable config
5. **`TrioWorkflowInstance`** - Implements `WorkflowInstance` + `_Runtime`
6. **`WorkflowClock`** - Trio clock for history-based time

### Phase 3: Runner and Execution

7. **`WorkflowRunner` abstract class** - Factory interface
8. **`TrioWorkflowRunner`** - Creates `TrioWorkflowInstance`
9. **`workflow.sleep()` and `workflow.time()`** - Public API

### Phase 4: Replay Verification

10. **Replay test** - Verify same history produces same result
11. **Parallel execution test** - Multiple workflows in threads
12. **Non-determinism detection** - Catch replay mismatches

## Example Usage

```python
from temporalio_trio import workflow
from temporalio_trio.worker import TrioWorkflowRunner, WorkflowInstanceDetails

@workflow.defn
class SleepWorkflow:
    @workflow.run
    async def run(self, sleep_seconds: int) -> str:
        print(f"Starting at {workflow.time()}")
        await workflow.sleep(sleep_seconds)
        print(f"Woke up at {workflow.time()}")
        return f"Slept for {sleep_seconds} seconds"

# Create runner
runner = TrioWorkflowRunner()

# Prepare workflow definition
defn = workflow._Definition.from_class(SleepWorkflow)
runner.prepare_workflow(defn)

# Create instance for execution
details = WorkflowInstanceDetails(
    defn=defn,
    info=workflow.Info(
        workflow_id="test-1",
        workflow_type="SleepWorkflow",
        run_id="run-1",
        task_queue="test-queue",
    ),
    randomness_seed=12345,
)
instance = runner.create_instance(details)

# Execute via activation (simplified)
activation = WorkflowActivation(
    jobs=[StartWorkflowJob(args=(10,))],
    history=[],
)
completion = instance.activate(activation)
print(f"Result: {completion.result}")  # "Slept for 10 seconds"
```

### Replay Example

```python
# First execution generates history
completion1, history = execute_workflow(SleepWorkflow, args=(10,))

# Replay with same history must produce identical result
completion2 = replay_workflow(SleepWorkflow, history)
assert completion1.result == completion2.result  # Deterministic!
```

### Parallel Execution Example

```python
from concurrent.futures import ThreadPoolExecutor

def run_workflow(workflow_id: str, seed: int):
    runner = TrioWorkflowRunner()
    defn = workflow._Definition.from_class(SleepWorkflow)
    runner.prepare_workflow(defn)

    details = WorkflowInstanceDetails(
        defn=defn,
        info=workflow.Info(
            workflow_id=workflow_id,
            workflow_type="SleepWorkflow",
            run_id=f"run-{workflow_id}",
            task_queue="test-queue",
        ),
        randomness_seed=seed,
    )
    instance = runner.create_instance(details)
    # ... activate and return result

# Each workflow is fully isolated and deterministic
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [
        executor.submit(run_workflow, f"wf-{i}", seed=i * 1000)
        for i in range(4)
    ]
    results = [f.result() for f in futures]
```

## Dependencies

### Required Trio Changes

This MVP depends on the per-runner deterministic scheduling feature:
- **Branch:** https://github.com/mfateev/trio/tree/temporal-deterministic-scheduling
- **Parameters:** `trio.run(deterministic=True, random_seed=seed)`

### Python Packages

```toml
dependencies = [
    "trio>=0.27.0",  # With deterministic scheduling patch
    "attrs>=23.2.0",
]
```

## Testing Strategy

### Unit Tests

```python
def test_workflow_sleep():
    """Test that workflow.sleep() works correctly."""

    @workflow.defn
    class TestWorkflow:
        @workflow.run
        async def run(self) -> float:
            start = workflow.time()
            await workflow.sleep(10)
            return workflow.time() - start

    runner = TrioWorkflowRunner()
    # ... setup and execute
    assert result == 10.0

def test_replay_determinism():
    """Test that replay produces identical results."""

    @workflow.defn
    class TestWorkflow:
        @workflow.run
        async def run(self) -> list[float]:
            times = []
            for _ in range(3):
                times.append(workflow.time())
                await workflow.sleep(5)
            return times

    # Execute and capture history
    result1, history = execute_new(TestWorkflow)

    # Replay must match
    result2 = replay(TestWorkflow, history)
    assert result1 == result2

def test_parallel_isolation():
    """Test that parallel workflows are isolated."""

    def run_in_thread(seed: int):
        # Each thread gets its own runner and instance
        runner = TrioWorkflowRunner()
        # ... execute workflow
        return result

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(run_in_thread, i) for i in range(4)]
        results = [f.result() for f in futures]

    # Same seed should produce same result
    assert run_in_thread(42) == run_in_thread(42)
```

## Architecture Comparison with SDK

| Component | SDK (asyncio) | Trio MVP |
|-----------|---------------|----------|
| Runtime context | `asyncio.get_running_loop().__temporal_workflow_runtime` | `ContextVar[_Runtime]` |
| WorkflowRunner | Abstract with `prepare_workflow()`, `create_instance()` | Same pattern |
| WorkflowInstance | Extends `asyncio.AbstractEventLoop` | Standalone, uses `trio.run()` |
| Time control | `_time_ns` attribute, custom event loop | `WorkflowClock` (trio.abc.Clock) |
| Determinism | Custom event loop scheduling | `trio.run(deterministic=True)` |

## Future Features (Post-MVP)

Once the MVP proves viability, features can be added by:

1. **Add abstract method to `_Runtime`**
2. **Implement in `TrioWorkflowInstance`**
3. **Expose via module function**

Examples:
- `workflow.random()` → `_Runtime.workflow_random()` → `return self._random`
- `workflow.info()` → `_Runtime.workflow_info()` → `return self._info`
- Signals/queries → `_Runtime.workflow_set_signal_handler()` etc.

## Open Questions

1. **History format** - Should we use Temporal's protobuf format or simplified dataclasses for MVP?

2. **Error handling** - How should we handle non-determinism detection in MVP?

3. **Activation model** - Should MVP support full activation/completion cycle or simplified execute model?
