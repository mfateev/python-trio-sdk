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

## Architecture

### Components

```
┌─────────────────────────────────────────────────────────┐
│                    WorkflowRunner                        │
│  ┌─────────────────────────────────────────────────────┐│
│  │                 trio.run()                          ││
│  │            deterministic=True                       ││
│  │            random_seed=<from history>               ││
│  │  ┌───────────────────────────────────────────────┐ ││
│  │  │              WorkflowContext                   │ ││
│  │  │  - current_time (from history)                │ ││
│  │  │  - pending_timers                             │ ││
│  │  │  - history_events                             │ ││
│  │  │  ┌─────────────────────────────────────────┐  │ ││
│  │  │  │         User Workflow Code              │  │ ││
│  │  │  │  async def run(self):                   │  │ ││
│  │  │  │      await workflow.sleep(10)           │  │ ││
│  │  │  │      return "done"                      │  │ ││
│  │  │  └─────────────────────────────────────────┘  │ ││
│  │  └───────────────────────────────────────────────┘ ││
│  └─────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────┘
```

### Key Classes

#### 1. `WorkflowContext`

Manages workflow state and provides the `workflow.*` API.

```python
@dataclass
class WorkflowContext:
    """Context for a single workflow execution."""

    workflow_id: str
    run_id: str

    # Time management
    current_time: float  # Logical time from history
    pending_timers: dict[int, trio.Event]  # timer_id -> wake event

    # History for replay
    history: list[HistoryEvent]
    history_index: int

    # Determinism
    random_seed: int
    _random: random.Random
```

#### 2. `WorkflowRunner`

Executes a workflow with deterministic scheduling.

```python
class WorkflowRunner:
    """Runs a workflow definition with trio."""

    def run(
        self,
        workflow_class: type,
        history: list[HistoryEvent],
    ) -> WorkflowResult:
        """Execute workflow with given history."""

        # Extract determinism seed from history
        seed = history[0].workflow_started.random_seed

        # Run with trio's deterministic scheduling
        return trio.run(
            self._execute_workflow,
            workflow_class,
            history,
            deterministic=True,
            random_seed=seed,
            clock=WorkflowClock(history),
        )
```

#### 3. `WorkflowClock`

Custom trio clock that advances based on workflow history.

```python
class WorkflowClock(trio.abc.Clock):
    """Clock controlled by workflow history."""

    def __init__(self, history: list[HistoryEvent]):
        self._history = history
        self._current_time = 0.0
        self._pending_wakeups: list[float] = []

    def current_time(self) -> float:
        return self._current_time

    def deadline_to_sleep_time(self, deadline: float) -> float:
        # Return 0 if we have a timer firing at this deadline in history
        if self._has_timer_fired(deadline):
            return 0.0
        # Otherwise block until history provides timer
        return float('inf')
```

### Workflow API

```python
# User-facing API (similar to current SDK)
from temporalio_trio import workflow

@workflow.defn
class MyWorkflow:
    @workflow.run
    async def run(self) -> str:
        # Sleep for 10 seconds (workflow time)
        await workflow.sleep(10)
        return "completed"
```

### History Events (Simplified)

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

1. **WorkflowClock** - Trio clock that reads time from history
2. **WorkflowContext** - Context manager for workflow execution
3. **workflow.sleep()** - Timer implementation using trio

### Phase 2: Workflow Execution

4. **@workflow.defn** - Decorator for workflow classes
5. **WorkflowRunner** - Execute workflow with deterministic trio
6. **History generation** - Record events during execution

### Phase 3: Replay Verification

7. **Replay test** - Verify same history produces same result
8. **Parallel execution test** - Multiple workflows in threads
9. **Non-determinism detection** - Catch replay mismatches

## Example Usage

```python
from temporalio_trio import workflow
from temporalio_trio.runner import WorkflowRunner

@workflow.defn
class SleepWorkflow:
    @workflow.run
    async def run(self, sleep_seconds: int) -> str:
        print(f"Starting at {workflow.time()}")
        await workflow.sleep(sleep_seconds)
        print(f"Woke up at {workflow.time()}")
        return f"Slept for {sleep_seconds} seconds"

# First execution - generates history
runner = WorkflowRunner()
result, history = runner.execute_new(
    SleepWorkflow,
    args=(10,),
    workflow_id="test-1",
)
print(f"Result: {result}")  # "Slept for 10 seconds"

# Replay - must produce identical result
replay_result = runner.replay(SleepWorkflow, history)
assert replay_result == result  # Deterministic!

# Parallel execution - each workflow isolated
from concurrent.futures import ThreadPoolExecutor

def run_workflow(workflow_id: str, seed: int):
    return runner.execute_new(
        SleepWorkflow,
        args=(5,),
        workflow_id=workflow_id,
        random_seed=seed,
    )

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
async def test_workflow_sleep():
    """Test that workflow.sleep() works correctly."""

    @workflow.defn
    class TestWorkflow:
        @workflow.run
        async def run(self) -> float:
            start = workflow.time()
            await workflow.sleep(10)
            return workflow.time() - start

    runner = WorkflowRunner()
    result, _ = runner.execute_new(TestWorkflow)
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

    runner = WorkflowRunner()
    result1, history = runner.execute_new(TestWorkflow)
    result2 = runner.replay(TestWorkflow, history)
    assert result1 == result2

def test_parallel_isolation():
    """Test that parallel workflows are isolated."""

    results = []

    def run_in_thread(seed: int):
        runner = WorkflowRunner()
        result, _ = runner.execute_new(
            TestWorkflow,
            random_seed=seed,
        )
        return result

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [executor.submit(run_in_thread, i) for i in range(4)]
        results = [f.result() for f in futures]

    # Same seed should produce same result
    assert run_in_thread(42) == run_in_thread(42)
```

## Timeline

| Phase | Deliverable | Estimate |
|-------|-------------|----------|
| 1 | WorkflowClock + Context | 2-3 days |
| 2 | Workflow execution + sleep | 2-3 days |
| 3 | Replay + parallel tests | 1-2 days |
| **Total** | **Working MVP** | **~1 week** |

## Open Questions

1. **Timer resolution** - What's the minimum sleep duration we should support?

2. **History format** - Should we use Temporal's protobuf format or a simplified JSON format for MVP?

3. **Error handling** - How should we handle non-determinism detection in MVP?

4. **Workflow input/output** - Should we support typed inputs in MVP or just use `*args`?

## Next Steps After MVP

Once the MVP proves viability:

1. Add activity support (with trio-based execution)
2. Implement signals and queries
3. Add Temporal server communication
4. Support workflow cancellation with cancel scopes
5. Child workflow support with nurseries
