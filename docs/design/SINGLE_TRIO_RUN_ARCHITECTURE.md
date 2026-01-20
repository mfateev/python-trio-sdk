# Single `trio.run()` Architecture for Temporal Workflows

## Executive Summary

This document outlines an architecture where all Temporal workflows execute within a single `trio.run()` call using FIFO scheduling and contextvar-based isolation. This aligns with the standard Trio programming model while maintaining workflow determinism guarantees.

## Current Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Main Thread                               │
│  trio.run(worker_main)                                       │
│    └── poll_activations_task                                 │
│          │                                                   │
│          ▼                                                   │
│    trio.to_thread.run_sync(instance.activate)  ──────┐      │
│                                                       │      │
└───────────────────────────────────────────────────────│──────┘
                                                        │
                    ┌───────────────────────────────────┼──────┐
                    │         Thread Pool               │      │
                    │  ┌────────────────────────────────▼────┐ │
                    │  │ Worker Thread 1                     │ │
                    │  │   trio.run(workflow_A)              │ │
                    │  └─────────────────────────────────────┘ │
                    │  ┌─────────────────────────────────────┐ │
                    │  │ Worker Thread 2                     │ │
                    │  │   trio.run(workflow_B)              │ │
                    │  └─────────────────────────────────────┘ │
                    └──────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────┐
│                    Tokio Thread                              │
│  Tokio Runtime                                               │
│    ├── poll_workflow_activation()                            │
│    ├── complete_workflow_activation()                        │
│    └── SDK Core operations                                   │
└─────────────────────────────────────────────────────────────┘
```

**Problems with current architecture:**
1. Thread pool overhead for each workflow activation
2. Multiple `trio.run()` calls (one per activation)
3. Replay-from-beginning model (re-executes entire workflow each activation)
4. Not idiomatic Trio usage

## Proposed Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Main Thread                               │
│  trio.run(worker_main, deterministic=True, fifo=True)        │
│    │                                                         │
│    ├── bridge_communication_task                             │
│    │     └── trio.to_thread / trio.from_thread               │
│    │                                                         │
│    ├── activation_dispatcher_task                            │
│    │                                                         │
│    ├── Workflow A [ContextVar: RuntimeA]                     │
│    │     ├── main_coroutine                                  │
│    │     ├── signal_handlers                                 │
│    │     └── child_tasks (activities, timers, children)      │
│    │                                                         │
│    ├── Workflow B [ContextVar: RuntimeB]                     │
│    │     ├── main_coroutine                                  │
│    │     └── child_tasks                                     │
│    │                                                         │
│    └── ... more workflows                                    │
│                                                              │
└─────────────────────────────────────────────────────────────┘
                         │
                         │ trio.to_thread (send completions)
                         │ trio.from_thread (receive activations)
                         ▼
┌─────────────────────────────────────────────────────────────┐
│                    Tokio Thread                              │
│  Tokio Runtime (unchanged)                                   │
│    ├── poll_workflow_activation()                            │
│    ├── complete_workflow_activation()                        │
│    └── SDK Core operations                                   │
└─────────────────────────────────────────────────────────────┘
```

## Key Design Principles

### 1. FIFO Scheduling for Determinism

With `trio.run(deterministic=True, fifo=True)`:
- Tasks execute in creation order (no random shuffling)
- Each workflow's internal task order depends only on its code structure
- Other workflows' tasks don't affect relative ordering

```python
# Workflow A spawns tasks:
nursery.start_soon(task_a1)  # counter=100
nursery.start_soon(task_a2)  # counter=101

# Workflow B spawns tasks:
nursery.start_soon(task_b1)  # counter=102

# FIFO execution order: task_a1, task_a2, task_b1
# Workflow A's relative order (a1 before a2) is preserved regardless of B
```

### 2. ContextVar Isolation

Each workflow gets isolated state via contextvars:

```python
from contextvars import ContextVar

_current_runtime: ContextVar[WorkflowRuntime] = ContextVar("workflow_runtime")

@dataclass
class WorkflowRuntime:
    # Identity
    run_id: str
    workflow_id: str
    workflow_type: str

    # Deterministic state (per-workflow)
    random: random.Random          # Seeded from activation
    time_ns: int                   # Logical time from activation

    # Sequence counters (per-workflow)
    timer_seq: int = 0
    activity_seq: int = 0
    child_workflow_seq: int = 0

    # Pending operations (per-workflow)
    pending_timers: Dict[int, trio.Event]
    pending_activities: Dict[int, trio.Event]
    pending_children: Dict[int, trio.Event]

    # Commands to send
    commands: List[Command]

    # Workflow state
    is_replaying: bool
    cancel_requested: bool
```

### 3. Event-Based Suspension (Not Replay-From-Beginning)

Workflows stay alive between activations, suspended on `trio.Event`:

```python
async def workflow_sleep(duration: float) -> None:
    runtime = _current_runtime.get()
    seq = runtime.next_timer_seq()

    # Check if timer already fired (replay)
    if seq in runtime.fired_timers:
        return  # Instant completion during replay

    # Create event for suspension
    event = trio.Event()
    runtime.pending_timers[seq] = event

    # Emit command
    runtime.commands.append(StartTimerCommand(seq=seq, duration_ms=int(duration * 1000)))

    # Suspend until timer fires
    await event.wait()
```

When activation arrives with `TimerFired(seq=N)`:
```python
def apply_timer_fired(runtime: WorkflowRuntime, seq: int):
    runtime.fired_timers.add(seq)
    if seq in runtime.pending_timers:
        runtime.pending_timers[seq].set()  # Wake up workflow
```

### 4. Workflow Lifecycle

```
┌─────────────────────────────────────────────────────────────┐
│                  Workflow Lifecycle                          │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────┐    StartWorkflow    ┌──────────────────────┐  │
│  │  (none)  │ ─────────────────▶ │     RUNNING          │  │
│  └──────────┘                     │  (executing code)    │  │
│                                   └──────────┬───────────┘  │
│                                              │               │
│                          await event ────────┼───────┐      │
│                                              ▼       │      │
│                                   ┌──────────────────▼───┐  │
│                                   │     SUSPENDED        │  │
│                                   │  (waiting for timer, │  │
│                                   │   activity, signal)  │  │
│                                   └──────────┬───────────┘  │
│                                              │               │
│              activation with fired event ────┘               │
│                                              │               │
│                                              ▼               │
│                                   ┌──────────────────────┐  │
│                                   │     COMPLETED        │  │
│                                   │  or FAILED           │  │
│                                   └──────────────────────┘  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### 5. Exception Boundaries

Each workflow runs in an isolated exception context:

```python
async def run_workflow_isolated(runtime: WorkflowRuntime, workflow_fn):
    """Run workflow with exception isolation."""
    token = _current_runtime.set(runtime)
    try:
        async with trio.open_nursery() as nursery:
            runtime.nursery = nursery
            try:
                result = await workflow_fn()
                runtime.commands.append(CompleteWorkflowCommand(result=result))
            except CancelledError:
                runtime.commands.append(CancelWorkflowCommand())
            except Exception as e:
                runtime.commands.append(FailWorkflowCommand(error=e))
    except BaseException as e:
        # Nursery-level failure (shouldn't happen in normal operation)
        runtime.commands.append(FailWorkflowCommand(error=e))
    finally:
        _current_runtime.reset(token)
```

## Activation Processing

### Dispatcher Task

```python
async def activation_dispatcher(
    bridge: TrioBridgeWrapper,
    workflows: Dict[str, WorkflowState],
):
    """Main loop: receive activations, dispatch to workflows."""
    async with trio.open_nursery() as nursery:
        while True:
            # Poll for activation (via bridge thread)
            activation = await bridge.poll_workflow_activation()

            run_id = activation.run_id

            if run_id not in workflows:
                # New workflow - spawn task
                workflow = WorkflowState(run_id=run_id)
                workflows[run_id] = workflow
                nursery.start_soon(run_workflow_task, workflow, activation)
            else:
                # Existing workflow - deliver activation
                workflow = workflows[run_id]
                workflow.pending_activations.append(activation)
                workflow.activation_event.set()

            # Wait for workflow to process and produce commands
            commands = await workflow.wait_for_commands()

            # Send completion
            await bridge.complete_workflow_activation(run_id, commands)
```

### Workflow Task

```python
async def run_workflow_task(workflow: WorkflowState, initial_activation):
    """Long-running task for a single workflow instance."""
    runtime = WorkflowRuntime(
        run_id=workflow.run_id,
        random=random.Random(initial_activation.randomness_seed),
        time_ns=initial_activation.timestamp_ns,
    )

    token = _current_runtime.set(runtime)
    try:
        # Apply initial activation
        apply_activation(runtime, initial_activation)

        # Start workflow main coroutine
        async with trio.open_nursery() as nursery:
            runtime.nursery = nursery
            nursery.start_soon(run_workflow_main, runtime)

            # Process subsequent activations
            while not runtime.is_complete:
                # Wait for next activation
                await workflow.activation_event.wait()
                workflow.activation_event = trio.Event()  # Reset

                for activation in workflow.pending_activations:
                    runtime.time_ns = activation.timestamp_ns
                    runtime.is_replaying = activation.is_replaying
                    apply_activation(runtime, activation)

                workflow.pending_activations.clear()

                # Signal that commands are ready
                workflow.commands_ready.set()
                await workflow.commands_consumed.wait()
                workflow.commands_consumed = trio.Event()
                runtime.commands.clear()
    finally:
        _current_runtime.reset(token)
```

## Tokio Thread Integration

The Tokio thread remains separate, handling I/O with the Temporal server.

### Option A: Keep Current Bridge Pattern (Recommended)

```python
class TrioBridgeWrapper:
    """Wrapper that bridges Trio ↔ Tokio thread."""

    def __init__(self):
        self._bridge = TrioAsyncBridge()  # Rust/PyO3
        self._trio_token = None

    async def start(self):
        self._trio_token = trio.lowlevel.current_trio_token()
        await trio.to_thread.run_sync(self._bridge.start)

    async def poll_workflow_activation(self) -> WorkflowActivation:
        """Poll for next activation (async, non-blocking)."""
        event = trio.Event()
        result_container = []

        def callback(result):
            result_container.append(result)
            trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        # Submit poll request to Tokio thread
        self._bridge.poll_workflow_activation_async(callback)

        # Wait for result
        await event.wait()
        return result_container[0]

    async def complete_workflow_activation(self, run_id: str, commands: List):
        """Send completion (async, non-blocking)."""
        event = trio.Event()

        def callback(result):
            trio.from_thread.run_sync(event.set, trio_token=self._trio_token)

        self._bridge.complete_workflow_activation_async(run_id, commands, callback)
        await event.wait()
```

### Option B: Single-Threaded Tokio (Advanced)

For maximum simplicity, we could run Tokio in the same thread using manual polling:

```python
async def unified_event_loop():
    """Run both Trio and Tokio in the same thread."""
    tokio_runtime = create_current_thread_tokio_runtime()

    while True:
        # Poll Tokio (non-blocking)
        tokio_runtime.poll_once()

        # Yield to Trio scheduler
        await trio.sleep(0)

        # Process any results from Tokio
        process_tokio_completions()
```

**Challenges with Option B:**
- Requires Rust changes to expose non-blocking poll
- Latency between Tokio events and Trio processing
- More complex error handling

**Recommendation:** Start with Option A (separate Tokio thread) as it requires minimal bridge changes.

## Determinism Guarantees

### What Makes Execution Deterministic

1. **FIFO Task Scheduling**: Tasks run in creation order
2. **Per-Workflow Random Seed**: `workflow.random()` uses seeded RNG from activation
3. **Per-Workflow Logical Time**: `workflow.time()` returns activation timestamp
4. **Per-Workflow Sequence Counters**: Timer/activity seq assigned in execution order
5. **ContextVar Isolation**: Each workflow's state is independent

### Why Multiple Workflows Don't Interfere

```python
# Workflow A
async def workflow_a():
    await workflow.sleep(1)  # seq=1 (A's counter)
    await workflow.sleep(2)  # seq=2 (A's counter)

# Workflow B
async def workflow_b():
    await workflow.sleep(3)  # seq=1 (B's counter)
```

Even though tasks interleave:
- A's sleeps always get seq 1, 2 (A's counter)
- B's sleep always gets seq 1 (B's counter)
- Counters are in contextvars, not shared

### Replay Scenario

**Original execution:**
```
A: sleep(1) → seq=1, create timer
B: sleep(3) → seq=1, create timer
A: sleep(2) → seq=2, create timer
[timers fire]
A: timer 1 fired → continue
A: timer 2 fired → continue
B: timer 1 fired → continue
```

**Replay of A only (B not started yet):**
```
A: sleep(1) → seq=1, check history → fired, continue
A: sleep(2) → seq=2, check history → fired, continue
[A completes identically]
```

B's presence/absence doesn't affect A's sequence numbers or execution order.

## Implementation Phases

### Phase 1: Core Infrastructure
- [ ] Implement `WorkflowRuntime` with contextvar isolation
- [ ] Implement event-based suspension for `workflow.sleep()`
- [ ] Update worker to use single `trio.run(deterministic=True, fifo=True)`
- [ ] Implement activation dispatcher

### Phase 2: Full Workflow Support
- [ ] Activities (schedule, complete, fail)
- [ ] Child workflows (start, complete, fail)
- [ ] Signals (delivery, handlers)
- [ ] Queries (stateless handlers)
- [ ] Timers (start, fire, cancel)

### Phase 3: Advanced Features
- [ ] Continue-as-new
- [ ] Cancellation propagation
- [ ] Update handlers
- [ ] Search attributes

### Phase 4: Optimization
- [ ] Consider single-threaded Tokio (Option B)
- [ ] Benchmark vs current architecture
- [ ] Memory usage optimization

## Benefits

| Aspect | Current | Proposed |
|--------|---------|----------|
| Threads per workflow | 1 (from pool) | 0 (shared) |
| `trio.run()` calls | 1 per activation | 1 total |
| Workflow state between activations | Recreated | Preserved |
| Execution model | Replay-from-beginning | Event-based suspension |
| Idiomatic Trio | No | Yes |
| Debugging | Multiple threads | Single thread |

## Risks and Mitigations

### Risk: Large Number of Workflows
**Concern:** Memory usage with many suspended workflows
**Mitigation:** Workflows are lightweight (just coroutine + contextvar state)

### Risk: Long-Running Workflow Blocks Others
**Concern:** CPU-bound workflow code blocks scheduler
**Mitigation:** Same as current - workflows should yield regularly; can add checkpoints

### Risk: Exception Propagation
**Concern:** Unhandled exception in one workflow affects others
**Mitigation:** Each workflow runs in isolated nursery with exception handling

### Risk: Contextvar Leakage
**Concern:** Workflow accidentally accesses another's state
**Mitigation:** Runtime validates contextvar is set; clear API boundaries

## Conclusion

The single `trio.run()` architecture provides a cleaner, more idiomatic Trio implementation while maintaining Temporal's determinism guarantees. The key enablers are:

1. **FIFO scheduling** - ensures task order depends only on creation order
2. **ContextVar isolation** - provides per-workflow state
3. **Event-based suspension** - keeps workflows alive between activations

This approach eliminates thread pool overhead, simplifies debugging, and aligns with how Trio applications are typically structured.
