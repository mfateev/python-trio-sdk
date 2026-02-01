# Migration Plan: Single-Threaded Workflow Execution

## Overview

This plan outlines the migration from the current multi-threaded, replay-from-beginning model to a single `trio.run()` with event-based suspension.

**Current State:** 205/205 tests passing, E2E validated
**Target State:** Single-threaded execution with same test coverage

## Migration Strategy

**Approach:** Incremental refactoring with parallel implementations

We'll build the new implementation alongside the existing one, allowing A/B testing and gradual migration. Each phase produces working code with passing tests.

---

## Phase 1: Foundation (ContextVar-Based Runtime)

### Goal
Replace thread-local runtime with contextvar-based runtime that works in both models.

### Tasks

#### 1.1 Create New Runtime Class
```python
# temporalio_trio/worker/_runtime.py (new file)

from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Dict, List, Optional
import random
import trio

@dataclass
class WorkflowRuntime:
    """Per-workflow isolated runtime state."""

    # Identity
    run_id: str
    workflow_id: str
    workflow_type: str
    task_queue: str

    # Deterministic state
    random: random.Random
    time_ns: int
    is_replaying: bool = False

    # Sequence counters
    timer_seq: int = 0
    activity_seq: int = 0
    child_workflow_seq: int = 0
    signal_seq: int = 0

    # Fired events (for replay)
    fired_timers: Dict[int, int] = field(default_factory=dict)  # seq -> fire_time_ns
    completed_activities: Dict[int, Any] = field(default_factory=dict)
    completed_children: Dict[int, Any] = field(default_factory=dict)

    # Pending events (for suspension)
    pending_timers: Dict[int, trio.Event] = field(default_factory=dict)
    pending_activities: Dict[int, trio.Event] = field(default_factory=dict)
    pending_children: Dict[int, trio.Event] = field(default_factory=dict)

    # Commands to emit
    commands: List[Command] = field(default_factory=list)

    # Workflow instance
    workflow_object: Optional[Any] = None
    nursery: Optional[trio.Nursery] = None

_current_runtime: ContextVar[Optional[WorkflowRuntime]] = ContextVar(
    "workflow_runtime", default=None
)
```

#### 1.2 Update workflow.py to Use New Runtime
```python
# temporalio_trio/workflow.py

def _get_runtime() -> WorkflowRuntime:
    runtime = _current_runtime.get()
    if runtime is None:
        raise RuntimeError("Not in workflow context")
    return runtime

async def sleep(duration: float) -> None:
    runtime = _get_runtime()
    await runtime.workflow_sleep(duration)

def time() -> float:
    return _get_runtime().time_ns / 1e9

def random() -> random.Random:
    return _get_runtime().random
```

#### 1.3 Tests
- [ ] Unit tests for WorkflowRuntime isolation
- [ ] Test contextvar propagation to child tasks
- [ ] Test multiple runtimes don't interfere

### Deliverable
New runtime module that can be used by both old and new execution models.

---

## Phase 2: Event-Based Timer Implementation

### Goal
Implement `workflow.sleep()` with event-based suspension instead of replay-from-beginning.

### Tasks

#### 2.1 Implement Timer Methods in WorkflowRuntime
```python
# In WorkflowRuntime class

def next_timer_seq(self) -> int:
    self.timer_seq += 1
    return self.timer_seq

async def workflow_sleep(self, duration: float) -> None:
    seq = self.next_timer_seq()

    # Check if already fired (replay)
    if seq in self.fired_timers:
        # Update time to fire time
        self.time_ns = self.fired_timers[seq]
        return

    # Create suspension event
    event = trio.Event()
    self.pending_timers[seq] = event

    # Emit command
    self.commands.append(StartTimerCommand(
        seq=seq,
        start_to_fire_timeout_ms=int(duration * 1000),
    ))

    # Suspend
    await event.wait()

    # Clean up
    del self.pending_timers[seq]

def apply_timer_fired(self, seq: int, fire_time_ns: int) -> None:
    """Called when activation contains TimerFired job."""
    self.fired_timers[seq] = fire_time_ns
    self.time_ns = fire_time_ns

    if seq in self.pending_timers:
        self.pending_timers[seq].set()
```

#### 2.2 Tests
- [ ] Test sleep creates timer command
- [ ] Test sleep suspends on event
- [ ] Test timer fired wakes up workflow
- [ ] Test replay path (timer already fired)
- [ ] Test multiple timers in sequence
- [ ] Test concurrent timers

### Deliverable
Working `workflow.sleep()` with event-based suspension.

---

## Phase 3: Single trio.run() Worker

### Goal
Create new worker that runs all workflows in single `trio.run(deterministic=True, fifo=True)`.

### Tasks

#### 3.1 Create New Worker Implementation
```python
# temporalio_trio/worker/_single_thread_worker.py (new file)

class SingleThreadWorker:
    """Worker that executes all workflows in a single trio.run()."""

    def __init__(
        self,
        client: Client,
        task_queue: str,
        workflows: List[Type],
        activities: List[Callable] = [],
    ):
        self._client = client
        self._task_queue = task_queue
        self._workflows = {w.__name__: w for w in workflows}
        self._activities = activities
        self._bridge: Optional[TrioBridgeWrapper] = None
        self._workflow_states: Dict[str, WorkflowState] = {}

    async def run(self) -> None:
        """Main entry point - runs in trio.run(deterministic=True, fifo=True)."""
        self._bridge = TrioBridgeWrapper()
        await self._bridge.start()

        try:
            async with trio.open_nursery() as nursery:
                self._root_nursery = nursery
                nursery.start_soon(self._poll_workflow_activations)
                nursery.start_soon(self._poll_activity_tasks)
        finally:
            await self._bridge.shutdown()

    async def _poll_workflow_activations(self) -> None:
        """Poll and dispatch workflow activations."""
        while True:
            activation = await self._bridge.poll_workflow_activation()
            await self._dispatch_activation(activation)

    async def _dispatch_activation(self, activation) -> None:
        run_id = activation.run_id

        if run_id not in self._workflow_states:
            # New workflow
            state = WorkflowState(run_id=run_id)
            self._workflow_states[run_id] = state
            self._root_nursery.start_soon(
                self._run_workflow, state, activation
            )
        else:
            # Existing workflow - deliver activation
            state = self._workflow_states[run_id]
            state.deliver_activation(activation)

        # Wait for commands
        commands = await state.wait_for_commands()

        # Send completion
        await self._bridge.complete_workflow_activation(run_id, commands)

        # Handle eviction
        if activation.has_eviction:
            del self._workflow_states[run_id]
```

#### 3.2 Create WorkflowState Class
```python
@dataclass
class WorkflowState:
    """Tracks state for a running workflow."""
    run_id: str
    runtime: Optional[WorkflowRuntime] = None

    # Activation delivery
    pending_activation: Optional[Activation] = None
    activation_event: trio.Event = field(default_factory=trio.Event)

    # Command collection
    commands_ready: trio.Event = field(default_factory=trio.Event)

    def deliver_activation(self, activation) -> None:
        self.pending_activation = activation
        self.activation_event.set()

    async def wait_for_commands(self) -> List[Command]:
        await self.commands_ready.wait()
        self.commands_ready = trio.Event()
        commands = list(self.runtime.commands)
        self.runtime.commands.clear()
        return commands
```

#### 3.3 Implement Workflow Execution Loop
```python
async def _run_workflow(self, state: WorkflowState, initial_activation) -> None:
    """Long-running task for a single workflow instance."""
    # Initialize runtime
    runtime = WorkflowRuntime(
        run_id=state.run_id,
        workflow_id=initial_activation.workflow_id,
        workflow_type=initial_activation.workflow_type,
        task_queue=self._task_queue,
        random=random.Random(initial_activation.randomness_seed),
        time_ns=initial_activation.timestamp_ns,
        is_replaying=initial_activation.is_replaying,
    )
    state.runtime = runtime

    token = _current_runtime.set(runtime)
    try:
        # Apply initial activation
        self._apply_activation(runtime, initial_activation)

        # Run workflow
        async with trio.open_nursery() as nursery:
            runtime.nursery = nursery

            # Start main workflow coroutine
            nursery.start_soon(self._execute_workflow_main, runtime)

            # Process subsequent activations
            while not runtime.is_complete:
                # Signal commands ready
                state.commands_ready.set()

                # Wait for next activation
                await state.activation_event.wait()
                state.activation_event = trio.Event()

                if state.pending_activation:
                    activation = state.pending_activation
                    state.pending_activation = None

                    runtime.time_ns = activation.timestamp_ns
                    runtime.is_replaying = activation.is_replaying
                    self._apply_activation(runtime, activation)

            # Final commands
            state.commands_ready.set()

    except Exception as e:
        runtime.commands.append(FailWorkflowCommand(error=e))
        state.commands_ready.set()

    finally:
        _current_runtime.reset(token)
```

#### 3.4 Tests
- [ ] Test single workflow execution
- [ ] Test multiple concurrent workflows
- [ ] Test workflow with timers
- [ ] Test activation delivery to existing workflow
- [ ] Test eviction cleanup

### Deliverable
New worker that runs workflows in single thread.

---

## Phase 4: Activity Support

### Goal
Implement activity scheduling and completion with event-based suspension.

### Tasks

#### 4.1 Activity Methods in WorkflowRuntime
```python
def next_activity_seq(self) -> int:
    self.activity_seq += 1
    return self.activity_seq

async def execute_activity(
    self,
    activity: str,
    args: tuple,
    **options,
) -> Any:
    seq = self.next_activity_seq()

    # Check if already completed (replay)
    if seq in self.completed_activities:
        result = self.completed_activities[seq]
        if isinstance(result, Exception):
            raise result
        return result

    # Create suspension event
    event = trio.Event()
    self.pending_activities[seq] = event

    # Emit command
    self.commands.append(ScheduleActivityCommand(
        seq=seq,
        activity_type=activity,
        arguments=args,
        **options,
    ))

    # Suspend
    await event.wait()

    # Get result
    result = self.completed_activities[seq]
    del self.pending_activities[seq]

    if isinstance(result, Exception):
        raise result
    return result

def apply_activity_resolved(self, seq: int, result: Any, error: Optional[Exception]) -> None:
    if error:
        self.completed_activities[seq] = error
    else:
        self.completed_activities[seq] = result

    if seq in self.pending_activities:
        self.pending_activities[seq].set()
```

#### 4.2 Tests
- [ ] Test activity scheduling
- [ ] Test activity completion
- [ ] Test activity failure
- [ ] Test activity replay
- [ ] Test concurrent activities

### Deliverable
Working activity support.

---

## Phase 5: Signals and Queries

### Goal
Implement signal delivery and query handling.

### Tasks

#### 5.1 Signal Handling
```python
# In workflow execution loop
async def _apply_signal(self, runtime: WorkflowRuntime, signal_job) -> None:
    handler = runtime.signal_handlers.get(signal_job.signal_name)
    if handler:
        result = handler(*signal_job.args)
        if inspect.iscoroutine(result):
            # Run signal handler as child task
            runtime.nursery.start_soon(self._run_signal_handler, result)

async def _run_signal_handler(self, coro) -> None:
    try:
        await coro
    except Exception as e:
        # Log but don't fail workflow
        logger.warning(f"Signal handler error: {e}")
```

#### 5.2 Query Handling
```python
def _apply_query(self, runtime: WorkflowRuntime, query_job) -> None:
    handler = runtime.query_handlers.get(query_job.query_name)
    if handler:
        try:
            result = handler(*query_job.args)
            runtime.commands.append(QuerySuccessCommand(
                query_id=query_job.query_id,
                result=result,
            ))
        except Exception as e:
            runtime.commands.append(QueryFailureCommand(
                query_id=query_job.query_id,
                error=e,
            ))
```

#### 5.3 Tests
- [ ] Test signal delivery
- [ ] Test async signal handler
- [ ] Test query response
- [ ] Test query error

### Deliverable
Working signals and queries.

---

## Phase 6: Child Workflows

### Goal
Implement child workflow execution with event-based suspension.

### Tasks

#### 6.1 Child Workflow Methods
```python
async def execute_child_workflow(
    self,
    workflow: str,
    args: tuple,
    **options,
) -> Any:
    seq = self.next_child_seq()

    # Check if already completed (replay)
    if seq in self.completed_children:
        result = self.completed_children[seq]
        if isinstance(result, Exception):
            raise result
        return result

    # Create suspension event
    event = trio.Event()
    self.pending_children[seq] = event

    # Emit command
    self.commands.append(StartChildWorkflowCommand(
        seq=seq,
        workflow_type=workflow,
        arguments=args,
        **options,
    ))

    # Suspend
    await event.wait()

    # Get result
    result = self.completed_children[seq]
    del self.pending_children[seq]

    if isinstance(result, Exception):
        raise result
    return result
```

#### 6.2 Tests
- [ ] Test child workflow start
- [ ] Test child workflow completion
- [ ] Test child workflow failure
- [ ] Test child workflow replay

### Deliverable
Working child workflow support.

---

## Phase 7: Cancellation

### Goal
Implement cancellation propagation through the workflow tree.

### Tasks

#### 7.1 Cancellation Handling
```python
def apply_cancel_workflow(self, runtime: WorkflowRuntime) -> None:
    runtime.cancel_requested = True
    # Cancel the nursery to propagate to all child tasks
    runtime.nursery.cancel_scope.cancel()

# In workflow code
async def workflow_sleep(self, duration: float) -> None:
    # ... existing code ...

    # Check for cancellation after waking
    if self.cancel_requested:
        raise CancelledError("Workflow cancelled")
```

#### 7.2 Tests
- [ ] Test workflow cancellation
- [ ] Test cancellation propagates to child tasks
- [ ] Test cancellation during sleep
- [ ] Test cancellation during activity

### Deliverable
Working cancellation support.

---

## Phase 8: Integration and Cutover

### Goal
Replace old implementation with new single-threaded model.

### Tasks

#### 8.1 Update Worker Class
```python
# temporalio_trio/worker/_worker.py

class Worker:
    def __init__(self, ...):
        # Same interface
        ...

    async def run(self) -> None:
        # Use new single-threaded implementation
        worker = SingleThreadWorker(...)

        # Run with FIFO scheduling
        # Note: This is called FROM trio.run(), so we're already in a trio context
        await worker.run()

# Entry point
def run_worker(...):
    trio.run(
        worker.run,
        deterministic=True,
        fifo=True,
    )
```

#### 8.2 Remove Old Implementation
- [ ] Remove `_workflow_instance.py` (old replay-from-beginning)
- [ ] Remove thread pool usage in `bridge_worker.py`
- [ ] Update imports

#### 8.3 Full Test Suite
- [ ] All unit tests pass
- [ ] All E2E tests pass
- [ ] Performance benchmarks

### Deliverable
Complete migration to single-threaded model.

---

## Phase 9: Optimization (Optional)

### Goal
Optimize performance and consider single-threaded Tokio.

### Tasks

#### 9.1 Benchmark
- [ ] Measure workflow throughput
- [ ] Measure latency
- [ ] Compare with old implementation

#### 9.2 Single-Threaded Tokio (Future)
- [ ] Evaluate feasibility
- [ ] Prototype manual polling
- [ ] Benchmark benefits

---

## File Changes Summary

### New Files
- `temporalio_trio/worker/_runtime.py` - WorkflowRuntime class
- `temporalio_trio/worker/_single_thread_worker.py` - New worker
- `temporalio_trio/worker/_workflow_state.py` - WorkflowState class

### Modified Files
- `temporalio_trio/workflow.py` - Use new runtime
- `temporalio_trio/worker/_worker.py` - Use new implementation
- `temporalio_trio/worker/__init__.py` - Update exports

### Removed Files (Phase 8)
- `temporalio_trio/worker/_workflow_instance.py` - Old implementation
- Thread pool related code in `bridge_worker.py`

---

## Testing Strategy

### Unit Tests
Each phase includes unit tests for new functionality.

### Integration Tests
Test workflow execution end-to-end with mock bridge.

### E2E Tests
Run against real Temporal server to validate behavior.

### Regression Tests
Existing 205 tests must continue to pass throughout migration.

---

## Rollback Plan

If issues are discovered:
1. Keep old implementation available behind flag
2. `Worker(use_single_thread=False)` falls back to old model
3. Remove flag after stabilization period

---

## Timeline Estimate

| Phase | Effort | Dependencies |
|-------|--------|--------------|
| Phase 1: Foundation | 1-2 days | None |
| Phase 2: Timers | 1-2 days | Phase 1 |
| Phase 3: Worker | 2-3 days | Phase 1, 2 |
| Phase 4: Activities | 1-2 days | Phase 3 |
| Phase 5: Signals/Queries | 1 day | Phase 3 |
| Phase 6: Child Workflows | 1-2 days | Phase 3 |
| Phase 7: Cancellation | 1 day | Phase 3-6 |
| Phase 8: Cutover | 1-2 days | Phase 1-7 |
| Phase 9: Optimization | TBD | Phase 8 |

**Total: ~10-15 days**

---

## Success Criteria

1. All 205 existing tests pass
2. All E2E tests pass
3. Single `trio.run()` for all workflows
4. No thread pool for workflow execution
5. Event-based suspension (not replay-from-beginning)
6. Deterministic execution with FIFO scheduling
