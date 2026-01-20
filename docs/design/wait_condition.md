# Design: `workflow.wait_condition()` for Trio SDK

## Overview

`wait_condition()` blocks workflow execution until a condition becomes true or a timeout expires. It's commonly used with signals to wait for external input.

```python
@workflow.defn
class ApprovalWorkflow:
    def __init__(self):
        self._approved = False

    @workflow.run
    async def run(self) -> str:
        try:
            await workflow.wait_condition(
                lambda: self._approved,
                timeout=timedelta(hours=24),
            )
            return "Approved!"
        except TimeoutError:
            return "Timed out waiting for approval"

    @workflow.signal
    def approve(self):
        self._approved = True
```

## SDK Comparison: asyncio vs Trio

### How asyncio SDK Works

The asyncio SDK's `_WorkflowInstanceImpl` **IS** an `asyncio.AbstractEventLoop`. This is critical:

```python
class _WorkflowInstanceImpl(WorkflowInstance, asyncio.AbstractEventLoop):
    def __init__(self, ...):
        self._conditions: List[Tuple[Callable[[], bool], asyncio.Future]] = []
        self._pending_timers: Dict[int, _TimerHandle] = {}
        ...

    async def workflow_wait_condition(self, fn, *, timeout=None, timeout_summary=None):
        fut = self.create_future()
        self._conditions.append((fn, fut))
        # This internally calls loop.call_later() which goes to _timer_impl()
        await asyncio.wait_for(fut, timeout)

    def _run_once(self, *, check_conditions: bool) -> None:
        # Process ready callbacks until nothing more to do
        while self._ready:
            handle = self._ready.popleft()
            handle._run()
        # Check conditions and resolve futures
        if check_conditions:
            self._check_conditions()

    def call_later(self, delay, callback, *args, **kwargs):
        # Intercepted! Creates a Temporal timer command
        return self._timer_impl(delay, options, callback, *args, **kwargs)
```

Key asyncio pattern:
1. **Event loop persists** - Workflow code awaits futures, suspends in place
2. **`_run_once()`** - Processes callbacks until nothing ready, then returns
3. **Futures resolved** - When jobs arrive, futures are resolved, callbacks become ready
4. **Workflow continues** - From exactly where it left off

### Current Trio SDK Problem

Our current Trio SDK uses **replay from beginning**:

```python
def activate(self, act: WorkflowActivation) -> WorkflowActivationCompletion:
    # Reset sequences for deterministic replay
    self._timer_seq = 0
    self._activity_seq = 0

    # Run workflow from beginning EVERY activation
    trio.run(self._run_workflow, deterministic=True, ...)
```

**Problems:**
1. **Inefficient** - Re-executes all workflow code each activation
2. **Scales poorly** - Workflows with many steps become slow
3. **Not idiomatic** - Doesn't match how asyncio SDK works

## New Design: Guest Mode Architecture

### Key Insight: `trio.lowlevel.start_guest_run()`

Trio provides **guest mode** - a standard API that runs Trio "in the background" on top of another event loop. Our fork extends it with deterministic scheduling:

```python
trio.lowlevel.start_guest_run(
    async_fn,
    run_sync_soon_threadsafe=callback_scheduler,
    done_callback=on_complete,
    deterministic=True,      # Our fork
    random_seed=seed,        # Our fork
    clock=workflow_clock,
)
```

This allows:
1. **Persistent Trio runtime** - Doesn't exit after each activation
2. **True suspension** - Workflow awaits `trio.Event`, suspends in place
3. **Efficient resumption** - Set event, workflow continues from where it stopped

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        TrioWorkflowInstance                             │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  ┌─────────────────┐     ┌──────────────────────────────────────────┐  │
│  │ Activation Loop │     │         Trio Guest Runtime               │  │
│  │   (sync)        │     │         (deterministic=True)             │  │
│  │                 │     │                                          │  │
│  │  activate()     │────▶│  workflow code runs                      │  │
│  │    │            │     │       │                                  │  │
│  │    │ process    │     │       ▼                                  │  │
│  │    │ jobs       │     │  await workflow.sleep()                  │  │
│  │    │            │     │       │                                  │  │
│  │    ▼            │     │       ▼                                  │  │
│  │  set events     │────▶│  await self._timer_events[id].wait() ◀──│  │
│  │    │            │     │       │                                  │  │
│  │    │            │◀────│  (suspended - no CPU usage)              │  │
│  │    │            │     │       │                                  │  │
│  │  return         │     │       ▼ (event set)                      │  │
│  │  completion     │◀────│  workflow continues...                   │  │
│  │                 │     │                                          │  │
│  └─────────────────┘     └──────────────────────────────────────────┘  │
│                                                                         │
│  Communication: trio.Event objects + run_sync_soon_threadsafe          │
└─────────────────────────────────────────────────────────────────────────┘
```

### State Management

```python
class TrioWorkflowInstance(WorkflowInstance, _Runtime):
    def __init__(self, det: WorkflowInstanceDetails):
        # ... existing fields ...

        # Guest mode state
        self._guest_running: bool = False
        self._workflow_outcome: outcome.Outcome | None = None

        # Event-based waiting (replaces _WorkflowYield)
        self._timer_events: dict[int, trio.Event] = {}
        self._activity_events: dict[int, trio.Event] = {}
        self._condition_events: dict[int, trio.Event] = {}
        self._signal_event: trio.Event | None = None  # Wakes workflow to check conditions

        # Pending operations tracking
        self._pending_timers: dict[int, StartTimerCommand] = {}
        self._pending_conditions: list[tuple[int, Callable[[], bool], trio.Event]] = []
```

### Execution Flow

```
Activation 1 (WorkflowStarted):
┌────────────────────────────────────────────────────────────────────────┐
│ 1. activate() called with WorkflowStartedJob                           │
│ 2. Start guest run (first activation only):                            │
│    trio.lowlevel.start_guest_run(                                      │
│        self._workflow_main,                                            │
│        run_sync_soon_threadsafe=self._schedule_sync,                   │
│        done_callback=self._on_workflow_done,                           │
│        deterministic=True,                                             │
│        random_seed=self._random.getrandbits(64),                       │
│    )                                                                   │
│ 3. Workflow runs until it hits:                                        │
│    await workflow.wait_condition(lambda: self._approved, timeout=60)   │
│ 4. Inside wait_condition:                                              │
│    - fn() = False                                                      │
│    - Create timer event and command                                    │
│    - Register condition checker                                        │
│    - await event.wait()  # Suspends HERE                               │
│ 5. Guest run yields control back to host                               │
│ 6. Commands collected: [StartTimerCommand(0, 60000ms)]                 │
│ 7. Return completion                                                   │
└────────────────────────────────────────────────────────────────────────┘

Activation 2 (Signal arrives):
┌────────────────────────────────────────────────────────────────────────┐
│ 1. activate() called with SignalWorkflowJob("approve")                 │
│ 2. Process signal: self._approved = True                               │
│ 3. Check all pending conditions:                                       │
│    - fn() = True (approved!)                                           │
│    - Set condition event                                               │
│ 4. Workflow resumes from await event.wait()                            │
│ 5. wait_condition returns (condition satisfied)                        │
│ 6. Workflow continues to completion                                    │
│ 7. Commands: [CancelTimerCommand(0), CompleteWorkflowCommand]          │
└────────────────────────────────────────────────────────────────────────┘

Activation 2 (Timeout - alternative):
┌────────────────────────────────────────────────────────────────────────┐
│ 1. activate() called with TimerFiredJob(timer_id=0)                    │
│ 2. Find timer event, set it with timeout flag                          │
│ 3. Workflow resumes from await event.wait()                            │
│ 4. wait_condition sees timeout flag, raises TimeoutError               │
│ 5. Workflow catches TimeoutError, continues                            │
│ 6. Commands: [CompleteWorkflowCommand("Timed out")]                    │
└────────────────────────────────────────────────────────────────────────┘
```

### Implementation

#### Host Loop Integration

```python
class TrioWorkflowInstance(WorkflowInstance, _Runtime):
    def __init__(self, det: WorkflowInstanceDetails):
        # ... existing init ...
        self._guest_running = False
        self._activation_complete = threading.Event()
        self._pending_callbacks: list[Callable[[], None]] = []
        self._callback_lock = threading.Lock()

    def _schedule_sync(self, fn: Callable[[], None]) -> None:
        """Called by Trio guest to schedule callbacks on host."""
        with self._callback_lock:
            self._pending_callbacks.append(fn)

    def _run_pending_callbacks(self) -> None:
        """Process callbacks scheduled by Trio guest."""
        with self._callback_lock:
            callbacks = self._pending_callbacks
            self._pending_callbacks = []
        for cb in callbacks:
            cb()

    def _on_workflow_done(self, outcome: outcome.Outcome) -> None:
        """Called when Trio guest run completes."""
        self._workflow_outcome = outcome
        self._guest_running = False
        self._activation_complete.set()
```

#### Activation Handler

```python
def activate(self, act: WorkflowActivation) -> WorkflowActivationCompletion:
    """Handle an activation."""
    self._time_ns = act.timestamp_ns
    self._commands = []

    # Start guest run on first activation
    if not self._guest_running and self._has_workflow_start(act):
        self._start_guest_run(act)

    # Process jobs and set events
    for job in act.jobs:
        self._process_job(job)

    # Run Trio guest until it suspends or completes
    self._drive_guest_run()

    return WorkflowActivationCompletion(commands=self._commands)

def _start_guest_run(self, act: WorkflowActivation) -> None:
    """Start the Trio guest run for this workflow."""
    self._guest_running = True
    self._extract_start_args(act)

    trio.lowlevel.start_guest_run(
        self._workflow_main,
        run_sync_soon_threadsafe=self._schedule_sync,
        done_callback=self._on_workflow_done,
        deterministic=True,
        random_seed=self._random.getrandbits(64),
        clock=WorkflowClock(self._time_ns),
    )

def _drive_guest_run(self) -> None:
    """Drive the guest run until it suspends waiting for events."""
    # Process any pending callbacks from Trio
    while True:
        self._run_pending_callbacks()
        if not self._pending_callbacks:
            break

    # Check conditions after processing signals
    self._check_all_conditions()
```

#### Timer Implementation (Event-Based)

```python
async def workflow_sleep(self, duration: float, summary: str | None) -> None:
    """Sleep for the given duration using event-based suspension."""
    timer_id = self._timer_seq
    self._timer_seq += 1

    # Check if this timer has already fired (replay)
    if timer_id in self._fired_timers:
        return

    # Check for cancellation
    if self._cancel_requested:
        raise _WorkflowCancelled()

    # Create event for this timer
    event = trio.Event()
    self._timer_events[timer_id] = event

    # Create timer command
    self._commands.append(
        StartTimerCommand(
            timer_id=timer_id,
            duration_ms=int(duration * 1000),
            summary=summary,
        )
    )

    # Suspend until timer fires
    await event.wait()

    # Clean up
    del self._timer_events[timer_id]
```

#### wait_condition Implementation

```python
async def workflow_wait_condition(
    self,
    fn: Callable[[], bool],
    *,
    timeout: float | None = None,
    timeout_summary: str | None = None,
) -> None:
    """Wait until condition returns True or timeout expires.

    Uses event-based suspension - workflow truly suspends until
    condition is satisfied or timeout fires.
    """
    # Check if already satisfied
    if fn():
        return

    # Check for cancellation
    if self._cancel_requested:
        raise _WorkflowCancelled()

    # Create condition event
    cond_seq = self._condition_seq
    self._condition_seq += 1
    cond_event = trio.Event()

    # Register condition for checking after signals
    self._pending_conditions.append((cond_seq, fn, cond_event))

    # Create timeout timer if specified
    timer_id: int | None = None
    if timeout is not None:
        timer_id = self._timer_seq
        self._timer_seq += 1

        timer_event = trio.Event()
        self._timer_events[timer_id] = timer_event

        self._commands.append(
            StartTimerCommand(
                timer_id=timer_id,
                duration_ms=int(timeout * 1000),
                summary=timeout_summary,
            )
        )

    try:
        # Wait for either condition satisfied or timeout
        if timer_id is not None:
            # Race between condition and timeout
            async with trio.open_nursery() as nursery:
                async def wait_condition():
                    await cond_event.wait()
                    nursery.cancel_scope.cancel()

                async def wait_timeout():
                    await self._timer_events[timer_id].wait()
                    nursery.cancel_scope.cancel()

                nursery.start_soon(wait_condition)
                nursery.start_soon(wait_timeout)

            # Determine which completed
            if cond_event.is_set():
                # Condition satisfied - cancel timer
                if timer_id not in self._fired_timers:
                    self._commands.append(CancelTimerCommand(timer_id=timer_id))
            else:
                # Timeout fired
                raise TimeoutError("Condition timed out")
        else:
            # No timeout - just wait for condition
            await cond_event.wait()
    finally:
        # Clean up
        self._pending_conditions = [
            (s, f, e) for s, f, e in self._pending_conditions if s != cond_seq
        ]
        if timer_id is not None and timer_id in self._timer_events:
            del self._timer_events[timer_id]

def _check_all_conditions(self) -> None:
    """Check all pending conditions and set events for satisfied ones."""
    for cond_seq, fn, event in self._pending_conditions:
        try:
            if fn():
                event.set()
        except Exception:
            # Condition check failed - will be raised when workflow resumes
            event.set()
```

#### Job Processing

```python
def _process_job(self, job: Any) -> None:
    """Process a job and set appropriate events."""
    if isinstance(job, WorkflowStartedJob):
        self._start_args = job.args

    elif isinstance(job, TimerFiredJob):
        self._fired_timers.add(job.timer_id)
        if job.timer_id in self._timer_events:
            self._timer_events[job.timer_id].set()

    elif isinstance(job, SignalWorkflowJob):
        # Apply signal (mutates workflow state)
        self._apply_signal_sync(job)
        # Conditions will be checked in _check_all_conditions()

    elif isinstance(job, CancelWorkflowJob):
        self._cancel_requested = True
        # Set all pending events to wake workflow for cancellation
        for event in self._timer_events.values():
            event.set()
        for _, _, event in self._pending_conditions:
            event.set()
```

### Comparison: Replay vs Guest Mode

| Aspect | Replay Model (Current) | Guest Mode (New) |
|--------|------------------------|------------------|
| **Execution** | Re-run from beginning each activation | Suspend and resume in place |
| **CPU Usage** | O(n) per activation for n steps | O(1) per activation |
| **Memory** | Recreate workflow object each time | Persistent workflow object |
| **Trio Primitive** | `trio.run()` + `_WorkflowYield` exception | `start_guest_run()` + `trio.Event` |
| **Condition Check** | Inline when code runs | After signals, set events |
| **State** | Reconstructed via replay | Maintained in live objects |

### Comparison: asyncio SDK vs Trio SDK (Guest Mode)

| Aspect | asyncio SDK | Trio SDK (Guest Mode) |
|--------|-------------|----------------------|
| **Runtime** | Custom AbstractEventLoop | Trio guest run |
| **Suspension** | `asyncio.Future.wait()` | `trio.Event.wait()` |
| **Resumption** | `future.set_result()` | `event.set()` |
| **Callback Queue** | `self._ready` deque | `run_sync_soon_threadsafe` |
| **Condition Check** | `_check_conditions()` in `_run_once()` | `_check_all_conditions()` after jobs |
| **Determinism** | Custom event loop control | `deterministic=True` parameter |

### Bridge Types Update

```python
# In _activation.py
@dataclass
class CancelTimerCommand:
    """Command to cancel a pending timer."""
    timer_id: int

# In _bridge_types.py
elif isinstance(cmd, CancelTimerCommand):
    bridge_cmd.cancel_timer.seq = cmd.timer_id
```

### Public API

```python
# In temporalio_trio/workflow.py

async def wait_condition(
    fn: Callable[[], bool],
    *,
    timeout: timedelta | float | None = None,
    timeout_summary: str | None = None,
) -> None:
    """Wait until a condition becomes true.

    The condition function is evaluated after each signal is processed.
    If the condition becomes true, execution continues immediately.
    If a timeout is specified and expires first, TimeoutError is raised.

    Args:
        fn: A callable returning True when condition is met.
            Must be deterministic and side-effect free.
        timeout: Optional maximum wait time (timedelta or seconds).
        timeout_summary: Optional description for Temporal UI.

    Raises:
        TimeoutError: If timeout expires before condition becomes true.
        CancelledError: If workflow is cancelled while waiting.

    Example:
        # Wait for approval signal
        await workflow.wait_condition(lambda: self._approved)

        # Wait with timeout
        try:
            await workflow.wait_condition(
                lambda: self._approved,
                timeout=timedelta(hours=1),
            )
        except TimeoutError:
            # Handle timeout
            pass
    """
    runtime = _Runtime.current()

    if isinstance(timeout, timedelta):
        timeout = timeout.total_seconds()

    await runtime.workflow_wait_condition(
        fn,
        timeout=timeout,
        timeout_summary=timeout_summary,
    )
```

## Implementation Steps

### Phase 1: Guest Mode Infrastructure
1. Add guest mode state to `TrioWorkflowInstance`
2. Implement `_start_guest_run()` and host loop integration
3. Implement `_drive_guest_run()` callback processing
4. Update `activate()` to use guest mode

### Phase 2: Event-Based Primitives
1. Convert `workflow_sleep()` to use `trio.Event`
2. Convert `workflow_execute_activity()` to use events
3. Add event cleanup on completion/cancellation

### Phase 3: wait_condition
1. Add `CancelTimerCommand` to `_activation.py`
2. Update bridge types for `CancelTimerCommand`
3. Implement `workflow_wait_condition()` with event racing
4. Implement `_check_all_conditions()`
5. Add public API in `workflow.py`

### Phase 4: Testing
1. Unit tests for guest mode lifecycle
2. Unit tests for event-based sleep
3. Unit tests for wait_condition scenarios
4. E2E tests with real Temporal server

## Test Cases

```python
# Test 1: Condition satisfied by signal
@workflow.defn
class SignalConditionWorkflow:
    def __init__(self):
        self._value = 0

    @workflow.run
    async def run(self, target: int) -> str:
        await workflow.wait_condition(lambda: self._value >= target)
        return f"Reached {self._value}"

    @workflow.signal
    def add(self, amount: int):
        self._value += amount


# Test 2: Timeout
@workflow.defn
class TimeoutConditionWorkflow:
    def __init__(self):
        self._done = False

    @workflow.run
    async def run(self) -> str:
        try:
            await workflow.wait_condition(
                lambda: self._done,
                timeout=1.0,
            )
            return "Done"
        except TimeoutError:
            return "Timed out"


# Test 3: Condition already true
@workflow.defn
class AlreadyTrueWorkflow:
    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(lambda: True)  # Immediate return
        return "Done"


# Test 4: Signal before timeout (timer cancellation)
@workflow.defn
class SignalBeforeTimeoutWorkflow:
    def __init__(self):
        self._approved = False

    @workflow.run
    async def run(self) -> str:
        await workflow.wait_condition(
            lambda: self._approved,
            timeout=60.0,
        )
        return "Approved"  # Timer should be cancelled

    @workflow.signal
    def approve(self):
        self._approved = True


# Test 5: Multiple concurrent conditions (uses nursery)
@workflow.defn
class MultiplConditionsWorkflow:
    def __init__(self):
        self._a = False
        self._b = False

    @workflow.run
    async def run(self) -> str:
        async with trio.open_nursery() as nursery:
            async def wait_a():
                await workflow.wait_condition(lambda: self._a)
                return "A"

            async def wait_b():
                await workflow.wait_condition(lambda: self._b)
                return "B"

            # Both run concurrently
            nursery.start_soon(wait_a)
            nursery.start_soon(wait_b)

        return "Both done"

    @workflow.signal
    def set_a(self):
        self._a = True

    @workflow.signal
    def set_b(self):
        self._b = True
```

## Migration Path

The guest mode architecture is a significant change. Migration strategy:

1. **Implement guest mode in parallel** - Don't remove replay model yet
2. **Feature flag** - Allow choosing between models for testing
3. **Validate correctness** - Extensive replay testing
4. **Performance benchmarks** - Measure improvement
5. **Remove replay model** - Once guest mode is proven

## Future Extensions

1. **`all_handlers_finished()`** - Built-in condition for handler completion
2. **Condition with result** - Return value from condition
3. **Named conditions** - For debugging visibility
4. **Condition timeout with default** - Return default value instead of raising
