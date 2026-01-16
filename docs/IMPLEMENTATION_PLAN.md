# Implementation Plan: Trio-Based Temporal SDK POC

This document outlines the phased implementation plan for the Trio-based Temporal SDK proof of concept.

## Overview

**Goal:** Prove that Trio can serve as the async runtime for deterministic Temporal workflow execution.

**Success Criteria:**
1. A workflow can call `await workflow.sleep(duration)` and wake up after the specified time
2. Replaying the same workflow history produces identical execution
3. Multiple workflows can run in parallel with isolated, deterministic execution
4. Architecture matches the existing Python SDK patterns

**Development Rules:**
- Each phase must have unit tests covering all new code
- Each phase must pass `poe lint` before proceeding
- Each phase must pass `poe test` before proceeding

## Phase Summary

| Phase | Focus | Key Deliverables |
|-------|-------|------------------|
| 1 | Core Infrastructure | `_Runtime`, `_Definition`, decorators |
| 2 | Workflow Instance | `WorkflowInstanceDetails`, `TrioWorkflowInstance` |
| 3 | Time & Clock | `WorkflowClock`, history events, time control |
| 4 | Runner & Execution | `WorkflowRunner`, `TrioWorkflowRunner`, public API |
| 5 | Replay Verification | Replay tests, parallel execution tests |

---

## Phase 1: Core Infrastructure

**Goal:** Establish the foundational patterns that all workflow APIs build upon.

### 1.1 `_Runtime` Abstract Base Class

The central pattern - all workflow APIs delegate to `_Runtime.current()`.

```python
# temporalio_trio/workflow.py

from abc import ABC, abstractmethod
from contextvars import ContextVar

_current_runtime: ContextVar[_Runtime | None] = ContextVar(
    '_current_runtime', default=None
)

class _Runtime(ABC):
    """Abstract runtime providing workflow APIs.

    Mirrors temporalio.workflow._Runtime from the SDK.
    """

    @staticmethod
    def current() -> _Runtime:
        """Get current runtime, raise if not in workflow context."""
        runtime = _current_runtime.get()
        if runtime is None:
            raise RuntimeError("Not in workflow context")
        return runtime

    @staticmethod
    def maybe_current() -> _Runtime | None:
        """Get current runtime or None."""
        return _current_runtime.get()

    # Abstract methods - implemented by TrioWorkflowInstance
    @abstractmethod
    def workflow_time_ns(self) -> int:
        """Get current workflow time in nanoseconds."""
        ...

    @abstractmethod
    async def workflow_sleep(self, duration: float, summary: str | None) -> None:
        """Sleep for the given duration."""
        ...
```

### 1.2 `_Definition` Dataclass

Stores workflow metadata from decorators.

```python
from dataclasses import dataclass
from typing import Callable, Awaitable

@dataclass
class _Definition:
    """Workflow definition metadata.

    Mirrors temporalio.workflow._Definition from the SDK.
    """
    name: str
    cls: type
    run_fn: Callable[..., Awaitable]

    @staticmethod
    def from_class(cls: type) -> _Definition | None:
        """Get definition from a decorated class."""
        return getattr(cls, '__temporal_workflow_definition', None)

    @staticmethod
    def must_from_class(cls: type) -> _Definition:
        """Get definition, raising if not found."""
        defn = _Definition.from_class(cls)
        if defn is None:
            raise ValueError(f"{cls.__name__} is not a workflow class")
        return defn
```

### 1.3 Decorators (`@workflow.defn`, `@workflow.run`)

```python
import inspect

def defn(cls: type | None = None, *, name: str | None = None) -> type:
    """Decorator for workflow classes.

    Mirrors @temporalio.workflow.defn from the SDK.
    """
    def decorator(cls: type) -> type:
        # Find the @workflow.run method
        run_fn = None
        for attr_name in dir(cls):
            method = getattr(cls, attr_name, None)
            if getattr(method, '__temporal_workflow_run', False):
                if run_fn is not None:
                    raise ValueError("Multiple @workflow.run methods found")
                run_fn = method

        if run_fn is None:
            raise ValueError("Workflow must have a @workflow.run method")

        # Create and attach definition
        definition = _Definition(
            name=name or cls.__name__,
            cls=cls,
            run_fn=run_fn,
        )
        setattr(cls, '__temporal_workflow_definition', definition)
        return cls

    if cls is not None:
        return decorator(cls)
    return decorator


def run(fn: Callable) -> Callable:
    """Decorator for workflow run method.

    Mirrors @temporalio.workflow.run from the SDK.
    """
    if not inspect.iscoroutinefunction(fn):
        raise ValueError("Workflow run method must be async")
    setattr(fn, '__temporal_workflow_run', True)
    return fn
```

### 1.4 Tests for Phase 1

```python
# tests/test_workflow_definition.py

import pytest
from temporalio_trio import workflow

def test_workflow_defn_decorator():
    """Test @workflow.defn creates definition."""
    @workflow.defn
    class MyWorkflow:
        @workflow.run
        async def run(self) -> str:
            return "done"

    defn = workflow._Definition.from_class(MyWorkflow)
    assert defn is not None
    assert defn.name == "MyWorkflow"
    assert defn.cls is MyWorkflow

def test_workflow_defn_custom_name():
    """Test custom workflow name."""
    @workflow.defn(name="CustomName")
    class MyWorkflow:
        @workflow.run
        async def run(self) -> str:
            return "done"

    defn = workflow._Definition.from_class(MyWorkflow)
    assert defn.name == "CustomName"

def test_workflow_defn_requires_run():
    """Test @workflow.defn requires @workflow.run method."""
    with pytest.raises(ValueError, match="must have a @workflow.run method"):
        @workflow.defn
        class BadWorkflow:
            async def run(self) -> str:  # Missing @workflow.run
                return "done"

def test_workflow_run_requires_async():
    """Test @workflow.run requires async function."""
    with pytest.raises(ValueError, match="must be async"):
        @workflow.run
        def not_async() -> str:
            return "done"

def test_runtime_current_outside_workflow():
    """Test _Runtime.current() raises outside workflow context."""
    with pytest.raises(RuntimeError, match="Not in workflow context"):
        workflow._Runtime.current()

def test_runtime_maybe_current_outside_workflow():
    """Test _Runtime.maybe_current() returns None outside workflow."""
    assert workflow._Runtime.maybe_current() is None
```

### Phase 1 Checklist

- [ ] Implement `_Runtime` abstract base class
- [ ] Implement `_Definition` dataclass
- [ ] Implement `@workflow.defn` decorator
- [ ] Implement `@workflow.run` decorator
- [ ] Write unit tests
- [ ] Pass `poe lint`
- [ ] Pass `poe test`

---

## Phase 2: Workflow Instance

**Goal:** Implement the workflow instance that handles activations and provides runtime APIs.

### 2.1 `Info` Dataclass

```python
from dataclasses import dataclass

@dataclass
class Info:
    """Information about a running workflow.

    Mirrors temporalio.workflow.Info from the SDK.
    Simplified for POC - SDK has many more fields.
    """
    workflow_id: str
    workflow_type: str
    run_id: str
    task_queue: str
```

### 2.2 `WorkflowInstanceDetails` Dataclass

```python
@dataclass(frozen=True)
class WorkflowInstanceDetails:
    """Immutable details for creating a workflow instance.

    Mirrors temporalio.worker.WorkflowInstanceDetails from the SDK.
    """
    defn: _Definition
    info: Info
    randomness_seed: int
```

### 2.3 `WorkflowInstance` Abstract Base Class

```python
from abc import ABC, abstractmethod

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
```

### 2.4 `TrioWorkflowInstance`

The core implementation that extends both `WorkflowInstance` and `_Runtime`.

```python
import random
import trio

class TrioWorkflowInstance(WorkflowInstance, _Runtime):
    """Trio-based workflow instance.

    Implements both WorkflowInstance (activation handling) and
    _Runtime (workflow API implementation).
    """

    def __init__(self, det: WorkflowInstanceDetails) -> None:
        self._defn = det.defn
        self._info = det.info
        self._random = random.Random(det.randomness_seed)
        self._time_ns: int = 0
        self._pending_timers: dict[int, trio.Event] = {}
        self._timer_seq: int = 0
        self._workflow_instance: object | None = None
        self._completion: WorkflowActivationCompletion | None = None

    def activate(
        self,
        act: WorkflowActivation
    ) -> WorkflowActivationCompletion:
        """Handle activation using Trio's deterministic scheduling."""
        # Implementation in Phase 3
        ...

    # _Runtime implementation
    def workflow_time_ns(self) -> int:
        return self._time_ns

    async def workflow_sleep(self, duration: float, summary: str | None) -> None:
        """Sleep using Trio, controlled by workflow clock."""
        # Implementation details in Phase 3
        await trio.sleep(duration)
```

### 2.5 Tests for Phase 2

```python
# tests/test_workflow_instance.py

import pytest
from temporalio_trio import workflow
from temporalio_trio.worker import (
    WorkflowInstanceDetails,
    TrioWorkflowInstance,
)

@workflow.defn
class SimpleWorkflow:
    @workflow.run
    async def run(self) -> str:
        return "done"

def test_workflow_instance_details_immutable():
    """Test WorkflowInstanceDetails is immutable."""
    defn = workflow._Definition.must_from_class(SimpleWorkflow)
    info = workflow.Info(
        workflow_id="test-1",
        workflow_type="SimpleWorkflow",
        run_id="run-1",
        task_queue="test-queue",
    )
    details = WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=12345,
    )

    with pytest.raises(AttributeError):
        details.randomness_seed = 99999  # type: ignore

def test_trio_workflow_instance_creation():
    """Test TrioWorkflowInstance can be created."""
    defn = workflow._Definition.must_from_class(SimpleWorkflow)
    info = workflow.Info(
        workflow_id="test-1",
        workflow_type="SimpleWorkflow",
        run_id="run-1",
        task_queue="test-queue",
    )
    details = WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=12345,
    )

    instance = TrioWorkflowInstance(details)
    assert instance._defn is defn
    assert instance._info is info
    assert instance.workflow_time_ns() == 0

def test_trio_workflow_instance_is_runtime():
    """Test TrioWorkflowInstance implements _Runtime."""
    defn = workflow._Definition.must_from_class(SimpleWorkflow)
    info = workflow.Info(
        workflow_id="test-1",
        workflow_type="SimpleWorkflow",
        run_id="run-1",
        task_queue="test-queue",
    )
    details = WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=12345,
    )

    instance = TrioWorkflowInstance(details)
    assert isinstance(instance, workflow._Runtime)
```

### Phase 2 Checklist

- [ ] Implement `Info` dataclass
- [ ] Implement `WorkflowInstanceDetails` dataclass
- [ ] Implement `WorkflowInstance` abstract base class
- [ ] Implement `TrioWorkflowInstance` (skeleton)
- [ ] Write unit tests
- [ ] Pass `poe lint`
- [ ] Pass `poe test`

---

## Phase 3: Time & Clock

**Goal:** Implement time control for deterministic workflow execution.

### 3.1 History Events (Simplified)

```python
from dataclasses import dataclass
from typing import Any, Union

@dataclass
class WorkflowStartedJob:
    """Job to start workflow execution."""
    workflow_type: str
    args: tuple[Any, ...]

@dataclass
class TimerFiredJob:
    """Job indicating a timer has fired."""
    timer_id: int

@dataclass
class WorkflowActivation:
    """Activation containing jobs to process."""
    jobs: list[WorkflowStartedJob | TimerFiredJob]
    timestamp_ns: int

@dataclass
class StartTimerCommand:
    """Command to start a timer."""
    timer_id: int
    duration_ms: int

@dataclass
class CompleteWorkflowCommand:
    """Command to complete the workflow."""
    result: Any

@dataclass
class WorkflowActivationCompletion:
    """Completion with commands to execute."""
    commands: list[StartTimerCommand | CompleteWorkflowCommand]
```

### 3.2 `WorkflowClock`

Custom Trio clock that advances based on workflow time.

```python
import trio.abc

class WorkflowClock(trio.abc.Clock):
    """Clock controlled by workflow execution.

    This is Trio-specific - allows controlling time for deterministic replay.
    """

    def __init__(self, start_time_ns: int = 0) -> None:
        self._current_time_ns = start_time_ns
        self._pending_wakeups: list[float] = []

    def current_time(self) -> float:
        return self._current_time_ns / 1e9

    def deadline_to_sleep_time(self, deadline: float) -> float:
        """Calculate sleep time until deadline.

        For workflow execution, we record the deadline and return 0
        to indicate we should wake up immediately (time is virtual).
        """
        if deadline <= self.current_time():
            return 0.0
        # Record that something wants to wake at this time
        self._pending_wakeups.append(deadline)
        # Return inf to indicate "wait for external event"
        return float('inf')

    def advance_to(self, time_ns: int) -> None:
        """Advance clock to specified time."""
        if time_ns < self._current_time_ns:
            raise ValueError("Cannot move time backwards")
        self._current_time_ns = time_ns

    def advance_by(self, duration_ns: int) -> None:
        """Advance clock by duration."""
        self._current_time_ns += duration_ns
```

### 3.3 Complete `TrioWorkflowInstance.activate()`

```python
class TrioWorkflowInstance(WorkflowInstance, _Runtime):
    # ... (from Phase 2)

    def activate(
        self,
        act: WorkflowActivation
    ) -> WorkflowActivationCompletion:
        """Handle activation using Trio's deterministic scheduling."""
        self._time_ns = act.timestamp_ns
        self._commands: list = []

        # Set up runtime context
        token = _current_runtime.set(self)
        try:
            # Create clock at current workflow time
            clock = WorkflowClock(self._time_ns)

            # Run activation with Trio
            trio.run(
                self._run_activation,
                act,
                deterministic=True,
                random_seed=self._random.getrandbits(64),
                clock=clock,
            )

            return WorkflowActivationCompletion(commands=self._commands)
        finally:
            _current_runtime.reset(token)

    async def _run_activation(self, act: WorkflowActivation) -> None:
        """Execute workflow code within Trio context."""
        for job in act.jobs:
            if isinstance(job, WorkflowStartedJob):
                await self._handle_workflow_started(job)
            elif isinstance(job, TimerFiredJob):
                await self._handle_timer_fired(job)

    async def _handle_workflow_started(self, job: WorkflowStartedJob) -> None:
        """Handle workflow start job."""
        # Create workflow instance
        self._workflow_instance = self._defn.cls()

        # Run the workflow
        try:
            result = await self._defn.run_fn(self._workflow_instance, *job.args)
            self._commands.append(CompleteWorkflowCommand(result=result))
        except Exception as e:
            # TODO: Handle workflow failure
            raise

    async def _handle_timer_fired(self, job: TimerFiredJob) -> None:
        """Handle timer fired job."""
        if job.timer_id in self._pending_timers:
            event = self._pending_timers.pop(job.timer_id)
            event.set()

    async def workflow_sleep(self, duration: float, summary: str | None) -> None:
        """Sleep by creating a timer command."""
        timer_id = self._timer_seq
        self._timer_seq += 1

        # Record timer command
        self._commands.append(StartTimerCommand(
            timer_id=timer_id,
            duration_ms=int(duration * 1000),
        ))

        # Create event to wait on
        event = trio.Event()
        self._pending_timers[timer_id] = event

        # Wait for timer to fire (will be triggered by TimerFiredJob)
        await event.wait()
```

### 3.4 Tests for Phase 3

```python
# tests/test_workflow_clock.py

import pytest
from temporalio_trio.worker import WorkflowClock

def test_workflow_clock_initial_time():
    """Test clock starts at specified time."""
    clock = WorkflowClock(start_time_ns=5_000_000_000)
    assert clock.current_time() == 5.0

def test_workflow_clock_advance():
    """Test clock can advance forward."""
    clock = WorkflowClock(start_time_ns=0)
    clock.advance_to(10_000_000_000)
    assert clock.current_time() == 10.0

def test_workflow_clock_cannot_go_backwards():
    """Test clock cannot move backwards."""
    clock = WorkflowClock(start_time_ns=10_000_000_000)
    with pytest.raises(ValueError, match="Cannot move time backwards"):
        clock.advance_to(5_000_000_000)

# tests/test_workflow_activation.py

import pytest
import trio
from temporalio_trio import workflow
from temporalio_trio.worker import (
    TrioWorkflowInstance,
    WorkflowInstanceDetails,
    WorkflowActivation,
    WorkflowStartedJob,
    CompleteWorkflowCommand,
)

@workflow.defn
class SimpleWorkflow:
    @workflow.run
    async def run(self, value: str) -> str:
        return f"Hello, {value}!"

def test_workflow_activation_start():
    """Test workflow can be started via activation."""
    defn = workflow._Definition.must_from_class(SimpleWorkflow)
    info = workflow.Info(
        workflow_id="test-1",
        workflow_type="SimpleWorkflow",
        run_id="run-1",
        task_queue="test-queue",
    )
    details = WorkflowInstanceDetails(defn=defn, info=info, randomness_seed=42)
    instance = TrioWorkflowInstance(details)

    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="SimpleWorkflow", args=("World",))],
        timestamp_ns=0,
    )

    completion = instance.activate(activation)

    assert len(completion.commands) == 1
    assert isinstance(completion.commands[0], CompleteWorkflowCommand)
    assert completion.commands[0].result == "Hello, World!"
```

### Phase 3 Checklist

- [ ] Implement history event dataclasses
- [ ] Implement `WorkflowClock`
- [ ] Implement `TrioWorkflowInstance.activate()`
- [ ] Implement timer handling
- [ ] Write unit tests
- [ ] Pass `poe lint`
- [ ] Pass `poe test`

---

## Phase 4: Runner & Public API

**Goal:** Implement the workflow runner and expose the public API.

### 4.1 `WorkflowRunner` Abstract Base Class

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
```

### 4.2 `TrioWorkflowRunner`

```python
class TrioWorkflowRunner(WorkflowRunner):
    """Workflow runner that uses Trio for async execution."""

    def __init__(self) -> None:
        self._prepared: set[str] = set()

    def prepare_workflow(self, defn: _Definition) -> None:
        """Validate workflow is compatible with Trio execution."""
        if not defn.run_fn:
            raise ValueError("Workflow must have a run method")

        # Validate it's an async function
        if not inspect.iscoroutinefunction(defn.run_fn):
            raise ValueError("Workflow run method must be async")

        self._prepared.add(defn.name)

    def create_instance(self, det: WorkflowInstanceDetails) -> WorkflowInstance:
        """Create a Trio-based workflow instance."""
        if det.defn.name not in self._prepared:
            raise ValueError(f"Workflow {det.defn.name} not prepared")

        return TrioWorkflowInstance(det)
```

### 4.3 Public API Functions

```python
# temporalio_trio/workflow.py

async def sleep(duration: float, *, summary: str | None = None) -> None:
    """Sleep for the given duration.

    Mirrors temporalio.workflow.sleep from the SDK.

    Args:
        duration: Sleep duration in seconds.
        summary: Optional description for debugging.
    """
    await _Runtime.current().workflow_sleep(duration, summary)


def time() -> float:
    """Get current workflow time in seconds.

    Mirrors temporalio.workflow.time from the SDK.
    """
    return _Runtime.current().workflow_time_ns() / 1e9


def time_ns() -> int:
    """Get current workflow time in nanoseconds.

    Mirrors temporalio.workflow.time_ns from the SDK.
    """
    return _Runtime.current().workflow_time_ns()


def info() -> Info:
    """Get current workflow info.

    Mirrors temporalio.workflow.info from the SDK.
    """
    return _Runtime.current().workflow_info()
```

### 4.4 Tests for Phase 4

```python
# tests/test_workflow_runner.py

import pytest
from temporalio_trio import workflow
from temporalio_trio.worker import (
    TrioWorkflowRunner,
    WorkflowInstanceDetails,
    WorkflowActivation,
    WorkflowStartedJob,
    StartTimerCommand,
    TimerFiredJob,
    CompleteWorkflowCommand,
)

@workflow.defn
class SleepWorkflow:
    @workflow.run
    async def run(self, sleep_seconds: int) -> str:
        start = workflow.time()
        await workflow.sleep(sleep_seconds)
        end = workflow.time()
        return f"Slept from {start} to {end}"

def test_runner_prepare_workflow():
    """Test runner can prepare workflows."""
    runner = TrioWorkflowRunner()
    defn = workflow._Definition.must_from_class(SleepWorkflow)

    runner.prepare_workflow(defn)

    assert defn.name in runner._prepared

def test_runner_create_instance():
    """Test runner creates instances."""
    runner = TrioWorkflowRunner()
    defn = workflow._Definition.must_from_class(SleepWorkflow)
    runner.prepare_workflow(defn)

    info = workflow.Info(
        workflow_id="test-1",
        workflow_type="SleepWorkflow",
        run_id="run-1",
        task_queue="test-queue",
    )
    details = WorkflowInstanceDetails(defn=defn, info=info, randomness_seed=42)

    instance = runner.create_instance(details)
    assert instance is not None

def test_runner_requires_preparation():
    """Test runner requires workflow to be prepared first."""
    runner = TrioWorkflowRunner()
    defn = workflow._Definition.must_from_class(SleepWorkflow)

    info = workflow.Info(
        workflow_id="test-1",
        workflow_type="SleepWorkflow",
        run_id="run-1",
        task_queue="test-queue",
    )
    details = WorkflowInstanceDetails(defn=defn, info=info, randomness_seed=42)

    with pytest.raises(ValueError, match="not prepared"):
        runner.create_instance(details)

def test_workflow_sleep_creates_timer():
    """Test workflow.sleep() creates timer command."""
    runner = TrioWorkflowRunner()
    defn = workflow._Definition.must_from_class(SleepWorkflow)
    runner.prepare_workflow(defn)

    info = workflow.Info(
        workflow_id="test-1",
        workflow_type="SleepWorkflow",
        run_id="run-1",
        task_queue="test-queue",
    )
    details = WorkflowInstanceDetails(defn=defn, info=info, randomness_seed=42)
    instance = runner.create_instance(details)

    # Start workflow
    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="SleepWorkflow", args=(10,))],
        timestamp_ns=0,
    )
    completion = instance.activate(activation)

    # Should have timer command (workflow is waiting)
    assert any(isinstance(cmd, StartTimerCommand) for cmd in completion.commands)

def test_workflow_completes_after_timer():
    """Test workflow completes after timer fires."""
    runner = TrioWorkflowRunner()
    defn = workflow._Definition.must_from_class(SleepWorkflow)
    runner.prepare_workflow(defn)

    info = workflow.Info(
        workflow_id="test-1",
        workflow_type="SleepWorkflow",
        run_id="run-1",
        task_queue="test-queue",
    )
    details = WorkflowInstanceDetails(defn=defn, info=info, randomness_seed=42)
    instance = runner.create_instance(details)

    # Start workflow - will create timer
    activation1 = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="SleepWorkflow", args=(10,))],
        timestamp_ns=0,
    )
    completion1 = instance.activate(activation1)

    timer_cmd = next(c for c in completion1.commands if isinstance(c, StartTimerCommand))

    # Fire timer - workflow should complete
    activation2 = WorkflowActivation(
        jobs=[TimerFiredJob(timer_id=timer_cmd.timer_id)],
        timestamp_ns=10_000_000_000,  # 10 seconds later
    )
    completion2 = instance.activate(activation2)

    # Should have completion
    assert any(isinstance(cmd, CompleteWorkflowCommand) for cmd in completion2.commands)
```

### Phase 4 Checklist

- [ ] Implement `WorkflowRunner` abstract base class
- [ ] Implement `TrioWorkflowRunner`
- [ ] Implement public API (`sleep`, `time`, `time_ns`, `info`)
- [ ] Write unit tests
- [ ] Pass `poe lint`
- [ ] Pass `poe test`

---

## Phase 5: Replay Verification

**Goal:** Verify deterministic replay and parallel execution.

### 5.1 Replay Test

```python
# tests/test_replay.py

import pytest
from temporalio_trio import workflow
from temporalio_trio.worker import (
    TrioWorkflowRunner,
    WorkflowInstanceDetails,
    WorkflowActivation,
    WorkflowStartedJob,
    TimerFiredJob,
    StartTimerCommand,
    CompleteWorkflowCommand,
)

@workflow.defn
class MultiSleepWorkflow:
    @workflow.run
    async def run(self) -> list[float]:
        times = []
        for i in range(3):
            times.append(workflow.time())
            await workflow.sleep(5)
        times.append(workflow.time())
        return times

def execute_workflow(runner, defn, seed=42):
    """Execute workflow and capture history."""
    info = workflow.Info(
        workflow_id="test-1",
        workflow_type=defn.name,
        run_id="run-1",
        task_queue="test-queue",
    )
    details = WorkflowInstanceDetails(defn=defn, info=info, randomness_seed=seed)
    instance = runner.create_instance(details)

    history = []
    current_time_ns = 0

    # Start workflow
    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type=defn.name, args=())],
        timestamp_ns=current_time_ns,
    )
    history.append(activation)
    completion = instance.activate(activation)

    # Process timer commands until complete
    while not any(isinstance(c, CompleteWorkflowCommand) for c in completion.commands):
        timer_cmds = [c for c in completion.commands if isinstance(c, StartTimerCommand)]
        if not timer_cmds:
            break

        # Advance time and fire timer
        timer_cmd = timer_cmds[0]
        current_time_ns += timer_cmd.duration_ms * 1_000_000

        activation = WorkflowActivation(
            jobs=[TimerFiredJob(timer_id=timer_cmd.timer_id)],
            timestamp_ns=current_time_ns,
        )
        history.append(activation)
        completion = instance.activate(activation)

    result_cmd = next(c for c in completion.commands if isinstance(c, CompleteWorkflowCommand))
    return result_cmd.result, history

def replay_workflow(runner, defn, history, seed=42):
    """Replay workflow with recorded history."""
    info = workflow.Info(
        workflow_id="test-1",
        workflow_type=defn.name,
        run_id="run-1",
        task_queue="test-queue",
    )
    details = WorkflowInstanceDetails(defn=defn, info=info, randomness_seed=seed)
    instance = runner.create_instance(details)

    completion = None
    for activation in history:
        completion = instance.activate(activation)

    result_cmd = next(c for c in completion.commands if isinstance(c, CompleteWorkflowCommand))
    return result_cmd.result

def test_replay_produces_same_result():
    """Test that replaying history produces identical result."""
    runner = TrioWorkflowRunner()
    defn = workflow._Definition.must_from_class(MultiSleepWorkflow)
    runner.prepare_workflow(defn)

    # Execute and capture history
    result1, history = execute_workflow(runner, defn)

    # Replay with same history
    result2 = replay_workflow(runner, defn, history)

    assert result1 == result2
    assert result1 == [0.0, 5.0, 10.0, 15.0]
```

### 5.2 Parallel Execution Test

```python
# tests/test_parallel.py

import pytest
from concurrent.futures import ThreadPoolExecutor
from temporalio_trio import workflow
from temporalio_trio.worker import (
    TrioWorkflowRunner,
    WorkflowInstanceDetails,
    WorkflowActivation,
    WorkflowStartedJob,
)

@workflow.defn
class DeterministicWorkflow:
    @workflow.run
    async def run(self, iterations: int) -> list[float]:
        """Workflow that should produce deterministic output."""
        import random as stdlib_random

        # This should fail if using global random
        times = []
        for _ in range(iterations):
            times.append(workflow.time())
            await workflow.sleep(1)
        return times

def run_workflow_in_thread(workflow_id: str, seed: int) -> list[float]:
    """Run a workflow in a separate thread."""
    runner = TrioWorkflowRunner()
    defn = workflow._Definition.must_from_class(DeterministicWorkflow)
    runner.prepare_workflow(defn)

    info = workflow.Info(
        workflow_id=workflow_id,
        workflow_type="DeterministicWorkflow",
        run_id=f"run-{workflow_id}",
        task_queue="test-queue",
    )
    details = WorkflowInstanceDetails(defn=defn, info=info, randomness_seed=seed)
    instance = runner.create_instance(details)

    # Execute workflow (simplified - full execution would need timer handling)
    activation = WorkflowActivation(
        jobs=[WorkflowStartedJob(workflow_type="DeterministicWorkflow", args=(3,))],
        timestamp_ns=0,
    )
    # ... execute to completion
    return result

def test_parallel_workflows_isolated():
    """Test that parallel workflows are fully isolated."""
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(run_workflow_in_thread, f"wf-{i}", seed=i * 1000)
            for i in range(4)
        ]
        results = [f.result() for f in futures]

    # Each workflow should complete without affecting others
    assert len(results) == 4

def test_same_seed_same_result():
    """Test that same seed produces same result."""
    result1 = run_workflow_in_thread("wf-a", seed=12345)
    result2 = run_workflow_in_thread("wf-b", seed=12345)

    assert result1 == result2

def test_different_seed_different_random():
    """Test that different seeds produce different random sequences."""
    # When we add workflow.random(), verify isolation
    pass
```

### Phase 5 Checklist

- [ ] Implement replay test
- [ ] Implement parallel execution test
- [ ] Verify determinism with same seed
- [ ] Verify isolation between parallel workflows
- [ ] Pass `poe lint`
- [ ] Pass `poe test`

---

## File Structure After Implementation

```
temporalio_trio/
├── __init__.py                 # Package exports
├── workflow.py                 # Public workflow API
│   ├── _Runtime (ABC)
│   ├── _Definition
│   ├── Info
│   ├── defn(), run() decorators
│   └── sleep(), time(), time_ns(), info()
└── worker/
    ├── __init__.py             # Worker exports
    ├── _workflow_instance.py   # Implementation
    │   ├── WorkflowRunner (ABC)
    │   ├── TrioWorkflowRunner
    │   ├── WorkflowInstance (ABC)
    │   ├── TrioWorkflowInstance
    │   ├── WorkflowInstanceDetails
    │   └── WorkflowClock
    └── _activation.py          # Activation types
        ├── WorkflowActivation
        ├── WorkflowActivationCompletion
        ├── WorkflowStartedJob
        ├── TimerFiredJob
        ├── StartTimerCommand
        └── CompleteWorkflowCommand

tests/
├── __init__.py
├── test_basic.py               # Existing
├── test_workflow_definition.py # Phase 1
├── test_workflow_instance.py   # Phase 2
├── test_workflow_clock.py      # Phase 3
├── test_workflow_activation.py # Phase 3
├── test_workflow_runner.py     # Phase 4
├── test_replay.py              # Phase 5
└── test_parallel.py            # Phase 5
```

---

## Dependencies

### Required: Trio Fork with Deterministic Scheduling

```bash
# Install from fork
pip install git+https://github.com/mfateev/trio.git@temporal-deterministic-scheduling
```

### pyproject.toml Updates

```toml
dependencies = [
    "trio>=0.27.0",  # With deterministic scheduling (from fork)
    "attrs>=23.2.0",
    "outcome>=1.3.0",
    "typing-extensions>=4.2.0,<5",
]
```

---

## Success Metrics

After completing all phases:

1. **Unit Tests:** All tests pass
2. **Linting:** `poe lint` passes (pyright, mypy, ruff)
3. **Replay:** Same history produces identical results
4. **Parallelism:** Multiple workflows run concurrently without interference
5. **API Compatibility:** Public API matches Temporal SDK patterns

---

## Next Steps (Post-POC)

Once POC is validated:

1. **Integrate with Temporal Server** - Real gRPC communication
2. **Add Activities** - External operation support
3. **Add Signals/Queries** - Workflow interaction
4. **Add Child Workflows** - Workflow composition
5. **Consider Sandbox** - Determinism enforcement (see Out of Scope in CLAUDE.md)
