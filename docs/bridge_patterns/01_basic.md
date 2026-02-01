# Basic Bridge Patterns (1-7)

These patterns cover the fundamental workflow lifecycle and timer operations.

## Pattern 1: Basic Workflow Complete

**Scenario**: Simple workflow that completes immediately.

### Flow
```
1. Start workflow via CLI
2. poll → [initialize_workflow(workflow_type, args)]
3. complete([CompleteWorkflowExecution(result)])
4. Verify workflow status: COMPLETED
```

### Key Fields

**initialize_workflow job**:
- `workflow_type`: Name of the workflow type
- `arguments`: List of Payload protobufs with workflow arguments
- `workflow_id`: Unique workflow identifier
- `parent_workflow_info`: Parent details (if child workflow)

**CompleteWorkflowExecution command**:
- `result`: Single Payload protobuf with return value

### Learnings
- First activation always has `initialize_workflow` job
- `run_id` in activation identifies this specific execution
- Result must be serialized as Payload protobuf

---

## Pattern 2: Timer Workflow

**Scenario**: Workflow with timer (sleep).

### Flow
```
1. Start workflow
2. poll → [initialize_workflow]
3. complete([StartTimer(seq=1, duration=1s)])
4. poll → [fire_timer(seq=1)]
5. complete([CompleteWorkflowExecution])
```

### Key Fields

**StartTimer command**:
- `seq`: Sequence number for matching (workflow-local)
- `start_to_fire_timeout`: Duration protobuf
- User metadata can include `summary` for UI visibility

**fire_timer job**:
- `seq`: Matches the StartTimer seq

### Learnings
- Timer `seq` numbers start at 1 and increment
- Duration is google.protobuf.Duration (seconds + nanos)
- Timer fires come as separate activation after duration elapses

---

## Pattern 3: Workflow Cancellation

**Scenario**: External cancellation request.

### Flow
```
1. Start workflow
2. poll → [initialize_workflow]
3. complete([StartTimer(seq=1, duration=60s)])  # Keep running
4. Cancel via CLI: temporal workflow cancel
5. poll → [cancel_workflow]
6. complete([CancelWorkflowExecution]) or handle gracefully
```

### Key Fields

**cancel_workflow job**:
- `details`: Optional cancellation reason/details as Payloads

**CancelWorkflowExecution command**:
- Empty command (just presence indicates cancellation)

### Learnings
- Workflow can choose to ignore cancellation (but shouldn't)
- Can complete with `CancelWorkflowExecution` or regular completion
- Cancellation is a request, not a force kill

---

## Pattern 4: Cache Eviction

**Scenario**: Workflow evicted from cache.

### Flow
```
1. Start workflow, process normally
2. Simulate cache pressure
3. poll → [remove_from_cache]
4. Discard workflow state
5. On next task, replay from history
```

### Key Fields

**remove_from_cache job**:
- `message`: Human-readable reason for eviction
- `reason`: Enum indicating cause (resource pressure, explicit, etc.)

### Learnings
- Evicted workflows must be re-created on next activation
- All in-memory state is lost
- History replay reconstructs workflow state
- This is why determinism matters!

---

## Pattern 5: Sticky Queue Behavior

**Scenario**: Understanding sticky task queues.

### Flow
```
1. Start workflow, complete first activation
2. poll → Receives task (workflow "sticks" to this worker)
3. Subsequent tasks come directly to this worker
4. If worker unavailable, falls back to normal queue after timeout
```

### Learnings
- Sticky queues improve performance by skipping replay
- Worker keeps recent workflow in memory
- Sticky timeout controls fallback behavior
- Cache eviction (Pattern 4) breaks stickiness

---

## Pattern 6: Multiple Timers

**Scenario**: Concurrent timers in same workflow.

### Flow
```
1. poll → [initialize_workflow]
2. complete([StartTimer(seq=1, 2s), StartTimer(seq=2, 1s)])
3. poll → [fire_timer(seq=2)]  # Shorter timer first
4. complete([])
5. poll → [fire_timer(seq=1)]
6. complete([CompleteWorkflowExecution])
```

### Key Fields
- Each timer has unique `seq`
- Multiple timers can fire in same activation (if same time)

### Learnings
- Timers fire in order of expiration
- Multiple timers can be started in one completion
- Can cancel timer before it fires with `cancel_timer`
- Sequence numbers are for matching, not ordering

---

## Pattern 7: Empty Completion

**Scenario**: Acknowledging activation without new commands.

### Flow
```
1. poll → [some_job]
2. complete([])  # No commands, just acknowledge
3. Workflow remains running
```

### Key Fields
- `successful.commands`: Empty list
- No failure/error indicated

### Learnings
- Empty completion is valid
- Used when job doesn't require new commands
- Common for signals that just update state
- Don't confuse with completing the workflow (which needs `complete_workflow_execution`)

---

## Test File

See `tests/bridge_patterns/test_basic.py` for executable examples of these patterns.

Note: Patterns 1-7 were validated during initial SDK development. The test infrastructure in `tests/bridge_patterns/` focuses on patterns 8-19.
