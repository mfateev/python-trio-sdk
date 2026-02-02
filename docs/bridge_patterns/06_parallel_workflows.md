# Parallel Workflow Patterns (20)

This pattern validates SDK-Core behavior when handling multiple concurrent workflows through the same bridge connection.

## State Machine: Parallel Workflow Processing

```
PARALLEL WORKFLOW PROCESSING
============================

Bridge maintains separate state per run_id:

  ┌────────────────────────────────────────────────────────────┐
  │                      SDK-Core                              │
  │                                                            │
  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐    │
  │  │ Workflow A   │  │ Workflow B   │  │ Workflow C   │    │
  │  │ run_id: aaa  │  │ run_id: bbb  │  │ run_id: ccc  │    │
  │  │ state: timer │  │ state: init  │  │ state: done  │    │
  │  └──────────────┘  └──────────────┘  └──────────────┘    │
  │         │                 │                               │
  │         └────────┬────────┘                               │
  │                  │                                        │
  │           ┌──────▼──────┐                                 │
  │           │  Activation │                                 │
  │           │    Queue    │                                 │
  │           └──────┬──────┘                                 │
  │                  │                                        │
  └──────────────────┼────────────────────────────────────────┘
                     │
              poll_workflow_activation()
                     │
                     ▼
  ┌────────────────────────────────────────────────────────────┐
  │                    Python SDK Worker                       │
  │                                                            │
  │   activation.run_id identifies which workflow              │
  │   completion.run_id routes response to correct workflow    │
  │                                                            │
  └────────────────────────────────────────────────────────────┘
```

## Pattern 20: Multiple Parallel Workflows

**Scenario**: Start N workflows, process activations interleaved, complete all.

### Sequence Diagram

```
┌────────┐     ┌────────┐     ┌────────┐     ┌──────────┐     ┌─────────┐
│ WF A   │     │ WF B   │     │ WF C   │     │ SDK-Core │     │ Worker  │
└───┬────┘     └───┬────┘     └───┬────┘     └────┬─────┘     └────┬────┘
    │              │              │               │                 │
    │ start        │              │               │                 │
    │─────────────────────────────────────────────▶                 │
    │              │ start        │               │                 │
    │              │──────────────────────────────▶                 │
    │              │              │ start         │                 │
    │              │              │───────────────▶                 │
    │              │              │               │                 │
    │              │              │               │ poll            │
    │              │              │               │◀────────────────│
    │              │              │               │                 │
    │              │              │               │ activation(B)   │
    │              │              │               │ run_id=bbb      │
    │              │              │               │────────────────▶│
    │              │              │               │                 │
    │              │              │               │ complete(bbb)   │
    │              │              │               │◀────────────────│
    │              │              │               │                 │
    │              │              │               │ poll            │
    │              │              │               │◀────────────────│
    │              │              │               │                 │
    │              │              │               │ activation(A)   │
    │              │              │               │ run_id=aaa      │
    │              │              │               │────────────────▶│
    │              │              │               │                 │
   ...            ...            ...             ...               ...
```

### Key Observations

#### 1. Activation Order is Non-Deterministic

```
ACTIVATION ORDER
================

Started workflows in order: A, B, C
Observed poll order: B, A, C  (or any permutation)

The order activations arrive does NOT match:
  • Start order
  • Workflow ID alphabetical order
  • Any predictable pattern

SDK Worker must track workflows by run_id, not assume order.
```

#### 2. Run ID Routing

```
RUN ID ROUTING
==============

Each workflow has a unique run_id:
  • Workflow A: run_id = "aaa..."
  • Workflow B: run_id = "bbb..."
  • Workflow C: run_id = "ccc..."

Activation arrives:
  activation.run_id = "bbb..."  → This is for Workflow B

Completion must use same run_id:
  completion.run_id = "bbb..."  → Routes to Workflow B

INVALID:
  activation.run_id = "bbb..."
  completion.run_id = "aaa..."  ❌ Wrong workflow!
```

#### 3. Timer Fire Order

```
TIMER FIRE ORDER - SURPRISING BEHAVIOR
======================================

Setup:
  Workflow 0: StartTimer(100ms)
  Workflow 1: StartTimer(200ms)
  Workflow 2: StartTimer(300ms)

Expected fire order (by duration): [0, 1, 2]
Observed fire order: May vary!

SDK-Core may batch or reorder timer fires.
Do NOT assume timers fire in strict duration order across workflows.

Within a SINGLE workflow, timers fire in expiration order.
Across workflows, order is not guaranteed.
```

### Test Scenarios

#### Basic Parallel (3 workflows)

```
Timeline:
  t1: start_workflow(A), start_workflow(B), start_workflow(C)
  t2: poll → activation(run_id=X, initialize_workflow)
  t3: complete(run_id=X, [StartTimer])
  t4: poll → activation(run_id=Y, initialize_workflow)
  t5: complete(run_id=Y, [StartTimer])
  t6: poll → activation(run_id=Z, initialize_workflow)
  t7: complete(run_id=Z, [StartTimer])
  t8: poll → activation(run_id=?, fire_timer)
  ... continue until all complete
```

#### Interleaved Completion

```
Timeline:
  t1: Initialize all 3 workflows with different timer durations
  t2: Timer fires arrive (order may not match duration order)
  t3: Complete each workflow as its timer fires

Key insight: Completion order depends on SDK-Core scheduling,
not strictly on timer expiration times.
```

#### Mixed Operations

```
Timeline:
  t1: Initialize workflow A (timer)
  t2: Initialize workflow B (activity)
  t3: Initialize workflow C (signal wait)
  t4: Send signal to C
  t5: Poll - may get any of: fire_timer(A), resolve_activity(B), signal_workflow(C)
  t6: Complete each based on received job type

Key insight: Different job types can arrive interleaved across workflows.
Worker must handle any job type for any workflow at any time.
```

### Code Pattern

```python
# Track workflows by run_id
workflows: dict[str, WorkflowState] = {}

while not all_complete(workflows):
    activation_bytes = await bridge.poll_workflow_activation()
    activation = parse_activation(activation_bytes)

    run_id = activation.run_id  # Identifies which workflow

    if run_id not in workflows:
        # New workflow
        workflows[run_id] = WorkflowState(run_id)

    # Process jobs for this specific workflow
    for job in activation.jobs:
        handle_job(workflows[run_id], job)

    # Complete with matching run_id
    completion = build_completion(run_id, commands)
    await bridge.complete_workflow_activation(completion)
```

### Learnings

1. **Run ID is the routing key**
   - Every activation has a run_id
   - Completion must use matching run_id
   - SDK-Core uses run_id to route internally

2. **Order is non-deterministic**
   - Don't assume start order = poll order
   - Don't assume timer duration order = fire order
   - Always identify workflow by run_id

3. **Independent state machines**
   - Each workflow is an independent state machine
   - Jobs for workflow A don't affect workflow B
   - Completions are per-workflow

4. **Single poll returns one activation**
   - Each poll returns activation for ONE workflow
   - Multiple jobs may be in that activation (same workflow)
   - Never multiple workflows in one activation

5. **Completion required before re-activation**
   - SDK-Core won't send another activation for the same workflow
     until the previous one is completed
   - Different workflows can have pending activations simultaneously

---

## Test File

See `tests/bridge_patterns/test_bridge_parallel.py` for executable examples.

```bash
uv run pytest tests/bridge_patterns/test_bridge_parallel.py -v -m temporal_server
```
