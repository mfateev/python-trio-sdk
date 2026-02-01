# Child Workflow Patterns (14-15)

These patterns cover spawning and managing child workflows.

## State Machine: Child Workflow Lifecycle

```
State Machine: Child Workflow
=============================

Parent States:
  [INIT]           - No child started
  [START_PENDING]  - StartChildWorkflowExecution sent
  [STARTED]        - Child running (got start confirmation)
  [CANCEL_PENDING] - CancelChildWorkflowExecution sent
  [RESOLVED]       - Child completed/failed/cancelled

Child States:
  [NOT_STARTED]    - Not yet scheduled
  [STARTING]       - Being created by server
  [RUNNING]        - Executing workflow code
  [COMPLETING]     - Finishing execution

Parent Transitions:
  INIT ──StartChildWorkflowExecution(seq=N)──▶ START_PENDING
  START_PENDING ──resolve_child_..._start(succeeded)──▶ STARTED
  START_PENDING ──resolve_child_..._start(failed)──▶ RESOLVED
  STARTED ──CancelChildWorkflowExecution(seq=N)──▶ CANCEL_PENDING
  STARTED ──resolve_child_..._execution(completed)──▶ RESOLVED
  CANCEL_PENDING ──resolve_child_..._execution(cancelled)──▶ RESOLVED
```

## Pattern 14: Child Workflow (Success)

**Scenario**: Start child workflow → receive completion.

### Sequence Diagram

```
┌──────────┐          ┌──────────┐          ┌───────────┐          ┌───────┐
│  Parent  │          │ SDK-Core │          │  Server   │          │ Child │
│ Workflow │          │          │          │           │          │Workflow│
└────┬─────┘          └────┬─────┘          └─────┬─────┘          └───┬───┘
     │                     │                      │                    │
     │◀──initialize_workflow                      │                    │
     │                     │                      │                    │
     │ StartChildWorkflow  │                      │                    │
     │ Execution(seq=1)    │                      │                    │
     │────────────────────▶│                      │                    │
     │                     │  StartChildWorkflow  │                    │
     │                     │─────────────────────▶│                    │
     │                     │                      │                    │
     │                     │      Started(run_id) │                    │
     │                     │◀─────────────────────│                    │
     │ resolve_child_      │                      │                    │
     │ workflow_execution_ │                      │                    │
     │ start(succeeded)    │                      │                    │
     │◀────────────────────│                      │                    │
     │                     │                      │                    │
     │ empty completion    │                      │ initialize_workflow│
     │────────────────────▶│                      │───────────────────▶│
     │                     │                      │                    │
     │                     │                      │ CompleteWorkflow   │
     │                     │                      │◀───────────────────│
     │                     │                      │                    │
     │                     │    Child Completed   │                    │
     │                     │◀─────────────────────│                    │
     │ resolve_child_      │                      │                    │
     │ workflow_execution  │                      │                    │
     │ (completed)         │                      │                    │
     │◀────────────────────│                      │                    │
     │                     │                      │                    │
     │ CompleteWorkflow    │                      │                    │
     │────────────────────▶│                      │                    │
     │                     │                      │                    │
```

### Key Fields

**StartChildWorkflowExecution command**:
```protobuf
start_child_workflow_execution {
  seq: 1                                    # Sequence number
  workflow_id: "child-wf-123"              # Child's workflow ID
  workflow_type: "ChildWorkflow"           # Child's type name
  task_queue: "my-queue"                   # Where to run child
  input: [Payload, ...]                    # Arguments

  # Timeouts
  workflow_execution_timeout { seconds: 3600 }  # Total including retries
  workflow_run_timeout { seconds: 300 }         # Single run
  workflow_task_timeout { seconds: 10 }         # Single task

  # Policies
  parent_close_policy: PARENT_CLOSE_POLICY_TERMINATE  # 1
  cancellation_type: CHILD_WORKFLOW_CANCELLATION_WAIT  # 2
  workflow_id_reuse_policy: WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE  # 1

  # Optional retry policy
  retry_policy { ... }
}
```

**resolve_child_workflow_execution_start job (success)**:
```protobuf
resolve_child_workflow_execution_start {
  seq: 1
  succeeded {
    run_id: "abc123-run-id"  # Child's run ID
  }
}
```

**resolve_child_workflow_execution job (success)**:
```protobuf
resolve_child_workflow_execution {
  seq: 1
  result {
    completed {
      result: Payload  # Child's return value
    }
  }
}
```

### Two-Phase Resolution

Child workflows have two-phase resolution:

1. **Start Phase**: `resolve_child_workflow_execution_start`
   - Confirms child was accepted and started
   - Returns child's `run_id`
   - Can fail if workflow ID conflicts or server rejects

2. **Completion Phase**: `resolve_child_workflow_execution`
   - Confirms child completed/failed/cancelled
   - Contains final result or failure

### Parent Close Policies

| Policy | Value | Behavior |
|--------|-------|----------|
| `TERMINATE` | 1 | Terminate child when parent closes |
| `ABANDON` | 2 | Let child continue running |
| `REQUEST_CANCEL` | 3 | Request cancellation, don't wait |

### Workflow ID Reuse Policies

| Policy | Value | Behavior |
|--------|-------|----------|
| `ALLOW_DUPLICATE` | 1 | Allow same ID if previous completed |
| `ALLOW_DUPLICATE_FAILED_ONLY` | 2 | Allow same ID only if previous failed |
| `REJECT_DUPLICATE` | 3 | Reject if workflow with ID exists |
| `TERMINATE_IF_RUNNING` | 4 | Terminate existing and start new |

---

## Pattern 14 Variant: Child Start Failed

**Scenario**: Child workflow cannot be started.

### Sequence Diagram

```
┌──────────┐          ┌──────────┐          ┌───────────┐
│  Parent  │          │ SDK-Core │          │  Server   │
│ Workflow │          │          │          │           │
└────┬─────┘          └────┬─────┘          └─────┬─────┘
     │                     │                      │
     │ StartChildWorkflow  │                      │
     │ (id="existing-id")  │                      │
     │────────────────────▶│                      │
     │                     │  StartChildWorkflow  │
     │                     │─────────────────────▶│
     │                     │                      │
     │                     │  Already Exists Error│
     │                     │◀─────────────────────│
     │ resolve_child_      │                      │
     │ workflow_execution_ │                      │
     │ start(failed)       │                      │
     │◀────────────────────│                      │
     │                     │                      │
```

### Key Fields

**resolve_child_workflow_execution_start job (failure)**:
```protobuf
resolve_child_workflow_execution_start {
  seq: 1
  failed {
    workflow_id: "child-wf-123"
    workflow_type: "ChildWorkflow"
    cause: START_CHILD_WORKFLOW_EXECUTION_FAILED_CAUSE_WORKFLOW_ALREADY_EXISTS
  }
}
```

### Start Failure Causes

| Cause | Description |
|-------|-------------|
| `WORKFLOW_ALREADY_EXISTS` | Workflow ID already in use |
| `NAMESPACE_NOT_FOUND` | Target namespace doesn't exist |
| Other | Server-specific errors |

---

## Pattern 15: Child Workflow Cancellation

**Scenario**: Cancel running child workflow.

### CRITICAL INSIGHT

**The `CancelChildWorkflowExecution` command MUST be sent in the SAME completion that acknowledges `resolve_child_workflow_execution_start`.**

You cannot:
1. Receive `resolve_child_workflow_execution_start`
2. Send empty completion (ACK)
3. Later send `CancelChildWorkflowExecution`

This fails because step 3 has no pending activation to complete.

### Correct Sequence Diagram

```
┌──────────┐          ┌──────────┐          ┌───────────┐          ┌───────┐
│  Parent  │          │ SDK-Core │          │  Server   │          │ Child │
│ Workflow │          │          │          │           │          │Workflow│
└────┬─────┘          └────┬─────┘          └─────┬─────┘          └───┬───┘
     │                     │                      │                    │
     │ StartChildWorkflow  │                      │                    │
     │ Execution(seq=1)    │                      │                    │
     │────────────────────▶│                      │                    │
     │                     │  StartChildWorkflow  │                    │
     │                     │─────────────────────▶│                    │
     │                     │                      │                    │
     │                     │      Started(run_id) │                    │
     │                     │◀─────────────────────│                    │
     │ resolve_child_      │                      │                    │
     │ workflow_execution_ │                      │                    │
     │ start(succeeded)    │                      │                    │
     │◀────────────────────│                      │                    │
     │                     │                      │                    │
     ├─────────────────────┼──────────────────────┼────────────────────┤
     │ CRITICAL: Cancel    │                      │                    │
     │ command goes in     │                      │                    │
     │ THIS completion!    │                      │                    │
     ├─────────────────────┼──────────────────────┼────────────────────┤
     │                     │                      │                    │
     │ CancelChildWorkflow │                      │                    │
     │ Execution(seq=1)    │                      │                    │
     │────────────────────▶│                      │                    │
     │                     │  CancelWorkflow      │                    │
     │                     │─────────────────────▶│                    │
     │                     │                      │ cancel_workflow    │
     │                     │                      │───────────────────▶│
     │                     │                      │                    │
     │                     │                      │ CancelWorkflow     │
     │                     │                      │ Execution          │
     │                     │                      │◀───────────────────│
     │                     │                      │                    │
     │                     │    Child Cancelled   │                    │
     │                     │◀─────────────────────│                    │
     │ resolve_child_      │                      │                    │
     │ workflow_execution  │                      │                    │
     │ (cancelled)         │                      │                    │
     │◀────────────────────│                      │                    │
     │                     │                      │                    │
     │ CompleteWorkflow    │                      │                    │
     │────────────────────▶│                      │                    │
     │                     │                      │                    │
```

### State Machine: Cancel Operation

```
CANCEL CHILD WORKFLOW - Correct Flow
=====================================

State 1: Parent receives resolve_child_workflow_execution_start(succeeded)
         Activation = PENDING

State 2: Parent builds completion with CancelChildWorkflowExecution(seq=1)
         Activation = COMPLETING

State 3: Parent sends completion
         Activation = COMPLETE (consumed)
         Cancel request sent to server

State 4: Parent receives resolve_child_workflow_execution(cancelled)
         New Activation = PENDING


CANCEL CHILD WORKFLOW - INCORRECT Flow (Will Fail!)
===================================================

State 1: Parent receives resolve_child_workflow_execution_start(succeeded)
         Activation = PENDING

State 2: Parent sends empty completion (just ACK)
         Activation = COMPLETE (consumed)

State 3: Parent tries to send CancelChildWorkflowExecution(seq=1)
         ERROR: No pending activation!  ❌
```

### Key Fields

**CancelChildWorkflowExecution command**:
```protobuf
cancel_child_workflow_execution {
  child_workflow_seq: 1  # Matches StartChildWorkflowExecution.seq
}
```

**resolve_child_workflow_execution job (cancelled)**:
```protobuf
resolve_child_workflow_execution {
  seq: 1
  result {
    cancelled {
      failure {
        message: "Child Workflow execution cancelled"
        canceled_failure_info { ... }
      }
    }
  }
}
```

### Child Cancellation Behavior

When parent cancels child:

1. Child receives `cancel_workflow` job
2. Child can:
   - Complete with `cancel_workflow_execution` (cooperative)
   - Complete normally (ignore cancel)
   - Fail with error

The `cancellation_type` on `StartChildWorkflowExecution` controls behavior:

| Type | Value | Behavior |
|------|-------|----------|
| `ABANDON` | 0 | Don't cancel child when parent cancels |
| `TRY_CANCEL` | 1 | Attempt cancel, don't wait |
| `WAIT_CANCELLATION_COMPLETED` | 2 | Wait for child to acknowledge (default) |
| `WAIT_CANCELLATION_REQUESTED` | 3 | Wait for cancel request to be delivered |

---

## Child Workflow on Same Task Queue

When child uses same task queue as parent, the same worker handles both:

```
INTERLEAVED ACTIVATIONS (Same Task Queue)
==========================================

Timeline:
  t1: poll → activation(run_id=PARENT, jobs=[initialize_workflow(Parent)])
  t2: complete([StartChildWorkflowExecution(seq=1)])

  t3: poll → activation(run_id=PARENT, jobs=[resolve_child_workflow_execution_start])
  t4: complete([])  # ACK

  t5: poll → activation(run_id=CHILD, jobs=[initialize_workflow(Child)])  ← Different run_id!
  t6: complete([CompleteWorkflowExecution(result)])

  t7: poll → activation(run_id=PARENT, jobs=[resolve_child_workflow_execution(completed)])
  t8: complete([CompleteWorkflowExecution])
```

### Key Considerations

1. **Track workflows by run_id**
   - Each activation has a run_id identifying which workflow
   - Don't confuse parent and child activations

2. **Order is NOT guaranteed**
   - Child might complete before start is acknowledged in parent
   - Handle both orders in code

3. **Different task queues**
   - Child can run on different task queue
   - Useful for routing to specialized workers

---

## Child Workflow Arguments

Arguments are encoded as Payloads:

```protobuf
start_child_workflow_execution {
  input: [
    Payload({"key": "value"}),
    Payload([1, 2, 3]),
    Payload("string_arg"),
    Payload(42)
  ]
}
```

Child receives in `initialize_workflow.arguments`.

---

## Test File

See `tests/bridge_patterns/test_bridge_child_workflows.py` for executable examples.

```bash
uv run pytest tests/bridge_patterns/test_bridge_child_workflows.py -v -m temporal_server
```
