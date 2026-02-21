# Advanced Patterns (16-19)

These patterns cover advanced workflow features: failure, continue-as-new, external signals, and search attributes.

## State Machine: Workflow Termination

```
WORKFLOW TERMINATION STATES
===========================

         ┌────────────────────────────────────────────────┐
         │                                                │
         │   RUNNING                                      │
         │     │                                          │
         │     ├──CompleteWorkflowExecution──▶ COMPLETED  │
         │     │                                          │
         │     ├──FailWorkflowExecution─────▶ FAILED     │
         │     │                                          │
         │     ├──CancelWorkflowExecution───▶ CANCELLED  │
         │     │                                          │
         │     └──ContinueAsNew─────────────▶ CONTINUED  │
         │                                    AS_NEW      │
         │                                       │        │
         │                                       ▼        │
         │                                   NEW RUN      │
         │                                   (RUNNING)    │
         └────────────────────────────────────────────────┘

Terminal states: COMPLETED, FAILED, CANCELLED
Non-terminal: CONTINUED_AS_NEW (new run starts)
```

## Pattern 16: Workflow Failure

**Scenario**: Workflow fails with exception.

### Sequence Diagram

```
┌──────────┐       ┌──────────┐       ┌─────────┐
│ Workflow │       │ SDK-Core │       │ Server  │
│          │       │          │       │         │
└────┬─────┘       └────┬─────┘       └────┬────┘
     │                  │                  │
     │◀──initialize_workflow               │
     │                  │                  │
     │   (error occurs) │                  │
     │                  │                  │
     │ FailWorkflow     │                  │
     │ Execution        │                  │
     │ (failure={...})  │                  │
     │─────────────────▶│                  │
     │                  │                  │
     │                  │ CompleteTask     │
     │                  │ (status=FAILED)  │
     │                  │─────────────────▶│
     │                  │                  │
     │                  │                  │
     │    (workflow terminates)            │
     │                  │                  │
```

### Key Fields

**FailWorkflowExecution command**:
```protobuf
fail_workflow_execution {
  failure {
    message: "Workflow failed due to business logic error"
    source: "PythonSDK"
    stack_trace: "Traceback (most recent call last):\n  File..."

    # Application failure details
    application_failure_info {
      type: "ValueError"              # Exception class name
      non_retryable: true             # If true, no retry on workflow level
      details {
        payloads: [Payload, ...]      # Structured error details
      }
    }
  }
}
```

### Failure Structure

```
FAILURE PROTOBUF STRUCTURE
==========================

message Failure {
  string message = 1;           # Human-readable error
  string source = 2;            # SDK or source identifier
  string stack_trace = 3;       # Call stack

  oneof failure_info {
    ApplicationFailureInfo application_failure_info = 4;
    TimeoutFailureInfo timeout_failure_info = 5;
    CanceledFailureInfo canceled_failure_info = 6;
    TerminatedFailureInfo terminated_failure_info = 7;
    ServerFailureInfo server_failure_info = 8;
    ActivityFailureInfo activity_failure_info = 9;
    ChildWorkflowExecutionFailureInfo child_workflow_execution_failure_info = 10;
  }

  Failure cause = 11;           # Nested cause (for chained exceptions)
}
```

### Failure with Details

```python
# Adding structured error details
cmd.fail_workflow_execution.failure.message = "Validation failed"
cmd.fail_workflow_execution.failure.application_failure_info.type = "ValidationError"

# Details is a repeated field - use payloads.append()
details_payload = common_pb2.Payload()
details_payload.data = json.dumps({"field": "email", "error": "invalid format"}).encode()
cmd.fail_workflow_execution.failure.application_failure_info.details.payloads.append(
    details_payload
)
```

### Learnings

1. **Failure types**
   - `application_failure_info`: Exception from workflow code
   - `activity_failure_info`: Wraps activity failure
   - `child_workflow_execution_failure_info`: Wraps child failure

2. **Structured details**
   - Use `details.payloads` field for machine-readable error info
   - Encoded as Payloads (any JSON-serializable data)
   - Note: `details` is a message with repeated `payloads` field

3. **Non-retryable flag**
   - `non_retryable: true` prevents workflow retry
   - Use for permanent failures (invalid input, etc.)

---

## Pattern 17: Continue-As-New

**Scenario**: Workflow continues as new execution.

### Sequence Diagram

```
┌──────────┐       ┌──────────┐       ┌─────────┐
│ Workflow │       │ SDK-Core │       │ Server  │
│ (run_1)  │       │          │       │         │
└────┬─────┘       └────┬─────┘       └────┬────┘
     │                  │                  │
     │◀──initialize_workflow               │
     │                  │                  │
     │ ContinueAsNew    │                  │
     │ (type, args)     │                  │
     │─────────────────▶│                  │
     │                  │                  │
     │                  │ CompleteTask     │
     │                  │ (CONTINUED_AS_NEW)
     │                  │─────────────────▶│
     │                  │                  │
     │    (run_1 ends)  │                  │
     │                  │                  │
                        │                  │
┌──────────┐            │                  │
│ Workflow │            │   create new run │
│ (run_2)  │            │◀─────────────────│
└────┬─────┘            │                  │
     │                  │                  │
     │◀──initialize_workflow               │
     │   (with new args)│                  │
     │                  │                  │
```

### CRITICAL: Cache Eviction Handling

When using continue-as-new with small `max_cached_workflows`, you may receive a `remove_from_cache` job before the new run's `initialize_workflow`. Handle this properly:

```
CONTINUE-AS-NEW WITH CACHE EVICTION
===================================

Timeline:
  t1: Run 1 sends ContinueAsNewWorkflowExecution
  t2: poll → [remove_from_cache]  ← Eviction of run_1!
      Handle: Discard old workflow state
  t3: poll → [initialize_workflow]  ← New run (run_2)
      Handle: Create fresh workflow


CORRECT handling:
  if jobs contain remove_from_cache:
      discard_workflow_state(run_id)
      # Don't send completion for eviction!

  if jobs contain initialize_workflow:
      create_workflow(run_id, ...)
      completion([...])
```

### Key Fields

**ContinueAsNewWorkflowExecution command**:
```protobuf
continue_as_new_workflow_execution {
  workflow_type: "SameOrDifferentWorkflow"  # Can change type!
  task_queue: "my-queue"                     # Can change queue!
  arguments: [Payload, ...]                  # New arguments

  # Optional timeouts (inherit if not set)
  workflow_run_timeout { seconds: 300 }
  workflow_task_timeout { seconds: 10 }

  # Optional retry policy
  retry_policy { ... }

  # Optional memo and search attributes
  memo: { ... }
  search_attributes: { ... }
}
```

### Use Cases

1. **Avoiding history growth**
   - Long-running workflows accumulate history
   - Continue-as-new starts fresh with reset history

2. **Iterative workflows**
   - Processing batches: continue-as-new after each batch
   - Cron-style: continue-as-new after each run

3. **Changing workflow type**
   - Migration: start as TypeA, continue as TypeB
   - State machine: different types for different phases

### Learnings

1. **Workflow ID preserved**
   - Same workflow ID, new run ID
   - External handles continue to work

2. **State not preserved**
   - All local variables are lost
   - Pass state through `arguments`

3. **Pending operations**
   - Pending timers, activities, children are NOT carried over
   - Must be re-scheduled in new execution

4. **Original workflow status**
   - Status becomes `CONTINUED_AS_NEW`
   - Not `COMPLETED` or `FAILED`

---

## Pattern 18: Signal External Workflow

**Scenario**: Send signal to another workflow.

### Sequence Diagram

```
┌──────────┐       ┌──────────┐       ┌─────────┐       ┌──────────┐
│ Signaling│       │ SDK-Core │       │ Server  │       │  Target  │
│ Workflow │       │          │       │         │       │ Workflow │
└────┬─────┘       └────┬─────┘       └────┬────┘       └────┬─────┘
     │                  │                  │                  │
     │ SignalExternal   │                  │                  │
     │ Workflow(seq=1,  │                  │                  │
     │  target_id,      │                  │                  │
     │  signal_name)    │                  │                  │
     │─────────────────▶│                  │                  │
     │                  │                  │                  │
     │                  │ SignalWorkflow   │                  │
     │                  │ (target_id)      │                  │
     │                  │─────────────────▶│                  │
     │                  │                  │                  │
     │                  │                  │ signal_workflow  │
     │                  │                  │─────────────────▶│
     │                  │                  │                  │
     │                  │   Signal         │                  │
     │                  │   Delivered      │                  │
     │                  │◀─────────────────│                  │
     │                  │                  │                  │
     │ resolve_signal_  │                  │                  │
     │ external_workflow│                  │                  │
     │ (seq=1, success) │                  │                  │
     │◀─────────────────│                  │                  │
     │                  │                  │                  │
```

### Key Fields

**SignalExternalWorkflowExecution command**:
```protobuf
signal_external_workflow_execution {
  seq: 1
  workflow_execution {
    workflow_id: "target-workflow-id"
    run_id: ""                          # Optional: empty = current run
  }
  signal_name: "my_signal"
  args: [Payload, ...]                  # Signal arguments
  headers: {}                           # Optional metadata
}
```

**resolve_signal_external_workflow job**:
```protobuf
resolve_signal_external_workflow {
  seq: 1
  # Success is indicated by ABSENCE of failure field!
  # Check: if failure.ByteSize() > 0 then failed, else succeeded
}
```

### CRITICAL: Success Detection

```
SIGNAL EXTERNAL - SUCCESS/FAILURE DETECTION
============================================

The resolve_signal_external_workflow job does NOT have a "status" oneof!
Instead, success is indicated by the ABSENCE of a failure:

Python code:
    resolve_job = activation.get_job("resolve_signal_external_workflow")

    # Check if failure is present
    if resolve_job.failure.ByteSize() > 0:
        # FAILED
        print(f"Signal failed: {resolve_job.failure.message}")
    else:
        # SUCCESS (no failure = success)
        print("Signal sent successfully")


INCORRECT (will error):
    status = resolve_job.WhichOneof("status")  ❌ No status oneof!
```

### Learnings

1. **Cross-workflow communication**
   - Send signals between workflows
   - Useful for coordination patterns

2. **Target must exist**
   - Signal fails if target workflow not found
   - Can check status before signaling

3. **Run ID is optional**
   - If omitted, signals most recent run
   - If provided, signals specific run only

4. **Resolution confirmation**
   - `resolve_signal_external_workflow` confirms delivery
   - Doesn't confirm signal was handled

---

## Pattern 19: Search Attributes

**Scenario**: Update workflow search attributes.

### Sequence Diagram

```
┌──────────┐       ┌──────────┐       ┌─────────┐
│ Workflow │       │ SDK-Core │       │ Server  │
│          │       │          │       │         │
└────┬─────┘       └────┬─────┘       └────┬────┘
     │                  │                  │
     │◀──initialize_workflow               │
     │                  │                  │
     │ UpsertSearch     │                  │
     │ Attributes       │                  │
     │ ({Status:"active"│                  │
     │   Priority: 5})  │                  │
     │─────────────────▶│                  │
     │                  │                  │
     │                  │ UpdateSearch     │
     │                  │ Attributes       │
     │                  │─────────────────▶│
     │                  │                  │
     │                  │   (indexed)      │
     │                  │                  │
     │ CompleteWorkflow │                  │
     │─────────────────▶│                  │
     │                  │                  │
```

### CRITICAL: Cache Eviction and Replay

Search attribute tests with small `max_cached_workflows` may trigger eviction and replay. The workflow must handle re-processing on replay:

```
SEARCH ATTRIBUTES WITH REPLAY
=============================

Initial run:
  t1: poll → [initialize_workflow]
  t2: complete([UpsertWorkflowSearchAttributes, StartTimer(seq=1)])

After eviction (replay):
  t3: poll → [remove_from_cache]  ← Eviction!
      Handle: Discard state
  t4: poll → [initialize_workflow, fire_timer(seq=1)]  ← Replayed!
      Handle: UpsertSearchAttributes already recorded in history
              Complete with just CompleteWorkflowExecution


Key insight: On replay, commands that were already recorded don't need
to be sent again. But new commands (CompleteWorkflowExecution) do.
```

### Key Fields

**UpsertWorkflowSearchAttributes command**:
```protobuf
upsert_workflow_search_attributes {
  search_attributes: {
    indexed_fields: {
      "CustomKeywordField": Payload("my-value")
      "CustomIntField": Payload(42)
      "CustomBoolField": Payload(true)
      "CustomDatetimeField": Payload("2024-01-15T10:30:00Z")
    }
  }
}
```

### Search Attribute Types

| Type | Payload Encoding | Example |
|------|------------------|---------|
| Keyword | String | `"status-active"` |
| Text | String | `"long description text"` |
| Int | Number | `42` |
| Double | Number | `3.14` |
| Bool | Boolean | `true` |
| Datetime | ISO8601 String | `"2024-01-15T10:30:00Z"` |
| KeywordList | String Array | `["tag1", "tag2"]` |

### Important Notes

1. **Custom attributes must be registered**
   - Server must have search attribute defined
   - Use `temporal operator search-attribute create` to register

2. **Standard search attributes**
   - Some are built-in: `WorkflowType`, `WorkflowId`, `StartTime`, etc.
   - These are set automatically

3. **Upsert semantics**
   - Existing attributes are updated
   - New attributes are added
   - Missing attributes are unchanged (not deleted)

4. **Visibility lag**
   - Search results may have slight delay
   - Eventual consistency with workflow state

### Learnings

1. **Type metadata**
   - Include `type` in payload metadata for proper indexing
   - Example: `payload.metadata["type"] = b"Keyword"`

2. **Query use cases**
   - Find all workflows with status="pending"
   - Filter by custom business fields
   - Analytics and reporting

---

## Worker Configuration: max_cached_workflows

During testing, we discovered that `max_cached_workflows` must be properly passed to SDK-Core:

```rust
// In Rust bridge (core_worker.rs)
let worker_config = WorkerConfig::builder()
    .namespace(config.namespace.clone())
    .task_queue(config.task_queue.clone())
    .max_cached_workflows(config.max_cached_workflows)  // CRITICAL!
    .versioning_strategy(...)
    .build()
```

Without this, SDK-Core uses default caching behavior, causing unexpected `remove_from_cache` jobs instead of `resolve_activity` jobs when testing with small cache sizes.

---

## Test File

See `tests/bridge_patterns/test_bridge_advanced.py` for executable examples.

```bash
uv run pytest tests/bridge_patterns/test_bridge_advanced.py -v -m temporal_server
```
