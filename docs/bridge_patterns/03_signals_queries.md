# Signal and Query Patterns (11-13)

These patterns cover external communication with running workflows.

## State Machine: Signals vs Queries

```
SIGNALS - Fire and Forget
=========================

External                     Workflow
   │                            │
   │  send_signal(name, args)   │
   │───────────────────────────▶│
   │                            │ (process signal)
   │      (no response)         │
   │                            │


QUERIES - Request/Response
==========================

External                     Workflow
   │                            │
   │  query(type, args)         │
   │───────────────────────────▶│
   │                            │ (execute query handler)
   │       result/error         │
   │◀───────────────────────────│
   │                            │


Key Differences:
  • Signals: Can modify workflow state, no response needed
  • Queries: Read-only, MUST respond with RespondToQuery
  • Signals: Can be batched with other jobs
  • Queries: Always arrive in SEPARATE activation (isolation)
```

## Pattern 11: Signal Workflow

**Scenario**: Receive signal from external source.

### Sequence Diagram

```
┌──────────┐       ┌──────────┐       ┌─────────┐       ┌──────────┐
│ External │       │  Server  │       │SDK-Core │       │ Workflow │
│  Client  │       │          │       │         │       │          │
└────┬─────┘       └────┬─────┘       └────┬────┘       └────┬─────┘
     │                  │                  │                  │
     │                  │                  │◀───(workflow     │
     │                  │                  │     running)─────│
     │                  │                  │                  │
     │ SignalWorkflow   │                  │                  │
     │ (name="my_signal"│                  │                  │
     │  input=[args])   │                  │                  │
     │─────────────────▶│                  │                  │
     │                  │                  │                  │
     │                  │ signal_workflow  │                  │
     │                  │─────────────────▶│                  │
     │                  │                  │                  │
     │                  │                  │ signal_workflow  │
     │                  │                  │ (name, input)    │
     │                  │                  │─────────────────▶│
     │                  │                  │                  │
     │                  │                  │  completion([])  │
     │                  │                  │◀─────────────────│
     │                  │                  │                  │
     │  (no response)   │                  │                  │
     │                  │                  │                  │
```

### Key Fields

**signal_workflow job**:
```protobuf
signal_workflow {
  signal_name: "my_signal"        # Name of the signal handler
  input: [Payload, ...]           # Signal arguments (0 or more)
  identity: "temporal-cli"        # Who sent the signal
  headers: {}                     # Optional metadata
}
```

### Signal Handling Rules

```
SIGNAL HANDLING
===============

Activation: [signal_workflow(name="update_status", input=[payload])]

Option 1: Process signal, no new commands
  complete([])  ✓

Option 2: Process signal, trigger new workflow behavior
  complete([StartTimer(seq=1, 5s)])  ✓

Option 3: Multiple signals in one activation
  Activation: [signal_workflow("a"), signal_workflow("b"), fire_timer(1)]
  complete([...])  # Handle all together


INVALID: Signals don't need RespondToQuery
  complete([RespondToQuery(...)])  ❌ No query_id to respond to!
```

### Learnings

1. **Signals don't require a response command**
   - Unlike queries, signals are fire-and-forget from workflow perspective
   - Empty completion `complete([])` is valid after receiving signal

2. **Signals can arrive with other jobs**
   - Multiple signals can be in same activation
   - Signals can arrive alongside `fire_timer` or other jobs

3. **Signal order is preserved**
   - Signals arrive in the order they were sent
   - History preserves signal ordering

4. **Signal arguments**
   - Encoded as Payloads using DataConverter
   - Can have 0 to N arguments

### Multiple Signals Example

```
BATCHED SIGNALS
===============

Timeline:
  t1: Client sends signal "a"
  t2: Client sends signal "b"
  t3: Timer fires

Activation at t3:
  jobs: [signal_workflow("a"), signal_workflow("b"), fire_timer(seq=1)]

Processing order:
  1. signal "a" handler
  2. signal "b" handler
  3. timer handler

Completion:
  commands: [StartTimer(seq=2), ...]  # Any new commands
```

---

## Pattern 12: Query Workflow

**Scenario**: Query workflow state synchronously.

### Sequence Diagram

```
┌──────────┐       ┌──────────┐       ┌─────────┐       ┌──────────┐
│ External │       │  Server  │       │SDK-Core │       │ Workflow │
│  Client  │       │          │       │         │       │          │
└────┬─────┘       └────┬─────┘       └────┬────┘       └────┬─────┘
     │                  │                  │                  │
     │ QueryWorkflow    │                  │                  │
     │ (type="status")  │                  │                  │
     │─────────────────▶│                  │                  │
     │                  │                  │                  │
     │  (client waits)  │ query_workflow   │                  │
     │                  │─────────────────▶│                  │
     │                  │                  │                  │
     │                  │                  │ query_workflow   │
     │                  │                  │ (query_id="q1",  │
     │                  │                  │  type="status")  │
     │                  │                  │─────────────────▶│
     │                  │                  │                  │
     │                  │                  │ RespondToQuery   │
     │                  │                  │ (query_id="q1",  │
     │                  │                  │  result=payload) │
     │                  │                  │◀─────────────────│
     │                  │                  │                  │
     │                  │  query_result    │                  │
     │                  │◀─────────────────│                  │
     │                  │                  │                  │
     │  result/error    │                  │                  │
     │◀─────────────────│                  │                  │
     │                  │                  │                  │
```

### Key Fields

**query_workflow job**:
```protobuf
query_workflow {
  query_id: "abc123-unique-id"    # Unique ID for this query request
  query_type: "get_status"        # Query handler name
  arguments: [Payload, ...]       # Query arguments
  headers: {}                     # Optional metadata
}
```

**RespondToQuery command (success)**:
```protobuf
respond_to_query {
  query_id: "abc123-unique-id"    # MUST match query_workflow.query_id
  succeeded {
    response: Payload             # Query result
  }
}
```

### CRITICAL: Query Isolation

```
QUERY ISOLATION RULE
====================

Queries ALWAYS arrive in a SEPARATE activation.

VALID activation:
  jobs: [query_workflow(id="q1", type="status")]
  ↳ This activation contains ONLY the query

NEVER HAPPENS:
  jobs: [query_workflow(...), fire_timer(...)]  ❌
  jobs: [query_workflow(...), signal_workflow(...)]  ❌
  jobs: [query_workflow("a"), query_workflow("b")]  ❌ (separate activations)


Why isolation?
  • Queries see consistent workflow state
  • No side effects from other jobs
  • Query handlers don't affect workflow history
```

### Query Response Requirements

```
QUERY RESPONSE - REQUIRED
=========================

Activation: [query_workflow(query_id="q1", query_type="get_status")]

MUST respond with matching query_id:
  complete([RespondToQuery(query_id="q1", succeeded={response: payload})])  ✓

INVALID responses:
  complete([])  ❌ Query left unanswered - client times out
  complete([RespondToQuery(query_id="q2", ...)])  ❌ Wrong query_id
  complete([CompleteWorkflowExecution(...)])  ❌ Doesn't answer query
```

### Important Rules

1. **Queries always arrive in separate activation**
   - Queries are never batched with signals or timers
   - This ensures consistent view of workflow state

2. **Query response must include matching query_id**
   - `respond_to_query.query_id` must equal `query_workflow.query_id`
   - Mismatched IDs result in error

3. **Queries don't affect workflow state**
   - Query handlers should be read-only
   - Commands from query response don't persist (except `respond_to_query`)

4. **Query timeout**
   - Caller specifies timeout
   - If workflow doesn't respond in time, query fails

### Query with Arguments

```protobuf
# Query: get_items(category="electronics", limit=10)
query_workflow {
  query_type: "get_items"
  arguments: [
    Payload("electronics"),
    Payload(10)
  ]
}
```

---

## Pattern 13: Query Failure

**Scenario**: Query handler returns error.

### Sequence Diagram

```
┌──────────┐       ┌──────────┐       ┌─────────┐       ┌──────────┐
│ External │       │  Server  │       │SDK-Core │       │ Workflow │
│  Client  │       │          │       │         │       │          │
└────┬─────┘       └────┬─────┘       └────┬────┘       └────┬─────┘
     │                  │                  │                  │
     │ QueryWorkflow    │                  │                  │
     │ (type="unknown") │                  │                  │
     │─────────────────▶│                  │                  │
     │                  │                  │                  │
     │                  │ query_workflow   │                  │
     │                  │─────────────────▶│                  │
     │                  │                  │                  │
     │                  │                  │ query_workflow   │
     │                  │                  │ (id="q1",        │
     │                  │                  │  type="unknown") │
     │                  │                  │─────────────────▶│
     │                  │                  │                  │
     │                  │                  │ RespondToQuery   │
     │                  │                  │ (id="q1",        │
     │                  │                  │  failed=error)   │
     │                  │                  │◀─────────────────│
     │                  │                  │                  │
     │                  │   query_failed   │                  │
     │                  │◀─────────────────│                  │
     │                  │                  │                  │
     │  error response  │                  │                  │
     │◀─────────────────│                  │                  │
     │                  │                  │                  │
```

### Key Fields

**RespondToQuery command (failure)**:
```protobuf
respond_to_query {
  query_id: "abc123-unique-id"
  failed {
    message: "Query handler not found: unknown_query"
    source: "PythonSDK"
    application_failure_info {
      type: "QueryNotFoundError"
    }
  }
}
```

### Common Query Failure Scenarios

| Scenario | Error Message |
|----------|---------------|
| Handler not found | "Query handler not found: {query_type}" |
| Handler raised exception | "{exception_type}: {message}" |
| Invalid arguments | "Invalid arguments for query: {details}" |
| Workflow not running | (Not applicable - query fails before reaching workflow) |

### Learnings

1. **Query failures are returned to caller**
   - CLI/client receives the error message
   - Workflow continues running normally

2. **Failed queries don't affect workflow state**
   - No side effects from failed query
   - Can retry query immediately

3. **Query failure vs workflow failure**
   - Query failure: Temporary, query-specific error
   - Workflow failure: Permanent, workflow is done

---

## Signals vs Queries Comparison

```
COMPARISON TABLE
================

                    Signal              Query
                    ──────              ─────
Direction           Fire-and-forget     Request-response
Response needed     No                  Yes (RespondToQuery)
Can modify state    Yes                 No (read-only)
Can be batched      Yes                 No (isolated)
Blocking caller     No                  Yes (waits for response)
Use case           "Do something"      "What is your state?"


When to use signals:
  • Triggering workflow actions
  • Updating workflow state
  • Async communication

When to use queries:
  • Getting workflow status
  • Reading workflow data
  • Debugging/monitoring
```

| Aspect | Signal | Query |
|--------|--------|-------|
| Direction | Fire-and-forget | Request-response |
| Response required | No | Yes (RespondToQuery) |
| Can modify state | Yes | No (read-only) |
| Can be batched | Yes (with other jobs) | No (separate activation) |
| Blocking | No | Yes (caller waits) |
| Retry | N/A | Client can retry |

---

## Async Query Handling

When implementing query handling from an async context (like Trio), the CLI call blocks. Use thread-based async wrappers:

```python
async def query_workflow_via_cli_async(
    workflow_id: str,
    query_type: str,
    args: list | None = None,
) -> dict:
    """Non-blocking query via CLI."""
    return await trio.to_thread.run_sync(
        lambda: query_workflow_via_cli(workflow_id, query_type, args)
    )
```

This prevents the async event loop from blocking while waiting for the CLI response.

---

## Test File

See `tests/bridge_patterns/test_bridge_signals_queries.py` for executable examples.

```bash
uv run pytest tests/bridge_patterns/test_bridge_signals_queries.py -v -m temporal_server
```
