# Bridge Integration Patterns

This directory documents SDK-Core bridge interaction patterns discovered through direct testing. Each pattern represents a specific workflow feature and its corresponding activation/completion protocol.

## Core Protocol Model

The SDK-Worker communication follows a strict **poll-complete** state machine:

```
                    ┌─────────────────────────────────────────────────────┐
                    │                 SDK-Core (Rust)                     │
                    │                                                     │
                    │  ┌─────────┐    ┌──────────┐    ┌─────────────┐   │
                    │  │ History │───▶│ Workflow │───▶│  Activation │   │
                    │  │  Store  │    │   Cache  │    │   Builder   │   │
                    │  └─────────┘    └──────────┘    └──────┬──────┘   │
                    │                                        │          │
                    └────────────────────────────────────────┼──────────┘
                                                             │
                                    poll_workflow_activation │
                                                             ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         Python SDK Worker                              │
│                                                                        │
│   ┌──────────┐        ┌────────────────┐        ┌──────────────────┐  │
│   │  IDLE    │──poll─▶│ PROCESSING     │──done─▶│ COMPLETING       │  │
│   │          │◀───────│ (run workflow) │◀───────│ (send commands)  │  │
│   └──────────┘  wait  └────────────────┘  next  └──────────────────┘  │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
                                                             │
                                complete_workflow_activation │
                                                             ▼
                    ┌─────────────────────────────────────────────────────┐
                    │                 SDK-Core (Rust)                     │
                    │                                                     │
                    │  ┌──────────────┐    ┌─────────────┐               │
                    │  │  Completion  │───▶│   Command   │───▶ Server   │
                    │  │   Handler    │    │   Executor  │               │
                    │  └──────────────┘    └─────────────┘               │
                    │                                                     │
                    └─────────────────────────────────────────────────────┘
```

## State Machine: Activation Processing

```
State Machine: Activation Completion
====================================

States:
  [PENDING]     - Activation received, not yet processed
  [PROCESSING]  - Workflow code executing
  [COMPLETING]  - Building completion commands
  [COMPLETE]    - Completion sent to SDK-Core

Transitions:
  PENDING     ──parse_jobs()────▶ PROCESSING
  PROCESSING  ──yield_commands()─▶ COMPLETING
  COMPLETING  ──send()───────────▶ COMPLETE
  COMPLETE    ──next_activation()▶ PENDING

Invariants:
  • Every activation MUST receive exactly one completion
  • Completion must use matching run_id from activation
  • Commands in completion are processed atomically
```

## Sequence Number State Machine

Many operations (timers, activities, child workflows) use sequence numbers for matching:

```
State Machine: Sequenced Operation (Timer/Activity/Child)
=========================================================

States:
  [SCHEDULED] - Command sent with seq=N
  [PENDING]   - Waiting for resolution
  [RESOLVED]  - Resolution job received with seq=N
  [COMPLETE]  - Workflow handled resolution

         schedule(seq=N)              resolve_*(seq=N)
INIT ──────────────────▶ SCHEDULED ─────────────────▶ RESOLVED
                              │                           │
                              │ cancel(seq=N)            │ handle()
                              ▼                           ▼
                         CANCELLED                    COMPLETE

Invariants:
  • seq numbers are workflow-local (reset per workflow)
  • seq must match between schedule and resolve
  • Cancellation may race with resolution
```

## Pattern Index

| Pattern | Name | File | Description |
|---------|------|------|-------------|
| 1-7 | Basic Patterns | [01_basic.md](01_basic.md) | Workflow lifecycle, timers, cancellation, caching |
| 8-10 | Activities | [02_activities.md](02_activities.md) | Activity execution, failure, cancellation |
| 11-13 | Signals & Queries | [03_signals_queries.md](03_signals_queries.md) | External communication with workflows |
| 14-15 | Child Workflows | [04_child_workflows.md](04_child_workflows.md) | Spawning and managing child workflows |
| 16-19 | Advanced | [05_advanced.md](05_advanced.md) | Failure, continue-as-new, external signals, search attributes |

## Activation Job Types

Jobs received from SDK-Core in `WorkflowActivation`:

| Job Type | Description | Response Required |
|----------|-------------|-------------------|
| `initialize_workflow` | Start a new workflow | Yes (commands or complete) |
| `fire_timer` | Timer has fired | Yes (empty or commands) |
| `cancel_workflow` | Cancellation requested | Yes (typically cancel cmd) |
| `signal_workflow` | Signal received | Yes (can be empty) |
| `query_workflow` | Query requested | Yes (RespondToQuery) |
| `resolve_activity` | Activity completed/failed/cancelled | Yes (empty or commands) |
| `resolve_child_workflow_execution_start` | Child workflow started | Yes (empty or commands) |
| `resolve_child_workflow_execution` | Child workflow completed | Yes (empty or commands) |
| `resolve_signal_external_workflow` | External signal resolved | Yes (empty or commands) |
| `remove_from_cache` | Workflow being evicted | No (discard state) |

## Completion Command Types

Commands sent to SDK-Core in `WorkflowActivationCompletion`:

| Command Type | Description | Key Parameters |
|--------------|-------------|----------------|
| `start_timer` | Start a timer | seq, duration |
| `cancel_timer` | Cancel a pending timer | seq |
| `complete_workflow_execution` | Complete workflow successfully | result payload |
| `fail_workflow_execution` | Fail workflow with error | failure info |
| `cancel_workflow_execution` | Mark workflow as cancelled | - |
| `schedule_activity` | Schedule an activity | seq, type, queue, args, timeouts |
| `request_cancel_activity` | Cancel an activity | seq |
| `respond_to_query` | Respond to a query | query_id, result or error |
| `start_child_workflow_execution` | Start a child workflow | seq, id, type, queue, args |
| `cancel_child_workflow_execution` | Cancel a child workflow | child_workflow_seq |
| `signal_external_workflow_execution` | Signal another workflow | seq, target, signal_name, args |
| `continue_as_new_workflow_execution` | Continue as new execution | type, args, queue |
| `upsert_workflow_search_attributes` | Update search attributes | attributes map |

## Critical Protocol Rules

### Rule 1: One Completion Per Activation

```
VALID:
  activation(jobs=[A, B]) → completion(commands=[X, Y, Z])

INVALID:
  activation(jobs=[A]) → completion_1(commands=[X])
                       → completion_2(commands=[Y])  ❌ Only first accepted!
```

### Rule 2: Commands in Same Completion Are Atomic

When you need to respond to a job AND issue new commands, they go in the SAME completion:

```
VALID (Pattern 15 - Cancel Child):
  activation(jobs=[resolve_child_workflow_execution_start])
  → completion(commands=[cancel_child_workflow_execution])  ✓

INVALID:
  activation(jobs=[resolve_child_workflow_execution_start])
  → completion(commands=[])  # ACK the activation
  → completion(commands=[cancel_child_workflow_execution])  ❌ No pending activation!
```

### Rule 3: Queries Are Special

Queries always arrive in a **separate** activation and **require** a `respond_to_query` command:

```
activation(jobs=[query_workflow(id="q1", type="get_status")])
→ completion(commands=[respond_to_query(query_id="q1", result=...)])

NEVER:
  activation(jobs=[query_workflow, fire_timer])  ❌ Queries are isolated
```

### Rule 4: Signals Don't Require Response Commands

Signals are fire-and-forget; empty completion is valid:

```
activation(jobs=[signal_workflow(name="my_signal")])
→ completion(commands=[])  ✓ Valid - signal processed, no response needed
```

## Running Tests

Tests require a running Temporal server:

```bash
# Start Temporal server
temporal server start-dev &

# Build bridge (if modified)
cd temporalio_trio_bridge && maturin develop --release && cd ..

# Run all bridge pattern tests
uv run pytest tests/bridge_patterns/ -v -m temporal_server

# Run specific pattern
uv run pytest tests/bridge_patterns/test_bridge_activities.py -v -m temporal_server
```

## Key Learnings Summary

### Sequence Numbers
- Activities, timers, and child workflows use `seq` numbers for matching
- SDK must track seq-to-promise mappings
- Sequence numbers are workflow-local (start at 1 for each workflow)

### Two-Phase Resolution (Child Workflows)
- Child workflows have two-phase resolution:
  1. `resolve_child_workflow_execution_start` - Child accepted/rejected
  2. `resolve_child_workflow_execution` - Child completed/failed/cancelled
- **Critical**: Commands for child (like cancel) must be in the completion responding to the start confirmation

### Queries are Special
- Queries always arrive in a separate activation
- Queries must be responded to with matching `query_id`
- Query responses don't affect workflow state

### Signals Don't Require Response
- Signals are fire-and-forget from the workflow's perspective
- No completion command needed for signal jobs
- Multiple signals can arrive in one activation

### Cache Eviction
- `remove_from_cache` job means workflow will be replayed
- `max_cached_workflows` config controls cache size
- Evicted workflows must reconstruct state from history

## References

- [Temporal SDK Core](https://github.com/temporalio/sdk-core)
- [Workflow Activation Proto](https://github.com/temporalio/sdk-core/blob/main/protos/local/temporal/sdk/core/workflow_activation/workflow_activation.proto)
- [Workflow Completion Proto](https://github.com/temporalio/sdk-core/blob/main/protos/local/temporal/sdk/core/workflow_completion/workflow_completion.proto)
