# Activity Patterns (8-10)

These patterns cover activity execution, failure handling, and cancellation.

## State Machine: Activity Lifecycle

```
State Machine: Activity
=======================

Workflow Side States:
  [INIT]        - No activity scheduled
  [SCHEDULED]   - ScheduleActivity(seq=N) sent
  [PENDING]     - Waiting for resolution
  [RESOLVED]    - resolve_activity(seq=N) received

Activity Worker States:
  [IDLE]        - Waiting for work
  [EXECUTING]   - Activity code running
  [HEARTBEATING]- Sending heartbeat
  [COMPLETING]  - Sending result

Workflow Transitions:
  INIT ──ScheduleActivity(seq=N)──▶ SCHEDULED
  SCHEDULED ──(internal)──▶ PENDING
  PENDING ──resolve_activity(completed)──▶ RESOLVED
  PENDING ──resolve_activity(failed)──▶ RESOLVED
  PENDING ──resolve_activity(cancelled)──▶ RESOLVED

Cancel Flow:
  PENDING ──RequestCancelActivity(seq=N)──▶ CANCEL_REQUESTED
  CANCEL_REQUESTED ──resolve_activity(cancelled)──▶ RESOLVED
  CANCEL_REQUESTED ──resolve_activity(completed)──▶ RESOLVED  # Activity may ignore cancel
```

## Pattern 8: Activity Execution (Success)

**Scenario**: Schedule activity → receive successful result.

### Sequence Diagram

```
┌──────────┐       ┌──────────┐       ┌─────────┐       ┌──────────┐
│ Workflow │       │ SDK-Core │       │ Server  │       │ Activity │
│          │       │          │       │         │       │  Worker  │
└────┬─────┘       └────┬─────┘       └────┬────┘       └────┬─────┘
     │                  │                  │                  │
     │ ScheduleActivity │                  │                  │
     │ (seq=1, type,    │                  │                  │
     │  queue, args)    │                  │                  │
     │─────────────────▶│                  │                  │
     │                  │ ScheduleActivity │                  │
     │                  │─────────────────▶│                  │
     │                  │                  │                  │
     │                  │                  │ poll_activity    │
     │                  │                  │◀─────────────────│
     │                  │                  │                  │
     │                  │                  │ ActivityTask     │
     │                  │                  │ (task_token,     │
     │                  │                  │  type, input)    │
     │                  │                  │─────────────────▶│
     │                  │                  │                  │
     │                  │                  │      (execute)   │
     │                  │                  │                  │
     │                  │                  │ CompleteActivity │
     │                  │                  │ (task_token,     │
     │                  │                  │  result)         │
     │                  │                  │◀─────────────────│
     │                  │                  │                  │
     │                  │ ActivityCompleted│                  │
     │                  │◀─────────────────│                  │
     │                  │                  │                  │
     │ resolve_activity │                  │                  │
     │ (seq=1,          │                  │                  │
     │  completed)      │                  │                  │
     │◀─────────────────│                  │                  │
     │                  │                  │                  │
```

### Key Fields

**ScheduleActivity command**:
```protobuf
schedule_activity {
  seq: 1                          # Sequence number for matching
  activity_id: "1"                # User-visible ID (can default to seq)
  activity_type: "TestActivity"   # Activity function name
  task_queue: "my-queue"          # Where to run (required)
  arguments: [Payload, ...]       # Encoded arguments

  # At least one timeout required:
  schedule_to_close_timeout { seconds: 60 }  # Max total time
  schedule_to_start_timeout { seconds: 30 }  # Max wait for pickup
  start_to_close_timeout { seconds: 30 }     # Max execution time
  heartbeat_timeout { seconds: 10 }          # Max between heartbeats

  # Optional retry policy:
  retry_policy {
    initial_interval { seconds: 1 }
    backoff_coefficient: 2.0
    maximum_interval { seconds: 60 }
    maximum_attempts: 3
    non_retryable_error_types: ["InvalidInputError"]
  }
}
```

**resolve_activity job (success)**:
```protobuf
resolve_activity {
  seq: 1
  result {
    completed {
      result: Payload  # Activity return value
    }
  }
}
```

### Activity Task Flow (Worker Side)

```
Activity Worker State Machine
=============================

       poll_activity_task()         execute(input)          complete_activity()
IDLE ─────────────────────▶ GOT_TASK ──────────────▶ DONE ────────────────────▶ IDLE
                               │                      │
                               │ heartbeat()          │ fail_activity()
                               │◀────────────────────▶│
                               │      (periodic)      │
```

```python
# Poll for activity task
activity_task_bytes = await bridge.poll_activity_task()
activity_task = ActivityTask.ParseFromString(activity_task_bytes)

# Task contains:
# - task_token: Opaque token for completion
# - start.activity_type: Function name
# - start.input: List of argument Payloads
# - start.attempt: Current retry attempt (1-based)
# - start.heartbeat_timeout: Duration for heartbeats

# Complete activity
completion = ActivityTaskCompletion()
completion.task_token = activity_task.task_token
completion.result.completed.result.CopyFrom(result_payload)
await bridge.complete_activity_task(completion.SerializeToString())
```

### Timeout Relationships

```
TIMEOUT TIMELINE
================

schedule ──────────────────────────────────────────────────▶ close
    │                                                          │
    │     start ─────────────────────────────────▶ close       │
    │       │                                        │         │
    │       │  heartbeat   heartbeat   heartbeat     │         │
    │       │◀───────────▶│◀─────────▶│◀────────────▶│         │
    │       │             │           │              │         │
    ├───────┼─────────────┴───────────┴──────────────┼─────────┤
    │       │                                        │         │
    └───────┴────────────────────────────────────────┴─────────┘

    schedule_to_start_timeout: time from schedule to pickup
    start_to_close_timeout: time from pickup to completion
    schedule_to_close_timeout: total time allowed
    heartbeat_timeout: max time between heartbeats
```

### Learnings
- `schedule_to_close_timeout` OR `start_to_close_timeout` is required
- Activity ID defaults to seq if not specified
- Result is decoded using DataConverter
- Activity worker can be same or different from workflow worker

---

## Pattern 9: Activity Failure

**Scenario**: Activity fails → receive failure info.

### Sequence Diagram

```
┌──────────┐       ┌──────────┐       ┌─────────┐       ┌──────────┐
│ Workflow │       │ SDK-Core │       │ Server  │       │ Activity │
│          │       │          │       │         │       │  Worker  │
└────┬─────┘       └────┬─────┘       └────┬────┘       └────┬─────┘
     │                  │                  │                  │
     │ ScheduleActivity │                  │                  │
     │─────────────────▶│                  │                  │
     │                  │─────────────────▶│                  │
     │                  │                  │                  │
     │                  │                  │ ActivityTask     │
     │                  │                  │─────────────────▶│
     │                  │                  │                  │
     │                  │                  │    (throws       │
     │                  │                  │     exception)   │
     │                  │                  │                  │
     │                  │                  │ FailActivity     │
     │                  │                  │ (task_token,     │
     │                  │                  │  failure)        │
     │                  │                  │◀─────────────────│
     │                  │                  │                  │
     │                  │   (retry logic)  │                  │
     │                  │   if retryable   │                  │
     │                  │◀────────────────▶│                  │
     │                  │                  │                  │
     │ resolve_activity │                  │                  │
     │ (seq=1, failed)  │                  │                  │
     │◀─────────────────│                  │                  │
     │                  │                  │                  │
```

### Key Fields

**resolve_activity job (failure)**:
```protobuf
resolve_activity {
  seq: 1
  result {
    failed {
      failure {
        message: "Activity failed: invalid input"
        source: "PythonSDK"
        stack_trace: "..."
        cause {                        # Nested - original exception
          message: "ValueError: bad input"
          application_failure_info {
            type: "ValueError"
            non_retryable: true
            details {
              payloads: [Payload, ...]  # Optional structured details
            }
          }
        }
      }
    }
  }
}
```

### Failure Wrapping

Activity failures are wrapped in an `ActivityFailureInfo`:

```
Failure Structure (Workflow receives):
=====================================

failure {
  message: "activity error"
  activity_failure_info {
    activity_type: "TestActivity"
    activity_id: "1"
    identity: "worker-123"
    ...
  }
  cause {                          # ← Original exception here
    message: "ValueError: bad input"
    application_failure_info {
      type: "ValueError"
    }
  }
}
```

### Failure Types

| Failure Type | Description | Retryable by Default |
|--------------|-------------|---------------------|
| `application_failure_info` | Exception from activity code | Yes (unless non_retryable=true) |
| `timeout_failure_info` | Activity timed out | Yes |
| `canceled_failure_info` | Activity was cancelled | No |
| `server_failure_info` | Server-side error | Depends |

### Learnings
- Failure contains message, source, and stack trace
- `application_failure_info.type` is the exception class name
- `non_retryable=true` prevents automatic retry
- Actual error is in `failure.cause`, wrapped by activity failure info

---

## Pattern 10: Activity Cancellation

**Scenario**: Cancel activity while running.

### Sequence Diagram

```
┌──────────┐       ┌──────────┐       ┌─────────┐       ┌──────────┐
│ Workflow │       │ SDK-Core │       │ Server  │       │ Activity │
│          │       │          │       │         │       │  Worker  │
└────┬─────┘       └────┬─────┘       └────┬────┘       └────┬─────┘
     │                  │                  │                  │
     │ ScheduleActivity │                  │                  │
     │ (timeout=60s)    │                  │                  │
     │─────────────────▶│                  │                  │
     │                  │─────────────────▶│                  │
     │                  │                  │                  │
     │                  │                  │ ActivityTask     │
     │                  │                  │─────────────────▶│
     │                  │                  │                  │
     │                  │                  │    (running)     │
     │                  │                  │                  │
     ├──────────────────┼──────────────────┼──────────────────┤
     │ Need activation  │                  │                  │
     │ to send cancel!  │                  │                  │
     ├──────────────────┼──────────────────┼──────────────────┤
     │                  │                  │                  │
     │ RequestCancel    │                  │                  │
     │ Activity(seq=1)  │                  │                  │
     │─────────────────▶│                  │                  │
     │                  │  CancelActivity  │                  │
     │                  │─────────────────▶│                  │
     │                  │                  │                  │
     │                  │                  │ (next heartbeat) │
     │                  │                  │◀─────────────────│
     │                  │                  │                  │
     │                  │                  │ Cancelled!       │
     │                  │                  │─────────────────▶│
     │                  │                  │                  │
     │                  │                  │ CancelActivity   │
     │                  │                  │ (task_token)     │
     │                  │                  │◀─────────────────│
     │                  │                  │                  │
     │ resolve_activity │                  │                  │
     │ (seq=1,          │                  │                  │
     │  cancelled)      │                  │                  │
     │◀─────────────────│                  │                  │
     │                  │                  │                  │
```

### CRITICAL: Cancel Requires Activation

To send `RequestCancelActivity`, the workflow needs a pending activation. Options:

1. **Concurrent timer**: Start a short timer alongside the activity
2. **External signal**: Receive signal that triggers cancellation
3. **Multiple activities**: Cancel from another activity's resolution

```
CANCEL TIMING
=============

Option 1: Timer wakes workflow
  t1: complete([ScheduleActivity(seq=1), StartTimer(seq=1, 1s)])
  t2: poll → [fire_timer(seq=1)]
  t3: complete([RequestCancelActivity(seq=1)])

Option 2: Signal wakes workflow
  t1: complete([ScheduleActivity(seq=1)])
  t2: (external signal sent)
  t3: poll → [signal_workflow(name="cancel")]
  t4: complete([RequestCancelActivity(seq=1)])
```

### Key Fields

**RequestCancelActivity command**:
```protobuf
request_cancel_activity {
  seq: 1  # Matches ScheduleActivity seq
}
```

**resolve_activity job (cancelled)**:
```protobuf
resolve_activity {
  seq: 1
  result {
    cancelled {
      failure {
        message: "Activity cancelled"
        canceled_failure_info {
          details {
            payloads: [Payload, ...]  # Optional cancellation details
          }
        }
      }
    }
  }
}
```

### Activity Cancellation Flow

```
Activity Heartbeat Cancellation Flow
====================================

Activity                   Server
   │                         │
   │ record_heartbeat()      │
   │────────────────────────▶│
   │                         │ check: cancel_requested?
   │                         │
   │ response: cancelled=true│
   │◀────────────────────────│
   │                         │
   │ (handle cancellation)   │
   │                         │
   │ fail_activity(cancelled)│
   │────────────────────────▶│
   │                         │
```

```python
# Activity heartbeat returns cancellation
try:
    await bridge.record_activity_heartbeat(details)
except CancellationError:
    # Cancellation requested!
    # Clean up and exit
    raise
```

### Learnings
- Cancellation is cooperative - activity must check heartbeat
- Activity can choose to ignore cancellation
- No heartbeat = no cancellation notification
- `heartbeat_timeout` determines how often activity should heartbeat
- Cancelled activities should clean up resources

---

## Activity Retry Behavior

When an activity fails with a retryable error:

```
RETRY FLOW (Handled by SDK-Core)
================================

Attempt 1: Failed (retryable)
           │
           │ backoff: initial_interval
           ▼
Attempt 2: Failed (retryable)
           │
           │ backoff: initial_interval * backoff_coefficient
           ▼
Attempt 3: Success
           │
           ▼
        Workflow receives resolve_activity(completed)


If all attempts fail:
        Workflow receives resolve_activity(failed)
```

1. SDK-Core schedules retry based on `retry_policy`
2. Workflow does NOT receive `resolve_activity` during retries
3. Only final result (success, failure after max retries, or timeout) surfaces

### Backoff Status (Rare)

In some cases, you may see a backoff status:

```protobuf
resolve_activity {
  seq: 1
  result {
    backoff {
      attempt: 2
      next_retry_delay { seconds: 2 }
    }
  }
}
```

This is rare - usually SDK-Core handles retries internally without surfacing them.

---

## Test File

See `tests/bridge_patterns/test_bridge_activities.py` for executable examples.

```bash
uv run pytest tests/bridge_patterns/test_bridge_activities.py -v -m temporal_server
```
