# Running the Trio-based Temporal Worker Example

> **✅ WORKING** - This example is fully functional! The SDK Core integration is complete and workflows execute successfully with real Temporal servers.

This guide shows how to run a simple Temporal worker using the trio-based SDK implementation.

## Current Status

✅ **All features working:**
- ✅ Worker class API (matches standard SDK)
- ✅ Workflow definition API (@workflow.defn, @workflow.run)
- ✅ Trio-native workflow execution
- ✅ SDK Core integration with deterministic timers
- ✅ Real Temporal server connection

## Prerequisites

1. **Temporal CLI** - Install from: https://docs.temporal.io/cli
   ```bash
   # macOS
   brew install temporal

   # Linux
   curl -sSf https://temporal.download/cli.sh | sh
   ```

2. **Python dependencies** - Install with uv:
   ```bash
   cd /home/sprite/workarea/workspaces/projects/tasks/trio-asyncio/python-trio-sdk
   uv sync
   ```

## Quick Start

### Step 1: Start Temporal Server

In a terminal, start the Temporal development server:

```bash
temporal server start-dev
```

This will:
- Start Temporal server on `localhost:7233`
- Start Temporal Web UI on `http://localhost:8233`
- Create the `default` namespace
- Run in the foreground (keep this terminal open)

### Step 2: Run the Worker

In a new terminal, navigate to the project directory and run the worker:

```bash
cd /home/sprite/workarea/workspaces/projects/tasks/trio-asyncio/python-trio-sdk

# Run the worker with uv
uv run python examples/bridge_worker_example.py
```

You should see output like:
```
2026-01-16 17:00:00 [INFO] root: Starting Trio bridge worker...
2026-01-16 17:00:00 [INFO] root: Use the Temporal CLI to start a workflow:
2026-01-16 17:00:00 [INFO] root:   temporal workflow start --type TimerWorkflow --task-queue trio-example-queue --input '5.0' --workflow-id my-timer-workflow
2026-01-16 17:00:00 [INFO] root: Press Ctrl+C to stop
```

Keep this running.

### Step 3: Start a Workflow

In a third terminal, use the Temporal CLI to start a workflow:

```bash
temporal workflow start \
  --type TimerWorkflow \
  --task-queue trio-example-queue \
  --input '5.0' \
  --workflow-id my-timer-workflow
```

This command:
- Starts a workflow of type `TimerWorkflow`
- Sends it to the `trio-example-queue` task queue (where our worker is listening)
- Passes `5.0` as input (sleep for 5 seconds)
- Assigns workflow ID `my-timer-workflow`

### Step 4: Watch the Workflow Execute

You should see output in the worker terminal:
```
2026-01-16 17:01:00 [INFO] root: TimerWorkflow started: workflow_id=my-timer-workflow, duration=5.0s
2026-01-16 17:01:05 [INFO] root: TimerWorkflow completed: workflow_id=my-timer-workflow
```

### Step 5: Check Workflow Result

Get the workflow result:

```bash
temporal workflow describe --workflow-id my-timer-workflow
```

Or view in the Web UI: http://localhost:8233

## What the Example Does

The `TimerWorkflow` is a simple workflow that:

1. Receives a duration parameter (in seconds)
2. Logs the start time
3. Sleeps for the specified duration using `workflow.sleep()`
4. Logs completion
5. Returns a message: `"Slept for {duration} seconds"`

This demonstrates:
- **Trio-based workflow execution** - Uses `async`/`await` with Trio
- **Deterministic timers** - `workflow.sleep()` is deterministic and replay-safe
- **Workflow info** - Access to workflow metadata via `workflow.info()`
- **Bridge worker integration** - Connects to real Temporal server

## Useful Commands

### Start Multiple Workflows

```bash
# Start multiple workflows with different durations
temporal workflow start --type TimerWorkflow --task-queue trio-example-queue --input '3.0' --workflow-id timer-1
temporal workflow start --type TimerWorkflow --task-queue trio-example-queue --input '7.0' --workflow-id timer-2
temporal workflow start --type TimerWorkflow --task-queue trio-example-queue --input '2.0' --workflow-id timer-3
```

### List Running Workflows

```bash
temporal workflow list
```

### Get Workflow Details

```bash
temporal workflow describe --workflow-id my-timer-workflow
```

### View Workflow History

```bash
temporal workflow show --workflow-id my-timer-workflow
```

## Stopping Everything

1. **Stop the worker**: Press `Ctrl+C` in the worker terminal
2. **Stop the Temporal server**: Press `Ctrl+C` in the server terminal

## Troubleshooting

### "Connection refused" error

Make sure the Temporal server is running:
```bash
temporal server start-dev
```

### "Task queue not found" error

The worker must be running before you start workflows. Start the worker first (Step 2).

### Import errors

Make sure you're in the correct directory:
```bash
cd /home/sprite/workarea/workspaces/projects/tasks/trio-asyncio/python-trio-sdk
```

And that dependencies are installed:
```bash
pip install temporalio trio
```

## Understanding the Code

### Workflow Definition

```python
@workflow.defn
class TimerWorkflow:
    @workflow.run
    async def run(self, duration: float) -> str:
        await workflow.sleep(duration)
        return f"Slept for {duration} seconds"
```

- `@workflow.defn` - Marks class as a workflow
- `@workflow.run` - Marks the workflow entry point
- `workflow.sleep()` - Deterministic sleep (uses Trio under the hood)

### Worker Setup

```python
trio_worker = TrioBridgeWorker(
    bridge_worker=bridge_worker,
    namespace="default",
    task_queue="trio-example-queue",
    workflows=[TimerWorkflow],
)
await trio_worker.run()
```

- `TrioBridgeWorker` - Trio-based worker implementation
- Registers workflows and listens on the task queue
- Processes workflow tasks using Trio's async runtime

## Next Steps

Try modifying the workflow to:
- Add multiple sleep calls
- Use `workflow.time()` to get current workflow time
- Log messages at different points
- Change the return value

All workflow APIs from `temporalio_trio.workflow` are available:
- `workflow.sleep(duration)` - Deterministic sleep
- `workflow.time()` - Current workflow time (seconds)
- `workflow.time_ns()` - Current workflow time (nanoseconds)
- `workflow.info()` - Workflow metadata (ID, run ID, etc.)
