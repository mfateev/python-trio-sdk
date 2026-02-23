# Quick Start Guide

This is the simplest way to see the Trio-based Temporal SDK in action!

## Step 1: Start Temporal Server

Open a terminal and run:

```bash
/home/dev/.temporalio/bin/temporal server start-dev
```

Keep this running in the background.

## Step 2: Run the Example Worker

In a **new terminal**, run:

```bash
cd /home/sprite/workarea/workspaces/projects/tasks/trio-asyncio/python-trio-sdk
uv run python examples/bridge_worker_example.py
```

You should see:
```
Starting Trio worker...
Use the Temporal CLI to start a workflow:
  temporal workflow start --type TimerWorkflow --task-queue trio-example-queue --input '5.0' --workflow-id my-timer-workflow
Press Ctrl+C to stop
```

Keep this running.

## Step 3: Start a Workflow

In a **third terminal**, run:

```bash
/home/dev/.temporalio/bin/temporal workflow start \
  --type TimerWorkflow \
  --task-queue trio-example-queue \
  --input '5.0' \
  --workflow-id my-timer-workflow
```

## Step 4: Watch It Execute

In the worker terminal (Step 2), you'll see:
```
TimerWorkflow started: workflow_id=my-timer-workflow, duration=5.0s
TimerWorkflow completed: workflow_id=my-timer-workflow
```

## Step 5: Check the Result

```bash
/home/dev/.temporalio/bin/temporal workflow describe --workflow-id my-timer-workflow
```

You should see:
```
Status: COMPLETED
Result: "Slept for 5.0 seconds"
```

## View the History

To see the timer events in the workflow history:

```bash
/home/dev/.temporalio/bin/temporal workflow show --workflow-id my-timer-workflow
```

You'll see events including:
- `TimerStarted` - When the workflow called `workflow.sleep(5.0)`
- `TimerFired` - When the timer completed after 5 seconds

## Try Different Durations

Start workflows with different sleep durations:

```bash
# 2 seconds
temporal workflow start --type TimerWorkflow --task-queue trio-example-queue --input '2.0' --workflow-id timer-2s

# 10 seconds
temporal workflow start --type TimerWorkflow --task-queue trio-example-queue --input '10.0' --workflow-id timer-10s
```

## Cleanup

1. Stop the worker: Press `Ctrl+C` in the worker terminal
2. Stop Temporal: Press `Ctrl+C` in the server terminal

## What's Happening?

- The **Temporal server** stores workflow state and history
- The **Trio worker** executes workflows using Trio's async runtime
- `workflow.sleep()` creates a **deterministic timer** recorded in history
- All workflow execution uses **Trio** instead of asyncio!
