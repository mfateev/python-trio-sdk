#!/usr/bin/env python3
"""Example of using activities with the Trio-based Temporal SDK.

This example demonstrates:
1. Defining async activities with @activity.defn
2. Using activity context (info, heartbeat, cancellation)
3. Running a worker with activities

Note: This is Phase 1 - activities can be executed when scheduled externally.
Workflow-activity integration (Phase 2) is not yet implemented.

To run this example:
1. Start a Temporal server: temporal server start-dev
2. Run this script: uv run python examples/activity_example.py
3. Schedule an activity externally using Temporal CLI or another client
"""

import logging

import trio

from temporalio_trio import activity
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# =============================================================================
# Activity Definitions
# =============================================================================


@activity.defn
async def greet(name: str) -> str:
    """Simple activity that returns a greeting.

    Demonstrates basic activity definition and execution.
    """
    info = activity.info()
    logger.info(f"Running greet activity (attempt {info.attempt})")
    return f"Hello, {name}!"


@activity.defn
async def process_data(data: list[int]) -> int:
    """Activity that processes data with heartbeats.

    Demonstrates heartbeat usage for long-running activities.
    """
    info = activity.info()
    logger.info(f"Processing {len(data)} items in activity {info.activity_id}")

    total = 0
    for i, value in enumerate(data):
        # Check for cancellation
        if activity.is_cancelled():
            logger.warning("Activity cancelled, returning partial result")
            return total

        # Process item
        total += value

        # Send periodic heartbeats
        if i % 10 == 0:
            activity.heartbeat(f"Processed {i + 1}/{len(data)} items")

        # Simulate work
        await trio.sleep(0.01)

    logger.info(f"Completed processing, total: {total}")
    return total


@activity.defn(name="long-running-task")
async def long_running_task(duration_seconds: float) -> str:
    """Activity that demonstrates cancellation handling.

    Demonstrates:
    - Custom activity name
    - Cancellation detection
    - Worker shutdown detection
    """
    info = activity.info()
    logger.info(f"Starting long-running task for {duration_seconds}s")

    start_time = trio.current_time()
    while trio.current_time() - start_time < duration_seconds:
        # Check for cancellation
        if activity.is_cancelled():
            logger.warning("Activity was cancelled")
            return "cancelled"

        # Check for worker shutdown
        if activity.is_worker_shutdown():
            logger.warning("Worker is shutting down")
            return "shutdown"

        # Send heartbeat
        elapsed = trio.current_time() - start_time
        activity.heartbeat(f"Running for {elapsed:.1f}s")

        await trio.sleep(0.5)

    return f"Completed after {duration_seconds}s"


@activity.defn
async def failing_activity() -> None:
    """Activity that demonstrates failure handling."""
    info = activity.info()
    logger.info(f"Running failing activity (attempt {info.attempt})")
    raise ValueError("This activity always fails!")


# =============================================================================
# Main
# =============================================================================


async def main():
    """Run a worker with activities."""
    # Connect to Temporal server
    logger.info("Connecting to Temporal server...")
    client = await Client.connect("localhost:7233")

    # Create worker with activities
    logger.info("Starting worker with activities...")
    worker = Worker(
        client,
        task_queue="activity-example-queue",
        activities=[
            greet,
            process_data,
            long_running_task,
            failing_activity,
        ],
    )

    # Run the worker
    logger.info("Worker running. Press Ctrl+C to stop.")
    logger.info("")
    logger.info("To test activities, use the Temporal CLI:")
    logger.info("  temporal activity execute --type greet --input '[\"World\"]' \\")
    logger.info("    --task-queue activity-example-queue --workflow-id test-wf")
    logger.info("")

    try:
        async with worker:
            # Keep running until interrupted
            await trio.sleep_forever()
    except KeyboardInterrupt:
        logger.info("Shutting down...")


if __name__ == "__main__":
    trio.run(main)
