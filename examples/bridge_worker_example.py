"""Example of using the Trio Worker to connect to a Temporal server.

This example shows how to:
1. Connect to a Temporal server
2. Create a Trio-based worker
3. Register workflows
4. Run the worker with Trio

Prerequisites:
- Temporal server running (e.g., `temporal server start-dev`)
- temporalio package installed
"""

import logging

import temporalio.client
import trio

from temporalio_trio import workflow
from temporalio_trio.worker import Worker

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


@workflow.defn
class TimerWorkflow:
    """Simple workflow that sleeps for a duration."""

    @workflow.run
    async def run(self, duration: float) -> str:
        """Sleep for the given duration and return a message.

        Args:
            duration: Sleep duration in seconds

        Returns:
            Completion message
        """
        workflow_info = workflow.info()
        logging.info(
            f"TimerWorkflow started: workflow_id={workflow_info.workflow_id}, "
            f"duration={duration}s"
        )

        await workflow.sleep(duration)

        logging.info(
            f"TimerWorkflow completed: workflow_id={workflow_info.workflow_id}"
        )
        return f"Slept for {duration} seconds"


async def run_worker():
    """Run the Trio worker."""
    import asyncio

    # NOTE: Client connection still uses asyncio (standard SDK)
    # The Trio worker will handle workflow execution using Trio
    async def connect_client():
        return await temporalio.client.Client.connect("localhost:7233")

    # Connect to Temporal using asyncio client
    client = asyncio.run(connect_client())

    # Create Trio worker - uses Trio for workflow execution!
    worker = Worker(
        client,
        task_queue="trio-example-queue",
        workflows=[TimerWorkflow],
    )

    logging.info("Starting Trio worker...")
    logging.info("Use the Temporal CLI to start a workflow:")
    logging.info(
        "  temporal workflow start "
        "--type TimerWorkflow "
        "--task-queue trio-example-queue "
        "--input '5.0' "
        "--workflow-id my-timer-workflow"
    )
    logging.info("Press Ctrl+C to stop")

    try:
        # Run the worker
        await worker.run()
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        worker.shutdown()


def main():
    """Entry point for the example."""
    # Run with Trio
    trio.run(run_worker)


if __name__ == "__main__":
    main()
