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
    # TODO(Phase 1): Once the Rust bridge is fully implemented, create a Trio-native
    # client connection API. For now, this example shows the intended API structure.
    #
    # The current temporalio.client.Client.connect() is asyncio-based.
    # Phase 1 will implement a Trio-native client that connects via our PyO3 bridge:
    #
    # from temporalio_trio.client import Client
    # client = await Client.connect("localhost:7233")

    # For now, this example demonstrates the Worker API structure but won't run
    # until Phase 1 Rust bridge integration is complete.
    raise NotImplementedError(
        "Client connection requires Phase 1 Rust bridge integration. "
        "The Worker API is ready, but client connection is not yet implemented."
    )

    # Create Trio worker - API matches the standard SDK Worker!
    # This part is already implemented and ready to use once client is available
    worker = Worker(
        client,  # Will be a Trio-native client from Phase 1
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
