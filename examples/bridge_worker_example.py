"""Example of using TrioBridgeWorker to connect to a Temporal server.

This example shows how to:
1. Connect to a Temporal server
2. Create a bridge worker
3. Register workflows
4. Run the worker with Trio

Prerequisites:
- Temporal server running (e.g., `temporal server start-dev`)
- temporalio package installed
"""

import asyncio
import logging

import temporalio.bridge.client
import temporalio.bridge.runtime
import temporalio.bridge.worker
import temporalio.client
import trio

from temporalio_trio import workflow
from temporalio_trio.bridge_worker import TrioBridgeWorker

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
    """Run the Trio bridge worker."""
    # Connect to Temporal server
    # Note: This uses asyncio's connect, which should work with Trio via the bridge
    client = await temporalio.client.Client.connect("localhost:7233")

    # Get the bridge client
    bridge_client = client._bridge_client

    # Create bridge worker config
    from temporalio.bridge.worker import (
        FixedSizeSlotSupplier,
        TunerHolder,
        WorkerConfig,
    )

    config = WorkerConfig(
        namespace="default",
        task_queue="trio-example-queue",
        build_id="",
        identity_override=None,
        max_cached_workflows=100,
        tuner=TunerHolder(
            workflow_slot_supplier=FixedSizeSlotSupplier(num_slots=100),
            activity_slot_supplier=FixedSizeSlotSupplier(num_slots=100),
            local_activity_slot_supplier=FixedSizeSlotSupplier(num_slots=100),
        ),
        max_concurrent_workflow_task_polls=5,
        nonsticky_to_sticky_poll_ratio=0.2,
        max_concurrent_activity_task_polls=5,
        no_remote_activities=True,  # No activities in this example
        sticky_queue_schedule_to_start_timeout_millis=10000,
        max_heartbeat_throttle_interval_millis=60000,
        default_heartbeat_throttle_interval_millis=30000,
        max_activities_per_second=None,
        max_task_queue_activities_per_second=None,
        graceful_shutdown_period_millis=0,
        use_worker_versioning=False,
        nondeterminism_as_workflow_fail=False,
        nondeterminism_as_workflow_fail_for_types=set(),
    )

    # Create bridge worker
    bridge_worker = temporalio.bridge.worker.Worker.create(bridge_client, config)

    # Validate the worker
    await bridge_worker.validate()

    # Create Trio bridge worker
    trio_worker = TrioBridgeWorker(
        bridge_worker=bridge_worker,
        namespace="default",
        task_queue="trio-example-queue",
        workflows=[TimerWorkflow],
    )

    logging.info("Starting Trio bridge worker...")
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
        await trio_worker.run()
    except KeyboardInterrupt:
        logging.info("Shutting down...")
        trio_worker.shutdown()
        await trio_worker.finalize_shutdown()


def main():
    """Entry point for the example."""
    # Run with Trio
    trio.run(run_worker)


if __name__ == "__main__":
    main()
