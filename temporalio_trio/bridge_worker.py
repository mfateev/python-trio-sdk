"""Trio-based workflow worker that polls from the Temporal bridge.

This module provides TrioBridgeWorker which integrates the POC workflow runtime
with the Temporal Rust bridge, enabling communication with Temporal servers.
"""

from __future__ import annotations

import logging
from typing import Any, Sequence, Type

import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2
import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2
import temporalio.bridge.worker
import temporalio.converter
import temporalio.workflow
import trio

from temporalio_trio.worker._bridge_types import (
    bridge_to_poc_activation,
    poc_to_bridge_completion,
)
from temporalio_trio.worker._workflow_instance import (
    TrioWorkflowRunner,
    WorkflowInstance,
    WorkflowInstanceDetails,
)
from temporalio_trio.workflow import Info, _Definition

__all__ = ["TrioBridgeWorker"]

logger = logging.getLogger(__name__)


class TrioBridgeWorker:
    """Trio-based workflow worker that polls from the bridge.

    This worker:
    1. Polls workflow activations from the Temporal bridge
    2. Converts bridge protobuf types to POC activation types
    3. Dispatches activations to TrioWorkflowInstance
    4. Converts POC completions back to bridge protobuf
    5. Sends completions back to the bridge

    Args:
        bridge_worker: Bridge worker instance for polling
        namespace: Temporal namespace
        task_queue: Task queue name
        workflows: List of workflow classes to register
        data_converter: Data converter for payload serialization
    """

    def __init__(
        self,
        bridge_worker: temporalio.bridge.worker.Worker,
        namespace: str,
        task_queue: str,
        workflows: Sequence[Type],
        data_converter: temporalio.converter.DataConverter | None = None,
    ) -> None:
        """Initialize the Trio bridge worker."""
        self._bridge_worker = bridge_worker
        self._namespace = namespace
        self._task_queue = task_queue
        self._data_converter = data_converter or temporalio.converter.DataConverter()
        self._shutdown_event = trio.Event()
        self._runner = TrioWorkflowRunner()

        # Prepare workflows
        self._workflows: dict[str, _Definition] = {}
        for workflow in workflows:
            # Get our own _Definition from the workflow class
            defn = _Definition.must_from_class(workflow)
            if defn.name and defn.name in self._workflows:
                raise ValueError(f"Duplicate workflow name: {defn.name}")
            # Prepare the workflow
            self._runner.prepare_workflow(defn)
            if defn.name:
                self._workflows[defn.name] = defn

        # Track workflow instances by run_id
        self._instances: dict[str, WorkflowInstance] = {}

    async def run(self) -> None:
        """Run the worker until shutdown.

        This method:
        1. Continuously polls for workflow activations
        2. Dispatches them in parallel using a nursery
        3. Handles shutdown gracefully
        """
        logger.info(
            f"Starting Trio bridge worker on {self._namespace}/{self._task_queue}"
        )

        async with trio.open_nursery() as nursery:
            # Start the polling loop
            nursery.start_soon(self._poll_loop)

            # Wait for shutdown signal
            await self._shutdown_event.wait()

            # Cancel the nursery to stop polling
            nursery.cancel_scope.cancel()

        logger.info("Trio bridge worker stopped")

    async def _poll_loop(self) -> None:
        """Poll for activations until shutdown."""
        try:
            while not self._shutdown_event.is_set():
                # Poll for activation (this returns a protobuf)
                try:
                    bridge_act = await self._bridge_worker.poll_workflow_activation()
                except Exception as e:
                    # Check if it's a PollShutdownError (not exported, check by name)
                    if e.__class__.__name__ == "PollShutdownError":
                        # Bridge is shutting down
                        logger.info("Bridge worker shutdown signal received")
                        break
                    raise

                # Handle the activation in the background
                # We don't await here to allow parallel activation handling
                async with trio.open_nursery() as act_nursery:
                    act_nursery.start_soon(self._handle_activation, bridge_act)

        except Exception as err:
            logger.exception("Error in poll loop")
            raise

    async def _handle_activation(
        self,
        bridge_act: temporalio.bridge.proto.workflow_activation.workflow_activation_pb2.WorkflowActivation,
    ) -> None:
        """Handle a single workflow activation.

        Args:
            bridge_act: Bridge activation protobuf
        """
        run_id = bridge_act.run_id

        try:
            # Check if this is a cache eviction
            is_eviction = False
            for job in bridge_act.jobs:
                if job.HasField("remove_from_cache"):
                    is_eviction = True
                    break

            if is_eviction:
                # Handle eviction: remove from cache and respond with empty completion
                logger.debug(f"Evicting workflow {run_id}")
                if run_id in self._instances:
                    del self._instances[run_id]

                # Send empty success completion
                comp = temporalio.bridge.proto.workflow_completion.workflow_completion_pb2.WorkflowActivationCompletion()
                comp.run_id = run_id
                comp.successful.SetInParent()
                await self._bridge_worker.complete_workflow_activation(comp)
                return

            # Convert bridge activation to POC activation
            poc_act = bridge_to_poc_activation(bridge_act, self._data_converter)

            # Get or create workflow instance
            instance = self._instances.get(run_id)
            if instance is None:
                # Find the WorkflowStartedJob to create the instance
                from temporalio_trio.worker._activation import WorkflowStartedJob

                started_job: WorkflowStartedJob | None = None
                for job in poc_act.jobs:  # type: ignore[assignment]
                    if isinstance(job, WorkflowStartedJob):
                        started_job = job
                        break

                if started_job is None:
                    raise RuntimeError(
                        f"No WorkflowStartedJob found for new workflow {run_id}"
                    )

                # Get workflow definition
                defn = self._workflows.get(started_job.workflow_type)
                if defn is None:
                    raise RuntimeError(
                        f"Unknown workflow type: {started_job.workflow_type}"
                    )

                # Create workflow info
                # Extract workflow_id from bridge activation
                workflow_id = None
                for job in bridge_act.jobs:
                    if job.HasField("initialize_workflow"):
                        workflow_id = job.initialize_workflow.workflow_id
                        break

                if workflow_id is None:
                    raise RuntimeError("No workflow_id in InitializeWorkflow")

                info = Info(
                    workflow_id=workflow_id,
                    run_id=run_id,
                    workflow_type=started_job.workflow_type,
                    task_queue=self._task_queue,
                )

                # Extract randomness seed
                randomness_seed = 0
                for job in bridge_act.jobs:
                    if job.HasField("initialize_workflow"):
                        randomness_seed = job.initialize_workflow.randomness_seed
                        break

                # Create instance details
                details = WorkflowInstanceDetails(
                    defn=defn,
                    info=info,
                    randomness_seed=randomness_seed,
                )

                # Create instance
                instance = self._runner.create_instance(details)
                self._instances[run_id] = instance

            # Run activation in a thread for isolation (Phase 3 improvement)
            # For Phase 1, we'll just call it directly
            poc_comp = instance.activate(poc_act)

            # Convert POC completion to bridge completion
            bridge_comp = poc_to_bridge_completion(
                run_id, poc_comp, self._data_converter
            )

            # Send completion to bridge
            await self._bridge_worker.complete_workflow_activation(bridge_comp)

            logger.debug(f"Completed activation for workflow {run_id}")

        except Exception as err:
            logger.exception(f"Error handling activation for workflow {run_id}")

            # Send failure completion
            comp = temporalio.bridge.proto.workflow_completion.workflow_completion_pb2.WorkflowActivationCompletion()
            comp.run_id = run_id
            comp.failed.failure.message = f"Workflow activation failed: {err}"
            comp.failed.failure.stack_trace = ""
            await self._bridge_worker.complete_workflow_activation(comp)

    def shutdown(self) -> None:
        """Initiate graceful shutdown of the worker.

        This signals the worker to stop polling and complete in-flight activations.
        """
        logger.info("Initiating worker shutdown")
        self._shutdown_event.set()
        self._bridge_worker.initiate_shutdown()

    async def finalize_shutdown(self) -> None:
        """Finalize shutdown of the bridge worker.

        This must be called after run() completes to properly clean up the bridge.
        """
        logger.info("Finalizing worker shutdown")
        await self._bridge_worker.finalize_shutdown()
