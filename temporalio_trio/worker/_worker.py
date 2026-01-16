"""High-level Worker API for Trio-based Temporal workflows.

This module provides a Worker class that matches the standard Temporal Python SDK
Worker interface, but uses Trio for async operations instead of asyncio.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional, Sequence, Type

import temporalio.bridge.worker
import temporalio.client
import temporalio.converter
import trio
import trio_asyncio

from temporalio_trio.bridge_worker import TrioBridgeWorker

logger = logging.getLogger(__name__)

__all__ = ["Worker"]


class Worker:
    """Worker to process Trio-based workflows.

    This worker matches the standard Temporal Python SDK Worker API but uses Trio
    for async operations. Once created, workers can be run and shut down explicitly
    via :py:meth:`run` and :py:meth:`shutdown`. Alternatively, workers can be used
    in an ``async with`` clause.

    Example:
        ```python
        from temporalio.client import Client
        from temporalio_trio.worker import Worker

        client = await Client.connect("localhost:7233")
        worker = Worker(
            client,
            task_queue="my-task-queue",
            workflows=[MyWorkflow],
        )

        async with worker:
            # Worker runs until context exits
            await trio.sleep_forever()
        ```
    """

    def __init__(
        self,
        client: temporalio.client.Client,
        *,
        task_queue: str,
        workflows: Sequence[Type] = [],
        data_converter: Optional[temporalio.converter.DataConverter] = None,
        namespace: Optional[str] = None,
        build_id: Optional[str] = None,
        identity: Optional[str] = None,
        max_cached_workflows: int = 1000,
        max_concurrent_workflow_task_polls: int = 5,
        nonsticky_to_sticky_poll_ratio: float = 0.2,
        max_concurrent_activity_task_polls: int = 5,
        no_remote_activities: bool = True,
        sticky_queue_schedule_to_start_timeout: timedelta = timedelta(seconds=10),
        max_heartbeat_throttle_interval: timedelta = timedelta(seconds=60),
        default_heartbeat_throttle_interval: timedelta = timedelta(seconds=30),
        max_activities_per_second: Optional[float] = None,
        max_task_queue_activities_per_second: Optional[float] = None,
        graceful_shutdown_timeout: timedelta = timedelta(),
        max_concurrent_workflow_tasks: Optional[int] = None,
        max_concurrent_activities: Optional[int] = None,
        max_concurrent_local_activities: Optional[int] = None,
    ) -> None:
        """Create a worker to process Trio-based workflows.

        Args:
            client: Client to use for this worker. Must be a connected
                :py:class:`temporalio.client.Client` instance.
            task_queue: Required task queue for this worker.
            workflows: Set of workflow classes decorated with
                :py:func:`@workflow.defn<temporalio_trio.workflow.defn>`.
            data_converter: Data converter for payload serialization. If not
                provided, the default converter is used.
            namespace: Namespace for this worker. If not provided, uses the
                client's namespace.
            build_id: Unique identifier for the current runtime. This is best
                set as a hash of all code and should change only when code does.
                If unset, a best-effort identifier is generated.
            identity: Identity for this worker client. If unset, the client
                identity is used.
            max_cached_workflows: If nonzero, workflows will be cached and
                sticky task queues will be used.
            max_concurrent_workflow_task_polls: Maximum number of concurrent
                poll workflow task requests we will perform at a time on this
                worker's task queue.
            nonsticky_to_sticky_poll_ratio: max_concurrent_workflow_task_polls *
                this number = the number of max pollers that will be allowed for
                the nonsticky queue when sticky tasks are enabled.
            max_concurrent_activity_task_polls: Maximum number of concurrent
                poll activity task requests we will perform at a time on this
                worker's task queue.
            no_remote_activities: If True, this worker will not poll for or
                execute remote activities.
            sticky_queue_schedule_to_start_timeout: How long a workflow task is
                allowed to sit on the sticky queue before it is timed out and
                moved to the non-sticky queue.
            max_heartbeat_throttle_interval: Maximum amount of time between
                sending each pending heartbeat to the server.
            default_heartbeat_throttle_interval: Default amount of time between
                sending each pending heartbeat to the server.
            max_activities_per_second: Maximum number of activities per second
                this worker will process. If not set, there is no limit.
            max_task_queue_activities_per_second: Maximum number of activities
                per second the task queue will dispatch to this worker.
            graceful_shutdown_timeout: Time to wait for worker to gracefully
                shutdown before forcing shutdown.
            max_concurrent_workflow_tasks: Maximum allowed number of workflow
                tasks that will ever be given to this worker at one time. If not
                set, defaults to 100.
            max_concurrent_activities: Maximum number of activity tasks that
                will ever be given to this worker concurrently. If not set and
                no_remote_activities is False, defaults to 100.
            max_concurrent_local_activities: Maximum number of local activity
                tasks that will ever be given to this worker concurrently. If not
                set, defaults to 100.

        Raises:
            ValueError: If no workflows are provided.
        """
        if not workflows:
            raise ValueError("At least one workflow must be specified")

        self._client = client
        self._task_queue = task_queue
        self._workflows = list(workflows)
        self._data_converter = data_converter
        self._namespace = namespace or "default"
        self._build_id = build_id
        self._identity = identity
        self._max_cached_workflows = max_cached_workflows
        self._max_concurrent_workflow_task_polls = max_concurrent_workflow_task_polls
        self._nonsticky_to_sticky_poll_ratio = nonsticky_to_sticky_poll_ratio
        self._max_concurrent_activity_task_polls = max_concurrent_activity_task_polls
        self._no_remote_activities = no_remote_activities
        self._sticky_queue_schedule_to_start_timeout = (
            sticky_queue_schedule_to_start_timeout
        )
        self._max_heartbeat_throttle_interval = max_heartbeat_throttle_interval
        self._default_heartbeat_throttle_interval = (
            default_heartbeat_throttle_interval
        )
        self._max_activities_per_second = max_activities_per_second
        self._max_task_queue_activities_per_second = (
            max_task_queue_activities_per_second
        )
        self._graceful_shutdown_timeout = graceful_shutdown_timeout
        self._max_concurrent_workflow_tasks = max_concurrent_workflow_tasks or 100
        self._max_concurrent_activities = max_concurrent_activities or 100
        self._max_concurrent_local_activities = max_concurrent_local_activities or 100

        # Internal state
        self._bridge_worker: Optional[temporalio.bridge.worker.Worker] = None
        self._trio_worker: Optional[TrioBridgeWorker] = None
        self._started = False
        self._shutdown_event = trio.Event()

    @property
    def task_queue(self) -> str:
        """Task queue this worker is on."""
        return self._task_queue

    @property
    def client(self) -> temporalio.client.Client:
        """Client currently set on the worker."""
        return self._client

    @property
    def is_running(self) -> bool:
        """Whether the worker is running."""
        return self._started and not self._shutdown_event.is_set()

    async def run(self) -> None:
        """Run the worker and wait on it to be shut down.

        This will not return until shutdown is complete. To shut down this worker,
        invoke :py:meth:`shutdown` from another task.

        Raises:
            RuntimeError: If the worker has already been started.
        """
        if self._started:
            raise RuntimeError("Worker has already been started")

        self._started = True

        try:
            # Initialize bridge worker inside trio-asyncio context
            async with trio_asyncio.open_loop():
                # Get bridge client
                bridge_client = self._client.service_client._bridge_client

                # Create bridge worker config
                from temporalio.bridge.worker import (
                    FixedSizeSlotSupplier,
                    PollerBehaviorSimpleMaximum,
                    TunerHolder,
                    WorkerConfig,
                    WorkerTaskTypes,
                    WorkerVersioningStrategyNone,
                )

                config = WorkerConfig(
                    namespace=self._namespace,
                    task_queue=self._task_queue,
                    versioning_strategy=WorkerVersioningStrategyNone(
                        build_id_no_versioning=self._build_id or ""
                    ),
                    identity_override=self._identity,
                    max_cached_workflows=self._max_cached_workflows,
                    tuner=TunerHolder(
                        workflow_slot_supplier=FixedSizeSlotSupplier(
                            num_slots=self._max_concurrent_workflow_tasks
                        ),
                        activity_slot_supplier=FixedSizeSlotSupplier(
                            num_slots=self._max_concurrent_activities
                        ),
                        local_activity_slot_supplier=FixedSizeSlotSupplier(
                            num_slots=self._max_concurrent_local_activities
                        ),
                        nexus_slot_supplier=FixedSizeSlotSupplier(num_slots=100),
                    ),
                    workflow_task_poller_behavior=PollerBehaviorSimpleMaximum(
                        simple_maximum=self._max_concurrent_workflow_task_polls
                    ),
                    nonsticky_to_sticky_poll_ratio=self._nonsticky_to_sticky_poll_ratio,
                    activity_task_poller_behavior=PollerBehaviorSimpleMaximum(
                        simple_maximum=self._max_concurrent_activity_task_polls
                    ),
                    no_remote_activities=self._no_remote_activities,
                    task_types=WorkerTaskTypes(
                        enable_workflows=True,
                        enable_local_activities=False,
                        enable_remote_activities=not self._no_remote_activities,
                        enable_nexus=False,
                    ),
                    sticky_queue_schedule_to_start_timeout_millis=int(
                        self._sticky_queue_schedule_to_start_timeout.total_seconds()
                        * 1000
                    ),
                    max_heartbeat_throttle_interval_millis=int(
                        self._max_heartbeat_throttle_interval.total_seconds() * 1000
                    ),
                    default_heartbeat_throttle_interval_millis=int(
                        self._default_heartbeat_throttle_interval.total_seconds() * 1000
                    ),
                    max_activities_per_second=self._max_activities_per_second,
                    max_task_queue_activities_per_second=self._max_task_queue_activities_per_second,
                    graceful_shutdown_period_millis=int(
                        self._graceful_shutdown_timeout.total_seconds() * 1000
                    ),
                    nondeterminism_as_workflow_fail=False,
                    nondeterminism_as_workflow_fail_for_types=set(),
                    nexus_task_poller_behavior=PollerBehaviorSimpleMaximum(
                        simple_maximum=5
                    ),
                    plugins=[],
                )

                # Create bridge worker
                self._bridge_worker = temporalio.bridge.worker.Worker.create(
                    bridge_client, config
                )

                # Validate the worker
                await trio_asyncio.run_aio_coroutine(self._bridge_worker.validate())

                # Create Trio bridge worker
                self._trio_worker = TrioBridgeWorker(
                    bridge_worker=self._bridge_worker,
                    namespace=self._namespace,
                    task_queue=self._task_queue,
                    workflows=self._workflows,
                    data_converter=self._data_converter,
                )

                logger.info(
                    f"Starting Trio worker on {self._namespace}/{self._task_queue}"
                )

                # Run the worker until shutdown
                async with trio.open_nursery() as nursery:
                    # Start the worker
                    nursery.start_soon(self._trio_worker.run)

                    # Wait for shutdown signal
                    await self._shutdown_event.wait()

                    # Cancel the nursery to stop the worker
                    nursery.cancel_scope.cancel()

                # Finalize shutdown
                if self._bridge_worker:
                    await trio_asyncio.run_aio_coroutine(
                        self._bridge_worker.finalize_shutdown()
                    )

                logger.info("Trio worker stopped")

        except Exception:
            logger.exception("Worker failed")
            raise

    def shutdown(self) -> None:
        """Initiate graceful shutdown of the worker.

        This signals the worker to stop polling and complete in-flight activations.
        The :py:meth:`run` method will return after shutdown is complete.
        """
        logger.info("Initiating worker shutdown")
        self._shutdown_event.set()
        if self._trio_worker:
            self._trio_worker.shutdown()

    async def __aenter__(self) -> Worker:
        """Start the worker and return self for use by ``async with``.

        This is a wrapper around :py:meth:`run`. The worker will run in the
        background until the context exits.

        Returns:
            Self, for use in the ``async with`` statement.
        """
        # Start the worker in a background task
        async def run_worker():
            try:
                await self.run()
            except Exception:
                logger.exception("Worker failed in async context")
                raise

        import trio

        nursery_manager = trio.open_nursery()
        self._context_nursery = await nursery_manager.__aenter__()
        self._context_nursery.start_soon(run_worker)

        # Give the worker a moment to start
        await trio.sleep(0.1)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Same as :py:meth:`shutdown` for use by ``async with``.

        This will shut down the worker and wait for it to complete.
        """
        self.shutdown()
        if hasattr(self, "_context_nursery"):
            await self._context_nursery.__aexit__(exc_type, exc_val, exc_tb)
