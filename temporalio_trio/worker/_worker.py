"""High-level Worker API for Trio-based Temporal workflows.

This module provides a Worker class that matches the standard Temporal Python SDK
Worker interface, but uses Trio for async operations instead of asyncio.

The Worker uses SingleThreadWorker which runs all workflows in a single trio.run()
with event-based suspension for efficient execution.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import timedelta
from typing import TYPE_CHECKING, Optional, Sequence, Type

import temporalio.bridge.worker
import temporalio.converter
import trio

from temporalio_trio._async_bridge import TrioBridgeWrapper
from temporalio_trio.worker._single_thread_worker import SingleThreadWorker

if TYPE_CHECKING:
    from temporalio_trio.client import Client

logger = logging.getLogger(__name__)

__all__ = ["Worker"]


class Worker:
    """Worker to process Trio-based workflows.

    This worker matches the standard Temporal Python SDK Worker API but uses Trio
    for async operations. It uses a single-threaded execution model (SingleThreadWorker)
    that runs all workflows in a single trio.run() with event-based suspension.

    Once created, workers can be run and shut down explicitly via :py:meth:`run`
    and :py:meth:`shutdown`. Alternatively, workers can be used in an ``async with``
    clause.

    Example:
        ```python
        from temporalio_trio.client import Client
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
        client: "Client",
        *,
        task_queue: str,
        workflows: Sequence[Type] = [],
        activities: Sequence[Callable] = [],
        data_converter: Optional[temporalio.converter.DataConverter] = None,
        namespace: Optional[str] = None,
        build_id: Optional[str] = None,
        identity: Optional[str] = None,
        max_cached_workflows: int = 1000,
        max_concurrent_workflow_task_polls: int = 5,
        nonsticky_to_sticky_poll_ratio: float = 0.2,
        max_concurrent_activity_task_polls: int = 5,
        no_remote_activities: bool = False,
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
        """Create a worker to process Trio-based workflows and activities.

        Args:
            client: Client to use for this worker. Must be a connected
                :py:class:`temporalio_trio.client.Client` instance.
            task_queue: Required task queue for this worker.
            workflows: Set of workflow classes decorated with
                :py:func:`@workflow.defn<temporalio_trio.workflow.defn>`.
            activities: Set of activity functions decorated with
                :py:func:`@activity.defn<temporalio_trio.activity.defn>`.
                All activities must be async (defined with `async def`).
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
            ValueError: If no workflows or activities are provided, or if
                activities are provided with no_remote_activities=True.
        """
        if not workflows and not activities:
            raise ValueError("At least one workflow or activity must be specified")
        if activities and no_remote_activities:
            raise ValueError(
                "Activities provided but no_remote_activities is True. "
                "Set no_remote_activities=False to enable activity execution."
            )

        self._client = client
        self._task_queue = task_queue
        self._workflows = list(workflows)
        self._activities = list(activities)
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
        self._default_heartbeat_throttle_interval = default_heartbeat_throttle_interval
        self._max_activities_per_second = max_activities_per_second
        self._max_task_queue_activities_per_second = (
            max_task_queue_activities_per_second
        )
        self._graceful_shutdown_timeout = graceful_shutdown_timeout
        self._max_concurrent_workflow_tasks = max_concurrent_workflow_tasks or 100
        self._max_concurrent_activities = max_concurrent_activities or 100
        self._max_concurrent_local_activities = max_concurrent_local_activities or 100

        # Internal state
        self._single_thread_worker: Optional[SingleThreadWorker] = None
        self._activity_worker = None  # TrioActivityWorker when activities are provided
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

    def _get_target_url(self) -> str:
        """Extract target URL from client, supporting both Trio and official SDK clients.

        Returns:
            Target URL with http/https scheme.

        Raises:
            RuntimeError: If target URL cannot be extracted from client.
        """
        # Try our Trio client's ClientConfig (dataclass with target_url attribute)
        if hasattr(self._client, "_config"):
            config = self._client._config
            # Our temporalio_trio.client.Client uses a ClientConfig dataclass
            if hasattr(config, "target_url"):
                target_url = config.target_url
            # Official temporalio.client.Client uses a dict with service_client
            elif isinstance(config, dict) and "service_client" in config:
                service_client = config["service_client"]
                if hasattr(service_client, "config") and hasattr(
                    service_client.config, "target_host"
                ):
                    target_url = service_client.config.target_host
                else:
                    raise RuntimeError(
                        f"Cannot extract target_url from client: "
                        f"service_client.config.target_host not found"
                    )
            else:
                raise RuntimeError(
                    f"Cannot extract target_url from client: "
                    f"unsupported _config type {type(config)}"
                )
        else:
            raise RuntimeError(
                f"Cannot extract target_url from client: no _config attribute"
            )

        # Ensure URL has scheme
        if not target_url.startswith(("http://", "https://")):
            target_url = f"http://{target_url}"

        return target_url

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

        bridge_wrapper = TrioBridgeWrapper()
        await bridge_wrapper.start()

        try:
            # Initialize bridge with Temporal configuration
            # Extract target_url from client config (supports both Trio and official SDK clients)
            target_url = self._get_target_url()

            await bridge_wrapper.initialize_with_config(
                target_url=target_url,
                namespace=self._namespace,
                task_queue=self._task_queue,
                identity=self._identity,
                max_cached_workflows=self._max_cached_workflows,
                max_concurrent_workflow_task_polls=self._max_concurrent_workflow_task_polls,
                sticky_queue_schedule_to_start_timeout_millis=int(
                    self._sticky_queue_schedule_to_start_timeout.total_seconds() * 1000
                ),
            )

            # Note: Skipping bridge validation - not implemented in Rust bridge yet
            # The initialization above already validates connection to Temporal server
            # await bridge_wrapper.validate()

            # Create workflow worker if workflows provided
            self._single_thread_worker = None
            if self._workflows:
                self._single_thread_worker = SingleThreadWorker(
                    bridge=bridge_wrapper,
                    task_queue=self._task_queue,
                    workflows=self._workflows,
                    activities=self._activities
                    if not self._no_remote_activities
                    else None,
                )

            # Create Trio activity worker if activities provided
            self._activity_worker = None
            if self._activities and not self._no_remote_activities:
                from temporalio_trio.worker._activity import TrioActivityWorker

                self._activity_worker = TrioActivityWorker(
                    bridge_wrapper=bridge_wrapper,
                    task_queue=self._task_queue,
                    activities=self._activities,
                    data_converter=self._data_converter,
                    max_heartbeat_throttle_interval=self._max_heartbeat_throttle_interval,
                    default_heartbeat_throttle_interval=self._default_heartbeat_throttle_interval,
                )

            logger.info(f"Starting Trio worker on {self._namespace}/{self._task_queue}")
            if self._workflows:
                logger.info(f"  Workflows: {[w.__name__ for w in self._workflows]}")
            if self._activities:
                logger.info(
                    f"  Activities: {[getattr(a, '__name__', str(a)) for a in self._activities]}"
                )

            async with trio.open_nursery() as nursery:
                # Start workflow worker
                if self._single_thread_worker:
                    nursery.start_soon(self._single_thread_worker.run)

                # Start activity worker
                if self._activity_worker:
                    nursery.start_soon(self._activity_worker.run)

                await self._shutdown_event.wait()

                # Initiate bridge shutdown to unblock poll_workflow_activation
                # This sends PollShutdownError to the poll loops, causing them to exit
                # But it does NOT close the bridge - in-flight completions can still proceed
                bridge_wrapper.initiate_shutdown()

                # Wait a brief moment for poll loops to receive the shutdown signal
                # and for any in-flight handlers to complete
                await trio.sleep(0.1)

                # Cancel the nursery to stop any remaining tasks
                # At this point, most handlers should have completed gracefully
                nursery.cancel_scope.cancel()

            # Shutdown bridge - now it's safe since handlers have drained
            await bridge_wrapper.shutdown()

            logger.info("Trio worker stopped")

        except Exception:
            logger.exception("Worker failed")
            # Ensure bridge is shut down on error
            await bridge_wrapper.shutdown()
            raise

    def shutdown(self) -> None:
        """Initiate graceful shutdown of the worker.

        This signals the worker to stop polling and complete in-flight activations.
        The :py:meth:`run` method will return after shutdown is complete.
        """
        logger.info("Initiating worker shutdown")
        self._shutdown_event.set()
        if self._single_thread_worker:
            self._single_thread_worker.shutdown()
        if self._activity_worker:
            self._activity_worker.shutdown()

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

        self._nursery_manager = trio.open_nursery()
        nursery = await self._nursery_manager.__aenter__()
        nursery.start_soon(run_worker)

        # Give the worker a moment to start
        await trio.sleep(0.1)

        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Same as :py:meth:`shutdown` for use by ``async with``.

        This will shut down the worker and wait for it to complete.
        """
        self.shutdown()
        if hasattr(self, "_nursery_manager"):
            return await self._nursery_manager.__aexit__(exc_type, exc_val, exc_tb)
