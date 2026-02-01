"""Single-threaded worker that runs all workflows in a single trio.run().

This module provides the SingleThreadWorker class which executes all workflows
in a single `trio.run(deterministic=True, fifo=True)` context. This is the
target architecture for the single-threaded migration, replacing the current
thread-pool-based execution model.

Key features:
- All workflows run in a single Trio context
- Event-based suspension (not replay-from-beginning)
- ContextVar isolation between workflows
- FIFO scheduling for determinism

This implements Phase 3 of the single-threaded migration plan.
"""

from __future__ import annotations

import inspect
import logging
import random
from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

import trio

from temporalio_trio.worker._activation import (
    ActivityResolvedJob,
    CancelWorkflowJob,
    ChildWorkflowResolvedJob,
    CompleteWorkflowCommand,
    FailWorkflowCommand,
    QueryWorkflowJob,
    SignalWorkflowJob,
    TimerFiredJob,
    WorkflowActivation,
    WorkflowActivationCompletion,
    WorkflowStartedJob,
)
from temporalio_trio.worker._runtime import (
    CancelWorkflowCommand,
    QueryFailureCommand,
    QuerySuccessCommand,
    WorkflowRuntime,
    reset_current_runtime,
    set_current_runtime,
)
from temporalio_trio.worker._workflow_state import WorkflowState
from temporalio_trio.workflow import _Definition, _Runtime

if TYPE_CHECKING:
    from temporalio_trio._async_bridge import TrioBridgeWrapper

__all__ = ["SingleThreadWorker"]

logger = logging.getLogger(__name__)


class SingleThreadWorker:
    """Worker that executes all workflows in a single trio.run().

    This worker implements the single-threaded execution model where all workflows
    run concurrently in a single Trio context. Each workflow gets its own task
    that stays alive between activations, suspended on trio.Event objects.

    The worker:
    1. Polls for workflow activations from the bridge
    2. Dispatches activations to existing workflows or creates new ones
    3. Waits for workflows to produce commands
    4. Sends completions back to the bridge

    Args:
        bridge: The TrioBridgeWrapper for Temporal communication.
        task_queue: The task queue name.
        workflows: List of workflow classes to register.
        activities: List of activity functions (not implemented yet).

    Attributes:
        _bridge: The bridge wrapper for Temporal communication.
        _task_queue: The task queue name.
        _workflows: Dict mapping workflow names to definitions.
        _activities: List of activity functions.
        _workflow_states: Dict mapping run_id to WorkflowState.
        _root_nursery: The root nursery for spawning workflow tasks.
        _shutdown_event: Event to signal shutdown.
    """

    def __init__(
        self,
        bridge: TrioBridgeWrapper,
        task_queue: str,
        workflows: Sequence[type],
        activities: Sequence[Callable[..., Any]] | None = None,
    ) -> None:
        """Initialize the single-thread worker.

        Args:
            bridge: Bridge wrapper for Temporal communication.
            task_queue: Task queue name.
            workflows: List of workflow classes to register.
            activities: List of activity functions (not implemented yet).
        """
        self._bridge = bridge
        self._task_queue = task_queue
        self._activities = list(activities) if activities else []
        self._shutdown_event = trio.Event()

        # Register workflow definitions
        self._workflows: dict[str, _Definition] = {}
        for workflow_cls in workflows:
            defn = _Definition.must_from_class(workflow_cls)
            if defn.name in self._workflows:
                raise ValueError(f"Duplicate workflow name: {defn.name}")
            self._workflows[defn.name] = defn

        # Runtime state (initialized in run())
        self._workflow_states: dict[str, WorkflowState] = {}
        self._root_nursery: trio.Nursery | None = None

    async def run(self) -> None:
        """Main entry point - runs all workflows in a single trio context.

        This method:
        1. Opens a nursery for workflow tasks
        2. Starts the poll loop for activations
        3. Waits for shutdown signal
        4. Gracefully stops all workflows

        Note: This should be called inside a `trio.run(deterministic=True, fifo=True)`.
        """
        logger.info(f"Starting SingleThreadWorker on task queue: {self._task_queue}")

        try:
            async with trio.open_nursery() as nursery:
                self._root_nursery = nursery

                # Start the poll loop
                nursery.start_soon(self._poll_workflow_activations)

                # Wait for shutdown
                await self._shutdown_event.wait()

                # Cancel all workflow tasks
                nursery.cancel_scope.cancel()

        finally:
            self._root_nursery = None
            logger.info("SingleThreadWorker stopped")

    async def _poll_workflow_activations(self) -> None:
        """Poll loop for workflow activations.

        Continuously polls the bridge for activations and dispatches them
        to the appropriate workflow task.
        """
        while not self._shutdown_event.is_set():
            try:
                # Poll for the next activation
                activation_bytes = await self._bridge.poll_workflow_activation()

                # Parse the activation
                # For unit tests, we use our POC activation types directly
                # For real bridge, we would parse protobuf here
                activation = self._parse_activation(activation_bytes)

                # Dispatch to the appropriate workflow
                await self._dispatch_activation(activation)

            except Exception as e:
                # Check for shutdown error
                error_name = e.__class__.__name__
                error_str = str(e)
                if (
                    "PollShutdownError" in error_name
                    or "PollShutdownError" in error_str
                ):
                    logger.info("Poll loop received shutdown signal")
                    break
                logger.exception("Error in poll loop")
                # Continue polling on other errors
                continue

    def _parse_activation(self, activation_bytes: bytes) -> WorkflowActivation:
        """Parse activation bytes into a WorkflowActivation.

        For unit tests with mock bridges, we may receive pre-parsed activations.
        For real bridges, we parse protobuf bytes here.

        Args:
            activation_bytes: Raw activation bytes or pre-parsed activation.

        Returns:
            Parsed WorkflowActivation.
        """
        # For mock bridges that return WorkflowActivation directly
        if isinstance(activation_bytes, WorkflowActivation):
            return activation_bytes

        # For real bridges, we would parse protobuf here
        # This is a placeholder for the actual implementation
        import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as wa

        from temporalio_trio.worker._bridge_types import bridge_to_poc_activation

        bridge_act = wa.WorkflowActivation()
        bridge_act.ParseFromString(activation_bytes)

        # Use default data converter for now
        import temporalio.converter

        data_converter = temporalio.converter.DataConverter()
        return bridge_to_poc_activation(bridge_act, data_converter)

    async def _dispatch_activation(self, activation: WorkflowActivation) -> None:
        """Dispatch an activation to the appropriate workflow.

        For new workflows, creates a new workflow task.
        For existing workflows, delivers the activation to the existing task.

        Args:
            activation: The activation to dispatch.
        """
        # Extract run_id from activation
        run_id = self._extract_run_id(activation)

        # Check for eviction
        if self._is_eviction(activation):
            logger.debug(f"Evicting workflow {run_id}")
            if run_id in self._workflow_states:
                del self._workflow_states[run_id]
            # Send empty completion
            await self._send_empty_completion(run_id)
            return

        # Check if this is a workflow start/replay
        has_workflow_start = any(
            isinstance(job, WorkflowStartedJob) for job in activation.jobs
        )

        if has_workflow_start and run_id in self._workflow_states:
            # This should not happen normally - cached workflows should not receive
            # initialize_workflow again. Log warning but DO NOT evict (matches sdk-python).
            logger.warning(
                f"Cache already exists for activation with initialize job (run_id={run_id})"
            )
            # Continue processing - deliver activation to existing workflow
            # (skip the WorkflowStartedJob since workflow is already started)

        # Handle empty activations for non-existent workflows (cleanup after completion)
        if not has_workflow_start and run_id not in self._workflow_states:
            logger.debug(
                f"Empty activation for completed workflow {run_id}, sending empty completion"
            )
            await self._send_empty_completion(run_id)
            return

        if run_id not in self._workflow_states:
            # New workflow - create state and spawn task
            state = WorkflowState(run_id=run_id)
            self._workflow_states[run_id] = state

            # Spawn the workflow task
            assert self._root_nursery is not None
            self._root_nursery.start_soon(self._run_workflow, state, activation)

            # Wait for initial commands
            commands = await state.wait_for_commands()
        else:
            # Existing workflow - deliver activation
            state = self._workflow_states[run_id]
            state.deliver_activation(activation)

            # Wait for commands
            commands = await state.wait_for_commands()

        # Send completion
        await self._send_completion(run_id, commands)

        # Handle workflow completion
        if state.is_complete:
            del self._workflow_states[run_id]

    def _extract_run_id(self, activation: WorkflowActivation) -> str:
        """Extract the run_id from an activation.

        Args:
            activation: The activation to extract from.

        Returns:
            The run_id string.

        Raises:
            RuntimeError: If run_id cannot be determined.
        """
        # Check if activation has run_id attribute (from bridge)
        if hasattr(activation, "run_id"):
            return activation.run_id  # type: ignore

        # For test activations, we need to derive run_id from jobs
        # In real usage, the bridge activation always has run_id
        for job in activation.jobs:
            if isinstance(job, WorkflowStartedJob):
                # Use workflow_type as a stand-in for run_id in tests
                # Real implementations would have run_id from the bridge
                return f"run-{id(activation)}"

        raise RuntimeError("Cannot determine run_id from activation")

    def _is_eviction(self, activation: WorkflowActivation) -> bool:
        """Check if the activation is a cache eviction.

        Args:
            activation: The activation to check.

        Returns:
            True if this is an eviction activation.
        """
        # Check for eviction job
        if hasattr(activation, "remove_from_cache"):
            return bool(activation.remove_from_cache)  # type: ignore
        return False

    async def _run_workflow(
        self, state: WorkflowState, initial_activation: WorkflowActivation
    ) -> None:
        """Long-running task for a single workflow instance.

        This task stays alive for the lifetime of the workflow, processing
        activations as they arrive and suspending on events between them.

        Args:
            state: The WorkflowState tracking this workflow.
            initial_activation: The first activation (contains WorkflowStartedJob).
        """
        # Extract workflow info from initial activation
        started_job = None
        for job in initial_activation.jobs:
            if isinstance(job, WorkflowStartedJob):
                started_job = job
                break

        if started_job is None:
            raise RuntimeError("Initial activation missing WorkflowStartedJob")

        # Get workflow definition
        defn = self._workflows.get(started_job.workflow_type)
        if defn is None:
            raise RuntimeError(f"Unknown workflow type: {started_job.workflow_type}")

        # Determine randomness seed (from activation or generate)
        randomness_seed = getattr(initial_activation, "randomness_seed", None)
        if randomness_seed is None:
            randomness_seed = random.getrandbits(64)

        # Create runtime with suspension callback
        runtime = WorkflowRuntime(
            run_id=state.run_id,
            workflow_id=state.run_id,  # Use run_id as workflow_id for now
            workflow_type=started_job.workflow_type,
            task_queue=self._task_queue,
            random=random.Random(randomness_seed),
            time_ns=initial_activation.timestamp_ns,
            is_replaying=getattr(initial_activation, "is_replaying", False),
            on_suspend=state.signal_commands_ready,  # Signal when workflow suspends
        )
        state.runtime = runtime

        # Set runtime as current (both _Runtime and legacy _runtime module)
        token = _Runtime.set_current(runtime)
        legacy_token = set_current_runtime(runtime)
        try:
            # Apply initial activation jobs
            self._apply_activation(runtime, initial_activation)

            # Run the workflow
            async with trio.open_nursery() as nursery:
                runtime.nursery = nursery

                # Start the main workflow coroutine
                nursery.start_soon(
                    self._execute_workflow_main, runtime, defn, started_job.args, state
                )

                # Wait for main workflow to either complete or need suspension
                # The workflow main will call signal_commands_ready when done
                # Then we process subsequent activations
                while not state.is_complete:
                    # Wait for next activation (first one will be delivered by
                    # dispatcher after it reads the initial commands)
                    try:
                        activation = await state.wait_for_activation()
                    except trio.EndOfChannel:
                        # Shutdown
                        break

                    # Update runtime state
                    runtime.time_ns = activation.timestamp_ns
                    runtime.is_replaying = getattr(activation, "is_replaying", False)

                    # Apply activation jobs
                    self._apply_activation(runtime, activation)

                    # Yield to scheduler to let woken tasks run
                    # For activations with jobs that wake workflows (TimerFired, etc.),
                    # we need to wait for the workflow to process and signal commands ready
                    await trio.sleep(0)

                    # Only force-signal for truly empty activations (0 jobs).
                    # Activations with jobs that wake workflows should NOT be force-signaled
                    # because the workflow needs time to process and produce commands.
                    if len(activation.jobs) == 0 and not state.commands_ready.is_set() and not state.is_complete:
                        # Empty activation (heartbeat) - signal with current commands
                        state.signal_commands_ready()

        except Exception as e:
            # Workflow task failed
            runtime.commands.append(FailWorkflowCommand(exception=e))
            state.mark_complete()
            state.signal_commands_ready()

        finally:
            _Runtime.reset_current(token)
            reset_current_runtime(legacy_token)

    async def _execute_workflow_main(
        self,
        runtime: WorkflowRuntime,
        defn: _Definition,
        args: tuple[Any, ...],
        state: WorkflowState,
    ) -> None:
        """Execute the main workflow coroutine.

        Args:
            runtime: The workflow runtime.
            defn: The workflow definition.
            args: Arguments to pass to the workflow.
            state: The workflow state for signaling.
        """
        try:
            # Create workflow instance
            workflow_obj = defn.cls()
            runtime.workflow_object = workflow_obj

            # Run the workflow
            result = await defn.run_fn(workflow_obj, *args)

            # Workflow completed successfully
            runtime.commands.append(CompleteWorkflowCommand(result=result))

        except trio.Cancelled:
            # Workflow was cancelled - emit CancelWorkflowCommand
            runtime.commands.append(CancelWorkflowCommand())

        except Exception as e:
            # Workflow failed
            runtime.commands.append(FailWorkflowCommand(exception=e))

        finally:
            # Mark state as complete and signal commands ready
            state.mark_complete()
            state.signal_commands_ready()

    def _apply_activation(
        self, runtime: WorkflowRuntime, activation: WorkflowActivation
    ) -> None:
        """Apply activation jobs to the runtime.

        This processes jobs in the activation and updates runtime state,
        waking up any suspended workflows as needed.

        Args:
            runtime: The workflow runtime.
            activation: The activation containing jobs.
        """
        for job in activation.jobs:
            if isinstance(job, TimerFiredJob):
                runtime.apply_timer_fired(job.timer_id, activation.timestamp_ns)
            elif isinstance(job, ActivityResolvedJob):
                runtime.apply_activity_resolved(
                    seq=job.seq,
                    result=job.result,
                    error=job.failure,
                )
            elif isinstance(job, SignalWorkflowJob):
                self._apply_signal(runtime, job)
            elif isinstance(job, QueryWorkflowJob):
                self._apply_query(runtime, job)
            elif isinstance(job, ChildWorkflowResolvedJob):
                runtime.apply_child_workflow_resolved(
                    seq=job.seq,
                    result=job.result,
                    error=job.failure,
                )
            elif isinstance(job, CancelWorkflowJob):
                runtime.apply_cancel_workflow()

    def _apply_signal(
        self, runtime: WorkflowRuntime, signal_job: SignalWorkflowJob
    ) -> None:
        """Deliver a signal to the workflow.

        If the handler exists, it is invoked with the signal arguments.
        If the handler returns a coroutine, it is started as a task in the
        workflow's nursery.

        Args:
            runtime: The workflow runtime.
            signal_job: The signal job containing the signal name and args.
        """
        handler = runtime.signal_handlers.get(signal_job.signal_name)
        if handler is None:
            logger.warning(
                f"No handler registered for signal: {signal_job.signal_name}"
            )
            return

        try:
            result = handler(*signal_job.args)
            if inspect.iscoroutine(result):
                # Run signal handler as child task in the workflow's nursery
                if runtime.nursery is not None:
                    runtime.nursery.start_soon(
                        self._run_signal_handler, result, signal_job.signal_name
                    )
                else:
                    logger.warning(
                        f"Cannot run async signal handler {signal_job.signal_name}: "
                        "no nursery available"
                    )
                    # Close the coroutine to avoid warnings
                    result.close()
        except Exception as e:
            # Log but don't fail workflow - signal handlers should not crash workflows
            logger.warning(f"Signal handler error for {signal_job.signal_name}: {e}")

    async def _run_signal_handler(
        self,
        coro: Any,
        signal_name: str,  # noqa: ANN401
    ) -> None:
        """Run an async signal handler.

        This wrapper ensures signal handler exceptions are logged but don't
        crash the workflow.

        Args:
            coro: The coroutine to run.
            signal_name: The name of the signal (for logging).
        """
        try:
            await coro
        except Exception as e:
            # Log but don't fail workflow
            logger.warning(f"Async signal handler error for {signal_name}: {e}")

    def _apply_query(
        self, runtime: WorkflowRuntime, query_job: QueryWorkflowJob
    ) -> None:
        """Execute a query and add the result command.

        Query handlers are synchronous and should not modify workflow state.
        The result (or error) is added as a command.

        Args:
            runtime: The workflow runtime.
            query_job: The query job containing the query type and args.
        """
        handler = runtime.query_handlers.get(query_job.query_type)
        if handler is None:
            runtime.commands.append(
                QueryFailureCommand(
                    query_id=query_job.query_id,
                    error=ValueError(
                        f"No handler registered for query: {query_job.query_type}"
                    ),
                )
            )
            return

        try:
            result = handler(*query_job.args)
            runtime.commands.append(
                QuerySuccessCommand(
                    query_id=query_job.query_id,
                    result=result,
                )
            )
        except Exception as e:
            runtime.commands.append(
                QueryFailureCommand(
                    query_id=query_job.query_id,
                    error=e,
                )
            )

    async def _send_completion(self, run_id: str, commands: list[Any]) -> None:
        """Send a completion with commands to the bridge.

        Args:
            run_id: The workflow run_id.
            commands: List of commands to send.
        """
        # Convert to protobuf and send
        # For now, just log the commands
        logger.debug(f"Sending completion for {run_id} with {len(commands)} commands")

        # Create completion protobuf
        import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2

        from temporalio_trio.worker._bridge_types import poc_to_bridge_completion

        poc_completion = WorkflowActivationCompletion(commands=commands)

        import temporalio.converter

        data_converter = temporalio.converter.DataConverter()
        bridge_completion = poc_to_bridge_completion(
            run_id, poc_completion, data_converter
        )

        completion_bytes = bridge_completion.SerializeToString()
        await self._bridge.complete_workflow_activation(completion_bytes)

    async def _send_empty_completion(self, run_id: str) -> None:
        """Send an empty success completion for cache eviction.

        Args:
            run_id: The workflow run_id.
        """
        import temporalio.bridge.proto.workflow_completion.workflow_completion_pb2 as wc

        comp = wc.WorkflowActivationCompletion()
        comp.run_id = run_id
        comp.successful.SetInParent()
        completion_bytes = comp.SerializeToString()
        await self._bridge.complete_workflow_activation(completion_bytes)

    def shutdown(self) -> None:
        """Initiate graceful shutdown of the worker."""
        logger.info("Initiating SingleThreadWorker shutdown")
        self._shutdown_event.set()
