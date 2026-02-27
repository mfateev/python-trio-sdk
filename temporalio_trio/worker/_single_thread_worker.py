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
from typing import TYPE_CHECKING, Any, NoReturn, cast

import temporalio.converter
import trio

from temporalio_trio.worker._activation import (
    ActivityResolvedJob,
    CancelWorkflowJob,
    ChildWorkflowResolvedJob,
    ChildWorkflowStartedJob,
    ChildWorkflowStartFailedJob,
    CompleteWorkflowCommand,
    ContinueAsNewCommand,
    FailWorkflowCommand,
    NotifyHasPatchJob,
    QueryWorkflowJob,
    SignalExternalResolvedJob,
    SignalWorkflowJob,
    TimerFiredJob,
    UpdateResponseCommand,
    UpdateWorkflowJob,
    WorkflowActivation,
    WorkflowActivationCompletion,
    WorkflowStartedJob,
)
from temporalio_trio.worker._interceptor import (
    ContinueAsNewInput,
    ExecuteWorkflowInput,
    HandleQueryInput,
    HandleSignalInput,
    HandleUpdateInput,
    Interceptor,
    SignalChildWorkflowInput,
    SignalExternalWorkflowInput,
    StartActivityInput,
    StartChildWorkflowInput,
    StartLocalActivityInput,
    WorkflowInboundInterceptor,
    WorkflowInterceptorClassInput,
    WorkflowOutboundInterceptor,
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
from temporalio_trio.workflow import ContinueAsNewError, _Definition, _Runtime

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
        workflow_failure_exception_types: Sequence[type[BaseException]] = [],
        interceptors: Sequence[Interceptor] = [],
        replay_mode: bool = False,
        on_eviction_hook: Callable[[str, int | None, str | None], None]
        | None = None,
        data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
        debug_mode: bool = False,
        disable_eager_activity_execution: bool = False,
    ) -> None:
        """Initialize the single-thread worker.

        Args:
            bridge: Bridge wrapper for Temporal communication.
            task_queue: Task queue name.
            workflows: List of workflow classes to register.
            activities: List of activity functions (not implemented yet).
            workflow_failure_exception_types: Exception types that cause
                workflow failure instead of task failure. Stored for
                future enforcement.
            interceptors: Interceptors for the worker.
            replay_mode: If True, use replay bridge operations
                (poll_replay_activation / complete_replay_activation).
            on_eviction_hook: Optional callback invoked on workflow eviction.
                Called as ``on_eviction_hook(run_id, reason, message)``.
            data_converter: Data converter for payload serialization.
            debug_mode: If True, enable debug mode.
            disable_eager_activity_execution: If true, will disable eager
                activity execution. Eager activity execution is an optimization
                on some servers that sends activities back to the same worker as
                the calling workflow if they can run there.
        """
        self._bridge = bridge
        self._task_queue = task_queue
        self._activities = list(activities) if activities else []
        self._workflow_failure_exception_types = list(workflow_failure_exception_types)
        self._interceptors = list(interceptors)
        self._replay_mode = replay_mode
        self._data_converter = data_converter
        self._debug_mode = debug_mode
        self._disable_eager_activity_execution = disable_eager_activity_execution
        self._on_eviction_hook = on_eviction_hook
        self._shutdown_event = trio.Event()

        # Collect workflow interceptor classes
        self._workflow_interceptor_classes: list[type[WorkflowInboundInterceptor]] = []
        interceptor_class_input = WorkflowInterceptorClassInput(
            unsafe_extern_functions={}
        )
        for interceptor in self._interceptors:
            cls = interceptor.workflow_interceptor_class(interceptor_class_input)
            if cls is not None:
                self._workflow_interceptor_classes.append(cls)

        # Register workflow definitions
        self._workflows: dict[str | None, _Definition] = {}
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
                logger.debug("Polling for next activation...")
                if self._replay_mode:
                    activation_bytes = await self._bridge.poll_replay_activation()
                else:
                    activation_bytes = await self._bridge.poll_workflow_activation()
                logger.debug(
                    f"Received activation: {len(activation_bytes) if isinstance(activation_bytes, bytes) else 'parsed'}"
                )

                # Parse the activation
                # For unit tests, we use our POC activation types directly
                # For real bridge, we would parse protobuf here
                activation = self._parse_activation(activation_bytes)

                # Dispatch to the appropriate workflow
                await self._dispatch_activation(activation)
                logger.debug("Dispatch complete, continuing poll loop")

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

        import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as wa

        from temporalio_trio.worker._bridge_types import bridge_to_poc_activation

        bridge_act = wa.WorkflowActivation()
        bridge_act.ParseFromString(activation_bytes)

        poc_act = bridge_to_poc_activation(bridge_act, self._data_converter)

        # Check if the raw protobuf had a remove_from_cache eviction job
        # (bridge_to_poc_activation tracks this via remove_from_cache flag,
        # but double-check the raw protobuf to be safe)
        for job in bridge_act.jobs:
            if job.WhichOneof("variant") == "remove_from_cache":
                poc_act.remove_from_cache = True
                rfc = job.remove_from_cache
                if poc_act.eviction_reason is None and rfc.reason:
                    poc_act.eviction_reason = int(rfc.reason)
                if poc_act.eviction_message is None and rfc.message:
                    poc_act.eviction_message = rfc.message
                break

        return poc_act

    async def _dispatch_activation(self, activation: WorkflowActivation) -> None:
        """Dispatch an activation to the appropriate workflow.

        For new workflows, creates a new workflow task.
        For existing workflows, delivers the activation to the existing task.

        Args:
            activation: The activation to dispatch.
        """
        # Extract run_id from activation
        run_id = self._extract_run_id(activation)

        job_types = [type(j).__name__ for j in activation.jobs]
        logger.debug(
            f"Dispatching activation for {run_id}: jobs={job_types}, "
            f"known={run_id in self._workflow_states}"
        )

        # Check for eviction
        if self._is_eviction(activation):
            logger.debug(f"Evicting workflow {run_id}")
            if run_id in self._workflow_states:
                del self._workflow_states[run_id]
            # Call eviction hook if set
            if self._on_eviction_hook is not None:
                self._on_eviction_hook(
                    run_id,
                    getattr(activation, "eviction_reason", None),
                    getattr(activation, "eviction_message", None),
                )
            # Send empty completion
            await self._send_empty_completion(run_id)
            logger.debug(f"Eviction complete for {run_id}, returning to poll loop")
            return

        # Check if this is a workflow start/replay
        has_workflow_start = any(
            isinstance(job, WorkflowStartedJob) for job in activation.jobs
        )

        if has_workflow_start and run_id in self._workflow_states:
            # SDK-Core sent initialize_workflow for a workflow we still have cached.
            existing_state = self._workflow_states[run_id]
            if existing_state.is_complete:
                # Workflow completed and is still in our cache. Since the state is
                # immutable after completion, we can safely use it for queries.
                # We don't need to replay - just send CompleteWorkflow to match history.
                # The query will come in a follow-up activation.
                logger.debug(
                    f"Received replay activation for completed workflow {run_id}, "
                    "using cached state (immutable after completion)"
                )
                # Send CompleteWorkflow to match history
                commands = [
                    CompleteWorkflowCommand(result=existing_state.completion_result)
                ]
                await self._send_completion(run_id, commands)
                return
            else:
                # Workflow is still running - this shouldn't happen
                logger.warning(
                    f"Cache already exists for activation with initialize job "
                    f"(run_id={run_id}), workflow still running"
                )
                # Continue processing - deliver to existing workflow

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

            # Check if workflow has completed but we're getting a follow-up activation
            # (typically a query after completion - workflow task is no longer running)
            if state.is_complete:
                # Handle queries directly - workflow task is done
                commands = self._handle_activation_for_completed_workflow(
                    state, activation
                )
            else:
                # Workflow task is still running - deliver and wait
                state.deliver_activation(activation)
                commands = await state.wait_for_commands()

        # Send completion
        await self._send_completion(run_id, commands)

        # NOTE: Do NOT delete workflow state on completion.
        # SDK-Core may send follow-up query activations after workflow completes.
        # The workflow state will be cleaned up when SDK-Core sends remove_from_cache.
        # This is critical for supporting queries on completed workflows.

    def _extract_run_id(self, activation: WorkflowActivation) -> str:
        """Extract the run_id from an activation.

        Args:
            activation: The activation to extract from.

        Returns:
            The run_id string.

        Raises:
            RuntimeError: If run_id cannot be determined.
        """
        # Use run_id from activation (set by bridge_to_poc_activation)
        if activation.run_id:
            return activation.run_id

        # Fallback for test activations that don't set run_id
        for job in activation.jobs:
            if isinstance(job, WorkflowStartedJob):
                return f"run-{id(activation)}"

        raise RuntimeError("Cannot determine run_id from activation")

    def _is_eviction(self, activation: WorkflowActivation) -> bool:
        """Check if the activation is a cache eviction.

        Args:
            activation: The activation to check.

        Returns:
            True if this is an eviction activation.
        """
        return getattr(activation, "is_eviction", False) or getattr(
            activation, "remove_from_cache", False
        )

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

        # Get workflow definition (fall back to dynamic workflow if no named match)
        defn = self._workflows.get(started_job.workflow_type)
        if defn is None:
            defn = self._workflows.get(None)
        if defn is None:
            raise RuntimeError(f"Unknown workflow type: {started_job.workflow_type}")

        # Determine randomness seed (from activation or generate)
        randomness_seed = getattr(initial_activation, "randomness_seed", None)
        if randomness_seed is None:
            randomness_seed = random.getrandbits(64)

        # Create runtime with suspension callback
        runtime = WorkflowRuntime(
            run_id=state.run_id,
            workflow_id=started_job.workflow_id or state.run_id,
            workflow_type=started_job.workflow_type,
            task_queue=self._task_queue,
            random=random.Random(randomness_seed),
            time_ns=initial_activation.timestamp_ns,
            is_replaying=getattr(initial_activation, "is_replaying", False),
            headers=started_job.headers,
            namespace=started_job.namespace,
            attempt=started_job.attempt,
            start_time_ns=started_job.start_time_ns,
            execution_timeout_ms=started_job.execution_timeout_ms,
            run_timeout_ms=started_job.run_timeout_ms,
            task_timeout_ms=started_job.task_timeout_ms,
            retry_policy_obj=started_job.retry_policy,
            continued_run_id=started_job.continued_run_id,
            cron_schedule=started_job.cron_schedule,
            parent_namespace=started_job.parent_namespace,
            parent_workflow_id=started_job.parent_workflow_id,
            parent_run_id=started_job.parent_run_id,
            root_workflow_id=started_job.root_workflow_id,
            root_run_id=started_job.root_run_id,
            raw_memo=started_job.raw_memo,
            priority=started_job.priority,
            on_suspend=state.signal_commands_ready,
            disable_eager_activity_execution=self._disable_eager_activity_execution,
        )
        state.runtime = runtime

        # Create workflow instance BEFORE applying activation
        # This allows queries in initial activation to be processed correctly
        workflow_obj = defn.cls()
        runtime.workflow_object = workflow_obj

        # Register signal and query handlers from definition
        self._register_handlers(runtime, defn, workflow_obj)

        # Build the workflow interceptor chain
        root_inbound = _WorkflowInboundImpl(runtime)
        inbound: WorkflowInboundInterceptor = root_inbound
        for interceptor_class in reversed(self._workflow_interceptor_classes):
            inbound = interceptor_class(inbound)
        inbound.init(_WorkflowOutboundImpl(runtime))
        runtime.inbound_interceptor = inbound
        runtime.outbound_interceptor = root_inbound._outbound

        # Set runtime as current (both _runtime.py and workflow.py contextvars)
        # WorkflowRuntime implements all _Runtime ABC methods directly via duck typing
        token = _Runtime.set_current(cast(_Runtime, runtime))
        legacy_token = set_current_runtime(runtime)
        try:
            # Apply initial activation jobs (now query handlers are registered)
            # Collect deferred updates and queries (need nursery/async context)
            deferred_updates: list[UpdateWorkflowJob] = []
            deferred_queries: list[QueryWorkflowJob] = []
            self._apply_activation(
                runtime,
                initial_activation,
                deferred_updates=deferred_updates,
                deferred_queries=deferred_queries,
            )

            # Run the workflow
            async with trio.open_nursery() as nursery:
                runtime.nursery = nursery

                # Apply deferred queries through interceptor chain
                for query_job in deferred_queries:
                    await self._apply_query_async(runtime, query_job)

                # Apply deferred updates now that nursery is available
                for update_job in deferred_updates:
                    await self._apply_update_async(runtime, update_job)

                # Start the main workflow coroutine (workflow object already created)
                nursery.start_soon(
                    self._execute_workflow_main,
                    runtime,
                    defn,
                    workflow_obj,
                    started_job.args,
                    state,
                )

                # Let all initial tasks (deferred update handlers, main
                # workflow) settle before signaling the first completion.
                # Temporarily disable on_suspend so that tasks calling
                # wait_condition/sleep don't trigger signal_commands_ready
                # prematurely — the coordinator handles that below.
                await self._wait_and_signal(runtime, state)

                # Process subsequent activations
                while not state.is_complete:
                    try:
                        activation = await state.wait_for_activation()
                    except trio.EndOfChannel:
                        # Shutdown
                        break

                    # Update runtime state
                    runtime.time_ns = activation.timestamp_ns
                    runtime.is_replaying = getattr(activation, "is_replaying", False)

                    # Apply activation jobs (updates and queries are deferred)
                    pending_updates: list[UpdateWorkflowJob] = []
                    pending_queries: list[QueryWorkflowJob] = []
                    self._apply_activation(
                        runtime,
                        activation,
                        deferred_updates=pending_updates,
                        deferred_queries=pending_queries,
                    )

                    # Process deferred queries through interceptor chain
                    for query_job in pending_queries:
                        await self._apply_query_async(runtime, query_job)

                    # Process deferred updates
                    for update_job in pending_updates:
                        await self._apply_update_async(runtime, update_job)

                    # Let all tasks settle, then signal commands ready
                    await self._wait_and_signal(runtime, state)

            # Nursery exited — either all tasks completed normally or
            # the scope was cancelled (workflow cancel/terminate).
            # Ensure commands are signaled so the dispatcher isn't stuck.
            if not state.commands_ready.is_set():
                state.signal_commands_ready()

        except Exception as e:
            # Workflow task failed
            runtime.commands.append(FailWorkflowCommand(exception=e))
            state.mark_complete()
            state.signal_commands_ready()

        finally:
            _Runtime.reset_current(token)
            reset_current_runtime(legacy_token)

    def _handle_activation_for_completed_workflow(
        self,
        state: WorkflowState,
        activation: WorkflowActivation,
    ) -> list[Any]:
        """Handle an activation for a workflow that has already completed.

        This is called when an activation arrives for a workflow whose task
        has already finished. This happens for queries after completion -
        SDK-Core may send query activations after the workflow completes.

        Args:
            state: The workflow state (workflow is complete, task exited).
            activation: The activation containing jobs (typically queries).

        Returns:
            List of commands to send in completion.
        """
        commands: list[Any] = []
        runtime = state.runtime

        if runtime is None:
            logger.warning(
                f"Received activation for completed workflow {state.run_id} "
                "but runtime is None"
            )
            return commands

        # Process jobs - only queries are meaningful for completed workflows
        for job in activation.jobs:
            if isinstance(job, QueryWorkflowJob):
                # Execute query handler
                handler = runtime.query_handlers.get(job.query_type)
                if handler is None:
                    commands.append(
                        QueryFailureCommand(
                            query_id=job.query_id,
                            error=ValueError(
                                f"No handler registered for query: {job.query_type}"
                            ),
                        )
                    )
                else:
                    try:
                        result = handler(*job.args)
                        commands.append(
                            QuerySuccessCommand(
                                query_id=job.query_id,
                                result=result,
                            )
                        )
                    except Exception as e:
                        commands.append(
                            QueryFailureCommand(
                                query_id=job.query_id,
                                error=e,
                            )
                        )
            else:
                logger.debug(
                    f"Ignoring non-query job for completed workflow: "
                    f"{type(job).__name__}"
                )

        return commands

    def _register_handlers(
        self,
        runtime: WorkflowRuntime,
        defn: _Definition,
        workflow_obj: Any,
    ) -> None:
        """Register signal and query handlers from workflow definition.

        This binds handlers from the definition to the workflow instance
        and registers them with the runtime.

        Args:
            runtime: The workflow runtime.
            defn: The workflow definition.
            workflow_obj: The workflow instance.
        """
        # Register signal handlers
        for signal_name, signal_defn in defn.signals.items():
            if signal_name is not None:  # Skip dynamic handler (None key)
                # Bind the method to the workflow instance
                bound_handler = signal_defn.fn.__get__(workflow_obj, type(workflow_obj))
                runtime.register_signal_handler(signal_name, bound_handler)

        # Register query handlers
        for query_name, query_defn in defn.queries.items():
            if query_name is not None:  # Skip dynamic handler (None key)
                # Bind the method to the workflow instance
                bound_handler = query_defn.fn.__get__(workflow_obj, type(workflow_obj))
                runtime.register_query_handler(query_name, bound_handler)

        # Register update handlers
        for update_name, update_defn in defn.updates.items():
            # Bind the handler method to the workflow instance
            bound_handler = update_defn.fn.__get__(workflow_obj, type(workflow_obj))
            # Bind the validator if present
            bound_validator = None
            if update_defn.validator is not None:
                bound_validator = update_defn.validator.__get__(
                    workflow_obj, type(workflow_obj)
                )
            runtime.register_update_handler(update_name, bound_handler, bound_validator)

    async def _wait_and_signal(
        self,
        runtime: WorkflowRuntime,
        state: WorkflowState,
    ) -> None:
        """Wait for all nursery tasks to settle, then signal commands ready.

        Temporarily disables ``on_suspend`` so that tasks entering
        ``wait_condition`` / ``workflow_sleep`` / ``start_activity`` during
        the wait don't trigger ``signal_commands_ready`` prematurely via
        ``on_suspend``.  The coordinator (this method) is the sole caller
        of ``signal_commands_ready`` during activation processing.

        Args:
            runtime: The workflow runtime.
            state: The workflow state for signaling.
        """
        saved = runtime.on_suspend
        runtime.on_suspend = None
        try:
            await trio.testing.wait_all_tasks_blocked()
        finally:
            runtime.on_suspend = saved

        if not state.commands_ready.is_set():
            state.signal_commands_ready()

    async def _execute_workflow_main(
        self,
        runtime: WorkflowRuntime,
        defn: _Definition,
        workflow_obj: Any,
        args: tuple[Any, ...],
        state: WorkflowState,
    ) -> None:
        """Execute the main workflow coroutine.

        Args:
            runtime: The workflow runtime.
            defn: The workflow definition.
            workflow_obj: The workflow instance.
            args: Arguments to pass to the workflow.
            state: The workflow state for signaling.
        """
        try:
            # Run the workflow through the interceptor chain
            result = await runtime.inbound_interceptor.execute_workflow(
                ExecuteWorkflowInput(
                    type=defn.cls,
                    run_fn=defn.run_fn,
                    args=args,
                    headers=runtime.headers,
                )
            )

            # Workflow completed successfully
            runtime.commands.append(CompleteWorkflowCommand(result=result))
            # Store result for potential replay (cached state is immutable after completion)
            state.completion_result = result

        except trio.Cancelled:
            # Workflow was cancelled - emit CancelWorkflowCommand
            runtime.commands.append(CancelWorkflowCommand())

        except ContinueAsNewError as e:
            # Workflow requested continue as new - apply the command
            logger.debug("Workflow requested continue as new")
            if hasattr(e, "_apply_command"):
                e._apply_command(runtime.commands)  # type: ignore[attr-defined]
            else:
                # Fallback: should not happen with proper implementation
                logger.warning("ContinueAsNewError without _apply_command method")

        except Exception as e:
            # Workflow failed
            runtime.commands.append(FailWorkflowCommand(exception=e))

        finally:
            # Notify condition waiters so that update handlers waiting on
            # workflow state (e.g., wait_condition(lambda: self.returned))
            # can observe the completion and finalize their results.
            runtime.notify_condition_waiters()
            # Mark state as complete. Do NOT call signal_commands_ready here;
            # the coordinator (_wait_and_signal) handles signaling after all
            # concurrent tasks (update handlers) have settled.
            state.mark_complete()

    def _apply_activation(
        self,
        runtime: WorkflowRuntime,
        activation: WorkflowActivation,
        *,
        deferred_updates: list[UpdateWorkflowJob] | None = None,
        deferred_queries: list[QueryWorkflowJob] | None = None,
    ) -> None:
        """Apply activation jobs to the runtime.

        This processes jobs in the activation and updates runtime state,
        waking up any suspended workflows as needed.

        Args:
            runtime: The workflow runtime.
            activation: The activation containing jobs.
            deferred_updates: If provided, collect update jobs here instead of
                processing them immediately. Used when nursery isn't set up yet.
            deferred_queries: If provided, collect query jobs here for async
                processing through the interceptor chain.
        """
        for job in activation.jobs:
            if isinstance(job, TimerFiredJob):
                runtime.apply_timer_fired(job.timer_id, activation.timestamp_ns)
            elif isinstance(job, ActivityResolvedJob):
                runtime.apply_activity_resolved(
                    seq=job.seq,
                    result=job.result,
                    error=job.failure,
                    backoff=job.backoff,
                )
            elif isinstance(job, SignalWorkflowJob):
                self._apply_signal(runtime, job)
            elif isinstance(job, QueryWorkflowJob):
                if deferred_queries is not None:
                    deferred_queries.append(job)
                else:
                    self._apply_query(runtime, job)
            elif isinstance(job, ChildWorkflowStartedJob):
                runtime.apply_child_workflow_started(
                    seq=job.seq,
                    run_id=job.run_id,
                )
            elif isinstance(job, ChildWorkflowStartFailedJob):
                runtime.apply_child_workflow_start_failed(
                    seq=job.seq,
                    workflow_id=job.workflow_id,
                    workflow_type=job.workflow_type,
                    cause=job.cause,
                )
            elif isinstance(job, ChildWorkflowResolvedJob):
                runtime.apply_child_workflow_resolved(
                    seq=job.seq,
                    result=job.result,
                    error=job.failure,
                )
            elif isinstance(job, SignalExternalResolvedJob):
                runtime.apply_signal_external_resolved(
                    seq=job.seq,
                    error=job.failure,
                )
            elif isinstance(job, UpdateWorkflowJob):
                # Always defer updates - they need to be awaited (for async handlers)
                if deferred_updates is not None:
                    deferred_updates.append(job)
                # If no deferred_updates list provided, silently skip
                # (updates are handled by the caller)
            elif isinstance(job, CancelWorkflowJob):
                runtime.apply_cancel_workflow()
            elif isinstance(job, NotifyHasPatchJob):
                runtime.apply_notify_has_patch(job.patch_id)

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
                    result.close()
        except Exception as e:
            logger.warning(f"Signal handler error for {signal_job.signal_name}: {e}")

        # Notify any wait_condition waiters that state may have changed
        runtime.notify_condition_waiters()

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

    async def _apply_update_async(
        self, runtime: WorkflowRuntime, update_job: UpdateWorkflowJob
    ) -> None:
        """Apply an update through the interceptor chain.

        This ensures:
        - The accepted command is always in the current activation's completion.
        - Sync handler's completed command is also in the current activation.
        - Async handler's completed command arrives in any subsequent activation's
          completion (via runtime.commands, picked up by signal_commands_ready).

        Args:
            runtime: The workflow runtime.
            update_job: The update job.
        """
        from temporalio_trio.workflow import (
            UpdateInfo,
            _current_update_info,
            _set_current_update_info,
        )

        _set_current_update_info(UpdateInfo(id=update_job.id, name=update_job.name))
        try:
            # Look up handler
            handler = runtime.update_handlers.get(update_job.name)
            if handler is None:
                handler = runtime.update_handlers.get(None)
            if handler is None:
                runtime.commands.append(
                    UpdateResponseCommand(
                        protocol_instance_id=update_job.protocol_instance_id,
                        rejected_failure=RuntimeError(
                            f"Update handler for '{update_job.name}' expected but not found, "
                            f"and there is no dynamic handler."
                        ),
                    )
                )
                return

            update_input = HandleUpdateInput(
                id=update_job.id,
                update=update_job.name,
                args=update_job.args,
                headers={},
            )

            # Run validator through interceptor chain if requested
            if update_job.run_validator:
                validator = runtime.update_validators.get(update_job.name)
                if validator is None:
                    validator = runtime.update_validators.get(None)
                if validator is not None:
                    try:
                        runtime.inbound_interceptor.handle_update_validator(
                            update_input
                        )
                    except Exception as e:
                        runtime.commands.append(
                            UpdateResponseCommand(
                                protocol_instance_id=update_job.protocol_instance_id,
                                rejected_failure=e,
                            )
                        )
                        return

            # Accept
            runtime.commands.append(
                UpdateResponseCommand(
                    protocol_instance_id=update_job.protocol_instance_id,
                    accepted=True,
                )
            )

            # Track in-progress
            runtime.in_progress_updates[update_job.id] = update_job.name

            # Run handler
            try:
                result = handler(*update_job.args)
                if inspect.iscoroutine(result):
                    # Async handler — spawn as nursery task
                    if runtime.nursery is not None:
                        runtime.nursery.start_soon(
                            self._run_update_handler_wrapper,
                            result,
                            update_job,
                            runtime,
                        )
                    else:
                        logger.warning(
                            f"Cannot run async update handler {update_job.name}: "
                            "no nursery available"
                        )
                        result.close()
                        runtime.commands.append(
                            UpdateResponseCommand(
                                protocol_instance_id=update_job.protocol_instance_id,
                                completed_result=None,
                                _is_completed=True,
                            )
                        )
                        runtime.in_progress_updates.pop(update_job.id, None)
                else:
                    # Sync handler — emit completed response immediately
                    runtime.commands.append(
                        UpdateResponseCommand(
                            protocol_instance_id=update_job.protocol_instance_id,
                            completed_result=result,
                            _is_completed=True,
                        )
                    )
                    runtime.in_progress_updates.pop(update_job.id, None)
            except Exception as e:
                runtime.commands.append(
                    UpdateResponseCommand(
                        protocol_instance_id=update_job.protocol_instance_id,
                        rejected_failure=e,
                    )
                )
                runtime.in_progress_updates.pop(update_job.id, None)

            # Notify any wait_condition waiters that state may have changed
            runtime.notify_condition_waiters()
        finally:
            _current_update_info.set(None)

    def _apply_update(
        self, runtime: WorkflowRuntime, update_job: UpdateWorkflowJob
    ) -> None:
        """Deliver an update to the workflow.

        The update protocol has multiple phases:
        1. Run validator (if requested) in read-only context
        2. Accept the update (emit accepted response)
        3. Run the handler (sync or async)
        4. Emit completed/rejected response

        Args:
            runtime: The workflow runtime.
            update_job: The update job containing update info and args.
        """
        from temporalio_trio.workflow import UpdateInfo, _set_current_update_info

        # Set current update info
        _set_current_update_info(UpdateInfo(id=update_job.id, name=update_job.name))

        # Look up handler by name, fall back to dynamic (None key)
        handler = runtime.update_handlers.get(update_job.name)
        if handler is None:
            handler = runtime.update_handlers.get(None)
        if handler is None:
            # No handler - reject
            runtime.commands.append(
                UpdateResponseCommand(
                    protocol_instance_id=update_job.protocol_instance_id,
                    rejected_failure=RuntimeError(
                        f"Update handler for '{update_job.name}' expected but not found, "
                        f"and there is no dynamic handler."
                    ),
                )
            )
            return

        # Run validator if requested
        if update_job.run_validator:
            validator = runtime.update_validators.get(update_job.name)
            if validator is None:
                validator = runtime.update_validators.get(None)
            if validator is not None:
                try:
                    validator(*update_job.args)
                except Exception as e:
                    # Validator rejected - emit rejected response
                    runtime.commands.append(
                        UpdateResponseCommand(
                            protocol_instance_id=update_job.protocol_instance_id,
                            rejected_failure=e,
                        )
                    )
                    return

        # Accept the update
        runtime.commands.append(
            UpdateResponseCommand(
                protocol_instance_id=update_job.protocol_instance_id,
                accepted=True,
            )
        )

        # Track in-progress
        runtime.in_progress_updates[update_job.id] = update_job.name

        # Run the handler
        try:
            result = handler(*update_job.args)
            if inspect.iscoroutine(result):
                # Async handler - run as nursery task
                if runtime.nursery is not None:
                    runtime.nursery.start_soon(
                        self._run_update_handler_wrapper,
                        result,
                        update_job,
                        runtime,
                    )
                else:
                    # No nursery available - close coroutine and complete with None
                    result.close()
                    runtime.commands.append(
                        UpdateResponseCommand(
                            protocol_instance_id=update_job.protocol_instance_id,
                            completed_result=None,
                            _is_completed=True,
                        )
                    )
                    runtime.in_progress_updates.pop(update_job.id, None)
            else:
                # Sync handler - emit completed response immediately
                runtime.commands.append(
                    UpdateResponseCommand(
                        protocol_instance_id=update_job.protocol_instance_id,
                        completed_result=result,
                        _is_completed=True,
                    )
                )
                runtime.in_progress_updates.pop(update_job.id, None)
        except Exception as e:
            # Handler failed - emit rejected response
            runtime.commands.append(
                UpdateResponseCommand(
                    protocol_instance_id=update_job.protocol_instance_id,
                    rejected_failure=e,
                )
            )
            runtime.in_progress_updates.pop(update_job.id, None)

        # Notify any wait_condition waiters that state may have changed
        runtime.notify_condition_waiters()

    async def _run_update_handler_wrapper(
        self,
        coro: Any,
        update_job: UpdateWorkflowJob,
        runtime: WorkflowRuntime,
    ) -> None:
        """Run an async update handler coroutine as a nursery task.

        This wrapper awaits the handler coroutine, emits the completion response,
        and cleans up in-progress tracking. The completed/rejected command is
        added to runtime.commands and picked up by the _run_workflow loop's
        existing trio.sleep(0) + signal_commands_ready() pattern:

        - If the handler completes during trio.sleep(0), the commands are included
          in the current activation's completion.
        - If the handler completes between activations (because an activation event
          woke it), the commands are included in that activation's completion.

        Args:
            coro: The coroutine to await.
            update_job: The update job (for protocol_instance_id and id).
            runtime: The workflow runtime.
        """
        from temporalio_trio.workflow import (
            UpdateInfo,
            _current_update_info,
            _set_current_update_info,
        )

        # Set current update info for this task context
        _set_current_update_info(UpdateInfo(id=update_job.id, name=update_job.name))

        try:
            result = await coro
            runtime.commands.append(
                UpdateResponseCommand(
                    protocol_instance_id=update_job.protocol_instance_id,
                    completed_result=result,
                    _is_completed=True,
                )
            )
        except Exception as e:
            logger.warning(f"Async update handler error for {update_job.name}: {e}")
            runtime.commands.append(
                UpdateResponseCommand(
                    protocol_instance_id=update_job.protocol_instance_id,
                    rejected_failure=e,
                )
            )
        finally:
            runtime.in_progress_updates.pop(update_job.id, None)
            runtime.notify_condition_waiters()
            _current_update_info.set(None)

    def _apply_query(
        self, runtime: WorkflowRuntime, query_job: QueryWorkflowJob
    ) -> None:
        """Execute a query directly (without interceptor chain).

        Used for activations on completed workflows where the nursery
        is not available. For normal query processing through interceptors,
        see _apply_query_async.

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

        prev_read_only = runtime._read_only
        runtime._read_only = True
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
        finally:
            runtime._read_only = prev_read_only

    async def _apply_query_async(
        self, runtime: WorkflowRuntime, query_job: QueryWorkflowJob
    ) -> None:
        """Execute a query through the interceptor chain.

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

        prev_read_only = runtime._read_only
        runtime._read_only = True
        try:
            result = await runtime.inbound_interceptor.handle_query(
                HandleQueryInput(
                    id=query_job.query_id,
                    query=query_job.query_type,
                    args=query_job.args,
                    headers={},
                )
            )
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
        finally:
            runtime._read_only = prev_read_only

    async def _send_completion(self, run_id: str, commands: list[Any]) -> None:
        """Send a completion with commands to the bridge.

        Args:
            run_id: The workflow run_id.
            commands: List of commands to send.
        """
        logger.debug(f"Sending completion for {run_id} with {len(commands)} commands")

        from temporalio_trio.worker._bridge_types import poc_to_bridge_completion

        poc_completion = WorkflowActivationCompletion(commands=commands)

        bridge_completion = poc_to_bridge_completion(
            run_id, poc_completion, self._data_converter
        )

        completion_bytes = bridge_completion.SerializeToString()
        if self._replay_mode:
            await self._bridge.complete_replay_activation(completion_bytes)
        else:
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
        if self._replay_mode:
            await self._bridge.complete_replay_activation(completion_bytes)
        else:
            await self._bridge.complete_workflow_activation(completion_bytes)

    def shutdown(self) -> None:
        """Initiate graceful shutdown of the worker."""
        logger.info("Initiating SingleThreadWorker shutdown")
        self._shutdown_event.set()


class _WorkflowInboundImpl(WorkflowInboundInterceptor):
    """Terminal inbound interceptor that routes to the workflow runtime."""

    def __init__(self, runtime: WorkflowRuntime) -> None:
        # We intentionally don't call super().__init__ - this is the terminal
        self._runtime = runtime
        self._outbound: WorkflowOutboundInterceptor | None = None

    def init(self, outbound: WorkflowOutboundInterceptor) -> None:
        self._outbound = outbound

    async def execute_workflow(self, input: ExecuteWorkflowInput) -> Any:
        # Call the run function as an unbound method with the workflow object
        return await input.run_fn(self._runtime.workflow_object, *input.args)

    async def handle_signal(self, input: HandleSignalInput) -> None:
        handler = self._runtime.signal_handlers.get(input.signal)
        if handler is None:
            logger.warning(f"No handler registered for signal: {input.signal}")
            return
        result = handler(*input.args)
        if inspect.iscoroutine(result):
            await result

    async def handle_query(self, input: HandleQueryInput) -> Any:
        handler = self._runtime.query_handlers.get(input.query)
        if handler is None:
            raise ValueError(f"No handler registered for query: {input.query}")
        return handler(*input.args)

    def handle_update_validator(self, input: HandleUpdateInput) -> None:
        validator = self._runtime.update_validators.get(input.update)
        if validator is None:
            validator = self._runtime.update_validators.get(None)
        if validator is not None:
            validator(*input.args)

    async def handle_update_handler(self, input: HandleUpdateInput) -> Any:
        handler = self._runtime.update_handlers.get(input.update)
        if handler is None:
            handler = self._runtime.update_handlers.get(None)
        if handler is None:
            raise RuntimeError(
                f"Update handler for '{input.update}' expected but not found, "
                f"and there is no dynamic handler."
            )
        result = handler(*input.args)
        if inspect.iscoroutine(result):
            result = await result
        return result


class _WorkflowOutboundImpl(WorkflowOutboundInterceptor):
    """Terminal outbound interceptor that delegates to the workflow runtime."""

    def __init__(self, runtime: WorkflowRuntime) -> None:
        # We intentionally don't call super().__init__ - this is the terminal
        self._runtime = runtime

    def continue_as_new(self, input: ContinueAsNewInput) -> NoReturn:
        self._runtime.workflow_continue_as_new(
            *input.args,
            workflow=input.workflow,
            task_queue=input.task_queue,
            run_timeout=input.run_timeout,
            task_timeout=input.task_timeout,
            retry_policy=input.retry_policy,
            memo=input.memo,
            search_attributes=input.search_attributes,  # type: ignore[arg-type]
        )

    def info(self) -> Any:
        return self._runtime.workflow_info()

    async def signal_child_workflow(self, input: SignalChildWorkflowInput) -> None:
        await self._runtime.workflow_signal_external_workflow(
            input.child_workflow_id,
            input.signal,
            input.args,
            run_id=None,
        )

    async def signal_external_workflow(
        self, input: SignalExternalWorkflowInput
    ) -> None:
        await self._runtime.workflow_signal_external_workflow(
            input.workflow_id,
            input.signal,
            input.args,
            run_id=input.workflow_run_id,
        )

    def start_activity(self, input: StartActivityInput) -> Any:
        return self._runtime._outbound_schedule_activity(input)

    async def start_child_workflow(self, input: StartChildWorkflowInput) -> Any:
        from temporalio_trio.workflow import (
            ChildWorkflowCancellationType,
            ParentClosePolicy,
        )

        return await self._runtime.workflow_start_child_workflow(
            input.workflow,
            *input.args,
            id=input.id,
            task_queue=input.task_queue,
            cancellation_type=ChildWorkflowCancellationType(input.cancellation_type),
            parent_close_policy=ParentClosePolicy(input.parent_close_policy),
            execution_timeout=input.execution_timeout,
            run_timeout=input.run_timeout,
            task_timeout=input.task_timeout,
            id_reuse_policy=input.id_reuse_policy,
            retry_policy=input.retry_policy,
            cron_schedule=input.cron_schedule,
            memo=input.memo,
            search_attributes=input.search_attributes,  # type: ignore[arg-type]
        )

    def start_local_activity(self, input: StartLocalActivityInput) -> Any:
        return self._runtime._outbound_schedule_local_activity(input)
