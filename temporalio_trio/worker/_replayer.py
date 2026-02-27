"""Replayer for replaying workflows from history."""

from __future__ import annotations

import logging
from collections.abc import Callable, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Mapping,
    Optional,
    Type,
)

import temporalio.api.history.v1
import temporalio.converter
import trio
from typing_extensions import TypedDict

from temporalio_trio._async_bridge import TrioBridgeWrapper
from temporalio_trio.client._workflow_handle import WorkflowHistory
from temporalio_trio.worker._interceptor import Interceptor
from temporalio_trio.worker._single_thread_worker import SingleThreadWorker
from temporalio_trio.workflow import NondeterminismError, _Definition

logger = logging.getLogger(__name__)

# Eviction reason constants derived from protobuf enum
import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as _wa_pb2

_EvictionReason = _wa_pb2.RemoveFromCache.EvictionReason
_EVICTION_REASON_NONDETERMINISM = int(_EvictionReason.NONDETERMINISM)
_EVICTION_REASON_CACHE_FULL = int(_EvictionReason.CACHE_FULL)
_EVICTION_REASON_LANG_REQUESTED = int(_EvictionReason.LANG_REQUESTED)


class Replayer:
    """Replayer to replay workflows from history."""

    def __init__(
        self,
        *,
        workflows: Sequence[Type],
        namespace: str = "ReplayNamespace",
        data_converter: temporalio.converter.DataConverter = temporalio.converter.DataConverter.default,
        interceptors: Sequence[Interceptor] = [],
        build_id: Optional[str] = None,
        identity: Optional[str] = None,
        workflow_failure_exception_types: Sequence[Type[BaseException]] = [],
        debug_mode: bool = False,
        disable_safe_workflow_eviction: bool = False,
    ) -> None:
        """Create a replayer to replay workflows from history.

        Args:
            workflows: Workflow classes to register for replay.
            namespace: Namespace for replay (default: "ReplayNamespace").
            data_converter: Data converter for payload serialization.
            interceptors: Interceptors for the worker.
            build_id: Build identifier for worker versioning.
            identity: Worker identity.
            workflow_failure_exception_types: Exception types that cause
                workflow failure instead of task failure.
            debug_mode: If True, enable debug mode.
            disable_safe_workflow_eviction: If True, disable safe workflow
                eviction. This should generally only be set true if a user
                is having non-determinism issues that are actually
                deterministic.
        """
        if not workflows:
            raise ValueError("At least one workflow must be specified")
        self._config = ReplayerConfig(
            workflows=list(workflows),
            namespace=namespace,
            data_converter=data_converter,
            interceptors=interceptors,
            build_id=build_id,
            identity=identity,
            workflow_failure_exception_types=workflow_failure_exception_types,
            debug_mode=debug_mode,
            disable_safe_workflow_eviction=disable_safe_workflow_eviction,
        )

    def config(self) -> ReplayerConfig:
        """Config, as a dictionary, used to create this replayer.

        Returns:
            Configuration, shallow-copied.
        """
        config = ReplayerConfig(**self._config)  # type: ignore[arg-type]
        config["workflows"] = list(config["workflows"])
        return config

    async def replay_workflow(
        self,
        history: WorkflowHistory,
        *,
        raise_on_replay_failure: bool = True,
    ) -> WorkflowReplayResult:
        """Replay a workflow for the given history.

        Args:
            history: The history to replay. Can be fetched directly, or use
                :py:meth:`WorkflowHistory.from_json` to parse a history
                downloaded via tctl or the web UI.
            raise_on_replay_failure: If True (the default), this will raise
                a replay failure if one is present.

        Returns:
            The replay result.
        """

        async def history_iterator() -> AsyncIterator[WorkflowHistory]:
            yield history

        async with self.workflow_replay_iterator(history_iterator()) as replay_iterator:
            async for result in replay_iterator:
                if raise_on_replay_failure and result.replay_failure:
                    raise result.replay_failure
                return result
            raise RuntimeError("No histories")

    async def replay_workflows(
        self,
        histories: AsyncIterator[WorkflowHistory],
        *,
        raise_on_replay_failure: bool = True,
    ) -> WorkflowReplayResults:
        """Replay workflows for the given histories.

        Args:
            histories: The histories to replay, from an async iterator.
            raise_on_replay_failure: If True (the default), this will raise
                the first replay failure seen.

        Returns:
            Aggregated results.
        """
        async with self.workflow_replay_iterator(histories) as replay_iterator:
            replay_failures: dict[str, Exception] = {}
            async for result in replay_iterator:
                if result.replay_failure:
                    if raise_on_replay_failure:
                        raise result.replay_failure
                    replay_failures[result.history.run_id] = result.replay_failure
            return WorkflowReplayResults(replay_failures=replay_failures)

    @asynccontextmanager
    async def workflow_replay_iterator(
        self, histories: AsyncIterator[WorkflowHistory]
    ) -> AsyncIterator[AsyncIterator[WorkflowReplayResult]]:
        """Replay workflows for the given histories.

        This is a context manager for use via ``async with``. The value is an
        iterator for use via ``async for``.

        Args:
            histories: The histories to replay, from an async iterator.

        Returns:
            An async iterator that returns replayed workflow results as they
            are replayed.
        """
        bridge = TrioBridgeWrapper()
        await bridge.start()

        last_replay_failure: Optional[Exception] = None
        last_replay_complete = trio.Event()
        worker_failed = trio.Event()
        worker_error: Optional[BaseException] = None

        def on_eviction_hook(
            run_id: str,
            reason: int | None,
            message: str | None,
        ) -> None:
            nonlocal last_replay_failure, last_replay_complete
            if reason == _EVICTION_REASON_NONDETERMINISM:
                last_replay_failure = NondeterminismError(message or "")
            elif (
                reason != _EVICTION_REASON_CACHE_FULL
                and reason != _EVICTION_REASON_LANG_REQUESTED
            ):
                last_replay_failure = RuntimeError(f"{reason}: {message}")
            else:
                last_replay_failure = None
            last_replay_complete.set()

        task_queue = f"replay-{self._config.get('build_id')}"

        # Compute nondeterminism-as-workflow-fail flags
        wf_fail_types = self._config.get("workflow_failure_exception_types", [])
        nondeterminism_as_wf_fail = any(
            issubclass(NondeterminismError, typ) for typ in wf_fail_types
        )
        # Per-workflow types: check each registered workflow's
        # failure_exception_types from its definition
        nondeterminism_as_wf_fail_for_types: set[str] = set()
        for wf_cls in self._config["workflows"]:
            defn = _Definition.must_from_class(wf_cls)
            if defn.name and any(
                issubclass(NondeterminismError, typ)
                for typ in defn.failure_exception_types
            ):
                nondeterminism_as_wf_fail_for_types.add(defn.name)

        try:
            # Initialize the replay worker in the bridge
            await bridge.initialize_replay_worker(
                namespace=self._config["namespace"],
                task_queue=task_queue,
                build_id=self._config.get("build_id"),
                identity=self._config.get("identity"),
                nondeterminism_as_workflow_fail=nondeterminism_as_wf_fail,
                nondeterminism_as_workflow_fail_for_types=nondeterminism_as_wf_fail_for_types,
            )

            # Create SingleThreadWorker in replay mode
            worker = SingleThreadWorker(
                bridge=bridge,
                task_queue=task_queue,
                workflows=self._config["workflows"],
                workflow_failure_exception_types=self._config[
                    "workflow_failure_exception_types"
                ],
                interceptors=self._config["interceptors"],
                replay_mode=True,
                on_eviction_hook=on_eviction_hook,
                data_converter=self._config["data_converter"],
                debug_mode=self._config["debug_mode"],
            )

            # We capture exceptions from the user's iteration and re-raise
            # them outside the nursery to avoid ExceptionGroup wrapping.
            captured_exception: Optional[BaseException] = None

            # Run worker in nursery
            async with trio.open_nursery() as nursery:
                worker_task_scope = trio.CancelScope()

                async def run_worker() -> None:
                    nonlocal worker_error
                    try:
                        with worker_task_scope:
                            await worker.run()
                    except BaseException as e:
                        worker_error = e
                        worker_failed.set()

                nursery.start_soon(run_worker)

                async def replay_iterator() -> AsyncIterator[WorkflowReplayResult]:
                    nonlocal last_replay_failure, last_replay_complete
                    async for history in histories:
                        # Clear last complete and push history
                        last_replay_complete = trio.Event()
                        last_replay_failure = None

                        # Serialize history as protobuf and push
                        history_proto = temporalio.api.history.v1.History(
                            events=history.events
                        )
                        history_bytes = history_proto.SerializeToString()

                        await bridge.push_replay_history(
                            history.workflow_id, history_bytes
                        )

                        # Wait for eviction event (replay complete) or worker failure
                        async with trio.open_nursery() as race_nursery:

                            async def wait_complete() -> None:
                                await last_replay_complete.wait()
                                race_nursery.cancel_scope.cancel()

                            async def wait_failed() -> None:
                                await worker_failed.wait()
                                race_nursery.cancel_scope.cancel()

                            race_nursery.start_soon(wait_complete)
                            race_nursery.start_soon(wait_failed)

                        if worker_failed.is_set() and worker_error is not None:
                            raise worker_error

                        yield WorkflowReplayResult(
                            history=history,
                            replay_failure=last_replay_failure,
                        )

                try:
                    yield replay_iterator()
                except BaseException as e:
                    captured_exception = e
                finally:
                    # Close the pusher to signal no more histories
                    try:
                        await bridge.close_replay_pusher()
                    except Exception:
                        logger.debug(
                            "Failed to close replay pusher",
                            exc_info=True,
                        )

                    # Shutdown the worker
                    worker.shutdown()
                    worker_task_scope.cancel()

                    # Cancel nursery to clean up
                    nursery.cancel_scope.cancel()

            # Re-raise outside the nursery to avoid ExceptionGroup
            if captured_exception is not None:
                raise captured_exception

        finally:
            # Shutdown the replay bridge
            try:
                await bridge.initiate_replay_shutdown()
                await bridge.finalize_replay_shutdown()
            except Exception:
                logger.debug("Failed to shutdown replay bridge", exc_info=True)

            # Shutdown the bridge itself
            try:
                await bridge.shutdown()
            except Exception:
                logger.debug("Failed to shutdown bridge", exc_info=True)


class ReplayerConfig(TypedDict, total=False):
    """TypedDict of config originally passed to :py:class:`Replayer`."""

    workflows: Sequence[Type]
    namespace: str
    data_converter: temporalio.converter.DataConverter
    interceptors: Sequence[Interceptor]
    build_id: Optional[str]
    identity: Optional[str]
    workflow_failure_exception_types: Sequence[Type[BaseException]]
    debug_mode: bool
    disable_safe_workflow_eviction: bool


@dataclass(frozen=True)
class WorkflowReplayResult:
    """Single workflow replay result."""

    history: WorkflowHistory
    """History originally passed for this workflow replay."""

    replay_failure: Optional[Exception]
    """Failure during replay if any.

    This does not mean your workflow exited by raising an error, but rather that
    some task failure such as
    :py:class:`temporalio_trio.workflow.NondeterminismError` was encountered
    during replay - likely indicating your workflow code is incompatible with
    the history.
    """


@dataclass(frozen=True)
class WorkflowReplayResults:
    """Results of replaying multiple workflows."""

    replay_failures: Mapping[str, Exception]
    """Replay failures, keyed by run ID."""
