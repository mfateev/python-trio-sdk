"""Tests for parallel workflow execution isolation (Phase 5).

These tests verify that multiple workflows can run concurrently without
interfering with each other, which is essential for Temporal's scalability.
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

import pytest

from temporalio_trio import workflow
from temporalio_trio.worker import (
    CompleteWorkflowCommand,
    StartTimerCommand,
    TimerFiredJob,
    TrioWorkflowRunner,
    WorkflowActivation,
    WorkflowInstanceDetails,
    WorkflowStartedJob,
)


# Test workflows for parallel execution
@workflow.defn
class CountingWorkflow:
    """Workflow that counts and sleeps."""

    @workflow.run
    async def run(self, workflow_id: str, iterations: int) -> dict[str, object]:
        times = []
        for i in range(iterations):
            times.append(workflow.time())
            await workflow.sleep(1)
        times.append(workflow.time())

        return {
            "workflow_id": workflow_id,
            "iterations": iterations,
            "times": times,
            "final_time": workflow.time(),
        }


@workflow.defn
class InfoCapturingWorkflow:
    """Workflow that captures its info at different points."""

    @workflow.run
    async def run(self) -> dict[str, str]:
        info_start = workflow.info()
        await workflow.sleep(5)
        info_end = workflow.info()

        return {
            "workflow_id": info_start.workflow_id,
            "run_id": info_start.run_id,
            "workflow_id_end": info_end.workflow_id,
            "run_id_end": info_end.run_id,
        }


def _create_details(
    workflow_cls: type,
    workflow_id: str,
    run_id: str = "run-1",
    randomness_seed: int = 42,
) -> WorkflowInstanceDetails:
    """Helper to create WorkflowInstanceDetails."""
    defn = workflow._Definition.must_from_class(workflow_cls)
    info = workflow.Info(
        workflow_id=workflow_id,
        workflow_type=defn.name,
        run_id=run_id,
        task_queue="test-queue",
        namespace="default",
        attempt=1,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    return WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=randomness_seed,
    )


def _execute_workflow_to_completion(
    runner: TrioWorkflowRunner,
    details: WorkflowInstanceDetails,
    args: tuple = (),
) -> object:
    """Execute a workflow to completion and return the result."""
    instance = runner.create_instance(details)
    current_time_ns = 0

    # Start workflow
    completion = instance.activate(
        WorkflowActivation(
            jobs=[WorkflowStartedJob(workflow_type=details.defn.name, args=args)],
            timestamp_ns=current_time_ns,
        )
    )

    # Process timer commands until complete
    while not any(isinstance(c, CompleteWorkflowCommand) for c in completion.commands):
        timer_cmds = [
            c for c in completion.commands if isinstance(c, StartTimerCommand)
        ]
        if not timer_cmds:
            break

        # Advance time and fire timer
        timer_cmd = timer_cmds[0]
        current_time_ns += timer_cmd.duration_ms * 1_000_000

        completion = instance.activate(
            WorkflowActivation(
                jobs=[TimerFiredJob(timer_id=timer_cmd.timer_id)],
                timestamp_ns=current_time_ns,
            )
        )

    # Get result
    result_cmd = next(
        (c for c in completion.commands if isinstance(c, CompleteWorkflowCommand)),
        None,
    )
    return result_cmd.result if result_cmd else None


def _run_workflow_in_thread(
    workflow_id: str,
    iterations: int,
    seed: int,
) -> dict[str, object]:
    """Run a CountingWorkflow in a separate thread.

    Each thread creates its own runner and instance to simulate
    parallel workflow execution.
    """
    runner = TrioWorkflowRunner()
    defn = workflow._Definition.must_from_class(CountingWorkflow)
    runner.prepare_workflow(defn)

    details = _create_details(
        CountingWorkflow,
        workflow_id=workflow_id,
        randomness_seed=seed,
    )

    result = _execute_workflow_to_completion(
        runner, details, args=(workflow_id, iterations)
    )
    return result  # type: ignore


def _run_info_workflow_in_thread(
    workflow_id: str,
    run_id: str,
    seed: int,
) -> dict[str, str]:
    """Run an InfoCapturingWorkflow in a separate thread."""
    runner = TrioWorkflowRunner()
    defn = workflow._Definition.must_from_class(InfoCapturingWorkflow)
    runner.prepare_workflow(defn)

    details = _create_details(
        InfoCapturingWorkflow,
        workflow_id=workflow_id,
        run_id=run_id,
        randomness_seed=seed,
    )

    result = _execute_workflow_to_completion(runner, details)
    return result  # type: ignore


class TestParallelWorkflowIsolation:
    """Tests for parallel workflow isolation."""

    def test_parallel_workflows_complete_independently(self) -> None:
        """Test that parallel workflows all complete without errors."""
        num_workflows = 4

        with ThreadPoolExecutor(max_workers=num_workflows) as executor:
            futures = [
                executor.submit(
                    _run_workflow_in_thread,
                    workflow_id=f"parallel-wf-{i}",
                    iterations=3,
                    seed=i * 1000,
                )
                for i in range(num_workflows)
            ]
            results = [f.result() for f in as_completed(futures)]

        # All workflows should complete
        assert len(results) == num_workflows

        # Each should have correct structure
        for result in results:
            assert "workflow_id" in result
            assert "iterations" in result
            assert "times" in result
            assert result["iterations"] == 3

    def test_parallel_workflows_have_isolated_ids(self) -> None:
        """Test that parallel workflows maintain their own IDs."""
        num_workflows = 4

        with ThreadPoolExecutor(max_workers=num_workflows) as executor:
            futures = {
                executor.submit(
                    _run_workflow_in_thread,
                    workflow_id=f"isolated-wf-{i}",
                    iterations=2,
                    seed=i,
                ): i
                for i in range(num_workflows)
            }
            results = {}
            for future in as_completed(futures):
                idx = futures[future]
                results[idx] = future.result()

        # Each workflow should report its own ID
        for i in range(num_workflows):
            assert results[i]["workflow_id"] == f"isolated-wf-{i}"

    def test_parallel_workflows_have_isolated_time(self) -> None:
        """Test that parallel workflows have isolated time progression."""
        # Run workflows with different iteration counts
        configs = [
            ("time-wf-0", 2),  # 2 iterations = 2 seconds
            ("time-wf-1", 4),  # 4 iterations = 4 seconds
            ("time-wf-2", 1),  # 1 iteration = 1 second
            ("time-wf-3", 3),  # 3 iterations = 3 seconds
        ]

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(
                    _run_workflow_in_thread,
                    workflow_id=wf_id,
                    iterations=iters,
                    seed=i,
                ): (wf_id, iters)
                for i, (wf_id, iters) in enumerate(configs)
            }
            results = {}
            for future in as_completed(futures):
                wf_id, iters = futures[future]
                results[wf_id] = future.result()

        # Each workflow should have its own final time based on iterations
        for wf_id, iters in configs:
            result = results[wf_id]
            # Final time should be iterations seconds (each sleep is 1s)
            assert result["final_time"] == float(iters)

    def test_parallel_workflows_info_isolation(self) -> None:
        """Test that workflow.info() returns correct info in parallel execution."""
        configs = [
            ("info-wf-0", "run-a"),
            ("info-wf-1", "run-b"),
            ("info-wf-2", "run-c"),
            ("info-wf-3", "run-d"),
        ]

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = {
                executor.submit(
                    _run_info_workflow_in_thread,
                    workflow_id=wf_id,
                    run_id=run_id,
                    seed=i,
                ): (wf_id, run_id)
                for i, (wf_id, run_id) in enumerate(configs)
            }
            results = {}
            for future in as_completed(futures):
                wf_id, run_id = futures[future]
                results[wf_id] = future.result()

        # Each workflow should have captured its own info
        for wf_id, run_id in configs:
            result = results[wf_id]
            assert result["workflow_id"] == wf_id
            assert result["run_id"] == run_id
            # Info should be consistent within workflow
            assert result["workflow_id_end"] == wf_id
            assert result["run_id_end"] == run_id


class TestSameSeedDeterminism:
    """Tests for determinism with same seed."""

    def test_same_seed_produces_same_result(self) -> None:
        """Test that same seed produces identical results."""
        seed = 12345

        # Run same workflow twice with same seed
        result1 = _run_workflow_in_thread("seed-test-1", iterations=3, seed=seed)
        result2 = _run_workflow_in_thread("seed-test-2", iterations=3, seed=seed)

        # Times should be identical (deterministic)
        assert result1["times"] == result2["times"]
        assert result1["final_time"] == result2["final_time"]

    def test_different_seeds_independent(self) -> None:
        """Test that different seeds produce independent executions."""
        # This test verifies isolation - different seeds shouldn't affect each other
        seeds = [111, 222, 333, 444]

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(
                    _run_workflow_in_thread,
                    workflow_id=f"seed-{seed}",
                    iterations=2,
                    seed=seed,
                )
                for seed in seeds
            ]
            results = [f.result() for f in as_completed(futures)]

        # All should complete successfully
        assert len(results) == 4

        # Each should have correct workflow_id
        workflow_ids = {r["workflow_id"] for r in results}
        expected_ids = {f"seed-{seed}" for seed in seeds}
        assert workflow_ids == expected_ids


class TestHighConcurrency:
    """Tests for higher concurrency levels."""

    def test_many_parallel_workflows(self) -> None:
        """Test many workflows running in parallel."""
        num_workflows = 10

        with ThreadPoolExecutor(max_workers=num_workflows) as executor:
            futures = [
                executor.submit(
                    _run_workflow_in_thread,
                    workflow_id=f"high-concurrency-{i}",
                    iterations=2,
                    seed=i * 100,
                )
                for i in range(num_workflows)
            ]
            results = [f.result() for f in as_completed(futures)]

        # All should complete
        assert len(results) == num_workflows

        # All should have unique workflow IDs
        workflow_ids = {r["workflow_id"] for r in results}
        assert len(workflow_ids) == num_workflows

    def test_parallel_with_shared_runner(self) -> None:
        """Test parallel workflows sharing a runner (but separate instances)."""
        runner = TrioWorkflowRunner()
        defn = workflow._Definition.must_from_class(CountingWorkflow)
        runner.prepare_workflow(defn)

        def run_with_shared_runner(workflow_id: str, seed: int) -> dict[str, object]:
            details = _create_details(
                CountingWorkflow,
                workflow_id=workflow_id,
                randomness_seed=seed,
            )
            return _execute_workflow_to_completion(
                runner, details, args=(workflow_id, 2)
            )  # type: ignore

        with ThreadPoolExecutor(max_workers=4) as executor:
            futures = [
                executor.submit(run_with_shared_runner, f"shared-runner-{i}", i * 50)
                for i in range(4)
            ]
            results = [f.result() for f in as_completed(futures)]

        # All should complete with correct IDs
        assert len(results) == 4
        workflow_ids = {r["workflow_id"] for r in results}
        assert len(workflow_ids) == 4


class TestSequentialVsParallel:
    """Tests comparing sequential and parallel execution."""

    def test_sequential_and_parallel_same_results(self) -> None:
        """Test that sequential and parallel execution produce same results."""
        configs = [
            ("compare-0", 2, 100),
            ("compare-1", 3, 200),
            ("compare-2", 1, 300),
        ]

        # Run sequentially
        sequential_results = {}
        for wf_id, iters, seed in configs:
            result = _run_workflow_in_thread(wf_id, iters, seed)
            sequential_results[wf_id] = result

        # Run in parallel
        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(_run_workflow_in_thread, wf_id, iters, seed): wf_id
                for wf_id, iters, seed in configs
            }
            parallel_results = {}
            for future in as_completed(futures):
                wf_id = futures[future]
                parallel_results[wf_id] = future.result()

        # Results should be identical
        for wf_id, _, _ in configs:
            assert sequential_results[wf_id] == parallel_results[wf_id]
