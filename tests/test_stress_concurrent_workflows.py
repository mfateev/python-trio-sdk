"""Stress tests for concurrent workflow execution.

These tests verify that the Trio-based worker can handle many concurrent
workflow executions without issues.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import pytest

from temporalio_trio import workflow
from temporalio_trio.worker._activation import (
    CompleteWorkflowCommand,
    StartTimerCommand,
    TimerFiredJob,
    WorkflowActivation,
    WorkflowStartedJob,
)
from temporalio_trio.worker._workflow_instance import (
    TrioWorkflowInstance,
    TrioWorkflowRunner,
    WorkflowInstanceDetails,
)
from temporalio_trio.workflow import Info, _Definition


def make_instance(workflow_cls: type, workflow_id: str) -> TrioWorkflowInstance:
    """Create a workflow instance for testing."""
    defn = _Definition.must_from_class(workflow_cls)
    info = Info(
        workflow_id=workflow_id,
        workflow_type=defn.name or "",
        run_id=f"run-{workflow_id}",
        task_queue="stress-test-queue",
        namespace="default",
        attempt=1,
        start_time=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    det = WorkflowInstanceDetails(
        defn=defn,
        info=info,
        randomness_seed=hash(workflow_id) & 0xFFFFFFFFFFFFFFFF,
    )
    return TrioWorkflowInstance(det)


class TestStressConcurrentInstances:
    """Tests for concurrent workflow instance handling."""

    def test_100_simple_workflows_complete(self):
        """Test that 100 simple workflows can complete successfully."""

        @workflow.defn
        class SimpleWorkflow:
            @workflow.run
            async def run(self, value: int) -> int:
                return value * 2

        num_workflows = 100
        instances = []

        # Create 100 workflow instances
        for i in range(num_workflows):
            instance = make_instance(SimpleWorkflow, f"workflow-{i}")
            instances.append((i, instance))

        # Process first activation for all workflows
        results = []
        for i, instance in instances:
            act = WorkflowActivation(
                jobs=[WorkflowStartedJob(workflow_type="SimpleWorkflow", args=(i,))],
                timestamp_ns=1000000000,
            )
            completion = instance.activate(act)

            # Should complete immediately with result
            assert len(completion.commands) == 1
            cmd = completion.commands[0]
            assert isinstance(cmd, CompleteWorkflowCommand)
            results.append(cmd.result)

        # Verify all results are correct
        expected = [i * 2 for i in range(num_workflows)]
        assert results == expected

    def test_100_workflows_with_timers(self):
        """Test 100 workflows that use timers."""

        @workflow.defn
        class TimerWorkflow:
            @workflow.run
            async def run(self, sleep_ms: int) -> str:
                await workflow.sleep(sleep_ms / 1000.0)
                return f"slept-{sleep_ms}"

        num_workflows = 100
        instances = []

        # Create workflow instances
        for i in range(num_workflows):
            instance = make_instance(TimerWorkflow, f"timer-workflow-{i}")
            instances.append((i, instance))

        # First activation - create timers
        pending_timers = []
        for i, instance in instances:
            sleep_ms = 100 + i  # Different sleep times
            act = WorkflowActivation(
                jobs=[
                    WorkflowStartedJob(workflow_type="TimerWorkflow", args=(sleep_ms,))
                ],
                timestamp_ns=1000000000,
            )
            completion = instance.activate(act)

            # Should have a start timer command
            assert len(completion.commands) == 1
            cmd = completion.commands[0]
            assert isinstance(cmd, StartTimerCommand)
            pending_timers.append((i, instance, cmd.timer_id, sleep_ms))

        # Second activation - timers fire
        results = []
        for i, instance, timer_id, sleep_ms in pending_timers:
            act = WorkflowActivation(
                jobs=[TimerFiredJob(timer_id=timer_id)],
                timestamp_ns=2000000000,
            )
            completion = instance.activate(act)

            # Should complete with result
            assert len(completion.commands) == 1
            cmd = completion.commands[0]
            assert isinstance(cmd, CompleteWorkflowCommand)
            results.append(cmd.result)

        # Verify all results
        expected = [f"slept-{100 + i}" for i in range(num_workflows)]
        assert results == expected

    def test_200_mixed_workflows(self):
        """Test 200 workflows with a mix of immediate completion and timers."""

        @workflow.defn
        class ImmediateWorkflow:
            @workflow.run
            async def run(self, value: int) -> int:
                return value

        @workflow.defn
        class TimerWorkflow:
            @workflow.run
            async def run(self, value: int) -> int:
                await workflow.sleep(0.1)
                return value * 10

        runner = TrioWorkflowRunner()
        runner.prepare_workflow(_Definition.must_from_class(ImmediateWorkflow))
        runner.prepare_workflow(_Definition.must_from_class(TimerWorkflow))

        num_each = 100
        immediate_results = []
        timer_instances = []

        # Process immediate workflows
        for i in range(num_each):
            instance = make_instance(ImmediateWorkflow, f"immediate-{i}")
            act = WorkflowActivation(
                jobs=[WorkflowStartedJob(workflow_type="ImmediateWorkflow", args=(i,))],
                timestamp_ns=1000000000,
            )
            completion = instance.activate(act)
            assert isinstance(completion.commands[0], CompleteWorkflowCommand)
            immediate_results.append(completion.commands[0].result)

        # Start timer workflows
        for i in range(num_each):
            instance = make_instance(TimerWorkflow, f"timer-{i}")
            act = WorkflowActivation(
                jobs=[WorkflowStartedJob(workflow_type="TimerWorkflow", args=(i,))],
                timestamp_ns=1000000000,
            )
            completion = instance.activate(act)
            assert isinstance(completion.commands[0], StartTimerCommand)
            timer_instances.append((i, instance, completion.commands[0].timer_id))

        # Complete timer workflows
        timer_results = []
        for i, instance, timer_id in timer_instances:
            act = WorkflowActivation(
                jobs=[TimerFiredJob(timer_id=timer_id)],
                timestamp_ns=2000000000,
            )
            completion = instance.activate(act)
            assert isinstance(completion.commands[0], CompleteWorkflowCommand)
            timer_results.append(completion.commands[0].result)

        # Verify results
        assert immediate_results == list(range(num_each))
        assert timer_results == [i * 10 for i in range(num_each)]

    def test_concurrent_instances_isolated(self):
        """Test that concurrent workflow instances are properly isolated."""

        @workflow.defn
        class StatefulWorkflow:
            def __init__(self):
                self.counter = 0

            @workflow.run
            async def run(self, increment: int) -> int:
                self.counter += increment
                await workflow.sleep(0.1)
                self.counter += increment
                return self.counter

        num_workflows = 50
        instances = []

        # Create instances with different increments
        for i in range(num_workflows):
            instance = make_instance(StatefulWorkflow, f"stateful-{i}")
            instances.append((i + 1, instance))  # increment = i + 1

        # First activation - increment counter once, create timer
        timers = []
        for increment, instance in instances:
            act = WorkflowActivation(
                jobs=[
                    WorkflowStartedJob(
                        workflow_type="StatefulWorkflow", args=(increment,)
                    )
                ],
                timestamp_ns=1000000000,
            )
            completion = instance.activate(act)
            assert isinstance(completion.commands[0], StartTimerCommand)
            timers.append((increment, instance, completion.commands[0].timer_id))

        # Second activation - increment counter again, complete
        results = []
        for increment, instance, timer_id in timers:
            act = WorkflowActivation(
                jobs=[TimerFiredJob(timer_id=timer_id)],
                timestamp_ns=2000000000,
            )
            completion = instance.activate(act)
            assert isinstance(completion.commands[0], CompleteWorkflowCommand)
            results.append(completion.commands[0].result)

        # Each workflow should have counter = increment * 2
        expected = [(i + 1) * 2 for i in range(num_workflows)]
        assert results == expected


class TestStressPerformance:
    """Performance tests for workflow execution."""

    def test_1000_simple_workflows_performance(self):
        """Test that 1000 simple workflows complete in reasonable time."""

        @workflow.defn
        class SimpleWorkflow:
            @workflow.run
            async def run(self) -> str:
                return "done"

        num_workflows = 1000
        start_time = time.time()

        for i in range(num_workflows):
            instance = make_instance(SimpleWorkflow, f"perf-workflow-{i}")
            act = WorkflowActivation(
                jobs=[WorkflowStartedJob(workflow_type="SimpleWorkflow", args=())],
                timestamp_ns=1000000000,
            )
            completion = instance.activate(act)
            assert isinstance(completion.commands[0], CompleteWorkflowCommand)

        elapsed = time.time() - start_time
        # Should complete all workflows in under 10 seconds (very generous limit)
        assert elapsed < 10.0, f"1000 workflows took {elapsed:.2f}s, expected < 10s"

        # Report throughput
        throughput = num_workflows / elapsed
        print(f"\nThroughput: {throughput:.0f} workflows/second")

    def test_workflow_instance_creation_performance(self):
        """Test that creating many workflow instances is fast."""

        @workflow.defn
        class SimpleWorkflow:
            @workflow.run
            async def run(self) -> str:
                return "done"

        num_instances = 5000
        start_time = time.time()

        instances = []
        for i in range(num_instances):
            instance = make_instance(SimpleWorkflow, f"instance-{i}")
            instances.append(instance)

        elapsed = time.time() - start_time
        # Should create all instances in under 5 seconds
        assert elapsed < 5.0, f"Creating {num_instances} instances took {elapsed:.2f}s"

        # Verify all instances were created
        assert len(instances) == num_instances

        # Report creation rate
        rate = num_instances / elapsed
        print(f"\nInstance creation rate: {rate:.0f} instances/second")


class TestStressMemory:
    """Memory-related stress tests."""

    def test_large_workflow_result(self):
        """Test workflows returning large results."""

        @workflow.defn
        class LargeResultWorkflow:
            @workflow.run
            async def run(self, size: int) -> list:
                return list(range(size))

        # Create 10 workflows each returning 10000 items
        num_workflows = 10
        result_size = 10000

        for i in range(num_workflows):
            instance = make_instance(LargeResultWorkflow, f"large-result-{i}")
            act = WorkflowActivation(
                jobs=[
                    WorkflowStartedJob(
                        workflow_type="LargeResultWorkflow", args=(result_size,)
                    )
                ],
                timestamp_ns=1000000000,
            )
            completion = instance.activate(act)
            assert isinstance(completion.commands[0], CompleteWorkflowCommand)
            result = completion.commands[0].result
            assert len(result) == result_size
            assert result == list(range(result_size))

    def test_large_workflow_arguments(self):
        """Test workflows with large arguments."""

        @workflow.defn
        class LargeArgsWorkflow:
            @workflow.run
            async def run(self, data: list) -> int:
                return sum(data)

        # Create workflow with large argument
        large_arg = list(range(10000))
        instance = make_instance(LargeArgsWorkflow, "large-args-1")
        act = WorkflowActivation(
            jobs=[
                WorkflowStartedJob(workflow_type="LargeArgsWorkflow", args=(large_arg,))
            ],
            timestamp_ns=1000000000,
        )
        completion = instance.activate(act)
        assert isinstance(completion.commands[0], CompleteWorkflowCommand)
        assert completion.commands[0].result == sum(large_arg)


class TestStressSignalsAndQueries:
    """Stress tests for signals and queries with many concurrent workflows."""

    def test_100_workflows_with_signals(self):
        """Test 100 workflows each receiving signals."""

        from temporalio_trio.worker._activation import SignalWorkflowJob

        @workflow.defn
        class SignalWorkflow:
            def __init__(self):
                self.values = []

            @workflow.signal
            def add_value(self, value: int) -> None:
                self.values.append(value)

            @workflow.run
            async def run(self) -> list:
                # Wait for signals, then return
                await workflow.sleep(0.1)
                return self.values

        num_workflows = 100
        instances = []

        # Start all workflows
        for i in range(num_workflows):
            instance = make_instance(SignalWorkflow, f"signal-workflow-{i}")
            act = WorkflowActivation(
                jobs=[WorkflowStartedJob(workflow_type="SignalWorkflow", args=())],
                timestamp_ns=1000000000,
            )
            completion = instance.activate(act)
            assert isinstance(completion.commands[0], StartTimerCommand)
            instances.append((i, instance, completion.commands[0].timer_id))

        # Send signals and complete each workflow
        results = []
        for i, instance, timer_id in instances:
            # Send 3 signals with workflow-specific values
            signals = [i * 10, i * 10 + 1, i * 10 + 2]
            signal_jobs = [
                SignalWorkflowJob(signal_name="add_value", args=(v,)) for v in signals
            ]

            # Timer fires with signals in same activation
            act = WorkflowActivation(
                jobs=[*signal_jobs, TimerFiredJob(timer_id=timer_id)],
                timestamp_ns=2000000000,
            )
            completion = instance.activate(act)
            assert isinstance(completion.commands[0], CompleteWorkflowCommand)
            results.append(completion.commands[0].result)

        # Verify each workflow received its signals
        for i, result in enumerate(results):
            expected = [i * 10, i * 10 + 1, i * 10 + 2]
            assert result == expected, f"Workflow {i} got {result}, expected {expected}"

    def test_100_workflows_with_queries(self):
        """Test 100 workflows each handling queries."""

        from temporalio_trio.worker._activation import (
            QueryResultCommand,
            QueryWorkflowJob,
        )

        @workflow.defn
        class QueryWorkflow:
            def __init__(self):
                self.value = 0

            @workflow.query
            def get_value(self) -> int:
                return self.value

            @workflow.run
            async def run(self, initial: int) -> int:
                self.value = initial
                await workflow.sleep(0.1)
                return self.value

        num_workflows = 100
        instances = []

        # Start all workflows with different initial values
        for i in range(num_workflows):
            instance = make_instance(QueryWorkflow, f"query-workflow-{i}")
            act = WorkflowActivation(
                jobs=[
                    WorkflowStartedJob(workflow_type="QueryWorkflow", args=(i * 100,))
                ],
                timestamp_ns=1000000000,
            )
            completion = instance.activate(act)
            assert isinstance(completion.commands[0], StartTimerCommand)
            instances.append((i, instance, completion.commands[0].timer_id))

        # Query each workflow
        query_results = []
        for i, instance, timer_id in instances:
            # Query the workflow
            act = WorkflowActivation(
                jobs=[
                    QueryWorkflowJob(
                        query_id=f"query-{i}", query_type="get_value", args=()
                    )
                ],
                timestamp_ns=1500000000,
            )
            completion = instance.activate(act)

            # Find the query result command
            query_cmd = None
            for cmd in completion.commands:
                if isinstance(cmd, QueryResultCommand):
                    query_cmd = cmd
                    break
            assert query_cmd is not None
            query_results.append(query_cmd.result)

        # Verify query results
        for i, result in enumerate(query_results):
            assert result == i * 100, f"Query {i} returned {result}, expected {i * 100}"
