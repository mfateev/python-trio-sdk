"""Tests for workflow query handlers."""

import pytest
import trio

from temporalio_trio import workflow
from temporalio_trio.workflow import _Definition, _QueryDefinition
from temporalio_trio.worker._activation import (
    QueryWorkflowJob,
    QueryResultCommand,
    WorkflowActivation,
    WorkflowStartedJob,
)
from temporalio_trio.worker._workflow_instance import (
    TrioWorkflowInstance,
    WorkflowInstanceDetails,
)
from temporalio_trio.workflow import Info


class TestQueryDecorator:
    """Tests for @workflow.query decorator."""

    def test_query_decorator_marks_method(self):
        """Test that @query decorator marks method with definition."""
        @workflow.defn
        class TestWorkflow:
            @workflow.query
            def get_status(self) -> str:
                return "ok"

            @workflow.run
            async def run(self) -> None:
                pass

        defn = _QueryDefinition.from_fn(TestWorkflow.get_status)
        assert defn is not None
        assert defn.name == "get_status"
        assert defn.is_method is True

    def test_query_decorator_with_custom_name(self):
        """Test query decorator with custom name."""
        @workflow.defn
        class TestWorkflow:
            @workflow.query(name="custom-query")
            def get_status(self) -> str:
                return "ok"

            @workflow.run
            async def run(self) -> None:
                pass

        defn = _QueryDefinition.from_fn(TestWorkflow.get_status)
        assert defn is not None
        assert defn.name == "custom-query"

    def test_query_decorator_dynamic(self):
        """Test dynamic query decorator."""
        @workflow.defn
        class TestWorkflow:
            @workflow.query(dynamic=True)
            def handle_any(self, name: str, *args):
                return f"query: {name}"

            @workflow.run
            async def run(self) -> None:
                pass

        defn = _QueryDefinition.from_fn(TestWorkflow.handle_any)
        assert defn is not None
        assert defn.name is None  # Dynamic handlers have None name

    def test_query_decorator_rejects_async(self):
        """Test that async query handlers are rejected."""
        with pytest.raises(ValueError, match="must be synchronous"):
            @workflow.defn
            class TestWorkflow:
                @workflow.query
                async def bad_query(self) -> str:
                    return "bad"

                @workflow.run
                async def run(self) -> None:
                    pass

    def test_query_collected_in_definition(self):
        """Test that queries are collected in workflow definition."""
        @workflow.defn
        class TestWorkflow:
            @workflow.query
            def query_one(self) -> int:
                return 1

            @workflow.query(name="query-two")
            def query_two(self) -> int:
                return 2

            @workflow.run
            async def run(self) -> None:
                pass

        defn = _Definition.must_from_class(TestWorkflow)
        assert "query_one" in defn.queries
        assert "query-two" in defn.queries
        assert len(defn.queries) == 2

    def test_duplicate_query_name_raises(self):
        """Test that duplicate query names raise error."""
        with pytest.raises(ValueError, match="Duplicate query"):
            @workflow.defn
            class TestWorkflow:
                @workflow.query(name="same-name")
                def query_one(self) -> int:
                    return 1

                @workflow.query(name="same-name")
                def query_two(self) -> int:
                    return 2

                @workflow.run
                async def run(self) -> None:
                    pass


class TestQueryHandler:
    """Tests for query handler invocation."""

    def _create_instance(self, workflow_cls) -> TrioWorkflowInstance:
        """Helper to create a workflow instance."""
        defn = _Definition.must_from_class(workflow_cls)
        info = Info(
            workflow_id="test-wf-id",
            run_id="test-run-id",
            workflow_type=defn.name,
            task_queue="test-queue",
        )
        details = WorkflowInstanceDetails(
            defn=defn,
            info=info,
            randomness_seed=12345,
        )
        return TrioWorkflowInstance(details)

    def test_query_handler_returns_value(self):
        """Test query handler returns a value."""
        @workflow.defn
        class TestWorkflow:
            @workflow.query
            def get_value(self) -> int:
                return 42

            @workflow.run
            async def run(self) -> str:
                return "done"

        instance = self._create_instance(TestWorkflow)

        # Create activation with start and query
        activation = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(
                    workflow_type="TestWorkflow",
                    args=(),
                ),
                QueryWorkflowJob(
                    query_id="query-1",
                    query_type="get_value",
                    args=(),
                ),
            ],
        )

        completion = instance.activate(activation)

        # Check that we have a query result command
        query_results = [c for c in completion.commands if isinstance(c, QueryResultCommand)]
        assert len(query_results) == 1
        assert query_results[0].query_id == "query-1"
        assert query_results[0].result == 42
        assert query_results[0].error is None

    def test_query_handler_with_args(self):
        """Test query handler with arguments."""
        @workflow.defn
        class TestWorkflow:
            @workflow.query
            def multiply(self, a: int, b: int) -> int:
                return a * b

            @workflow.run
            async def run(self) -> str:
                return "done"

        instance = self._create_instance(TestWorkflow)

        activation = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(workflow_type="TestWorkflow", args=()),
                QueryWorkflowJob(
                    query_id="query-1",
                    query_type="multiply",
                    args=(6, 7),
                ),
            ],
        )

        completion = instance.activate(activation)

        query_results = [c for c in completion.commands if isinstance(c, QueryResultCommand)]
        assert len(query_results) == 1
        assert query_results[0].result == 42

    def test_query_reads_workflow_state(self):
        """Test that query can read workflow instance state."""
        @workflow.defn
        class CounterWorkflow:
            def __init__(self):
                self.count = 100

            @workflow.query
            def get_count(self) -> int:
                return self.count

            @workflow.run
            async def run(self) -> int:
                return self.count

        instance = self._create_instance(CounterWorkflow)

        activation = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(workflow_type="CounterWorkflow", args=()),
                QueryWorkflowJob(
                    query_id="query-1",
                    query_type="get_count",
                    args=(),
                ),
            ],
        )

        completion = instance.activate(activation)

        query_results = [c for c in completion.commands if isinstance(c, QueryResultCommand)]
        assert len(query_results) == 1
        assert query_results[0].result == 100

    def test_multiple_queries_same_activation(self):
        """Test multiple queries in same activation."""
        @workflow.defn
        class TestWorkflow:
            @workflow.query
            def query_a(self) -> str:
                return "a"

            @workflow.query
            def query_b(self) -> str:
                return "b"

            @workflow.run
            async def run(self) -> str:
                return "done"

        instance = self._create_instance(TestWorkflow)

        activation = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(workflow_type="TestWorkflow", args=()),
                QueryWorkflowJob(query_id="q1", query_type="query_a", args=()),
                QueryWorkflowJob(query_id="q2", query_type="query_b", args=()),
            ],
        )

        completion = instance.activate(activation)

        query_results = [c for c in completion.commands if isinstance(c, QueryResultCommand)]
        assert len(query_results) == 2

        results_by_id = {r.query_id: r.result for r in query_results}
        assert results_by_id["q1"] == "a"
        assert results_by_id["q2"] == "b"

    def test_query_not_found_error(self):
        """Test that unknown query returns error."""
        @workflow.defn
        class TestWorkflow:
            @workflow.query
            def known_query(self) -> str:
                return "known"

            @workflow.run
            async def run(self) -> str:
                return "done"

        instance = self._create_instance(TestWorkflow)

        activation = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(workflow_type="TestWorkflow", args=()),
                QueryWorkflowJob(
                    query_id="query-1",
                    query_type="unknown_query",
                    args=(),
                ),
            ],
        )

        completion = instance.activate(activation)

        # Should still succeed (workflow runs) but with a query error
        query_results = [c for c in completion.commands if isinstance(c, QueryResultCommand)]
        assert len(query_results) == 1
        assert query_results[0].query_id == "query-1"
        assert query_results[0].error is not None
        assert "not found" in query_results[0].error.lower()

    def test_query_handler_exception_returns_error(self):
        """Test that exceptions in query handlers return errors."""
        @workflow.defn
        class TestWorkflow:
            @workflow.query
            def failing_query(self) -> str:
                raise ValueError("Query failed intentionally")

            @workflow.run
            async def run(self) -> str:
                return "done"

        instance = self._create_instance(TestWorkflow)

        activation = WorkflowActivation(
            timestamp_ns=1000,
            jobs=[
                WorkflowStartedJob(workflow_type="TestWorkflow", args=()),
                QueryWorkflowJob(
                    query_id="query-1",
                    query_type="failing_query",
                    args=(),
                ),
            ],
        )

        completion = instance.activate(activation)

        query_results = [c for c in completion.commands if isinstance(c, QueryResultCommand)]
        assert len(query_results) == 1
        assert query_results[0].error is not None
        assert "Query failed intentionally" in query_results[0].error


class TestQueryDefinition:
    """Tests for _QueryDefinition class."""

    def test_from_fn_returns_none_for_non_query(self):
        """Test that from_fn returns None for non-query functions."""
        def regular_func():
            pass

        assert _QueryDefinition.from_fn(regular_func) is None

    def test_from_fn_returns_definition_for_query(self):
        """Test that from_fn returns definition for query functions."""
        @workflow.query
        def my_query():
            return 1

        defn = _QueryDefinition.from_fn(my_query)
        assert defn is not None
        assert defn.name == "my_query"
