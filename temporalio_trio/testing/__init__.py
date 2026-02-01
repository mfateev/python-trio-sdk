"""Testing utilities for Temporal workflows with Trio.

This module provides testing infrastructure for Temporal workflows using Trio.

Example:
    async def test_my_workflow():
        async with await WorkflowEnvironment.start_local() as env:
            worker = Worker(
                env.client,
                task_queue="test-queue",
                workflows=[MyWorkflow],
            )
            async with worker:
                result = await env.client.execute_workflow(
                    MyWorkflow.run,
                    "arg",
                    id="test-workflow-1",
                    task_queue="test-queue",
                )
                assert result == "expected"
"""

from ._workflow import WorkflowEnvironment

__all__ = [
    "WorkflowEnvironment",
]
