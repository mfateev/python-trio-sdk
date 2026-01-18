"""Temporal client for Trio async runtime.

This module provides a pure Trio client for interacting with Temporal workflows.
The API matches the official Temporal Python SDK, but uses Trio instead of asyncio.

Example:
    import trio
    from temporalio_trio.client import Client

    async def main():
        # Connect to Temporal
        client = await Client.connect("localhost:7233")

        try:
            # Start a workflow
            handle = await client.start_workflow(
                "MyWorkflow",
                "arg1",
                id="workflow-123",
                task_queue="my-queue",
            )

            # Wait for result
            result = await handle.result()
            print(f"Result: {result}")

        finally:
            await client.close()

    trio.run(main)
"""

from ._client import Client, ClientConfig
from ._workflow_handle import WorkflowHandle

__all__ = [
    "Client",
    "ClientConfig",
    "WorkflowHandle",
]
