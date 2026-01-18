"""Example of using the Trio Temporal client.

This example demonstrates how to:
1. Connect to a Temporal server
2. Start a workflow
3. Wait for the workflow result
4. Use workflow handles for operations
"""

import trio

from temporalio_trio.client import Client


async def main():
    """Main example function."""
    # Connect to Temporal server
    client = await Client.connect(
        "localhost:7233",
        namespace="default",
    )

    try:
        print("Connected to Temporal server")

        # Example 1: Execute a workflow (start + wait for result)
        print("\n=== Example 1: Execute Workflow ===")
        result = await client.execute_workflow(
            "GreetingWorkflow",  # Workflow name
            "World",  # Workflow argument
            id="greeting-workflow-1",
            task_queue="my-task-queue",
        )
        print(f"Workflow result: {result}")

        # Example 2: Start a workflow and get a handle
        print("\n=== Example 2: Start Workflow and Get Handle ===")
        handle = await client.start_workflow(
            "GreetingWorkflow",
            "Alice",
            id="greeting-workflow-2",
            task_queue="my-task-queue",
        )
        print(f"Started workflow: {handle.workflow_id}")
        print(f"Run ID: {handle.run_id}")

        # Wait for result
        result = await handle.result()
        print(f"Workflow result: {result}")

        # Example 3: Get handle to existing workflow
        print("\n=== Example 3: Get Existing Workflow Handle ===")
        existing_handle = client.get_workflow_handle("greeting-workflow-1")
        # Can query, signal, cancel, or terminate
        # await existing_handle.signal("update_greeting", "Hi")
        # await existing_handle.cancel()
        print(f"Got handle to workflow: {existing_handle.workflow_id}")

        # Example 4: Start workflow with options
        print("\n=== Example 4: Workflow with Options ===")
        handle = await client.start_workflow(
            "LongRunningWorkflow",
            id="long-running-1",
            task_queue="my-task-queue",
            execution_timeout=3600.0,  # 1 hour timeout
            run_timeout=1800.0,  # 30 minute run timeout
        )
        print(f"Started long-running workflow: {handle.workflow_id}")

        # You can cancel it later
        # await handle.cancel()

    finally:
        # Always close the client
        await client.close()
        print("\nClient closed")


if __name__ == "__main__":
    # Run with Trio
    trio.run(main)
