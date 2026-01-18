"""Complete example: Worker + Client with live Temporal server.

This example demonstrates a complete Trio-based Temporal application:
1. Defines workflows
2. Starts a worker to process workflows
3. Uses the Trio client to start and interact with workflows
4. All running against a live Temporal server

Prerequisites:
    - Temporal server running on localhost:7233
    - Run: temporal server start-dev

Usage:
    python examples/client_worker_example.py
"""

import trio

from temporalio_trio import workflow
from temporalio_trio.client import Client
from temporalio_trio.worker import Worker


# ============================================================================
# Workflow Definitions
# ============================================================================


@workflow.defn
class GreetingWorkflow:
    """Simple workflow that returns a personalized greeting."""

    @workflow.run
    async def run(self, name: str) -> str:
        """Generate greeting for the given name.

        Args:
            name: Name to greet

        Returns:
            Greeting message
        """
        # Simulate some work with a delay
        await workflow.sleep(1.0)
        return f"Hello, {name}! Welcome to Trio Temporal."


@workflow.defn
class CountdownWorkflow:
    """Workflow that counts down from a number."""

    @workflow.run
    async def run(self, start: int) -> list[int]:
        """Count down from start to 1.

        Args:
            start: Starting number

        Returns:
            List of numbers in countdown
        """
        countdown = []
        for i in range(start, 0, -1):
            countdown.append(i)
            print(f"  Countdown: {i}")
            await workflow.sleep(0.5)

        return countdown


@workflow.defn
class DataProcessingWorkflow:
    """Workflow that processes data with multiple steps."""

    @workflow.run
    async def run(self, data: list[int]) -> dict:
        """Process data through multiple steps.

        Args:
            data: List of integers to process

        Returns:
            Dictionary with processing results
        """
        print(f"  Processing {len(data)} items...")

        # Step 1: Calculate statistics
        await workflow.sleep(0.3)
        total = sum(data)
        average = total / len(data) if data else 0

        # Step 2: Find min/max
        await workflow.sleep(0.3)
        minimum = min(data) if data else 0
        maximum = max(data) if data else 0

        # Step 3: Return results
        await workflow.sleep(0.3)
        return {
            "count": len(data),
            "sum": total,
            "average": average,
            "min": minimum,
            "max": maximum,
        }


# ============================================================================
# Worker Setup
# ============================================================================


async def run_worker(task_queue: str):
    """Start a worker to process workflows.

    Args:
        task_queue: Task queue name to listen on
    """
    print(f"\n🔧 Starting worker on task queue: {task_queue}")

    # Create Trio client for worker
    client = await Client.connect("localhost:7233", namespace="default")

    # Create worker
    worker = Worker(
        client=client,
        task_queue=task_queue,
        workflows=[GreetingWorkflow, CountdownWorkflow, DataProcessingWorkflow],
    )

    print("✅ Worker ready and listening for tasks\n")

    # Run worker (this will run until cancelled)
    try:
        await worker.run()
    finally:
        await client.close()


# ============================================================================
# Client Examples
# ============================================================================


async def example_simple_greeting(client: Client, task_queue: str):
    """Example 1: Simple workflow execution."""
    print("\n" + "=" * 60)
    print("Example 1: Simple Greeting Workflow")
    print("=" * 60)

    # Execute workflow and wait for result
    result = await client.execute_workflow(
        GreetingWorkflow,
        "Alice",
        id="greeting-alice",
        task_queue=task_queue,
    )

    print(f"✅ Result: {result}")


async def example_countdown(client: Client, task_queue: str):
    """Example 2: Workflow with observable progress."""
    print("\n" + "=" * 60)
    print("Example 2: Countdown Workflow")
    print("=" * 60)

    # Start workflow and get handle
    handle = await client.start_workflow(
        CountdownWorkflow,
        5,  # Count from 5
        id="countdown-5",
        task_queue=task_queue,
    )

    print(f"🚀 Started workflow: {handle.workflow_id}")
    print(f"   Run ID: {handle.run_id}")

    # Wait for result
    result = await handle.result()
    print(f"✅ Countdown complete: {result}")


async def example_data_processing(client: Client, task_queue: str):
    """Example 3: Data processing workflow."""
    print("\n" + "=" * 60)
    print("Example 3: Data Processing Workflow")
    print("=" * 60)

    data = [10, 25, 30, 15, 40, 35, 20]
    print(f"📊 Processing data: {data}")

    result = await client.execute_workflow(
        DataProcessingWorkflow,
        data,
        id="data-processing-1",
        task_queue=task_queue,
    )

    print(f"✅ Processing complete:")
    for key, value in result.items():
        print(f"   {key}: {value}")


async def example_multiple_workflows(client: Client, task_queue: str):
    """Example 4: Multiple workflows in parallel."""
    print("\n" + "=" * 60)
    print("Example 4: Multiple Workflows in Parallel")
    print("=" * 60)

    names = ["Bob", "Charlie", "Diana", "Eve"]
    print(f"🚀 Starting {len(names)} workflows in parallel...")

    # Start all workflows concurrently
    async with trio.open_nursery() as nursery:
        handles = []

        async def start_workflow(name: str, index: int):
            handle = await client.start_workflow(
                GreetingWorkflow,
                name,
                id=f"greeting-parallel-{index}",
                task_queue=task_queue,
            )
            handles.append((name, handle))
            print(f"   Started workflow for {name}")

        for i, name in enumerate(names):
            nursery.start_soon(start_workflow, name, i)

    # Wait for all results in parallel
    print(f"\n⏳ Waiting for {len(handles)} workflows to complete...")

    async with trio.open_nursery() as nursery:
        results = []

        async def get_result(name: str, handle):
            result = await handle.result()
            results.append((name, result))

        for name, handle in handles:
            nursery.start_soon(get_result, name, handle)

    # Display results
    print(f"\n✅ All workflows complete:")
    for name, result in results:
        print(f"   {name}: {result}")


async def example_workflow_handle_operations(client: Client, task_queue: str):
    """Example 5: Workflow handle operations."""
    print("\n" + "=" * 60)
    print("Example 5: Workflow Handle Operations")
    print("=" * 60)

    # Start a long-running workflow
    handle = await client.start_workflow(
        CountdownWorkflow,
        10,
        id="countdown-cancelable",
        task_queue=task_queue,
    )

    print(f"🚀 Started workflow: {handle.workflow_id}")

    # Wait a bit, then cancel
    await trio.sleep(2.0)
    print("🛑 Canceling workflow...")
    await handle.cancel()

    # Try to get result (should raise exception)
    try:
        await handle.result()
    except RuntimeError as e:
        print(f"✅ Workflow was canceled as expected: {e}")


async def example_get_existing_handle(client: Client, task_queue: str):
    """Example 6: Get handle to existing workflow."""
    print("\n" + "=" * 60)
    print("Example 6: Get Handle to Existing Workflow")
    print("=" * 60)

    workflow_id = "greeting-existing"

    # Start a workflow
    await client.start_workflow(
        GreetingWorkflow,
        "Frank",
        id=workflow_id,
        task_queue=task_queue,
    )
    print(f"🚀 Started workflow: {workflow_id}")

    # Get handle to the same workflow (without knowing the run ID)
    handle = client.get_workflow_handle(workflow_id)
    print(f"📌 Got handle to existing workflow: {handle.workflow_id}")

    # Get result
    result = await handle.result()
    print(f"✅ Result: {result}")


async def run_client_examples(task_queue: str):
    """Run all client examples.

    Args:
        task_queue: Task queue name where worker is listening
    """
    print("\n" + "=" * 60)
    print("🌟 Trio Temporal Client Examples")
    print("=" * 60)

    # Connect to Temporal server
    print("\n🔗 Connecting to Temporal server...")
    client = await Client.connect("localhost:7233", namespace="default")
    print("✅ Connected to Temporal")

    try:
        # Give worker time to start
        await trio.sleep(1.0)

        # Run examples
        await example_simple_greeting(client, task_queue)
        await example_countdown(client, task_queue)
        await example_data_processing(client, task_queue)
        await example_multiple_workflows(client, task_queue)
        await example_workflow_handle_operations(client, task_queue)
        await example_get_existing_handle(client, task_queue)

        print("\n" + "=" * 60)
        print("✅ All examples completed successfully!")
        print("=" * 60 + "\n")

    finally:
        # Close client
        await client.close()
        print("🔌 Client disconnected")


# ============================================================================
# Main Entry Point
# ============================================================================


async def main():
    """Main entry point: Start worker and run client examples."""
    task_queue = "trio-example-queue"

    print("\n" + "=" * 60)
    print("🚀 Trio Temporal Example Application")
    print("=" * 60)
    print("\nThis example demonstrates:")
    print("  - Defining workflows with @workflow.defn")
    print("  - Starting a worker to process workflows")
    print("  - Using the Trio client to interact with workflows")
    print("  - Various workflow patterns (simple, parallel, cancellation)")
    print("\nPrerequisites:")
    print("  - Temporal server running on localhost:7233")
    print("  - Run: temporal server start-dev")
    print("=" * 60)

    # Run worker and client examples concurrently
    async with trio.open_nursery() as nursery:
        # Start worker
        nursery.start_soon(run_worker, task_queue)

        # Run client examples
        nursery.start_soon(run_client_examples, task_queue)

        # After examples complete, cancel the worker
        await trio.sleep(15.0)  # Give time for all examples
        print("\n⏹️  Shutting down worker...")
        nursery.cancel_scope.cancel()


if __name__ == "__main__":
    print("\n🎯 Starting Trio Temporal example...")
    print("📝 Press Ctrl+C to stop\n")

    try:
        trio.run(main)
    except KeyboardInterrupt:
        print("\n\n👋 Example stopped by user")
    except Exception as e:
        print(f"\n\n❌ Error: {e}")
        raise
    finally:
        print("\n✅ Example finished\n")
