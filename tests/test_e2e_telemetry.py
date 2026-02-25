"""End-to-end tests for telemetry/metrics integration.

These tests require a running Temporal server and validate that Prometheus
metrics are properly exported when telemetry is configured.

Run these tests with:
    pytest -v -m temporal_server tests/test_e2e_telemetry.py
"""

import socket
import time
import urllib.request

import pytest
import trio

from temporalio_trio import workflow
from temporalio_trio.client import Client
from temporalio_trio.runtime import PrometheusConfig, TelemetryConfig
from temporalio_trio.worker import Worker


def _find_free_port() -> int:
    """Find a free TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@workflow.defn
class MetricsTestWorkflow:
    """Simple workflow for metrics testing."""

    @workflow.run
    async def run(self, name: str) -> str:
        return f"hello {name}"


@pytest.fixture
async def trio_client():
    """Create a Trio-based Temporal client."""
    client = await Client.connect("localhost:7233", namespace="default")
    yield client
    await client.close()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_worker_with_prometheus_metrics(trio_client):
    """Test that Prometheus metrics endpoint is started and serves metrics.

    Starts a worker with Prometheus telemetry configured, then verifies that
    the /metrics endpoint is available and returns temporal metrics (pollers
    are enough to generate metrics without needing a complete workflow).
    """
    prom_port = _find_free_port()
    task_queue = f"telemetry-test-{int(time.time())}"

    telemetry = TelemetryConfig(
        metrics=PrometheusConfig(bind_address=f"127.0.0.1:{prom_port}"),
    )

    worker = Worker(
        client=trio_client,
        task_queue=task_queue,
        workflows=[MetricsTestWorkflow],
        telemetry=telemetry,
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)

        # Give worker time to start and generate some poll metrics
        # Telemetry needs a moment to bind the Prometheus port
        await trio.sleep(2)

        try:
            # Fetch Prometheus metrics endpoint
            metrics_url = f"http://127.0.0.1:{prom_port}/metrics"
            metrics_text = await trio.to_thread.run_sync(
                lambda: urllib.request.urlopen(metrics_url, timeout=5).read().decode()
            )

            # Verify temporal metrics are present (worker pollers generate
            # metrics even without running workflows)
            assert "temporal_" in metrics_text, (
                f"Expected 'temporal_' prefix in metrics output, "
                f"got:\n{metrics_text[:500]}"
            )

            # Verify some specific expected metric names
            # These are always emitted by SDK-Core when a worker is polling
            assert any(
                name in metrics_text
                for name in [
                    "temporal_long_request",
                    "temporal_request",
                    "temporal_num_pollers",
                    "temporal_worker_start",
                    "temporal_sticky_cache_size",
                ]
            ), f"Expected known temporal metrics, got:\n{metrics_text[:1000]}"

        finally:
            await worker.shutdown()
            await trio.sleep(0.3)
            nursery.cancel_scope.cancel()


@pytest.mark.temporal_server
@pytest.mark.trio
async def test_worker_without_telemetry(trio_client):
    """Test backward compatibility: worker starts and shuts down without telemetry."""
    task_queue = f"no-telemetry-test-{int(time.time())}"

    worker = Worker(
        client=trio_client,
        task_queue=task_queue,
        workflows=[MetricsTestWorkflow],
        # No telemetry parameter - should work as before
    )

    async with trio.open_nursery() as nursery:
        nursery.start_soon(worker.run)

        # Let worker initialize (needs time to connect to server)
        await trio.sleep(0.5)

        try:
            # Verify worker is running (it started without error)
            assert worker.is_running
        finally:
            await worker.shutdown()
            await trio.sleep(0.3)
            nursery.cancel_scope.cancel()
