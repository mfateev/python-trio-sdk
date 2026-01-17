# End-to-End Testing Guide

This document explains how to run end-to-end integration tests that validate the Trio SDK against a real Temporal server.

## Prerequisites

### 1. Temporal Server

You need a running Temporal server. The easiest way is to use the Temporal dev server:

```bash
# Option A: Using temporal CLI (recommended)
temporal server start-dev

# Option B: Using Docker
docker run -p 7233:7233 temporalio/auto-setup:latest
```

The server should be accessible at `localhost:7233`.

### 2. Temporal CLI

The tests use the Temporal CLI to validate workflow results. The CLI should be in your PATH.

If you have the temporal binary in a different location (like `/home/sprite/workarea/bin/temporal`), you can:

```bash
# Add to PATH temporarily
export PATH="/home/sprite/workarea/bin:$PATH"

# Or create a symlink
ln -s /home/sprite/workarea/bin/temporal ~/.local/bin/temporal
```

Verify the CLI is accessible:
```bash
temporal --version
```

## Running E2E Tests

### Run All E2E Tests

```bash
# Make sure temporal server is running first!
cd /path/to/python-trio-sdk

# Run e2e tests
uv run pytest -v -m temporal_server tests/test_e2e_integration.py
```

### Run Specific E2E Test

```bash
# Test workflow execution end-to-end
uv run pytest -v -m temporal_server tests/test_e2e_integration.py::test_e2e_workflow_execution

# Test worker connection only
uv run pytest -v -m temporal_server tests/test_e2e_integration.py::test_e2e_worker_connection
```

### Run All Tests Except E2E

```bash
# Skip e2e tests (useful for CI without Temporal server)
uv run pytest -v -m "not temporal_server"
```

## What The E2E Tests Validate

### test_e2e_workflow_execution

This comprehensive test validates the entire workflow execution path:

1. **Client Connection**: Connects to Temporal server at localhost:7233
2. **Worker Startup**: Starts a Trio worker with SDK Core integration
3. **Workflow Execution**: Executes a simple timer workflow
4. **Result Validation (Python)**: Verifies workflow result from Python SDK
5. **Result Validation (CLI)**: Uses `temporal workflow describe` to validate via CLI
6. **Graceful Shutdown**: Cleanly shuts down the worker

This test confirms that:
- The Rust bridge successfully integrates with SDK Core
- Workers can poll for and execute workflows
- Workflow timers work correctly with deterministic time
- Results are properly returned to the client
- The entire async architecture (Trio + Tokio) works end-to-end

### test_e2e_worker_connection

A simpler test that validates:
- Client can connect to Temporal server
- Worker initialization succeeds
- Worker can start and validate connection
- Graceful shutdown works

## Troubleshooting

### "Connection refused" errors

**Problem**: Tests fail with connection errors.

**Solution**: Ensure Temporal server is running on localhost:7233:
```bash
# Terminal 1: Start server
temporal server start-dev

# Terminal 2: Run tests
uv run pytest -v -m temporal_server
```

### "temporal: command not found"

**Problem**: Temporal CLI not in PATH.

**Solution**: Add temporal binary to PATH:
```bash
export PATH="/home/sprite/workarea/bin:$PATH"
temporal --version  # Verify
```

### Test timeouts

**Problem**: Tests timeout waiting for workflows.

**Solution**: Check that:
1. Temporal server is running and responsive
2. No firewall blocking localhost:7233
3. Worker is starting successfully (check logs)

### Workflow not found

**Problem**: CLI can't find workflow.

**Solution**: Check namespace:
```bash
# List workflows in default namespace
temporal workflow list --namespace default

# Describe specific workflow
temporal workflow describe --workflow-id <id> --namespace default
```

## Test Architecture

The e2e tests use this architecture:

```
Python (Trio)
    ↓
TrioBridgeWrapper (Python)
    ↓
TrioAsyncBridge (Rust/PyO3)
    ↓
CoreWorkerHandle (Rust)
    ↓
Temporal SDK Core (Rust/Tokio)
    ↓
Temporal Server (gRPC)
```

Tests validate this entire stack works correctly.

## Continuous Integration

For CI environments without a Temporal server:

```bash
# Run all tests except e2e
uv run pytest -v -m "not temporal_server"
```

To include e2e tests in CI, you'll need to:
1. Start temporal dev server as a service
2. Wait for it to be ready
3. Run e2e tests
4. Clean up

Example GitHub Actions snippet:
```yaml
- name: Start Temporal Server
  run: |
    temporal server start-dev &
    sleep 5  # Wait for server to be ready

- name: Run E2E Tests
  run: uv run pytest -v -m temporal_server
```

## Development Workflow

When developing, you typically want to:

1. **Start Temporal server** (once, keep it running):
   ```bash
   temporal server start-dev
   ```

2. **Run unit tests** (fast, no server required):
   ```bash
   uv run pytest -v -m "not temporal_server"
   ```

3. **Run e2e tests** (when validating integration):
   ```bash
   uv run pytest -v -m temporal_server
   ```

4. **Run all tests**:
   ```bash
   uv run pytest -v
   ```
