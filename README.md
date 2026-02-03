# Temporal Python SDK with Trio Support

> **EXPERIMENTAL** - This SDK is functional but not yet production-ready. Use for testing and evaluation.

## Overview

This repository contains an experimental implementation of the [Temporal](https://temporal.io) Python SDK using [Trio](https://trio.readthedocs.io/) instead of asyncio. The goal is to leverage Trio's structured concurrency model for deterministic workflow execution.

## Current Status

**Functional with significant feature coverage:**

| Feature | Status |
|---------|--------|
| Workflow execution | ✅ Working |
| Timers (`workflow.sleep()`) | ✅ Working |
| Activities | ✅ Working |
| Child workflows | ✅ Working |
| Signals | ✅ Working |
| Queries | ✅ Working |
| Continue-as-new | ✅ Working |
| Workflow cancellation | ✅ Working |
| Replay/determinism | ✅ Working |
| Signal external workflow | ⚠️ Bridge only |
| Upsert search attributes | ⚠️ Bridge only |

**Test Coverage:** 545 tests passing (unit + E2E)

### Architecture

The SDK uses a pure async bridge between Trio and Rust/Tokio:

```
┌─────────────────────────────────────────┐
│ Trio Layer (Pure Trio)                  │
│   ├─ workflow.py (public API)           │
│   ├─ SingleThreadWorker                 │
│   └─ TrioBridgeWrapper                  │
└────────────┬────────────────────────────┘
             │ (async callbacks)
┌────────────▼────────────────────────────┐
│ Rust Bridge (PyO3 + Tokio)              │
│   └─ temporalio-sdk-core                │
└─────────────────────────────────────────┘
```

**Key features:**
- No `trio-asyncio` dependency (pure async bridge)
- Single Rust thread for all I/O
- Event-based workflow suspension
- Deterministic scheduling via Trio fork

### Prerequisites

This SDK depends on a Trio fork with deterministic scheduling:
- [Trio Fork](https://github.com/mfateev/trio/tree/temporal-deterministic-scheduling)

## Installation

```bash
# Not yet published to PyPI
pip install git+https://github.com/mfateev/python-trio-sdk.git
```

## Quick Start

```python
import trio
from temporalio_trio import workflow
from temporalio_trio.worker import Worker
from temporalio_trio.client import Client

@workflow.defn
class GreetingWorkflow:
    @workflow.run
    async def run(self, name: str) -> str:
        await workflow.sleep(1)  # Deterministic timer
        return f"Hello, {name}!"

async def main():
    client = await Client.connect("localhost:7233")
    worker = Worker(
        client=client,
        task_queue="greeting-queue",
        workflows=[GreetingWorkflow],
    )
    await worker.run()

trio.run(main)
```

## Development

### Setup

```bash
# Clone the repository
git clone https://github.com/mfateev/python-trio-sdk.git
cd python-trio-sdk

# Install dependencies
uv sync --all-groups

# Build Rust bridge (required)
cd temporalio_trio_bridge && maturin develop --release && cd ..
```

### Testing

**Run all tests (requires Temporal server):**
```bash
# Terminal 1: Start Temporal dev server
temporal server start-dev

# Terminal 2: Run all tests
uv run pytest -v
```

**Run only unit tests (no server required):**
```bash
uv run pytest -v -m "not temporal_server"
```

**Run E2E tests only:**
```bash
uv run pytest -v -m temporal_server
```

### Linting

```bash
uv run poe lint     # Run all linters
uv run poe format   # Auto-fix formatting
```

## Documentation

| Document | Description |
|----------|-------------|
| [docs/GAP_ANALYSIS.md](docs/GAP_ANALYSIS.md) | Current feature gaps and priorities |
| [docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md) | Original POC implementation plan |
| [MIGRATION_PLAN.md](MIGRATION_PLAN.md) | trio-asyncio migration (complete) |
| [tests/E2E_TESTING.md](tests/E2E_TESTING.md) | E2E test guide |

## Related Projects

- [Temporal Python SDK](https://github.com/temporalio/sdk-python) - Official asyncio-based SDK
- [Trio](https://github.com/python-trio/trio) - Async library for Python
- [Trio Deterministic Scheduling Proposal](https://github.com/mfateev/trio/blob/temporal-deterministic-scheduling/TRIO_PROPOSAL.md)

## License

MIT License - see [LICENSE](LICENSE) for details.

## Disclaimer

This is an experimental project and is not officially supported by Temporal Technologies Inc.
