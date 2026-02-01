# Temporal Python SDK with Trio Support

> **EXPERIMENTAL** - This is an experimental project exploring the use of [Trio](https://trio.readthedocs.io/) as the async runtime for the Temporal Python SDK.

## Overview

This repository contains an experimental implementation of the [Temporal](https://temporal.io) Python SDK using Trio instead of asyncio. The goal is to leverage Trio's structured concurrency model for workflow execution.

## Status

This project is in early experimental stages and is **not ready for production use**.

### Why Trio?

- **Structured Concurrency**: Trio's nursery-based task management aligns well with Temporal's workflow model
- **Deterministic Scheduling**: Trio can be configured for deterministic task ordering, essential for workflow replay
- **Cancellation Semantics**: Trio's cancel scopes provide clear cancellation boundaries

### Prerequisites

This SDK depends on proposed changes to Trio for per-runner deterministic scheduling:
- [Trio Fork with Deterministic Scheduling](https://github.com/mfateev/trio/tree/temporal-deterministic-scheduling)

## Installation

```bash
# Not yet published to PyPI
pip install git+https://github.com/mfateev/python-trio-sdk.git
```

## Development

This project uses:
- [uv](https://github.com/astral-sh/uv) for package management
- [poethepoet](https://github.com/nat-n/poethepoet) for task running
- [ruff](https://github.com/astral-sh/ruff) for linting/formatting
- [pytest](https://pytest.org/) with pytest-trio for testing

### Setup

```bash
# Clone the repository
git clone https://github.com/mfateev/python-trio-sdk.git
cd python-trio-sdk

# Install dependencies
uv sync --all-groups

# Run tests
uv run poe test

# Format code
uv run poe format

# Lint
uv run poe lint
```

### Testing

The project includes both unit tests and end-to-end integration tests.

**Run all tests:**
```bash
uv run pytest -v
```

**Run only unit tests (no server required):**
```bash
uv run pytest -v -m "not temporal_server"
```

**Run end-to-end tests (requires Temporal server):**
```bash
# Terminal 1: Start Temporal dev server
temporal server start-dev

# Terminal 2: Run e2e tests
uv run pytest -v -m temporal_server
```

For detailed information about e2e tests, see [tests/E2E_TESTING.md](tests/E2E_TESTING.md).

## Related Projects

- [Temporal Python SDK](https://github.com/temporalio/sdk-python) - Official asyncio-based SDK
- [Trio](https://github.com/python-trio/trio) - Async library for Python
- [Trio Deterministic Scheduling Proposal](https://github.com/mfateev/trio/blob/temporal-deterministic-scheduling/TRIO_PROPOSAL.md)

## License

MIT License - see [LICENSE](LICENSE) for details.

## Disclaimer

This is an experimental project and is not officially supported by Temporal Technologies Inc.
