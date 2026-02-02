# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This is an experimental implementation of the Temporal Python SDK using [Trio](https://trio.readthedocs.io/) instead of asyncio as the async runtime. The goal is to leverage Trio's structured concurrency model for workflow execution with deterministic scheduling.

**Status**: Experimental, not ready for production use.

## Sprite Checkpoints (DO NOT USE)

Do not create Sprite environment checkpoints. All code is saved when pushed to git. Git commits and pushes are the source of truth for code persistence.

## Git Workflow (IMPORTANT)

**The `main` branch is protected.** All changes must go through pull requests.

### Branch Strategy
```bash
# Always work from a feature branch, never commit directly to main
git checkout -b feature/my-feature

# Or use the existing task branch
git checkout task/trio-asyncio
```

### Making Changes
```bash
# 1. Create or switch to feature branch
git checkout -b feature/description

# 2. Make changes, test, lint
uv run pytest -v -m "not temporal_server"
uv run poe lint
uv run poe format

# 3. Commit changes
git add <files>
git commit -m "feat(component): description"

# 4. Push branch
git push -u origin feature/description

# 5. Create PR to merge into main
gh pr create --base main --title "Description" --body "Details"
```

### Pull Request Requirements
- All tests must pass
- Code must be linted and formatted
- PR description should explain the changes
- Use `gh pr create` to create PRs from the command line

## Essential Commands

### Development Setup
```bash
# Install dependencies (uses uv package manager)
uv sync --all-groups

# Install in editable mode
uv pip install -e .
```

### Building the Rust Bridge
```bash
# Build and install the Rust bridge (PyO3)
cd temporalio_trio_bridge
cargo build --release
maturin develop --release
cd ..
```

### Testing
```bash
# Run all tests
uv run pytest

# Run with coverage (target 90%+)
uv run pytest --cov=temporalio_trio --cov-report=term-missing tests/

# Run only unit tests (no Temporal server required)
uv run pytest -v -m "not temporal_server"

# Run e2e tests (requires Temporal server)
# Terminal 1: temporal server start-dev
# Terminal 2:
uv run pytest -v -m temporal_server

# Run specific test
uv run pytest tests/test_bridge.py::test_poll_activation -v
```

### Linting and Formatting
```bash
# Format code
uv run poe format

# Run all linters (pyright, mypy, ruff)
uv run poe lint

# Run type checkers only
uv run poe lint-types
```

### Task Runners (poethepoet)
```bash
uv run poe test     # Run tests
uv run poe format   # Auto-fix formatting
uv run poe lint     # Run all linters
```

## Architecture

### High-Level Structure

This SDK implements a **fully async bridge** between Trio (Python) and Tokio (Rust), eliminating the need for `trio-asyncio`. The architecture uses:

1. **Rust Bridge** (`temporalio_trio_bridge/`): PyO3 module with single Tokio thread
2. **Python Wrapper** (`temporalio_trio/_async_bridge.py`): Trio-friendly wrapper
3. **Worker Implementation** (`temporalio_trio/worker/`): Workflow execution runtime
4. **Workflow API** (`temporalio_trio/workflow.py`): Public API matching SDK patterns

### Async Bridge Pattern (Critical)

The bridge achieves true async on both sides without blocking threads:

```
Python/Trio Task
    ↓ (sends request)
TrioBridgeWrapper
    ↓ (creates trio.Event)
TrioAsyncBridge (Rust/PyO3)
    ↓ (single thread + Tokio runtime)
Temporal SDK Core (Rust)
    ↓ (delivers result via trio.from_thread callback)
Trio Event fires
    ↓
Result returned to Python
```

**Key Components:**
- **Request Queue**: Python → Rust (non-blocking)
- **Trio Events**: For async waiting (no blocked threads)
- **trio.from_thread**: Rust → Python result delivery
- **Single Rust Thread**: Handles all I/O via Tokio

### Package Structure

```
temporalio_trio/
├── __init__.py              # Public exports
├── workflow.py              # @workflow.defn, @workflow.run, sleep(), time()
├── _async_bridge.py         # TrioBridgeWrapper (Trio ↔ Rust bridge)
└── worker/
    ├── __init__.py          # Worker exports
    ├── _worker.py           # Worker class (high-level API)
    ├── _single_thread_worker.py  # SingleThreadWorker (event-based execution)
    ├── _runtime.py          # WorkflowRuntime (contextvar-isolated state)
    ├── _workflow_state.py   # WorkflowState (per-workflow coordination)
    ├── _workflow_instance.py # TrioWorkflowInstance (workflow execution)
    ├── _clock.py            # WorkflowClock (deterministic time)
    ├── _activation.py       # Activation processing
    └── _bridge_types.py     # Bridge type conversions

temporalio_trio_bridge/      # Rust bridge (PyO3)
├── Cargo.toml
└── src/
    ├── lib.rs               # PyO3 module definition
    ├── bridge.rs            # TrioAsyncBridge
    ├── core_worker.rs       # SDK Core integration
    └── types.rs             # Type conversions

sdk-core/                    # Git submodule (temporalio-sdk-core)
```

### Runtime Pattern (Matches SDK)

All workflow APIs delegate to `_Runtime.current()`:

```python
# Public API (workflow.py)
async def sleep(duration: float) -> None:
    await _Runtime.current().workflow_sleep(duration)

def time() -> float:
    return _Runtime.current().workflow_time_ns() / 1e9
```

**Context Management**: Uses `contextvars.ContextVar` instead of asyncio event loop attachment.

### Workflow Execution Flow

All workflows execute within a **single `trio.run()` call** using FIFO scheduling and contextvar-based isolation:

1. **Worker.run()** calls `trio.run(deterministic=True, fifo=True)` with the worker main task
2. **SingleThreadWorker** polls for activations via the bridge (non-blocking)
3. **New workflow** → spawns task in shared nursery with isolated `WorkflowRuntime` contextvar
4. **Existing workflow** → delivers activation, wakes suspended `trio.Event`
5. **Workflow suspends** on `trio.Event` when waiting for timers/activities/child workflows
6. **Activation received** → fires event, workflow resumes
7. **Completion** sent back to bridge via `complete_workflow_activation()`

**Key benefits:**
- No thread pool overhead (all workflows in single thread)
- Workflows stay alive between activations (event-based suspension)
- FIFO scheduling ensures deterministic task ordering
- ContextVar isolation prevents workflows from interfering

### Deterministic Scheduling (Critical Feature)

Workflows run with deterministic task scheduling via a custom Trio fork:
- Fork: `github.com/mfateev/trio` (branch: `temporal-deterministic-scheduling`)
- Feature: `trio.run(deterministic=True, fifo=True)`
- FIFO mode: Tasks execute in creation order (no random shuffling)
- Purpose: Ensures workflow replay produces identical results regardless of other workflows

## Key Design Principles

### 1. Match SDK Patterns (CRITICAL)

Follow the official Temporal Python SDK for ALL design decisions. Only difference should be Trio vs asyncio.

**Reference SDK files when implementing features:**
- `temporalio/workflow.py` - Workflow API patterns
- `temporalio/worker/_workflow_instance.py` - WorkflowRunner pattern
- `temporalio/worker/_replayer.py` - Replay patterns

### 2. No trio-asyncio Dependency

The bridge was migrated from trio-asyncio to the fully async pattern (see `ASYNC_BRIDGE_DESIGN.md` and `MIGRATION_PLAN.md`). Never reintroduce trio-asyncio.

### 3. Runtime via ContextVar

Use `contextvars.ContextVar` for `_Runtime.current()`, not asyncio loop attachment.

### 4. Structured Concurrency

Trio's nursery-based task management aligns well with Temporal workflows. Use nurseries for child workflows and concurrent operations.

## Testing Guidelines

### All Tests Must Pass (CRITICAL)

**You are NOT done until ALL tests pass.** This is a hard requirement:

- Run `uv run pytest` before considering any task complete
- Zero failures are acceptable - not 2, not 1, ZERO
- Skipped tests are acceptable only for clearly documented unimplemented features
- If tests fail, fix the underlying code, not the tests

### Test Coverage Requirements

- **Target**: 90%+ coverage on new code
- **Run**: `uv run pytest --cov=temporalio_trio --cov-report=term-missing tests/`
- **Before committing**: Verify coverage meets target AND all tests pass

### Test Types

1. **Unit Tests** (`tests/test_*.py`): Fast, no server required
2. **E2E Tests** (`tests/test_e2e_*.py`): Require Temporal server, marked with `@pytest.mark.temporal_server`

### E2E Testing

See `tests/E2E_TESTING.md` for details.

**Quick start:**
```bash
# Terminal 1: Start server
temporal server start-dev

# Terminal 2: Run e2e tests
uv run pytest -v -m temporal_server
```

**Note**: Temporal CLI must be in PATH. If using custom location:
```bash
export PATH="/home/sprite/workarea/bin:$PATH"
```

### Handling Test Failures (CRITICAL)

**NEVER fix or remove a test to workaround a failure.** Tests exist to catch bugs and regressions. When a test fails:

1. **Investigate the root cause** - Why is the test failing? What changed?
2. **Fix the underlying issue** in the tested functionality, not the test
3. **Understand the test's intent** - What behavior is it validating?
4. **Only modify tests if they are incorrect** - And only after user confirmation

**Process when encountering test failures:**

```bash
# 1. Run the failing test to understand the failure
uv run pytest tests/test_file.py::test_name -vv

# 2. Investigate the root cause in the implementation
# Read the code being tested, understand what changed

# 3. Fix the implementation to make the test pass
# Make changes to the actual code, not the test

# 4. Verify the fix
uv run pytest tests/test_file.py::test_name -vv

# 5. Run all tests to ensure no regressions
uv run pytest
```

**If you believe a test is incorrect or should be removed:**
- **Stop and ask the user for confirmation** before modifying or removing it
- Explain why you think the test is wrong
- Propose what should change and why
- Wait for explicit approval before proceeding

**Common scenarios:**

- ❌ Test fails after code change → Remove assertions to make it pass
- ✅ Test fails after code change → Fix the code change to maintain correct behavior
- ❌ Test fails in CI → Skip the test with `@pytest.mark.skip`
- ✅ Test fails in CI → Investigate why it fails and fix the underlying issue
- ❌ Test is hard to fix → Modify test expectations to match new behavior
- ✅ Test is hard to fix → Ask user if the new behavior is correct, then either fix code or update test with approval

Tests are the safety net that prevents regressions. Respect them.

### E2E Shutdown Race Condition (RESOLVED)

The previous multi-threaded architecture had a shutdown race condition. This was resolved by the single-threaded `SingleThreadWorker` architecture which:
- Handles all workflows in a single `trio.run()` context
- Uses proper shutdown coordination via cancel scopes
- Drains in-flight activations before completing shutdown

If E2E tests show shutdown-related failures, investigate the `SingleThreadWorker` shutdown logic in `temporalio_trio/worker/_single_thread_worker.py`.

## Development Workflow

### Making Changes

1. **Read existing implementation** before modifying
2. **Run tests** to understand current behavior
3. **Make changes** incrementally
4. **Test with coverage**: `uv run pytest --cov=temporalio_trio`
5. **Lint**: `uv run poe lint`
6. **Format**: `uv run poe format`
7. **Commit** with descriptive message

### Committing Code

Before each commit:
```bash
# 1. Run tests with coverage (verify 90%+ on new code)
uv run pytest --cov=temporalio_trio --cov-report=term-missing tests/

# 2. Run lint checks
uv run poe lint

# 3. Format code
uv run poe format

# 4. Commit
git add <files>
git commit -m "feat(component): description"
```

### Debugging

```bash
# Run tests with verbose output
uv run pytest -vv -s tests/test_file.py::test_name

# Debug level logging
uv run pytest tests/ --log-cli-level=DEBUG

# Stop on first failure
uv run pytest -x tests/
```

## Bridge Development

### Modifying Rust Bridge

When changing `temporalio_trio_bridge/`:

1. **Make Rust changes** in `temporalio_trio_bridge/src/`
2. **Rebuild**: `cd temporalio_trio_bridge && maturin develop --release`
3. **Test**: `uv run pytest tests/test_bridge.py`
4. **Verify no memory leaks**: Long-running worker tests

### Common Bridge Operations

The bridge exposes these core operations:
- `initialize_with_client()` - Connect to Temporal server
- `poll_workflow_activation()` - Poll for work (async)
- `complete_workflow_activation()` - Send completion (async)
- `validate()` - Validate configuration
- `initiate_shutdown()` - Begin shutdown (sync)
- `finalize_shutdown()` - Complete shutdown (async)

### Bridge Performance

- **Single Rust thread** handles all I/O
- **No thread-per-operation** overhead
- **Overhead per operation**: ~100μs (negligible for workflows)
- **Scales to 500+ concurrent workflows** with one thread

## Important Files and Concepts

### Core Files (Must Read Before Changes)

- `temporalio_trio/workflow.py` - Public workflow API, _Runtime pattern
- `temporalio_trio/_async_bridge.py` - Bridge wrapper, async pattern
- `temporalio_trio/worker/_workflow_instance.py` - Workflow execution
- `temporalio_trio/worker/_worker.py` - Worker high-level API

### Documentation Files

- `ASYNC_BRIDGE_DESIGN.md` - Bridge architecture (proven by POC)
- `MIGRATION_PLAN.md` - Migration from trio-asyncio (completed)
- `POC_RESULTS.md` - POC validation results
- `tests/E2E_TESTING.md` - E2E test guide

### Configuration Files

- `pyproject.toml` - Dependencies, tool config, poe tasks
- `temporalio_trio_bridge/Cargo.toml` - Rust dependencies

## Dependencies

### Python Dependencies
- `trio` (custom fork with deterministic scheduling)
- `attrs`, `outcome` (Trio ecosystem)
- `temporalio` (official SDK - for types and bridge protocol)
- `protobuf` (protocol buffers)

### Rust Dependencies
- `pyo3` (Python bindings)
- `tokio` (async runtime)
- `temporalio-sdk-core` (from git submodule)
- `temporalio-client`, `temporalio-common`

### Development Dependencies
- `pytest`, `pytest-trio`, `pytest-cov`, `pytest-timeout`
- `pyright`, `mypy` (type checking)
- `ruff` (linting/formatting)
- `poethepoet` (task runner)

## Trio Fork Details

This project uses a custom Trio fork:
- **Repo**: `github.com/mfateev/trio`
- **Branch**: `temporal-deterministic-scheduling`
- **Feature**: Per-runner deterministic scheduling with seed control
- **Purpose**: Ensures workflow replay produces identical task execution order

## Common Pitfalls

### 1. Don't Use trio-asyncio
The bridge was migrated away from trio-asyncio. Never import or use it.

### 2. Don't Use Asyncio Event Loop Patterns
Use `contextvars.ContextVar` for context, not asyncio loop attachment.

### 3. Always Rebuild After Rust Changes
After changing Rust code: `cd temporalio_trio_bridge && maturin develop --release`

### 4. Test Coverage Before Committing
Run `uv run pytest --cov=temporalio_trio` and verify 90%+ coverage on new code.

### 5. E2E Tests Require Server
Tests marked with `@pytest.mark.temporal_server` need a running Temporal server.

## Experimental Status

This is an experimental project:
- **Not production-ready**
- **APIs may change**
- **Testing phase only**
- **No official Temporal support**

The goal is to validate Trio's structured concurrency model for Temporal workflows before proposing upstream changes.
