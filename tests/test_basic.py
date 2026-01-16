"""Basic tests for temporalio_trio package."""

import pytest
import trio

import temporalio_trio


def test_version() -> None:
    """Test that version is set."""
    assert temporalio_trio.__version__ == "0.1.0"


@pytest.mark.trio
async def test_trio_works() -> None:
    """Test that trio async tests work."""
    result = []

    async def task(name: str) -> None:
        result.append(f"{name}-start")
        await trio.sleep(0)
        result.append(f"{name}-end")

    async with trio.open_nursery() as nursery:
        nursery.start_soon(task, "a")
        nursery.start_soon(task, "b")

    assert len(result) == 4
    assert "a-start" in result
    assert "b-start" in result
