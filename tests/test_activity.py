"""Tests for activity definition and context."""

import pytest
import trio

from temporalio_trio import activity


class TestActivityDefnDecorator:
    """Tests for @activity.defn decorator."""

    def test_defn_creates_definition(self) -> None:
        """Test @activity.defn creates definition."""

        @activity.defn
        async def my_activity() -> str:
            return "done"

        defn = activity._Definition.from_callable(my_activity)
        assert defn is not None
        assert defn.name == "my_activity"
        assert defn.fn is my_activity
        assert defn.is_async is True

    def test_defn_with_custom_name(self) -> None:
        """Test @activity.defn with custom name."""

        @activity.defn(name="CustomActivity")
        async def my_activity() -> str:
            return "done"

        defn = activity._Definition.from_callable(my_activity)
        assert defn is not None
        assert defn.name == "CustomActivity"

    def test_defn_requires_async(self) -> None:
        """Test @activity.defn requires async function."""
        with pytest.raises(TypeError, match="must be async"):

            @activity.defn
            def sync_activity() -> str:
                return "done"

    def test_defn_without_parentheses(self) -> None:
        """Test @activity.defn works without parentheses."""

        @activity.defn
        async def my_activity() -> str:
            return "done"

        defn = activity._Definition.from_callable(my_activity)
        assert defn is not None
        assert defn.name == "my_activity"

    def test_defn_with_parentheses(self) -> None:
        """Test @activity.defn works with parentheses."""

        @activity.defn()
        async def my_activity() -> str:
            return "done"

        defn = activity._Definition.from_callable(my_activity)
        assert defn is not None

    def test_defn_preserves_function(self) -> None:
        """Test @activity.defn preserves the original function."""

        @activity.defn
        async def my_activity(x: int) -> int:
            return x * 2

        assert my_activity.__name__ == "my_activity"

    def test_defn_rejects_duplicate_definition(self) -> None:
        """Test @activity.defn rejects already decorated functions."""

        @activity.defn
        async def my_activity() -> str:
            return "done"

        with pytest.raises(ValueError, match="already contains activity definition"):

            @activity.defn
            async def wrapped() -> str:
                return "done"

            # Try to apply again
            activity.defn(wrapped)

    def test_defn_rejects_keyword_only_args(self) -> None:
        """Test @activity.defn rejects keyword-only arguments."""
        with pytest.raises(TypeError, match="cannot have keyword-only arguments"):

            @activity.defn
            async def bad_activity(*, required_kwarg: str) -> str:
                return required_kwarg


class TestActivityDefinition:
    """Tests for _Definition class."""

    def test_from_callable_returns_none_for_non_activity(self) -> None:
        """Test from_callable returns None for non-activity functions."""

        async def not_an_activity() -> str:
            return "done"

        defn = activity._Definition.from_callable(not_an_activity)
        assert defn is None

    def test_must_from_callable_raises_for_non_activity(self) -> None:
        """Test must_from_callable raises for non-activity functions."""

        async def not_an_activity() -> str:
            return "done"

        with pytest.raises(TypeError, match="missing attributes"):
            activity._Definition.must_from_callable(not_an_activity)

    def test_definition_extracts_type_hints(self) -> None:
        """Test definition extracts arg and return type hints."""

        @activity.defn
        async def typed_activity(name: str, count: int) -> str:
            return f"{name} x {count}"

        defn = activity._Definition.from_callable(typed_activity)
        assert defn is not None
        assert defn.arg_types is not None
        assert len(defn.arg_types) == 2
        assert defn.ret_type == str


class TestActivityContext:
    """Tests for activity context."""

    def test_in_activity_returns_false_outside_context(self) -> None:
        """Test in_activity returns False when not in activity context."""
        assert activity.in_activity() is False

    def test_info_raises_outside_context(self) -> None:
        """Test info() raises when not in activity context."""
        with pytest.raises(RuntimeError, match="Not in activity context"):
            activity.info()

    def test_heartbeat_raises_outside_context(self) -> None:
        """Test heartbeat() raises when not in activity context."""
        with pytest.raises(RuntimeError, match="Not in activity context"):
            activity.heartbeat()

    def test_is_cancelled_raises_outside_context(self) -> None:
        """Test is_cancelled() raises when not in activity context."""
        with pytest.raises(RuntimeError, match="Not in activity context"):
            activity.is_cancelled()

    def test_is_worker_shutdown_raises_outside_context(self) -> None:
        """Test is_worker_shutdown() raises when not in activity context."""
        with pytest.raises(RuntimeError, match="Not in activity context"):
            activity.is_worker_shutdown()


@pytest.mark.trio
class TestTrioEvent:
    """Tests for _TrioEvent wrapper."""

    async def test_trio_event_set_and_is_set(self) -> None:
        """Test _TrioEvent.set() and is_set()."""
        event = activity._TrioEvent(trio.Event())

        assert event.is_set() is False
        event.set()
        assert event.is_set() is True

    async def test_trio_event_wait(self) -> None:
        """Test _TrioEvent.wait()."""
        event = activity._TrioEvent(trio.Event())

        async def setter():
            await trio.sleep(0.01)
            event.set()

        async with trio.open_nursery() as nursery:
            nursery.start_soon(setter)
            await event.wait()

        assert event.is_set() is True


@pytest.mark.trio
class TestActivityContextIntegration:
    """Integration tests for activity context."""

    async def test_context_provides_info(self) -> None:
        """Test context provides info() correctly."""
        from datetime import datetime, timezone

        import temporalio.common
        import temporalio.converter

        info = activity.Info(
            activity_id="test-id",
            activity_type="test_activity",
            attempt=1,
            current_attempt_scheduled_time=datetime.now(timezone.utc),
            heartbeat_details=[],
            heartbeat_timeout=None,
            is_local=False,
            schedule_to_close_timeout=None,
            scheduled_time=datetime.now(timezone.utc),
            start_to_close_timeout=None,
            started_time=datetime.now(timezone.utc),
            task_queue="test-queue",
            task_token=b"test-token",
            workflow_id="workflow-123",
            workflow_namespace="default",
            workflow_run_id="run-456",
            workflow_type="TestWorkflow",
            priority=temporalio.common.Priority.default,
            retry_policy=None,
        )

        cancelled_event = activity._TrioEvent(trio.Event())
        shutdown_event = activity._TrioEvent(trio.Event())

        context = activity._Context(
            info=lambda: info,
            heartbeat=lambda *args: None,
            cancelled_event=cancelled_event,
            worker_shutdown_event=shutdown_event,
            payload_converter_class_or_instance=temporalio.converter.DataConverter.default.payload_converter,
        )

        token = activity._Context.set(context)
        try:
            assert activity.in_activity() is True

            retrieved_info = activity.info()
            assert retrieved_info.activity_id == "test-id"
            assert retrieved_info.activity_type == "test_activity"
            assert retrieved_info.task_queue == "test-queue"

            # Test is_cancelled
            assert activity.is_cancelled() is False
            cancelled_event.set()
            assert activity.is_cancelled() is True

            # Test is_worker_shutdown
            assert activity.is_worker_shutdown() is False
            shutdown_event.set()
            assert activity.is_worker_shutdown() is True
        finally:
            activity._Context.reset(token)

        # After reset, should be outside context
        assert activity.in_activity() is False

    async def test_context_heartbeat(self) -> None:
        """Test context heartbeat callback."""
        from datetime import datetime, timezone

        import temporalio.common
        import temporalio.converter

        heartbeat_calls = []

        def track_heartbeat(*details):
            heartbeat_calls.append(details)

        info = activity.Info(
            activity_id="test-id",
            activity_type="test_activity",
            attempt=1,
            current_attempt_scheduled_time=datetime.now(timezone.utc),
            heartbeat_details=[],
            heartbeat_timeout=None,
            is_local=False,
            schedule_to_close_timeout=None,
            scheduled_time=datetime.now(timezone.utc),
            start_to_close_timeout=None,
            started_time=datetime.now(timezone.utc),
            task_queue="test-queue",
            task_token=b"test-token",
            workflow_id="workflow-123",
            workflow_namespace="default",
            workflow_run_id="run-456",
            workflow_type="TestWorkflow",
            priority=temporalio.common.Priority.default,
            retry_policy=None,
        )

        context = activity._Context(
            info=lambda: info,
            heartbeat=track_heartbeat,
            cancelled_event=activity._TrioEvent(trio.Event()),
            worker_shutdown_event=activity._TrioEvent(trio.Event()),
            payload_converter_class_or_instance=temporalio.converter.DataConverter.default.payload_converter,
        )

        token = activity._Context.set(context)
        try:
            activity.heartbeat("progress", 50)
            activity.heartbeat("done")

            assert len(heartbeat_calls) == 2
            assert heartbeat_calls[0] == ("progress", 50)
            assert heartbeat_calls[1] == ("done",)
        finally:
            activity._Context.reset(token)


class TestLoggerAdapter:
    """Tests for LoggerAdapter."""

    def test_logger_adapter_created(self) -> None:
        """Test logger adapter is available."""
        assert activity.logger is not None
        assert isinstance(activity.logger, activity.LoggerAdapter)

    def test_logger_adapter_has_base_logger(self) -> None:
        """Test logger adapter has accessible base logger."""
        assert activity.logger.base_logger is not None
