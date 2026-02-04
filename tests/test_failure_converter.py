"""Tests for failure converter.

Tests the conversion of Temporal Failure protobuf messages to Python exception types.
"""

import pytest
import temporalio.api.common.v1
import temporalio.api.failure.v1
import temporalio.converter
from temporalio.exceptions import (
    ActivityError,
    ApplicationError,
    CancelledError,
    ChildWorkflowError,
    FailureError,
    RetryState,
    ServerError,
    TerminatedError,
    TimeoutError,
    TimeoutType,
)

from temporalio_trio.worker._failure_converter import failure_to_exception


@pytest.fixture
def payload_converter():
    """Get the default payload converter."""
    return temporalio.converter.DataConverter().payload_converter


class TestApplicationError:
    """Tests for ApplicationError conversion."""

    def test_basic_application_error(self, payload_converter):
        """Test converting a basic application failure."""
        failure = temporalio.api.failure.v1.Failure()
        failure.message = "Something went wrong"
        failure.application_failure_info.type = "CustomError"
        failure.application_failure_info.non_retryable = False

        exc = failure_to_exception(failure, payload_converter)

        assert isinstance(exc, ApplicationError)
        # ApplicationError str() includes the type prefix: "CustomError: Something went wrong"
        assert "Something went wrong" in str(exc)
        assert exc.type == "CustomError"
        assert exc.non_retryable is False

    def test_non_retryable_application_error(self, payload_converter):
        """Test converting a non-retryable application failure."""
        failure = temporalio.api.failure.v1.Failure()
        failure.message = "Fatal error"
        failure.application_failure_info.type = "FatalError"
        failure.application_failure_info.non_retryable = True

        exc = failure_to_exception(failure, payload_converter)

        assert isinstance(exc, ApplicationError)
        assert exc.non_retryable is True

    def test_application_error_with_details(self, payload_converter):
        """Test converting application failure with details."""
        failure = temporalio.api.failure.v1.Failure()
        failure.message = "Error with details"
        failure.application_failure_info.type = "DetailedError"

        # Add details payload
        detail_payload = temporalio.api.common.v1.Payload()
        detail_payload.data = b'"extra-info"'
        detail_payload.metadata["encoding"] = b"json/plain"
        failure.application_failure_info.details.payloads.append(detail_payload)

        exc = failure_to_exception(failure, payload_converter)

        assert isinstance(exc, ApplicationError)
        assert exc.details == ("extra-info",)


class TestActivityError:
    """Tests for ActivityError conversion."""

    def test_basic_activity_error(self, payload_converter):
        """Test converting a basic activity failure."""
        failure = temporalio.api.failure.v1.Failure()
        failure.message = "Activity failed"
        failure.activity_failure_info.scheduled_event_id = 10
        failure.activity_failure_info.started_event_id = 11
        failure.activity_failure_info.identity = "worker-123"
        failure.activity_failure_info.activity_type.name = "my_activity"
        failure.activity_failure_info.activity_id = "activity-1"
        failure.activity_failure_info.retry_state = (
            temporalio.api.enums.v1.RetryState.RETRY_STATE_MAXIMUM_ATTEMPTS_REACHED
        )

        exc = failure_to_exception(failure, payload_converter)

        assert isinstance(exc, ActivityError)
        assert exc.scheduled_event_id == 10
        assert exc.started_event_id == 11
        assert exc.identity == "worker-123"
        assert exc.activity_type == "my_activity"
        assert exc.activity_id == "activity-1"
        assert exc.retry_state == RetryState.MAXIMUM_ATTEMPTS_REACHED

    def test_activity_error_with_cause(self, payload_converter):
        """Test that activity errors preserve the cause chain."""
        failure = temporalio.api.failure.v1.Failure()
        failure.message = "Activity failed"
        failure.activity_failure_info.activity_type.name = "my_activity"
        failure.activity_failure_info.activity_id = "activity-1"

        # Add cause as application error
        failure.cause.message = "Root cause error"
        failure.cause.application_failure_info.type = "RootCauseError"

        exc = failure_to_exception(failure, payload_converter)

        assert isinstance(exc, ActivityError)
        assert exc.__cause__ is not None
        assert isinstance(exc.__cause__, ApplicationError)
        # ApplicationError str() includes the type prefix
        assert "Root cause error" in str(exc.__cause__)
        assert exc.__cause__.type == "RootCauseError"


class TestChildWorkflowError:
    """Tests for ChildWorkflowError conversion."""

    def test_basic_child_workflow_error(self, payload_converter):
        """Test converting a basic child workflow failure."""
        failure = temporalio.api.failure.v1.Failure()
        failure.message = "Child workflow failed"
        failure.child_workflow_execution_failure_info.namespace = "default"
        failure.child_workflow_execution_failure_info.workflow_execution.workflow_id = (
            "child-workflow-1"
        )
        failure.child_workflow_execution_failure_info.workflow_execution.run_id = (
            "run-123"
        )
        failure.child_workflow_execution_failure_info.workflow_type.name = "ChildWorkflow"
        failure.child_workflow_execution_failure_info.initiated_event_id = 20
        failure.child_workflow_execution_failure_info.started_event_id = 21
        failure.child_workflow_execution_failure_info.retry_state = (
            temporalio.api.enums.v1.RetryState.RETRY_STATE_NON_RETRYABLE_FAILURE
        )

        exc = failure_to_exception(failure, payload_converter)

        assert isinstance(exc, ChildWorkflowError)
        assert exc.namespace == "default"
        assert exc.workflow_id == "child-workflow-1"
        assert exc.run_id == "run-123"
        assert exc.workflow_type == "ChildWorkflow"
        assert exc.initiated_event_id == 20
        assert exc.started_event_id == 21
        assert exc.retry_state == RetryState.NON_RETRYABLE_FAILURE

    def test_child_workflow_error_with_cause(self, payload_converter):
        """Test that child workflow errors preserve the cause chain."""
        failure = temporalio.api.failure.v1.Failure()
        failure.message = "Child workflow failed"
        failure.child_workflow_execution_failure_info.workflow_execution.workflow_id = (
            "child-1"
        )
        failure.child_workflow_execution_failure_info.workflow_type.name = "ChildWorkflow"

        # Add cause as application error from child
        failure.cause.message = "Child threw error"
        failure.cause.application_failure_info.type = "ChildError"

        exc = failure_to_exception(failure, payload_converter)

        assert isinstance(exc, ChildWorkflowError)
        assert exc.__cause__ is not None
        assert isinstance(exc.__cause__, ApplicationError)
        assert exc.__cause__.type == "ChildError"


class TestTimeoutError:
    """Tests for TimeoutError conversion."""

    def test_start_to_close_timeout(self, payload_converter):
        """Test converting a start-to-close timeout failure."""
        failure = temporalio.api.failure.v1.Failure()
        failure.message = "Activity timed out"
        failure.timeout_failure_info.timeout_type = (
            temporalio.api.enums.v1.TimeoutType.TIMEOUT_TYPE_START_TO_CLOSE
        )

        exc = failure_to_exception(failure, payload_converter)

        assert isinstance(exc, TimeoutError)
        assert exc.type == TimeoutType.START_TO_CLOSE

    def test_schedule_to_start_timeout(self, payload_converter):
        """Test converting a schedule-to-start timeout failure."""
        failure = temporalio.api.failure.v1.Failure()
        failure.message = "Activity not started in time"
        failure.timeout_failure_info.timeout_type = (
            temporalio.api.enums.v1.TimeoutType.TIMEOUT_TYPE_SCHEDULE_TO_START
        )

        exc = failure_to_exception(failure, payload_converter)

        assert isinstance(exc, TimeoutError)
        assert exc.type == TimeoutType.SCHEDULE_TO_START


class TestCancelledError:
    """Tests for CancelledError conversion."""

    def test_basic_cancelled_error(self, payload_converter):
        """Test converting a basic cancellation failure."""
        failure = temporalio.api.failure.v1.Failure()
        failure.message = "Operation was cancelled"
        failure.canceled_failure_info.SetInParent()

        exc = failure_to_exception(failure, payload_converter)

        assert isinstance(exc, CancelledError)
        assert "cancelled" in str(exc).lower()


class TestTerminatedError:
    """Tests for TerminatedError conversion."""

    def test_basic_terminated_error(self, payload_converter):
        """Test converting a termination failure."""
        failure = temporalio.api.failure.v1.Failure()
        failure.message = "Workflow was terminated"
        failure.terminated_failure_info.SetInParent()

        exc = failure_to_exception(failure, payload_converter)

        assert isinstance(exc, TerminatedError)


class TestServerError:
    """Tests for ServerError conversion."""

    def test_basic_server_error(self, payload_converter):
        """Test converting a server failure."""
        failure = temporalio.api.failure.v1.Failure()
        failure.message = "Internal server error"
        failure.server_failure_info.non_retryable = True

        exc = failure_to_exception(failure, payload_converter)

        assert isinstance(exc, ServerError)
        assert exc.non_retryable is True


class TestGenericFailure:
    """Tests for generic failure conversion."""

    def test_failure_without_info(self, payload_converter):
        """Test converting a failure without any specific info."""
        failure = temporalio.api.failure.v1.Failure()
        failure.message = "Unknown failure"

        exc = failure_to_exception(failure, payload_converter)

        assert isinstance(exc, FailureError)
        assert str(exc) == "Unknown failure"


class TestCauseChains:
    """Tests for cause chain preservation."""

    def test_deep_cause_chain(self, payload_converter):
        """Test that deep cause chains are properly preserved."""
        # Create: ActivityError -> ApplicationError -> ApplicationError
        failure = temporalio.api.failure.v1.Failure()
        failure.message = "Activity failed"
        failure.activity_failure_info.activity_type.name = "my_activity"
        failure.activity_failure_info.activity_id = "act-1"

        # First level cause
        failure.cause.message = "Application error"
        failure.cause.application_failure_info.type = "AppError"

        # Second level cause
        failure.cause.cause.message = "Root cause"
        failure.cause.cause.application_failure_info.type = "RootError"

        exc = failure_to_exception(failure, payload_converter)

        # Check the chain
        assert isinstance(exc, ActivityError)
        assert exc.__cause__ is not None
        assert isinstance(exc.__cause__, ApplicationError)
        assert exc.__cause__.type == "AppError"

        assert exc.__cause__.__cause__ is not None
        assert isinstance(exc.__cause__.__cause__, ApplicationError)
        assert exc.__cause__.__cause__.type == "RootError"


class TestBridgeTypeIntegration:
    """Tests for integration with bridge type conversion."""

    def test_activity_resolution_failed(self, payload_converter):
        """Test that activity resolution failure uses proper exception types."""
        import temporalio.bridge.proto.activity_result.activity_result_pb2 as act_result_pb
        import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb

        from temporalio_trio.worker._bridge_types import bridge_to_poc_activation

        # Create bridge activation with resolve_activity job (failed)
        bridge_act = act_pb.WorkflowActivation()
        bridge_act.run_id = "test-run-id"
        bridge_act.timestamp.seconds = 1000

        job = bridge_act.jobs.add()
        job.resolve_activity.seq = 1

        # Set up the failure
        job.resolve_activity.result.failed.failure.message = "Activity threw error"
        job.resolve_activity.result.failed.failure.activity_failure_info.activity_type.name = (
            "my_activity"
        )
        job.resolve_activity.result.failed.failure.activity_failure_info.activity_id = (
            "act-1"
        )

        # Add application error as cause
        job.resolve_activity.result.failed.failure.cause.message = "Custom error"
        job.resolve_activity.result.failed.failure.cause.application_failure_info.type = (
            "CustomError"
        )

        # Convert
        data_converter = temporalio.converter.DataConverter()
        poc_act = bridge_to_poc_activation(bridge_act, data_converter)

        # Verify the failure is properly typed
        assert len(poc_act.jobs) == 1
        activity_job = poc_act.jobs[0]
        assert activity_job.failure is not None
        assert isinstance(activity_job.failure, ActivityError)
        assert activity_job.failure.__cause__ is not None
        assert isinstance(activity_job.failure.__cause__, ApplicationError)
        assert activity_job.failure.__cause__.type == "CustomError"

    def test_child_workflow_resolution_failed(self, payload_converter):
        """Test that child workflow resolution failure uses proper exception types."""
        import temporalio.bridge.proto.workflow_activation.workflow_activation_pb2 as act_pb

        from temporalio_trio.worker._bridge_types import bridge_to_poc_activation

        # Create bridge activation with resolve_child_workflow_execution job (failed)
        bridge_act = act_pb.WorkflowActivation()
        bridge_act.run_id = "test-run-id"
        bridge_act.timestamp.seconds = 1000

        job = bridge_act.jobs.add()
        job.resolve_child_workflow_execution.seq = 1

        # Set up the failure
        job.resolve_child_workflow_execution.result.failed.failure.message = (
            "Child workflow threw error"
        )
        job.resolve_child_workflow_execution.result.failed.failure.child_workflow_execution_failure_info.workflow_type.name = (
            "ChildWorkflow"
        )
        job.resolve_child_workflow_execution.result.failed.failure.child_workflow_execution_failure_info.workflow_execution.workflow_id = (
            "child-1"
        )

        # Add application error as cause
        job.resolve_child_workflow_execution.result.failed.failure.cause.message = (
            "Child error"
        )
        job.resolve_child_workflow_execution.result.failed.failure.cause.application_failure_info.type = (
            "ChildException"
        )

        # Convert
        data_converter = temporalio.converter.DataConverter()
        poc_act = bridge_to_poc_activation(bridge_act, data_converter)

        # Verify the failure is properly typed
        assert len(poc_act.jobs) == 1
        child_job = poc_act.jobs[0]
        assert child_job.failure is not None
        assert isinstance(child_job.failure, ChildWorkflowError)
        assert child_job.failure.__cause__ is not None
        assert isinstance(child_job.failure.__cause__, ApplicationError)
        assert child_job.failure.__cause__.type == "ChildException"
