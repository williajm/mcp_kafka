"""Tests for error classes."""

import pytest

from mcp_kafka.utils.errors import (
    ConsumerGroupNotFound,
    KafkaConnectionError,
    KafkaHealthCheckError,
    KafkaOperationError,
    MCPKafkaError,
    SafetyError,
    TopicNotFound,
    UnsafeOperationError,
    ValidationError,
)


class TestErrorHierarchy:
    """Tests for error class hierarchy."""

    def test_mcp_kafka_error_is_base(self) -> None:
        """Test MCPKafkaError is base exception."""
        error = MCPKafkaError("test error")
        assert isinstance(error, Exception)
        assert str(error) == "test error"

    def test_kafka_connection_error(self) -> None:
        """Test KafkaConnectionError inherits from MCPKafkaError."""
        error = KafkaConnectionError("connection failed")
        assert isinstance(error, MCPKafkaError)
        assert isinstance(error, Exception)

    def test_kafka_health_check_error(self) -> None:
        """Test KafkaHealthCheckError inherits from MCPKafkaError."""
        error = KafkaHealthCheckError("health check failed")
        assert isinstance(error, MCPKafkaError)

    def test_kafka_operation_error(self) -> None:
        """Test KafkaOperationError inherits from MCPKafkaError."""
        error = KafkaOperationError("operation failed")
        assert isinstance(error, MCPKafkaError)

    def test_validation_error(self) -> None:
        """Test ValidationError inherits from MCPKafkaError."""
        error = ValidationError("invalid input")
        assert isinstance(error, MCPKafkaError)

    def test_safety_error(self) -> None:
        """Test SafetyError inherits from MCPKafkaError."""
        error = SafetyError("safety check failed")
        assert isinstance(error, MCPKafkaError)

    def test_unsafe_operation_error(self) -> None:
        """Test UnsafeOperationError inherits from SafetyError."""
        error = UnsafeOperationError("unsafe operation")
        assert isinstance(error, SafetyError)
        assert isinstance(error, MCPKafkaError)

    def test_topic_not_found(self) -> None:
        """Test TopicNotFound inherits from MCPKafkaError."""
        error = TopicNotFound("topic not found")
        assert isinstance(error, MCPKafkaError)

    def test_consumer_group_not_found(self) -> None:
        """Test ConsumerGroupNotFound inherits from MCPKafkaError."""
        error = ConsumerGroupNotFound("consumer group not found")
        assert isinstance(error, MCPKafkaError)

    def test_errors_can_be_raised(self) -> None:
        """Test all errors can be raised and caught."""
        with pytest.raises(KafkaConnectionError):
            raise KafkaConnectionError("test")

        with pytest.raises(MCPKafkaError):
            raise TopicNotFound("test")

        with pytest.raises(SafetyError):
            raise UnsafeOperationError("test")
