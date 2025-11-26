"""Utility modules for MCP Kafka."""

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
from mcp_kafka.utils.logger import get_logger, setup_logger

__all__ = [
    "MCPKafkaError",
    "KafkaConnectionError",
    "KafkaHealthCheckError",
    "KafkaOperationError",
    "ValidationError",
    "SafetyError",
    "UnsafeOperationError",
    "TopicNotFound",
    "ConsumerGroupNotFound",
    "setup_logger",
    "get_logger",
]
