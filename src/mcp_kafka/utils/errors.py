"""Custom exceptions for MCP Kafka."""


class MCPKafkaError(Exception):
    """Base exception for all MCP Kafka errors."""


class KafkaConnectionError(MCPKafkaError):
    """Raised when unable to connect to Kafka cluster."""


class KafkaHealthCheckError(MCPKafkaError):
    """Raised when Kafka health check fails."""


class KafkaOperationError(MCPKafkaError):
    """Raised when a Kafka operation fails."""


class ValidationError(MCPKafkaError):
    """Raised when input validation fails."""


class SafetyError(MCPKafkaError):
    """Raised when a safety check fails."""


class UnsafeOperationError(SafetyError):
    """Raised when an unsafe operation is attempted."""


class TopicNotFound(MCPKafkaError):  # noqa: N818
    """Raised when a topic is not found."""


class ConsumerGroupNotFound(MCPKafkaError):  # noqa: N818
    """Raised when a consumer group is not found."""


class RateLimitError(SafetyError):
    """Raised when rate limit is exceeded."""


class AuthenticationError(MCPKafkaError):
    """Raised when authentication fails."""


class AuthorizationError(MCPKafkaError):
    """Raised when authorization fails."""
