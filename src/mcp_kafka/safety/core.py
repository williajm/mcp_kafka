"""Access control enforcement for MCP Kafka server."""

import re
from enum import Enum

from loguru import logger

from mcp_kafka.config import SafetyConfig
from mcp_kafka.utils.errors import SafetyError, UnsafeOperationError, ValidationError


class AccessLevel(str, Enum):
    """Access level for operations.

    READ: Can only perform read operations (list, describe, consume, etc.)
    READ_WRITE: Can perform both read and write operations (produce, create, reset, etc.)
    """

    READ = "read"
    READ_WRITE = "read_write"


# Operations that require READ_WRITE access level
READ_WRITE_OPERATIONS: frozenset[str] = frozenset(
    {
        "kafka_create_topic",
        "kafka_produce_message",
        "kafka_reset_offsets",
    }
)

# Internal Kafka topics that should be blocked
INTERNAL_TOPIC_PREFIXES: tuple[str, ...] = ("__",)

# Internal consumer group prefixes that should be blocked
INTERNAL_CONSUMER_GROUP_PREFIXES: tuple[str, ...] = ("__",)

# Valid topic name pattern (Kafka topic naming rules)
TOPIC_NAME_PATTERN = re.compile(r"^[a-zA-Z0-9._-]+$")
MAX_TOPIC_NAME_LENGTH = 249


class AccessEnforcer:
    """Enforces access control policies for Kafka operations.

    This class validates tool access, topic names, consumer groups,
    and message sizes based on the configured safety policies.
    """

    def __init__(self, config: SafetyConfig) -> None:
        """Initialize access enforcer with safety configuration.

        Args:
            config: Safety configuration containing access policies
        """
        self._config = config
        self._topic_blocklist: set[str] = set(config.topic_blocklist)
        self._allowed_tools: set[str] | None = (
            set(config.allowed_tools) if config.allowed_tools else None
        )
        self._denied_tools: set[str] | None = (
            set(config.denied_tools) if config.denied_tools else None
        )

    @property
    def config(self) -> SafetyConfig:
        """Return the safety configuration."""
        return self._config

    def get_effective_access_level(self) -> AccessLevel:
        """Get effective access level based on configuration.

        Returns:
            READ_WRITE if write operations are allowed, READ otherwise
        """
        if self._config.allow_write_operations:
            return AccessLevel.READ_WRITE
        return AccessLevel.READ

    def can_access_tool(self, tool_name: str) -> bool:
        """Check if a tool can be accessed based on access level and filters.

        Args:
            tool_name: Name of the tool to check

        Returns:
            True if tool can be accessed, False otherwise
        """
        # Check denied tools first (takes precedence)
        if self._denied_tools and tool_name in self._denied_tools:
            logger.debug(f"Tool '{tool_name}' is in denied_tools list")
            return False

        # Check allowed tools whitelist
        if self._allowed_tools and tool_name not in self._allowed_tools:
            logger.debug(f"Tool '{tool_name}' is not in allowed_tools list")
            return False

        # Check access level for write operations
        if tool_name in READ_WRITE_OPERATIONS and not self._config.allow_write_operations:
            logger.debug(
                f"Tool '{tool_name}' requires write access but write operations are disabled"
            )
            return False

        return True

    def validate_tool_access(self, tool_name: str) -> None:
        """Validate that a tool can be accessed, raising an error if not.

        Args:
            tool_name: Name of the tool to validate

        Raises:
            UnsafeOperationError: If the tool cannot be accessed
        """
        if not self.can_access_tool(tool_name):
            if self._denied_tools and tool_name in self._denied_tools:
                raise UnsafeOperationError(f"Tool '{tool_name}' is denied by configuration")
            if self._allowed_tools and tool_name not in self._allowed_tools:
                raise UnsafeOperationError(f"Tool '{tool_name}' is not in allowed tools list")
            if tool_name in READ_WRITE_OPERATIONS:
                raise UnsafeOperationError(
                    f"Tool '{tool_name}' requires write access. "
                    "Set SAFETY_ALLOW_WRITE_OPERATIONS=true to enable."
                )
            raise UnsafeOperationError(f"Access to tool '{tool_name}' is not allowed")

    def is_protected_topic(self, topic: str) -> bool:
        """Check if a topic is protected (internal or blocklisted).

        Args:
            topic: Topic name to check

        Returns:
            True if topic is protected, False otherwise
        """
        # Check if it's an internal topic
        for prefix in INTERNAL_TOPIC_PREFIXES:
            if topic.startswith(prefix):
                return True

        # Check if it's in the blocklist
        return topic in self._topic_blocklist

    def is_blocklisted_topic(self, topic: str) -> bool:
        """Check if a topic is in the blocklist.

        Args:
            topic: Topic name to check

        Returns:
            True if topic is blocklisted, False otherwise
        """
        return topic in self._topic_blocklist

    def validate_topic_name(self, topic: str) -> None:
        """Validate a topic name format and access.

        Args:
            topic: Topic name to validate

        Raises:
            ValidationError: If topic name format is invalid
            SafetyError: If topic is protected/blocked
        """
        # Check for empty name
        if not topic:
            raise ValidationError("Topic name cannot be empty")

        # Check length
        if len(topic) > MAX_TOPIC_NAME_LENGTH:
            raise ValidationError(
                f"Topic name exceeds maximum length of {MAX_TOPIC_NAME_LENGTH} characters"
            )

        # Check format
        if not TOPIC_NAME_PATTERN.match(topic):
            raise ValidationError(
                f"Invalid topic name '{topic}'. "
                "Topic names can only contain alphanumeric characters, "
                "periods, underscores, and hyphens."
            )

        # Check if protected
        if self.is_protected_topic(topic):
            raise SafetyError(f"Access to topic '{topic}' is not allowed")

    def is_protected_consumer_group(self, group_id: str) -> bool:
        """Check if a consumer group is protected (internal).

        Args:
            group_id: Consumer group ID to check

        Returns:
            True if consumer group is protected, False otherwise
        """
        return any(group_id.startswith(prefix) for prefix in INTERNAL_CONSUMER_GROUP_PREFIXES)

    def validate_consumer_group(self, group_id: str) -> None:
        """Validate a consumer group ID.

        Args:
            group_id: Consumer group ID to validate

        Raises:
            ValidationError: If group ID is invalid
            SafetyError: If group is protected
        """
        if not group_id:
            raise ValidationError("Consumer group ID cannot be empty")

        if self.is_protected_consumer_group(group_id):
            raise SafetyError(f"Access to consumer group '{group_id}' is not allowed")

    def validate_message_size(self, size: int) -> None:
        """Validate message size against configured limit.

        Args:
            size: Message size in bytes

        Raises:
            ValidationError: If size is invalid or exceeds limit
        """
        if size < 0:
            raise ValidationError("Message size cannot be negative")

        if size > self._config.max_message_size:
            raise ValidationError(
                f"Message size {size} bytes exceeds maximum allowed "
                f"size of {self._config.max_message_size} bytes"
            )

    def validate_consume_limit(self, limit: int) -> None:
        """Validate the number of messages to consume.

        Args:
            limit: Number of messages requested

        Raises:
            ValidationError: If limit is invalid or exceeds maximum
        """
        if limit < 1:
            raise ValidationError("Consume limit must be at least 1")

        if limit > self._config.max_consume_messages:
            raise ValidationError(
                f"Consume limit {limit} exceeds maximum allowed "
                f"limit of {self._config.max_consume_messages}"
            )
