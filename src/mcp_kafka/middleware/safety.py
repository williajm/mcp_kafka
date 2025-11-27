"""Safety middleware for MCP Kafka server.

This middleware enforces access control, validates inputs,
and ensures operations comply with configured safety policies.
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from loguru import logger

from mcp_kafka.config import SafetyConfig
from mcp_kafka.safety.core import AccessEnforcer, AccessLevel
from mcp_kafka.utils.errors import SafetyError, ValidationError


@dataclass
class ToolContext:
    """Context for a tool invocation."""

    tool_name: str
    arguments: dict[str, Any]


@dataclass
class ToolResult:
    """Result from a tool invocation."""

    success: bool
    data: Any | None = None
    error: str | None = None


# Type alias for tool handlers
ToolHandler = Callable[[ToolContext], Awaitable[ToolResult]]


class SafetyMiddleware:
    """Middleware that enforces safety policies on tool invocations.

    This middleware wraps tool handlers to validate access permissions,
    topic names, consumer groups, and message sizes before execution.
    """

    def __init__(self, config: SafetyConfig) -> None:
        """Initialize safety middleware with configuration.

        Args:
            config: Safety configuration containing access policies
        """
        self._config = config
        self._enforcer = AccessEnforcer(config)

    @property
    def enforcer(self) -> AccessEnforcer:
        """Return the access enforcer instance."""
        return self._enforcer

    @property
    def access_level(self) -> AccessLevel:
        """Return the current effective access level."""
        return self._enforcer.get_effective_access_level()

    def validate_request(self, context: ToolContext) -> None:
        """Validate a tool request before execution.

        This method performs all safety checks on the request:
        - Tool access permissions
        - Topic name validation (if applicable)
        - Consumer group validation (if applicable)
        - Message size validation (if applicable)

        Args:
            context: Tool invocation context

        Raises:
            UnsafeOperationError: If tool access is denied
            ValidationError: If input validation fails
            SafetyError: If safety policy is violated
        """
        tool_name = context.tool_name
        args = context.arguments

        logger.debug(f"Validating request for tool '{tool_name}'")

        # Validate tool access
        self._enforcer.validate_tool_access(tool_name)

        # Validate topic if present
        topic = args.get("topic")
        if topic:
            self._enforcer.validate_topic_name(topic)

        # Validate topics list if present
        topics = args.get("topics")
        if topics and isinstance(topics, list):
            for t in topics:
                self._enforcer.validate_topic_name(t)

        # Validate consumer group if present
        group_id = args.get("group_id") or args.get("consumer_group")
        if group_id:
            self._enforcer.validate_consumer_group(group_id)

        # Validate message size if present (for produce operations)
        message = args.get("message") or args.get("value")
        if message:
            if isinstance(message, str):
                size = len(message.encode("utf-8"))
            elif isinstance(message, bytes):
                size = len(message)
            else:
                # Try to estimate size for other types
                size = len(str(message).encode("utf-8"))
            self._enforcer.validate_message_size(size)

        # Validate consume limit if present
        limit = args.get("limit") or args.get("max_messages")
        if limit is not None:
            self._enforcer.validate_consume_limit(int(limit))

        logger.debug(f"Request validation passed for tool '{tool_name}'")

    async def wrap_handler(self, handler: ToolHandler, context: ToolContext) -> ToolResult:
        """Wrap a tool handler with safety validation.

        Args:
            handler: The original tool handler
            context: Tool invocation context

        Returns:
            ToolResult from the handler or an error result
        """
        try:
            self.validate_request(context)
            return await handler(context)
        except (SafetyError, ValidationError) as e:
            logger.warning(f"Safety check failed for '{context.tool_name}': {e}")
            return ToolResult(success=False, error=str(e))
        except Exception as e:
            logger.error(f"Unexpected error in '{context.tool_name}': {e}")
            return ToolResult(success=False, error=f"Internal error: {e}")

    def create_wrapper(
        self, handler: ToolHandler
    ) -> Callable[[ToolContext], Awaitable[ToolResult]]:
        """Create a wrapped handler that includes safety validation.

        Args:
            handler: The original tool handler

        Returns:
            Wrapped handler function
        """

        async def wrapped(context: ToolContext) -> ToolResult:
            return await self.wrap_handler(handler, context)

        return wrapped
