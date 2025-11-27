"""Tests for safety middleware module."""

import pytest

from mcp_kafka.config import SafetyConfig
from mcp_kafka.middleware import SafetyMiddleware, ToolContext, ToolResult
from mcp_kafka.safety import AccessLevel
from mcp_kafka.utils.errors import SafetyError, UnsafeOperationError, ValidationError


class TestToolContext:
    """Tests for ToolContext dataclass."""

    def test_creation(self) -> None:
        """Test ToolContext creation."""
        ctx = ToolContext(tool_name="test_tool", arguments={"key": "value"})
        assert ctx.tool_name == "test_tool"
        assert ctx.arguments == {"key": "value"}

    def test_empty_arguments(self) -> None:
        """Test ToolContext with empty arguments."""
        ctx = ToolContext(tool_name="test_tool", arguments={})
        assert ctx.arguments == {}


class TestToolResult:
    """Tests for ToolResult dataclass."""

    def test_success_result(self) -> None:
        """Test successful ToolResult."""
        result = ToolResult(success=True, data={"result": "data"})
        assert result.success is True
        assert result.data == {"result": "data"}
        assert result.error is None

    def test_error_result(self) -> None:
        """Test error ToolResult."""
        result = ToolResult(success=False, error="Something failed")
        assert result.success is False
        assert result.data is None
        assert result.error == "Something failed"

    def test_default_values(self) -> None:
        """Test ToolResult default values."""
        result = ToolResult(success=True)
        assert result.data is None
        assert result.error is None


class TestSafetyMiddlewareInit:
    """Tests for SafetyMiddleware initialization."""

    def test_default_config(self) -> None:
        """Test initialization with default config."""
        config = SafetyConfig()
        middleware = SafetyMiddleware(config)
        assert middleware.enforcer is not None
        assert middleware.access_level == AccessLevel.READ

    def test_read_write_access_level(self) -> None:
        """Test access level when write operations enabled."""
        config = SafetyConfig(allow_write_operations=True)
        middleware = SafetyMiddleware(config)
        assert middleware.access_level == AccessLevel.READ_WRITE


class TestSafetyMiddlewareValidation:
    """Tests for SafetyMiddleware validate_request."""

    def test_validate_read_tool(self) -> None:
        """Test validation passes for read tool."""
        config = SafetyConfig()
        middleware = SafetyMiddleware(config)
        ctx = ToolContext(tool_name="kafka_list_topics", arguments={})
        # Should not raise
        middleware.validate_request(ctx)

    def test_validate_write_tool_blocked(self) -> None:
        """Test validation fails for write tool when disabled."""
        config = SafetyConfig()
        middleware = SafetyMiddleware(config)
        ctx = ToolContext(tool_name="kafka_create_topic", arguments={"topic": "test"})
        with pytest.raises(UnsafeOperationError, match="requires write access"):
            middleware.validate_request(ctx)

    def test_validate_write_tool_allowed(self) -> None:
        """Test validation passes for write tool when enabled."""
        config = SafetyConfig(allow_write_operations=True)
        middleware = SafetyMiddleware(config)
        ctx = ToolContext(
            tool_name="kafka_create_topic",
            arguments={"topic": "test-topic"},
        )
        # Should not raise
        middleware.validate_request(ctx)

    def test_validate_blocked_topic(self) -> None:
        """Test validation fails for blocked topic."""
        config = SafetyConfig()
        middleware = SafetyMiddleware(config)
        ctx = ToolContext(
            tool_name="kafka_describe_topic",
            arguments={"topic": "__consumer_offsets"},
        )
        with pytest.raises(SafetyError, match="not allowed"):
            middleware.validate_request(ctx)

    def test_validate_invalid_topic_name(self) -> None:
        """Test validation fails for invalid topic name."""
        config = SafetyConfig()
        middleware = SafetyMiddleware(config)
        ctx = ToolContext(
            tool_name="kafka_describe_topic",
            arguments={"topic": "invalid topic name"},
        )
        with pytest.raises(ValidationError, match="Invalid topic name"):
            middleware.validate_request(ctx)

    def test_validate_topics_list(self) -> None:
        """Test validation of topics list argument."""
        config = SafetyConfig()
        middleware = SafetyMiddleware(config)

        # Valid topics
        ctx = ToolContext(
            tool_name="kafka_list_topics",
            arguments={"topics": ["topic1", "topic2"]},
        )
        middleware.validate_request(ctx)

        # Invalid topic in list
        ctx = ToolContext(
            tool_name="kafka_list_topics",
            arguments={"topics": ["valid-topic", "__internal"]},
        )
        with pytest.raises(SafetyError, match="not allowed"):
            middleware.validate_request(ctx)

    def test_validate_consumer_group(self) -> None:
        """Test validation of consumer group."""
        config = SafetyConfig()
        middleware = SafetyMiddleware(config)

        # Valid group_id
        ctx = ToolContext(
            tool_name="kafka_describe_consumer_group",
            arguments={"group_id": "my-group"},
        )
        middleware.validate_request(ctx)

        # Valid consumer_group
        ctx = ToolContext(
            tool_name="kafka_describe_consumer_group",
            arguments={"consumer_group": "my-group"},
        )
        middleware.validate_request(ctx)

    def test_validate_protected_consumer_group(self) -> None:
        """Test validation fails for protected consumer group."""
        config = SafetyConfig()
        middleware = SafetyMiddleware(config)
        ctx = ToolContext(
            tool_name="kafka_describe_consumer_group",
            arguments={"group_id": "__internal_group"},
        )
        with pytest.raises(SafetyError, match="not allowed"):
            middleware.validate_request(ctx)

    def test_validate_message_size_string(self) -> None:
        """Test validation of message size for string message."""
        config = SafetyConfig(allow_write_operations=True, max_message_size=100)
        middleware = SafetyMiddleware(config)

        # Valid message
        ctx = ToolContext(
            tool_name="kafka_produce_message",
            arguments={"topic": "test", "message": "short message"},
        )
        middleware.validate_request(ctx)

        # Message too large
        ctx = ToolContext(
            tool_name="kafka_produce_message",
            arguments={"topic": "test", "message": "x" * 101},
        )
        with pytest.raises(ValidationError, match="exceeds maximum"):
            middleware.validate_request(ctx)

    def test_validate_message_size_bytes(self) -> None:
        """Test validation of message size for bytes message."""
        config = SafetyConfig(allow_write_operations=True, max_message_size=50)
        middleware = SafetyMiddleware(config)

        ctx = ToolContext(
            tool_name="kafka_produce_message",
            arguments={"topic": "test", "value": b"short"},
        )
        middleware.validate_request(ctx)

        ctx = ToolContext(
            tool_name="kafka_produce_message",
            arguments={"topic": "test", "value": b"x" * 51},
        )
        with pytest.raises(ValidationError, match="exceeds maximum"):
            middleware.validate_request(ctx)

    def test_validate_consume_limit(self) -> None:
        """Test validation of consume limit."""
        config = SafetyConfig(max_consume_messages=10)
        middleware = SafetyMiddleware(config)

        # Valid limit
        ctx = ToolContext(
            tool_name="kafka_consume_messages",
            arguments={"topic": "test", "limit": 5},
        )
        middleware.validate_request(ctx)

        # Limit too high
        ctx = ToolContext(
            tool_name="kafka_consume_messages",
            arguments={"topic": "test", "limit": 11},
        )
        with pytest.raises(ValidationError, match="exceeds maximum"):
            middleware.validate_request(ctx)

    def test_validate_max_messages_alias(self) -> None:
        """Test validation of max_messages alias for limit."""
        config = SafetyConfig(max_consume_messages=10)
        middleware = SafetyMiddleware(config)

        ctx = ToolContext(
            tool_name="kafka_consume_messages",
            arguments={"topic": "test", "max_messages": 11},
        )
        with pytest.raises(ValidationError, match="exceeds maximum"):
            middleware.validate_request(ctx)

    def test_validate_denied_tool(self) -> None:
        """Test validation fails for denied tool."""
        config = SafetyConfig(denied_tools=["blocked_tool"])
        middleware = SafetyMiddleware(config)
        ctx = ToolContext(tool_name="blocked_tool", arguments={})
        with pytest.raises(UnsafeOperationError, match="denied by configuration"):
            middleware.validate_request(ctx)


class TestSafetyMiddlewareWrapHandler:
    """Tests for SafetyMiddleware wrap_handler."""

    @pytest.mark.asyncio
    async def test_wrap_handler_success(self) -> None:
        """Test wrap_handler passes through successful result."""
        config = SafetyConfig()
        middleware = SafetyMiddleware(config)

        async def handler(
            ctx: ToolContext,
        ) -> ToolResult:  # NOSONAR - must be async to match ToolHandler type
            return ToolResult(success=True, data={"key": "value"})

        ctx = ToolContext(tool_name="kafka_list_topics", arguments={})
        result = await middleware.wrap_handler(handler, ctx)
        assert result.success is True
        assert result.data == {"key": "value"}

    @pytest.mark.asyncio
    async def test_wrap_handler_safety_error(self) -> None:
        """Test wrap_handler catches safety errors."""
        config = SafetyConfig()
        middleware = SafetyMiddleware(config)

        async def handler(
            ctx: ToolContext,
        ) -> ToolResult:  # NOSONAR - must be async to match ToolHandler type
            return ToolResult(success=True)

        ctx = ToolContext(
            tool_name="kafka_describe_topic",
            arguments={"topic": "__consumer_offsets"},
        )
        result = await middleware.wrap_handler(handler, ctx)
        assert result.success is False
        assert result.error is not None
        assert "not allowed" in result.error

    @pytest.mark.asyncio
    async def test_wrap_handler_validation_error(self) -> None:
        """Test wrap_handler catches validation errors."""
        config = SafetyConfig()
        middleware = SafetyMiddleware(config)

        async def handler(
            ctx: ToolContext,
        ) -> ToolResult:  # NOSONAR - must be async to match ToolHandler type
            return ToolResult(success=True)

        ctx = ToolContext(
            tool_name="kafka_describe_topic",
            arguments={"topic": "invalid topic name"},  # Contains space
        )
        result = await middleware.wrap_handler(handler, ctx)
        assert result.success is False
        assert result.error is not None
        assert "Invalid topic name" in result.error

    @pytest.mark.asyncio
    async def test_wrap_handler_unexpected_error(self) -> None:
        """Test wrap_handler catches unexpected errors."""
        config = SafetyConfig()
        middleware = SafetyMiddleware(config)

        async def handler(ctx: ToolContext) -> ToolResult:
            raise RuntimeError("Unexpected error")

        ctx = ToolContext(tool_name="kafka_list_topics", arguments={})
        result = await middleware.wrap_handler(handler, ctx)
        assert result.success is False
        assert result.error is not None
        assert "Internal error" in result.error


class TestSafetyMiddlewareCreateWrapper:
    """Tests for SafetyMiddleware create_wrapper."""

    @pytest.mark.asyncio
    async def test_create_wrapper(self) -> None:
        """Test create_wrapper returns wrapped handler."""
        config = SafetyConfig()
        middleware = SafetyMiddleware(config)

        async def handler(
            ctx: ToolContext,
        ) -> ToolResult:  # NOSONAR - must be async to match ToolHandler type
            return ToolResult(success=True, data="original")

        wrapped = middleware.create_wrapper(handler)
        ctx = ToolContext(tool_name="kafka_list_topics", arguments={})
        result = await wrapped(ctx)
        assert result.success is True
        assert result.data == "original"

    @pytest.mark.asyncio
    async def test_wrapped_handler_validates(self) -> None:
        """Test wrapped handler performs validation."""
        config = SafetyConfig()
        middleware = SafetyMiddleware(config)

        async def handler(
            ctx: ToolContext,
        ) -> ToolResult:  # NOSONAR - must be async to match ToolHandler type
            return ToolResult(success=True)

        wrapped = middleware.create_wrapper(handler)
        ctx = ToolContext(
            tool_name="kafka_create_topic",
            arguments={"topic": "test"},
        )
        result = await wrapped(ctx)
        assert result.success is False
        assert result.error is not None
        assert "write access" in result.error
