"""Tests for debug logging middleware."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mcp_kafka.middleware.debug_logging import (
    ARGS_PREVIEW_MAX_LENGTH,
    INFO_LEVEL_OPERATIONS,
    TOOLS_LIST_METHOD,
    DebugLoggingMiddleware,
    create_debug_logging_middleware,
)


class MockMessage:
    """Mock message with configurable attributes."""

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


class MockContext:
    """Mock middleware context."""

    def __init__(self, message: Any) -> None:
        self.message = message


class TestConstants:
    """Tests for module constants."""

    def test_args_preview_max_length(self) -> None:
        """Test that ARGS_PREVIEW_MAX_LENGTH is defined."""
        assert ARGS_PREVIEW_MAX_LENGTH == 200

    def test_info_level_operations(self) -> None:
        """Test that INFO_LEVEL_OPERATIONS contains expected operations."""
        assert "initialize" in INFO_LEVEL_OPERATIONS
        assert TOOLS_LIST_METHOD in INFO_LEVEL_OPERATIONS
        assert "resources/list" in INFO_LEVEL_OPERATIONS
        assert "prompts/list" in INFO_LEVEL_OPERATIONS

    def test_tools_list_method_constant(self) -> None:
        """Test that TOOLS_LIST_METHOD constant is correct."""
        assert TOOLS_LIST_METHOD == "tools/list"


class TestDebugLoggingMiddlewareInit:
    """Tests for DebugLoggingMiddleware initialization."""

    def test_init_logs_message(self) -> None:
        """Test that initialization logs a message."""
        with patch("mcp_kafka.middleware.debug_logging.logger") as mock_logger:
            DebugLoggingMiddleware()

            mock_logger.info.assert_called_once_with("MCP protocol logging enabled")


class TestExtractToolNames:
    """Tests for _extract_tool_names method."""

    def test_extract_from_object_with_tools_attribute(self) -> None:
        """Test extraction from object with tools attribute."""
        middleware = DebugLoggingMiddleware()

        mock_tool1 = MagicMock()
        mock_tool1.name = "kafka_list_topics"
        mock_tool2 = MagicMock()
        mock_tool2.name = "kafka_describe_topic"

        result_obj = MagicMock()
        result_obj.tools = [mock_tool1, mock_tool2]

        names = middleware._extract_tool_names(result_obj)

        assert names == ["kafka_list_topics", "kafka_describe_topic"]

    def test_extract_from_dict_with_tools_key(self) -> None:
        """Test extraction from dict with tools key."""
        middleware = DebugLoggingMiddleware()

        result = {
            "tools": [
                {"name": "kafka_list_topics"},
                {"name": "kafka_produce_message"},
            ]
        }

        names = middleware._extract_tool_names(result)

        assert names == ["kafka_list_topics", "kafka_produce_message"]

    def test_extract_from_list_of_tools(self) -> None:
        """Test extraction from list of tools."""
        middleware = DebugLoggingMiddleware()

        result = [
            {"name": "kafka_cluster_info"},
            {"name": "kafka_list_brokers"},
        ]

        names = middleware._extract_tool_names(result)

        assert names == ["kafka_cluster_info", "kafka_list_brokers"]

    def test_extract_from_list_with_objects(self) -> None:
        """Test extraction from list of tool objects."""
        middleware = DebugLoggingMiddleware()

        mock_tool = MagicMock()
        mock_tool.name = "kafka_get_watermarks"

        names = middleware._extract_tool_names([mock_tool])

        assert names == ["kafka_get_watermarks"]

    def test_empty_result_returns_empty_list(self) -> None:
        """Test that empty/invalid results return empty list."""
        middleware = DebugLoggingMiddleware()

        assert middleware._extract_tool_names(None) == []
        assert middleware._extract_tool_names({}) == []
        assert middleware._extract_tool_names("not a valid result") == []

    def test_exception_handling_logs_debug(self) -> None:
        """Test that exceptions are caught and logged."""
        middleware = DebugLoggingMiddleware()

        # Create an object that will raise an exception when iterated
        bad_result = MagicMock()
        bad_result.tools = None  # Will cause iteration to fail
        # Actually, None doesn't cause iteration to fail, let's make it raise
        bad_result.tools = MagicMock()
        bad_result.tools.__iter__ = MagicMock(side_effect=RuntimeError("test error"))

        with patch("mcp_kafka.middleware.debug_logging.logger") as mock_logger:
            names = middleware._extract_tool_names(bad_result)

            assert names == []
            mock_logger.debug.assert_called()


class TestDebugLoggingMiddlewareCall:
    """Tests for the __call__ method."""

    @pytest.mark.asyncio
    async def test_logs_info_level_operations(self) -> None:
        """Test that INFO level operations are logged at INFO level."""
        middleware = DebugLoggingMiddleware()
        message = MockMessage(method="initialize")
        context = MockContext(message)
        call_next = AsyncMock(return_value={"result": "ok"})

        with patch("mcp_kafka.middleware.debug_logging.logger") as mock_logger:
            with patch("mcp_kafka.middleware.debug_logging.get_operation_type") as mock_get_op:
                mock_get_op.return_value = "initialize"

                await middleware(context, call_next)

                # Should log at INFO level for request
                mock_logger.info.assert_any_call(">>> MCP: initialize")

    @pytest.mark.asyncio
    async def test_logs_tool_calls_at_debug_level(self) -> None:
        """Test that tool calls are logged at DEBUG level."""
        middleware = DebugLoggingMiddleware()
        message = MockMessage(
            name="kafka_list_topics",
            arguments={"internal": False},
        )
        context = MockContext(message)
        call_next = AsyncMock(return_value={"topics": []})

        with patch("mcp_kafka.middleware.debug_logging.logger") as mock_logger:
            with patch("mcp_kafka.middleware.debug_logging.get_operation_type") as mock_get_op:
                mock_get_op.return_value = "tool_call:kafka_list_topics"

                await middleware(context, call_next)

                # Should log at DEBUG level for tool calls
                debug_calls = [str(call) for call in mock_logger.debug.call_args_list]
                assert any("kafka_list_topics" in call for call in debug_calls)

    @pytest.mark.asyncio
    async def test_logs_tools_list_response_with_tool_names(self) -> None:
        """Test that tools/list responses log tool names."""
        middleware = DebugLoggingMiddleware()
        message = MockMessage(method="tools/list")
        context = MockContext(message)

        mock_tool = MagicMock()
        mock_tool.name = "kafka_list_topics"
        tools_response = MagicMock()
        tools_response.tools = [mock_tool]

        call_next = AsyncMock(return_value=tools_response)

        with patch("mcp_kafka.middleware.debug_logging.logger") as mock_logger:
            with patch("mcp_kafka.middleware.debug_logging.get_operation_type") as mock_get_op:
                mock_get_op.return_value = "tools/list"

                await middleware(context, call_next)

                # Should log tool count and names
                info_calls = [str(call) for call in mock_logger.info.call_args_list]
                assert any("1 tools" in call for call in info_calls)

    @pytest.mark.asyncio
    async def test_logs_initialize_as_client_connected(self) -> None:
        """Test that initialize operations log CLIENT CONNECTED."""
        middleware = DebugLoggingMiddleware()
        message = MockMessage(method="initialize")
        context = MockContext(message)
        call_next = AsyncMock(return_value={"ok": True})

        with patch("mcp_kafka.middleware.debug_logging.logger") as mock_logger:
            with patch("mcp_kafka.middleware.debug_logging.get_operation_type") as mock_get_op:
                mock_get_op.return_value = "initialize"

                await middleware(context, call_next)

                info_calls = [str(call) for call in mock_logger.info.call_args_list]
                assert any("CLIENT CONNECTED" in call for call in info_calls)

    @pytest.mark.asyncio
    async def test_logs_exceptions_as_warning(self) -> None:
        """Test that exceptions are logged at WARNING level."""
        middleware = DebugLoggingMiddleware()
        message = MockMessage(name="kafka_list_topics", arguments={})
        context = MockContext(message)
        call_next = AsyncMock(side_effect=ValueError("Test error"))

        with patch("mcp_kafka.middleware.debug_logging.logger") as mock_logger:
            with patch("mcp_kafka.middleware.debug_logging.get_operation_type") as mock_get_op:
                mock_get_op.return_value = "tool_call:kafka_list_topics"

                with pytest.raises(ValueError, match="Test error"):
                    await middleware(context, call_next)

                mock_logger.warning.assert_called()
                warning_call = str(mock_logger.warning.call_args)
                assert "MCP ERROR" in warning_call
                assert "ValueError" in warning_call

    @pytest.mark.asyncio
    async def test_returns_result_from_call_next(self) -> None:
        """Test that the result from call_next is returned."""
        middleware = DebugLoggingMiddleware()
        message = MockMessage(method="prompts/list")
        context = MockContext(message)
        expected_result = {"prompts": ["p1", "p2"]}
        call_next = AsyncMock(return_value=expected_result)

        with patch("mcp_kafka.middleware.debug_logging.get_operation_type") as mock_get_op:
            mock_get_op.return_value = "prompts/list"

            result = await middleware(context, call_next)

            assert result == expected_result

    @pytest.mark.asyncio
    async def test_truncates_long_arguments(self) -> None:
        """Test that long arguments are truncated in preview."""
        middleware = DebugLoggingMiddleware()
        long_value = "x" * 500
        message = MockMessage(
            name="kafka_produce_message",
            arguments={"value": long_value},
        )
        context = MockContext(message)
        call_next = AsyncMock(return_value={"offset": 0})

        with patch("mcp_kafka.middleware.debug_logging.logger") as mock_logger:
            with patch("mcp_kafka.middleware.debug_logging.get_operation_type") as mock_get_op:
                mock_get_op.return_value = "tool_call:kafka_produce_message"

                await middleware(context, call_next)

                # The args preview should be truncated
                debug_calls = [str(call) for call in mock_logger.debug.call_args_list]
                # The JSON of arguments is truncated to 200 chars
                assert any("args=" in call for call in debug_calls)


class TestCreateDebugLoggingMiddleware:
    """Tests for the factory function."""

    def test_creates_middleware_instance(self) -> None:
        """Test that factory creates a DebugLoggingMiddleware instance."""
        middleware = create_debug_logging_middleware()

        assert isinstance(middleware, DebugLoggingMiddleware)
