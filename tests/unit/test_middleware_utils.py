"""Tests for middleware utility functions."""

from typing import Any

from mcp_kafka.middleware.utils import (
    TOOL_CALL_PREFIX,
    get_operation_name,
    get_operation_type,
)


class MockMessage:
    """Mock message with configurable attributes."""

    def __init__(self, **kwargs: Any) -> None:
        for key, value in kwargs.items():
            setattr(self, key, value)


class MockContext:
    """Mock middleware context."""

    def __init__(self, message: Any, operation: str | None = None) -> None:
        self.message = message
        if operation is not None:
            self.operation = operation


class TestToolCallPrefix:
    """Tests for the TOOL_CALL_PREFIX constant."""

    def test_prefix_value(self) -> None:
        """Test that the prefix is correct."""
        assert TOOL_CALL_PREFIX == "tool_call:"

    def test_prefix_length(self) -> None:
        """Test that prefix length is as expected for slicing."""
        assert len(TOOL_CALL_PREFIX) == 10


class TestGetOperationType:
    """Tests for get_operation_type function."""

    def test_tool_call_with_name_and_arguments(self) -> None:
        """Test detection of tool calls with name and arguments attributes."""
        message = MockMessage(name="kafka_list_topics", arguments={"internal": False})
        context = MockContext(message)

        result = get_operation_type(context)

        assert result == "tool_call:kafka_list_topics"

    def test_tool_call_with_none_name(self) -> None:
        """Test handling when name attribute is None."""
        message = MockMessage(name=None, arguments={})
        context = MockContext(message)

        result = get_operation_type(context)

        # Should fall through to other checks
        assert result != "tool_call:None"

    def test_mcp_protocol_method(self) -> None:
        """Test detection of MCP protocol methods."""
        message = MockMessage(method="tools/list")
        context = MockContext(message)

        result = get_operation_type(context)

        assert result == "tools/list"

    def test_mcp_protocol_method_none(self) -> None:
        """Test handling when method attribute is None."""
        message = MockMessage(method=None)
        context = MockContext(message)

        result = get_operation_type(context)

        # Should fall through to class name
        assert result == "MockMessage"

    def test_context_operation_attribute(self) -> None:
        """Test fallback to context.operation attribute."""
        message = MockMessage()
        context = MockContext(message, operation="custom_operation")

        result = get_operation_type(context)

        assert result == "custom_operation"

    def test_dict_message_with_method(self) -> None:
        """Test handling of dictionary message with method key."""
        message = {"method": "resources/list"}
        context = MockContext(message)

        result = get_operation_type(context)

        assert result == "resources/list"

    def test_dict_message_with_tool_call(self) -> None:
        """Test handling of dictionary message with tool call structure."""
        message = {"name": "kafka_describe_topic", "arguments": {"topic": "test"}}
        context = MockContext(message)

        result = get_operation_type(context)

        assert result == "tool_call:kafka_describe_topic"

    def test_fallback_to_class_name(self) -> None:
        """Test fallback to message class name."""
        message = MockMessage()
        context = MockContext(message)

        result = get_operation_type(context)

        assert result == "MockMessage"

    def test_fallback_for_object_class(self) -> None:
        """Test fallback returns mcp_protocol for generic object."""
        message = object()
        context = MockContext(message)

        result = get_operation_type(context)

        assert result == "mcp_protocol"

    def test_fallback_for_dict_without_special_keys(self) -> None:
        """Test fallback for dict without method or name keys."""
        message = {"some_key": "some_value"}
        context = MockContext(message)

        result = get_operation_type(context)

        assert result == "mcp_protocol"


class TestGetOperationName:
    """Tests for get_operation_name function."""

    def test_extracts_name_from_message(self) -> None:
        """Test extraction of name directly from message."""
        message = MockMessage(name="kafka_produce_message", arguments={})
        context = MockContext(message)

        result = get_operation_name(context)

        assert result == "kafka_produce_message"

    def test_extracts_name_from_dict_tool_call(self) -> None:
        """Test that tool name is extracted from dict message with tool call."""
        # Dict messages without direct name attribute use get_operation_type
        # which detects name+arguments and returns tool_call:name
        # get_operation_name then strips the prefix
        message = {"name": "kafka_reset_offsets", "arguments": {"group_id": "test"}}
        context = MockContext(message)

        result = get_operation_name(context)

        # Since dict doesn't have getattr('name'), falls through to
        # get_operation_type which returns "tool_call:kafka_reset_offsets"
        # then the prefix is stripped
        assert result == "kafka_reset_offsets"

    def test_returns_operation_type_when_no_prefix(self) -> None:
        """Test that non-tool operations are returned as-is."""
        message = MockMessage(method="tools/list")
        context = MockContext(message)

        result = get_operation_name(context)

        assert result == "tools/list"

    def test_tool_name_extraction_from_prefixed_type(self) -> None:
        """Test extraction when message has name attribute."""
        message = MockMessage(name="kafka_get_watermarks", arguments={"topic": "test"})
        context = MockContext(message)

        result = get_operation_name(context)

        assert result == "kafka_get_watermarks"
