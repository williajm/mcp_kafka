"""Utility functions for middleware operations."""

from typing import Any

from fastmcp.server.middleware import MiddlewareContext

# Prefix used for tool call operation types
TOOL_CALL_PREFIX = "tool_call:"


def get_operation_type(context: MiddlewareContext[Any]) -> str:
    """Determine the type of MCP operation from context.

    Args:
        context: FastMCP middleware context

    Returns:
        Operation type string (e.g., "tools/list", "tool_call:kafka_list_topics")
    """
    message = context.message

    # Check if it's a tool call (has 'name' attribute for tool name)
    if hasattr(message, "name") and hasattr(message, "arguments"):
        tool_name = getattr(message, "name", None)
        if tool_name:
            return f"{TOOL_CALL_PREFIX}{tool_name}"

    # Check for MCP protocol methods (tools/list, prompts/list, etc.)
    if hasattr(message, "method"):
        method = getattr(message, "method", None)
        if method:
            return str(method)

    # Check for operation attribute
    if hasattr(context, "operation"):
        return str(context.operation)

    # Try to infer from message dictionary
    if isinstance(message, dict):
        if "method" in message:
            return str(message["method"])
        if "name" in message and "arguments" in message:
            return f"{TOOL_CALL_PREFIX}{message['name']}"

    # Fall back to message type class name
    message_type = type(message).__name__
    return message_type if message_type not in ["object", "dict"] else "mcp_protocol"


def get_operation_name(context: MiddlewareContext[Any]) -> str:
    """Get a human-readable operation name from context.

    Args:
        context: FastMCP middleware context

    Returns:
        Operation name string
    """
    tool_name = getattr(context.message, "name", None)
    if tool_name:
        return str(tool_name)

    operation_type = get_operation_type(context)
    if operation_type.startswith(TOOL_CALL_PREFIX):
        return operation_type[len(TOOL_CALL_PREFIX) :]

    return operation_type
