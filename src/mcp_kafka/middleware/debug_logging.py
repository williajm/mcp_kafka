"""Debug logging middleware for MCP protocol operations.

This middleware logs all incoming MCP protocol requests and outgoing responses.
Connection events and tools/list are logged at INFO level.
Tool calls are logged at DEBUG level.
"""

import json
from typing import Any

from fastmcp.server.middleware import CallNext, Middleware, MiddlewareContext

from mcp_kafka.middleware.utils import TOOL_CALL_PREFIX, get_operation_type
from mcp_kafka.utils.logger import get_logger

logger = get_logger(__name__)

# MCP protocol method constants
TOOLS_LIST_METHOD = "tools/list"

# Operations to log at INFO level (connection/discovery)
INFO_LEVEL_OPERATIONS = {
    "initialize",
    "initialized",
    "notifications/initialized",
    TOOLS_LIST_METHOD,
    "resources/list",
    "prompts/list",
}

# Maximum length for argument preview in logs
ARGS_PREVIEW_MAX_LENGTH = 200


class DebugLoggingMiddleware(Middleware):
    """FastMCP middleware for MCP protocol logging.

    Logs:
    - Connection events (initialize) at INFO level
    - Tool discovery (tools/list) at INFO level with tool names
    - Tool calls at DEBUG level with arguments and results
    """

    def __init__(self) -> None:
        """Initialize debug logging middleware."""
        logger.info("MCP protocol logging enabled")

    def _extract_tool_names(self, result: Any) -> list[str]:
        """Extract tool names from a tools/list response."""
        tool_names: list[str] = []
        try:
            # Handle different result formats
            if hasattr(result, "tools"):
                tools = result.tools
            elif isinstance(result, dict) and "tools" in result:
                tools = result["tools"]
            elif isinstance(result, list):
                tools = result
            else:
                return tool_names

            for tool in tools:
                if hasattr(tool, "name"):
                    tool_names.append(tool.name)
                elif isinstance(tool, dict) and "name" in tool:
                    tool_names.append(tool["name"])
        except Exception as e:
            logger.debug(f"Failed to extract tool names from result: {e}")
        return tool_names

    async def __call__(
        self,
        context: MiddlewareContext[Any],
        call_next: CallNext[Any, Any],
    ) -> Any:
        """Log MCP protocol request and response.

        Args:
            context: FastMCP middleware context
            call_next: Next middleware/handler in the chain

        Returns:
            Result from next handler
        """
        # Extract operation information
        operation_type = get_operation_type(context)
        arguments = getattr(context.message, "arguments", None) or getattr(
            context.message, "params", {}
        )

        # Determine if this is a high-priority operation (INFO level)
        is_info_level = any(op in operation_type for op in INFO_LEVEL_OPERATIONS)
        is_tool_call = operation_type.startswith(TOOL_CALL_PREFIX)

        # Log incoming request
        if is_info_level:
            logger.info(f">>> MCP: {operation_type}")
        elif is_tool_call:
            tool_name = operation_type[len(TOOL_CALL_PREFIX) :]
            args_preview = json.dumps(arguments)[:ARGS_PREVIEW_MAX_LENGTH] if arguments else "{}"
            logger.debug(f">>> MCP CALL: {tool_name} | args={args_preview}")
        else:
            logger.debug(f">>> MCP: {operation_type}")

        # Execute the operation
        try:
            result = await call_next(context)

            # Log successful response
            if operation_type == TOOLS_LIST_METHOD or TOOLS_LIST_METHOD in operation_type:
                tool_names = self._extract_tool_names(result)
                logger.info(f"<<< MCP: {TOOLS_LIST_METHOD} returned {len(tool_names)} tools:")
                for name in tool_names:
                    logger.info(f"    - {name}")
            elif "initialize" in operation_type:
                logger.info(f"<<< MCP: {operation_type} - CLIENT CONNECTED")
            elif is_tool_call:
                logger.debug(f"<<< MCP OK: {operation_type[len(TOOL_CALL_PREFIX) :]}")
            else:
                logger.debug(f"<<< MCP: {operation_type} - OK")

            return result

        except Exception as e:
            logger.warning(f"<<< MCP ERROR: {operation_type} - {type(e).__name__}: {e}")
            raise


def create_debug_logging_middleware() -> DebugLoggingMiddleware:
    """Factory function to create debug logging middleware.

    Returns:
        Configured DebugLoggingMiddleware instance
    """
    return DebugLoggingMiddleware()
