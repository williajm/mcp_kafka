"""Middleware stack for tool invocations.

This module provides a unified middleware stack that applies rate limiting
and audit logging to all tool invocations.
"""

import time
from dataclasses import dataclass
from typing import Any

from loguru import logger

from mcp_kafka.config import SecurityConfig
from mcp_kafka.middleware.audit import AuditMiddleware
from mcp_kafka.middleware.rate_limit import RateLimitMiddleware
from mcp_kafka.middleware.safety import ToolContext, ToolResult


@dataclass
class ToolInvocation:
    """Tracks state for a single tool invocation."""

    tool_name: str
    arguments: dict[str, Any]
    start_time: float
    user_id: str | None = None


class MiddlewareStack:
    """Unified middleware stack for tool invocations.

    Applies rate limiting and audit logging to tool calls.
    This class provides a simple interface for synchronous tool functions.
    """

    def __init__(self, config: SecurityConfig) -> None:
        """Initialize middleware stack.

        Args:
            config: Security configuration
        """
        self._config = config
        self._rate_limiter = RateLimitMiddleware(config)
        self._audit = AuditMiddleware(config)

        logger.info(
            f"Middleware stack initialized: "
            f"rate_limit={self._rate_limiter.enabled}, "
            f"audit={self._audit.enabled}"
        )

    @property
    def rate_limiter(self) -> RateLimitMiddleware:
        """Return the rate limiter middleware."""
        return self._rate_limiter

    @property
    def audit(self) -> AuditMiddleware:
        """Return the audit middleware."""
        return self._audit

    def before_tool(
        self,
        tool_name: str,
        arguments: dict[str, Any],
        user_id: str | None = None,
    ) -> ToolInvocation:
        """Call before tool execution.

        Applies rate limiting check.

        Args:
            tool_name: Name of the tool being invoked
            arguments: Tool arguments
            user_id: Optional user identifier

        Returns:
            ToolInvocation tracking object

        Raises:
            RateLimitError: If rate limit exceeded
        """
        logger.debug(f"Tool call: {tool_name} with args: {arguments}")

        # Create context for rate limiting
        context = ToolContext(tool_name=tool_name, arguments=arguments)

        # Check rate limit (raises RateLimitError if exceeded)
        self._rate_limiter.validate_request(context)

        # Return invocation tracker
        return ToolInvocation(
            tool_name=tool_name,
            arguments=arguments,
            start_time=time.perf_counter(),
            user_id=user_id,
        )

    def after_tool(
        self,
        invocation: ToolInvocation,
        success: bool,
        result: Any = None,
        error: str | None = None,
    ) -> None:
        """Call after tool execution.

        Logs audit entry.

        Args:
            invocation: The invocation tracking object from before_tool
            success: Whether the tool succeeded
            result: Tool result (not logged, just for completeness)
            error: Error message if failed
        """
        duration_ms = (time.perf_counter() - invocation.start_time) * 1000

        if success:
            logger.debug(f"Tool completed: {invocation.tool_name} in {duration_ms:.2f}ms")
        else:
            logger.warning(f"Tool failed: {invocation.tool_name} in {duration_ms:.2f}ms - {error}")

        context = ToolContext(
            tool_name=invocation.tool_name,
            arguments=invocation.arguments,
        )
        tool_result = ToolResult(success=success, data=result, error=error)

        entry = self._audit.create_audit_entry(
            context, tool_result, duration_ms, invocation.user_id
        )
        self._audit.log_audit_entry(entry)

    def close(self) -> None:
        """Close middleware resources."""
        self._audit.close()

    def get_stats(self) -> dict[str, Any]:
        """Get middleware statistics.

        Returns:
            Dictionary with rate limit and audit stats
        """
        return {
            "rate_limit": self._rate_limiter.get_stats(),
            "audit": self._audit.get_stats(),
        }
