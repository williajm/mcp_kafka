"""Rate limiting middleware for MCP Kafka server.

This middleware enforces request rate limits to prevent abuse
and ensure fair resource usage.
"""

from collections.abc import Awaitable, Callable
from typing import Any

from limits import RateLimitItemPerMinute, storage, strategies
from loguru import logger

from mcp_kafka.config import SecurityConfig
from mcp_kafka.middleware.safety import ToolContext, ToolHandler, ToolResult
from mcp_kafka.utils.errors import RateLimitError


class RateLimitMiddleware:
    """Middleware that enforces rate limits on tool invocations.

    Uses an in-memory sliding window rate limiter to limit the number
    of requests per minute. Rate limits are applied globally across
    all tools.
    """

    def __init__(self, config: SecurityConfig) -> None:
        """Initialize rate limit middleware with configuration.

        Args:
            config: Security configuration containing rate limit settings
        """
        self._config = config
        self._enabled = config.rate_limit_enabled
        self._rpm = config.rate_limit_rpm

        # Initialize in-memory storage and rate limiter
        self._storage = storage.MemoryStorage()
        self._limiter = strategies.MovingWindowRateLimiter(self._storage)
        self._rate_limit = RateLimitItemPerMinute(self._rpm)

        logger.debug(f"Rate limit middleware initialized: enabled={self._enabled}, rpm={self._rpm}")

    @property
    def enabled(self) -> bool:
        """Return whether rate limiting is enabled."""
        return self._enabled

    @property
    def requests_per_minute(self) -> int:
        """Return the configured requests per minute limit."""
        return self._rpm

    def check_rate_limit(self, key: str = "global") -> bool:
        """Check if a request is within the rate limit.

        Args:
            key: Identifier for rate limit bucket (default: "global")

        Returns:
            True if within limit, False if exceeded
        """
        if not self._enabled:
            return True

        return bool(self._limiter.test(self._rate_limit, key))

    def hit_rate_limit(self, key: str = "global") -> bool:
        """Record a request and check if within rate limit.

        Args:
            key: Identifier for rate limit bucket (default: "global")

        Returns:
            True if request was allowed, False if rate limit exceeded
        """
        if not self._enabled:
            return True

        return bool(self._limiter.hit(self._rate_limit, key))

    def get_remaining(self, key: str = "global") -> int:
        """Get the number of remaining requests in the current window.

        Args:
            key: Identifier for rate limit bucket (default: "global")

        Returns:
            Number of remaining requests
        """
        if not self._enabled:
            return self._rpm

        window_stats = self._limiter.get_window_stats(self._rate_limit, key)
        return max(0, int(window_stats.remaining))

    def validate_request(self, context: ToolContext) -> None:
        """Validate a tool request against rate limits.

        Args:
            context: Tool invocation context

        Raises:
            RateLimitError: If rate limit is exceeded
        """
        if not self._enabled:
            return

        key = "global"  # Use global rate limiting for simplicity

        if not self.hit_rate_limit(key):
            remaining = self.get_remaining(key)
            logger.warning(
                f"Rate limit exceeded for tool '{context.tool_name}': "
                f"limit={self._rpm}/min, remaining={remaining}"
            )
            raise RateLimitError(
                f"Rate limit exceeded. Maximum {self._rpm} requests per minute allowed. "
                "Please wait before making more requests."
            )

        logger.debug(
            f"Rate limit check passed for '{context.tool_name}': "
            f"remaining={self.get_remaining(key)}/{self._rpm}"
        )

    async def wrap_handler(self, handler: ToolHandler, context: ToolContext) -> ToolResult:
        """Wrap a tool handler with rate limit validation.

        Args:
            handler: The original tool handler
            context: Tool invocation context

        Returns:
            ToolResult from the handler or an error result
        """
        try:
            self.validate_request(context)
            return await handler(context)
        except RateLimitError as e:
            return ToolResult(success=False, error=str(e))
        except Exception as e:
            logger.error(f"Unexpected error in rate limit middleware: {e}")
            return ToolResult(success=False, error=f"Internal error: {e}")

    def create_wrapper(
        self, handler: ToolHandler
    ) -> Callable[[ToolContext], Awaitable[ToolResult]]:
        """Create a wrapped handler that includes rate limit validation.

        Args:
            handler: The original tool handler

        Returns:
            Wrapped handler function
        """

        async def wrapped(context: ToolContext) -> ToolResult:
            return await self.wrap_handler(handler, context)

        return wrapped

    def reset(self, key: str = "global") -> None:
        """Reset rate limit counter for testing purposes.

        Args:
            key: Identifier for rate limit bucket to reset
        """
        self._storage.reset()
        logger.debug(f"Rate limit counter reset for key: {key}")

    def get_stats(self, key: str = "global") -> dict[str, Any]:
        """Get rate limit statistics for monitoring.

        Args:
            key: Identifier for rate limit bucket

        Returns:
            Dictionary containing rate limit statistics
        """
        remaining = self.get_remaining(key)
        return {
            "enabled": self._enabled,
            "limit_per_minute": self._rpm,
            "remaining": remaining,
            "used": self._rpm - remaining,
        }
