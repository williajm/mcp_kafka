"""Audit logging middleware for MCP Kafka server.

This middleware logs all tool invocations to an audit log file
for compliance and security monitoring purposes.
"""

import json
import time
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from loguru import logger

from mcp_kafka.config import SecurityConfig
from mcp_kafka.middleware.safety import ToolContext, ToolHandler, ToolResult

# Constants
MAX_ARGUMENT_LENGTH = 1000  # Truncate arguments longer than this
TRUNCATED_PREVIEW_LENGTH = 100  # Number of characters to show in truncated preview


class AuditMiddleware:
    """Middleware that logs all tool invocations for audit purposes.

    Creates a structured audit trail of all operations including:
    - Timestamp
    - Tool name
    - Arguments (sanitized)
    - Result status
    - Duration
    """

    def __init__(self, config: SecurityConfig) -> None:
        """Initialize audit middleware with configuration.

        Args:
            config: Security configuration containing audit settings
        """
        self._config = config
        self._enabled = config.audit_log_enabled
        self._log_file = config.audit_log_file
        self._audit_logger: int | None = None

        if self._enabled:
            self._setup_audit_logger()

        logger.debug(
            f"Audit middleware initialized: enabled={self._enabled}, log_file={self._log_file}"
        )

    def _setup_audit_logger(self) -> None:
        """Set up dedicated audit logger with file rotation."""
        # Ensure parent directory exists
        self._log_file.parent.mkdir(parents=True, exist_ok=True)

        # Add audit log handler with rotation
        self._audit_logger = logger.add(
            self._log_file,
            format="{message}",  # Raw JSON format
            rotation="10 MB",
            retention="90 days",
            compression="gz",
            filter=lambda record: record["extra"].get("audit", False),
            serialize=False,  # We'll handle JSON serialization ourselves
        )

    @property
    def enabled(self) -> bool:
        """Return whether audit logging is enabled."""
        return self._enabled

    @property
    def log_file(self) -> Path:
        """Return the audit log file path."""
        return self._log_file

    def _sanitize_arguments(self, arguments: dict[str, Any]) -> dict[str, Any]:
        """Sanitize arguments to remove sensitive data.

        Args:
            arguments: Raw tool arguments

        Returns:
            Sanitized arguments safe for logging
        """
        sensitive_keys = {
            "password",
            "passwd",
            "pwd",
            "secret",
            "key",
            "apikey",
            "api_key",
            "token",
            "bearer",
            "jwt",
            "credential",
            "auth",
            "sasl",
            "ssl",
            "private",
        }
        sanitized: dict[str, Any] = {}

        for k, v in arguments.items():
            key_lower = k.lower()
            if any(sensitive in key_lower for sensitive in sensitive_keys):
                sanitized[k] = "[REDACTED]"
            elif isinstance(v, str) and len(v) > MAX_ARGUMENT_LENGTH:
                sanitized[k] = f"{v[:TRUNCATED_PREVIEW_LENGTH]}...[truncated, {len(v)} chars]"
            elif isinstance(v, bytes) and len(v) > MAX_ARGUMENT_LENGTH:
                sanitized[k] = f"[bytes, {len(v)} bytes, truncated]"
            else:
                sanitized[k] = v

        return sanitized

    def create_audit_entry(
        self,
        context: ToolContext,
        result: ToolResult,
        duration_ms: float,
        user_id: str | None = None,
    ) -> dict[str, Any]:
        """Create an audit log entry.

        Args:
            context: Tool invocation context
            result: Result from tool execution
            duration_ms: Execution duration in milliseconds
            user_id: Optional user identifier

        Returns:
            Audit entry dictionary
        """
        return {
            "timestamp": datetime.now(UTC).isoformat(),
            "event_type": "tool_invocation",
            "tool_name": context.tool_name,
            "arguments": self._sanitize_arguments(context.arguments),
            "success": result.success,
            "error": result.error,
            "duration_ms": round(duration_ms, 2),
            "user_id": user_id,
        }

    def log_audit_entry(self, entry: dict[str, Any]) -> None:
        """Write an audit entry to the log file.

        Args:
            entry: Audit entry dictionary to log
        """
        if not self._enabled:
            return

        try:
            json_entry = json.dumps(entry, default=str)
            logger.bind(audit=True).info(json_entry)
        except Exception as e:
            logger.error(f"Failed to write audit log entry: {e}")

    async def wrap_handler(
        self,
        handler: ToolHandler,
        context: ToolContext,
        user_id: str | None = None,
    ) -> ToolResult:
        """Wrap a tool handler with audit logging.

        Args:
            handler: The original tool handler
            context: Tool invocation context
            user_id: Optional user identifier for audit trail

        Returns:
            ToolResult from the handler
        """
        start_time = time.perf_counter()

        try:
            result = await handler(context)
        except Exception as e:
            # Log failed executions too
            end_time = time.perf_counter()
            duration_ms = (end_time - start_time) * 1000
            result = ToolResult(success=False, error=str(e))
            entry = self.create_audit_entry(context, result, duration_ms, user_id)
            self.log_audit_entry(entry)
            raise

        end_time = time.perf_counter()
        duration_ms = (end_time - start_time) * 1000

        entry = self.create_audit_entry(context, result, duration_ms, user_id)
        self.log_audit_entry(entry)

        return result

    def create_wrapper(
        self, handler: ToolHandler, user_id: str | None = None
    ) -> Callable[[ToolContext], Awaitable[ToolResult]]:
        """Create a wrapped handler that includes audit logging.

        Args:
            handler: The original tool handler
            user_id: Optional user identifier for audit trail

        Returns:
            Wrapped handler function
        """

        async def wrapped(context: ToolContext) -> ToolResult:
            return await self.wrap_handler(handler, context, user_id)

        return wrapped

    def close(self) -> None:
        """Close the audit logger and release resources."""
        if self._audit_logger is not None:
            try:
                logger.remove(self._audit_logger)
                self._audit_logger = None
            except ValueError:
                # Handler already removed
                pass

    def get_stats(self) -> dict[str, Any]:
        """Get audit logger statistics for monitoring.

        Returns:
            Dictionary containing audit logger statistics
        """
        log_size = 0
        if self._log_file.exists():
            log_size = self._log_file.stat().st_size

        return {
            "enabled": self._enabled,
            "log_file": str(self._log_file),
            "log_size_bytes": log_size,
        }
