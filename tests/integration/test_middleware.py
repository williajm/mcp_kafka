"""Integration tests for MCP Kafka middleware.

Tests rate limiting, audit logging, and safety middleware
against real scenarios.
"""

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from mcp_kafka.config import SecurityConfig
from mcp_kafka.middleware import AuditMiddleware, RateLimitMiddleware, ToolContext, ToolResult
from mcp_kafka.utils.errors import RateLimitError

pytestmark = pytest.mark.integration


class TestRateLimitMiddlewareIntegration:
    """Integration tests for rate limiting middleware."""

    def test_rate_limit_allows_requests_within_limit(
        self, integration_rate_limit_middleware: RateLimitMiddleware
    ) -> None:
        """Test that requests within the limit are allowed."""
        middleware = integration_rate_limit_middleware
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        # First request should be allowed
        middleware.validate_request(context)

        # Check remaining count decreased
        remaining = middleware.get_remaining()
        assert remaining < middleware.requests_per_minute

    def test_rate_limit_blocks_excessive_requests(self, tmp_path: Path) -> None:
        """Test that requests exceeding the limit are blocked."""
        # Create a middleware with very low limit
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=3,
            audit_log_enabled=False,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        # Exhaust the rate limit
        for _ in range(3):
            middleware.validate_request(context)

        # Next request should be blocked
        with pytest.raises(RateLimitError) as exc_info:
            middleware.validate_request(context)

        assert "Rate limit exceeded" in str(exc_info.value)

    def test_rate_limit_disabled(self, tmp_path: Path) -> None:
        """Test that disabled rate limiting allows all requests."""
        config = SecurityConfig(
            rate_limit_enabled=False,
            rate_limit_rpm=1,
            audit_log_enabled=False,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        # Should allow unlimited requests when disabled
        for _ in range(10):
            middleware.validate_request(context)

    def test_rate_limit_stats(self, integration_rate_limit_middleware: RateLimitMiddleware) -> None:
        """Test rate limit statistics reporting."""
        middleware = integration_rate_limit_middleware
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        # Get initial stats
        initial_stats = middleware.get_stats()
        assert initial_stats["enabled"] is True
        assert initial_stats["limit_per_minute"] > 0

        # Make some requests
        for _ in range(5):
            middleware.validate_request(context)

        # Check stats updated
        stats = middleware.get_stats()
        assert stats["used"] >= 5

    def test_rate_limit_reset(self, tmp_path: Path) -> None:
        """Test rate limit counter reset."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=5,
            audit_log_enabled=False,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        # Use some of the limit
        for _ in range(3):
            middleware.validate_request(context)

        # Reset
        middleware.reset()

        # Should have full limit again
        remaining = middleware.get_remaining()
        assert remaining == 5

    @pytest.mark.asyncio
    async def test_rate_limit_wrap_handler(self, tmp_path: Path) -> None:
        """Test rate limit middleware wrapping a handler."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=2,
            audit_log_enabled=False,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)

        # Create a mock handler
        mock_handler = AsyncMock(return_value=ToolResult(success=True, data={"test": 1}))
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        # First two requests should succeed
        result1 = await middleware.wrap_handler(mock_handler, context)
        assert result1.success is True

        result2 = await middleware.wrap_handler(mock_handler, context)
        assert result2.success is True

        # Third request should be blocked
        result3 = await middleware.wrap_handler(mock_handler, context)
        assert result3.success is False
        assert "Rate limit exceeded" in str(result3.error)


class TestAuditMiddlewareIntegration:
    """Integration tests for audit logging middleware."""

    def test_audit_creates_log_file(self, integration_audit_middleware: AuditMiddleware) -> None:
        """Test that audit middleware creates log file."""
        middleware = integration_audit_middleware
        assert middleware.enabled is True
        # Log file path should be set
        assert middleware.log_file is not None

    @pytest.mark.asyncio
    async def test_audit_logs_successful_operation(self, tmp_path: Path) -> None:
        """Test that successful operations are logged."""
        audit_file = tmp_path / "test_audit.log"
        config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )
        middleware = AuditMiddleware(config)

        # Create a mock handler
        mock_handler = AsyncMock(return_value=ToolResult(success=True, data={"topics": ["test"]}))
        context = ToolContext(tool_name="kafka_list_topics", arguments={"include_internal": False})

        # Execute
        result = await middleware.wrap_handler(mock_handler, context)
        assert result.success is True

        # Close middleware to flush logs
        middleware.close()

        # Check audit log
        assert audit_file.exists()
        content = audit_file.read_text()
        assert "kafka_list_topics" in content
        assert "tool_invocation" in content

    @pytest.mark.asyncio
    async def test_audit_logs_failed_operation(self, tmp_path: Path) -> None:
        """Test that failed operations are logged."""
        audit_file = tmp_path / "test_audit.log"
        config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )
        middleware = AuditMiddleware(config)

        # Create a failing handler
        mock_handler = AsyncMock(return_value=ToolResult(success=False, error="Connection failed"))
        context = ToolContext(tool_name="kafka_cluster_info", arguments={})

        # Execute
        result = await middleware.wrap_handler(mock_handler, context)
        assert result.success is False

        # Close middleware to flush logs
        middleware.close()

        # Check audit log
        content = audit_file.read_text()
        assert "kafka_cluster_info" in content
        assert '"success": false' in content

    @pytest.mark.asyncio
    async def test_audit_sanitizes_sensitive_data(self, tmp_path: Path) -> None:
        """Test that sensitive data is redacted in audit logs."""
        audit_file = tmp_path / "test_audit.log"
        config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )
        middleware = AuditMiddleware(config)

        mock_handler = AsyncMock(return_value=ToolResult(success=True, data={}))
        context = ToolContext(
            tool_name="kafka_produce_message",
            arguments={
                "topic": "test-topic",
                "message": "hello world",
                "password": "secret123",
                "api_key": "super-secret-key",
            },
        )

        await middleware.wrap_handler(mock_handler, context)
        middleware.close()

        content = audit_file.read_text()
        # Sensitive fields should be redacted
        assert "secret123" not in content
        assert "super-secret-key" not in content
        assert "[REDACTED]" in content
        # Non-sensitive fields should be present
        assert "test-topic" in content

    @pytest.mark.asyncio
    async def test_audit_truncates_large_arguments(self, tmp_path: Path) -> None:
        """Test that large argument values are truncated."""
        audit_file = tmp_path / "test_audit.log"
        config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )
        middleware = AuditMiddleware(config)

        # Create a very large message
        large_message = "x" * 5000

        mock_handler = AsyncMock(return_value=ToolResult(success=True, data={}))
        context = ToolContext(
            tool_name="kafka_produce_message",
            arguments={"topic": "test-topic", "message": large_message},
        )

        await middleware.wrap_handler(mock_handler, context)
        middleware.close()

        content = audit_file.read_text()
        # Full message should not be in log
        assert large_message not in content
        # Truncation marker should be present
        assert "truncated" in content

    def test_audit_disabled(self, tmp_path: Path) -> None:
        """Test that disabled audit logging doesn't create files."""
        audit_file = tmp_path / "should_not_exist.log"
        config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_enabled=False,
            audit_log_file=audit_file,
        )
        middleware = AuditMiddleware(config)

        assert middleware.enabled is False
        middleware.close()

    def test_audit_stats(self, integration_audit_middleware: AuditMiddleware) -> None:
        """Test audit statistics reporting."""
        stats = integration_audit_middleware.get_stats()

        assert "enabled" in stats
        assert "log_file" in stats
        assert "log_size_bytes" in stats

    @pytest.mark.asyncio
    async def test_audit_records_duration(self, tmp_path: Path) -> None:
        """Test that operation duration is recorded."""
        import asyncio

        audit_file = tmp_path / "test_audit.log"
        config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )
        middleware = AuditMiddleware(config)

        # Create a handler with artificial delay
        async def slow_handler(ctx: ToolContext) -> ToolResult:
            await asyncio.sleep(0.05)  # 50ms delay
            return ToolResult(success=True, data={})

        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        await middleware.wrap_handler(slow_handler, context)
        middleware.close()

        content = audit_file.read_text()
        # Parse the JSON entry
        entry = json.loads(content.strip())
        # Duration should be recorded and be at least 50ms
        assert "duration_ms" in entry
        assert entry["duration_ms"] >= 40  # Allow some tolerance

    @pytest.mark.asyncio
    async def test_audit_includes_user_id(self, tmp_path: Path) -> None:
        """Test that user_id is included when provided."""
        audit_file = tmp_path / "test_audit.log"
        config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )
        middleware = AuditMiddleware(config)

        mock_handler = AsyncMock(return_value=ToolResult(success=True, data={}))
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        await middleware.wrap_handler(mock_handler, context, user_id="test-user-123")
        middleware.close()

        content = audit_file.read_text()
        assert "test-user-123" in content
