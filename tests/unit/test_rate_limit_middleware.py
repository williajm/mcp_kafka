"""Unit tests for rate limit middleware."""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from mcp_kafka.config import SecurityConfig
from mcp_kafka.middleware import RateLimitMiddleware, ToolContext, ToolResult
from mcp_kafka.utils.errors import RateLimitError


class TestRateLimitMiddlewareInit:
    """Tests for RateLimitMiddleware initialization."""

    def test_default_config(self, tmp_path: Path) -> None:
        """Test initialization with default config."""
        config = SecurityConfig(
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)

        assert middleware.enabled is True
        assert middleware.requests_per_minute == 60

    def test_disabled_config(self, tmp_path: Path) -> None:
        """Test initialization with rate limiting disabled."""
        config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)

        assert middleware.enabled is False

    def test_custom_rpm(self, tmp_path: Path) -> None:
        """Test initialization with custom requests per minute."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=100,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)

        assert middleware.requests_per_minute == 100


class TestRateLimitMiddlewareChecks:
    """Tests for rate limit checking."""

    def test_check_rate_limit_within_limit(self, tmp_path: Path) -> None:
        """Test that requests within limit pass."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=10,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)

        assert middleware.check_rate_limit() is True

    def test_check_rate_limit_disabled(self, tmp_path: Path) -> None:
        """Test that disabled rate limit always passes."""
        config = SecurityConfig(
            rate_limit_enabled=False,
            rate_limit_rpm=1,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)

        # Should pass even many times when disabled
        for _ in range(10):
            assert middleware.check_rate_limit() is True

    def test_hit_rate_limit_tracks_requests(self, tmp_path: Path) -> None:
        """Test that hit_rate_limit tracks requests."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=5,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)

        # First 5 requests should succeed
        for _ in range(5):
            assert middleware.hit_rate_limit() is True

        # 6th request should fail
        assert middleware.hit_rate_limit() is False

    def test_get_remaining(self, tmp_path: Path) -> None:
        """Test getting remaining requests."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=10,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)

        initial = middleware.get_remaining()
        assert initial == 10

        middleware.hit_rate_limit()
        after_one = middleware.get_remaining()
        assert after_one == 9

    def test_get_remaining_disabled(self, tmp_path: Path) -> None:
        """Test get_remaining when disabled."""
        config = SecurityConfig(
            rate_limit_enabled=False,
            rate_limit_rpm=10,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)

        # When disabled, should return the limit
        assert middleware.get_remaining() == 10


class TestRateLimitMiddlewareValidation:
    """Tests for request validation."""

    def test_validate_request_passes(self, tmp_path: Path) -> None:
        """Test that valid request passes validation."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=10,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        # Should not raise
        middleware.validate_request(context)

    def test_validate_request_exceeds_limit(self, tmp_path: Path) -> None:
        """Test that exceeding limit raises error."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=2,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        # First 2 requests pass
        middleware.validate_request(context)
        middleware.validate_request(context)

        # 3rd request fails
        with pytest.raises(RateLimitError) as exc_info:
            middleware.validate_request(context)

        assert "Rate limit exceeded" in str(exc_info.value)

    def test_validate_request_disabled(self, tmp_path: Path) -> None:
        """Test that validation passes when disabled."""
        config = SecurityConfig(
            rate_limit_enabled=False,
            rate_limit_rpm=1,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        # Should pass many times when disabled
        for _ in range(10):
            middleware.validate_request(context)


class TestRateLimitMiddlewareWrapper:
    """Tests for handler wrapping."""

    @pytest.mark.asyncio
    async def test_wrap_handler_success(self, tmp_path: Path) -> None:
        """Test wrapping handler with successful request."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=10,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)

        handler = AsyncMock(return_value=ToolResult(success=True, data={"test": 1}))
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        result = await middleware.wrap_handler(handler, context)

        assert result.success is True
        assert result.data == {"test": 1}
        handler.assert_called_once_with(context)

    @pytest.mark.asyncio
    async def test_wrap_handler_rate_limit_exceeded(self, tmp_path: Path) -> None:
        """Test wrapping handler when rate limit exceeded."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=1,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)

        handler = AsyncMock(return_value=ToolResult(success=True, data={}))
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        # First request succeeds
        result1 = await middleware.wrap_handler(handler, context)
        assert result1.success is True

        # Second request fails due to rate limit
        result2 = await middleware.wrap_handler(handler, context)
        assert result2.success is False
        assert "Rate limit exceeded" in str(result2.error)

    @pytest.mark.asyncio
    async def test_create_wrapper(self, tmp_path: Path) -> None:
        """Test create_wrapper method."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=10,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)

        handler = AsyncMock(return_value=ToolResult(success=True, data={}))
        wrapped = middleware.create_wrapper(handler)
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        result = await wrapped(context)
        assert result.success is True


class TestRateLimitMiddlewareStats:
    """Tests for statistics and reset."""

    def test_reset(self, tmp_path: Path) -> None:
        """Test resetting rate limit counter."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=5,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)

        # Use some of the limit
        for _ in range(3):
            middleware.hit_rate_limit()

        assert middleware.get_remaining() == 2

        # Reset
        middleware.reset()

        assert middleware.get_remaining() == 5

    def test_get_stats(self, tmp_path: Path) -> None:
        """Test getting statistics."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=10,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = RateLimitMiddleware(config)

        # Use some requests
        middleware.hit_rate_limit()
        middleware.hit_rate_limit()

        stats = middleware.get_stats()

        assert stats["enabled"] is True
        assert stats["limit_per_minute"] == 10
        assert stats["remaining"] == 8
        assert stats["used"] == 2
