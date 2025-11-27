"""Unit tests for middleware stack."""

import pytest

from mcp_kafka.config import SecurityConfig
from mcp_kafka.middleware.stack import MiddlewareStack, ToolInvocation
from mcp_kafka.utils.errors import RateLimitError


class TestMiddlewareStack:
    """Tests for MiddlewareStack."""

    def test_init_creates_middleware(self) -> None:
        """Test that init creates rate limiter and audit middleware."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=60,
            audit_log_enabled=True,
        )
        stack = MiddlewareStack(config)

        assert stack.rate_limiter is not None
        assert stack.audit is not None
        assert stack.rate_limiter.enabled is True
        assert stack.audit.enabled is True

        stack.close()

    def test_before_tool_returns_invocation(self) -> None:
        """Test that before_tool returns a ToolInvocation."""
        config = SecurityConfig(
            rate_limit_enabled=False,  # Disable to avoid rate limit
            audit_log_enabled=False,
        )
        stack = MiddlewareStack(config)

        invocation = stack.before_tool("test_tool", {"arg1": "value1"})

        assert isinstance(invocation, ToolInvocation)
        assert invocation.tool_name == "test_tool"
        assert invocation.arguments == {"arg1": "value1"}
        assert invocation.start_time > 0

        stack.close()

    def test_before_tool_checks_rate_limit(self) -> None:
        """Test that before_tool enforces rate limiting."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=1,  # Very low limit
            audit_log_enabled=False,
        )
        stack = MiddlewareStack(config)

        # First call should succeed
        stack.before_tool("test_tool", {})

        # Second call should fail (rate limit exceeded)
        with pytest.raises(RateLimitError):
            stack.before_tool("test_tool", {})

        stack.close()

    def test_after_tool_logs_audit(self, tmp_path) -> None:
        """Test that after_tool creates audit log entry."""
        audit_file = tmp_path / "audit.log"
        config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )
        stack = MiddlewareStack(config)

        invocation = stack.before_tool("test_tool", {"key": "value"})
        stack.after_tool(invocation, success=True, result={"data": "test"})

        stack.close()

        # Verify audit log was written
        assert audit_file.exists()
        content = audit_file.read_text()
        assert "test_tool" in content
        assert "success" in content.lower() or "true" in content.lower()

    def test_after_tool_logs_failure(self, tmp_path) -> None:
        """Test that after_tool logs failures."""
        audit_file = tmp_path / "audit.log"
        config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )
        stack = MiddlewareStack(config)

        invocation = stack.before_tool("test_tool", {})
        stack.after_tool(invocation, success=False, error="Test error")

        stack.close()

        content = audit_file.read_text()
        assert "test_tool" in content
        assert "Test error" in content

    def test_get_stats(self) -> None:
        """Test that get_stats returns middleware statistics."""
        config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=60,
            audit_log_enabled=True,
        )
        stack = MiddlewareStack(config)

        stats = stack.get_stats()

        assert "rate_limit" in stats
        assert "audit" in stats
        assert stats["rate_limit"]["enabled"] is True
        assert stats["rate_limit"]["limit_per_minute"] == 60
        assert stats["audit"]["enabled"] is True

        stack.close()

    def test_disabled_middleware(self) -> None:
        """Test stack with disabled middleware."""
        config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_enabled=False,
        )
        stack = MiddlewareStack(config)

        # Should not raise even with high call volume
        for _ in range(100):
            invocation = stack.before_tool("test_tool", {})
            stack.after_tool(invocation, success=True)

        stats = stack.get_stats()
        assert stats["rate_limit"]["enabled"] is False
        assert stats["audit"]["enabled"] is False

        stack.close()


class TestToolInvocation:
    """Tests for ToolInvocation dataclass."""

    def test_tool_invocation_fields(self) -> None:
        """Test ToolInvocation has expected fields."""
        invocation = ToolInvocation(
            tool_name="test",
            arguments={"a": 1},
            start_time=123.456,
            user_id="user123",
        )

        assert invocation.tool_name == "test"
        assert invocation.arguments == {"a": 1}
        assert invocation.start_time == 123.456
        assert invocation.user_id == "user123"

    def test_tool_invocation_optional_user_id(self) -> None:
        """Test ToolInvocation with no user_id."""
        invocation = ToolInvocation(
            tool_name="test",
            arguments={},
            start_time=0.0,
        )

        assert invocation.user_id is None
