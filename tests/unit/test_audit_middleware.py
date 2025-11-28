"""Unit tests for audit middleware."""

import json
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from mcp_kafka.config import SecurityConfig
from mcp_kafka.middleware import AuditMiddleware, ToolContext, ToolResult


class TestAuditMiddlewareInit:
    """Tests for AuditMiddleware initialization."""

    def test_enabled_config(self, tmp_path: Path) -> None:
        """Test initialization with audit enabled."""
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        assert middleware.enabled is True
        assert middleware.log_file == tmp_path / "audit.log"
        middleware.close()

    def test_disabled_config(self, tmp_path: Path) -> None:
        """Test initialization with audit disabled."""
        config = SecurityConfig(
            audit_log_enabled=False,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        assert middleware.enabled is False
        middleware.close()


class TestAuditMiddlewareSanitization:
    """Tests for argument sanitization."""

    def test_sanitize_normal_arguments(self, tmp_path: Path) -> None:
        """Test sanitization of normal arguments."""
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        args = {"topic": "test-topic", "partition": 0}
        sanitized = middleware._sanitize_arguments(args)

        assert sanitized["topic"] == "test-topic"
        assert sanitized["partition"] == 0
        middleware.close()

    def test_sanitize_sensitive_arguments(self, tmp_path: Path) -> None:
        """Test sanitization of sensitive arguments."""
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        args = {
            "topic": "test-topic",
            "password": "secret123",
            "api_key": "key-abc",
            "auth_token": "token-xyz",
        }
        sanitized = middleware._sanitize_arguments(args)

        assert sanitized["topic"] == "test-topic"
        assert sanitized["password"] == "[REDACTED]"
        assert sanitized["api_key"] == "[REDACTED]"
        assert sanitized["auth_token"] == "[REDACTED]"
        middleware.close()

    def test_sanitize_expanded_sensitive_keys(self, tmp_path: Path) -> None:
        """Test sanitization of expanded set of sensitive keys."""
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        args = {
            "topic": "test-topic",
            "passwd": "pass123",
            "pwd": "pwd123",
            "apikey": "apikey123",
            "bearer_token": "bearer123",
            "jwt_token": "jwt123",
            "sasl_password": "sasl123",
            "ssl_key": "ssl123",
            "private_key": "private123",
        }
        sanitized = middleware._sanitize_arguments(args)

        # Normal args should pass through
        assert sanitized["topic"] == "test-topic"

        # All sensitive keys should be redacted
        assert sanitized["passwd"] == "[REDACTED]"
        assert sanitized["pwd"] == "[REDACTED]"
        assert sanitized["apikey"] == "[REDACTED]"
        assert sanitized["bearer_token"] == "[REDACTED]"
        assert sanitized["jwt_token"] == "[REDACTED]"
        assert sanitized["sasl_password"] == "[REDACTED]"
        assert sanitized["ssl_key"] == "[REDACTED]"
        assert sanitized["private_key"] == "[REDACTED]"
        middleware.close()

    def test_sanitize_long_string_arguments(self, tmp_path: Path) -> None:
        """Test sanitization of very long string arguments."""
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        long_message = "x" * 2000
        args = {"message": long_message}
        sanitized = middleware._sanitize_arguments(args)

        assert "truncated" in sanitized["message"]
        assert len(sanitized["message"]) < len(long_message)
        middleware.close()

    def test_sanitize_long_bytes_arguments(self, tmp_path: Path) -> None:
        """Test sanitization of very long bytes arguments."""
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        long_bytes = b"x" * 2000
        args = {"data": long_bytes}
        sanitized = middleware._sanitize_arguments(args)

        assert "bytes" in sanitized["data"]
        assert "truncated" in sanitized["data"]
        middleware.close()


class TestAuditMiddlewareAuditEntry:
    """Tests for audit entry creation."""

    def test_create_audit_entry_success(self, tmp_path: Path) -> None:
        """Test creating audit entry for successful operation."""
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        context = ToolContext(tool_name="kafka_list_topics", arguments={"internal": False})
        result = ToolResult(success=True, data={"topics": ["test"]})

        entry = middleware.create_audit_entry(context, result, 45.5)

        assert entry["tool_name"] == "kafka_list_topics"
        assert entry["success"] is True
        assert entry["error"] is None
        assert entry["duration_ms"] == 45.5
        assert entry["event_type"] == "tool_invocation"
        assert "timestamp" in entry
        middleware.close()

    def test_create_audit_entry_failure(self, tmp_path: Path) -> None:
        """Test creating audit entry for failed operation."""
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        context = ToolContext(tool_name="kafka_describe_topic", arguments={"topic": "test"})
        result = ToolResult(success=False, error="Topic not found")

        entry = middleware.create_audit_entry(context, result, 12.3)

        assert entry["success"] is False
        assert entry["error"] == "Topic not found"
        middleware.close()

    def test_create_audit_entry_with_user_id(self, tmp_path: Path) -> None:
        """Test creating audit entry with user ID."""
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        context = ToolContext(tool_name="kafka_list_topics", arguments={})
        result = ToolResult(success=True, data={})

        entry = middleware.create_audit_entry(context, result, 10.0, user_id="user-123")

        assert entry["user_id"] == "user-123"
        middleware.close()


class TestAuditMiddlewareLogging:
    """Tests for audit logging."""

    def test_log_audit_entry_disabled(self, tmp_path: Path) -> None:
        """Test that logging is skipped when disabled."""
        config = SecurityConfig(
            audit_log_enabled=False,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        entry = {"test": "entry"}
        middleware.log_audit_entry(entry)

        # File should not be created when disabled
        assert not (tmp_path / "audit.log").exists()
        middleware.close()

    def test_log_audit_entry_enabled(self, tmp_path: Path) -> None:
        """Test that entries are logged when enabled."""
        audit_file = tmp_path / "audit.log"
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )
        middleware = AuditMiddleware(config)

        entry = {"test": "entry", "value": 123}
        middleware.log_audit_entry(entry)

        middleware.close()

        # File should contain the entry
        assert audit_file.exists()
        content = audit_file.read_text()
        parsed = json.loads(content.strip())
        assert parsed["test"] == "entry"
        assert parsed["value"] == 123


class TestAuditMiddlewareWrapper:
    """Tests for handler wrapping."""

    @pytest.mark.asyncio
    async def test_wrap_handler_success(self, tmp_path: Path) -> None:
        """Test wrapping handler with successful operation."""
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        handler = AsyncMock(return_value=ToolResult(success=True, data={"test": 1}))
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        result = await middleware.wrap_handler(handler, context)

        assert result.success is True
        assert result.data == {"test": 1}
        handler.assert_called_once_with(context)
        middleware.close()

    @pytest.mark.asyncio
    async def test_wrap_handler_failure(self, tmp_path: Path) -> None:
        """Test wrapping handler with failed operation."""
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        handler = AsyncMock(return_value=ToolResult(success=False, error="Test error"))
        context = ToolContext(tool_name="kafka_describe_topic", arguments={"topic": "test"})

        result = await middleware.wrap_handler(handler, context)

        assert result.success is False
        assert result.error == "Test error"
        middleware.close()

    @pytest.mark.asyncio
    async def test_wrap_handler_exception(self, tmp_path: Path) -> None:
        """Test wrapping handler that raises exception."""
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        handler = AsyncMock(side_effect=RuntimeError("Test exception"))
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        with pytest.raises(RuntimeError):
            await middleware.wrap_handler(handler, context)

        middleware.close()

    @pytest.mark.asyncio
    async def test_wrap_handler_with_user_id(self, tmp_path: Path) -> None:
        """Test wrapping handler with user ID."""
        audit_file = tmp_path / "audit.log"
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )
        middleware = AuditMiddleware(config)

        handler = AsyncMock(return_value=ToolResult(success=True, data={}))
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        await middleware.wrap_handler(handler, context, user_id="test-user")
        middleware.close()

        content = audit_file.read_text()
        assert "test-user" in content

    @pytest.mark.asyncio
    async def test_create_wrapper(self, tmp_path: Path) -> None:
        """Test create_wrapper method."""
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        handler = AsyncMock(return_value=ToolResult(success=True, data={}))
        wrapped = middleware.create_wrapper(handler, user_id="wrapper-user")
        context = ToolContext(tool_name="kafka_list_topics", arguments={})

        result = await wrapped(context)
        assert result.success is True
        middleware.close()


class TestAuditMiddlewareStats:
    """Tests for statistics and lifecycle."""

    def test_get_stats(self, tmp_path: Path) -> None:
        """Test getting statistics."""
        audit_file = tmp_path / "audit.log"
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )
        middleware = AuditMiddleware(config)

        stats = middleware.get_stats()

        assert stats["enabled"] is True
        assert str(audit_file) in stats["log_file"]
        assert "log_size_bytes" in stats
        middleware.close()

    def test_close_removes_handler(self, tmp_path: Path) -> None:
        """Test that close removes the log handler."""
        config = SecurityConfig(
            audit_log_enabled=True,
            audit_log_file=tmp_path / "audit.log",
        )
        middleware = AuditMiddleware(config)

        # Close should not raise
        middleware.close()

        # Multiple closes should be safe
        middleware.close()
