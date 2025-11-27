"""Tests for CLI entry point."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from mcp_kafka.__main__ import (
    Transport,
    app,
    create_http_middleware,
    run_health_check,
    warn_insecure_http_config,
)
from mcp_kafka.config import SecurityConfig

runner = CliRunner()


class TestTransport:
    """Tests for Transport enum."""

    def test_transport_stdio(self) -> None:
        """Test stdio transport value."""
        assert Transport.stdio == "stdio"
        assert Transport.stdio.value == "stdio"

    def test_transport_http(self) -> None:
        """Test http transport value."""
        assert Transport.http == "http"
        assert Transport.http.value == "http"


class TestCLI:
    """Tests for CLI commands."""

    def test_version_flag(self) -> None:
        """Test --version flag."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "mcp-kafka" in result.stdout

    def test_version_short_flag(self) -> None:
        """Test -v flag."""
        result = runner.invoke(app, ["-v"])
        assert result.exit_code == 0
        assert "mcp-kafka" in result.stdout

    @patch("mcp_kafka.__main__.KafkaClientWrapper")
    @patch("mcp_kafka.__main__.setup_logger")
    def test_health_check_success(
        self, mock_setup_logger: MagicMock, mock_kafka_client: MagicMock
    ) -> None:
        """Test --health-check with successful connection."""
        mock_client_instance = MagicMock()
        mock_client_instance.health_check.return_value = {
            "status": "healthy",
            "cluster_id": "test-cluster",
            "broker_count": 3,
            "topic_count": 10,
        }
        mock_kafka_client.return_value = mock_client_instance

        result = runner.invoke(app, ["--health-check"])

        assert result.exit_code == 0
        assert "healthy" in result.output
        assert "test-cluster" in result.output
        mock_client_instance.close.assert_called_once()

    @patch("mcp_kafka.__main__.KafkaClientWrapper")
    @patch("mcp_kafka.__main__.setup_logger")
    def test_health_check_failure(
        self, mock_setup_logger: MagicMock, mock_kafka_client: MagicMock
    ) -> None:
        """Test --health-check with failed connection."""
        mock_kafka_client.side_effect = Exception("Connection failed")

        # Use mix_stderr=False to capture both stdout and stderr in output
        result = runner.invoke(app, ["--health-check"], catch_exceptions=False)

        assert result.exit_code == 1
        # Error message goes to stderr, check output which combines both
        assert "Health check failed" in result.output or "Connection failed" in result.output

    @patch("mcp_kafka.__main__.create_mcp_server")
    @patch("mcp_kafka.__main__.setup_logger")
    def test_stdio_transport_creates_server(
        self, mock_setup_logger: MagicMock, mock_create_server: MagicMock
    ) -> None:
        """Test stdio transport creates and runs server."""
        mock_mcp = MagicMock()
        mock_kafka_client = MagicMock()
        mock_middleware = MagicMock()
        mock_create_server.return_value = (mock_mcp, mock_kafka_client, mock_middleware)

        runner.invoke(app, ["--transport", "stdio"])

        # Server should be created
        mock_create_server.assert_called_once()
        # MCP.run should be called
        mock_mcp.run.assert_called_once()
        # Cleanup should happen
        mock_middleware.close.assert_called_once()
        mock_kafka_client.close.assert_called_once()

    @patch("mcp_kafka.__main__.asyncio.run")
    @patch("mcp_kafka.__main__.create_mcp_server")
    @patch("mcp_kafka.__main__.setup_logger")
    def test_http_transport_creates_server(
        self,
        mock_setup_logger: MagicMock,
        mock_create_server: MagicMock,
        mock_asyncio_run: MagicMock,
    ) -> None:
        """Test HTTP transport creates and runs server."""
        mock_mcp = MagicMock()
        mock_mcp.run_http_async = AsyncMock()
        mock_kafka_client = MagicMock()
        mock_middleware = MagicMock()
        mock_create_server.return_value = (mock_mcp, mock_kafka_client, mock_middleware)

        runner.invoke(app, ["--transport", "http", "--host", "0.0.0.0", "--port", "9000"])

        # Server should be created
        mock_create_server.assert_called_once()
        # asyncio.run should be called with run_http_async coroutine
        mock_asyncio_run.assert_called_once()
        # Verify run_http_async was called with correct args
        mock_mcp.run_http_async.assert_called_once_with(
            transport="streamable-http",
            host="0.0.0.0",
            port=9000,
            middleware=None,
        )
        # Cleanup should happen
        mock_middleware.close.assert_called_once()
        mock_kafka_client.close.assert_called_once()

    def test_invalid_transport(self) -> None:
        """Test invalid transport value."""
        result = runner.invoke(app, ["--transport", "invalid"])

        assert result.exit_code == 2  # Typer returns 2 for invalid choice

    @patch("mcp_kafka.__main__.create_mcp_server")
    @patch("mcp_kafka.__main__.setup_logger")
    def test_default_transport_is_stdio(
        self, mock_setup_logger: MagicMock, mock_create_server: MagicMock
    ) -> None:
        """Test that default transport is stdio when no --transport flag is provided."""
        mock_mcp = MagicMock()
        mock_kafka_client = MagicMock()
        mock_middleware = MagicMock()
        mock_create_server.return_value = (mock_mcp, mock_kafka_client, mock_middleware)

        # Invoke without --transport flag
        runner.invoke(app, [])

        # Should use stdio (mcp.run() with no arguments)
        mock_mcp.run.assert_called_once_with()

    @patch("mcp_kafka.__main__.create_mcp_server")
    @patch("mcp_kafka.__main__.setup_logger")
    def test_stdio_transport_calls_run_without_arguments(
        self, mock_setup_logger: MagicMock, mock_create_server: MagicMock
    ) -> None:
        """Test that stdio transport calls mcp.run() without any transport arguments."""
        mock_mcp = MagicMock()
        mock_kafka_client = MagicMock()
        mock_middleware = MagicMock()
        mock_create_server.return_value = (mock_mcp, mock_kafka_client, mock_middleware)

        runner.invoke(app, ["--transport", "stdio"])

        # Verify run is called with NO arguments (not with transport="stdio")
        mock_mcp.run.assert_called_once_with()

    @patch("mcp_kafka.__main__.asyncio.run")
    @patch("mcp_kafka.__main__.create_mcp_server")
    @patch("mcp_kafka.__main__.setup_logger")
    def test_http_transport_default_host_and_port(
        self,
        mock_setup_logger: MagicMock,
        mock_create_server: MagicMock,
        mock_asyncio_run: MagicMock,
    ) -> None:
        """Test HTTP transport uses default host (127.0.0.1) and port (8000)."""
        mock_mcp = MagicMock()
        mock_mcp.run_http_async = AsyncMock()
        mock_kafka_client = MagicMock()
        mock_middleware = MagicMock()
        mock_create_server.return_value = (mock_mcp, mock_kafka_client, mock_middleware)

        # Only specify --transport http, no --host or --port
        runner.invoke(app, ["--transport", "http"])

        # Should use default host and port
        mock_mcp.run_http_async.assert_called_once_with(
            transport="streamable-http",
            host="127.0.0.1",
            port=8000,
            middleware=None,
        )

    @patch("mcp_kafka.__main__.create_mcp_server")
    @patch("mcp_kafka.__main__.setup_logger")
    def test_cleanup_happens_when_middleware_is_none(
        self, mock_setup_logger: MagicMock, mock_create_server: MagicMock
    ) -> None:
        """Test cleanup works when middleware is None."""
        mock_mcp = MagicMock()
        mock_kafka_client = MagicMock()
        # Middleware is None (can happen if security features are disabled)
        mock_create_server.return_value = (mock_mcp, mock_kafka_client, None)

        runner.invoke(app, ["--transport", "stdio"])

        # Server should run
        mock_mcp.run.assert_called_once()
        # Kafka client should still be closed
        mock_kafka_client.close.assert_called_once()

    @patch("mcp_kafka.__main__.create_mcp_server")
    @patch("mcp_kafka.__main__.setup_logger")
    def test_cleanup_happens_on_server_exception(
        self, mock_setup_logger: MagicMock, mock_create_server: MagicMock
    ) -> None:
        """Test cleanup happens even when mcp.run() raises an exception."""
        mock_mcp = MagicMock()
        mock_mcp.run.side_effect = RuntimeError("Server crashed")
        mock_kafka_client = MagicMock()
        mock_middleware = MagicMock()
        mock_create_server.return_value = (mock_mcp, mock_kafka_client, mock_middleware)

        # The exception will propagate but cleanup should still happen
        runner.invoke(app, ["--transport", "stdio"], catch_exceptions=True)

        # Cleanup should still be called due to finally block
        mock_middleware.close.assert_called_once()
        mock_kafka_client.close.assert_called_once()

    @patch("mcp_kafka.__main__.asyncio.run")
    @patch("mcp_kafka.__main__.create_mcp_server")
    @patch("mcp_kafka.__main__.setup_logger")
    def test_http_transport_custom_host_only(
        self,
        mock_setup_logger: MagicMock,
        mock_create_server: MagicMock,
        mock_asyncio_run: MagicMock,
    ) -> None:
        """Test HTTP transport with custom host but default port."""
        mock_mcp = MagicMock()
        mock_mcp.run_http_async = AsyncMock()
        mock_kafka_client = MagicMock()
        mock_middleware = MagicMock()
        mock_create_server.return_value = (mock_mcp, mock_kafka_client, mock_middleware)

        runner.invoke(app, ["--transport", "http", "--host", "0.0.0.0"])

        mock_mcp.run_http_async.assert_called_once_with(
            transport="streamable-http",
            host="0.0.0.0",
            port=8000,
            middleware=None,
        )

    @patch("mcp_kafka.__main__.asyncio.run")
    @patch("mcp_kafka.__main__.create_mcp_server")
    @patch("mcp_kafka.__main__.setup_logger")
    def test_http_transport_custom_port_only(
        self,
        mock_setup_logger: MagicMock,
        mock_create_server: MagicMock,
        mock_asyncio_run: MagicMock,
    ) -> None:
        """Test HTTP transport with custom port but default host."""
        mock_mcp = MagicMock()
        mock_mcp.run_http_async = AsyncMock()
        mock_kafka_client = MagicMock()
        mock_middleware = MagicMock()
        mock_create_server.return_value = (mock_mcp, mock_kafka_client, mock_middleware)

        runner.invoke(app, ["--transport", "http", "--port", "3000"])

        mock_mcp.run_http_async.assert_called_once_with(
            transport="streamable-http",
            host="127.0.0.1",
            port=3000,
            middleware=None,
        )


class TestCreateHttpMiddleware:
    """Tests for create_http_middleware function."""

    def test_returns_empty_list_when_oauth_disabled(self) -> None:
        """Test that no middleware is returned when OAuth is disabled."""
        config = SecurityConfig(oauth_enabled=False)
        result = create_http_middleware(config)
        assert result == []

    def test_returns_oauth_middleware_when_enabled(self) -> None:
        """Test that OAuth middleware is returned when enabled."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",  # type: ignore[arg-type]
            oauth_audience=["test-audience"],
        )
        result = create_http_middleware(config)
        assert len(result) == 1
        # Verify it's a Middleware instance wrapping OAuthMiddleware
        from starlette.middleware import Middleware

        from mcp_kafka.auth.middleware import OAuthMiddleware

        assert isinstance(result[0], Middleware)
        assert result[0].cls == OAuthMiddleware


class TestWarnInsecureHttpConfig:
    """Tests for warn_insecure_http_config function."""

    def test_no_warning_for_localhost(self) -> None:
        """Test no warning is logged for localhost binding."""
        mock_logger = MagicMock()
        config = SecurityConfig(oauth_enabled=False)

        warn_insecure_http_config("127.0.0.1", config, mock_logger)

        mock_logger.warning.assert_not_called()

    def test_no_warning_for_localhost_string(self) -> None:
        """Test no warning is logged for 'localhost' string."""
        mock_logger = MagicMock()
        config = SecurityConfig(oauth_enabled=False)

        warn_insecure_http_config("localhost", config, mock_logger)

        mock_logger.warning.assert_not_called()

    def test_no_warning_for_ipv6_localhost(self) -> None:
        """Test no warning is logged for IPv6 localhost."""
        mock_logger = MagicMock()
        config = SecurityConfig(oauth_enabled=False)

        warn_insecure_http_config("::1", config, mock_logger)

        mock_logger.warning.assert_not_called()

    def test_warning_for_non_localhost_without_oauth(self) -> None:
        """Test warning is logged for non-localhost without OAuth."""
        mock_logger = MagicMock()
        config = SecurityConfig(oauth_enabled=False)

        warn_insecure_http_config("0.0.0.0", config, mock_logger)

        # Should log multiple warnings
        assert mock_logger.warning.call_count >= 5
        # Check for key warning messages
        warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
        warning_text = " ".join(warning_calls)
        assert "SECURITY WARNING" in warning_text
        assert "OAuth" in warning_text

    def test_warning_for_non_localhost_with_oauth(self) -> None:
        """Test warning is logged for non-localhost even with OAuth (TLS warning)."""
        mock_logger = MagicMock()
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",  # type: ignore[arg-type]
        )

        warn_insecure_http_config("0.0.0.0", config, mock_logger)

        # Should still warn about TLS
        assert mock_logger.warning.call_count >= 3
        warning_calls = [str(call) for call in mock_logger.warning.call_args_list]
        warning_text = " ".join(warning_calls)
        assert "TLS" in warning_text


class TestRunHealthCheck:
    """Tests for run_health_check function."""

    @patch("mcp_kafka.__main__.KafkaClientWrapper")
    def test_successful_health_check_exits_with_zero(self, mock_kafka_client: MagicMock) -> None:
        """Test successful health check exits with code 0."""
        import typer

        mock_client_instance = MagicMock()
        mock_client_instance.health_check.return_value = {
            "status": "healthy",
            "cluster_id": "test-cluster",
            "broker_count": 3,
            "topic_count": 10,
        }
        mock_kafka_client.return_value = mock_client_instance
        mock_logger = MagicMock()
        mock_config = MagicMock()

        with pytest.raises(typer.Exit) as exc_info:
            run_health_check(mock_config, mock_logger)

        assert exc_info.value.exit_code == 0
        mock_client_instance.close.assert_called_once()

    @patch("mcp_kafka.__main__.KafkaClientWrapper")
    def test_failed_health_check_exits_with_one(self, mock_kafka_client: MagicMock) -> None:
        """Test failed health check exits with code 1."""
        import typer

        mock_kafka_client.side_effect = Exception("Connection failed")
        mock_logger = MagicMock()
        mock_config = MagicMock()

        with pytest.raises(typer.Exit) as exc_info:
            run_health_check(mock_config, mock_logger)

        assert exc_info.value.exit_code == 1
