"""Tests for CLI entry point."""

from unittest.mock import MagicMock, patch

from typer.testing import CliRunner

from mcp_kafka.__main__ import Transport, app

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

    @patch("mcp_kafka.__main__.setup_logger")
    def test_stdio_transport_not_implemented(self, mock_setup_logger: MagicMock) -> None:
        """Test stdio transport shows not implemented."""
        result = runner.invoke(app, ["--transport", "stdio"])

        assert result.exit_code == 1
        assert "not yet implemented" in result.stdout

    @patch("mcp_kafka.__main__.setup_logger")
    def test_http_transport_not_implemented(self, mock_setup_logger: MagicMock) -> None:
        """Test http transport shows not implemented."""
        result = runner.invoke(app, ["--transport", "http"])

        assert result.exit_code == 1
        assert "not yet implemented" in result.stdout
        assert "HTTP transport" in result.stdout

    @patch("mcp_kafka.__main__.setup_logger")
    def test_host_and_port_options(self, mock_setup_logger: MagicMock) -> None:
        """Test --host and --port options."""
        result = runner.invoke(app, ["--transport", "http", "--host", "0.0.0.0", "--port", "9000"])

        # Should fail since not implemented, but host/port should be parsed
        assert result.exit_code == 1
        assert "0.0.0.0:9000" in result.stdout
