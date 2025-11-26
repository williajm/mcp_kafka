"""Tests for logger module."""

from pathlib import Path
from unittest.mock import patch

from mcp_kafka.config import ServerConfig
from mcp_kafka.utils.logger import get_logger, setup_logger


class TestLogger:
    """Tests for logger functions."""

    def test_get_logger_returns_logger(self) -> None:
        """Test get_logger returns a logger instance."""
        log = get_logger("test")
        assert log is not None

    def test_get_logger_without_name(self) -> None:
        """Test get_logger works without name."""
        log = get_logger()
        assert log is not None

    def test_setup_logger_human_readable(self, server_config: ServerConfig) -> None:
        """Test setup_logger with human-readable format."""
        with patch("mcp_kafka.utils.logger.logger") as mock_logger:
            setup_logger(server_config)
            # Should remove default handler and add new ones
            mock_logger.remove.assert_called_once()
            mock_logger.add.assert_called()

    def test_setup_logger_json(self, server_config: ServerConfig) -> None:
        """Test setup_logger with JSON format."""
        server_config.json_logging = True
        with patch("mcp_kafka.utils.logger.logger") as mock_logger:
            setup_logger(server_config)
            mock_logger.remove.assert_called_once()
            # Check that serialize=True was passed
            calls = mock_logger.add.call_args_list
            assert any(call.kwargs.get("serialize") is True for call in calls)

    def test_setup_logger_with_file(self, server_config: ServerConfig, tmp_path: Path) -> None:
        """Test setup_logger with log file."""
        log_file = tmp_path / "test.log"
        with patch("mcp_kafka.utils.logger.logger") as mock_logger:
            setup_logger(server_config, log_file)
            # Should add stderr handler and file handler
            assert mock_logger.add.call_count >= 2
