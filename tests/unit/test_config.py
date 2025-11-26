"""Tests for configuration module."""

import pytest

from mcp_kafka.config import (
    Config,
    KafkaConfig,
    SafetyConfig,
    SecurityConfig,
    ServerConfig,
    _parse_comma_separated_list,
)


class TestParseCommaSeparatedList:
    """Tests for _parse_comma_separated_list helper."""

    def test_parse_none(self) -> None:
        """Test parsing None returns empty list."""
        assert _parse_comma_separated_list(None) == []

    def test_parse_empty_string(self) -> None:
        """Test parsing empty string returns empty list."""
        assert _parse_comma_separated_list("") == []

    def test_parse_list(self) -> None:
        """Test parsing list returns same list."""
        assert _parse_comma_separated_list(["a", "b"]) == ["a", "b"]

    def test_parse_comma_separated(self) -> None:
        """Test parsing comma-separated string."""
        assert _parse_comma_separated_list("a,b,c") == ["a", "b", "c"]

    def test_parse_comma_separated_with_spaces(self) -> None:
        """Test parsing comma-separated string with spaces."""
        assert _parse_comma_separated_list("a, b, c") == ["a", "b", "c"]

    def test_parse_json_array(self) -> None:
        """Test parsing JSON array string."""
        assert _parse_comma_separated_list('["a", "b"]') == ["a", "b"]


class TestKafkaConfig:
    """Tests for KafkaConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = KafkaConfig()
        assert config.bootstrap_servers == "localhost:9092"
        assert config.client_id == "mcp-kafka"
        assert config.timeout == 30
        assert config.security_protocol == "PLAINTEXT"
        assert config.sasl_mechanism is None

    def test_custom_values(self) -> None:
        """Test custom configuration values."""
        config = KafkaConfig(
            bootstrap_servers="kafka:9093",
            client_id="test-client",
            timeout=60,
        )
        assert config.bootstrap_servers == "kafka:9093"
        assert config.client_id == "test-client"
        assert config.timeout == 60

    def test_invalid_security_protocol(self) -> None:
        """Test invalid security protocol raises error."""
        with pytest.raises(ValueError, match="Invalid security_protocol"):
            KafkaConfig(security_protocol="INVALID")

    def test_valid_security_protocols(self) -> None:
        """Test valid security protocols."""
        for protocol in ["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"]:
            config = KafkaConfig(security_protocol=protocol)
            assert config.security_protocol == protocol

    def test_invalid_sasl_mechanism(self) -> None:
        """Test invalid SASL mechanism raises error."""
        with pytest.raises(ValueError, match="Invalid sasl_mechanism"):
            KafkaConfig(sasl_mechanism="INVALID")

    def test_valid_sasl_mechanisms(self) -> None:
        """Test valid SASL mechanisms."""
        for mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI"]:
            if mechanism == "GSSAPI":
                config = KafkaConfig(
                    sasl_mechanism=mechanism,
                    sasl_kerberos_principal="test@REALM",
                )
            else:
                config = KafkaConfig(
                    sasl_mechanism=mechanism,
                    sasl_username="user",
                    sasl_password="pass",
                )
            assert config.sasl_mechanism == mechanism

    def test_sasl_plain_requires_credentials(self) -> None:
        """Test SASL PLAIN requires username and password."""
        with pytest.raises(ValueError, match="requires"):
            KafkaConfig(sasl_mechanism="PLAIN")

    def test_sasl_gssapi_requires_principal(self) -> None:
        """Test SASL GSSAPI requires principal."""
        with pytest.raises(ValueError, match="requires"):
            KafkaConfig(sasl_mechanism="GSSAPI")


class TestSafetyConfig:
    """Tests for SafetyConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = SafetyConfig()
        assert config.allow_write_operations is False
        assert config.max_consume_messages == 100
        assert config.max_message_size == 1048576
        assert "__consumer_offsets" in config.topic_blocklist

    def test_topic_blocklist_parsing(self) -> None:
        """Test topic blocklist parsing."""
        config = SafetyConfig(topic_blocklist="topic1,topic2")
        assert config.topic_blocklist == ["topic1", "topic2"]

    def test_topic_blocklist_list(self) -> None:
        """Test topic blocklist as list."""
        config = SafetyConfig(topic_blocklist=["topic1", "topic2"])
        assert config.topic_blocklist == ["topic1", "topic2"]

    def test_allowed_tools_none(self) -> None:
        """Test allowed_tools None means allow all."""
        config = SafetyConfig()
        assert config.allowed_tools is None

    def test_allowed_tools_parsing(self) -> None:
        """Test allowed_tools parsing."""
        config = SafetyConfig(allowed_tools="tool1,tool2")
        assert config.allowed_tools == ["tool1", "tool2"]

    def test_denied_tools_parsing(self) -> None:
        """Test denied_tools parsing."""
        config = SafetyConfig(denied_tools="tool1,tool2")
        assert config.denied_tools == ["tool1", "tool2"]


class TestSecurityConfig:
    """Tests for SecurityConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = SecurityConfig()
        assert config.rate_limit_enabled is True
        assert config.rate_limit_rpm == 60
        assert config.audit_log_enabled is True
        assert config.oauth_enabled is False

    def test_allowed_client_ips_parsing(self) -> None:
        """Test allowed_client_ips parsing."""
        config = SecurityConfig(allowed_client_ips="192.168.1.1,192.168.1.2")
        assert config.allowed_client_ips == ["192.168.1.1", "192.168.1.2"]

    def test_oauth_requires_issuer(self) -> None:
        """Test OAuth requires issuer when enabled."""
        with pytest.raises(ValueError, match="oauth_issuer not configured"):
            SecurityConfig(oauth_enabled=True)

    def test_oauth_with_issuer(self) -> None:
        """Test OAuth with issuer configured."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",
        )
        assert config.oauth_enabled is True


class TestServerConfig:
    """Tests for ServerConfig."""

    def test_default_values(self) -> None:
        """Test default configuration values."""
        config = ServerConfig()
        assert config.server_name == "mcp-kafka"
        assert config.log_level == "INFO"
        assert config.json_logging is False
        assert config.debug_mode is False

    def test_log_level_validation(self) -> None:
        """Test log level validation."""
        with pytest.raises(ValueError, match="Invalid log level"):
            ServerConfig(log_level="INVALID")

    def test_valid_log_levels(self) -> None:
        """Test valid log levels."""
        for level in ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]:
            config = ServerConfig(log_level=level)
            assert config.log_level == level

    def test_log_level_case_insensitive(self) -> None:
        """Test log level is case insensitive."""
        config = ServerConfig(log_level="debug")
        assert config.log_level == "DEBUG"


class TestConfig:
    """Tests for main Config container."""

    def test_config_creates_all_sub_configs(self) -> None:
        """Test Config creates all sub-configurations."""
        config = Config()
        assert isinstance(config.kafka, KafkaConfig)
        assert isinstance(config.safety, SafetyConfig)
        assert isinstance(config.security, SecurityConfig)
        assert isinstance(config.server, ServerConfig)

    def test_config_repr(self) -> None:
        """Test Config repr."""
        config = Config()
        repr_str = repr(config)
        assert "Config(" in repr_str
        assert "kafka=" in repr_str
        assert "safety=" in repr_str
        assert "security=" in repr_str
        assert "server=" in repr_str
