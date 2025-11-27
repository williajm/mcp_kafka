"""Pytest fixtures for MCP Kafka tests."""

from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest

from mcp_kafka.config import Config, KafkaConfig, SafetyConfig, SecurityConfig, ServerConfig
from mcp_kafka.kafka_wrapper import KafkaClientWrapper
from mcp_kafka.safety.core import AccessEnforcer


@pytest.fixture
def kafka_config() -> KafkaConfig:
    """Create test Kafka configuration."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        client_id="test-client",
        timeout=10,
        security_protocol="PLAINTEXT",
    )


@pytest.fixture
def kafka_config_sasl() -> KafkaConfig:
    """Create test Kafka configuration with SASL."""
    return KafkaConfig(
        bootstrap_servers="localhost:9092",
        client_id="test-client",
        timeout=10,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="PLAIN",
        sasl_username="test-user",
        sasl_password="test-password",
    )


@pytest.fixture
def safety_config() -> SafetyConfig:
    """Create test safety configuration."""
    return SafetyConfig(
        allow_write_operations=False,
        max_consume_messages=100,
    )


@pytest.fixture
def safety_config_write() -> SafetyConfig:
    """Create test safety configuration with write enabled."""
    return SafetyConfig(
        allow_write_operations=True,
        max_consume_messages=100,
    )


@pytest.fixture
def security_config() -> SecurityConfig:
    """Create test security configuration."""
    return SecurityConfig(
        rate_limit_enabled=True,
        rate_limit_rpm=60,
        audit_log_enabled=False,
        oauth_enabled=False,
    )


@pytest.fixture
def server_config() -> ServerConfig:
    """Create test server configuration."""
    return ServerConfig(
        server_name="test-mcp-kafka",
        log_level="DEBUG",
        json_logging=False,
        debug_mode=True,
    )


@pytest.fixture
def config(
    kafka_config: KafkaConfig,
    safety_config: SafetyConfig,
    security_config: SecurityConfig,
    server_config: ServerConfig,
) -> Config:
    """Create test configuration."""
    cfg = Config.__new__(Config)
    cfg.kafka = kafka_config
    cfg.safety = safety_config
    cfg.security = security_config
    cfg.server = server_config
    return cfg


@pytest.fixture
def mock_admin_client() -> MagicMock:
    """Create mock Kafka AdminClient."""
    mock = MagicMock()

    # Mock metadata
    mock_broker = MagicMock()
    mock_broker.host = "localhost"
    mock_broker.port = 9092

    mock_metadata = MagicMock()
    mock_metadata.cluster_id = "test-cluster-id"
    mock_metadata.brokers = {1: mock_broker}
    mock_metadata.topics = {"test-topic": MagicMock()}

    mock.list_topics.return_value = mock_metadata

    return mock


@pytest.fixture
def kafka_client_wrapper(
    kafka_config: KafkaConfig, mock_admin_client: MagicMock
) -> Generator[KafkaClientWrapper, None, None]:
    """Create Kafka client wrapper with mocked client."""
    with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
        mock_cls.return_value = mock_admin_client

        wrapper = KafkaClientWrapper(kafka_config)
        yield wrapper
        wrapper.close()


@pytest.fixture
def access_enforcer(safety_config: SafetyConfig) -> AccessEnforcer:
    """Create access enforcer with default safety configuration."""
    return AccessEnforcer(safety_config)


@pytest.fixture
def access_enforcer_write(safety_config_write: SafetyConfig) -> AccessEnforcer:
    """Create access enforcer with write operations enabled."""
    return AccessEnforcer(safety_config_write)
