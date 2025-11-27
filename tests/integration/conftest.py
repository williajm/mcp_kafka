"""Pytest fixtures for MCP Kafka integration tests.

Uses testcontainers to spin up a real Kafka instance for testing.
"""

import os
from collections.abc import Generator

import pytest
from testcontainers.kafka import KafkaContainer

from mcp_kafka.config import KafkaConfig, SafetyConfig, SecurityConfig
from mcp_kafka.kafka_wrapper import KafkaClientWrapper
from mcp_kafka.middleware import AuditMiddleware, RateLimitMiddleware, SafetyMiddleware
from mcp_kafka.safety.core import AccessEnforcer

# Skip integration tests if SKIP_INTEGRATION_TESTS is set
# This allows CI to skip these tests if Kafka is not available
pytestmark = pytest.mark.integration


@pytest.fixture(scope="session")
def kafka_container() -> Generator[KafkaContainer, None, None]:
    """Start a Kafka container for integration tests.

    This fixture has session scope to reuse the same container
    across all integration tests, improving test performance.
    """
    # Skip if explicitly disabled
    if os.environ.get("SKIP_INTEGRATION_TESTS", "").lower() == "true":
        pytest.skip("Integration tests disabled via SKIP_INTEGRATION_TESTS")

    container = KafkaContainer()
    try:
        container.start()
        yield container
    finally:
        container.stop()


@pytest.fixture
def kafka_bootstrap_servers(kafka_container: KafkaContainer) -> str:
    """Get the bootstrap servers from the running Kafka container."""
    return kafka_container.get_bootstrap_server()


@pytest.fixture
def integration_kafka_config(kafka_bootstrap_servers: str) -> KafkaConfig:
    """Create Kafka configuration for integration tests."""
    return KafkaConfig(
        bootstrap_servers=kafka_bootstrap_servers,
        client_id="integration-test-client",
        timeout=30,
        security_protocol="PLAINTEXT",
    )


@pytest.fixture
def integration_safety_config() -> SafetyConfig:
    """Create safety configuration for integration tests."""
    return SafetyConfig(
        allow_write_operations=True,
        max_consume_messages=100,
        max_message_size=1048576,
    )


@pytest.fixture
def integration_safety_config_readonly() -> SafetyConfig:
    """Create read-only safety configuration for integration tests."""
    return SafetyConfig(
        allow_write_operations=False,
        max_consume_messages=100,
    )


@pytest.fixture
def integration_security_config(tmp_path: str) -> SecurityConfig:
    """Create security configuration for integration tests."""
    from pathlib import Path

    return SecurityConfig(
        rate_limit_enabled=True,
        rate_limit_rpm=100,
        audit_log_enabled=True,
        audit_log_file=Path(tmp_path) / "test_audit.log",
        oauth_enabled=False,
    )


@pytest.fixture
def integration_security_config_no_audit() -> SecurityConfig:
    """Create security configuration without audit logging."""
    return SecurityConfig(
        rate_limit_enabled=True,
        rate_limit_rpm=60,
        audit_log_enabled=False,
        oauth_enabled=False,
    )


@pytest.fixture
def integration_kafka_client(
    integration_kafka_config: KafkaConfig,
) -> Generator[KafkaClientWrapper, None, None]:
    """Create a real Kafka client for integration tests."""
    client = KafkaClientWrapper(integration_kafka_config)
    yield client
    client.close()


@pytest.fixture
def integration_access_enforcer(
    integration_safety_config: SafetyConfig,
) -> AccessEnforcer:
    """Create access enforcer for integration tests with write access."""
    return AccessEnforcer(integration_safety_config)


@pytest.fixture
def integration_access_enforcer_readonly(
    integration_safety_config_readonly: SafetyConfig,
) -> AccessEnforcer:
    """Create read-only access enforcer for integration tests."""
    return AccessEnforcer(integration_safety_config_readonly)


@pytest.fixture
def integration_safety_middleware(
    integration_safety_config: SafetyConfig,
) -> SafetyMiddleware:
    """Create safety middleware for integration tests."""
    return SafetyMiddleware(integration_safety_config)


@pytest.fixture
def integration_rate_limit_middleware(
    integration_security_config: SecurityConfig,
) -> RateLimitMiddleware:
    """Create rate limit middleware for integration tests."""
    return RateLimitMiddleware(integration_security_config)


@pytest.fixture
def integration_audit_middleware(
    integration_security_config: SecurityConfig,
) -> Generator[AuditMiddleware, None, None]:
    """Create audit middleware for integration tests."""
    middleware = AuditMiddleware(integration_security_config)
    yield middleware
    middleware.close()


@pytest.fixture
def unique_topic_name() -> str:
    """Generate a unique topic name for each test."""
    import uuid

    return f"test-topic-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def unique_consumer_group() -> str:
    """Generate a unique consumer group name for each test."""
    import uuid

    return f"test-group-{uuid.uuid4().hex[:8]}"
