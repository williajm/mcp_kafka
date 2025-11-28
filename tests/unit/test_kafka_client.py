"""Tests for Kafka client wrapper."""

from unittest.mock import MagicMock, patch

import pytest

from mcp_kafka.config import KafkaConfig
from mcp_kafka.kafka_wrapper.client import (
    MAX_GROUP_ID_LENGTH,
    TEMP_CONSUMER_PREFIX,
    KafkaClientWrapper,
    _build_safe_group_id,
)
from mcp_kafka.utils.errors import KafkaConnectionError, KafkaHealthCheckError


class TestBuildSafeGroupId:
    """Tests for _build_safe_group_id helper function."""

    def test_short_suffix_no_truncation(self) -> None:
        """Test that short suffixes are not truncated."""
        result = _build_safe_group_id("mcp-kafka", "watermark-check-my-topic")
        assert result == "mcp-kafka-watermark-check-my-topic"
        assert len(result) <= MAX_GROUP_ID_LENGTH

    def test_long_suffix_gets_truncated(self) -> None:
        """Test that long suffixes are truncated with hash appended."""
        # Create a suffix that would exceed 249 chars with prefix
        long_suffix = "a" * 250
        result = _build_safe_group_id("mcp-kafka", long_suffix)

        assert len(result) <= MAX_GROUP_ID_LENGTH
        assert result.startswith("mcp-kafka-")
        # Should end with 8-char hash
        assert len(result.split("-")[-1]) == 8

    def test_exactly_at_limit_no_truncation(self) -> None:
        """Test suffix that results in exactly 249 chars is not truncated."""
        prefix = "mcp-kafka"
        # Need suffix length = 249 - len(prefix) - 1 (dash) = 239
        suffix = "x" * 239
        result = _build_safe_group_id(prefix, suffix)

        assert len(result) == MAX_GROUP_ID_LENGTH
        # Exact match proves no truncation/hashing occurred
        assert result == f"{prefix}-{suffix}"

    def test_one_over_limit_gets_truncated(self) -> None:
        """Test suffix that results in 250 chars gets truncated."""
        prefix = "mcp-kafka"
        # Need suffix length = 240 to get 250 total
        suffix = "y" * 240
        result = _build_safe_group_id(prefix, suffix)

        assert len(result) <= MAX_GROUP_ID_LENGTH
        # Should have hash appended (format: prefix-truncated-hash8)
        parts = result.split("-")
        assert len(parts[-1]) == 8  # Hash suffix

    def test_hash_provides_uniqueness(self) -> None:
        """Test that different long suffixes produce different results."""
        long_suffix1 = "topic-" + "a" * 250
        long_suffix2 = "topic-" + "b" * 250
        result1 = _build_safe_group_id("mcp-kafka", long_suffix1)
        result2 = _build_safe_group_id("mcp-kafka", long_suffix2)

        # Both should be truncated to same length
        assert len(result1) == len(result2) <= MAX_GROUP_ID_LENGTH
        # But should be different due to hash
        assert result1 != result2

    def test_same_suffix_produces_same_hash(self) -> None:
        """Test that the same suffix always produces the same result."""
        long_suffix = "deterministic-" + "z" * 250
        result1 = _build_safe_group_id("mcp-kafka", long_suffix)
        result2 = _build_safe_group_id("mcp-kafka", long_suffix)

        assert result1 == result2

    def test_realistic_watermark_check_long_topic(self) -> None:
        """Test realistic watermark check with max-length topic name."""
        # Max topic name is 249 chars
        topic_name = "t" * 249
        suffix = f"watermark-check-{topic_name}"
        result = _build_safe_group_id(TEMP_CONSUMER_PREFIX, suffix)

        assert len(result) <= MAX_GROUP_ID_LENGTH
        assert result.startswith(f"{TEMP_CONSUMER_PREFIX}-watermark-check-")

    def test_realistic_lag_check_long_group(self) -> None:
        """Test realistic lag check with max-length consumer group ID."""
        # Max group ID is 249 chars
        group_id = "g" * 249
        suffix = f"lag-check-{group_id}"
        result = _build_safe_group_id(TEMP_CONSUMER_PREFIX, suffix)

        assert len(result) <= MAX_GROUP_ID_LENGTH
        assert result.startswith(f"{TEMP_CONSUMER_PREFIX}-lag-check-")


class TestKafkaClientWrapper:
    """Tests for KafkaClientWrapper."""

    def test_init(self, kafka_config: KafkaConfig) -> None:
        """Test wrapper initialization."""
        wrapper = KafkaClientWrapper(kafka_config)
        assert wrapper.config == kafka_config
        assert wrapper._admin_client is None

    def test_repr_disconnected(self, kafka_config: KafkaConfig) -> None:
        """Test repr when disconnected."""
        wrapper = KafkaClientWrapper(kafka_config)
        repr_str = repr(wrapper)
        assert "localhost:9092" in repr_str
        assert "disconnected" in repr_str

    def test_build_client_config_plaintext(self, kafka_config: KafkaConfig) -> None:
        """Test building client config for plaintext."""
        wrapper = KafkaClientWrapper(kafka_config)
        config = wrapper._build_client_config()
        assert config["bootstrap.servers"] == "localhost:9092"
        assert config["client.id"] == "test-client"
        assert config["security.protocol"] == "PLAINTEXT"

    def test_build_client_config_sasl(self, kafka_config_sasl: KafkaConfig) -> None:
        """Test building client config for SASL."""
        wrapper = KafkaClientWrapper(kafka_config_sasl)
        config = wrapper._build_client_config()
        assert config["security.protocol"] == "SASL_PLAINTEXT"
        assert config["sasl.mechanism"] == "PLAIN"
        assert config["sasl.username"] == "test-user"
        assert config["sasl.password"] == "test-password"

    def test_connect_success(self, kafka_config: KafkaConfig, mock_admin_client: MagicMock) -> None:
        """Test successful connection."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = mock_admin_client
            wrapper = KafkaClientWrapper(kafka_config)
            wrapper._connect()
            assert wrapper._admin_client is not None
            mock_cls.assert_called_once()

    def test_connect_failure(self, kafka_config: KafkaConfig) -> None:
        """Test connection failure."""
        from confluent_kafka import KafkaException

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.side_effect = KafkaException("Connection failed")
            wrapper = KafkaClientWrapper(kafka_config)
            with pytest.raises(KafkaConnectionError, match="Cannot connect"):
                wrapper._connect()

    def test_admin_property_lazy_init(
        self, kafka_config: KafkaConfig, mock_admin_client: MagicMock
    ) -> None:
        """Test admin property lazily initializes client."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = mock_admin_client
            wrapper = KafkaClientWrapper(kafka_config)
            assert wrapper._admin_client is None
            admin = wrapper.admin
            assert admin is not None
            assert wrapper._admin_client is not None

    def test_health_check_success(
        self, kafka_config: KafkaConfig, mock_admin_client: MagicMock
    ) -> None:
        """Test successful health check."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = mock_admin_client
            wrapper = KafkaClientWrapper(kafka_config)
            health = wrapper.health_check()
            assert health["status"] == "healthy"
            assert health["cluster_id"] == "test-cluster-id"
            assert health["broker_count"] == 1
            assert health["topic_count"] == 1

    def test_health_check_failure(
        self, kafka_config: KafkaConfig, mock_admin_client: MagicMock
    ) -> None:
        """Test health check failure."""
        from confluent_kafka import KafkaException

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_admin_client.list_topics.side_effect = KafkaException("Failed")
            mock_cls.return_value = mock_admin_client
            wrapper = KafkaClientWrapper(kafka_config)
            wrapper._admin_client = mock_admin_client  # Skip connect
            with pytest.raises(KafkaHealthCheckError, match="Health check failed"):
                wrapper.health_check()

    def test_close(self, kafka_config: KafkaConfig, mock_admin_client: MagicMock) -> None:
        """Test close method."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = mock_admin_client
            wrapper = KafkaClientWrapper(kafka_config)
            wrapper._connect()
            assert wrapper._admin_client is not None
            wrapper.close()
            assert wrapper._admin_client is None

    def test_context_manager(self, kafka_config: KafkaConfig, mock_admin_client: MagicMock) -> None:
        """Test context manager usage."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = mock_admin_client
            with KafkaClientWrapper(kafka_config) as wrapper:
                wrapper._connect()
                assert wrapper._admin_client is not None
            # After context exit, client should be closed
            assert wrapper._admin_client is None

    def test_acquire_context_manager(
        self, kafka_config: KafkaConfig, mock_admin_client: MagicMock
    ) -> None:
        """Test acquire context manager."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = mock_admin_client
            wrapper = KafkaClientWrapper(kafka_config)
            with wrapper.acquire() as admin:
                assert admin is not None
                assert admin == mock_admin_client

    def test_acquire_kafka_exception(
        self, kafka_config: KafkaConfig, mock_admin_client: MagicMock
    ) -> None:
        """Test acquire context manager with KafkaException."""
        from confluent_kafka import KafkaException

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = mock_admin_client
            wrapper = KafkaClientWrapper(kafka_config)
            wrapper._admin_client = mock_admin_client
            with pytest.raises(KafkaException):
                with wrapper.acquire():
                    raise KafkaException("Operation failed")

    def test_acquire_generic_exception(
        self, kafka_config: KafkaConfig, mock_admin_client: MagicMock
    ) -> None:
        """Test acquire context manager with generic exception."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = mock_admin_client
            wrapper = KafkaClientWrapper(kafka_config)
            wrapper._admin_client = mock_admin_client
            with pytest.raises(ValueError):
                with wrapper.acquire():
                    raise ValueError("Generic error")

    def test_connect_generic_exception(self, kafka_config: KafkaConfig) -> None:
        """Test connection with generic exception."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.side_effect = RuntimeError("Unexpected error")
            wrapper = KafkaClientWrapper(kafka_config)
            with pytest.raises(KafkaConnectionError, match="Unexpected error"):
                wrapper._connect()

    def test_repr_connected(self, kafka_config: KafkaConfig, mock_admin_client: MagicMock) -> None:
        """Test repr when connected."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = mock_admin_client
            wrapper = KafkaClientWrapper(kafka_config)
            wrapper._connect()
            repr_str = repr(wrapper)
            assert "localhost:9092" in repr_str
            assert "connected" in repr_str
            assert "disconnected" not in repr_str

    def test_build_client_config_gssapi(self) -> None:
        """Test building client config for GSSAPI/Kerberos."""
        from pathlib import Path

        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            client_id="test-client",
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="GSSAPI",
            sasl_kerberos_service_name="kafka",
            sasl_kerberos_principal="kafka/broker@REALM",
            sasl_kerberos_keytab=Path("/etc/keytabs/kafka.keytab"),
        )
        wrapper = KafkaClientWrapper(config)
        client_config = wrapper._build_client_config()
        assert client_config["sasl.mechanism"] == "GSSAPI"
        assert client_config["sasl.kerberos.service.name"] == "kafka"
        assert client_config["sasl.kerberos.principal"] == "kafka/broker@REALM"
        assert client_config["sasl.kerberos.keytab"] == "/etc/keytabs/kafka.keytab"

    def test_build_client_config_ssl(self) -> None:
        """Test building client config with SSL."""
        from pathlib import Path

        config = KafkaConfig(
            bootstrap_servers="localhost:9092",
            client_id="test-client",
            security_protocol="SSL",
            ssl_ca_location=Path("/certs/ca.pem"),
            ssl_certificate_location=Path("/certs/client.pem"),
            ssl_key_location=Path("/certs/client.key"),
            ssl_key_password="secret",
        )
        wrapper = KafkaClientWrapper(config)
        client_config = wrapper._build_client_config()
        assert client_config["security.protocol"] == "SSL"
        assert client_config["ssl.ca.location"] == "/certs/ca.pem"
        assert client_config["ssl.certificate.location"] == "/certs/client.pem"
        assert client_config["ssl.key.location"] == "/certs/client.key"
        assert client_config["ssl.key.password"] == "secret"

    def test_admin_property_raises_when_connect_fails(self, kafka_config: KafkaConfig) -> None:
        """Test admin property raises when connection fails and client is None."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.side_effect = KafkaConnectionError("Failed")
            wrapper = KafkaClientWrapper(kafka_config)
            with pytest.raises(KafkaConnectionError):
                _ = wrapper.admin

    def test_is_connection_error_detects_transport_failure(self, kafka_config: KafkaConfig) -> None:
        """Test _is_connection_error detects transport failures."""
        wrapper = KafkaClientWrapper(kafka_config)

        # Should detect connection errors
        assert wrapper._is_connection_error(Exception("_TRANSPORT failure"))
        assert wrapper._is_connection_error(Exception("Broker transport failure"))
        assert wrapper._is_connection_error(Exception("_TIMED_OUT"))
        assert wrapper._is_connection_error(Exception("Host resolution failure"))
        assert wrapper._is_connection_error(Exception("All brokers down"))
        assert wrapper._is_connection_error(Exception("Connection refused"))

        # Should NOT detect non-connection errors
        assert not wrapper._is_connection_error(Exception("Topic not found"))
        assert not wrapper._is_connection_error(Exception("Invalid partition"))
        assert not wrapper._is_connection_error(ValueError("Bad value"))

    def test_handle_connection_error_resets_on_transport_failure(
        self, kafka_config: KafkaConfig, mock_admin_client: MagicMock
    ) -> None:
        """Test handle_connection_error resets connection on transport failures."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = mock_admin_client
            wrapper = KafkaClientWrapper(kafka_config)
            wrapper._connect()

            # Verify connected
            assert wrapper._admin_client is not None

            # Handle a transport error - should reset
            wrapper.handle_connection_error(Exception("Broker transport failure"))
            assert wrapper._admin_client is None

    def test_handle_connection_error_ignores_non_connection_errors(
        self, kafka_config: KafkaConfig, mock_admin_client: MagicMock
    ) -> None:
        """Test handle_connection_error ignores non-connection errors."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = mock_admin_client
            wrapper = KafkaClientWrapper(kafka_config)
            wrapper._connect()

            # Verify connected
            assert wrapper._admin_client is not None

            # Handle a non-connection error - should NOT reset
            wrapper.handle_connection_error(Exception("Topic not found"))
            assert wrapper._admin_client is not None

    def test_acquire_resets_connection_on_transport_failure(
        self, kafka_config: KafkaConfig, mock_admin_client: MagicMock
    ) -> None:
        """Test acquire context manager resets connection on transport failures."""
        from confluent_kafka import KafkaException

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = mock_admin_client
            wrapper = KafkaClientWrapper(kafka_config)
            wrapper._admin_client = mock_admin_client

            # Simulate a transport error inside acquire
            with pytest.raises(KafkaException):
                with wrapper.acquire():
                    raise KafkaException("_TRANSPORT: Broker transport failure")

            # Connection should be reset for auto-recovery
            assert wrapper._admin_client is None
