"""Kafka client wrapper with connection management and health checks."""

import hashlib
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer
from confluent_kafka.admin import AdminClient
from loguru import logger

from mcp_kafka.config import KafkaConfig
from mcp_kafka.utils.errors import KafkaConnectionError, KafkaHealthCheckError

# Consumer group ID prefix for temporary consumers
TEMP_CONSUMER_PREFIX = "mcp-kafka"

# Kafka's maximum length for consumer group IDs
MAX_GROUP_ID_LENGTH = 249


def _build_safe_group_id(prefix: str, suffix: str) -> str:
    """Build a consumer group ID that stays within Kafka's 249 character limit.

    If the combined prefix-suffix would exceed 249 characters, the suffix is
    truncated and a short hash is appended for uniqueness.

    Args:
        prefix: The group ID prefix (e.g., "mcp-kafka")
        suffix: The suffix to append (e.g., "watermark-check-my-topic")

    Returns:
        A group ID guaranteed to be <= 249 characters
    """
    full_id = f"{prefix}-{suffix}"

    if len(full_id) <= MAX_GROUP_ID_LENGTH:
        return full_id

    # Need to truncate - append 8 hex chars from SHA-256 hash for uniqueness
    hash_suffix = hashlib.sha256(suffix.encode()).hexdigest()[:8]

    # Calculate available space for truncated suffix
    # Overhead is: prefix length + separator dash + dash before hash + 8-char hash
    overhead = len(prefix) + 1 + 1 + 8
    max_suffix_len = MAX_GROUP_ID_LENGTH - overhead

    truncated_suffix = suffix[:max_suffix_len]
    return f"{prefix}-{truncated_suffix}-{hash_suffix}"


class KafkaClientWrapper:
    """Thread-safe Kafka client wrapper with health checks and connection management."""

    def __init__(self, config: KafkaConfig) -> None:
        """Initialize Kafka client wrapper.

        Args:
            config: Kafka configuration settings

        """
        self.config = config
        self._admin_client: AdminClient | None = None
        logger.debug(f"Initialized KafkaClientWrapper for {config.bootstrap_servers}")

    @property
    def admin(self) -> AdminClient:
        """Get AdminClient with lazy initialization.

        Returns:
            Initialized AdminClient

        Raises:
            KafkaConnectionError: If unable to connect to Kafka cluster

        """
        if self._admin_client is None:
            self._connect()
        if self._admin_client is None:
            raise KafkaConnectionError("Failed to initialize Kafka AdminClient")
        return self._admin_client

    def _build_client_config(self) -> dict[str, Any]:
        """Build confluent-kafka configuration dict.

        Returns:
            Configuration dictionary for confluent-kafka clients

        """
        config: dict[str, Any] = {
            "bootstrap.servers": self.config.bootstrap_servers,
            "client.id": self.config.client_id,
        }

        # Security protocol
        config["security.protocol"] = self.config.security_protocol

        # SASL Authentication
        if self.config.sasl_mechanism:
            config["sasl.mechanism"] = self.config.sasl_mechanism

            if self.config.sasl_mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]:
                config["sasl.username"] = self.config.sasl_username
                if self.config.sasl_password:
                    config["sasl.password"] = self.config.sasl_password.get_secret_value()
            elif self.config.sasl_mechanism == "GSSAPI":
                config["sasl.kerberos.service.name"] = self.config.sasl_kerberos_service_name
                if self.config.sasl_kerberos_keytab:
                    config["sasl.kerberos.keytab"] = str(self.config.sasl_kerberos_keytab)
                if self.config.sasl_kerberos_principal:
                    config["sasl.kerberos.principal"] = self.config.sasl_kerberos_principal

        # SSL/TLS
        if self.config.ssl_ca_location:
            config["ssl.ca.location"] = str(self.config.ssl_ca_location)
        if self.config.ssl_certificate_location:
            config["ssl.certificate.location"] = str(self.config.ssl_certificate_location)
        if self.config.ssl_key_location:
            config["ssl.key.location"] = str(self.config.ssl_key_location)
        if self.config.ssl_key_password:
            config["ssl.key.password"] = self.config.ssl_key_password.get_secret_value()

        return config

    def _connect(self) -> None:
        """Establish connection to Kafka cluster.

        Raises:
            KafkaConnectionError: If connection fails

        """
        try:
            logger.info(f"Connecting to Kafka cluster at {self.config.bootstrap_servers}")

            kafka_config = self._build_client_config()
            self._admin_client = AdminClient(kafka_config)

            # Health check - list topics to verify connection
            metadata = self._admin_client.list_topics(timeout=self.config.timeout)
            broker_count = len(metadata.brokers)
            topic_count = len(metadata.topics)

            logger.success(
                f"Connected to Kafka cluster: {broker_count} brokers, {topic_count} topics"
            )

        except KafkaException as e:
            logger.error(f"Failed to connect to Kafka cluster: {e}")
            raise KafkaConnectionError(f"Cannot connect to Kafka cluster: {e}") from e
        except Exception as e:
            logger.error(f"Unexpected error connecting to Kafka cluster: {e}")
            raise KafkaConnectionError(f"Unexpected error: {e}") from e

    def health_check(self) -> dict[str, Any]:
        """Perform comprehensive health check of Kafka cluster.

        Returns:
            Health status dictionary with cluster info

        Raises:
            KafkaHealthCheckError: If health check fails

        """
        try:
            metadata = self.admin.list_topics(timeout=self.config.timeout)

            brokers = {
                broker_id: {"host": broker.host, "port": broker.port}
                for broker_id, broker in metadata.brokers.items()
            }

            health_status = {
                "status": "healthy",
                "cluster_id": metadata.cluster_id,
                "brokers": brokers,
                "broker_count": len(metadata.brokers),
                "topic_count": len(metadata.topics),
            }

            logger.debug("Kafka health check passed")
            return health_status

        except KafkaException as e:
            logger.error(f"Kafka health check failed: {e}")
            raise KafkaHealthCheckError(f"Health check failed: {e}") from e

    def close(self) -> None:
        """Close the Kafka client connections."""
        if self._admin_client is not None:
            # Note: AdminClient doesn't have an explicit close method
            # We release the reference to allow garbage collection
            self._admin_client = None
            logger.debug("Kafka client reference released")

    def create_consumer_config(
        self,
        group_id: str,
        enable_auto_commit: bool = False,
        auto_offset_reset: str = "latest",
    ) -> dict[str, Any]:
        """Create consumer configuration with the wrapper's connection settings.

        Args:
            group_id: Consumer group ID
            enable_auto_commit: Whether to enable auto commit (default: False)
            auto_offset_reset: Offset reset policy (default: "latest")

        Returns:
            Configuration dictionary for confluent-kafka Consumer

        """
        config = self._build_client_config()
        config["group.id"] = group_id
        config["enable.auto.commit"] = enable_auto_commit
        config["auto.offset.reset"] = auto_offset_reset
        return config

    def create_producer_config(self) -> dict[str, Any]:
        """Create producer configuration with the wrapper's connection settings.

        Returns:
            Configuration dictionary for confluent-kafka Producer

        """
        return self._build_client_config()

    @contextmanager
    def temporary_consumer(
        self,
        group_id_suffix: str,
        enable_auto_commit: bool = False,
        auto_offset_reset: str = "latest",
    ) -> Generator[Consumer, None, None]:
        """Context manager for creating a temporary consumer.

        Creates a consumer with the wrapper's connection settings and ensures
        proper cleanup on exit.

        Args:
            group_id_suffix: Suffix for the consumer group ID (prefixed with TEMP_CONSUMER_PREFIX)
            enable_auto_commit: Whether to enable auto commit (default: False)
            auto_offset_reset: Offset reset policy (default: "latest")

        Yields:
            Consumer instance

        Example:
            with wrapper.temporary_consumer("lag-check") as consumer:
                low, high = consumer.get_watermark_offsets(tp)

        """
        group_id = _build_safe_group_id(TEMP_CONSUMER_PREFIX, group_id_suffix)
        config = self.create_consumer_config(
            group_id=group_id,
            enable_auto_commit=enable_auto_commit,
            auto_offset_reset=auto_offset_reset,
        )
        consumer = Consumer(config)
        try:
            yield consumer
        finally:
            consumer.close()

    @contextmanager
    def temporary_producer(self) -> Generator[Producer, None, None]:
        """Context manager for creating a temporary producer.

        Creates a producer with the wrapper's connection settings and ensures
        proper flush on exit.

        Yields:
            Producer instance

        Example:
            with wrapper.temporary_producer() as producer:
                producer.produce(topic, value=b"message")

        """
        config = self.create_producer_config()
        producer = Producer(config)
        try:
            yield producer
        finally:
            producer.flush(timeout=self.config.timeout)

    def _reset_connection(self) -> None:
        """Reset the cached AdminClient to force reconnection on next use."""
        if self._admin_client is not None:
            logger.warning("Resetting Kafka connection due to error")
            self._admin_client = None

    def _is_connection_error(self, error: Exception) -> bool:
        """Check if the error indicates a connection/transport failure.

        Args:
            error: The exception to check

        Returns:
            True if this is a connection-related error that warrants reconnection
        """
        error_str = str(error).lower()
        connection_indicators = [
            "_transport",
            "_timed_out",
            "_resolve",
            "broker transport failure",
            "host resolution failure",
            "all brokers down",
            "connection refused",
            "kafkaconnectionerror",
        ]
        return any(indicator in error_str for indicator in connection_indicators)

    def handle_connection_error(self, error: Exception) -> None:
        """Handle an error by resetting the connection if it's connection-related.

        This method should be called by tool implementations when they catch
        exceptions. If the error indicates a connection/transport failure,
        the cached AdminClient is reset so the next operation will reconnect.

        Args:
            error: The exception that was caught
        """
        if self._is_connection_error(error):
            self._reset_connection()

    @contextmanager
    def acquire(self) -> Generator[AdminClient, None, None]:
        """Context manager for AdminClient access.

        Automatically resets the connection on transport/connection errors
        so subsequent calls can reconnect with fresh metadata.

        Yields:
            AdminClient instance

        Example:
            with wrapper.acquire() as admin:
                metadata = admin.list_topics()

        """
        try:
            yield self.admin
        except KafkaException as e:
            logger.error(f"Kafka operation failed: {e}")
            if self._is_connection_error(e):
                self._reset_connection()
            raise
        except Exception as e:
            logger.error(f"Unexpected error in Kafka operation: {e}")
            raise

    def __enter__(self) -> "KafkaClientWrapper":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager and close client."""
        self.close()

    def __repr__(self) -> str:
        """Return string representation."""
        status = "connected" if self._admin_client is not None else "disconnected"
        servers = self.config.bootstrap_servers
        return f"KafkaClientWrapper(bootstrap_servers={servers}, status={status})"
