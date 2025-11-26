"""Kafka client wrapper with connection management and health checks."""

from collections.abc import Generator
from contextlib import contextmanager
from typing import Any

from confluent_kafka import KafkaException
from confluent_kafka.admin import AdminClient
from loguru import logger

from mcp_kafka.config import KafkaConfig
from mcp_kafka.utils.errors import KafkaConnectionError, KafkaHealthCheckError


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

    @contextmanager
    def acquire(self) -> Generator[AdminClient, None, None]:
        """Context manager for AdminClient access.

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
