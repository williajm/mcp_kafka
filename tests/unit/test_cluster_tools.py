"""Tests for cluster information tools."""

from unittest.mock import MagicMock, patch

import pytest

from mcp_kafka.config import KafkaConfig, SafetyConfig
from mcp_kafka.fastmcp_tools.cluster import get_cluster_info, get_watermarks, list_brokers
from mcp_kafka.kafka_wrapper.client import KafkaClientWrapper
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.errors import (
    KafkaOperationError,
    SafetyError,
    TopicNotFound,
    ValidationError,
)


class TestGetClusterInfo:
    """Tests for get_cluster_info function."""

    def test_get_cluster_info_success(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test getting cluster info successfully."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_broker_1 = MagicMock()
            mock_broker_1.host = "broker1.example.com"
            mock_broker_1.port = 9092

            mock_broker_2 = MagicMock()
            mock_broker_2.host = "broker2.example.com"
            mock_broker_2.port = 9092

            mock_topic = MagicMock()
            mock_topic.partitions = {0: MagicMock()}

            mock_metadata = MagicMock()
            mock_metadata.cluster_id = "test-cluster-123"
            mock_metadata.brokers = {1: mock_broker_1, 2: mock_broker_2}
            mock_metadata.topics = {
                "topic-1": mock_topic,
                "topic-2": mock_topic,
                "__consumer_offsets": mock_topic,  # Internal, should not be counted
            }

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            info = get_cluster_info(client, enforcer)

            assert info.cluster_id == "test-cluster-123"
            assert info.broker_count == 2
            assert info.topic_count == 2  # Excludes internal topics
            assert info.controller_id == 1  # First broker

    def test_get_cluster_info_no_topics(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test getting cluster info with no topics."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_broker = MagicMock()
            mock_broker.host = "localhost"
            mock_broker.port = 9092

            mock_metadata = MagicMock()
            mock_metadata.cluster_id = "test-cluster"
            mock_metadata.brokers = {1: mock_broker}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            info = get_cluster_info(client, enforcer)

            assert info.topic_count == 0
            assert info.broker_count == 1

    def test_get_cluster_info_kafka_error(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test get_cluster_info handles Kafka errors."""
        from confluent_kafka import KafkaException

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_admin = MagicMock()
            mock_admin.list_topics.side_effect = KafkaException("Connection failed")
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            client._admin_client = mock_admin
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(KafkaOperationError, match="Failed to get cluster info"):
                get_cluster_info(client, enforcer)


class TestListBrokers:
    """Tests for list_brokers function."""

    def test_list_brokers_success(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test listing brokers successfully."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_broker_1 = MagicMock()
            mock_broker_1.host = "broker1.example.com"
            mock_broker_1.port = 9092

            mock_broker_2 = MagicMock()
            mock_broker_2.host = "broker2.example.com"
            mock_broker_2.port = 9093

            mock_broker_3 = MagicMock()
            mock_broker_3.host = "broker3.example.com"
            mock_broker_3.port = 9094

            mock_metadata = MagicMock()
            mock_metadata.cluster_id = "test-cluster"
            mock_metadata.brokers = {1: mock_broker_1, 2: mock_broker_2, 3: mock_broker_3}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            brokers = list_brokers(client, enforcer)

            assert len(brokers) == 3
            assert brokers[0].id == 1
            assert brokers[0].host == "broker1.example.com"
            assert brokers[0].port == 9092
            assert brokers[1].id == 2
            assert brokers[2].id == 3

    def test_list_brokers_single(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test listing a single broker."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_broker = MagicMock()
            mock_broker.host = "localhost"
            mock_broker.port = 9092

            mock_metadata = MagicMock()
            mock_metadata.cluster_id = "test-cluster"
            mock_metadata.brokers = {1: mock_broker}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            brokers = list_brokers(client, enforcer)

            assert len(brokers) == 1
            assert brokers[0].id == 1

    def test_list_brokers_kafka_error(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test list_brokers handles Kafka errors."""
        from confluent_kafka import KafkaException

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_admin = MagicMock()
            mock_admin.list_topics.side_effect = KafkaException("Connection failed")
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            client._admin_client = mock_admin
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(KafkaOperationError, match="Failed to list brokers"):
                list_brokers(client, enforcer)


class TestGetWatermarks:
    """Tests for get_watermarks function."""

    def test_get_watermarks_success(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test getting watermarks successfully."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_partition = MagicMock()
            mock_partition.replicas = [1]

            mock_topic = MagicMock()
            mock_topic.partitions = {0: mock_partition, 1: mock_partition, 2: mock_partition}

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {"test-topic": mock_topic}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_cls.return_value = mock_admin

            # Mock consumer for watermarks
            mock_consumer = MagicMock()
            mock_consumer.get_watermark_offsets.side_effect = [
                (0, 100),  # partition 0
                (10, 200),  # partition 1
                (50, 150),  # partition 2
            ]

            client = KafkaClientWrapper(kafka_config)
            # Mock temporary_consumer context manager
            client.temporary_consumer = MagicMock()
            client.temporary_consumer.return_value.__enter__ = MagicMock(return_value=mock_consumer)
            client.temporary_consumer.return_value.__exit__ = MagicMock(return_value=False)
            enforcer = AccessEnforcer(safety_config)

            watermarks = get_watermarks(client, enforcer, "test-topic")

            assert len(watermarks) == 3
            assert watermarks[0].partition == 0
            assert watermarks[0].low_watermark == 0
            assert watermarks[0].high_watermark == 100
            assert watermarks[0].message_count == 100

            assert watermarks[1].partition == 1
            assert watermarks[1].low_watermark == 10
            assert watermarks[1].high_watermark == 200
            assert watermarks[1].message_count == 190

            assert watermarks[2].partition == 2
            assert watermarks[2].message_count == 100

    def test_get_watermarks_topic_not_found(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test get_watermarks raises TopicNotFound for missing topic."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(TopicNotFound, match="Topic 'nonexistent' not found"):
                get_watermarks(client, enforcer, "nonexistent")

    def test_get_watermarks_protected_topic(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test get_watermarks raises SafetyError for protected topics."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(SafetyError, match="not allowed"):
                get_watermarks(client, enforcer, "__consumer_offsets")

    def test_get_watermarks_invalid_topic_name(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test get_watermarks raises ValidationError for invalid topic name."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="Invalid topic name"):
                get_watermarks(client, enforcer, "invalid topic!")

    def test_get_watermarks_empty_topic_name(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test get_watermarks raises ValidationError for empty topic name."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="cannot be empty"):
                get_watermarks(client, enforcer, "")

    def test_get_watermarks_kafka_error(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test get_watermarks handles Kafka errors."""
        from confluent_kafka import KafkaException

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_admin = MagicMock()
            mock_admin.list_topics.side_effect = KafkaException("Connection failed")
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            client._admin_client = mock_admin
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(KafkaOperationError, match="Failed to get watermarks"):
                get_watermarks(client, enforcer, "test-topic")

    def test_get_watermarks_uses_temporary_consumer(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test that temporary_consumer context manager is used for watermarks."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_partition = MagicMock()
            mock_partition.replicas = [1]

            mock_topic = MagicMock()
            mock_topic.partitions = {0: mock_partition}

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {"test-topic": mock_topic}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_cls.return_value = mock_admin

            mock_consumer = MagicMock()
            mock_consumer.get_watermark_offsets.return_value = (0, 100)

            client = KafkaClientWrapper(kafka_config)
            # Mock temporary_consumer context manager
            client.temporary_consumer = MagicMock()
            client.temporary_consumer.return_value.__enter__ = MagicMock(return_value=mock_consumer)
            mock_exit = MagicMock(return_value=False)
            client.temporary_consumer.return_value.__exit__ = mock_exit
            enforcer = AccessEnforcer(safety_config)

            get_watermarks(client, enforcer, "test-topic")

            # Verify temporary_consumer was called with correct suffix
            client.temporary_consumer.assert_called_once()
            call_args = client.temporary_consumer.call_args[0][0]
            assert "watermark-check-test-topic" in call_args
            # Verify context manager __exit__ was called (ensures cleanup)
            mock_exit.assert_called_once()
