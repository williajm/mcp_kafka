"""Tests for topic management tools."""

from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import pytest

from mcp_kafka.config import KafkaConfig, SafetyConfig
from mcp_kafka.fastmcp_tools.topic import create_topic, describe_topic, list_topics
from mcp_kafka.kafka_wrapper.client import KafkaClientWrapper
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.errors import KafkaOperationError, SafetyError, TopicNotFound, ValidationError


class TestListTopics:
    """Tests for list_topics function."""

    def test_list_topics_success(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test listing topics successfully."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            # Create mock partition
            mock_partition = MagicMock()
            mock_partition.replicas = [1, 2, 3]

            # Create mock topic metadata
            mock_topic = MagicMock()
            mock_topic.partitions = {0: mock_partition, 1: mock_partition}

            # Create mock metadata
            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {
                "test-topic-1": mock_topic,
                "test-topic-2": mock_topic,
            }

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            topics = list_topics(client, enforcer)

            assert len(topics) == 2
            assert topics[0].name == "test-topic-1"
            assert topics[0].partition_count == 2
            assert topics[0].replication_factor == 3
            assert topics[0].is_internal is False

    def test_list_topics_excludes_internal_by_default(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test that internal topics are excluded by default."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_partition = MagicMock()
            mock_partition.replicas = [1]

            mock_topic = MagicMock()
            mock_topic.partitions = {0: mock_partition}

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {
                "test-topic": mock_topic,
                "__consumer_offsets": mock_topic,
            }

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            topics = list_topics(client, enforcer, include_internal=False)

            assert len(topics) == 1
            assert topics[0].name == "test-topic"

    def test_list_topics_includes_internal_when_requested(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test that internal topics are included when requested."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_partition = MagicMock()
            mock_partition.replicas = [1]

            mock_topic = MagicMock()
            mock_topic.partitions = {0: mock_partition}

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {
                "test-topic": mock_topic,
                "__internal_test": mock_topic,  # Internal but not blocklisted
            }

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            topics = list_topics(client, enforcer, include_internal=True)

            assert len(topics) == 2
            topic_names = [t.name for t in topics]
            assert "__internal_test" in topic_names

    def test_list_topics_blocklist_always_enforced(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test that blocklisted topics are never returned, even with include_internal=True."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_partition = MagicMock()
            mock_partition.replicas = [1]

            mock_topic = MagicMock()
            mock_topic.partitions = {0: mock_partition}

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {
                "test-topic": mock_topic,
                "__consumer_offsets": mock_topic,  # In default blocklist
                "__transaction_state": mock_topic,  # In default blocklist
                "__internal_test": mock_topic,  # Internal but not blocklisted
            }

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            # Even with include_internal=True, blocklisted topics should be excluded
            topics = list_topics(client, enforcer, include_internal=True)

            topic_names = [t.name for t in topics]
            assert "test-topic" in topic_names
            assert "__internal_test" in topic_names
            # Blocklisted topics should NOT be included
            assert "__consumer_offsets" not in topic_names
            assert "__transaction_state" not in topic_names
            assert len(topics) == 2

    def test_list_topics_empty(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test listing topics when no topics exist."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            topics = list_topics(client, enforcer)

            assert len(topics) == 0

    def test_list_topics_kafka_error(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test list_topics handles Kafka errors."""
        from confluent_kafka import KafkaException

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_admin = MagicMock()
            mock_admin.list_topics.side_effect = KafkaException("Connection failed")
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            client._admin_client = mock_admin
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(KafkaOperationError, match="Failed to list topics"):
                list_topics(client, enforcer)


class TestDescribeTopic:
    """Tests for describe_topic function."""

    def test_describe_topic_success(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test describing a topic successfully."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            # Create mock partitions
            mock_partition_0 = MagicMock()
            mock_partition_0.leader = 1
            mock_partition_0.replicas = [1, 2]
            mock_partition_0.isrs = [1, 2]

            mock_partition_1 = MagicMock()
            mock_partition_1.leader = 2
            mock_partition_1.replicas = [2, 1]
            mock_partition_1.isrs = [2, 1]

            mock_topic = MagicMock()
            mock_topic.partitions = {0: mock_partition_0, 1: mock_partition_1}

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {"test-topic": mock_topic}

            # Mock config
            mock_config_entry = MagicMock()
            mock_config_entry.value = "delete"

            mock_config_future = Future()
            mock_config_future.set_result({"cleanup.policy": mock_config_entry})

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.describe_configs.return_value = {MagicMock(): mock_config_future}
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            result = describe_topic(client, enforcer, "test-topic")

            assert result.name == "test-topic"
            assert result.partition_count == 2
            assert result.replication_factor == 2
            assert len(result.partitions) == 2
            assert result.partitions[0].partition == 0
            assert result.partitions[0].leader == 1

    def test_describe_topic_not_found(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test describe_topic raises TopicNotFound for missing topic."""
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
                describe_topic(client, enforcer, "nonexistent")

    def test_describe_topic_protected(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test describe_topic raises SafetyError for protected topics."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(SafetyError, match="not allowed"):
                describe_topic(client, enforcer, "__consumer_offsets")

    def test_describe_topic_invalid_name(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test describe_topic raises ValidationError for invalid topic name."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="Invalid topic name"):
                describe_topic(client, enforcer, "invalid topic!")

    def test_describe_topic_empty_name(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test describe_topic raises ValidationError for empty topic name."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="cannot be empty"):
                describe_topic(client, enforcer, "")

    def test_describe_topic_kafka_error(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test describe_topic handles Kafka errors."""
        from confluent_kafka import KafkaException

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_admin = MagicMock()
            mock_admin.list_topics.side_effect = KafkaException("Connection failed")
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            client._admin_client = mock_admin
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(KafkaOperationError, match="Failed to describe topic"):
                describe_topic(client, enforcer, "test-topic")

    def test_describe_topic_config_failure_returns_empty(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test that config fetch failure returns empty config dict."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_partition = MagicMock()
            mock_partition.leader = 1
            mock_partition.replicas = [1]
            mock_partition.isrs = [1]

            mock_topic = MagicMock()
            mock_topic.partitions = {0: mock_partition}

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {"test-topic": mock_topic}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.describe_configs.side_effect = Exception("Config fetch failed")
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            result = describe_topic(client, enforcer, "test-topic")

            assert result.name == "test-topic"
            assert result.config == {}  # Empty config due to failure


class TestCreateTopic:
    """Tests for create_topic function."""

    def test_create_topic_success(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test creating a topic successfully."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            # Mock list_topics to show topic doesn't exist
            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            # Mock create_topics
            mock_create_future = Future()
            mock_create_future.set_result(None)

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.create_topics.return_value = {"new-topic": mock_create_future}
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            result = create_topic(client, enforcer, "new-topic", partitions=3, replication_factor=2)

            assert result.topic == "new-topic"
            assert result.partitions == 3
            assert result.replication_factor == 2
            mock_admin.create_topics.assert_called_once()

    def test_create_topic_with_config(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test creating a topic with custom configuration."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_create_future = Future()
            mock_create_future.set_result(None)

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.create_topics.return_value = {"config-topic": mock_create_future}
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            topic_config = {"retention.ms": "604800000", "cleanup.policy": "compact"}
            result = create_topic(
                client,
                enforcer,
                "config-topic",
                partitions=1,
                replication_factor=1,
                config=topic_config,
            )

            assert result.topic == "config-topic"
            assert result.config == topic_config

    def test_create_topic_already_exists(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test create_topic raises error when topic already exists."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_partition = MagicMock()
            mock_partition.replicas = [1]

            mock_topic = MagicMock()
            mock_topic.partitions = {0: mock_partition}

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {"existing-topic": mock_topic}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(KafkaOperationError, match="already exists"):
                create_topic(client, enforcer, "existing-topic")

    def test_create_topic_invalid_name(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test create_topic raises ValidationError for invalid topic name."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="Invalid topic name"):
                create_topic(client, enforcer, "invalid topic!")

    def test_create_topic_protected_name(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test create_topic raises SafetyError for protected topic names."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(SafetyError, match="not allowed"):
                create_topic(client, enforcer, "__internal-topic")

    def test_create_topic_invalid_partitions_zero(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test create_topic raises ValidationError for zero partitions."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="at least 1"):
                create_topic(client, enforcer, "test-topic", partitions=0)

    def test_create_topic_invalid_partitions_negative(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test create_topic raises ValidationError for negative partitions."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="at least 1"):
                create_topic(client, enforcer, "test-topic", partitions=-1)

    def test_create_topic_invalid_partitions_exceeds_max(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test create_topic raises ValidationError when partitions exceed max."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="cannot exceed"):
                create_topic(client, enforcer, "test-topic", partitions=10001)

    def test_create_topic_invalid_replication_factor_zero(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test create_topic raises ValidationError for zero replication factor."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="Replication factor must be at least 1"):
                create_topic(client, enforcer, "test-topic", replication_factor=0)

    def test_create_topic_invalid_replication_factor_exceeds_max(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test create_topic raises ValidationError when replication factor exceeds max."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="cannot exceed"):
                create_topic(client, enforcer, "test-topic", replication_factor=16)

    def test_create_topic_kafka_error(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test create_topic handles Kafka errors."""
        from confluent_kafka import KafkaException

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_create_future = Future()
            mock_create_future.set_exception(KafkaException("Replication factor too high"))

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.create_topics.return_value = {"test-topic": mock_create_future}
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(KafkaOperationError, match="Failed to create topic"):
                create_topic(client, enforcer, "test-topic", replication_factor=5)
