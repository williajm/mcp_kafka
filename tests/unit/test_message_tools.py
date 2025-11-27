"""Tests for message operation tools."""

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from mcp_kafka.config import KafkaConfig, SafetyConfig
from mcp_kafka.fastmcp_tools.message import consume_messages, produce_message
from mcp_kafka.kafka_wrapper.client import KafkaClientWrapper
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.errors import (
    KafkaOperationError,
    SafetyError,
    TopicNotFound,
    ValidationError,
)


def make_poll_side_effect(messages: list[Any]) -> Any:
    """Create a side_effect that returns messages then None forever."""
    idx = 0

    def poll_func(*args: Any, **kwargs: Any) -> Any:
        nonlocal idx
        if idx < len(messages):
            result = messages[idx]
            idx += 1
            return result
        return None

    return poll_func


class TestConsumeMessages:
    """Tests for consume_messages function."""

    def test_consume_messages_success(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test consuming messages successfully."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Consumer") as mock_consumer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                # Create mock messages
                mock_msg_1 = MagicMock()
                mock_msg_1.error.return_value = None
                mock_msg_1.topic.return_value = "test-topic"
                mock_msg_1.partition.return_value = 0
                mock_msg_1.offset.return_value = 100
                mock_msg_1.timestamp.return_value = (1, 1234567890000)
                mock_msg_1.key.return_value = b"key1"
                mock_msg_1.value.return_value = b"value1"
                mock_msg_1.headers.return_value = [("header1", b"hval1")]

                mock_msg_2 = MagicMock()
                mock_msg_2.error.return_value = None
                mock_msg_2.topic.return_value = "test-topic"
                mock_msg_2.partition.return_value = 0
                mock_msg_2.offset.return_value = 101
                mock_msg_2.timestamp.return_value = (1, 1234567891000)
                mock_msg_2.key.return_value = None
                mock_msg_2.value.return_value = b"value2"
                mock_msg_2.headers.return_value = None

                mock_consumer = MagicMock()
                mock_consumer.get_watermark_offsets.return_value = (0, 110)
                mock_consumer.poll.side_effect = make_poll_side_effect([mock_msg_1, mock_msg_2])
                mock_consumer_cls.return_value = mock_consumer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                messages = consume_messages(client, enforcer, "test-topic", limit=5, timeout=0.1)

                assert len(messages) == 2
                assert messages[0].topic == "test-topic"
                assert messages[0].partition == 0
                assert messages[0].offset == 100
                assert messages[0].key == "key1"
                assert messages[0].value == "value1"
                assert messages[0].headers == {"header1": "hval1"}

                assert messages[1].offset == 101
                assert messages[1].key is None

    def test_consume_messages_specific_partition(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test consuming from a specific partition."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Consumer") as mock_consumer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition, 1: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                mock_msg = MagicMock()
                mock_msg.error.return_value = None
                mock_msg.topic.return_value = "test-topic"
                mock_msg.partition.return_value = 1
                mock_msg.offset.return_value = 50
                mock_msg.timestamp.return_value = (0, None)
                mock_msg.key.return_value = None
                mock_msg.value.return_value = b"data"
                mock_msg.headers.return_value = None

                mock_consumer = MagicMock()
                mock_consumer.get_watermark_offsets.return_value = (0, 100)
                mock_consumer.poll.side_effect = make_poll_side_effect([mock_msg])
                mock_consumer_cls.return_value = mock_consumer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                messages = consume_messages(
                    client, enforcer, "test-topic", partition=1, limit=5, timeout=0.1
                )

                assert len(messages) == 1
                assert messages[0].partition == 1

                # Verify assign was called with only partition 1
                mock_consumer.assign.assert_called_once()
                assigned = mock_consumer.assign.call_args[0][0]
                assert len(assigned) == 1
                assert assigned[0].partition == 1

    def test_consume_messages_with_offset(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test consuming from a specific offset."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Consumer") as mock_consumer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                mock_consumer = MagicMock()
                mock_consumer.poll.return_value = None
                mock_consumer_cls.return_value = mock_consumer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                consume_messages(client, enforcer, "test-topic", offset=500, limit=5)

                # Verify offset was set
                mock_consumer.assign.assert_called_once()
                assigned = mock_consumer.assign.call_args[0][0]
                assert assigned[0].offset == 500

    def test_consume_messages_topic_not_found(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test consume_messages raises TopicNotFound for missing topic."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(TopicNotFound, match="Topic 'nonexistent' not found"):
                consume_messages(client, enforcer, "nonexistent")

    def test_consume_messages_protected_topic(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test consume_messages raises SafetyError for protected topics."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            mock_admin_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(SafetyError, match="not allowed"):
                consume_messages(client, enforcer, "__consumer_offsets")

    def test_consume_messages_invalid_topic_name(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test consume_messages raises ValidationError for invalid topic name."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            mock_admin_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="Invalid topic name"):
                consume_messages(client, enforcer, "invalid topic!")

    def test_consume_messages_invalid_partition(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test consume_messages raises error for invalid partition."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Consumer"):
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition}  # Only partition 0 exists

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                with pytest.raises(KafkaOperationError, match="Partition 5 does not exist"):
                    consume_messages(client, enforcer, "test-topic", partition=5)

    def test_consume_messages_limit_exceeded(self, kafka_config: KafkaConfig) -> None:
        """Test consume_messages raises ValidationError for limit exceeding max."""
        safety_config = SafetyConfig(max_consume_messages=10)

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            mock_admin_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="exceeds maximum"):
                consume_messages(client, enforcer, "test-topic", limit=100)

    def test_consume_messages_limit_zero(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test consume_messages raises ValidationError for limit of 0."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            mock_admin_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="at least 1"):
                consume_messages(client, enforcer, "test-topic", limit=0)

    def test_consume_messages_handles_message_error(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test that message errors are logged but don't fail the operation."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Consumer") as mock_consumer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                # First message has an error
                mock_msg_error = MagicMock()
                mock_msg_error.error.return_value = MagicMock()

                # Second message is good
                mock_msg_good = MagicMock()
                mock_msg_good.error.return_value = None
                mock_msg_good.topic.return_value = "test-topic"
                mock_msg_good.partition.return_value = 0
                mock_msg_good.offset.return_value = 100
                mock_msg_good.timestamp.return_value = (0, None)
                mock_msg_good.key.return_value = None
                mock_msg_good.value.return_value = b"value"
                mock_msg_good.headers.return_value = None

                mock_consumer = MagicMock()
                mock_consumer.get_watermark_offsets.return_value = (0, 110)
                mock_consumer.poll.side_effect = make_poll_side_effect(
                    [mock_msg_error, mock_msg_good]
                )
                mock_consumer_cls.return_value = mock_consumer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                messages = consume_messages(client, enforcer, "test-topic", limit=5, timeout=0.1)

                # Should only get the good message
                assert len(messages) == 1
                assert messages[0].offset == 100

    def test_consume_messages_handles_decode_error(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test that non-UTF8 content is converted to string."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Consumer") as mock_consumer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                # Message with binary data that can't be decoded as UTF-8
                mock_msg = MagicMock()
                mock_msg.error.return_value = None
                mock_msg.topic.return_value = "test-topic"
                mock_msg.partition.return_value = 0
                mock_msg.offset.return_value = 100
                mock_msg.timestamp.return_value = (0, None)

                # Simulate bytes that fail to decode as UTF-8
                binary_key = b"\xff\xfe"
                binary_value = b"\x00\x01\x02"

                def key_decode_fail():
                    return binary_key

                def value_decode_fail():
                    return binary_value

                mock_msg.key.return_value = binary_key
                mock_msg.value.return_value = binary_value
                mock_msg.headers.return_value = [("bin-header", b"\xff\xfe")]

                mock_consumer = MagicMock()
                mock_consumer.get_watermark_offsets.return_value = (0, 110)
                mock_consumer.poll.side_effect = make_poll_side_effect([mock_msg])
                mock_consumer_cls.return_value = mock_consumer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                messages = consume_messages(client, enforcer, "test-topic", limit=5, timeout=0.1)

                # Should still get the message with str() fallback
                assert len(messages) == 1

    def test_consume_messages_consumer_closed(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test that consumer is properly closed after consuming."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Consumer") as mock_consumer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                mock_consumer = MagicMock()
                mock_consumer.get_watermark_offsets.return_value = (0, 100)
                mock_consumer.poll.return_value = None
                mock_consumer_cls.return_value = mock_consumer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                consume_messages(client, enforcer, "test-topic", limit=5)

                # Verify consumer was closed
                mock_consumer.close.assert_called_once()

    def test_consume_messages_empty_topic(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test consuming from an empty topic returns empty list."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Consumer") as mock_consumer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                mock_consumer = MagicMock()
                # Empty topic: low = high = 0
                mock_consumer.get_watermark_offsets.return_value = (0, 0)
                mock_consumer.poll.return_value = None
                mock_consumer_cls.return_value = mock_consumer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                messages = consume_messages(client, enforcer, "test-topic", limit=5)

                assert len(messages) == 0

    def test_consume_messages_timestamp_not_present(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test consuming message without timestamp."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Consumer") as mock_consumer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                mock_msg = MagicMock()
                mock_msg.error.return_value = None
                mock_msg.topic.return_value = "test-topic"
                mock_msg.partition.return_value = 0
                mock_msg.offset.return_value = 100
                mock_msg.timestamp.return_value = (0, -1)  # No timestamp
                mock_msg.key.return_value = None
                mock_msg.value.return_value = b"value"
                mock_msg.headers.return_value = None

                mock_consumer = MagicMock()
                mock_consumer.get_watermark_offsets.return_value = (0, 110)
                mock_consumer.poll.side_effect = make_poll_side_effect([mock_msg])
                mock_consumer_cls.return_value = mock_consumer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                messages = consume_messages(client, enforcer, "test-topic", limit=5, timeout=0.1)

                assert len(messages) == 1
                assert messages[0].timestamp is None


class TestProduceMessage:
    """Tests for produce_message function."""

    def test_produce_message_success(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test producing a message successfully."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Producer") as mock_producer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                # Mock producer with delivery callback simulation
                mock_producer = MagicMock()

                def produce_side_effect(**kwargs: Any) -> None:
                    # Simulate delivery callback
                    callback = kwargs.get("callback")
                    if callback:
                        mock_msg = MagicMock()
                        mock_msg.topic.return_value = "test-topic"
                        mock_msg.partition.return_value = 0
                        mock_msg.offset.return_value = 42
                        mock_msg.timestamp.return_value = (1, 1234567890000)
                        callback(None, mock_msg)

                mock_producer.produce.side_effect = produce_side_effect
                mock_producer_cls.return_value = mock_producer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                result = produce_message(client, enforcer, "test-topic", value="Hello, Kafka!")

                assert result.topic == "test-topic"
                assert result.partition == 0
                assert result.offset == 42
                mock_producer.flush.assert_called()

    def test_produce_message_with_key(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test producing a message with a key."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Producer") as mock_producer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                mock_producer = MagicMock()

                def produce_side_effect(**kwargs: Any) -> None:
                    callback = kwargs.get("callback")
                    if callback:
                        mock_msg = MagicMock()
                        mock_msg.topic.return_value = "test-topic"
                        mock_msg.partition.return_value = 0
                        mock_msg.offset.return_value = 100
                        mock_msg.timestamp.return_value = (1, 1234567890000)
                        callback(None, mock_msg)

                mock_producer.produce.side_effect = produce_side_effect
                mock_producer_cls.return_value = mock_producer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                result = produce_message(
                    client, enforcer, "test-topic", value="value", key="my-key"
                )

                assert result.topic == "test-topic"
                # Verify key was passed to produce
                call_kwargs = mock_producer.produce.call_args[1]
                assert call_kwargs["key"] == b"my-key"

    def test_produce_message_with_headers(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test producing a message with headers."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Producer") as mock_producer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                mock_producer = MagicMock()

                def produce_side_effect(**kwargs: Any) -> None:
                    callback = kwargs.get("callback")
                    if callback:
                        mock_msg = MagicMock()
                        mock_msg.topic.return_value = "test-topic"
                        mock_msg.partition.return_value = 0
                        mock_msg.offset.return_value = 100
                        mock_msg.timestamp.return_value = (1, 1234567890000)
                        callback(None, mock_msg)

                mock_producer.produce.side_effect = produce_side_effect
                mock_producer_cls.return_value = mock_producer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                headers = {"correlation-id": "abc123", "content-type": "application/json"}
                result = produce_message(
                    client, enforcer, "test-topic", value="value", headers=headers
                )

                assert result.topic == "test-topic"
                # Verify headers were passed to produce
                call_kwargs = mock_producer.produce.call_args[1]
                assert call_kwargs["headers"] == [
                    ("correlation-id", b"abc123"),
                    ("content-type", b"application/json"),
                ]

    def test_produce_message_specific_partition(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test producing to a specific partition."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Producer") as mock_producer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition, 1: mock_partition, 2: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                mock_producer = MagicMock()

                def produce_side_effect(**kwargs: Any) -> None:
                    callback = kwargs.get("callback")
                    if callback:
                        mock_msg = MagicMock()
                        mock_msg.topic.return_value = "test-topic"
                        mock_msg.partition.return_value = 2
                        mock_msg.offset.return_value = 50
                        mock_msg.timestamp.return_value = (1, 1234567890000)
                        callback(None, mock_msg)

                mock_producer.produce.side_effect = produce_side_effect
                mock_producer_cls.return_value = mock_producer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                result = produce_message(client, enforcer, "test-topic", value="value", partition=2)

                assert result.partition == 2
                # Verify partition was passed to produce
                call_kwargs = mock_producer.produce.call_args[1]
                assert call_kwargs["partition"] == 2

    def test_produce_message_topic_not_found(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test produce_message raises TopicNotFound for missing topic."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(TopicNotFound, match="Topic 'nonexistent' not found"):
                produce_message(client, enforcer, "nonexistent", value="test")

    def test_produce_message_protected_topic(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test produce_message raises SafetyError for protected topics."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            mock_admin_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(SafetyError, match="not allowed"):
                produce_message(client, enforcer, "__consumer_offsets", value="test")

    def test_produce_message_invalid_topic_name(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test produce_message raises ValidationError for invalid topic name."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            mock_admin_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="Invalid topic name"):
                produce_message(client, enforcer, "invalid topic!", value="test")

    def test_produce_message_invalid_partition(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test produce_message raises error for invalid partition."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            mock_partition = MagicMock()
            mock_partition.replicas = [1]

            mock_topic = MagicMock()
            mock_topic.partitions = {0: mock_partition}  # Only partition 0 exists

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {"test-topic": mock_topic}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(KafkaOperationError, match="Partition 5 does not exist"):
                produce_message(client, enforcer, "test-topic", value="test", partition=5)

    def test_produce_message_size_exceeds_limit(self, kafka_config: KafkaConfig) -> None:
        """Test produce_message raises ValidationError for message exceeding size limit."""
        safety_config = SafetyConfig(max_message_size=10)  # Very small limit

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            mock_admin_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="exceeds maximum"):
                produce_message(client, enforcer, "test-topic", value="A" * 100)

    def test_produce_message_delivery_failure(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test produce_message handles delivery failures."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Producer") as mock_producer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                mock_producer = MagicMock()

                def produce_side_effect(**kwargs: Any) -> None:
                    callback = kwargs.get("callback")
                    if callback:
                        # Simulate delivery error
                        callback("Delivery failed: broker unavailable", None)

                mock_producer.produce.side_effect = produce_side_effect
                mock_producer_cls.return_value = mock_producer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                with pytest.raises(KafkaOperationError, match="delivery failed"):
                    produce_message(client, enforcer, "test-topic", value="test")

    def test_produce_message_no_confirmation(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test produce_message handles missing delivery confirmation."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Producer") as mock_producer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                mock_producer = MagicMock()
                # Don't call the callback - simulates timeout/no confirmation
                mock_producer.produce.return_value = None
                mock_producer_cls.return_value = mock_producer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                with pytest.raises(KafkaOperationError, match="confirmation not received"):
                    produce_message(client, enforcer, "test-topic", value="test")

    def test_produce_message_kafka_exception(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test produce_message handles KafkaException during produce."""
        from confluent_kafka import KafkaException

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            with patch("mcp_kafka.fastmcp_tools.message.Producer") as mock_producer_cls:
                mock_partition = MagicMock()
                mock_partition.replicas = [1]

                mock_topic = MagicMock()
                mock_topic.partitions = {0: mock_partition}

                mock_metadata = MagicMock()
                mock_metadata.brokers = {1: MagicMock()}
                mock_metadata.topics = {"test-topic": mock_topic}

                mock_admin = MagicMock()
                mock_admin.list_topics.return_value = mock_metadata
                mock_admin_cls.return_value = mock_admin

                mock_producer = MagicMock()
                mock_producer.produce.side_effect = KafkaException("Buffer full")
                mock_producer_cls.return_value = mock_producer

                client = KafkaClientWrapper(kafka_config)
                enforcer = AccessEnforcer(safety_config)

                with pytest.raises(KafkaOperationError, match="Failed to produce message"):
                    produce_message(client, enforcer, "test-topic", value="test")

    def test_produce_message_size_with_headers_exceeds_limit(
        self, kafka_config: KafkaConfig
    ) -> None:
        """Test produce_message validates total message size including headers."""
        # Set a small max message size
        safety_config = SafetyConfig(max_message_size=50)

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_admin_cls:
            mock_admin_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            # Small value but large headers should exceed limit
            with pytest.raises(ValidationError, match="exceeds maximum"):
                produce_message(
                    client,
                    enforcer,
                    "test-topic",
                    value="small",
                    headers={"large-header": "A" * 100},
                )
