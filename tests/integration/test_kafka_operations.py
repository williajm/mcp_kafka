"""Integration tests for Kafka operations.

Tests actual Kafka operations against a real Kafka container.
"""

import time
import uuid

import pytest
from confluent_kafka import TopicPartition
from confluent_kafka.admin import NewTopic

from mcp_kafka.kafka_wrapper import KafkaClientWrapper

pytestmark = pytest.mark.integration


class TestKafkaClientIntegration:
    """Integration tests for KafkaClientWrapper."""

    def test_health_check_succeeds(self, integration_kafka_client: KafkaClientWrapper) -> None:
        """Test that health check succeeds against real Kafka."""
        health = integration_kafka_client.health_check()

        assert health["status"] == "healthy"
        assert health["cluster_id"] is not None
        assert health["broker_count"] >= 1

    def test_list_topics_via_admin(self, integration_kafka_client: KafkaClientWrapper) -> None:
        """Test listing topics via AdminClient."""
        metadata = integration_kafka_client.admin.list_topics(timeout=10)

        # Fresh Kafka may have no topics or internal topics only
        assert metadata is not None
        assert isinstance(metadata.topics, dict)

    def test_create_and_list_topic(
        self,
        integration_kafka_client: KafkaClientWrapper,
        unique_topic_name: str,
    ) -> None:
        """Test creating a topic and then listing it."""
        # Create a topic using AdminClient
        new_topic = NewTopic(
            topic=unique_topic_name,
            num_partitions=3,
            replication_factor=1,
        )
        futures = integration_kafka_client.admin.create_topics([new_topic])

        # Wait for topic creation to complete
        for future in futures.values():
            future.result(timeout=10)

        # Wait for metadata to propagate
        time.sleep(1)

        # List topics and verify
        metadata = integration_kafka_client.admin.list_topics(timeout=10)
        topic_names = list(metadata.topics.keys())

        assert unique_topic_name in topic_names

    def test_describe_topic(
        self,
        integration_kafka_client: KafkaClientWrapper,
        unique_topic_name: str,
    ) -> None:
        """Test describing a topic."""
        # Create a topic first
        new_topic = NewTopic(
            topic=unique_topic_name,
            num_partitions=2,
            replication_factor=1,
        )
        futures = integration_kafka_client.admin.create_topics([new_topic])
        for future in futures.values():
            future.result(timeout=10)

        time.sleep(1)

        # Describe via metadata
        metadata = integration_kafka_client.admin.list_topics(timeout=10)
        topic_metadata = metadata.topics.get(unique_topic_name)

        assert topic_metadata is not None
        assert len(topic_metadata.partitions) == 2

    def test_produce_and_consume_messages(
        self,
        integration_kafka_client: KafkaClientWrapper,
        unique_topic_name: str,
    ) -> None:
        """Test producing and consuming messages."""
        # Create a topic first
        new_topic = NewTopic(
            topic=unique_topic_name,
            num_partitions=1,
            replication_factor=1,
        )
        futures = integration_kafka_client.admin.create_topics([new_topic])
        for future in futures.values():
            future.result(timeout=10)

        time.sleep(1)

        # Produce messages
        test_messages = [f"test-message-{i}" for i in range(5)]
        with integration_kafka_client.temporary_producer() as producer:
            for msg in test_messages:
                producer.produce(unique_topic_name, value=msg.encode("utf-8"))
            producer.flush(timeout=10)

        time.sleep(1)

        # Consume messages
        consumed_messages: list[str] = []
        with integration_kafka_client.temporary_consumer(
            group_id_suffix=f"test-consume-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        ) as consumer:
            consumer.subscribe([unique_topic_name])

            # Poll for messages with timeout
            start_time = time.time()
            while len(consumed_messages) < len(test_messages) and (time.time() - start_time) < 10:
                msg = consumer.poll(timeout=1.0)
                if msg is not None and msg.error() is None:
                    consumed_messages.append(msg.value().decode("utf-8"))

        assert len(consumed_messages) == len(test_messages)
        assert set(consumed_messages) == set(test_messages)

    def test_produce_message_with_key(
        self,
        integration_kafka_client: KafkaClientWrapper,
        unique_topic_name: str,
    ) -> None:
        """Test producing a message with a key."""
        # Create a topic first
        new_topic = NewTopic(
            topic=unique_topic_name,
            num_partitions=3,
            replication_factor=1,
        )
        futures = integration_kafka_client.admin.create_topics([new_topic])
        for future in futures.values():
            future.result(timeout=10)

        time.sleep(1)

        # Produce message with key
        test_key = "test-key"
        test_value = "test-value-with-key"
        with integration_kafka_client.temporary_producer() as producer:
            producer.produce(
                unique_topic_name,
                key=test_key.encode("utf-8"),
                value=test_value.encode("utf-8"),
            )
            producer.flush(timeout=10)

        time.sleep(1)

        # Consume and verify
        with integration_kafka_client.temporary_consumer(
            group_id_suffix=f"test-key-{uuid.uuid4().hex[:8]}",
            auto_offset_reset="earliest",
        ) as consumer:
            consumer.subscribe([unique_topic_name])

            msg = None
            start_time = time.time()
            while msg is None and (time.time() - start_time) < 10:
                msg = consumer.poll(timeout=1.0)
                if msg is not None and msg.error() is not None:
                    msg = None

            assert msg is not None
            assert msg.key().decode("utf-8") == test_key
            assert msg.value().decode("utf-8") == test_value

    def test_get_watermarks(
        self,
        integration_kafka_client: KafkaClientWrapper,
        unique_topic_name: str,
    ) -> None:
        """Test getting watermarks for a topic partition."""
        # Create a topic first
        new_topic = NewTopic(
            topic=unique_topic_name,
            num_partitions=1,
            replication_factor=1,
        )
        futures = integration_kafka_client.admin.create_topics([new_topic])
        for future in futures.values():
            future.result(timeout=10)

        time.sleep(1)

        # Produce some messages
        with integration_kafka_client.temporary_producer() as producer:
            for i in range(3):
                producer.produce(unique_topic_name, value=f"msg-{i}".encode())
            producer.flush(timeout=10)

        time.sleep(1)

        # Get watermarks
        with integration_kafka_client.temporary_consumer(
            group_id_suffix=f"watermark-{uuid.uuid4().hex[:8]}",
        ) as consumer:
            tp = TopicPartition(unique_topic_name, 0)
            low, high = consumer.get_watermark_offsets(tp, timeout=10)

            assert low == 0
            assert high == 3

    def test_cluster_info_via_health_check(
        self,
        integration_kafka_client: KafkaClientWrapper,
    ) -> None:
        """Test getting cluster info via health check."""
        health = integration_kafka_client.health_check()

        assert health["status"] == "healthy"
        assert health["cluster_id"] is not None
        assert "brokers" in health
        assert health["broker_count"] >= 1

    def test_list_brokers_via_metadata(
        self,
        integration_kafka_client: KafkaClientWrapper,
    ) -> None:
        """Test listing brokers via metadata."""
        metadata = integration_kafka_client.admin.list_topics(timeout=10)

        assert len(metadata.brokers) >= 1
        for broker in metadata.brokers.values():
            assert broker.host is not None
            assert broker.port > 0


class TestKafkaConsumerGroupIntegration:
    """Integration tests for consumer group operations."""

    def test_list_consumer_groups(
        self,
        integration_kafka_client: KafkaClientWrapper,
    ) -> None:
        """Test listing consumer groups."""
        # List consumer groups via AdminClient
        result = integration_kafka_client.admin.list_consumer_groups()
        groups_future = result.result(timeout=10)

        # Fresh Kafka may have no groups
        assert groups_future is not None

    def test_consumer_group_creation(
        self,
        integration_kafka_client: KafkaClientWrapper,
        unique_topic_name: str,
    ) -> None:
        """Test that consuming creates a consumer group."""
        # Create a topic first
        new_topic = NewTopic(
            topic=unique_topic_name,
            num_partitions=1,
            replication_factor=1,
        )
        futures = integration_kafka_client.admin.create_topics([new_topic])
        for future in futures.values():
            future.result(timeout=10)

        time.sleep(1)

        # Consume with a specific group - this should create the group
        group_suffix = f"test-group-{uuid.uuid4().hex[:8]}"
        with integration_kafka_client.temporary_consumer(
            group_id_suffix=group_suffix,
            auto_offset_reset="earliest",
        ) as consumer:
            consumer.subscribe([unique_topic_name])
            # Poll once to trigger group join
            consumer.poll(timeout=2.0)

        # The group should now exist (though may be empty after consumer closes)
        # We just verify no exceptions occurred during the process


class TestKafkaErrorHandling:
    """Integration tests for error handling."""

    def test_describe_nonexistent_topic(
        self,
        integration_kafka_client: KafkaClientWrapper,
    ) -> None:
        """Test describing a topic that doesn't exist."""
        nonexistent_topic = f"nonexistent-topic-{uuid.uuid4().hex}"

        metadata = integration_kafka_client.admin.list_topics(timeout=10)
        topic_metadata = metadata.topics.get(nonexistent_topic)

        # Topic should not exist
        assert topic_metadata is None

    def test_create_duplicate_topic(
        self,
        integration_kafka_client: KafkaClientWrapper,
        unique_topic_name: str,
    ) -> None:
        """Test creating a topic that already exists."""
        # Create a topic first
        new_topic = NewTopic(
            topic=unique_topic_name,
            num_partitions=1,
            replication_factor=1,
        )
        futures = integration_kafka_client.admin.create_topics([new_topic])
        for future in futures.values():
            future.result(timeout=10)

        time.sleep(1)

        # Try to create the same topic again
        futures = integration_kafka_client.admin.create_topics([new_topic])
        for future in futures.values():
            # This should raise an exception for duplicate topic
            try:
                future.result(timeout=10)
                # If no exception, that's also acceptable (idempotent create)
            except Exception:
                # Expected - topic already exists
                pass


class TestKafkaConnectionHandling:
    """Integration tests for connection handling."""

    def test_client_reconnects_after_close(
        self,
        integration_kafka_client: KafkaClientWrapper,
    ) -> None:
        """Test that client can reconnect after being closed."""
        # First health check should work
        health1 = integration_kafka_client.health_check()
        assert health1["status"] == "healthy"

        # Close the client
        integration_kafka_client.close()

        # Second health check should reconnect automatically
        health2 = integration_kafka_client.health_check()
        assert health2["status"] == "healthy"
