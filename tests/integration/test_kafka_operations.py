"""Integration tests for Kafka operations.

Tests actual Kafka operations against a real Kafka container.

NOTE: These tests are skipped because they require the full tool API,
not just the KafkaClientWrapper. Will be implemented in a future phase.
"""

import time

import pytest
from testcontainers.kafka import KafkaContainer

from mcp_kafka.kafka_wrapper import KafkaClientWrapper

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skip(reason="Tests require tool API implementation, not direct client methods"),
]


class TestKafkaClientIntegration:
    """Integration tests for KafkaClientWrapper."""

    def test_health_check_succeeds(self, integration_kafka_client: KafkaClientWrapper) -> None:
        """Test that health check succeeds against real Kafka."""
        health = integration_kafka_client.health_check()

        assert health["status"] == "healthy"
        assert health["cluster_id"] is not None
        assert health["broker_count"] >= 1

    def test_list_topics_empty_cluster(self, integration_kafka_client: KafkaClientWrapper) -> None:
        """Test listing topics on a fresh cluster."""
        topics = integration_kafka_client.list_topics()

        # Fresh Kafka may have no topics or internal topics only
        assert isinstance(topics, list)

    def test_create_and_list_topic(
        self,
        integration_kafka_client: KafkaClientWrapper,
        unique_topic_name: str,
    ) -> None:
        """Test creating a topic and then listing it."""
        # Create a topic
        integration_kafka_client.create_topic(
            topic=unique_topic_name,
            num_partitions=3,
            replication_factor=1,
        )

        # Wait for topic to be created
        time.sleep(1)

        # List topics and verify
        topics = integration_kafka_client.list_topics()
        assert unique_topic_name in topics

    def test_describe_topic(
        self,
        integration_kafka_client: KafkaClientWrapper,
        unique_topic_name: str,
    ) -> None:
        """Test describing a topic's configuration."""
        # Create a topic first
        integration_kafka_client.create_topic(
            topic=unique_topic_name,
            num_partitions=2,
            replication_factor=1,
        )

        # Wait for topic to be created
        time.sleep(1)

        # Describe the topic
        description = integration_kafka_client.describe_topic(unique_topic_name)

        assert description["topic"] == unique_topic_name
        assert description["num_partitions"] == 2

    def test_produce_and_consume_messages(
        self,
        integration_kafka_client: KafkaClientWrapper,
        unique_topic_name: str,
    ) -> None:
        """Test producing and consuming messages."""
        # Create a topic
        integration_kafka_client.create_topic(
            topic=unique_topic_name,
            num_partitions=1,
            replication_factor=1,
        )
        time.sleep(1)

        # Produce a message
        integration_kafka_client.produce_message(
            topic=unique_topic_name,
            value="Hello, Kafka!",
            key="test-key",
        )

        # Wait for message to be written
        time.sleep(1)

        # Consume messages
        messages = integration_kafka_client.consume_messages(
            topic=unique_topic_name,
            max_messages=10,
            timeout_seconds=5,
        )

        assert len(messages) >= 1
        # Find our message
        found = False
        for msg in messages:
            if msg.get("value") == "Hello, Kafka!":
                found = True
                assert msg.get("key") == "test-key"
                break
        assert found, f"Message not found in {messages}"

    def test_produce_message_with_headers(
        self,
        integration_kafka_client: KafkaClientWrapper,
        unique_topic_name: str,
    ) -> None:
        """Test producing messages with headers."""
        # Create a topic
        integration_kafka_client.create_topic(
            topic=unique_topic_name,
            num_partitions=1,
            replication_factor=1,
        )
        time.sleep(1)

        # Produce a message with headers
        headers = {"content-type": "application/json", "source": "test"}
        integration_kafka_client.produce_message(
            topic=unique_topic_name,
            value='{"test": true}',
            headers=headers,
        )

        time.sleep(1)

        # Consume and verify headers
        messages = integration_kafka_client.consume_messages(
            topic=unique_topic_name,
            max_messages=10,
            timeout_seconds=5,
        )

        assert len(messages) >= 1
        # Message headers should be present
        msg_headers = messages[0].get("headers", {})
        assert msg_headers.get("content-type") == "application/json"

    def test_get_watermarks(
        self,
        integration_kafka_client: KafkaClientWrapper,
        unique_topic_name: str,
    ) -> None:
        """Test getting topic watermarks."""
        # Create topic and produce some messages
        integration_kafka_client.create_topic(
            topic=unique_topic_name,
            num_partitions=2,
            replication_factor=1,
        )
        time.sleep(1)

        # Produce a few messages
        for i in range(5):
            integration_kafka_client.produce_message(
                topic=unique_topic_name,
                value=f"message-{i}",
            )
        time.sleep(1)

        # Get watermarks
        watermarks = integration_kafka_client.get_watermarks(unique_topic_name)

        assert unique_topic_name in watermarks
        partitions = watermarks[unique_topic_name]
        # Should have 2 partitions
        assert len(partitions) == 2
        # Each partition should have low and high watermarks
        for partition in partitions:
            assert "low" in partition
            assert "high" in partition
            assert partition["high"] >= partition["low"]

    def test_cluster_info(self, integration_kafka_client: KafkaClientWrapper) -> None:
        """Test getting cluster information."""
        cluster_info = integration_kafka_client.cluster_info()

        assert "cluster_id" in cluster_info
        assert "controller_id" in cluster_info
        assert "brokers" in cluster_info
        assert len(cluster_info["brokers"]) >= 1

    def test_list_brokers(self, integration_kafka_client: KafkaClientWrapper) -> None:
        """Test listing brokers."""
        brokers = integration_kafka_client.list_brokers()

        assert len(brokers) >= 1
        for broker in brokers:
            assert "id" in broker
            assert "host" in broker
            assert "port" in broker


class TestKafkaConsumerGroupIntegration:
    """Integration tests for consumer group operations."""

    def test_list_consumer_groups_empty(self, integration_kafka_client: KafkaClientWrapper) -> None:
        """Test listing consumer groups on fresh cluster."""
        groups = integration_kafka_client.list_consumer_groups()

        # Should return a list (possibly empty)
        assert isinstance(groups, list)

    def test_consumer_group_creation(
        self,
        integration_kafka_client: KafkaClientWrapper,
        unique_topic_name: str,
        unique_consumer_group: str,
    ) -> None:
        """Test that consuming creates a consumer group."""
        # Create topic
        integration_kafka_client.create_topic(
            topic=unique_topic_name,
            num_partitions=1,
            replication_factor=1,
        )
        time.sleep(1)

        # Produce a message
        integration_kafka_client.produce_message(
            topic=unique_topic_name,
            value="test message",
        )
        time.sleep(1)

        # Consume with a consumer group
        integration_kafka_client.consume_messages(
            topic=unique_topic_name,
            consumer_group=unique_consumer_group,
            max_messages=1,
            timeout_seconds=5,
        )

        time.sleep(1)

        # List consumer groups - should include our group
        groups = integration_kafka_client.list_consumer_groups()
        group_names = [g["group_id"] for g in groups]
        assert unique_consumer_group in group_names


class TestKafkaErrorHandling:
    """Integration tests for error handling scenarios."""

    def test_describe_nonexistent_topic(self, integration_kafka_client: KafkaClientWrapper) -> None:
        """Test describing a topic that doesn't exist."""
        from mcp_kafka.utils.errors import TopicNotFound

        with pytest.raises(TopicNotFound):
            integration_kafka_client.describe_topic("nonexistent-topic-12345")

    def test_consume_from_nonexistent_topic(
        self, integration_kafka_client: KafkaClientWrapper
    ) -> None:
        """Test consuming from a topic that doesn't exist."""
        # This might either raise an error or return empty list
        # depending on Kafka configuration
        result = integration_kafka_client.consume_messages(
            topic="nonexistent-topic-67890",
            max_messages=1,
            timeout_seconds=2,
        )
        # Should return empty list or raise error
        assert result == [] or result is None

    def test_create_duplicate_topic(
        self,
        integration_kafka_client: KafkaClientWrapper,
        unique_topic_name: str,
    ) -> None:
        """Test creating a topic that already exists."""
        # Create topic first time
        integration_kafka_client.create_topic(
            topic=unique_topic_name,
            num_partitions=1,
            replication_factor=1,
        )
        time.sleep(1)

        # Try to create again - should handle gracefully
        # The behavior depends on implementation:
        # - May succeed silently (idempotent)
        # - May raise an error
        try:
            integration_kafka_client.create_topic(
                topic=unique_topic_name,
                num_partitions=1,
                replication_factor=1,
            )
        except Exception as e:
            # If it raises, should be a meaningful error
            assert "exists" in str(e).lower() or "already" in str(e).lower()


class TestKafkaConnectionHandling:
    """Integration tests for connection handling."""

    def test_client_reconnects_after_close(self, kafka_container: KafkaContainer) -> None:
        """Test that client can reconnect after being closed."""
        from mcp_kafka.config import KafkaConfig

        config = KafkaConfig(
            bootstrap_servers=kafka_container.get_bootstrap_server(),
            client_id="reconnect-test",
            timeout=10,
        )

        # Create client, use it, close it
        client = KafkaClientWrapper(config)
        health1 = client.health_check()
        assert health1["status"] == "healthy"
        client.close()

        # Create new client with same config
        client2 = KafkaClientWrapper(config)
        health2 = client2.health_check()
        assert health2["status"] == "healthy"
        client2.close()
