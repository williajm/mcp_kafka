"""Cluster information tools for MCP Kafka."""

from confluent_kafka import KafkaException, TopicPartition
from loguru import logger

from mcp_kafka.fastmcp_tools.common import BrokerInfo, ClusterInfo, WatermarkInfo
from mcp_kafka.kafka_wrapper.client import KafkaClientWrapper
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.errors import KafkaOperationError, TopicNotFound


def get_cluster_info(
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,  # noqa: ARG001 - Reserved for future use
) -> ClusterInfo:
    """Get information about the Kafka cluster.

    Args:
        client: Kafka client wrapper
        enforcer: Access enforcer for validation (reserved for future use)

    Returns:
        Cluster information

    Raises:
        KafkaOperationError: If operation fails
    """
    try:
        logger.debug("Getting cluster info")
        metadata = client.admin.list_topics(timeout=client.config.timeout)

        # Get controller ID from metadata (fallback to first broker if not available)
        controller_id = (
            metadata.controller_id
            if metadata.controller_id is not None
            else next(iter(metadata.brokers.keys()), -1)
        )

        # Count non-internal topics
        topic_count = sum(1 for topic in metadata.topics if not topic.startswith("__"))

        info = ClusterInfo(
            cluster_id=metadata.cluster_id,
            controller_id=controller_id,
            broker_count=len(metadata.brokers),
            topic_count=topic_count,
        )

        logger.info(f"Cluster info: {info.broker_count} brokers, {info.topic_count} topics")
        return info

    except KafkaException as e:
        logger.error(f"Failed to get cluster info: {e}")
        raise KafkaOperationError(f"Failed to get cluster info: {e}") from e


def list_brokers(
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,  # noqa: ARG001 - Reserved for future use
) -> list[BrokerInfo]:
    """List all brokers in the cluster.

    Args:
        client: Kafka client wrapper
        enforcer: Access enforcer for validation (reserved for future use)

    Returns:
        List of broker information

    Raises:
        KafkaOperationError: If operation fails
    """
    try:
        logger.debug("Listing brokers")
        metadata = client.admin.list_topics(timeout=client.config.timeout)

        brokers: list[BrokerInfo] = []
        for broker_id, broker in metadata.brokers.items():
            brokers.append(
                BrokerInfo(
                    id=broker_id,
                    host=broker.host,
                    port=broker.port,
                    rack=None,  # Rack info not available in basic metadata
                )
            )

        logger.info(f"Listed {len(brokers)} brokers")
        return sorted(brokers, key=lambda b: b.id)

    except KafkaException as e:
        logger.error(f"Failed to list brokers: {e}")
        raise KafkaOperationError(f"Failed to list brokers: {e}") from e


def get_watermarks(
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    topic: str,
) -> list[WatermarkInfo]:
    """Get low and high watermarks for all partitions of a topic.

    Args:
        client: Kafka client wrapper
        enforcer: Access enforcer for validation
        topic: Topic name

    Returns:
        List of watermark information per partition

    Raises:
        TopicNotFound: If topic does not exist
        ValidationError: If topic name is invalid
        SafetyError: If topic is protected
        KafkaOperationError: If operation fails
    """
    # Validate topic name and access
    enforcer.validate_topic_name(topic)

    try:
        logger.debug(f"Getting watermarks for topic: {topic}")

        # First verify topic exists
        metadata = client.admin.list_topics(timeout=client.config.timeout)
        if topic not in metadata.topics:
            raise TopicNotFound(f"Topic '{topic}' not found")

        topic_metadata = metadata.topics[topic]
        partition_count = len(topic_metadata.partitions)

        watermarks: list[WatermarkInfo] = []

        # Use temporary consumer to get watermarks
        with client.temporary_consumer(f"watermark-check-{topic}") as temp_consumer:
            for partition_id in range(partition_count):
                tp = TopicPartition(topic, partition_id)
                low, high = temp_consumer.get_watermark_offsets(tp, timeout=client.config.timeout)

                watermarks.append(
                    WatermarkInfo(
                        topic=topic,
                        partition=partition_id,
                        low_watermark=low,
                        high_watermark=high,
                        message_count=max(0, high - low),
                    )
                )

        logger.info(
            f"Got watermarks for topic '{topic}': {len(watermarks)} partitions, "
            f"total messages: {sum(w.message_count for w in watermarks)}"
        )
        return sorted(watermarks, key=lambda w: w.partition)

    except TopicNotFound:
        raise
    except KafkaException as e:
        logger.error(f"Failed to get watermarks for topic '{topic}': {e}")
        raise KafkaOperationError(f"Failed to get watermarks for topic '{topic}': {e}") from e
