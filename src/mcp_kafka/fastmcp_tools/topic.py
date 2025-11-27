"""Topic management tools for MCP Kafka."""

from concurrent.futures import Future
from typing import Any

from confluent_kafka import KafkaException
from confluent_kafka.admin import ConfigResource, ResourceType
from loguru import logger

from mcp_kafka.fastmcp_tools.common import PartitionInfo, TopicDetail, TopicInfo
from mcp_kafka.kafka_wrapper.client import KafkaClientWrapper
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.errors import KafkaOperationError, TopicNotFound


def list_topics(
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    include_internal: bool = False,
) -> list[TopicInfo]:
    """List all Kafka topics with metadata.

    Args:
        client: Kafka client wrapper
        enforcer: Access enforcer for validation
        include_internal: Whether to include internal topics (default: False)

    Returns:
        List of topic information objects

    Raises:
        KafkaOperationError: If operation fails
    """
    try:
        logger.debug("Listing Kafka topics")
        metadata = client.admin.list_topics(timeout=client.config.timeout)

        topics: list[TopicInfo] = []
        for topic_name, topic_metadata in metadata.topics.items():
            # Skip protected topics unless include_internal is True
            is_internal = topic_name.startswith("__")
            if not include_internal and enforcer.is_protected_topic(topic_name):
                continue

            # Get replication factor from first partition
            replication_factor = 0
            if topic_metadata.partitions:
                first_partition = list(topic_metadata.partitions.values())[0]
                replication_factor = len(first_partition.replicas)

            topics.append(
                TopicInfo(
                    name=topic_name,
                    partition_count=len(topic_metadata.partitions),
                    replication_factor=replication_factor,
                    is_internal=is_internal,
                )
            )

        logger.info(f"Listed {len(topics)} topics")
        return sorted(topics, key=lambda t: t.name)

    except KafkaException as e:
        logger.error(f"Failed to list topics: {e}")
        raise KafkaOperationError(f"Failed to list topics: {e}") from e


def describe_topic(
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    topic: str,
) -> TopicDetail:
    """Get detailed information about a specific topic.

    Args:
        client: Kafka client wrapper
        enforcer: Access enforcer for validation
        topic: Topic name to describe

    Returns:
        Detailed topic information

    Raises:
        TopicNotFound: If topic does not exist
        ValidationError: If topic name is invalid
        SafetyError: If topic is protected
        KafkaOperationError: If operation fails
    """
    # Validate topic name and access
    enforcer.validate_topic_name(topic)

    try:
        logger.debug(f"Describing topic: {topic}")
        metadata = client.admin.list_topics(timeout=client.config.timeout)

        if topic not in metadata.topics:
            raise TopicNotFound(f"Topic '{topic}' not found")

        topic_metadata = metadata.topics[topic]

        # Build partition info
        partitions: list[PartitionInfo] = []
        for partition_id, partition_metadata in topic_metadata.partitions.items():
            partitions.append(
                PartitionInfo(
                    partition=partition_id,
                    leader=partition_metadata.leader,
                    replicas=list(partition_metadata.replicas),
                    isrs=list(partition_metadata.isrs),
                )
            )

        # Get topic configuration
        config = _get_topic_config(client, topic)

        # Calculate replication factor
        replication_factor = 0
        if partitions:
            replication_factor = len(partitions[0].replicas)

        result = TopicDetail(
            name=topic,
            partition_count=len(partitions),
            replication_factor=replication_factor,
            is_internal=topic.startswith("__"),
            partitions=sorted(partitions, key=lambda p: p.partition),
            config=config,
        )

        logger.info(f"Described topic '{topic}': {len(partitions)} partitions")
        return result

    except TopicNotFound:
        raise
    except KafkaException as e:
        logger.error(f"Failed to describe topic '{topic}': {e}")
        raise KafkaOperationError(f"Failed to describe topic '{topic}': {e}") from e


def _get_topic_config(client: KafkaClientWrapper, topic: str) -> dict[str, str]:
    """Get topic configuration.

    Args:
        client: Kafka client wrapper
        topic: Topic name

    Returns:
        Dictionary of configuration key-value pairs
    """
    try:
        resource = ConfigResource(ResourceType.TOPIC, topic)
        futures: dict[ConfigResource, Future[Any]] = client.admin.describe_configs([resource])

        config: dict[str, str] = {}
        for future in futures.values():
            result = future.result(timeout=client.config.timeout)
            for config_name, config_entry in result.items():
                # Only include non-default configurations
                if config_entry.value is not None:
                    config[config_name] = config_entry.value

        return config

    except Exception as e:
        logger.warning(f"Failed to get topic config for '{topic}': {e}")
        return {}
