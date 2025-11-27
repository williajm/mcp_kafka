"""Topic management tools for MCP Kafka."""

from concurrent.futures import Future
from typing import Any

from confluent_kafka import KafkaException
from confluent_kafka.admin import ConfigResource, NewTopic, ResourceType
from loguru import logger

from mcp_kafka.fastmcp_tools.common import PartitionInfo, TopicCreated, TopicDetail, TopicInfo
from mcp_kafka.kafka_wrapper.client import KafkaClientWrapper
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.errors import KafkaOperationError, TopicNotFound, ValidationError

# Constants
# Maximum partitions per topic (Kafka's recommended limit for cluster stability)
MAX_PARTITIONS = 10000
# Maximum replication factor (reasonable limit; actual max depends on broker count)
MAX_REPLICATION_FACTOR = 15


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
            is_internal = topic_name.startswith("__")

            # Always skip blocklisted topics (safety enforcement)
            if enforcer.is_blocklisted_topic(topic_name):
                continue

            # Skip internal topics unless include_internal is True
            if is_internal and not include_internal:
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


def create_topic(  # noqa: PLR0913
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    topic: str,
    partitions: int = 1,
    replication_factor: int = 1,
    config: dict[str, str] | None = None,
) -> TopicCreated:
    """Create a new Kafka topic.

    Args:
        client: Kafka client wrapper
        enforcer: Access enforcer for validation
        topic: Topic name to create
        partitions: Number of partitions (default: 1)
        replication_factor: Replication factor (default: 1)
        config: Optional topic configuration (e.g., retention.ms, cleanup.policy)

    Returns:
        TopicCreated response with created topic details

    Raises:
        ValidationError: If topic name is invalid or parameters are out of range
        SafetyError: If topic name is protected
        KafkaOperationError: If topic already exists or creation fails
    """
    # Validate topic name and access
    enforcer.validate_topic_name(topic)

    # Validate partitions and replication factor
    if partitions < 1:
        raise ValidationError("Number of partitions must be at least 1")
    if partitions > MAX_PARTITIONS:
        raise ValidationError(f"Number of partitions cannot exceed {MAX_PARTITIONS}")
    if replication_factor < 1:
        raise ValidationError("Replication factor must be at least 1")
    if replication_factor > MAX_REPLICATION_FACTOR:
        raise ValidationError(f"Replication factor cannot exceed {MAX_REPLICATION_FACTOR}")

    try:
        logger.debug(f"Creating topic: {topic} (partitions={partitions}, rf={replication_factor})")

        # Check if topic already exists
        metadata = client.admin.list_topics(timeout=client.config.timeout)
        if topic in metadata.topics:
            raise KafkaOperationError(f"Topic '{topic}' already exists")

        # Create new topic specification
        topic_config = config or {}
        new_topic = NewTopic(
            topic=topic,
            num_partitions=partitions,
            replication_factor=replication_factor,
            config=topic_config,
        )

        # Create the topic
        futures = client.admin.create_topics([new_topic])

        # Wait for creation to complete
        for topic_name, future in futures.items():
            future.result(timeout=client.config.timeout)
            logger.info(
                f"Created topic '{topic_name}': "
                f"{partitions} partitions, replication factor {replication_factor}"
            )

        return TopicCreated(
            topic=topic,
            partitions=partitions,
            replication_factor=replication_factor,
            config=topic_config,
        )

    except KafkaOperationError:
        raise
    except KafkaException as e:
        logger.error(f"Failed to create topic '{topic}': {e}")
        raise KafkaOperationError(f"Failed to create topic '{topic}': {e}") from e
