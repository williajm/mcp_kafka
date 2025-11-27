"""Message operation tools for MCP Kafka."""

import time
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaException, TopicPartition
from loguru import logger

from mcp_kafka.fastmcp_tools.common import ConsumedMessage
from mcp_kafka.kafka_wrapper.client import TEMP_CONSUMER_PREFIX, KafkaClientWrapper
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.errors import KafkaOperationError, TopicNotFound


def _decode_bytes(data: bytes | None) -> str | None:
    """Decode bytes to UTF-8 string, falling back to str() on failure."""
    if data is None:
        return None
    try:
        return data.decode("utf-8")
    except (UnicodeDecodeError, AttributeError):
        return str(data)


def _decode_headers(msg: Any) -> dict[str, str]:
    """Decode message headers to dict."""
    headers: dict[str, str] = {}
    if not msg.headers():
        return headers
    for header_key, header_value in msg.headers():
        headers[header_key] = _decode_bytes(header_value) or ""
    return headers


def _parse_message(msg: Any) -> ConsumedMessage:
    """Parse a Kafka message into a ConsumedMessage."""
    timestamp = msg.timestamp()
    return ConsumedMessage(
        topic=msg.topic(),
        partition=msg.partition(),
        offset=msg.offset(),
        timestamp=timestamp[1] if timestamp[0] != 0 else None,
        key=_decode_bytes(msg.key()),
        value=_decode_bytes(msg.value()),
        headers=_decode_headers(msg),
    )


def _validate_partition(
    partition: int | None,
    partition_count: int,
    topic: str,
) -> list[int]:
    """Validate partition and return list of partitions to consume from."""
    if partition is None:
        return list(range(partition_count))
    if partition < 0 or partition >= partition_count:
        max_partition = partition_count - 1
        raise KafkaOperationError(
            f"Partition {partition} does not exist. "
            f"Topic '{topic}' has {partition_count} partitions (0-{max_partition})"
        )
    return [partition]


def _build_topic_partitions(  # noqa: PLR0913
    consumer: Consumer,
    topic: str,
    partitions: list[int],
    offset: int | None,
    limit: int,
    timeout: int,
) -> list[TopicPartition]:
    """Build TopicPartition list with appropriate offsets."""
    topic_partitions = []
    for p in partitions:
        tp = TopicPartition(topic, p)
        if offset is not None:
            tp.offset = offset
        else:
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=timeout)
            tp.offset = max(low, high - limit)
        topic_partitions.append(tp)
    return topic_partitions


def _poll_messages(
    consumer: Consumer,
    limit: int,
    timeout: float,
) -> list[ConsumedMessage]:
    """Poll for messages from consumer."""
    messages: list[ConsumedMessage] = []
    remaining = limit
    start_time = time.monotonic()

    while remaining > 0:
        elapsed = time.monotonic() - start_time
        remaining_timeout = timeout - elapsed
        if remaining_timeout <= 0:
            break

        poll_time = min(1.0, remaining_timeout)
        msg = consumer.poll(timeout=poll_time)

        if msg is None:
            continue
        if msg.error():
            logger.warning(f"Message error: {msg.error()}")
            continue

        messages.append(_parse_message(msg))
        remaining -= 1

    return messages


def consume_messages(  # noqa: PLR0913
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    topic: str,
    partition: int | None = None,
    offset: int | None = None,
    limit: int = 10,
    timeout: float = 5.0,
) -> list[ConsumedMessage]:
    """Consume messages from a topic (read-only peek, does not commit offsets).

    Args:
        client: Kafka client wrapper
        enforcer: Access enforcer for validation
        topic: Topic name to consume from
        partition: Specific partition to consume from (None = all partitions)
        offset: Starting offset (None = latest - limit)
        limit: Maximum number of messages to return (default: 10)
        timeout: Timeout in seconds for polling (default: 5.0)

    Returns:
        List of consumed messages

    Raises:
        TopicNotFound: If topic does not exist
        ValidationError: If topic name or limit is invalid
        SafetyError: If topic is protected
        KafkaOperationError: If operation fails
    """
    enforcer.validate_topic_name(topic)
    enforcer.validate_consume_limit(limit)

    try:
        logger.debug(f"Consuming messages from topic: {topic}")

        metadata = client.admin.list_topics(timeout=client.config.timeout)
        if topic not in metadata.topics:
            raise TopicNotFound(f"Topic '{topic}' not found")

        topic_metadata = metadata.topics[topic]
        partition_count = len(topic_metadata.partitions)
        partitions = _validate_partition(partition, partition_count, topic)

        group_id = f"{TEMP_CONSUMER_PREFIX}-consume-{uuid.uuid4().hex[:8]}"
        consumer_config = client.create_consumer_config(
            group_id=group_id,
            enable_auto_commit=False,
            auto_offset_reset="latest",
        )

        consumer = Consumer(consumer_config)
        try:
            topic_partitions = _build_topic_partitions(
                consumer, topic, partitions, offset, limit, client.config.timeout
            )
            consumer.assign(topic_partitions)
            messages = _poll_messages(consumer, limit, timeout)
        finally:
            consumer.close()

        logger.info(f"Consumed {len(messages)} messages from topic '{topic}'")
        return sorted(messages, key=lambda m: (m.partition, m.offset))

    except TopicNotFound:
        raise
    except KafkaException as e:
        logger.error(f"Failed to consume messages from '{topic}': {e}")
        raise KafkaOperationError(f"Failed to consume messages from '{topic}': {e}") from e
