"""Message operation tools for MCP Kafka."""

import time
import uuid
from typing import Any

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from loguru import logger

from mcp_kafka.fastmcp_tools.common import ConsumedMessage, ProduceResult, validate_partition_exists
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


def _encode_message_parts(
    value: str,
    key: str | None,
    headers: dict[str, str] | None,
) -> tuple[bytes, bytes | None, int]:
    """Encode message parts and calculate total size.

    Args:
        value: Message value string
        key: Optional message key string
        headers: Optional message headers

    Returns:
        Tuple of (value_bytes, key_bytes, total_message_size)
    """
    value_bytes = value.encode("utf-8")
    key_bytes = key.encode("utf-8") if key else None
    headers_size = sum(
        len(k.encode("utf-8")) + len(v.encode("utf-8")) for k, v in (headers or {}).items()
    )
    message_size = len(value_bytes) + (len(key_bytes) if key_bytes else 0) + headers_size
    return value_bytes, key_bytes, message_size


def _prepare_kafka_headers(headers: dict[str, str] | None) -> list[tuple[str, bytes]] | None:
    """Convert headers dict to Kafka format.

    Args:
        headers: Optional headers dictionary

    Returns:
        List of (key, bytes_value) tuples or None
    """
    if not headers:
        return None
    return [(k, v.encode("utf-8")) for k, v in headers.items()]


def _create_delivery_callback(
    result_container: dict[str, Any],
) -> Any:
    """Create a delivery callback that populates the result container.

    Args:
        result_container: Mutable dict to store delivery result or error

    Returns:
        Callback function for producer.produce()
    """

    def callback(err: Any, msg: Any) -> None:
        if err:
            result_container["error"] = KafkaOperationError(f"Message delivery failed: {err}")
        else:
            result_container["topic"] = msg.topic()
            result_container["partition"] = msg.partition()
            result_container["offset"] = msg.offset()
            timestamp = msg.timestamp()
            result_container["timestamp"] = timestamp[1] if timestamp[0] != 0 else None

    return callback


def _execute_produce(  # noqa: PLR0913
    producer: Producer,
    topic: str,
    value_bytes: bytes,
    key_bytes: bytes | None,
    partition: int | None,
    kafka_headers: list[tuple[str, bytes]] | None,
    timeout: int,
) -> ProduceResult:
    """Execute the produce operation and return result.

    Args:
        producer: Kafka producer instance
        topic: Topic name
        value_bytes: Encoded message value
        key_bytes: Encoded message key or None
        partition: Target partition or None
        kafka_headers: Encoded headers or None
        timeout: Timeout for flush operation

    Returns:
        ProduceResult with delivery details

    Raises:
        KafkaOperationError: If delivery fails
    """
    result_container: dict[str, Any] = {}
    callback = _create_delivery_callback(result_container)

    producer.produce(
        topic=topic,
        value=value_bytes,
        key=key_bytes,
        partition=partition if partition is not None else -1,
        headers=kafka_headers,
        callback=callback,
    )

    producer.flush(timeout=timeout)

    if "error" in result_container:
        raise result_container["error"]

    if "topic" not in result_container:
        raise KafkaOperationError("Message delivery confirmation not received")

    return ProduceResult(
        topic=result_container["topic"],
        partition=result_container["partition"],
        offset=result_container["offset"],
        timestamp=result_container.get("timestamp"),
    )


def produce_message(  # noqa: PLR0913
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    topic: str,
    value: str,
    key: str | None = None,
    partition: int | None = None,
    headers: dict[str, str] | None = None,
) -> ProduceResult:
    """Produce a message to a Kafka topic.

    Args:
        client: Kafka client wrapper
        enforcer: Access enforcer for validation
        topic: Topic name to produce to
        value: Message value (string)
        key: Optional message key (string)
        partition: Optional specific partition to produce to
        headers: Optional message headers

    Returns:
        ProduceResult with details of the produced message

    Raises:
        TopicNotFound: If topic does not exist
        ValidationError: If topic name or message size is invalid
        SafetyError: If topic is protected
        KafkaOperationError: If produce operation fails
    """
    # Validate topic name and access
    enforcer.validate_topic_name(topic)

    # Encode message parts and validate size
    value_bytes, key_bytes, message_size = _encode_message_parts(value, key, headers)
    enforcer.validate_message_size(message_size)

    try:
        logger.debug(f"Producing message to topic: {topic}")

        # Verify topic exists
        metadata = client.admin.list_topics(timeout=client.config.timeout)
        if topic not in metadata.topics:
            raise TopicNotFound(f"Topic '{topic}' not found")

        # Validate partition if specified
        topic_metadata = metadata.topics[topic]
        partition_count = len(topic_metadata.partitions)
        validate_partition_exists(partition, partition_count, topic)

        # Create producer and execute
        producer_config = client.create_producer_config()
        kafka_headers = _prepare_kafka_headers(headers)

        producer = Producer(producer_config)
        try:
            result = _execute_produce(
                producer,
                topic,
                value_bytes,
                key_bytes,
                partition,
                kafka_headers,
                client.config.timeout,
            )
            logger.info(
                f"Produced message to topic '{topic}' "
                f"partition {result.partition} offset {result.offset}"
            )
            return result
        finally:
            producer.flush(timeout=client.config.timeout)

    except TopicNotFound:
        raise
    except KafkaOperationError:
        raise
    except KafkaException as e:
        logger.error(f"Failed to produce message to '{topic}': {e}")
        raise KafkaOperationError(f"Failed to produce message to '{topic}': {e}") from e
