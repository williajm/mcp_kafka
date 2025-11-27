"""Consumer group management tools for MCP Kafka."""

from concurrent.futures import Future
from typing import Any, NoReturn

from confluent_kafka import KafkaException, TopicPartition
from confluent_kafka.admin import _ConsumerGroupState as ConsumerGroupState
from loguru import logger

from mcp_kafka.fastmcp_tools.common import (
    ConsumerGroupDetail,
    ConsumerGroupInfo,
    ConsumerGroupMember,
    OffsetResetResult,
    PartitionLag,
    validate_partition_exists,
)
from mcp_kafka.kafka_wrapper.client import KafkaClientWrapper
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.errors import (
    ConsumerGroupNotFound,
    KafkaOperationError,
    TopicNotFound,
    ValidationError,
)


def _state_to_string(state: ConsumerGroupState | None) -> str:
    """Convert ConsumerGroupState enum to string."""
    if state is None:
        return "Unknown"
    state_map = {
        ConsumerGroupState.UNKNOWN: "Unknown",
        ConsumerGroupState.PREPARING_REBALANCING: "PreparingRebalance",
        ConsumerGroupState.COMPLETING_REBALANCING: "CompletingRebalance",
        ConsumerGroupState.STABLE: "Stable",
        ConsumerGroupState.DEAD: "Dead",
        ConsumerGroupState.EMPTY: "Empty",
    }
    return state_map.get(state, "Unknown")


def _build_member_info(member: Any) -> ConsumerGroupMember:
    """Build ConsumerGroupMember from admin API member result.

    Args:
        member: Member object from describe_consumer_groups result

    Returns:
        ConsumerGroupMember with member details
    """
    assignments: list[dict[str, int | str]] = []
    if member.assignment and member.assignment.topic_partitions:
        assignments = [
            {"topic": tp.topic, "partition": tp.partition}
            for tp in member.assignment.topic_partitions
        ]

    return ConsumerGroupMember(
        member_id=member.member_id,
        client_id=member.client_id or "",
        client_host=member.host or "",
        assignments=assignments,
    )


def list_consumer_groups(
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    include_internal: bool = False,
) -> list[ConsumerGroupInfo]:
    """List all consumer groups.

    Args:
        client: Kafka client wrapper
        enforcer: Access enforcer for validation
        include_internal: Whether to include internal groups (default: False)

    Returns:
        List of consumer group information objects

    Raises:
        KafkaOperationError: If operation fails
    """
    try:
        logger.debug("Listing consumer groups")
        future = client.admin.list_consumer_groups()
        result = future.result(timeout=client.config.timeout)

        groups: list[ConsumerGroupInfo] = []
        for group in result.valid:
            # Skip protected groups unless include_internal is True
            if not include_internal and enforcer.is_protected_consumer_group(group.group_id):
                continue

            groups.append(
                ConsumerGroupInfo(
                    group_id=group.group_id,
                    protocol_type=group.protocol_type or "consumer",
                    state=_state_to_string(group.state) if group.state else "Unknown",
                )
            )

        logger.info(f"Listed {len(groups)} consumer groups")
        return sorted(groups, key=lambda g: g.group_id)

    except KafkaException as e:
        logger.error(f"Failed to list consumer groups: {e}")
        raise KafkaOperationError(f"Failed to list consumer groups: {e}") from e


def describe_consumer_group(
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    group_id: str,
) -> ConsumerGroupDetail:
    """Get detailed information about a consumer group including members and lag.

    Args:
        client: Kafka client wrapper
        enforcer: Access enforcer for validation
        group_id: Consumer group ID

    Returns:
        Detailed consumer group information

    Raises:
        ConsumerGroupNotFound: If group does not exist
        ValidationError: If group ID is invalid
        SafetyError: If group is protected
        KafkaOperationError: If operation fails
    """
    # Validate group ID and access
    enforcer.validate_consumer_group(group_id)

    try:
        logger.debug(f"Describing consumer group: {group_id}")
        futures: dict[str, Future[Any]] = client.admin.describe_consumer_groups([group_id])

        for gid, future in futures.items():
            if gid != group_id:
                continue

            result = future.result(timeout=client.config.timeout)

            # Build member info using helper function
            members = [_build_member_info(m) for m in result.members]

            # Get lag information
            partition_lags = get_consumer_lag(client, enforcer, group_id)
            total_lag = sum(pl.lag for pl in partition_lags)

            detail = ConsumerGroupDetail(
                group_id=group_id,
                protocol_type=result.protocol_type or "consumer",
                state=_state_to_string(result.state) if result.state else "Unknown",
                coordinator_id=result.coordinator.id if result.coordinator else None,
                members=members,
                partition_lags=partition_lags,
                total_lag=total_lag,
            )

            logger.info(
                f"Described consumer group '{group_id}': "
                f"{len(members)} members, total lag: {total_lag}"
            )
            return detail

        raise ConsumerGroupNotFound(f"Consumer group '{group_id}' not found")

    except ConsumerGroupNotFound:
        raise
    except KafkaException as e:
        logger.error(f"Failed to describe consumer group '{group_id}': {e}")
        raise KafkaOperationError(f"Failed to describe consumer group '{group_id}': {e}") from e


def get_consumer_lag(
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    group_id: str,
) -> list[PartitionLag]:
    """Get lag information for all partitions consumed by a consumer group.

    Args:
        client: Kafka client wrapper
        enforcer: Access enforcer for validation
        group_id: Consumer group ID

    Returns:
        List of partition lag information

    Raises:
        ConsumerGroupNotFound: If group does not exist
        ValidationError: If group ID is invalid
        SafetyError: If group is protected
        KafkaOperationError: If operation fails
    """
    # Validate group ID and access
    enforcer.validate_consumer_group(group_id)

    try:
        logger.debug(f"Getting consumer lag for group: {group_id}")

        # Get committed offsets for the group
        futures = client.admin.list_consumer_group_offsets([group_id])
        offset_result = None

        for gid, future in futures.items():
            if gid == group_id:
                offset_result = future.result(timeout=client.config.timeout)
                break

        if offset_result is None:
            raise ConsumerGroupNotFound(f"Consumer group '{group_id}' not found")

        partition_lags: list[PartitionLag] = []

        # Get high watermarks for each partition using a temporary consumer
        with client.temporary_consumer(f"lag-check-{group_id}") as temp_consumer:
            for tp in offset_result.topic_partitions:
                # Skip if topic is protected
                if enforcer.is_protected_topic(tp.topic):
                    continue

                # Get high watermark
                _, high = temp_consumer.get_watermark_offsets(
                    TopicPartition(tp.topic, tp.partition),
                    timeout=client.config.timeout,
                )

                current_offset = tp.offset if tp.offset >= 0 else 0
                lag = max(0, high - current_offset)

                partition_lags.append(
                    PartitionLag(
                        topic=tp.topic,
                        partition=tp.partition,
                        current_offset=current_offset,
                        log_end_offset=high,
                        lag=lag,
                    )
                )

        logger.info(
            f"Got lag for group '{group_id}': {len(partition_lags)} partitions, "
            f"total lag: {sum(pl.lag for pl in partition_lags)}"
        )
        return sorted(partition_lags, key=lambda p: (p.topic, p.partition))

    except ConsumerGroupNotFound:
        raise
    except KafkaException as e:
        logger.error(f"Failed to get consumer lag for '{group_id}': {e}")
        raise KafkaOperationError(f"Failed to get consumer lag for '{group_id}': {e}") from e


def _get_partitions_to_reset(
    partition: int | None,
    partition_count: int,
    topic: str,
) -> list[int]:
    """Determine which partitions to reset.

    Args:
        partition: Specific partition to reset (None = all partitions)
        partition_count: Total number of partitions in the topic
        topic: Topic name for error messages

    Returns:
        List of partition IDs to reset

    Raises:
        KafkaOperationError: If specified partition does not exist
    """
    validate_partition_exists(partition, partition_count, topic)
    if partition is not None:
        return [partition]
    return list(range(partition_count))


def _get_current_offsets(
    client: KafkaClientWrapper,
    group_id: str,
    topic: str,
) -> dict[tuple[str, int], int]:
    """Get current committed offsets for a consumer group.

    Args:
        client: Kafka client wrapper
        group_id: Consumer group ID
        topic: Topic name to filter offsets for

    Returns:
        Dictionary mapping (topic, partition) to current offset
    """
    futures = client.admin.list_consumer_group_offsets([group_id])
    current_offsets: dict[tuple[str, int], int] = {}

    for gid, future in futures.items():
        if gid == group_id:
            result = future.result(timeout=client.config.timeout)
            for tp in result.topic_partitions:
                if tp.topic == topic:
                    current_offsets[(tp.topic, tp.partition)] = max(0, tp.offset)
            break

    return current_offsets


def _calculate_target_offset(
    offset: int | str,
    low: int,
    high: int,
    partition: int,
) -> tuple[int, bool]:
    """Calculate the target offset and whether clamping occurred.

    Args:
        offset: Target offset specification ('earliest', 'latest', or numeric)
        low: Low watermark for the partition
        high: High watermark for the partition
        partition: Partition ID (for logging)

    Returns:
        Tuple of (target_offset, was_clamped)

    Raises:
        ValidationError: If offset string is invalid
    """
    if isinstance(offset, str):
        if offset == "earliest":
            return low, False
        if offset == "latest":
            return high, False
        raise ValidationError(f"Invalid offset string '{offset}'. Must be 'earliest' or 'latest'")

    # Numeric offset - clamp to valid range
    clamped_offset = max(low, min(offset, high))
    was_clamped = clamped_offset != offset
    if was_clamped:
        logger.warning(
            f"Requested offset {offset} for partition {partition} "
            f"clamped to valid range [{low}, {high}] -> {clamped_offset}"
        )
    return clamped_offset, was_clamped


def _get_watermarks(
    client: KafkaClientWrapper,
    group_id: str,
    topic: str,
    partitions: list[int],
) -> dict[int, tuple[int, int]]:
    """Get watermarks for partitions.

    Args:
        client: Kafka client wrapper
        group_id: Consumer group ID (used for temp consumer naming)
        topic: Topic name
        partitions: List of partition IDs

    Returns:
        Dictionary mapping partition ID to (low, high) watermarks
    """
    watermarks: dict[int, tuple[int, int]] = {}
    with client.temporary_consumer(f"reset-{group_id}") as temp_consumer:
        for p in partitions:
            low, high = temp_consumer.get_watermark_offsets(
                TopicPartition(topic, p), timeout=client.config.timeout
            )
            watermarks[p] = (low, high)
    return watermarks


def _build_target_topic_partitions(
    topic: str,
    partitions: list[int],
    watermarks: dict[int, tuple[int, int]],
    offset: int | str,
) -> list[TopicPartition]:
    """Build TopicPartition list with target offsets.

    Args:
        topic: Topic name
        partitions: List of partition IDs
        watermarks: Watermarks for each partition
        offset: Target offset specification

    Returns:
        List of TopicPartition with target offsets set
    """
    topic_partitions: list[TopicPartition] = []
    for p in partitions:
        low, high = watermarks[p]
        target_offset, _ = _calculate_target_offset(offset, low, high, p)
        tp = TopicPartition(topic, p, target_offset)
        topic_partitions.append(tp)
    return topic_partitions


def _process_alter_results(
    client: KafkaClientWrapper,
    group_id: str,
    alter_futures: dict[str, Any],
    current_offsets: dict[tuple[str, int], int],
) -> list[OffsetResetResult]:
    """Process alter consumer group offsets results.

    Args:
        client: Kafka client wrapper
        group_id: Consumer group ID
        alter_futures: Futures from alter_consumer_group_offsets
        current_offsets: Previous offsets for comparison

    Returns:
        List of OffsetResetResult

    Raises:
        KafkaOperationError: If any partition reset failed
    """
    results: list[OffsetResetResult] = []
    for gid, future in alter_futures.items():
        if gid != group_id:
            continue

        result = future.result(timeout=client.config.timeout)
        for tp in result.topic_partitions:
            if tp.error is not None:
                raise KafkaOperationError(
                    f"Failed to reset offset for {tp.topic}:{tp.partition}: {tp.error}"
                )

            previous = current_offsets.get((tp.topic, tp.partition), 0)
            results.append(
                OffsetResetResult(
                    group_id=group_id,
                    topic=tp.topic,
                    partition=tp.partition,
                    previous_offset=previous,
                    new_offset=tp.offset,
                )
            )
    return results


def _handle_reset_kafka_exception(e: KafkaException, group_id: str) -> NoReturn:
    """Handle KafkaException during offset reset and raise appropriate error.

    Args:
        e: The KafkaException to handle
        group_id: Consumer group ID for error messages

    Raises:
        KafkaOperationError: For coordinator or active group errors
        ConsumerGroupNotFound: If group not found
    """
    error_str = str(e)
    if "COORDINATOR_NOT_AVAILABLE" in error_str:
        raise KafkaOperationError(
            f"Cannot reset offsets for group '{group_id}': Group coordinator not available"
        ) from e
    if "GROUP_ID_NOT_FOUND" in error_str or "not found" in error_str.lower():
        raise ConsumerGroupNotFound(f"Consumer group '{group_id}' not found") from e
    if "NOT_COORDINATOR" in error_str or "active" in error_str.lower():
        raise KafkaOperationError(
            f"Cannot reset offsets for group '{group_id}': "
            "Group must be inactive (no active consumers)"
        ) from e
    logger.error(f"Failed to reset offsets for group '{group_id}': {e}")
    raise KafkaOperationError(f"Failed to reset offsets for '{group_id}': {e}") from e


def reset_offsets(  # noqa: PLR0913
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    group_id: str,
    topic: str,
    partition: int | None = None,
    offset: int | str = "latest",
) -> list[OffsetResetResult]:
    """Reset consumer group offsets for a topic.

    The consumer group must be inactive (no active members) for offset reset to work.
    If a numeric offset is specified that falls outside the valid range [low, high],
    it will be clamped to the nearest valid value and a warning will be logged.

    Args:
        client: Kafka client wrapper
        enforcer: Access enforcer for validation
        group_id: Consumer group ID
        topic: Topic name to reset offsets for
        partition: Specific partition to reset (None = all partitions)
        offset: Target offset - can be:
            - "earliest": Beginning of the topic
            - "latest": End of the topic
            - int: Specific offset value (clamped to valid range)

    Returns:
        List of OffsetResetResult for each partition reset

    Raises:
        ConsumerGroupNotFound: If group does not exist
        ValidationError: If group ID or topic name is invalid
        SafetyError: If group or topic is protected
        KafkaOperationError: If operation fails (e.g., group is active)
    """
    # Validate group ID and topic name
    enforcer.validate_consumer_group(group_id)
    enforcer.validate_topic_name(topic)

    try:
        logger.debug(f"Resetting offsets for group '{group_id}' on topic '{topic}'")

        # Verify topic exists and get partition info
        metadata = client.admin.list_topics(timeout=client.config.timeout)
        if topic not in metadata.topics:
            raise TopicNotFound(f"Topic '{topic}' not found")

        topic_metadata = metadata.topics[topic]
        partition_count = len(topic_metadata.partitions)

        # Determine which partitions to reset (validates partition if specified)
        partitions_to_reset = _get_partitions_to_reset(partition, partition_count, topic)

        # Get current offsets and watermarks
        current_offsets = _get_current_offsets(client, group_id, topic)
        watermarks = _get_watermarks(client, group_id, topic, partitions_to_reset)

        # Build topic partitions with target offsets
        topic_partitions = _build_target_topic_partitions(
            topic, partitions_to_reset, watermarks, offset
        )

        # Perform the offset reset and process results
        request = {group_id: topic_partitions}
        alter_futures = client.admin.alter_consumer_group_offsets(request)
        results = _process_alter_results(client, group_id, alter_futures, current_offsets)

        logger.info(
            f"Reset offsets for group '{group_id}' on topic '{topic}': {len(results)} partitions"
        )
        return sorted(results, key=lambda r: r.partition)

    except (ConsumerGroupNotFound, TopicNotFound, ValidationError):
        raise
    except KafkaOperationError:
        raise
    except KafkaException as e:
        _handle_reset_kafka_exception(e, group_id)
