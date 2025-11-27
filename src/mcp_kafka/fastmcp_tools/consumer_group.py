"""Consumer group management tools for MCP Kafka."""

from concurrent.futures import Future
from typing import Any

from confluent_kafka import KafkaException, TopicPartition
from confluent_kafka.admin import _ConsumerGroupState as ConsumerGroupState
from loguru import logger

from mcp_kafka.fastmcp_tools.common import (
    ConsumerGroupDetail,
    ConsumerGroupInfo,
    ConsumerGroupMember,
    PartitionLag,
)
from mcp_kafka.kafka_wrapper.client import KafkaClientWrapper
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.errors import ConsumerGroupNotFound, KafkaOperationError


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
