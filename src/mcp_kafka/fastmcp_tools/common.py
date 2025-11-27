"""Common models and utilities for MCP Kafka tools."""

from pydantic import BaseModel, Field

# Common field descriptions to avoid duplication
DESC_TOPIC_NAME = "Topic name"
DESC_PARTITION_ID = "Partition ID"


class BrokerInfo(BaseModel):
    """Information about a Kafka broker."""

    id: int = Field(description="Broker ID")
    host: str = Field(description="Broker hostname")
    port: int = Field(description="Broker port")
    rack: str | None = Field(default=None, description="Broker rack ID")


class PartitionInfo(BaseModel):
    """Information about a topic partition."""

    partition: int = Field(description=DESC_PARTITION_ID)
    leader: int = Field(description="Leader broker ID")
    replicas: list[int] = Field(description="List of replica broker IDs")
    isrs: list[int] = Field(description="List of in-sync replica broker IDs")


class TopicInfo(BaseModel):
    """Information about a Kafka topic."""

    name: str = Field(description=DESC_TOPIC_NAME)
    partition_count: int = Field(description="Number of partitions")
    replication_factor: int = Field(description="Replication factor")
    is_internal: bool = Field(default=False, description="Whether this is an internal topic")


class TopicDetail(TopicInfo):
    """Detailed information about a Kafka topic including partitions and config."""

    partitions: list[PartitionInfo] = Field(description="Partition details")
    config: dict[str, str] = Field(default_factory=dict, description="Topic configuration")


class ConsumerGroupInfo(BaseModel):
    """Basic information about a consumer group."""

    group_id: str = Field(description="Consumer group ID")
    protocol_type: str = Field(description="Protocol type (usually 'consumer')")
    state: str = Field(description="Group state (Empty, Stable, etc.)")


class ConsumerGroupMember(BaseModel):
    """Information about a consumer group member."""

    member_id: str = Field(description="Member ID")
    client_id: str = Field(description="Client ID")
    client_host: str = Field(description="Client host/IP")
    assignments: list[dict[str, int | str]] = Field(
        default_factory=list, description="Topic-partition assignments"
    )


class PartitionLag(BaseModel):
    """Lag information for a single partition."""

    topic: str = Field(description=DESC_TOPIC_NAME)
    partition: int = Field(description=DESC_PARTITION_ID)
    current_offset: int = Field(description="Current committed offset")
    log_end_offset: int = Field(description="Log end offset (high watermark)")
    lag: int = Field(description="Number of messages behind")


class ConsumerGroupDetail(ConsumerGroupInfo):
    """Detailed information about a consumer group."""

    coordinator_id: int | None = Field(default=None, description="Coordinator broker ID")
    members: list[ConsumerGroupMember] = Field(default_factory=list, description="Group members")
    partition_lags: list[PartitionLag] = Field(
        default_factory=list, description="Lag per partition"
    )
    total_lag: int = Field(default=0, description="Total lag across all partitions")


class ClusterInfo(BaseModel):
    """Information about the Kafka cluster."""

    cluster_id: str | None = Field(default=None, description="Cluster ID")
    controller_id: int = Field(description="Controller broker ID")
    broker_count: int = Field(description="Number of brokers")
    topic_count: int = Field(description="Number of topics")


class WatermarkInfo(BaseModel):
    """Watermark information for a topic partition."""

    topic: str = Field(description=DESC_TOPIC_NAME)
    partition: int = Field(description=DESC_PARTITION_ID)
    low_watermark: int = Field(description="Low watermark (earliest offset)")
    high_watermark: int = Field(description="High watermark (next offset to be written)")
    message_count: int = Field(description="Approximate message count in partition")


class ConsumedMessage(BaseModel):
    """A consumed Kafka message."""

    topic: str = Field(description=DESC_TOPIC_NAME)
    partition: int = Field(description="Partition number")
    offset: int = Field(description="Message offset")
    timestamp: int | None = Field(default=None, description="Message timestamp (ms since epoch)")
    key: str | None = Field(default=None, description="Message key (decoded as UTF-8 if present)")
    value: str | None = Field(default=None, description="Message value (decoded as UTF-8)")
    headers: dict[str, str] = Field(default_factory=dict, description="Message headers")


# WRITE operation response models


class TopicCreated(BaseModel):
    """Response for successful topic creation."""

    topic: str = Field(description=DESC_TOPIC_NAME)
    partitions: int = Field(description="Number of partitions created")
    replication_factor: int = Field(description="Replication factor")
    config: dict[str, str] = Field(default_factory=dict, description="Topic configuration applied")


class ProduceResult(BaseModel):
    """Result of producing a message to Kafka."""

    topic: str = Field(description=DESC_TOPIC_NAME)
    partition: int = Field(description="Partition the message was written to")
    offset: int = Field(description="Offset of the produced message")
    timestamp: int | None = Field(default=None, description="Message timestamp (ms since epoch)")


class OffsetResetResult(BaseModel):
    """Result of resetting offsets for a consumer group."""

    group_id: str = Field(description="Consumer group ID")
    topic: str = Field(description=DESC_TOPIC_NAME)
    partition: int = Field(description=DESC_PARTITION_ID)
    previous_offset: int = Field(description="Previous committed offset")
    new_offset: int = Field(description="New offset after reset")


# Shared validation helpers


def validate_partition_exists(
    partition: int | None,
    partition_count: int,
    topic: str,
) -> None:
    """Validate that a partition exists for the given topic.

    Args:
        partition: Partition number to validate (None skips validation)
        partition_count: Total number of partitions in the topic
        topic: Topic name for error messages

    Raises:
        KafkaOperationError: If partition does not exist
    """
    from mcp_kafka.utils.errors import KafkaOperationError  # noqa: PLC0415

    if partition is None:
        return
    if partition < 0 or partition >= partition_count:
        max_partition = partition_count - 1
        raise KafkaOperationError(
            f"Partition {partition} does not exist. "
            f"Topic '{topic}' has {partition_count} partitions (0-{max_partition})"
        )
