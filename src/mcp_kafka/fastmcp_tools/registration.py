"""Tool registration for FastMCP server."""

from typing import Any

from fastmcp import FastMCP
from loguru import logger
from pydantic import Field

from mcp_kafka.fastmcp_tools import cluster, consumer_group, message, topic
from mcp_kafka.fastmcp_tools.common import (
    BrokerInfo,
    ClusterInfo,
    ConsumedMessage,
    ConsumerGroupDetail,
    ConsumerGroupInfo,
    PartitionLag,
    TopicDetail,
    TopicInfo,
    WatermarkInfo,
)
from mcp_kafka.kafka_wrapper.client import KafkaClientWrapper
from mcp_kafka.safety.core import AccessEnforcer


def register_tools(
    mcp: FastMCP,
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
) -> None:
    """Register all Kafka tools with the FastMCP server.

    Args:
        mcp: FastMCP server instance
        client: Kafka client wrapper
        enforcer: Access enforcer for validation
    """
    logger.info("Registering Kafka tools")

    # Topic tools
    @mcp.tool(  # type: ignore[misc]
        name="kafka_list_topics",
        description="List all Kafka topics with metadata (partition count, replication factor)",
    )
    def kafka_list_topics(
        include_internal: bool = Field(
            default=False,
            description="Include internal topics (e.g., __consumer_offsets)",
        ),
    ) -> list[dict[str, Any]]:
        """List all Kafka topics."""
        enforcer.validate_tool_access("kafka_list_topics")
        topics = topic.list_topics(client, enforcer, include_internal)
        return [t.model_dump() for t in topics]

    @mcp.tool(  # type: ignore[misc]
        name="kafka_describe_topic",
        description="Get detailed information about a topic including partitions and configuration",
    )
    def kafka_describe_topic(
        topic_name: str = Field(description="Name of the topic to describe"),
    ) -> dict[str, Any]:
        """Get detailed topic information."""
        enforcer.validate_tool_access("kafka_describe_topic")
        result = topic.describe_topic(client, enforcer, topic_name)
        return result.model_dump()

    # Consumer group tools
    @mcp.tool(  # type: ignore[misc]
        name="kafka_list_consumer_groups",
        description="List all consumer groups with their state and protocol type",
    )
    def kafka_list_consumer_groups(
        include_internal: bool = Field(
            default=False,
            description="Include internal consumer groups",
        ),
    ) -> list[dict[str, Any]]:
        """List all consumer groups."""
        enforcer.validate_tool_access("kafka_list_consumer_groups")
        groups = consumer_group.list_consumer_groups(client, enforcer, include_internal)
        return [g.model_dump() for g in groups]

    @mcp.tool(  # type: ignore[misc]
        name="kafka_describe_consumer_group",
        description="Get detailed consumer group information including members and lag",
    )
    def kafka_describe_consumer_group(
        group_id: str = Field(description="Consumer group ID to describe"),
    ) -> dict[str, Any]:
        """Get detailed consumer group information."""
        enforcer.validate_tool_access("kafka_describe_consumer_group")
        result = consumer_group.describe_consumer_group(client, enforcer, group_id)
        return result.model_dump()

    @mcp.tool(  # type: ignore[misc]
        name="kafka_get_consumer_lag",
        description="Get lag per partition for a consumer group",
    )
    def kafka_get_consumer_lag(
        group_id: str = Field(description="Consumer group ID"),
    ) -> list[dict[str, Any]]:
        """Get consumer lag information."""
        enforcer.validate_tool_access("kafka_get_consumer_lag")
        lags = consumer_group.get_consumer_lag(client, enforcer, group_id)
        return [lag.model_dump() for lag in lags]

    # Cluster tools
    @mcp.tool(  # type: ignore[misc]
        name="kafka_cluster_info",
        description="Get Kafka cluster metadata (cluster ID, controller, broker/topic counts)",
    )
    def kafka_cluster_info() -> dict[str, Any]:
        """Get cluster information."""
        enforcer.validate_tool_access("kafka_cluster_info")
        result = cluster.get_cluster_info(client, enforcer)
        return result.model_dump()

    @mcp.tool(  # type: ignore[misc]
        name="kafka_list_brokers",
        description="List all brokers in the Kafka cluster",
    )
    def kafka_list_brokers() -> list[dict[str, Any]]:
        """List all brokers."""
        enforcer.validate_tool_access("kafka_list_brokers")
        brokers = cluster.list_brokers(client, enforcer)
        return [b.model_dump() for b in brokers]

    @mcp.tool(  # type: ignore[misc]
        name="kafka_get_watermarks",
        description="Get low/high watermarks and message counts for all partitions of a topic",
    )
    def kafka_get_watermarks(
        topic_name: str = Field(description="Topic name to get watermarks for"),
    ) -> list[dict[str, Any]]:
        """Get topic watermarks."""
        enforcer.validate_tool_access("kafka_get_watermarks")
        watermarks = cluster.get_watermarks(client, enforcer, topic_name)
        return [w.model_dump() for w in watermarks]

    # Message tools
    @mcp.tool(  # type: ignore[misc]
        name="kafka_consume_messages",
        description="Consume messages from a topic (read-only peek, does not commit offsets)",
    )
    def kafka_consume_messages(
        topic_name: str = Field(description="Topic name to consume from"),
        partition: int | None = Field(
            default=None,
            description="Specific partition to consume from (None = all partitions)",
        ),
        offset: int | None = Field(
            default=None,
            description="Starting offset (None = latest messages)",
        ),
        limit: int = Field(
            default=10,
            description="Maximum number of messages to return",
            ge=1,
            le=1000,
        ),
        timeout: float = Field(
            default=5.0,
            description="Timeout in seconds for polling",
            ge=0.1,
            le=60.0,
        ),
    ) -> list[dict[str, Any]]:
        """Consume messages from a topic."""
        enforcer.validate_tool_access("kafka_consume_messages")
        messages = message.consume_messages(
            client, enforcer, topic_name, partition, offset, limit, timeout
        )
        return [m.model_dump() for m in messages]

    logger.success("Registered 9 Kafka READ tools")


# Export models for type hints
__all__ = [
    "register_tools",
    "TopicInfo",
    "TopicDetail",
    "ConsumerGroupInfo",
    "ConsumerGroupDetail",
    "PartitionLag",
    "ClusterInfo",
    "BrokerInfo",
    "WatermarkInfo",
    "ConsumedMessage",
]
