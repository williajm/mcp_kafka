"""Tool registration for FastMCP server."""

from typing import Annotated, Any

from fastmcp import FastMCP
from loguru import logger
from pydantic import Field

from mcp_kafka.config import SecurityConfig
from mcp_kafka.fastmcp_tools import cluster, consumer_group, message, topic
from mcp_kafka.fastmcp_tools.common import (
    BrokerInfo,
    ClusterInfo,
    ConsumedMessage,
    ConsumerGroupDetail,
    ConsumerGroupInfo,
    OffsetResetResult,
    PartitionLag,
    ProduceResult,
    TopicCreated,
    TopicDetail,
    TopicInfo,
    WatermarkInfo,
)
from mcp_kafka.kafka_wrapper.client import KafkaClientWrapper
from mcp_kafka.middleware.stack import MiddlewareStack
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.errors import ValidationError


def register_tools(  # noqa: PLR0915
    mcp: FastMCP,
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    security_config: SecurityConfig | None = None,
) -> MiddlewareStack | None:
    """Register all Kafka tools with the FastMCP server.

    Args:
        mcp: FastMCP server instance
        client: Kafka client wrapper
        enforcer: Access enforcer for validation
        security_config: Optional security configuration for middleware

    Returns:
        MiddlewareStack instance if security_config provided, None otherwise
    """
    logger.info("Registering Kafka tools")

    # Initialize middleware stack if security config provided
    middleware: MiddlewareStack | None = None
    if security_config:
        middleware = MiddlewareStack(security_config)

    # Topic tools
    @mcp.tool(
        name="kafka_list_topics",
        description="List all Kafka topics with metadata (partition count, replication factor)",
    )
    def kafka_list_topics(
        include_internal: Annotated[
            bool, Field(description="Include internal topics (e.g., __consumer_offsets)")
        ] = False,
    ) -> list[dict[str, Any]]:
        """List all Kafka topics."""
        args = {"include_internal": include_internal}
        invocation = middleware.before_tool("kafka_list_topics", args) if middleware else None
        try:
            enforcer.validate_tool_access("kafka_list_topics")
            topics = topic.list_topics(client, enforcer, include_internal)
            result = [t.model_dump() for t in topics]
            if middleware and invocation:
                middleware.after_tool(invocation, success=True, result=result)
            return result
        except Exception as e:
            if middleware and invocation:
                middleware.after_tool(invocation, success=False, error=str(e))
            raise

    @mcp.tool(
        name="kafka_describe_topic",
        description="Get detailed information about a topic including partitions and configuration",
    )
    def kafka_describe_topic(
        topic_name: Annotated[str, Field(description="Name of the topic to describe")],
    ) -> dict[str, Any]:
        """Get detailed topic information."""
        args = {"topic_name": topic_name}
        invocation = middleware.before_tool("kafka_describe_topic", args) if middleware else None
        try:
            enforcer.validate_tool_access("kafka_describe_topic")
            result = topic.describe_topic(client, enforcer, topic_name)
            result_dict = result.model_dump()
            if middleware and invocation:
                middleware.after_tool(invocation, success=True, result=result_dict)
            return result_dict
        except Exception as e:
            if middleware and invocation:
                middleware.after_tool(invocation, success=False, error=str(e))
            raise

    # Consumer group tools
    @mcp.tool(
        name="kafka_list_consumer_groups",
        description="List all consumer groups with their state and protocol type",
    )
    def kafka_list_consumer_groups(
        include_internal: Annotated[
            bool, Field(description="Include internal consumer groups")
        ] = False,
    ) -> list[dict[str, Any]]:
        """List all consumer groups."""
        args = {"include_internal": include_internal}
        invocation = (
            middleware.before_tool("kafka_list_consumer_groups", args) if middleware else None
        )
        try:
            enforcer.validate_tool_access("kafka_list_consumer_groups")
            groups = consumer_group.list_consumer_groups(client, enforcer, include_internal)
            result = [g.model_dump() for g in groups]
            if middleware and invocation:
                middleware.after_tool(invocation, success=True, result=result)
            return result
        except Exception as e:
            if middleware and invocation:
                middleware.after_tool(invocation, success=False, error=str(e))
            raise

    @mcp.tool(
        name="kafka_describe_consumer_group",
        description="Get detailed consumer group information including members and lag",
    )
    def kafka_describe_consumer_group(
        group_id: Annotated[str, Field(description="Consumer group ID to describe")],
    ) -> dict[str, Any]:
        """Get detailed consumer group information."""
        args = {"group_id": group_id}
        invocation = (
            middleware.before_tool("kafka_describe_consumer_group", args) if middleware else None
        )
        try:
            enforcer.validate_tool_access("kafka_describe_consumer_group")
            result = consumer_group.describe_consumer_group(client, enforcer, group_id)
            result_dict = result.model_dump()
            if middleware and invocation:
                middleware.after_tool(invocation, success=True, result=result_dict)
            return result_dict
        except Exception as e:
            if middleware and invocation:
                middleware.after_tool(invocation, success=False, error=str(e))
            raise

    @mcp.tool(
        name="kafka_get_consumer_lag",
        description="Get lag per partition for a consumer group",
    )
    def kafka_get_consumer_lag(
        group_id: Annotated[str, Field(description="Consumer group ID")],
    ) -> list[dict[str, Any]]:
        """Get consumer lag information."""
        args = {"group_id": group_id}
        invocation = middleware.before_tool("kafka_get_consumer_lag", args) if middleware else None
        try:
            enforcer.validate_tool_access("kafka_get_consumer_lag")
            lags = consumer_group.get_consumer_lag(client, enforcer, group_id)
            result = [lag.model_dump() for lag in lags]
            if middleware and invocation:
                middleware.after_tool(invocation, success=True, result=result)
            return result
        except Exception as e:
            if middleware and invocation:
                middleware.after_tool(invocation, success=False, error=str(e))
            raise

    # Cluster tools
    @mcp.tool(
        name="kafka_cluster_info",
        description="Get Kafka cluster metadata (cluster ID, controller, broker/topic counts)",
    )
    def kafka_cluster_info() -> dict[str, Any]:
        """Get cluster information."""
        args: dict[str, Any] = {}
        invocation = middleware.before_tool("kafka_cluster_info", args) if middleware else None
        try:
            enforcer.validate_tool_access("kafka_cluster_info")
            result = cluster.get_cluster_info(client, enforcer)
            result_dict = result.model_dump()
            if middleware and invocation:
                middleware.after_tool(invocation, success=True, result=result_dict)
            return result_dict
        except Exception as e:
            if middleware and invocation:
                middleware.after_tool(invocation, success=False, error=str(e))
            raise

    @mcp.tool(
        name="kafka_list_brokers",
        description="List all brokers in the Kafka cluster",
    )
    def kafka_list_brokers() -> list[dict[str, Any]]:
        """List all brokers."""
        args: dict[str, Any] = {}
        invocation = middleware.before_tool("kafka_list_brokers", args) if middleware else None
        try:
            enforcer.validate_tool_access("kafka_list_brokers")
            brokers = cluster.list_brokers(client, enforcer)
            result = [b.model_dump() for b in brokers]
            if middleware and invocation:
                middleware.after_tool(invocation, success=True, result=result)
            return result
        except Exception as e:
            if middleware and invocation:
                middleware.after_tool(invocation, success=False, error=str(e))
            raise

    @mcp.tool(
        name="kafka_get_watermarks",
        description="Get low/high watermarks and message counts for all partitions of a topic",
    )
    def kafka_get_watermarks(
        topic_name: Annotated[str, Field(description="Topic name to get watermarks for")],
    ) -> list[dict[str, Any]]:
        """Get topic watermarks."""
        args = {"topic_name": topic_name}
        invocation = middleware.before_tool("kafka_get_watermarks", args) if middleware else None
        try:
            enforcer.validate_tool_access("kafka_get_watermarks")
            watermarks = cluster.get_watermarks(client, enforcer, topic_name)
            result = [w.model_dump() for w in watermarks]
            if middleware and invocation:
                middleware.after_tool(invocation, success=True, result=result)
            return result
        except Exception as e:
            if middleware and invocation:
                middleware.after_tool(invocation, success=False, error=str(e))
            raise

    # Message tools
    @mcp.tool(
        name="kafka_consume_messages",
        description="Consume messages from a topic (read-only peek, does not commit offsets)",
    )
    def kafka_consume_messages(
        topic_name: Annotated[str, Field(description="Topic name to consume from")],
        partition: Annotated[
            int | None,
            Field(description="Specific partition to consume from (None = all partitions)"),
        ] = None,
        offset: Annotated[
            int | None, Field(description="Starting offset (None = latest messages)")
        ] = None,
        limit: Annotated[
            int, Field(description="Maximum number of messages to return (1-1000)", ge=1, le=1000)
        ] = 10,
        timeout: Annotated[
            float, Field(description="Timeout in seconds for polling (0.1-60.0)", ge=0.1, le=60.0)
        ] = 5.0,
    ) -> list[dict[str, Any]]:
        """Consume messages from a topic."""
        args = {
            "topic_name": topic_name,
            "partition": partition,
            "offset": offset,
            "limit": limit,
            "timeout": timeout,
        }
        invocation = middleware.before_tool("kafka_consume_messages", args) if middleware else None
        try:
            enforcer.validate_tool_access("kafka_consume_messages")
            messages = message.consume_messages(
                client, enforcer, topic_name, partition, offset, limit, timeout
            )
            result = [m.model_dump() for m in messages]
            if middleware and invocation:
                middleware.after_tool(invocation, success=True, result=result)
            return result
        except Exception as e:
            if middleware and invocation:
                middleware.after_tool(invocation, success=False, error=str(e))
            raise

    logger.success("Registered 9 Kafka READ tools")

    # ==================== WRITE TOOLS ====================
    # These tools require SAFETY_ALLOW_WRITE_OPERATIONS=true

    @mcp.tool(
        name="kafka_create_topic",
        description="Create a new Kafka topic (requires write access)",
    )
    def kafka_create_topic(
        topic_name: Annotated[str, Field(description="Name of the topic to create")],
        partitions: Annotated[
            int, Field(description="Number of partitions (1-10000)", ge=1, le=10000)
        ] = 1,
        replication_factor: Annotated[
            int,
            Field(
                description="Replication factor (1-15, must not exceed broker count)",
                ge=1,
                le=15,
            ),
        ] = 1,
        config: Annotated[
            dict[str, str] | None,
            Field(description="Topic configuration (e.g., retention.ms, cleanup.policy)"),
        ] = None,
    ) -> dict[str, Any]:
        """Create a new Kafka topic."""
        args = {
            "topic_name": topic_name,
            "partitions": partitions,
            "replication_factor": replication_factor,
            "config": config,
        }
        invocation = middleware.before_tool("kafka_create_topic", args) if middleware else None
        try:
            enforcer.validate_tool_access("kafka_create_topic")
            result = topic.create_topic(
                client, enforcer, topic_name, partitions, replication_factor, config
            )
            result_dict = result.model_dump()
            if middleware and invocation:
                middleware.after_tool(invocation, success=True, result=result_dict)
            return result_dict
        except Exception as e:
            if middleware and invocation:
                middleware.after_tool(invocation, success=False, error=str(e))
            raise

    @mcp.tool(
        name="kafka_produce_message",
        description="Produce a message to a Kafka topic (requires write access)",
    )
    def kafka_produce_message(
        topic_name: Annotated[str, Field(description="Topic name to produce to")],
        value: Annotated[str, Field(description="Message value (string)")],
        key: Annotated[str | None, Field(description="Optional message key")] = None,
        partition: Annotated[
            int | None, Field(description="Specific partition to produce to (None = auto)")
        ] = None,
        headers: Annotated[
            dict[str, str] | None, Field(description="Optional message headers")
        ] = None,
    ) -> dict[str, Any]:
        """Produce a message to a Kafka topic."""
        args = {
            "topic_name": topic_name,
            "value": value,
            "key": key,
            "partition": partition,
            "headers": headers,
        }
        invocation = middleware.before_tool("kafka_produce_message", args) if middleware else None
        try:
            enforcer.validate_tool_access("kafka_produce_message")
            result = message.produce_message(
                client, enforcer, topic_name, value, key, partition, headers
            )
            result_dict = result.model_dump()
            if middleware and invocation:
                middleware.after_tool(invocation, success=True, result=result_dict)
            return result_dict
        except Exception as e:
            if middleware and invocation:
                middleware.after_tool(invocation, success=False, error=str(e))
            raise

    @mcp.tool(
        name="kafka_reset_offsets",
        description="Reset consumer group offsets (requires write access, group must be inactive)",
    )
    def kafka_reset_offsets(
        group_id: Annotated[str, Field(description="Consumer group ID")],
        topic_name: Annotated[str, Field(description="Topic name to reset offsets for")],
        partition: Annotated[
            int | None, Field(description="Specific partition to reset (None = all partitions)")
        ] = None,
        offset: Annotated[
            str, Field(description="Target offset: 'earliest', 'latest', or a numeric offset")
        ] = "latest",
    ) -> list[dict[str, Any]]:
        """Reset consumer group offsets."""
        args = {
            "group_id": group_id,
            "topic_name": topic_name,
            "partition": partition,
            "offset": offset,
        }
        invocation = middleware.before_tool("kafka_reset_offsets", args) if middleware else None
        try:
            enforcer.validate_tool_access("kafka_reset_offsets")
            # Convert string offset to int if it's a numeric string
            offset_value: int | str = offset
            if offset not in ("earliest", "latest"):
                try:
                    offset_value = int(offset)
                except ValueError as e:
                    raise ValidationError(
                        f"Invalid offset '{offset}'. Must be 'earliest', 'latest', or a number"
                    ) from e
            results = consumer_group.reset_offsets(
                client, enforcer, group_id, topic_name, partition, offset_value
            )
            result = [r.model_dump() for r in results]
            if middleware and invocation:
                middleware.after_tool(invocation, success=True, result=result)
            return result
        except Exception as e:
            if middleware and invocation:
                middleware.after_tool(invocation, success=False, error=str(e))
            raise

    logger.success("Registered 3 Kafka WRITE tools")

    return middleware


# Export models for type hints
__all__ = [
    "register_tools",
    # READ models
    "TopicInfo",
    "TopicDetail",
    "ConsumerGroupInfo",
    "ConsumerGroupDetail",
    "PartitionLag",
    "ClusterInfo",
    "BrokerInfo",
    "WatermarkInfo",
    "ConsumedMessage",
    # WRITE models
    "TopicCreated",
    "ProduceResult",
    "OffsetResetResult",
]
