"""Tool registration for FastMCP server."""

from collections.abc import Callable
from typing import Annotated, Any, TypeVar

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
from mcp_kafka.version import __version__

T = TypeVar("T")


def _execute_with_middleware(  # noqa: PLR0913
    tool_name: str,
    args: dict[str, Any],
    middleware: MiddlewareStack | None,
    enforcer: AccessEnforcer,
    executor: Callable[[], T],
    client: KafkaClientWrapper | None = None,
) -> T:
    """Execute a tool with middleware wrapping.

    This helper centralizes the middleware before/after logic to reduce
    cognitive complexity in tool registrations.

    Args:
        tool_name: Name of the tool being executed
        args: Arguments passed to the tool
        middleware: Optional middleware stack for rate limiting/audit
        enforcer: Access enforcer for validation
        executor: Callable that performs the actual tool operation
        client: Optional Kafka client for connection reset on errors

    Returns:
        Result from the executor

    Raises:
        Any exception from the executor or enforcer
    """
    invocation = middleware.before_tool(tool_name, args) if middleware else None
    try:
        enforcer.validate_tool_access(tool_name)
        result = executor()
        if middleware and invocation:
            middleware.after_tool(invocation, success=True, result=result)
        return result
    except Exception as e:
        if middleware and invocation:
            middleware.after_tool(invocation, success=False, error=str(e))
        # Reset Kafka connection on connection-related errors for auto-recovery
        if client is not None:
            client.handle_connection_error(e)
        raise


def _register_topic_tools(
    mcp: FastMCP,
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    middleware: MiddlewareStack | None,
) -> None:
    """Register topic-related tools."""

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

        def execute() -> list[dict[str, Any]]:
            topics = topic.list_topics(client, enforcer, include_internal)
            return [t.model_dump() for t in topics]

        return _execute_with_middleware(
            "kafka_list_topics",
            {"include_internal": include_internal},
            middleware,
            enforcer,
            execute,
            client,
        )

    @mcp.tool(
        name="kafka_describe_topic",
        description="Get detailed information about a topic including partitions and configuration",
    )
    def kafka_describe_topic(
        topic_name: Annotated[str, Field(description="Name of the topic to describe")],
    ) -> dict[str, Any]:
        """Get detailed topic information."""

        def execute() -> dict[str, Any]:
            result = topic.describe_topic(client, enforcer, topic_name)
            return result.model_dump()

        return _execute_with_middleware(
            "kafka_describe_topic",
            {"topic_name": topic_name},
            middleware,
            enforcer,
            execute,
            client,
        )


def _register_consumer_group_tools(
    mcp: FastMCP,
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    middleware: MiddlewareStack | None,
) -> None:
    """Register consumer group tools."""

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

        def execute() -> list[dict[str, Any]]:
            groups = consumer_group.list_consumer_groups(client, enforcer, include_internal)
            return [g.model_dump() for g in groups]

        return _execute_with_middleware(
            "kafka_list_consumer_groups",
            {"include_internal": include_internal},
            middleware,
            enforcer,
            execute,
            client,
        )

    @mcp.tool(
        name="kafka_describe_consumer_group",
        description="Get detailed consumer group information including members and lag",
    )
    def kafka_describe_consumer_group(
        group_id: Annotated[str, Field(description="Consumer group ID to describe")],
    ) -> dict[str, Any]:
        """Get detailed consumer group information."""

        def execute() -> dict[str, Any]:
            result = consumer_group.describe_consumer_group(client, enforcer, group_id)
            return result.model_dump()

        return _execute_with_middleware(
            "kafka_describe_consumer_group",
            {"group_id": group_id},
            middleware,
            enforcer,
            execute,
            client,
        )

    @mcp.tool(
        name="kafka_get_consumer_lag",
        description="Get lag per partition for a consumer group",
    )
    def kafka_get_consumer_lag(
        group_id: Annotated[str, Field(description="Consumer group ID")],
    ) -> list[dict[str, Any]]:
        """Get consumer lag information."""

        def execute() -> list[dict[str, Any]]:
            lags = consumer_group.get_consumer_lag(client, enforcer, group_id)
            return [lag.model_dump() for lag in lags]

        return _execute_with_middleware(
            "kafka_get_consumer_lag",
            {"group_id": group_id},
            middleware,
            enforcer,
            execute,
            client,
        )


def _register_cluster_tools(
    mcp: FastMCP,
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    middleware: MiddlewareStack | None,
) -> None:
    """Register cluster tools."""

    @mcp.tool(
        name="kafka_cluster_info",
        description="Get Kafka cluster metadata (cluster ID, controller, broker/topic counts)",
    )
    def kafka_cluster_info() -> dict[str, Any]:
        """Get cluster information."""

        def execute() -> dict[str, Any]:
            result = cluster.get_cluster_info(client, enforcer)
            return result.model_dump()

        return _execute_with_middleware(
            "kafka_cluster_info",
            {},
            middleware,
            enforcer,
            execute,
            client,
        )

    @mcp.tool(
        name="kafka_list_brokers",
        description="List all brokers in the Kafka cluster",
    )
    def kafka_list_brokers() -> list[dict[str, Any]]:
        """List all brokers."""

        def execute() -> list[dict[str, Any]]:
            brokers = cluster.list_brokers(client, enforcer)
            return [b.model_dump() for b in brokers]

        return _execute_with_middleware(
            "kafka_list_brokers",
            {},
            middleware,
            enforcer,
            execute,
            client,
        )

    @mcp.tool(
        name="kafka_get_watermarks",
        description="Get low/high watermarks and message counts for all partitions of a topic",
    )
    def kafka_get_watermarks(
        topic_name: Annotated[str, Field(description="Topic name to get watermarks for")],
    ) -> list[dict[str, Any]]:
        """Get topic watermarks."""

        def execute() -> list[dict[str, Any]]:
            watermarks = cluster.get_watermarks(client, enforcer, topic_name)
            return [w.model_dump() for w in watermarks]

        return _execute_with_middleware(
            "kafka_get_watermarks",
            {"topic_name": topic_name},
            middleware,
            enforcer,
            execute,
            client,
        )


def _register_message_tools(
    mcp: FastMCP,
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    middleware: MiddlewareStack | None,
) -> None:
    """Register message tools."""

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

        def execute() -> list[dict[str, Any]]:
            messages = message.consume_messages(
                client, enforcer, topic_name, partition, offset, limit, timeout
            )
            return [m.model_dump() for m in messages]

        return _execute_with_middleware(
            "kafka_consume_messages",
            {
                "topic_name": topic_name,
                "partition": partition,
                "offset": offset,
                "limit": limit,
                "timeout": timeout,
            },
            middleware,
            enforcer,
            execute,
            client,
        )


def _register_write_tools(
    mcp: FastMCP,
    client: KafkaClientWrapper,
    enforcer: AccessEnforcer,
    middleware: MiddlewareStack | None,
) -> None:
    """Register write tools (require SAFETY_ALLOW_WRITE_OPERATIONS=true)."""

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

        def execute() -> dict[str, Any]:
            result = topic.create_topic(
                client, enforcer, topic_name, partitions, replication_factor, config
            )
            return result.model_dump()

        return _execute_with_middleware(
            "kafka_create_topic",
            {
                "topic_name": topic_name,
                "partitions": partitions,
                "replication_factor": replication_factor,
                "config": config,
            },
            middleware,
            enforcer,
            execute,
            client,
        )

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

        def execute() -> dict[str, Any]:
            result = message.produce_message(
                client, enforcer, topic_name, value, key, partition, headers
            )
            return result.model_dump()

        return _execute_with_middleware(
            "kafka_produce_message",
            {
                "topic_name": topic_name,
                "value": value,
                "key": key,
                "partition": partition,
                "headers": headers,
            },
            middleware,
            enforcer,
            execute,
            client,
        )

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

        def execute() -> list[dict[str, Any]]:
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
            return [r.model_dump() for r in results]

        return _execute_with_middleware(
            "kafka_reset_offsets",
            {
                "group_id": group_id,
                "topic_name": topic_name,
                "partition": partition,
                "offset": offset,
            },
            middleware,
            enforcer,
            execute,
            client,
        )


def register_tools(
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
    logger.info(f"Registering Kafka tools (mcp-kafka v{__version__})")

    # Initialize middleware stack if security config provided
    middleware: MiddlewareStack | None = None
    if security_config:
        middleware = MiddlewareStack(security_config)

    # Register all tool categories with debug logging
    logger.debug("Registering topic tools: kafka_list_topics, kafka_describe_topic")
    _register_topic_tools(mcp, client, enforcer, middleware)

    logger.debug(
        "Registering consumer group tools: kafka_list_consumer_groups, "
        "kafka_describe_consumer_group, kafka_get_consumer_lag"
    )
    _register_consumer_group_tools(mcp, client, enforcer, middleware)

    logger.debug(
        "Registering cluster tools: kafka_cluster_info, kafka_list_brokers, kafka_get_watermarks"
    )
    _register_cluster_tools(mcp, client, enforcer, middleware)

    logger.debug("Registering message tools: kafka_consume_messages")
    _register_message_tools(mcp, client, enforcer, middleware)
    logger.success("Registered 9 Kafka READ tools")

    logger.debug(
        "Registering write tools: kafka_create_topic, kafka_produce_message, kafka_reset_offsets"
    )
    _register_write_tools(mcp, client, enforcer, middleware)
    logger.success("Registered 3 Kafka WRITE tools")

    return middleware


# Export models for type hints
__all__ = [
    "register_tools",
    "_execute_with_middleware",
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
