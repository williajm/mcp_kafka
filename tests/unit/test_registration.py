"""Unit tests for tool registration module."""

from unittest.mock import MagicMock, patch

import pytest
from fastmcp import FastMCP

from mcp_kafka.config import SafetyConfig, SecurityConfig
from mcp_kafka.fastmcp_tools.registration import (
    _execute_with_middleware,
    register_tools,
)
from mcp_kafka.kafka_wrapper.client import KafkaClientWrapper
from mcp_kafka.middleware.stack import MiddlewareStack
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.errors import UnsafeOperationError


class TestExecuteWithMiddleware:
    """Tests for _execute_with_middleware helper."""

    def test_execute_without_middleware_success(self) -> None:
        """Test execution without middleware."""
        enforcer = MagicMock(spec=AccessEnforcer)
        enforcer.validate_tool_access = MagicMock()

        result = _execute_with_middleware(
            "test_tool",
            {"arg": "value"},
            None,  # No middleware
            enforcer,
            lambda: {"result": "success"},
        )

        assert result == {"result": "success"}
        enforcer.validate_tool_access.assert_called_once_with("test_tool")

    def test_execute_with_middleware_success(self, tmp_path) -> None:
        """Test execution with middleware on success."""
        enforcer = MagicMock(spec=AccessEnforcer)
        enforcer.validate_tool_access = MagicMock()

        audit_file = tmp_path / "audit.log"
        config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )
        middleware = MiddlewareStack(config)

        result = _execute_with_middleware(
            "test_tool",
            {"arg": "value"},
            middleware,
            enforcer,
            lambda: {"result": "success"},
        )

        assert result == {"result": "success"}
        enforcer.validate_tool_access.assert_called_once_with("test_tool")
        middleware.close()

        # Verify audit log was written
        content = audit_file.read_text()
        assert "test_tool" in content

    def test_execute_with_middleware_failure(self, tmp_path) -> None:
        """Test execution with middleware on failure."""
        enforcer = MagicMock(spec=AccessEnforcer)
        enforcer.validate_tool_access = MagicMock()

        audit_file = tmp_path / "audit.log"
        config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )
        middleware = MiddlewareStack(config)

        def failing_executor() -> dict:
            raise ValueError("Test error")

        with pytest.raises(ValueError, match="Test error"):
            _execute_with_middleware(
                "test_tool",
                {"arg": "value"},
                middleware,
                enforcer,
                failing_executor,
            )

        middleware.close()

        # Verify audit log captured the failure
        content = audit_file.read_text()
        assert "test_tool" in content
        assert "Test error" in content

    def test_execute_with_access_denied(self) -> None:
        """Test execution when access is denied."""
        enforcer = MagicMock(spec=AccessEnforcer)
        enforcer.validate_tool_access.side_effect = UnsafeOperationError(
            "Write operations not allowed"
        )

        with pytest.raises(UnsafeOperationError):
            _execute_with_middleware(
                "kafka_create_topic",
                {},
                None,
                enforcer,
                lambda: {},
            )

    def test_execute_with_middleware_logs_access_denied(self, tmp_path) -> None:
        """Test that access denied is logged by middleware."""
        enforcer = MagicMock(spec=AccessEnforcer)
        enforcer.validate_tool_access.side_effect = UnsafeOperationError(
            "Write operations not allowed"
        )

        audit_file = tmp_path / "audit.log"
        config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )
        middleware = MiddlewareStack(config)

        with pytest.raises(UnsafeOperationError):
            _execute_with_middleware(
                "kafka_create_topic",
                {"topic_name": "test"},
                middleware,
                enforcer,
                lambda: {},
            )

        middleware.close()

        # Verify failure was logged
        content = audit_file.read_text()
        assert "kafka_create_topic" in content


class TestRegisterTools:
    """Tests for register_tools function."""

    @patch("mcp_kafka.fastmcp_tools.registration.MiddlewareStack")
    def test_register_tools_without_security_config(self, mock_middleware_class: MagicMock) -> None:
        """Test register_tools without security config returns None middleware."""
        mcp = MagicMock(spec=FastMCP)
        client = MagicMock(spec=KafkaClientWrapper)
        enforcer = MagicMock(spec=AccessEnforcer)

        result = register_tools(mcp, client, enforcer, security_config=None)

        assert result is None
        mock_middleware_class.assert_not_called()
        # Should register 12 tools (9 READ + 3 WRITE)
        assert mcp.tool.call_count == 12

    @patch("mcp_kafka.fastmcp_tools.registration.MiddlewareStack")
    def test_register_tools_with_security_config(self, mock_middleware_class: MagicMock) -> None:
        """Test register_tools with security config creates middleware."""
        mcp = MagicMock(spec=FastMCP)
        client = MagicMock(spec=KafkaClientWrapper)
        enforcer = MagicMock(spec=AccessEnforcer)
        config = SecurityConfig(
            rate_limit_enabled=True,
            audit_log_enabled=True,
        )

        mock_middleware = MagicMock()
        mock_middleware_class.return_value = mock_middleware

        result = register_tools(mcp, client, enforcer, security_config=config)

        assert result == mock_middleware
        mock_middleware_class.assert_called_once_with(config)

    def test_register_tools_registers_all_tools(self) -> None:
        """Test that all 12 tools are registered."""
        mcp = MagicMock(spec=FastMCP)
        client = MagicMock(spec=KafkaClientWrapper)
        enforcer = MagicMock(spec=AccessEnforcer)

        register_tools(mcp, client, enforcer)

        # Verify 12 tools registered
        assert mcp.tool.call_count == 12

        # Get all tool names from decorator calls
        tool_names = [
            call.kwargs["name"] for call in mcp.tool.call_args_list if call.kwargs.get("name")
        ]

        # Verify expected tools
        expected_tools = [
            "kafka_list_topics",
            "kafka_describe_topic",
            "kafka_list_consumer_groups",
            "kafka_describe_consumer_group",
            "kafka_get_consumer_lag",
            "kafka_cluster_info",
            "kafka_list_brokers",
            "kafka_get_watermarks",
            "kafka_consume_messages",
            "kafka_create_topic",
            "kafka_produce_message",
            "kafka_reset_offsets",
        ]
        assert set(tool_names) == set(expected_tools)


class TestRegisteredToolExecution:
    """Tests for registered tool execution through middleware."""

    def setup_method(self) -> None:
        """Set up test fixtures."""
        self.mcp = FastMCP(name="test")
        self.safety_config = SafetyConfig(allow_write_operations=True)
        self.enforcer = AccessEnforcer(self.safety_config)

    @patch("mcp_kafka.fastmcp_tools.topic.list_topics")
    @patch("mcp_kafka.kafka_wrapper.client.AdminClient")
    def test_list_topics_tool_execution(
        self, mock_admin: MagicMock, mock_list_topics: MagicMock
    ) -> None:
        """Test kafka_list_topics tool execution."""
        from mcp_kafka.config import KafkaConfig
        from mcp_kafka.fastmcp_tools.common import TopicInfo

        mock_list_topics.return_value = [
            TopicInfo(name="test-topic", partition_count=3, replication_factor=1)
        ]

        config = KafkaConfig(bootstrap_servers="localhost:9092")
        client = KafkaClientWrapper(config)

        register_tools(self.mcp, client, self.enforcer)

        # Get the registered tool
        tool_fn = None
        for tool in self.mcp._tool_manager._tools.values():
            if tool.name == "kafka_list_topics":
                tool_fn = tool.fn
                break

        assert tool_fn is not None
        result = tool_fn(include_internal=False)

        assert len(result) == 1
        assert result[0]["name"] == "test-topic"

    @patch("mcp_kafka.fastmcp_tools.cluster.get_cluster_info")
    @patch("mcp_kafka.kafka_wrapper.client.AdminClient")
    def test_cluster_info_tool_execution(
        self, mock_admin: MagicMock, mock_cluster_info: MagicMock
    ) -> None:
        """Test kafka_cluster_info tool execution."""
        from mcp_kafka.config import KafkaConfig
        from mcp_kafka.fastmcp_tools.common import ClusterInfo

        mock_cluster_info.return_value = ClusterInfo(
            cluster_id="test-cluster",
            controller_id=1,
            broker_count=3,
            topic_count=10,
        )

        config = KafkaConfig(bootstrap_servers="localhost:9092")
        client = KafkaClientWrapper(config)

        register_tools(self.mcp, client, self.enforcer)

        # Get the registered tool
        tool_fn = None
        for tool in self.mcp._tool_manager._tools.values():
            if tool.name == "kafka_cluster_info":
                tool_fn = tool.fn
                break

        assert tool_fn is not None
        result = tool_fn()

        assert result["cluster_id"] == "test-cluster"
        assert result["broker_count"] == 3

    @patch("mcp_kafka.fastmcp_tools.topic.create_topic")
    @patch("mcp_kafka.kafka_wrapper.client.AdminClient")
    def test_create_topic_tool_execution(
        self, mock_admin: MagicMock, mock_create_topic: MagicMock
    ) -> None:
        """Test kafka_create_topic tool execution."""
        from mcp_kafka.config import KafkaConfig
        from mcp_kafka.fastmcp_tools.common import TopicCreated

        mock_create_topic.return_value = TopicCreated(
            topic="new-topic",
            partitions=3,
            replication_factor=1,
        )

        config = KafkaConfig(bootstrap_servers="localhost:9092")
        client = KafkaClientWrapper(config)

        register_tools(self.mcp, client, self.enforcer)

        # Get the registered tool
        tool_fn = None
        for tool in self.mcp._tool_manager._tools.values():
            if tool.name == "kafka_create_topic":
                tool_fn = tool.fn
                break

        assert tool_fn is not None
        result = tool_fn(topic_name="new-topic", partitions=3, replication_factor=1)

        assert result["topic"] == "new-topic"
        assert result["partitions"] == 3

    @patch("mcp_kafka.fastmcp_tools.consumer_group.reset_offsets")
    @patch("mcp_kafka.kafka_wrapper.client.AdminClient")
    def test_reset_offsets_with_numeric_offset(
        self, mock_admin: MagicMock, mock_reset_offsets: MagicMock
    ) -> None:
        """Test kafka_reset_offsets with numeric offset string."""
        from mcp_kafka.config import KafkaConfig
        from mcp_kafka.fastmcp_tools.common import OffsetResetResult

        mock_reset_offsets.return_value = [
            OffsetResetResult(
                group_id="test-group", topic="test", partition=0, previous_offset=0, new_offset=100
            )
        ]

        config = KafkaConfig(bootstrap_servers="localhost:9092")
        client = KafkaClientWrapper(config)

        register_tools(self.mcp, client, self.enforcer)

        # Get the registered tool
        tool_fn = None
        for tool in self.mcp._tool_manager._tools.values():
            if tool.name == "kafka_reset_offsets":
                tool_fn = tool.fn
                break

        assert tool_fn is not None
        result = tool_fn(group_id="test-group", topic_name="test", partition=None, offset="100")

        assert len(result) == 1
        assert result[0]["new_offset"] == 100
        # Verify reset_offsets was called with int offset
        mock_reset_offsets.assert_called_once()
        call_args = mock_reset_offsets.call_args
        assert call_args[0][4] is None  # partition
        assert call_args[0][5] == 100  # offset as int

    @patch("mcp_kafka.kafka_wrapper.client.AdminClient")
    def test_reset_offsets_with_invalid_offset(self, mock_admin: MagicMock) -> None:
        """Test kafka_reset_offsets with invalid offset string."""
        from mcp_kafka.config import KafkaConfig
        from mcp_kafka.utils.errors import ValidationError

        config = KafkaConfig(bootstrap_servers="localhost:9092")
        client = KafkaClientWrapper(config)

        register_tools(self.mcp, client, self.enforcer)

        # Get the registered tool
        tool_fn = None
        for tool in self.mcp._tool_manager._tools.values():
            if tool.name == "kafka_reset_offsets":
                tool_fn = tool.fn
                break

        assert tool_fn is not None
        with pytest.raises(ValidationError, match="Invalid offset"):
            tool_fn(
                group_id="test-group",
                topic_name="test",
                partition=None,
                offset="invalid",
            )

    @patch("mcp_kafka.fastmcp_tools.consumer_group.reset_offsets")
    @patch("mcp_kafka.kafka_wrapper.client.AdminClient")
    def test_reset_offsets_with_earliest(
        self, mock_admin: MagicMock, mock_reset_offsets: MagicMock
    ) -> None:
        """Test kafka_reset_offsets with 'earliest' offset."""
        from mcp_kafka.config import KafkaConfig
        from mcp_kafka.fastmcp_tools.common import OffsetResetResult

        mock_reset_offsets.return_value = [
            OffsetResetResult(
                group_id="test-group", topic="test", partition=0, previous_offset=100, new_offset=0
            )
        ]

        config = KafkaConfig(bootstrap_servers="localhost:9092")
        client = KafkaClientWrapper(config)

        register_tools(self.mcp, client, self.enforcer)

        # Get the registered tool
        tool_fn = None
        for tool in self.mcp._tool_manager._tools.values():
            if tool.name == "kafka_reset_offsets":
                tool_fn = tool.fn
                break

        assert tool_fn is not None
        result = tool_fn(
            group_id="test-group", topic_name="test", partition=None, offset="earliest"
        )

        assert len(result) == 1
        # Verify reset_offsets was called with string offset
        call_args = mock_reset_offsets.call_args
        assert call_args[0][5] == "earliest"  # offset as string


class TestMiddlewareIntegration:
    """Tests for middleware integration with tools."""

    def test_rate_limiting_applied_to_tools(self, tmp_path) -> None:
        """Test that rate limiting is applied to tool execution."""
        from mcp_kafka.config import KafkaConfig, SafetyConfig
        from mcp_kafka.utils.errors import RateLimitError

        mcp = FastMCP(name="test")
        safety_config = SafetyConfig(allow_write_operations=False)
        enforcer = AccessEnforcer(safety_config)

        security_config = SecurityConfig(
            rate_limit_enabled=True,
            rate_limit_rpm=1,  # Very low limit
            audit_log_enabled=False,
        )

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient"):
            kafka_config = KafkaConfig(bootstrap_servers="localhost:9092")
            client = KafkaClientWrapper(kafka_config)
            middleware = register_tools(mcp, client, enforcer, security_config)

        # Get the registered tool
        tool_fn = None
        for tool in mcp._tool_manager._tools.values():
            if tool.name == "kafka_cluster_info":
                tool_fn = tool.fn
                break

        assert tool_fn is not None

        # Mock the actual cluster call to avoid Kafka connection
        with patch("mcp_kafka.fastmcp_tools.cluster.get_cluster_info") as mock_cluster:
            from mcp_kafka.fastmcp_tools.common import ClusterInfo

            mock_cluster.return_value = ClusterInfo(
                cluster_id="test", controller_id=1, broker_count=1, topic_count=1
            )

            # First call should succeed
            tool_fn()

            # Second call should fail due to rate limit
            with pytest.raises(RateLimitError):
                tool_fn()

        if middleware:
            middleware.close()

    def test_audit_logging_applied_to_tools(self, tmp_path) -> None:
        """Test that audit logging is applied to tool execution."""
        from mcp_kafka.config import KafkaConfig, SafetyConfig

        mcp = FastMCP(name="test")
        safety_config = SafetyConfig(allow_write_operations=False)
        enforcer = AccessEnforcer(safety_config)

        audit_file = tmp_path / "audit.log"
        security_config = SecurityConfig(
            rate_limit_enabled=False,
            audit_log_enabled=True,
            audit_log_file=audit_file,
        )

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient"):
            kafka_config = KafkaConfig(bootstrap_servers="localhost:9092")
            client = KafkaClientWrapper(kafka_config)
            middleware = register_tools(mcp, client, enforcer, security_config)

        # Get the registered tool
        tool_fn = None
        for tool in mcp._tool_manager._tools.values():
            if tool.name == "kafka_list_brokers":
                tool_fn = tool.fn
                break

        assert tool_fn is not None

        # Mock the actual broker call
        with patch("mcp_kafka.fastmcp_tools.cluster.list_brokers") as mock_brokers:
            from mcp_kafka.fastmcp_tools.common import BrokerInfo

            mock_brokers.return_value = [BrokerInfo(id=1, host="localhost", port=9092)]

            tool_fn()

        if middleware:
            middleware.close()

        # Verify audit log was written
        content = audit_file.read_text()
        assert "kafka_list_brokers" in content
