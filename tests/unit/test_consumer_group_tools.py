"""Tests for consumer group management tools."""

from concurrent.futures import Future
from unittest.mock import MagicMock, patch

import pytest
from confluent_kafka import KafkaException

from mcp_kafka.config import KafkaConfig, SafetyConfig
from mcp_kafka.fastmcp_tools.consumer_group import (
    describe_consumer_group,
    get_consumer_lag,
    list_consumer_groups,
)
from mcp_kafka.kafka_wrapper.client import KafkaClientWrapper
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.errors import (
    ConsumerGroupNotFound,
    KafkaOperationError,
    SafetyError,
    ValidationError,
)


class TestListConsumerGroups:
    """Tests for list_consumer_groups function."""

    def test_list_consumer_groups_success(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test listing consumer groups successfully."""
        from confluent_kafka.admin import _ConsumerGroupState as ConsumerGroupState

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            # Create mock consumer group listings
            mock_group_1 = MagicMock()
            mock_group_1.group_id = "test-group-1"
            mock_group_1.protocol_type = "consumer"
            mock_group_1.state = ConsumerGroupState.STABLE

            mock_group_2 = MagicMock()
            mock_group_2.group_id = "test-group-2"
            mock_group_2.protocol_type = "consumer"
            mock_group_2.state = ConsumerGroupState.EMPTY

            mock_result = MagicMock()
            mock_result.valid = [mock_group_1, mock_group_2]

            mock_future = Future()
            mock_future.set_result(mock_result)

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.list_consumer_groups.return_value = mock_future
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            groups = list_consumer_groups(client, enforcer)

            assert len(groups) == 2
            assert groups[0].group_id == "test-group-1"
            assert groups[0].state == "Stable"
            assert groups[1].group_id == "test-group-2"
            assert groups[1].state == "Empty"

    def test_list_consumer_groups_excludes_internal_by_default(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test that internal groups are excluded by default."""
        from confluent_kafka.admin import _ConsumerGroupState as ConsumerGroupState

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_group = MagicMock()
            mock_group.group_id = "test-group"
            mock_group.protocol_type = "consumer"
            mock_group.state = ConsumerGroupState.STABLE

            mock_internal_group = MagicMock()
            mock_internal_group.group_id = "__internal-group"
            mock_internal_group.protocol_type = "consumer"
            mock_internal_group.state = ConsumerGroupState.STABLE

            mock_result = MagicMock()
            mock_result.valid = [mock_group, mock_internal_group]

            mock_future = Future()
            mock_future.set_result(mock_result)

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.list_consumer_groups.return_value = mock_future
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            groups = list_consumer_groups(client, enforcer, include_internal=False)

            assert len(groups) == 1
            assert groups[0].group_id == "test-group"

    def test_list_consumer_groups_includes_internal_when_requested(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test that internal groups are included when requested."""
        from confluent_kafka.admin import _ConsumerGroupState as ConsumerGroupState

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_group = MagicMock()
            mock_group.group_id = "__internal-group"
            mock_group.protocol_type = "consumer"
            mock_group.state = ConsumerGroupState.STABLE

            mock_result = MagicMock()
            mock_result.valid = [mock_group]

            mock_future = Future()
            mock_future.set_result(mock_result)

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.list_consumer_groups.return_value = mock_future
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            groups = list_consumer_groups(client, enforcer, include_internal=True)

            assert len(groups) == 1
            assert groups[0].group_id == "__internal-group"

    def test_list_consumer_groups_empty(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test listing consumer groups when none exist."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_result = MagicMock()
            mock_result.valid = []

            mock_future = Future()
            mock_future.set_result(mock_result)

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.list_consumer_groups.return_value = mock_future
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            groups = list_consumer_groups(client, enforcer)

            assert len(groups) == 0

    def test_list_consumer_groups_kafka_error(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test list_consumer_groups handles Kafka errors."""
        from confluent_kafka import KafkaException

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_future = Future()
            mock_future.set_exception(KafkaException("Connection failed"))

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.list_consumer_groups.return_value = mock_future
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(KafkaOperationError, match="Failed to list consumer groups"):
                list_consumer_groups(client, enforcer)


class TestDescribeConsumerGroup:
    """Tests for describe_consumer_group function."""

    def test_describe_consumer_group_success(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test describing a consumer group successfully."""
        from confluent_kafka.admin import _ConsumerGroupState as ConsumerGroupState

        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            # Mock member assignment
            mock_tp = MagicMock()
            mock_tp.topic = "test-topic"
            mock_tp.partition = 0

            mock_assignment = MagicMock()
            mock_assignment.topic_partitions = [mock_tp]

            # Mock member
            mock_member = MagicMock()
            mock_member.member_id = "member-1"
            mock_member.client_id = "client-1"
            mock_member.host = "192.168.1.1"
            mock_member.assignment = mock_assignment

            # Mock coordinator
            mock_coordinator = MagicMock()
            mock_coordinator.id = 1

            # Mock describe result
            mock_result = MagicMock()
            mock_result.protocol_type = "consumer"
            mock_result.state = ConsumerGroupState.STABLE
            mock_result.coordinator = mock_coordinator
            mock_result.members = [mock_member]

            mock_describe_future = Future()
            mock_describe_future.set_result(mock_result)

            # Mock offset result
            mock_offset_tp = MagicMock()
            mock_offset_tp.topic = "test-topic"
            mock_offset_tp.partition = 0
            mock_offset_tp.offset = 100

            mock_offset_result = MagicMock()
            mock_offset_result.topic_partitions = [mock_offset_tp]

            mock_offset_future = Future()
            mock_offset_future.set_result(mock_offset_result)

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.describe_consumer_groups.return_value = {"test-group": mock_describe_future}
            mock_admin.list_consumer_group_offsets.return_value = {"test-group": mock_offset_future}
            mock_cls.return_value = mock_admin

            # Mock consumer for watermarks
            mock_consumer = MagicMock()
            mock_consumer.get_watermark_offsets.return_value = (0, 150)

            client = KafkaClientWrapper(kafka_config)
            # Mock temporary_consumer context manager
            client.temporary_consumer = MagicMock()
            client.temporary_consumer.return_value.__enter__ = MagicMock(return_value=mock_consumer)
            client.temporary_consumer.return_value.__exit__ = MagicMock(return_value=False)
            enforcer = AccessEnforcer(safety_config)

            result = describe_consumer_group(client, enforcer, "test-group")

            assert result.group_id == "test-group"
            assert result.state == "Stable"
            assert result.coordinator_id == 1
            assert len(result.members) == 1
            assert result.members[0].member_id == "member-1"
            assert result.total_lag == 50

    def test_describe_consumer_group_protected(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test describe_consumer_group raises SafetyError for protected groups."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(SafetyError, match="not allowed"):
                describe_consumer_group(client, enforcer, "__internal-group")

    def test_describe_consumer_group_empty_id(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test describe_consumer_group raises ValidationError for empty group ID."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ValidationError, match="cannot be empty"):
                describe_consumer_group(client, enforcer, "")


class TestGetConsumerLag:
    """Tests for get_consumer_lag function."""

    def test_get_consumer_lag_success(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test getting consumer lag successfully."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            # Mock offset result
            mock_tp_1 = MagicMock()
            mock_tp_1.topic = "test-topic"
            mock_tp_1.partition = 0
            mock_tp_1.offset = 100

            mock_tp_2 = MagicMock()
            mock_tp_2.topic = "test-topic"
            mock_tp_2.partition = 1
            mock_tp_2.offset = 200

            mock_offset_result = MagicMock()
            mock_offset_result.topic_partitions = [mock_tp_1, mock_tp_2]

            mock_offset_future = Future()
            mock_offset_future.set_result(mock_offset_result)

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.list_consumer_group_offsets.return_value = {"test-group": mock_offset_future}
            mock_cls.return_value = mock_admin

            # Mock consumer for watermarks
            mock_consumer = MagicMock()
            mock_consumer.get_watermark_offsets.side_effect = [
                (0, 150),  # partition 0
                (0, 250),  # partition 1
            ]

            client = KafkaClientWrapper(kafka_config)
            # Mock temporary_consumer context manager
            client.temporary_consumer = MagicMock()
            client.temporary_consumer.return_value.__enter__ = MagicMock(return_value=mock_consumer)
            client.temporary_consumer.return_value.__exit__ = MagicMock(return_value=False)
            enforcer = AccessEnforcer(safety_config)

            lags = get_consumer_lag(client, enforcer, "test-group")

            assert len(lags) == 2
            assert lags[0].topic == "test-topic"
            assert lags[0].partition == 0
            assert lags[0].current_offset == 100
            assert lags[0].log_end_offset == 150
            assert lags[0].lag == 50

            assert lags[1].partition == 1
            assert lags[1].lag == 50

    def test_get_consumer_lag_protected_group(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test get_consumer_lag raises SafetyError for protected groups."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_cls.return_value = MagicMock()

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(SafetyError, match="not allowed"):
                get_consumer_lag(client, enforcer, "__internal-group")

    def test_get_consumer_lag_not_found(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test get_consumer_lag raises ConsumerGroupNotFound for missing group."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.list_consumer_group_offsets.return_value = {}
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(ConsumerGroupNotFound, match="not found"):
                get_consumer_lag(client, enforcer, "nonexistent")

    def test_get_consumer_lag_skips_protected_topics(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test get_consumer_lag skips protected topics."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            # Mock offset result with protected topic
            mock_tp_normal = MagicMock()
            mock_tp_normal.topic = "test-topic"
            mock_tp_normal.partition = 0
            mock_tp_normal.offset = 100

            mock_tp_internal = MagicMock()
            mock_tp_internal.topic = "__consumer_offsets"
            mock_tp_internal.partition = 0
            mock_tp_internal.offset = 50

            mock_offset_result = MagicMock()
            mock_offset_result.topic_partitions = [mock_tp_normal, mock_tp_internal]

            mock_offset_future = Future()
            mock_offset_future.set_result(mock_offset_result)

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.list_consumer_group_offsets.return_value = {"test-group": mock_offset_future}
            mock_cls.return_value = mock_admin

            mock_consumer = MagicMock()
            mock_consumer.get_watermark_offsets.return_value = (0, 150)

            client = KafkaClientWrapper(kafka_config)
            # Mock temporary_consumer context manager
            client.temporary_consumer = MagicMock()
            client.temporary_consumer.return_value.__enter__ = MagicMock(return_value=mock_consumer)
            client.temporary_consumer.return_value.__exit__ = MagicMock(return_value=False)
            enforcer = AccessEnforcer(safety_config)

            lags = get_consumer_lag(client, enforcer, "test-group")

            # Should only include the normal topic, not the internal one
            assert len(lags) == 1
            assert lags[0].topic == "test-topic"

    def test_get_consumer_lag_negative_offset_treated_as_zero(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test that negative offsets (e.g., -1001 for no offset) are treated as 0."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_tp = MagicMock()
            mock_tp.topic = "test-topic"
            mock_tp.partition = 0
            mock_tp.offset = -1001  # No committed offset

            mock_offset_result = MagicMock()
            mock_offset_result.topic_partitions = [mock_tp]

            mock_offset_future = Future()
            mock_offset_future.set_result(mock_offset_result)

            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.list_consumer_group_offsets.return_value = {"test-group": mock_offset_future}
            mock_cls.return_value = mock_admin

            mock_consumer = MagicMock()
            mock_consumer.get_watermark_offsets.return_value = (0, 100)

            client = KafkaClientWrapper(kafka_config)
            # Mock temporary_consumer context manager
            client.temporary_consumer = MagicMock()
            client.temporary_consumer.return_value.__enter__ = MagicMock(return_value=mock_consumer)
            client.temporary_consumer.return_value.__exit__ = MagicMock(return_value=False)
            enforcer = AccessEnforcer(safety_config)

            lags = get_consumer_lag(client, enforcer, "test-group")

            assert len(lags) == 1
            assert lags[0].current_offset == 0
            assert lags[0].lag == 100

    def test_get_consumer_lag_kafka_exception(
        self, kafka_config: KafkaConfig, safety_config: SafetyConfig
    ) -> None:
        """Test get_consumer_lag raises KafkaOperationError on Kafka failures."""
        with patch("mcp_kafka.kafka_wrapper.client.AdminClient") as mock_cls:
            mock_metadata = MagicMock()
            mock_metadata.brokers = {1: MagicMock()}
            mock_metadata.topics = {}

            mock_admin = MagicMock()
            mock_admin.list_topics.return_value = mock_metadata
            mock_admin.list_consumer_group_offsets.side_effect = KafkaException("Connection failed")
            mock_cls.return_value = mock_admin

            client = KafkaClientWrapper(kafka_config)
            enforcer = AccessEnforcer(safety_config)

            with pytest.raises(KafkaOperationError, match="Failed to get consumer lag"):
                get_consumer_lag(client, enforcer, "test-group")
