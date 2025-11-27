"""Fuzz tests for input validation functions.

Uses Hypothesis for property-based testing to find edge cases
and potential security issues in validation logic.
"""

import pytest
from hypothesis import assume, given, settings
from hypothesis import strategies as st

from mcp_kafka.config import SafetyConfig
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.errors import SafetyError, ValidationError

pytestmark = pytest.mark.fuzz


class TestTopicNameFuzz:
    """Fuzz tests for topic name validation."""

    @given(st.text())
    @settings(max_examples=500)
    def test_topic_name_never_crashes(self, topic_name: str) -> None:
        """Test that topic name validation never crashes."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        # Should either succeed or raise SafetyError/ValidationError
        try:
            enforcer.validate_topic_name(topic_name)
        except (SafetyError, ValidationError):
            pass  # Expected for invalid names

    @given(st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789._-", min_size=1, max_size=100))
    @settings(max_examples=200)
    def test_valid_topic_names_accepted(self, topic_name: str) -> None:
        """Test that valid topic names are accepted."""
        # Skip internal topic names
        assume(not topic_name.startswith("__"))

        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        # Should not raise for valid names
        enforcer.validate_topic_name(topic_name)

    @given(
        st.text(min_size=1).filter(
            lambda x: any(
                c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-"
                for c in x
            )
        )
    )
    @settings(max_examples=200)
    def test_invalid_chars_rejected(self, topic_name: str) -> None:
        """Test that topic names with invalid characters are rejected."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        with pytest.raises((SafetyError, ValidationError)):
            enforcer.validate_topic_name(topic_name)

    @given(st.sampled_from(["__consumer_offsets", "__transaction_state"]))
    def test_internal_topics_rejected(self, topic_name: str) -> None:
        """Test that internal topic names are always rejected."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        with pytest.raises(SafetyError):
            enforcer.validate_topic_name(topic_name)

    @given(st.text(min_size=250, max_size=300))
    @settings(max_examples=50)
    def test_very_long_topic_names(self, topic_name: str) -> None:
        """Test handling of extremely long topic names."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        # Should handle gracefully - either accept or reject
        try:
            enforcer.validate_topic_name(topic_name)
        except (SafetyError, ValidationError):
            pass


class TestConsumerGroupFuzz:
    """Fuzz tests for consumer group validation."""

    @given(st.text())
    @settings(max_examples=500)
    def test_consumer_group_never_crashes(self, group_id: str) -> None:
        """Test that consumer group validation never crashes."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        try:
            enforcer.validate_consumer_group(group_id)
        except (SafetyError, ValidationError):
            pass

    @given(st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789-_", min_size=1, max_size=50))
    @settings(max_examples=200)
    def test_valid_consumer_groups_accepted(self, group_id: str) -> None:
        """Test that valid consumer group IDs are accepted."""
        # Skip internal group names
        assume(not group_id.startswith("__"))

        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        # Should not raise for valid group IDs
        enforcer.validate_consumer_group(group_id)

    @given(st.text(min_size=1, max_size=50).map(lambda x: "__" + x))
    @settings(max_examples=100)
    def test_internal_consumer_groups_rejected(self, group_id: str) -> None:
        """Test that internal consumer group IDs are rejected."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        with pytest.raises(SafetyError):
            enforcer.validate_consumer_group(group_id)


class TestMessageSizeFuzz:
    """Fuzz tests for message size validation."""

    @given(st.integers())
    @settings(max_examples=500)
    def test_message_size_never_crashes(self, size: int) -> None:
        """Test that message size validation never crashes."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        try:
            enforcer.validate_message_size(size)
        except (SafetyError, ValidationError):
            pass

    @given(st.integers(min_value=1, max_value=1048576))  # 1 byte to 1 MB
    @settings(max_examples=200)
    def test_valid_message_sizes_accepted(self, size: int) -> None:
        """Test that valid message sizes are accepted."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        enforcer.validate_message_size(size)

    @given(st.integers(max_value=-1))
    @settings(max_examples=100)
    def test_negative_sizes_rejected(self, size: int) -> None:
        """Test that negative message sizes are rejected."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        with pytest.raises(ValidationError):
            enforcer.validate_message_size(size)

    @given(st.integers(min_value=10485761))  # > 10 MB
    @settings(max_examples=100)
    def test_extremely_large_sizes_rejected(self, size: int) -> None:
        """Test that extremely large message sizes are rejected."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        with pytest.raises(ValidationError):
            enforcer.validate_message_size(size)


class TestConsumeLimitFuzz:
    """Fuzz tests for consume limit validation."""

    @given(st.integers())
    @settings(max_examples=500)
    def test_consume_limit_never_crashes(self, limit: int) -> None:
        """Test that consume limit validation never crashes."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        try:
            enforcer.validate_consume_limit(limit)
        except (SafetyError, ValidationError):
            pass

    @given(st.integers(min_value=1, max_value=100))
    @settings(max_examples=200)
    def test_valid_limits_accepted(self, limit: int) -> None:
        """Test that valid consume limits are accepted."""
        config = SafetyConfig(
            allow_write_operations=True,
            max_consume_messages=100,
        )
        enforcer = AccessEnforcer(config)

        enforcer.validate_consume_limit(limit)

    @given(st.integers(max_value=0))
    @settings(max_examples=100)
    def test_zero_or_negative_limits_rejected(self, limit: int) -> None:
        """Test that zero or negative consume limits are rejected."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        with pytest.raises((SafetyError, ValidationError)):
            enforcer.validate_consume_limit(limit)

    @given(st.integers(min_value=101))
    @settings(max_examples=100)
    def test_limits_over_max_rejected(self, limit: int) -> None:
        """Test that limits over the maximum are rejected."""
        config = SafetyConfig(
            allow_write_operations=True,
            max_consume_messages=100,
        )
        enforcer = AccessEnforcer(config)

        with pytest.raises(ValidationError):
            enforcer.validate_consume_limit(limit)


class TestToolAccessFuzz:
    """Fuzz tests for tool access validation."""

    @given(st.text())
    @settings(max_examples=500)
    def test_tool_access_never_crashes(self, tool_name: str) -> None:
        """Test that tool access validation never crashes."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        try:
            enforcer.validate_tool_access(tool_name)
        except (SafetyError, ValidationError):
            pass

    @given(
        st.sampled_from(
            [
                "kafka_list_topics",
                "kafka_describe_topic",
                "kafka_list_consumer_groups",
                "kafka_describe_consumer_group",
                "kafka_get_consumer_lag",
                "kafka_cluster_info",
                "kafka_list_brokers",
                "kafka_get_watermarks",
                "kafka_consume_messages",
            ]
        )
    )
    def test_read_tools_always_allowed(self, tool_name: str) -> None:
        """Test that read-only tools are always allowed."""
        config = SafetyConfig(allow_write_operations=False)
        enforcer = AccessEnforcer(config)

        # Should not raise
        enforcer.validate_tool_access(tool_name)

    @given(
        st.sampled_from(
            [
                "kafka_create_topic",
                "kafka_produce_message",
                "kafka_reset_offsets",
            ]
        )
    )
    def test_write_tools_blocked_in_read_mode(self, tool_name: str) -> None:
        """Test that write tools are blocked in read-only mode."""
        config = SafetyConfig(allow_write_operations=False)
        enforcer = AccessEnforcer(config)

        with pytest.raises(SafetyError):
            enforcer.validate_tool_access(tool_name)

    @given(
        st.sampled_from(
            [
                "kafka_create_topic",
                "kafka_produce_message",
                "kafka_reset_offsets",
            ]
        )
    )
    def test_write_tools_allowed_in_write_mode(self, tool_name: str) -> None:
        """Test that write tools are allowed in write mode."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)

        # Should not raise
        enforcer.validate_tool_access(tool_name)


class TestBlocklistFuzz:
    """Fuzz tests for topic blocklist functionality."""

    @given(
        st.lists(
            st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789._-", min_size=1),
            min_size=0,
            max_size=10,
        )
    )
    @settings(max_examples=200)
    def test_custom_blocklist(self, blocklist: list[str]) -> None:
        """Test that custom blocklists work correctly."""
        config = SafetyConfig(
            allow_write_operations=True,
            topic_blocklist=blocklist,
        )
        enforcer = AccessEnforcer(config)

        for topic in blocklist:
            # Blocklisted topics should be rejected
            with pytest.raises(SafetyError):
                enforcer.validate_topic_name(topic)

    @given(
        st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789._-", min_size=1, max_size=50),
        st.lists(
            st.text(alphabet="abcdefghijklmnopqrstuvwxyz0123456789._-", min_size=1),
            min_size=0,
            max_size=5,
        ),
    )
    @settings(max_examples=200)
    def test_non_blocklisted_topics_allowed(self, topic: str, blocklist: list[str]) -> None:
        """Test that non-blocklisted topics are allowed."""
        # Skip if topic is in blocklist or starts with __
        assume(topic not in blocklist)
        assume(not topic.startswith("__"))

        config = SafetyConfig(
            allow_write_operations=True,
            topic_blocklist=blocklist,
        )
        enforcer = AccessEnforcer(config)

        # Should not raise
        enforcer.validate_topic_name(topic)
