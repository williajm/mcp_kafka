"""Tests for access control module."""

import pytest

from mcp_kafka.config import SafetyConfig
from mcp_kafka.safety import (
    INTERNAL_CONSUMER_GROUP_PREFIXES,
    INTERNAL_TOPIC_PREFIXES,
    MAX_TOPIC_NAME_LENGTH,
    READ_WRITE_OPERATIONS,
    TOPIC_NAME_PATTERN,
    AccessEnforcer,
    AccessLevel,
)
from mcp_kafka.utils.errors import SafetyError, UnsafeOperationError, ValidationError


class TestAccessLevel:
    """Tests for AccessLevel enum."""

    def test_read_level(self) -> None:
        """Test READ access level."""
        assert AccessLevel.READ.value == "read"
        assert AccessLevel.READ.name == "READ"

    def test_read_write_level(self) -> None:
        """Test READ_WRITE access level."""
        assert AccessLevel.READ_WRITE.value == "read_write"
        assert AccessLevel.READ_WRITE.name == "READ_WRITE"

    def test_string_comparison(self) -> None:
        """Test AccessLevel can be compared via value."""
        assert AccessLevel.READ.value == "read"
        assert AccessLevel.READ_WRITE.value == "read_write"

    def test_all_levels_exist(self) -> None:
        """Test all expected access levels exist."""
        levels = list(AccessLevel)
        assert len(levels) == 2
        assert AccessLevel.READ in levels
        assert AccessLevel.READ_WRITE in levels


class TestConstants:
    """Tests for module constants."""

    def test_read_write_operations(self) -> None:
        """Test READ_WRITE_OPERATIONS contains expected tools."""
        assert "kafka_create_topic" in READ_WRITE_OPERATIONS
        assert "kafka_produce_message" in READ_WRITE_OPERATIONS
        assert "kafka_reset_offsets" in READ_WRITE_OPERATIONS
        assert len(READ_WRITE_OPERATIONS) == 3

    def test_internal_topic_prefixes(self) -> None:
        """Test internal topic prefixes."""
        assert "__" in INTERNAL_TOPIC_PREFIXES

    def test_internal_consumer_group_prefixes(self) -> None:
        """Test internal consumer group prefixes."""
        assert "__" in INTERNAL_CONSUMER_GROUP_PREFIXES

    def test_max_topic_name_length(self) -> None:
        """Test max topic name length."""
        assert MAX_TOPIC_NAME_LENGTH == 249

    def test_topic_name_pattern(self) -> None:
        """Test topic name pattern matches valid names."""
        assert TOPIC_NAME_PATTERN.match("my-topic")
        assert TOPIC_NAME_PATTERN.match("my_topic")
        assert TOPIC_NAME_PATTERN.match("my.topic")
        assert TOPIC_NAME_PATTERN.match("MyTopic123")
        assert not TOPIC_NAME_PATTERN.match("my topic")
        assert not TOPIC_NAME_PATTERN.match("my@topic")


class TestAccessEnforcerInit:
    """Tests for AccessEnforcer initialization."""

    def test_default_config(self) -> None:
        """Test initialization with default config."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        assert enforcer.config == config

    def test_custom_topic_blocklist(self) -> None:
        """Test initialization with custom topic blocklist."""
        config = SafetyConfig(topic_blocklist=["custom-blocked"])
        enforcer = AccessEnforcer(config)
        assert enforcer.is_protected_topic("custom-blocked")

    def test_allowed_tools_set(self) -> None:
        """Test initialization with allowed tools."""
        config = SafetyConfig(allowed_tools=["tool1", "tool2"])
        enforcer = AccessEnforcer(config)
        assert enforcer.can_access_tool("tool1")
        assert not enforcer.can_access_tool("tool3")

    def test_denied_tools_set(self) -> None:
        """Test initialization with denied tools."""
        config = SafetyConfig(denied_tools=["blocked-tool"])
        enforcer = AccessEnforcer(config)
        assert not enforcer.can_access_tool("blocked-tool")


class TestAccessEnforcerAccessLevel:
    """Tests for access level determination."""

    def test_read_only_default(self) -> None:
        """Test default access level is READ."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        assert enforcer.get_effective_access_level() == AccessLevel.READ

    def test_read_write_when_enabled(self) -> None:
        """Test READ_WRITE when write operations enabled."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)
        assert enforcer.get_effective_access_level() == AccessLevel.READ_WRITE


class TestAccessEnforcerToolAccess:
    """Tests for tool access validation."""

    def test_read_tool_allowed_by_default(self) -> None:
        """Test read tools are allowed by default."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        assert enforcer.can_access_tool("kafka_list_topics")
        assert enforcer.can_access_tool("kafka_describe_topic")

    def test_write_tool_blocked_by_default(self) -> None:
        """Test write tools are blocked by default."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        assert not enforcer.can_access_tool("kafka_create_topic")
        assert not enforcer.can_access_tool("kafka_produce_message")
        assert not enforcer.can_access_tool("kafka_reset_offsets")

    def test_write_tool_allowed_when_enabled(self) -> None:
        """Test write tools allowed when enabled."""
        config = SafetyConfig(allow_write_operations=True)
        enforcer = AccessEnforcer(config)
        assert enforcer.can_access_tool("kafka_create_topic")
        assert enforcer.can_access_tool("kafka_produce_message")

    def test_denied_tools_takes_precedence(self) -> None:
        """Test denied_tools takes precedence over allowed_tools."""
        config = SafetyConfig(
            allowed_tools=["tool1", "tool2"],
            denied_tools=["tool2"],
        )
        enforcer = AccessEnforcer(config)
        assert enforcer.can_access_tool("tool1")
        assert not enforcer.can_access_tool("tool2")

    def test_allowed_tools_whitelist(self) -> None:
        """Test allowed_tools acts as whitelist."""
        config = SafetyConfig(allowed_tools=["allowed-tool"])
        enforcer = AccessEnforcer(config)
        assert enforcer.can_access_tool("allowed-tool")
        assert not enforcer.can_access_tool("other-tool")

    def test_validate_tool_access_raises_for_denied(self) -> None:
        """Test validate_tool_access raises for denied tool."""
        config = SafetyConfig(denied_tools=["blocked"])
        enforcer = AccessEnforcer(config)
        with pytest.raises(UnsafeOperationError, match="denied by configuration"):
            enforcer.validate_tool_access("blocked")

    def test_validate_tool_access_raises_for_not_allowed(self) -> None:
        """Test validate_tool_access raises for tool not in whitelist."""
        config = SafetyConfig(allowed_tools=["allowed"])
        enforcer = AccessEnforcer(config)
        with pytest.raises(UnsafeOperationError, match="not in allowed"):
            enforcer.validate_tool_access("other")

    def test_validate_tool_access_raises_for_write_disabled(self) -> None:
        """Test validate_tool_access raises for write tool when disabled."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        with pytest.raises(UnsafeOperationError, match="requires write access"):
            enforcer.validate_tool_access("kafka_create_topic")


class TestAccessEnforcerTopicValidation:
    """Tests for topic validation."""

    def test_valid_topic_names(self) -> None:
        """Test valid topic names pass validation."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)

        # These should not raise
        enforcer.validate_topic_name("my-topic")
        enforcer.validate_topic_name("my_topic")
        enforcer.validate_topic_name("my.topic")
        enforcer.validate_topic_name("MyTopic123")
        enforcer.validate_topic_name("a")

    def test_empty_topic_name(self) -> None:
        """Test empty topic name raises ValidationError."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        with pytest.raises(ValidationError, match="cannot be empty"):
            enforcer.validate_topic_name("")

    def test_topic_name_too_long(self) -> None:
        """Test topic name exceeding max length raises error."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        long_name = "a" * (MAX_TOPIC_NAME_LENGTH + 1)
        with pytest.raises(ValidationError, match="exceeds maximum length"):
            enforcer.validate_topic_name(long_name)

    def test_invalid_topic_characters(self) -> None:
        """Test topic name with invalid characters raises error."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)

        with pytest.raises(ValidationError, match="Invalid topic name"):
            enforcer.validate_topic_name("my topic")  # space
        with pytest.raises(ValidationError, match="Invalid topic name"):
            enforcer.validate_topic_name("my@topic")  # special char
        with pytest.raises(ValidationError, match="Invalid topic name"):
            enforcer.validate_topic_name("my/topic")  # slash

    def test_internal_topic_blocked(self) -> None:
        """Test internal topics are blocked."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        with pytest.raises(SafetyError, match="not allowed"):
            enforcer.validate_topic_name("__consumer_offsets")

    def test_blocklisted_topic(self) -> None:
        """Test blocklisted topics are blocked."""
        config = SafetyConfig(topic_blocklist=["blocked-topic"])
        enforcer = AccessEnforcer(config)
        with pytest.raises(SafetyError, match="not allowed"):
            enforcer.validate_topic_name("blocked-topic")

    def test_is_protected_topic_internal(self) -> None:
        """Test is_protected_topic detects internal topics."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        assert enforcer.is_protected_topic("__consumer_offsets")
        assert enforcer.is_protected_topic("__transaction_state")
        assert enforcer.is_protected_topic("__any_internal")

    def test_is_protected_topic_blocklist(self) -> None:
        """Test is_protected_topic detects blocklisted topics."""
        config = SafetyConfig(topic_blocklist=["custom-blocked"])
        enforcer = AccessEnforcer(config)
        assert enforcer.is_protected_topic("custom-blocked")
        assert not enforcer.is_protected_topic("normal-topic")


class TestAccessEnforcerConsumerGroupValidation:
    """Tests for consumer group validation."""

    def test_valid_consumer_group(self) -> None:
        """Test valid consumer groups pass validation."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)

        # Should not raise
        enforcer.validate_consumer_group("my-consumer-group")
        enforcer.validate_consumer_group("group_1")

    def test_empty_consumer_group(self) -> None:
        """Test empty consumer group raises error."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        with pytest.raises(ValidationError, match="cannot be empty"):
            enforcer.validate_consumer_group("")

    def test_internal_consumer_group_blocked(self) -> None:
        """Test internal consumer groups are blocked."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        with pytest.raises(SafetyError, match="not allowed"):
            enforcer.validate_consumer_group("__internal_group")

    def test_is_protected_consumer_group(self) -> None:
        """Test is_protected_consumer_group detection."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        assert enforcer.is_protected_consumer_group("__internal")
        assert not enforcer.is_protected_consumer_group("normal-group")


class TestAccessEnforcerMessageSizeValidation:
    """Tests for message size validation."""

    def test_valid_message_size(self) -> None:
        """Test valid message sizes pass validation."""
        config = SafetyConfig(max_message_size=1000)
        enforcer = AccessEnforcer(config)

        # Should not raise
        enforcer.validate_message_size(0)
        enforcer.validate_message_size(500)
        enforcer.validate_message_size(1000)

    def test_negative_message_size(self) -> None:
        """Test negative message size raises error."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        with pytest.raises(ValidationError, match="cannot be negative"):
            enforcer.validate_message_size(-1)

    def test_message_size_exceeds_limit(self) -> None:
        """Test message size exceeding limit raises error."""
        config = SafetyConfig(max_message_size=1000)
        enforcer = AccessEnforcer(config)
        with pytest.raises(ValidationError, match="exceeds maximum"):
            enforcer.validate_message_size(1001)


class TestAccessEnforcerConsumeLimitValidation:
    """Tests for consume limit validation."""

    def test_valid_consume_limit(self) -> None:
        """Test valid consume limits pass validation."""
        config = SafetyConfig(max_consume_messages=100)
        enforcer = AccessEnforcer(config)

        # Should not raise
        enforcer.validate_consume_limit(1)
        enforcer.validate_consume_limit(50)
        enforcer.validate_consume_limit(100)

    def test_consume_limit_zero(self) -> None:
        """Test consume limit of 0 raises error."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        with pytest.raises(ValidationError, match="must be at least 1"):
            enforcer.validate_consume_limit(0)

    def test_consume_limit_negative(self) -> None:
        """Test negative consume limit raises error."""
        config = SafetyConfig()
        enforcer = AccessEnforcer(config)
        with pytest.raises(ValidationError, match="must be at least 1"):
            enforcer.validate_consume_limit(-1)

    def test_consume_limit_exceeds_max(self) -> None:
        """Test consume limit exceeding max raises error."""
        config = SafetyConfig(max_consume_messages=100)
        enforcer = AccessEnforcer(config)
        with pytest.raises(ValidationError, match="exceeds maximum"):
            enforcer.validate_consume_limit(101)
