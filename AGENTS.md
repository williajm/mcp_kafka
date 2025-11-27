# MCP Kafka - Claude Code Instructions

## Project Overview

MCP Kafka is a Model Context Protocol server for Apache Kafka. It provides 12 tools for Kafka operations with a 2-tier access control system (READ / READ_WRITE).

## Key Architecture

```
src/mcp_kafka/
├── config.py              # Pydantic settings (KafkaConfig, SafetyConfig, SecurityConfig)
├── fastmcp_server.py      # FastMCP server wrapper
├── kafka_wrapper/
│   └── client.py          # KafkaClientWrapper with connection management
├── fastmcp_tools/
│   ├── registration.py    # Tool registration with FastMCP
│   ├── common.py          # Pydantic response models
│   ├── topic.py           # list_topics, describe_topic
│   ├── consumer_group.py  # list/describe consumer groups, get_consumer_lag
│   ├── cluster.py         # cluster_info, list_brokers, get_watermarks
│   └── message.py         # consume_messages
├── safety/
│   └── core.py            # AccessEnforcer class
├── middleware/
│   └── safety.py          # SafetyMiddleware, ToolContext, ToolResult
└── utils/
    ├── errors.py          # Exception hierarchy
    └── validation.py      # Input validation
```

## Commands

```bash
# Install dependencies
uv sync --all-extras

# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src/mcp_kafka --cov-fail-under=80

# Linting
uv run ruff check src/ tests/

# Formatting
uv run ruff format src/ tests/

# Type checking
uv run mypy src/

# Run the server
uv run mcp-kafka
```

## Code Style

- Use Ruff for linting and formatting
- Type hints required (mypy --strict)
- Follow SOLID principles
- 80% test coverage minimum
- Pydantic for all configuration and response models

## Testing

- Tests in `tests/unit/` - mock Kafka clients
- Use `patch("mcp_kafka.kafka_wrapper.client.AdminClient")` for mocking
- For consumer operations, mock `client.temporary_consumer` context manager
- Use fixtures from `conftest.py` for configs

## Access Control

Two tiers:
- **READ**: All read operations (default)
- **READ_WRITE**: Create topics, produce messages, reset offsets

Protected resources:
- Internal topics: `__consumer_offsets`, `__transaction_state`
- Internal consumer groups: prefix `__`
- Blocklisted topics via `SAFETY_TOPIC_BLOCKLIST`

## Environment Variables

Key variables:
- `KAFKA_BOOTSTRAP_SERVERS`: Broker addresses
- `KAFKA_SECURITY_PROTOCOL`: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL
- `SAFETY_ALLOW_WRITE_OPERATIONS`: Enable write tools (default: false)
- `SAFETY_MAX_CONSUME_MESSAGES`: Limit per consume (default: 100)

## Implementation Plan

See `IMPLEMENTATION_PLAN.md` for full phase breakdown:
- Phase 1-2: Complete (Foundation, Access Control)
- Phase 3: Complete (READ Tools)
- Phase 4: Complete (WRITE Tools)
- Phase 5-6: Pending (Middleware, Docs)
