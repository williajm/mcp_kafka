# MCP Kafka Implementation Plan

## Overview

A Model Context Protocol server for Apache Kafka with:
- **12 tools** for v0.1.0 (focused, essential operations)
- **Simple 2-tier access control**: READ and READ/WRITE
- **Deep consumer group management** (lag monitoring, offset inspection)
- **Universal Kafka support** via confluent-kafka with SASL + mTLS
- **Python 3.11+**

## Principles

- **Clean Code**: Follow SOLID principles, meaningful names, small functions
- **Trusted Libraries**: Use established 3rd-party libs (authlib for OAuth, pydantic for validation)
- **Test-Driven**: 80% unit test coverage enforced via GitHub Actions
- **Tooling**: uv for package management, Ruff for formatting/linting

## Project Structure

```
src/mcp_kafka/
├── __init__.py
├── __main__.py               # Typer CLI entry point
├── version.py
├── config.py                 # Pydantic config (KafkaConfig, SafetyConfig, SecurityConfig)
├── fastmcp_server.py         # FastMCP server wrapper
├── fastmcp_resources.py      # Resource templates
├── fastmcp_prompts.py        # AI prompts
├── kafka_wrapper/
│   ├── client.py             # KafkaClientWrapper (lazy init, health checks)
│   └── schema_registry.py    # Schema Registry client
├── fastmcp_tools/
│   ├── registration.py       # Tool registration
│   ├── common.py             # Shared models
│   ├── topic.py              # Topic tools
│   ├── message.py            # Message tools
│   ├── consumer_group.py     # Consumer group tools
│   └── cluster.py            # Cluster tools
├── middleware/
│   ├── safety.py             # Access control enforcement
│   ├── rate_limit.py
│   └── audit.py
├── safety/
│   └── core.py               # AccessEnforcer class
├── auth/                     # Uses authlib for OAuth
│   └── middleware.py         # AuthMiddleware (HTTP)
└── utils/
    ├── errors.py             # Exception hierarchy
    ├── validation.py         # Input validation
    └── logger.py             # Loguru setup

tests/
├── conftest.py
├── unit/                     # 80% coverage required
├── integration/              # Kafka container tests
└── fuzz/                     # ClusterFuzzLite

.github/workflows/            # CI with status checks
```

## Tools (12 total)

### Topic Management (3 tools)
| Tool | Access | Description |
|------|--------|-------------|
| `kafka_list_topics` | READ | List all topics with metadata |
| `kafka_describe_topic` | READ | Get detailed topic info and config |
| `kafka_create_topic` | READ/WRITE | Create a new topic |

### Message Operations (2 tools)
| Tool | Access | Description |
|------|--------|-------------|
| `kafka_consume_messages` | READ | Consume messages (read-only peek) |
| `kafka_produce_message` | READ/WRITE | Produce message to topic |

### Consumer Group Management (4 tools)
| Tool | Access | Description |
|------|--------|-------------|
| `kafka_list_consumer_groups` | READ | List all consumer groups |
| `kafka_describe_consumer_group` | READ | Get group details, members, lag |
| `kafka_get_consumer_lag` | READ | Get lag per partition |
| `kafka_reset_offsets` | READ/WRITE | Reset offsets to position |

### Cluster Info (3 tools)
| Tool | Access | Description |
|------|--------|-------------|
| `kafka_cluster_info` | READ | Get cluster metadata |
| `kafka_list_brokers` | READ | List all brokers |
| `kafka_get_watermarks` | READ | Get topic partition watermarks |

## Access Control

Simple 2-tier system:
```python
class AccessLevel(str, Enum):
    READ = "read"           # Read-only operations
    READ_WRITE = "read_write"  # State-changing operations

READ_WRITE_OPERATIONS = {
    "kafka_create_topic",
    "kafka_produce_message",
    "kafka_reset_offsets",
}
# All others are READ
```

### Validations
- Block internal topics (`__consumer_offsets`, `__transaction_state`)
- Block internal consumer groups (starting with `__`)
- Validate topic name format
- Enforce message size limits

## Configuration (Environment Variables)

```bash
# Kafka Connection
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL

# SASL Auth
KAFKA_SASL_MECHANISM=SCRAM-SHA-256
KAFKA_SASL_USERNAME=user
KAFKA_SASL_PASSWORD=pass

# mTLS
KAFKA_SSL_CA_LOCATION=/path/to/ca.crt
KAFKA_SSL_CERTIFICATE_LOCATION=/path/to/client.crt
KAFKA_SSL_KEY_LOCATION=/path/to/client.key

# Access Control
SAFETY_ALLOW_WRITE_OPERATIONS=false
SAFETY_MAX_CONSUME_MESSAGES=100

# Security
SECURITY_RATE_LIMIT_RPM=60
SECURITY_AUDIT_LOG_ENABLED=true
```

## Middleware Stack (outer to inner)
1. AuditMiddleware - Compliance trail
2. AuthMiddleware - OAuth via authlib (HTTP only)
3. SafetyMiddleware - READ/READ_WRITE enforcement
4. RateLimitMiddleware - Abuse prevention

## Dependencies

```toml
dependencies = [
    "fastmcp>=2.13.1",
    "confluent-kafka>=2.3.0",
    "pydantic>=2.12.4",
    "pydantic-settings>=2.12.0",
    "starlette>=0.50.0",
    "uvicorn>=0.38.0",
    "loguru>=0.7.3",
    "limits>=5.6.0",
    "authlib>=1.6.5",        # Trusted OAuth lib
    "httpx>=0.28.1",
    "typer>=0.20.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=9.0.0",
    "pytest-asyncio>=1.3.0",
    "pytest-cov>=4.1.0",
    "testcontainers>=4.0.0",
    "mypy>=1.8.0",
    "ruff>=0.14.5",
    "hypothesis>=6.0.0",
    "atheris>=2.3.0",
]
```

## Implementation Phases

### Phase 1: Foundation + CI
- Project structure, pyproject.toml, uv.lock
- Configuration system (KafkaConfig, SafetyConfig, SecurityConfig)
- KafkaClientWrapper with SASL/mTLS
- Exception hierarchy, logging
- Basic CLI entry point
- **Quality checks (all enforced as status checks)**:
  - `ruff check` - Linting
  - `ruff format --check` - Formatting
  - `mypy --strict` - Type checking
  - `pytest --cov-fail-under=80` - Unit tests with 80% coverage
  - Bandit - Python security linter
  - CodeQL - GitHub semantic security analysis
  - Dependency Review - Vulnerability + OpenSSF Scorecard check on PRs
  - License Compliance - Fail on GPL/AGPL/LGPL (MIT incompatible)
  - Pre-commit hooks (trailing-whitespace, end-of-file-fixer, check-yaml, check-json, check-toml, check-merge-conflict, detect-private-key, check-added-large-files)
- **Workflows**: ci.yml, bandit.yml, codeql.yml, dependency-review.yml, license-compliance.yml, pre-commit.yml
- **Files**: .pre-commit-config.yaml, .github/codeql/codeql-config.yml

### Phase 2: Access Control
- AccessLevel enum (READ, READ_WRITE)
- AccessEnforcer class
- Safety middleware
- Kafka-specific validations
- **Unit tests for access control**

### Phase 3: READ Tools (9 tools)
- Topic: list, describe
- Consumer Group: list, describe, lag
- Cluster: info, list_brokers, watermarks
- Message: consume
- **Unit tests for each tool**

### Phase 4: READ/WRITE Tools (3 tools)
- Topic: create
- Message: produce
- Consumer Group: reset_offsets
- **Unit tests for each tool**

### Phase 5: Middleware + Integration Tests
- Rate limiting (via limits lib)
- Audit logging
- OAuth middleware (via authlib)
- **Integration tests with Kafka container**
- **Fuzz tests**

### Phase 6: Documentation
- README, CONFIGURATION.md
- .env.example

## Testing Strategy

### Unit Tests (80% coverage required)
- Mock confluent-kafka clients
- Test all tool functions in isolation
- Test access control logic
- Test configuration parsing
- Test middleware behavior
- **Enforced via GitHub Actions status check**

### Integration Tests
- Use Testcontainers with Kafka
- Test actual Kafka operations
- Test authentication flows
- **Enforced via GitHub Actions status check**

### Fuzz Tests
- Fuzz input validation functions
- Fuzz topic name validation
- **Enforced via GitHub Actions (ClusterFuzzLite)**

## GitHub Actions Status Checks

All PRs must pass:

### Core Quality
1. `ruff check` - Linting
2. `ruff format --check` - Formatting
3. `mypy --strict` - Type checking
4. `pytest --cov-fail-under=80` - Unit tests with 80% coverage
5. Pre-commit hooks

### Security
6. Bandit - Python security scan
7. CodeQL - Semantic security analysis
8. Dependency Review - Vulnerability check (fail on moderate+)

### Compliance
9. License Compliance - Fail on GPL/AGPL/LGPL

### Later Phases
10. `pytest -m integration` - Integration tests (Phase 5)
11. Fuzz tests via ClusterFuzzLite (Phase 5)

## Future (v0.2.0+)
- Schema Registry tools
- Resource templates
- AI prompts
- Additional tools as needed
