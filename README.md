# MCP Kafka

| Category | Status |
| --- | --- |
| **Build & CI** | [![CI](https://github.com/williajm/mcp_kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/williajm/mcp_kafka/actions/workflows/ci.yml) [![CodeQL](https://github.com/williajm/mcp_kafka/actions/workflows/codeql.yml/badge.svg)](https://github.com/williajm/mcp_kafka/actions/workflows/codeql.yml) [![Pre-commit](https://github.com/williajm/mcp_kafka/actions/workflows/pre-commit.yml/badge.svg)](https://github.com/williajm/mcp_kafka/actions/workflows/pre-commit.yml) [![Dependency Review](https://github.com/williajm/mcp_kafka/actions/workflows/dependency-review.yml/badge.svg)](https://github.com/williajm/mcp_kafka/actions/workflows/dependency-review.yml) [![License Compliance](https://github.com/williajm/mcp_kafka/actions/workflows/license-compliance.yml/badge.svg)](https://github.com/williajm/mcp_kafka/actions/workflows/license-compliance.yml) |
| **SonarCloud** | [![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=williajm_mcp_kafka&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=williajm_mcp_kafka) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=williajm_mcp_kafka&metric=coverage)](https://sonarcloud.io/summary/new_code?id=williajm_mcp_kafka) [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=williajm_mcp_kafka&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=williajm_mcp_kafka) [![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=williajm_mcp_kafka&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=williajm_mcp_kafka) [![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=williajm_mcp_kafka&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=williajm_mcp_kafka) |
| **Security** | [![Bandit](https://github.com/williajm/mcp_kafka/actions/workflows/bandit.yml/badge.svg)](https://github.com/williajm/mcp_kafka/actions/workflows/bandit.yml) [![Dependabot](https://img.shields.io/badge/Dependabot-enabled-blue.svg?logo=dependabot)](https://github.com/williajm/mcp_kafka/security/dependabot) |
| **Technology** | [![Python 3.11-3.14](https://img.shields.io/badge/python-3.11--3.14-blue.svg)](https://www.python.org/downloads/) [![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20.svg?logo=apachekafka&logoColor=white)](https://kafka.apache.org/) [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT) [![Code style: ruff](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff) [![type-checked: mypy](https://img.shields.io/badge/type--checked-mypy-blue.svg)](https://mypy-lang.org/) [![MCP](https://img.shields.io/badge/MCP-1.2.0+-5865F2.svg)](https://modelcontextprotocol.io) |

A Model Context Protocol (MCP) server for Apache Kafka that provides AI assistants with safe, controlled access to Kafka clusters.

## Features

- **12 Kafka tools** for topic management, message operations, consumer groups, and cluster info
- **2-tier access control**: READ (default) and READ/WRITE modes
- **Universal Kafka support**: SASL (PLAIN, SCRAM, GSSAPI) and mTLS authentication
- **Safety controls**: Protected internal topics, consumer group validation, message size limits
- **Built with FastMCP**: Modern MCP server implementation

## Installation

```bash
# Using uv (recommended)
uv add mcp-kafka

# Using pip
pip install mcp-kafka
```

## Quick Start

### 1. Configure Environment

```bash
# Basic connection
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092

# SASL authentication
export KAFKA_SECURITY_PROTOCOL=SASL_SSL
export KAFKA_SASL_MECHANISM=SCRAM-SHA-256
export KAFKA_SASL_USERNAME=your-username
export KAFKA_SASL_PASSWORD=your-password

# Enable write operations (disabled by default)
export SAFETY_ALLOW_WRITE_OPERATIONS=true
```

### 2. Run the Server

```bash
# stdio transport (default, for MCP clients)
uv run mcp-kafka

# HTTP transport (for web integrations)
uv run mcp-kafka --transport http --host 127.0.0.1 --port 8000

# Using convenience scripts
./scripts/http-read.sh      # Read-only HTTP server
./scripts/http-readwrite.sh # Read-write HTTP server
```

#### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--transport` | `stdio` | Transport type: `stdio` or `http` |
| `--host` | `127.0.0.1` | Host to bind (HTTP only) |
| `--port` | `8000` | Port to bind (HTTP only) |
| `--health-check` | - | Run health check and exit |
| `--version`, `-v` | - | Show version and exit |

### 3. Connect to MCP Client

Add to your MCP client configuration (e.g., Claude Desktop):

```json
{
  "mcpServers": {
    "kafka": {
      "command": "uv",
      "args": ["run", "mcp-kafka"],
      "env": {
        "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092"
      }
    }
  }
}
```

## Available Tools

### Topic Management

| Tool | Access | Description |
|------|--------|-------------|
| `kafka_list_topics` | READ | List all topics with partition counts |
| `kafka_describe_topic` | READ | Get detailed topic info and configuration |
| `kafka_create_topic` | READ/WRITE | Create a new topic with partitions, replication factor, and config |

### Message Operations

| Tool | Access | Description |
|------|--------|-------------|
| `kafka_consume_messages` | READ | Peek at messages (no offset commit) |
| `kafka_produce_message` | READ/WRITE | Produce a message with optional key, headers, and partition |

### Consumer Group Management

| Tool | Access | Description |
|------|--------|-------------|
| `kafka_list_consumer_groups` | READ | List all consumer groups |
| `kafka_describe_consumer_group` | READ | Get group details, members, and lag |
| `kafka_get_consumer_lag` | READ | Get lag per partition |
| `kafka_reset_offsets` | READ/WRITE | Reset consumer group offsets to earliest, latest, or specific offset |

### Cluster Information

| Tool | Access | Description |
|------|--------|-------------|
| `kafka_cluster_info` | READ | Get cluster metadata |
| `kafka_list_brokers` | READ | List all brokers |
| `kafka_get_watermarks` | READ | Get topic partition watermarks |

## Configuration

For detailed configuration options and examples, see [CONFIGURATION.md](CONFIGURATION.md).

### Environment Variables

#### Kafka Connection

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker addresses |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Security protocol (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL) |
| `KAFKA_CLIENT_ID` | `mcp-kafka` | Client identifier |
| `KAFKA_TIMEOUT` | `30` | Operation timeout in seconds |

#### SASL Authentication

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_SASL_MECHANISM` | - | SASL mechanism (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI) |
| `KAFKA_SASL_USERNAME` | - | SASL username |
| `KAFKA_SASL_PASSWORD` | - | SASL password |
| `KAFKA_SASL_KERBEROS_SERVICE_NAME` | `kafka` | Kerberos service name |
| `KAFKA_SASL_KERBEROS_KEYTAB` | - | Path to Kerberos keytab |
| `KAFKA_SASL_KERBEROS_PRINCIPAL` | - | Kerberos principal |

#### SSL/TLS

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_SSL_CA_LOCATION` | - | CA certificate path |
| `KAFKA_SSL_CERTIFICATE_LOCATION` | - | Client certificate path |
| `KAFKA_SSL_KEY_LOCATION` | - | Client key path |
| `KAFKA_SSL_KEY_PASSWORD` | - | Client key password |

#### Safety Controls

| Variable | Default | Description |
|----------|---------|-------------|
| `SAFETY_ALLOW_WRITE_OPERATIONS` | `false` | Enable READ/WRITE tools |
| `SAFETY_MAX_MESSAGE_SIZE` | `1048576` | Max message size in bytes (1MB) |
| `SAFETY_MAX_CONSUME_MESSAGES` | `100` | Max messages per consume request |
| `SAFETY_TOPIC_BLOCKLIST` | - | Comma-separated blocked topic patterns |

#### Security Controls

| Variable | Default | Description |
|----------|---------|-------------|
| `SECURITY_RATE_LIMIT_ENABLED` | `true` | Enable rate limiting |
| `SECURITY_RATE_LIMIT_RPM` | `60` | Max requests per minute |
| `SECURITY_AUDIT_LOG_ENABLED` | `true` | Enable audit logging |
| `SECURITY_AUDIT_LOG_FILE` | `mcp_audit.log` | Audit log file path |
| `SECURITY_OAUTH_ENABLED` | `false` | Enable OAuth/OIDC authentication |
| `SECURITY_OAUTH_ISSUER` | - | OAuth issuer URL (e.g., https://auth.example.com) |
| `SECURITY_OAUTH_AUDIENCE` | - | Expected JWT audience claim |
| `SECURITY_OAUTH_JWKS_URL` | - | JWKS endpoint URL (auto-derived from issuer if not set) |

## Access Control

MCP Kafka uses a simple 2-tier access control system:

### READ Access (Default)

- List topics, consumer groups, and brokers
- Describe topics and consumer groups
- Consume messages (read-only peek)
- Get cluster info and watermarks

### READ/WRITE Access

Requires `SAFETY_ALLOW_WRITE_OPERATIONS=true`:

- Create topics
- Produce messages
- Reset consumer group offsets

### Protected Resources

The following resources are always protected:

- **Internal topics**: `__consumer_offsets`, `__transaction_state`
- **Internal consumer groups**: Groups starting with `__`
- **Topics in blocklist**: Configured via `SAFETY_TOPIC_BLOCKLIST`

## Development

### Prerequisites

- Python 3.11+
- uv package manager

### Setup

```bash
# Clone repository
git clone https://github.com/williajm/mcp_kafka.git
cd mcp_kafka

# Install dependencies
uv sync --all-extras

# Run tests
uv run pytest

# Run linting
uv run ruff check src/ tests/
uv run ruff format --check src/ tests/

# Run type checking
uv run mypy src/
```

### Local Kafka for Testing

A Docker Compose environment is provided for local development:

```bash
# Start Kafka
docker compose -f docker/docker-compose.yml up -d

# Create test topics
docker compose -f docker/docker-compose.yml exec kafka \
  kafka-topics --create --topic test-topic --partitions 3 --replication-factor 1 \
  --bootstrap-server localhost:9092

# Stop Kafka
docker compose -f docker/docker-compose.yml down
```

## License

MIT License - see LICENSE file for details.
