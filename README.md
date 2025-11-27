# MCP Kafka

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
# Using uv
uv run mcp-kafka

# Using Python
python -m mcp_kafka
```

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
