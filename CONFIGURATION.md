# MCP Kafka Configuration Guide

This document provides detailed configuration information for MCP Kafka.

## Table of Contents

- [Quick Start](#quick-start)
- [Configuration Methods](#configuration-methods)
- [Kafka Connection](#kafka-connection)
- [Authentication](#authentication)
- [Safety Controls](#safety-controls)
- [Security Controls](#security-controls)
- [Server Configuration](#server-configuration)
- [Configuration Examples](#configuration-examples)

## Quick Start

1. Copy `.env.example` to `.env`
2. Set `KAFKA_BOOTSTRAP_SERVERS` to your Kafka broker addresses
3. Configure authentication if required (SASL/SSL)
4. Run `uv run mcp-kafka`

## Configuration Methods

MCP Kafka supports configuration via:

1. **Environment variables** (recommended for production)
2. **`.env` file** (recommended for development)
3. **Both** (environment variables override `.env` values)

## Kafka Connection

### Basic Connection

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated list of broker addresses |
| `KAFKA_CLIENT_ID` | `mcp-kafka` | Client identifier for connections |
| `KAFKA_TIMEOUT` | `30` | Operation timeout in seconds |
| `KAFKA_SECURITY_PROTOCOL` | `PLAINTEXT` | Security protocol (see below) |

### Security Protocols

| Protocol | Description | Requirements |
|----------|-------------|--------------|
| `PLAINTEXT` | No encryption or authentication | None |
| `SSL` | TLS encryption, optional client certs | SSL certificates |
| `SASL_PLAINTEXT` | SASL authentication, no encryption | SASL credentials |
| `SASL_SSL` | SASL authentication with TLS | Both SASL and SSL |

## Authentication

### SASL Authentication

Configure SASL for username/password or Kerberos authentication.

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_SASL_MECHANISM` | - | `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`, or `GSSAPI` |
| `KAFKA_SASL_USERNAME` | - | Username for PLAIN/SCRAM |
| `KAFKA_SASL_PASSWORD` | - | Password for PLAIN/SCRAM |

#### SCRAM Authentication Example

```bash
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=SCRAM-SHA-256
KAFKA_SASL_USERNAME=myuser
KAFKA_SASL_PASSWORD=mypassword
```

### Kerberos (GSSAPI)

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_SASL_KERBEROS_SERVICE_NAME` | `kafka` | Kerberos service name |
| `KAFKA_SASL_KERBEROS_KEYTAB` | - | Path to keytab file |
| `KAFKA_SASL_KERBEROS_PRINCIPAL` | - | Kerberos principal |

#### Kerberos Authentication Example

```bash
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=GSSAPI
KAFKA_SASL_KERBEROS_SERVICE_NAME=kafka
KAFKA_SASL_KERBEROS_PRINCIPAL=kafka-client@EXAMPLE.COM
KAFKA_SASL_KERBEROS_KEYTAB=/etc/security/keytabs/kafka.keytab
```

### SSL/TLS Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_SSL_CA_LOCATION` | - | CA certificate path |
| `KAFKA_SSL_CERTIFICATE_LOCATION` | - | Client certificate path (for mTLS) |
| `KAFKA_SSL_KEY_LOCATION` | - | Client private key path (for mTLS) |
| `KAFKA_SSL_KEY_PASSWORD` | - | Private key password |

#### mTLS Authentication Example

```bash
KAFKA_SECURITY_PROTOCOL=SSL
KAFKA_SSL_CA_LOCATION=/certs/ca.crt
KAFKA_SSL_CERTIFICATE_LOCATION=/certs/client.crt
KAFKA_SSL_KEY_LOCATION=/certs/client.key
KAFKA_SSL_KEY_PASSWORD=keypassword
```

## Safety Controls

Safety controls protect against accidental data modification and enforce operational limits.

### Access Control

| Variable | Default | Description |
|----------|---------|-------------|
| `SAFETY_ALLOW_WRITE_OPERATIONS` | `false` | Enable write tools (create topic, produce, reset offsets) |

When `false` (default), only READ tools are available:
- `kafka_list_topics`, `kafka_describe_topic`
- `kafka_consume_messages`
- `kafka_list_consumer_groups`, `kafka_describe_consumer_group`, `kafka_get_consumer_lag`
- `kafka_cluster_info`, `kafka_list_brokers`, `kafka_get_watermarks`

When `true`, additional READ/WRITE tools are enabled:
- `kafka_create_topic`
- `kafka_produce_message`
- `kafka_reset_offsets`

### Operational Limits

| Variable | Default | Range | Description |
|----------|---------|-------|-------------|
| `SAFETY_MAX_MESSAGE_SIZE` | `1048576` (1MB) | 1 - 10485760 | Max message size for produce |
| `SAFETY_MAX_CONSUME_MESSAGES` | `100` | 1 - 10000 | Max messages per consume request |

### Topic Protection

| Variable | Default | Description |
|----------|---------|-------------|
| `SAFETY_TOPIC_BLOCKLIST` | `__consumer_offsets,__transaction_state` | Topics blocked from all operations |

Internal topics (`__consumer_offsets`, `__transaction_state`) are always blocked regardless of this setting.

Example:
```bash
SAFETY_TOPIC_BLOCKLIST=sensitive-data,audit-*,internal-*
```

### Tool Filtering

Control which tools are available to clients:

| Variable | Default | Description |
|----------|---------|-------------|
| `SAFETY_ALLOWED_TOOLS` | - | Whitelist of allowed tool names (comma-separated) |
| `SAFETY_DENIED_TOOLS` | - | Blacklist of denied tool names (comma-separated) |

Rules:
- If `SAFETY_ALLOWED_TOOLS` is set, only those tools are available
- `SAFETY_DENIED_TOOLS` takes precedence over `SAFETY_ALLOWED_TOOLS`
- Access level restrictions still apply (write tools need `SAFETY_ALLOW_WRITE_OPERATIONS=true`)

Example - Allow only read operations on topics:
```bash
SAFETY_ALLOWED_TOOLS=kafka_list_topics,kafka_describe_topic
```

Example - Allow everything except producing messages:
```bash
SAFETY_DENIED_TOOLS=kafka_produce_message
```

## Security Controls

### Rate Limiting

Protect against abuse with request rate limiting:

| Variable | Default | Range | Description |
|----------|---------|-------|-------------|
| `SECURITY_RATE_LIMIT_ENABLED` | `true` | - | Enable rate limiting |
| `SECURITY_RATE_LIMIT_RPM` | `60` | 1 - 1000 | Max requests per minute |

Rate limiting uses a sliding window algorithm. When the limit is exceeded, requests return an error until the window passes.

### Audit Logging

Track all operations for compliance and debugging:

| Variable | Default | Description |
|----------|---------|-------------|
| `SECURITY_AUDIT_LOG_ENABLED` | `true` | Enable audit logging |
| `SECURITY_AUDIT_LOG_FILE` | `mcp_audit.log` | Audit log file path |

Audit logs are JSON-formatted and include:
- Timestamp
- Tool name and parameters
- Client information
- Success/failure status
- Execution duration

Sensitive data (passwords, message content) is automatically redacted.

### Network Access Control

Restrict which IP addresses can connect:

| Variable | Default | Description |
|----------|---------|-------------|
| `SECURITY_ALLOWED_CLIENT_IPS` | - | Comma-separated allowed IPs/CIDRs |

When empty (default), all IPs are allowed. Supports individual IPs and CIDR notation:

```bash
SECURITY_ALLOWED_CLIENT_IPS=127.0.0.1,10.0.0.0/8,192.168.1.0/24
```

### OAuth/OIDC Authentication

Enable OAuth authentication for HTTP transport:

| Variable | Default | Description |
|----------|---------|-------------|
| `SECURITY_OAUTH_ENABLED` | `false` | Enable OAuth authentication |
| `SECURITY_OAUTH_ISSUER` | - | OAuth issuer URL (required if enabled) |
| `SECURITY_OAUTH_JWKS_URL` | - | JWKS endpoint (auto-discovered if not set) |
| `SECURITY_OAUTH_AUDIENCE` | - | Expected JWT audience claim(s) |

When enabled, clients must provide a valid JWT bearer token. The token is validated against the JWKS endpoint.

Example with Auth0:
```bash
SECURITY_OAUTH_ENABLED=true
SECURITY_OAUTH_ISSUER=https://mycompany.auth0.com/
SECURITY_OAUTH_AUDIENCE=mcp-kafka-api
```

Example with Keycloak:
```bash
SECURITY_OAUTH_ENABLED=true
SECURITY_OAUTH_ISSUER=https://keycloak.example.com/realms/myrealm
SECURITY_OAUTH_AUDIENCE=mcp-kafka
```

## Server Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `MCP_SERVER_NAME` | `mcp-kafka` | Server name in MCP protocol |
| `MCP_LOG_LEVEL` | `INFO` | Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `MCP_JSON_LOGGING` | `false` | Enable JSON structured logging |
| `MCP_DEBUG_MODE` | `false` | Show detailed errors in responses |
| `MCP_KAFKA_LOG_PATH` | `mcp_kafka.log` | Path to application log file |

### HTTP Transport

These variables are used by the convenience scripts (`scripts/http-read.sh`, `scripts/http-readwrite.sh`):

| Variable | Default | Description |
|----------|---------|-------------|
| `MCP_HTTP_HOST` | `127.0.0.1` | HTTP server bind address |
| `MCP_HTTP_PORT` | `8000` | HTTP server port |

### Logging

Standard logging (default):
```
2024-01-15 10:30:45 | INFO     | mcp_kafka.server:handle:123 - Processing request
```

JSON logging (`MCP_JSON_LOGGING=true`):
```json
{"timestamp": "2024-01-15T10:30:45Z", "level": "INFO", "logger": "mcp_kafka.server", "message": "Processing request"}
```

## Configuration Examples

### Development (Local Kafka)

```bash
# .env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
SAFETY_ALLOW_WRITE_OPERATIONS=true
MCP_LOG_LEVEL=DEBUG
MCP_DEBUG_MODE=true
```

### Production (Confluent Cloud)

```bash
# .env
KAFKA_BOOTSTRAP_SERVERS=pkc-xxxxx.region.provider.confluent.cloud:9092
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=PLAIN
KAFKA_SASL_USERNAME=<api-key>
KAFKA_SASL_PASSWORD=<api-secret>

SAFETY_ALLOW_WRITE_OPERATIONS=false
SAFETY_MAX_CONSUME_MESSAGES=50

SECURITY_RATE_LIMIT_RPM=30
SECURITY_AUDIT_LOG_FILE=/var/log/mcp-kafka/audit.log

MCP_LOG_LEVEL=INFO
MCP_JSON_LOGGING=true
```

### Production (AWS MSK with IAM)

```bash
# .env
KAFKA_BOOTSTRAP_SERVERS=b-1.msk-cluster.region.amazonaws.com:9098
KAFKA_SECURITY_PROTOCOL=SASL_SSL
KAFKA_SASL_MECHANISM=AWS_MSK_IAM

SAFETY_ALLOW_WRITE_OPERATIONS=false
SECURITY_ALLOWED_CLIENT_IPS=10.0.0.0/8
```

### Restricted Access (Read-only, Limited Tools)

```bash
# .env
KAFKA_BOOTSTRAP_SERVERS=kafka.example.com:9092

SAFETY_ALLOW_WRITE_OPERATIONS=false
SAFETY_ALLOWED_TOOLS=kafka_list_topics,kafka_describe_topic,kafka_get_consumer_lag
SAFETY_TOPIC_BLOCKLIST=production-*,pii-*

SECURITY_RATE_LIMIT_RPM=10
```

### OAuth-Protected API

```bash
# .env
KAFKA_BOOTSTRAP_SERVERS=kafka.example.com:9092

SECURITY_OAUTH_ENABLED=true
SECURITY_OAUTH_ISSUER=https://auth.example.com
SECURITY_OAUTH_AUDIENCE=mcp-kafka
SECURITY_ALLOWED_CLIENT_IPS=10.0.0.0/8

MCP_JSON_LOGGING=true
```

### HTTP Transport (Development)

```bash
# Start HTTP server with environment variables
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
MCP_HTTP_HOST=127.0.0.1 \
MCP_HTTP_PORT=8000 \
./scripts/http-read.sh

# Or with write access
KAFKA_BOOTSTRAP_SERVERS=localhost:9092 \
./scripts/http-readwrite.sh
```
