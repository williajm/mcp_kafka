#!/bin/bash
# Start MCP Kafka HTTP server in READ-WRITE mode
# This server can perform all operations including writes

set -e

# Default configuration
HOST="${MCP_HTTP_HOST:-127.0.0.1}"
PORT="${MCP_HTTP_PORT:-8000}"

# Logging configuration
export MCP_LOG_LEVEL="${MCP_LOG_LEVEL:-INFO}"
export SECURITY_AUDIT_LOG_ENABLED="${SECURITY_AUDIT_LOG_ENABLED:-true}"
export SECURITY_AUDIT_LOG_FILE="${SECURITY_AUDIT_LOG_FILE:-./mcp_audit.log}"

echo "Starting MCP Kafka HTTP Server (READ-WRITE mode)"
echo "================================================="
echo "  Host: $HOST"
echo "  Port: $PORT"
echo "  Kafka: ${KAFKA_BOOTSTRAP_SERVERS:-localhost:9094}"
echo "  Log level: $MCP_LOG_LEVEL"
echo "  Audit log: $SECURITY_AUDIT_LOG_FILE"
echo ""
echo "Available tools: ALL operations"
echo "  READ:"
echo "    - kafka_cluster_info"
echo "    - kafka_list_brokers"
echo "    - kafka_list_topics"
echo "    - kafka_describe_topic"
echo "    - kafka_get_watermarks"
echo "    - kafka_list_consumer_groups"
echo "    - kafka_describe_consumer_group"
echo "    - kafka_get_consumer_lag"
echo "    - kafka_consume_messages"
echo "  WRITE:"
echo "    - kafka_create_topic"
echo "    - kafka_produce_message"
echo "    - kafka_reset_offsets"
echo ""
echo "WARNING: Write operations are enabled!"
echo ""

# Enable write operations
export SAFETY_ALLOW_WRITE_OPERATIONS=true

# Start the server
exec uv run mcp-kafka \
    --transport http \
    --host "$HOST" \
    --port "$PORT"
