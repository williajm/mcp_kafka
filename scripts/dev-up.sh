#!/bin/bash
# Start the local Kafka development environment

set -e

echo "🚀 Starting Kafka development environment..."

# Start core services
docker compose up -d kafka kafka-ui

echo "⏳ Waiting for Kafka to be healthy..."
until docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    sleep 2
done

echo ""
echo "✅ Kafka is ready!"
echo ""
echo "Services:"
echo "  - Kafka Broker:  localhost:9094 (external) / kafka:9092 (internal)"
echo "  - Kafka UI:      http://localhost:8080"
echo ""
echo "Quick commands:"
echo "  ./scripts/seed-data.sh     - Create test topics and sample data"
echo "  ./scripts/live-producer.sh - Start continuous message production"
echo "  ./scripts/dev-down.sh      - Stop all services"
echo ""
echo "Test MCP server health check:"
echo "  KAFKA_BOOTSTRAP_SERVERS=localhost:9094 uv run mcp-kafka --health-check"
