#!/bin/bash
# Seed Kafka with test topics and sample data

set -e

echo "🌱 Seeding Kafka with test data..."

docker compose --profile seed up kafka-producer

echo ""
echo "✅ Test data created!"
echo ""
echo "Topics created:"
echo "  - orders (3 partitions, 20 messages)"
echo "  - users (2 partitions, 10 messages)"
echo "  - events (4 partitions, 30 messages)"
echo "  - logs (1 partition, 15 messages)"
echo ""
echo "View in Kafka UI: http://localhost:8080"
