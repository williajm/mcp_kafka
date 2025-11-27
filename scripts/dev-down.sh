#!/bin/bash
# Stop the local Kafka development environment

set -e

echo "🛑 Stopping Kafka development environment..."

docker compose --profile seed --profile live down

echo "✅ All services stopped."
echo ""
echo "To also remove volumes (all data): docker compose down -v"
