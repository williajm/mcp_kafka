#!/bin/bash
# Start continuous message production for live testing

set -e

echo "📡 Starting live message producer..."
echo "   Press Ctrl+C to stop"
echo ""

docker compose --profile live up kafka-live-producer
