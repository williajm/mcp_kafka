#!/bin/bash
# Build the MCP Kafka Docker image

set -e

VERSION=${1:-latest}
IMAGE_NAME="mcp-kafka"

echo "🐳 Building MCP Kafka Docker image..."
echo "   Version: $VERSION"
echo ""

docker build -t "$IMAGE_NAME:$VERSION" .

if [[ "$VERSION" != "latest" ]]; then
    docker tag "$IMAGE_NAME:$VERSION" "$IMAGE_NAME:latest"
fi

echo ""
echo "✅ Image built: $IMAGE_NAME:$VERSION"
echo ""
echo "Run with:"
echo "  docker run --rm -it --network host -e KAFKA_BOOTSTRAP_SERVERS=localhost:9094 $IMAGE_NAME --health-check"
