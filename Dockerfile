# Multi-stage build for minimal image size
FROM python:3.14-slim AS builder

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency files and README (required by pyproject.toml)
COPY pyproject.toml uv.lock README.md ./

# Install dependencies into a virtual environment
RUN uv sync --frozen --no-dev --no-install-project

# Copy source code
COPY src/ src/

# Install the project
RUN uv sync --frozen --no-dev


# Runtime stage
FROM python:3.14-slim AS runtime

# Security: run as non-root user
RUN groupadd --gid 1000 mcp && \
    useradd --uid 1000 --gid 1000 --shell /bin/bash --create-home mcp

WORKDIR /app

# Copy virtual environment from builder
COPY --from=builder /app/.venv /app/.venv

# Set PATH to use venv
ENV PATH="/app/.venv/bin:$PATH"
ENV PYTHONUNBUFFERED=1

# Default Kafka connection (override with env vars)
ENV KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
ENV KAFKA_CLIENT_ID="mcp-kafka"
ENV MCP_LOG_LEVEL="INFO"

# Switch to non-root user
USER mcp

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "from mcp_kafka.config import Config; Config()" || exit 1

# Default command - stdio transport for MCP
ENTRYPOINT ["mcp-kafka"]
CMD ["--transport", "stdio"]

# Labels
LABEL org.opencontainers.image.title="MCP Kafka Server"
LABEL org.opencontainers.image.description="Model Context Protocol server for Apache Kafka"
LABEL org.opencontainers.image.source="https://github.com/williajm/mcp_kafka"
LABEL org.opencontainers.image.licenses="MIT"
