"""MCP Kafka server entry point.

This module provides the main entry point for running the MCP Kafka server.
"""

import os
from enum import Enum
from pathlib import Path

import typer
from fastmcp import FastMCP

from mcp_kafka.config import Config
from mcp_kafka.fastmcp_tools.registration import register_tools
from mcp_kafka.kafka_wrapper import KafkaClientWrapper
from mcp_kafka.middleware.stack import MiddlewareStack
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.logger import get_logger, setup_logger
from mcp_kafka.version import __version__


class Transport(str, Enum):
    """Supported transport types."""

    stdio = "stdio"
    sse = "sse"


app = typer.Typer(
    name="mcp-kafka",
    help="MCP Kafka Server",
    add_completion=False,
)


def version_callback(value: bool) -> None:
    """Print version and exit."""
    if value:
        typer.echo(f"mcp-kafka {__version__}")
        raise typer.Exit()


def create_mcp_server(config: Config) -> tuple[FastMCP, KafkaClientWrapper, MiddlewareStack | None]:
    """Create and configure the MCP server.

    Args:
        config: Application configuration

    Returns:
        Tuple of (FastMCP server, Kafka client, Middleware stack)
    """
    # Create FastMCP server
    mcp = FastMCP(
        name=config.server.server_name,
    )

    # Create Kafka client
    kafka_client = KafkaClientWrapper(config.kafka)

    # Create access enforcer
    enforcer = AccessEnforcer(config.safety)

    # Register tools with middleware
    middleware = register_tools(mcp, kafka_client, enforcer, config.security)

    return mcp, kafka_client, middleware


@app.callback(invoke_without_command=True)
def main(
    transport: Transport = typer.Option(
        Transport.stdio,
        "--transport",
        help="Transport type (stdio or sse)",
    ),
    host: str = typer.Option(
        "127.0.0.1",
        "--host",
        help="Host to bind server (SSE transport only)",
    ),
    port: int = typer.Option(
        8000,
        "--port",
        help="Port to bind server (SSE transport only)",
    ),
    _version: bool = typer.Option(
        False,
        "--version",
        "-v",
        help="Show version and exit",
        callback=version_callback,
        is_eager=True,
    ),
    health_check: bool = typer.Option(
        False,
        "--health-check",
        help="Run health check and exit",
    ),
) -> None:
    """Run the MCP Kafka server with the specified transport."""
    # Load configuration
    config = Config()

    # Setup logging to file
    log_path = os.getenv("MCP_KAFKA_LOG_PATH")
    log_file = Path(log_path) if log_path else Path("mcp_kafka.log")
    setup_logger(config.server, log_file)

    logger = get_logger(__name__)
    logger.info("=" * 60)
    logger.info(f"MCP Kafka Server v{__version__}")
    logger.info("=" * 60)
    logger.info(f"Configuration: {config}")

    # Health check mode
    if health_check:
        logger.info("Running health check...")
        try:
            kafka_client = KafkaClientWrapper(config.kafka)
            health = kafka_client.health_check()
            typer.echo(f"Kafka cluster: {health['status']}")
            typer.echo(f"  Cluster ID: {health['cluster_id']}")
            typer.echo(f"  Brokers: {health['broker_count']}")
            typer.echo(f"  Topics: {health['topic_count']}")
            kafka_client.close()
        except Exception as e:
            typer.echo(f"Health check failed: {e}", err=True)
            raise typer.Exit(1) from e
        raise typer.Exit(0)

    # Create MCP server
    mcp, kafka_client, middleware = create_mcp_server(config)

    logger.info(f"Starting server with {transport.value} transport...")
    logger.info(f"Rate limiting: {'enabled' if config.security.rate_limit_enabled else 'disabled'}")
    logger.info(f"Audit logging: {'enabled' if config.security.audit_log_enabled else 'disabled'}")

    try:
        if transport == Transport.stdio:
            logger.info("Running in stdio mode...")
            mcp.run()
        elif transport == Transport.sse:
            logger.info(f"Running SSE server on {host}:{port}...")
            # Note: OAuth middleware would be added here for SSE transport
            # when config.security.oauth_enabled is True
            mcp.run(transport="sse", host=host, port=port)
    finally:
        # Cleanup
        if middleware:
            middleware.close()
        kafka_client.close()
        logger.info("Server shutdown complete")


if __name__ == "__main__":
    app()
