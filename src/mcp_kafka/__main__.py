"""MCP Kafka server entry point.

This module provides the main entry point for running the MCP Kafka server.
"""

import os
from enum import Enum
from pathlib import Path

import typer

from mcp_kafka.config import Config
from mcp_kafka.kafka_wrapper import KafkaClientWrapper
from mcp_kafka.utils.logger import get_logger, setup_logger
from mcp_kafka.version import __version__


class Transport(str, Enum):
    """Supported transport types."""

    stdio = "stdio"
    http = "http"


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


@app.callback(invoke_without_command=True)
def main(
    transport: Transport = typer.Option(
        Transport.stdio,
        "--transport",
        help="Transport type",
    ),
    host: str = typer.Option(
        "127.0.0.1",
        "--host",
        help="Host to bind server",
    ),
    port: int = typer.Option(
        8000,
        "--port",
        help="Port to bind server",
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

    # Server mode - placeholder until FastMCP server is implemented
    logger.info(f"Starting server with {transport} transport...")

    if transport == Transport.stdio:
        typer.echo("stdio transport not yet implemented")
        raise typer.Exit(1)
    if transport == Transport.http:
        typer.echo(f"HTTP transport on {host}:{port} not yet implemented")
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
