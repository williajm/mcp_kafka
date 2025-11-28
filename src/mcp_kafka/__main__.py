"""MCP Kafka server entry point.

This module provides the main entry point for running the MCP Kafka server.
"""

import asyncio
import os
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING

import typer
from fastmcp import FastMCP
from starlette.middleware import Middleware

from mcp_kafka.auth.middleware import OAuthMiddleware
from mcp_kafka.config import Config, SecurityConfig
from mcp_kafka.fastmcp_tools.registration import register_tools
from mcp_kafka.kafka_wrapper import KafkaClientWrapper
from mcp_kafka.middleware.debug_logging import create_debug_logging_middleware
from mcp_kafka.middleware.stack import MiddlewareStack
from mcp_kafka.safety.core import AccessEnforcer
from mcp_kafka.utils.logger import get_logger, setup_logger
from mcp_kafka.version import __version__

if TYPE_CHECKING:
    from loguru import Logger


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


def create_mcp_server(
    config: Config,
) -> tuple[FastMCP, KafkaClientWrapper, MiddlewareStack | None]:
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

    # Add debug logging middleware (logs all MCP calls at DEBUG level)
    debug_middleware = create_debug_logging_middleware()
    mcp.add_middleware(debug_middleware)

    # Create Kafka client
    kafka_client = KafkaClientWrapper(config.kafka)

    # Create access enforcer
    enforcer = AccessEnforcer(config.safety)

    # Register tools with middleware
    middleware = register_tools(mcp, kafka_client, enforcer, config.security)

    return mcp, kafka_client, middleware


def run_health_check(config: Config, logger: "Logger") -> None:
    """Execute Kafka health check and exit.

    Args:
        config: Application configuration
        logger: Logger instance

    Raises:
        typer.Exit: Always exits after health check
    """
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


def create_http_middleware(security_config: SecurityConfig) -> list[Middleware]:
    """Create HTTP middleware stack for the server.

    Args:
        security_config: Security configuration

    Returns:
        List of Starlette Middleware instances
    """
    middleware_list: list[Middleware] = []

    # Add OAuth middleware if enabled
    if security_config.oauth_enabled:
        middleware_list.append(Middleware(OAuthMiddleware, config=security_config))

    return middleware_list


def warn_insecure_http_config(host: str, security_config: SecurityConfig, logger: "Logger") -> None:
    """Log warnings for potentially insecure HTTP configurations.

    Args:
        host: Host address the server will bind to
        security_config: Security configuration
        logger: Logger instance
    """
    is_localhost = host in ("127.0.0.1", "localhost", "::1")

    if not is_localhost:
        logger.warning("=" * 60)
        logger.warning("SECURITY WARNING: Server binding to non-localhost address")
        logger.warning(f"  Host: {host}")

        if not security_config.oauth_enabled:
            logger.warning("  OAuth authentication is DISABLED")
            logger.warning("  Anyone with network access can use all tools!")
            logger.warning("  Consider enabling OAuth or using a reverse proxy")

        logger.warning("  Traffic is unencrypted (no TLS)")
        logger.warning("  Consider using a TLS-terminating reverse proxy")
        logger.warning("=" * 60)


@app.callback(invoke_without_command=True)
def main(
    transport: Transport = typer.Option(
        Transport.stdio,
        "--transport",
        help="Transport type (stdio or http)",
    ),
    host: str = typer.Option(
        "127.0.0.1",
        "--host",
        help="Host to bind server (HTTP transport only)",
    ),
    port: int = typer.Option(
        8000,
        "--port",
        help="Port to bind server (HTTP transport only)",
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
        run_health_check(config, logger)

    # Create MCP server
    mcp, kafka_client, middleware = create_mcp_server(config)

    logger.info(f"Starting server with {transport.value} transport...")
    logger.info(f"Rate limiting: {'enabled' if config.security.rate_limit_enabled else 'disabled'}")
    logger.info(f"Audit logging: {'enabled' if config.security.audit_log_enabled else 'disabled'}")

    try:
        if transport == Transport.stdio:
            logger.info("Running in stdio mode...")
            mcp.run()
        elif transport == Transport.http:
            # Warn about insecure configurations
            warn_insecure_http_config(host, config.security, logger)

            # Build HTTP middleware stack (OAuth if enabled)
            http_middleware = create_http_middleware(config.security)

            logger.info(f"Running HTTP server on {host}:{port}...")
            logger.info(f"OAuth: {'enabled' if config.security.oauth_enabled else 'disabled'}")

            # Use run_http_async with middleware support
            # The CLI option "http" maps to FastMCP's "streamable-http" transport,
            # which is the modern MCP HTTP transport (SSE is deprecated)
            asyncio.run(
                mcp.run_http_async(
                    transport="streamable-http",
                    host=host,
                    port=port,
                    middleware=http_middleware if http_middleware else None,
                )
            )
    finally:
        # Cleanup
        if middleware:
            middleware.close()
        kafka_client.close()
        logger.info("Server shutdown complete")


if __name__ == "__main__":
    app()
