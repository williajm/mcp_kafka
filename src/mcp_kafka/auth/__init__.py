"""Authentication for MCP Kafka server."""

from mcp_kafka.auth.middleware import (
    JWKSClient,
    OAuthMiddleware,
    OAuthValidator,
    TokenClaims,
    create_oauth_middleware,
)

__all__ = [
    "JWKSClient",
    "OAuthMiddleware",
    "OAuthValidator",
    "TokenClaims",
    "create_oauth_middleware",
]
