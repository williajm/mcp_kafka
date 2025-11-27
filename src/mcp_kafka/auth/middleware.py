"""OAuth/OIDC authentication middleware for MCP Kafka server.

This middleware validates JWT tokens from the Authorization header
using the configured OAuth/OIDC provider's JWKS endpoint.
"""

import ipaddress
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlparse

import httpx
from authlib.jose import JsonWebKey, jwt
from authlib.jose.errors import JoseError
from loguru import logger
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from mcp_kafka.config import SecurityConfig
from mcp_kafka.utils.errors import AuthenticationError, AuthorizationError

# Constants
BEARER_TOKEN_PARTS = 2  # "Bearer" + token
MAX_JWKS_RESPONSE_SIZE = 1024 * 1024  # 1MB max JWKS response
ALLOWED_JWT_ALGORITHMS = ["RS256", "RS384", "RS512", "ES256", "ES384", "ES512"]

# Private IP ranges to block for SSRF protection
# These are blocklisted ranges, not connection targets
PRIVATE_IP_RANGES = [
    ipaddress.ip_network("0.0.0.0/8"),  # "This" network (RFC 5735)  # NOSONAR
    ipaddress.ip_network("10.0.0.0/8"),  # NOSONAR
    ipaddress.ip_network("172.16.0.0/12"),  # NOSONAR
    ipaddress.ip_network("192.168.0.0/16"),  # NOSONAR
    ipaddress.ip_network("169.254.0.0/16"),  # Link-local  # NOSONAR
    ipaddress.ip_network("127.0.0.0/8"),  # Loopback
]


@dataclass
class TokenClaims:
    """Parsed and validated JWT token claims."""

    subject: str
    issuer: str
    audience: list[str]
    expires_at: int
    issued_at: int
    raw_claims: dict[str, Any]


def _is_private_ip(hostname: str) -> bool:
    """Check if a hostname resolves to a private IP address.

    Args:
        hostname: Hostname to check

    Returns:
        True if the hostname is a private IP or resolves to one
    """
    try:
        # Check if it's already an IP address
        ip = ipaddress.ip_address(hostname)
        return any(ip in network for network in PRIVATE_IP_RANGES)
    except ValueError:
        # Not an IP address, could be a hostname
        # For hostnames, we can't resolve without making a network call
        # Block known dangerous hostnames (IPs are handled by network ranges above)
        dangerous_hosts = {"localhost", "metadata.google.internal"}
        return hostname.lower() in dangerous_hosts


def _validate_jwks_url(url: str) -> None:
    """Validate JWKS URL to prevent SSRF attacks.

    Args:
        url: URL to validate

    Raises:
        AuthenticationError: If URL is potentially dangerous
    """
    parsed = urlparse(url)

    # Only allow HTTPS (except for testing with localhost)
    if parsed.scheme not in ("https", "http"):
        raise AuthenticationError(f"JWKS URL must use HTTPS: {url}")

    # Check for private IPs
    hostname = parsed.hostname or ""
    if _is_private_ip(hostname):
        raise AuthenticationError(f"JWKS URL cannot point to private IP ranges: {url}")


class JWKSClient:
    """Client for fetching and caching JWKS from OAuth provider."""

    def __init__(self, jwks_url: str, cache_ttl: int = 3600) -> None:
        """Initialize JWKS client.

        Args:
            jwks_url: URL to fetch JWKS from
            cache_ttl: Cache time-to-live in seconds (default: 1 hour)

        Raises:
            AuthenticationError: If JWKS URL is invalid or potentially dangerous
        """
        _validate_jwks_url(jwks_url)
        self._jwks_url = jwks_url
        self._cache_ttl = cache_ttl
        self._jwks: JsonWebKey | None = None
        self._last_fetch: float = 0

    async def get_jwks(self) -> JsonWebKey:
        """Fetch JWKS from the configured URL, using cache when valid.

        Returns:
            JsonWebKey set for token verification

        Raises:
            AuthenticationError: If JWKS cannot be fetched
        """
        current_time = time.time()

        # Return cached JWKS if still valid
        if self._jwks is not None and (current_time - self._last_fetch) < self._cache_ttl:
            return self._jwks

        try:
            async with httpx.AsyncClient(verify=True) as client:
                response = await client.get(self._jwks_url, timeout=10.0)
                response.raise_for_status()

                # Validate response size to prevent memory exhaustion
                content_length = response.headers.get("content-length")
                if content_length and int(content_length) > MAX_JWKS_RESPONSE_SIZE:
                    raise AuthenticationError(f"JWKS response too large: {content_length} bytes")

                jwks_data = response.json()

            # Validate JWKS structure
            if not isinstance(jwks_data, dict) or "keys" not in jwks_data:
                raise AuthenticationError("Invalid JWKS structure: missing 'keys' field")

            self._jwks = JsonWebKey.import_key_set(jwks_data)
            self._last_fetch = current_time
            logger.debug(f"Fetched JWKS from {self._jwks_url}")
            return self._jwks

        except httpx.HTTPError as e:
            logger.error(f"Failed to fetch JWKS from {self._jwks_url}: {e}")
            raise AuthenticationError("Failed to fetch JWKS") from e
        except AuthenticationError:
            raise
        except Exception as e:
            logger.error(f"Failed to parse JWKS: {e}")
            raise AuthenticationError("Invalid JWKS format") from e

    def clear_cache(self) -> None:
        """Clear the JWKS cache."""
        self._jwks = None
        self._last_fetch = 0


class OAuthValidator:
    """Validates OAuth/OIDC JWT tokens."""

    def __init__(self, config: SecurityConfig) -> None:
        """Initialize OAuth validator.

        Args:
            config: Security configuration with OAuth settings
        """
        self._config = config
        self._enabled = config.oauth_enabled
        self._issuer = str(config.oauth_issuer) if config.oauth_issuer else None
        self._audience = (
            config.oauth_audience
            if isinstance(config.oauth_audience, list)
            else [config.oauth_audience]
        )

        # Determine JWKS URL (use configured or derive from issuer)
        jwks_url = str(config.oauth_jwks_url) if config.oauth_jwks_url else None
        if not jwks_url and self._issuer:
            # Standard OIDC discovery pattern
            jwks_url = f"{self._issuer.rstrip('/')}/.well-known/jwks.json"

        self._jwks_client = JWKSClient(jwks_url) if jwks_url else None

        logger.debug(f"OAuth validator initialized: enabled={self._enabled}, issuer={self._issuer}")

    @property
    def enabled(self) -> bool:
        """Return whether OAuth is enabled."""
        return self._enabled

    def extract_token(self, authorization_header: str | None) -> str | None:
        """Extract JWT token from Authorization header.

        Args:
            authorization_header: Value of Authorization header

        Returns:
            JWT token string or None if not present/invalid format
        """
        if not authorization_header:
            return None

        parts = authorization_header.split()
        if len(parts) != BEARER_TOKEN_PARTS or parts[0].lower() != "bearer":
            return None

        return parts[1]

    async def validate_token(self, token: str) -> TokenClaims:
        """Validate a JWT token and extract claims.

        Args:
            token: JWT token string

        Returns:
            TokenClaims with validated claims

        Raises:
            AuthenticationError: If token is invalid or expired
            AuthorizationError: If token claims don't match requirements
        """
        if not self._jwks_client:
            raise AuthenticationError("JWKS client not configured")

        try:
            jwks = await self._jwks_client.get_jwks()

            # Decode with explicit algorithm restriction to prevent algorithm confusion
            claims = jwt.decode(
                token,
                jwks,
                claims_options={"alg": {"values": ALLOWED_JWT_ALGORITHMS}},
            )

            # Validate standard claims (expiration, not-before, etc.)
            claims.validate()

            # Validate issuer
            if self._issuer and claims.get("iss") != self._issuer:
                raise AuthorizationError("Invalid token issuer")

            # Validate audience
            if self._audience:
                token_aud = claims.get("aud")
                # Handle missing audience claim
                if token_aud is None:
                    raise AuthorizationError("Token missing required audience claim")
                # Normalize to list
                if isinstance(token_aud, str):
                    token_aud = [token_aud]
                # Check if any configured audience matches
                if not any(aud in token_aud for aud in self._audience if aud):
                    raise AuthorizationError("Invalid token audience")

            # Normalize audience for TokenClaims
            raw_aud = claims.get("aud", [])
            normalized_aud: list[str] = [raw_aud] if isinstance(raw_aud, str) else (raw_aud or [])

            return TokenClaims(
                subject=claims.get("sub", ""),
                issuer=claims.get("iss", ""),
                audience=normalized_aud,
                expires_at=claims.get("exp", 0),
                issued_at=claims.get("iat", 0),
                raw_claims=dict(claims),
            )

        except JoseError as e:
            logger.warning(f"JWT validation failed: {e}")
            raise AuthenticationError("Invalid or expired token") from e


class OAuthMiddleware(BaseHTTPMiddleware):
    """Starlette middleware for OAuth/OIDC authentication.

    This middleware validates JWT tokens from the Authorization header
    and attaches the validated claims to the request state for
    downstream handlers to use.
    """

    def __init__(self, app: Any, config: SecurityConfig) -> None:
        """Initialize OAuth middleware.

        Args:
            app: Starlette/FastAPI application
            config: Security configuration with OAuth settings
        """
        super().__init__(app)
        self._config = config
        self._validator = OAuthValidator(config)

        logger.debug(f"OAuth middleware initialized: enabled={self._validator.enabled}")

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Process request through OAuth validation.

        Args:
            request: Incoming HTTP request
            call_next: Next middleware/handler in chain

        Returns:
            Response from next handler or error response
        """
        # Skip auth for health check endpoints
        if request.url.path in ("/health", "/healthz", "/ready"):
            return await call_next(request)

        # Skip if OAuth is disabled
        if not self._validator.enabled:
            return await call_next(request)

        # Extract and validate token
        auth_header = request.headers.get("Authorization")
        token = self._validator.extract_token(auth_header)

        if not token:
            return JSONResponse(
                status_code=401,
                content={"error": "Missing or invalid Authorization header"},
                headers={"WWW-Authenticate": "Bearer"},
            )

        try:
            claims = await self._validator.validate_token(token)
            # Attach claims to request state for downstream use
            request.state.user_claims = claims
            request.state.user_id = claims.subject

        except AuthenticationError as e:
            logger.warning(f"Authentication failed: {e}")
            return JSONResponse(
                status_code=401,
                content={"error": str(e)},
                headers={"WWW-Authenticate": "Bearer"},
            )
        except AuthorizationError as e:
            logger.warning(f"Authorization failed: {e}")
            return JSONResponse(
                status_code=403,
                content={"error": str(e)},
            )

        return await call_next(request)


def create_oauth_middleware(config: SecurityConfig) -> type[OAuthMiddleware]:
    """Create a configured OAuth middleware class.

    This factory function creates a middleware class that can be added
    to a Starlette/FastAPI application.

    Args:
        config: Security configuration with OAuth settings

    Returns:
        Configured OAuthMiddleware class
    """

    class ConfiguredOAuthMiddleware(OAuthMiddleware):
        def __init__(self, app: Any) -> None:
            super().__init__(app, config)

    return ConfiguredOAuthMiddleware
