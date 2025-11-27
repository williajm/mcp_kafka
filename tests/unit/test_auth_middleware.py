"""Unit tests for OAuth middleware."""

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mcp_kafka.auth.middleware import (
    ALLOWED_JWT_ALGORITHMS,
    BEARER_TOKEN_PARTS,
    MAX_JWKS_RESPONSE_SIZE,
    PRIVATE_IP_RANGES,
    JWKSClient,
    OAuthMiddleware,
    OAuthValidator,
    TokenClaims,
    _is_private_ip,
    _validate_jwks_url,
    create_oauth_middleware,
)
from mcp_kafka.config import SecurityConfig
from mcp_kafka.utils.errors import AuthenticationError, AuthorizationError


class TestTokenClaims:
    """Tests for TokenClaims dataclass."""

    def test_token_claims_creation(self) -> None:
        """Test creating token claims."""
        claims = TokenClaims(
            subject="user-123",
            issuer="https://auth.example.com",
            audience=["api"],
            expires_at=1234567890,
            issued_at=1234567800,
            raw_claims={"sub": "user-123", "custom": "value"},
        )

        assert claims.subject == "user-123"
        assert claims.issuer == "https://auth.example.com"
        assert claims.audience == ["api"]
        assert claims.expires_at == 1234567890
        assert claims.issued_at == 1234567800
        assert claims.raw_claims["custom"] == "value"


class TestJWKSClient:
    """Tests for JWKSClient."""

    def test_jwks_client_init(self) -> None:
        """Test JWKSClient initialization."""
        client = JWKSClient("https://auth.example.com/.well-known/jwks.json")

        assert client._jwks_url == "https://auth.example.com/.well-known/jwks.json"
        assert client._cache_ttl == 3600
        assert client._jwks is None

    def test_jwks_client_custom_ttl(self) -> None:
        """Test JWKSClient with custom TTL."""
        client = JWKSClient("https://auth.example.com/.well-known/jwks.json", cache_ttl=7200)

        assert client._cache_ttl == 7200

    def test_clear_cache(self) -> None:
        """Test clearing JWKS cache."""
        client = JWKSClient("https://auth.example.com/.well-known/jwks.json")
        client._jwks = MagicMock()
        client._last_fetch = 1000.0

        client.clear_cache()

        assert client._jwks is None
        assert client._last_fetch == 0


class TestOAuthValidator:
    """Tests for OAuthValidator."""

    def test_oauth_validator_disabled(self, tmp_path: Path) -> None:
        """Test OAuthValidator when OAuth is disabled."""
        config = SecurityConfig(
            oauth_enabled=False,
            audit_log_file=tmp_path / "audit.log",
        )
        validator = OAuthValidator(config)

        assert validator.enabled is False

    def test_oauth_validator_enabled(self, tmp_path: Path) -> None:
        """Test OAuthValidator when OAuth is enabled."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",
            oauth_audience=["api"],
            audit_log_file=tmp_path / "audit.log",
        )
        validator = OAuthValidator(config)

        assert validator.enabled is True
        assert validator._issuer == "https://auth.example.com/"

    def test_oauth_validator_jwks_url_from_issuer(self, tmp_path: Path) -> None:
        """Test that JWKS URL is derived from issuer."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",
            audit_log_file=tmp_path / "audit.log",
        )
        validator = OAuthValidator(config)

        assert validator._jwks_client is not None
        assert ".well-known/jwks.json" in validator._jwks_client._jwks_url

    def test_oauth_validator_explicit_jwks_url(self, tmp_path: Path) -> None:
        """Test using explicit JWKS URL."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",
            oauth_jwks_url="https://auth.example.com/oauth/keys",
            audit_log_file=tmp_path / "audit.log",
        )
        validator = OAuthValidator(config)

        assert validator._jwks_client is not None
        assert validator._jwks_client._jwks_url == "https://auth.example.com/oauth/keys"


class TestOAuthValidatorTokenExtraction:
    """Tests for token extraction from headers."""

    def test_extract_token_valid_bearer(self, tmp_path: Path) -> None:
        """Test extracting valid Bearer token."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",
            audit_log_file=tmp_path / "audit.log",
        )
        validator = OAuthValidator(config)

        token = validator.extract_token("Bearer eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9")

        assert token == "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9"

    def test_extract_token_lowercase_bearer(self, tmp_path: Path) -> None:
        """Test extracting token with lowercase 'bearer'."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",
            audit_log_file=tmp_path / "audit.log",
        )
        validator = OAuthValidator(config)

        token = validator.extract_token("bearer test-token")

        assert token == "test-token"

    def test_extract_token_none_header(self, tmp_path: Path) -> None:
        """Test extracting token with None header."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",
            audit_log_file=tmp_path / "audit.log",
        )
        validator = OAuthValidator(config)

        token = validator.extract_token(None)

        assert token is None

    def test_extract_token_invalid_format(self, tmp_path: Path) -> None:
        """Test extracting token with invalid format."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",
            audit_log_file=tmp_path / "audit.log",
        )
        validator = OAuthValidator(config)

        # No space
        assert validator.extract_token("BearerToken") is None
        # Not Bearer
        assert validator.extract_token("Basic abc123") is None
        # Too many parts
        assert validator.extract_token("Bearer token extra") is None

    def test_bearer_token_parts_constant(self) -> None:
        """Test BEARER_TOKEN_PARTS constant."""
        assert BEARER_TOKEN_PARTS == 2


class TestOAuthValidatorTokenValidation:
    """Tests for token validation."""

    @pytest.mark.asyncio
    async def test_validate_token_no_jwks_client(self, tmp_path: Path) -> None:
        """Test validation fails without JWKS client."""
        config = SecurityConfig(
            oauth_enabled=False,
            audit_log_file=tmp_path / "audit.log",
        )
        validator = OAuthValidator(config)

        with pytest.raises(AuthenticationError) as exc_info:
            await validator.validate_token("test-token")

        assert "JWKS client not configured" in str(exc_info.value)


class TestOAuthMiddleware:
    """Tests for OAuthMiddleware."""

    @pytest.mark.asyncio
    async def test_middleware_disabled_skips_auth(self, tmp_path: Path) -> None:
        """Test that disabled OAuth skips authentication."""
        config = SecurityConfig(
            oauth_enabled=False,
            audit_log_file=tmp_path / "audit.log",
        )

        app = AsyncMock()
        middleware = OAuthMiddleware(app, config)

        request = MagicMock()
        request.url.path = "/api/test"

        call_next = AsyncMock(return_value="response")

        result = await middleware.dispatch(request, call_next)

        assert result == "response"
        call_next.assert_called_once_with(request)

    @pytest.mark.asyncio
    async def test_middleware_skips_health_endpoints(self, tmp_path: Path) -> None:
        """Test that health check endpoints skip auth."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",
            audit_log_file=tmp_path / "audit.log",
        )

        app = AsyncMock()
        middleware = OAuthMiddleware(app, config)
        call_next = AsyncMock(return_value="response")

        for path in ["/health", "/healthz", "/ready"]:
            request = MagicMock()
            request.url.path = path

            result = await middleware.dispatch(request, call_next)

            assert result == "response"

    @pytest.mark.asyncio
    async def test_middleware_missing_auth_header(self, tmp_path: Path) -> None:
        """Test response for missing Authorization header."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",
            audit_log_file=tmp_path / "audit.log",
        )

        app = AsyncMock()
        middleware = OAuthMiddleware(app, config)

        request = MagicMock()
        request.url.path = "/api/test"
        request.headers.get.return_value = None

        call_next = AsyncMock()

        result = await middleware.dispatch(request, call_next)

        assert result.status_code == 401
        call_next.assert_not_called()

    @pytest.mark.asyncio
    async def test_middleware_invalid_auth_format(self, tmp_path: Path) -> None:
        """Test response for invalid Authorization format."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",
            audit_log_file=tmp_path / "audit.log",
        )

        app = AsyncMock()
        middleware = OAuthMiddleware(app, config)

        request = MagicMock()
        request.url.path = "/api/test"
        request.headers.get.return_value = "Basic abc123"

        call_next = AsyncMock()

        result = await middleware.dispatch(request, call_next)

        assert result.status_code == 401


class TestCreateOAuthMiddleware:
    """Tests for create_oauth_middleware factory."""

    def test_create_middleware_factory(self, tmp_path: Path) -> None:
        """Test middleware factory function."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",
            audit_log_file=tmp_path / "audit.log",
        )

        middleware_class = create_oauth_middleware(config)

        # Should be a class
        assert isinstance(middleware_class, type)

        # Should be callable with just app
        app = MagicMock()
        instance = middleware_class(app)
        assert instance is not None


class TestErrorClasses:
    """Tests for authentication/authorization error classes."""

    def test_authentication_error(self) -> None:
        """Test AuthenticationError."""
        error = AuthenticationError("Invalid token")
        assert str(error) == "Invalid token"

    def test_authorization_error(self) -> None:
        """Test AuthorizationError."""
        error = AuthorizationError("Insufficient permissions")
        assert str(error) == "Insufficient permissions"


class TestSSRFProtection:
    """Tests for SSRF protection in JWKS URL validation."""

    def test_private_ip_ranges_defined(self) -> None:
        """Test that private IP ranges are properly defined."""
        assert len(PRIVATE_IP_RANGES) == 6
        # Verify common private ranges are included
        range_strs = [str(r) for r in PRIVATE_IP_RANGES]
        assert "0.0.0.0/8" in range_strs
        assert "10.0.0.0/8" in range_strs
        assert "172.16.0.0/12" in range_strs
        assert "192.168.0.0/16" in range_strs
        assert "127.0.0.0/8" in range_strs

    def test_is_private_ip_loopback(self) -> None:
        """Test detection of loopback addresses."""
        assert _is_private_ip("127.0.0.1") is True
        assert _is_private_ip("127.0.0.2") is True

    def test_is_private_ip_private_ranges(self) -> None:
        """Test detection of private IP ranges."""
        assert _is_private_ip("10.0.0.1") is True
        assert _is_private_ip("172.16.0.1") is True
        assert _is_private_ip("192.168.1.1") is True
        assert _is_private_ip("169.254.1.1") is True

    def test_is_private_ip_public(self) -> None:
        """Test that public IPs are not flagged."""
        assert _is_private_ip("8.8.8.8") is False
        assert _is_private_ip("1.1.1.1") is False

    def test_is_private_ip_dangerous_hostnames(self) -> None:
        """Test detection of dangerous hostnames."""
        assert _is_private_ip("localhost") is True
        assert _is_private_ip("metadata.google.internal") is True

    def test_is_private_ip_special_addresses(self) -> None:
        """Test detection of special IP addresses (0.0.0.0/8 range)."""
        assert _is_private_ip("0.0.0.0") is True
        assert _is_private_ip("0.0.0.1") is True

    def test_is_private_ip_safe_hostname(self) -> None:
        """Test that safe hostnames are allowed."""
        assert _is_private_ip("auth.example.com") is False
        assert _is_private_ip("oauth.provider.io") is False

    def test_validate_jwks_url_https(self) -> None:
        """Test that HTTPS URLs are accepted."""
        # Should not raise
        _validate_jwks_url("https://auth.example.com/.well-known/jwks.json")

    def test_validate_jwks_url_http_allowed(self) -> None:
        """Test that HTTP URLs are allowed (for testing)."""
        # HTTP is allowed for testing scenarios
        _validate_jwks_url("http://auth.example.com/.well-known/jwks.json")

    def test_validate_jwks_url_invalid_scheme(self) -> None:
        """Test that non-HTTP(S) schemes are rejected."""
        with pytest.raises(AuthenticationError) as exc_info:
            _validate_jwks_url("ftp://auth.example.com/jwks.json")
        assert "must use HTTPS" in str(exc_info.value)

    def test_validate_jwks_url_private_ip(self) -> None:
        """Test that private IP URLs are rejected."""
        with pytest.raises(AuthenticationError) as exc_info:
            _validate_jwks_url("https://192.168.1.1/.well-known/jwks.json")
        assert "private IP" in str(exc_info.value)

    def test_validate_jwks_url_localhost(self) -> None:
        """Test that localhost URLs are rejected."""
        with pytest.raises(AuthenticationError) as exc_info:
            _validate_jwks_url("https://localhost/.well-known/jwks.json")
        assert "private IP" in str(exc_info.value)

    def test_validate_jwks_url_loopback(self) -> None:
        """Test that loopback URLs are rejected."""
        with pytest.raises(AuthenticationError) as exc_info:
            _validate_jwks_url("https://127.0.0.1/.well-known/jwks.json")
        assert "private IP" in str(exc_info.value)


class TestSecurityConstants:
    """Tests for security-related constants."""

    def test_allowed_jwt_algorithms(self) -> None:
        """Test that only secure algorithms are allowed."""
        # RSA algorithms
        assert "RS256" in ALLOWED_JWT_ALGORITHMS
        assert "RS384" in ALLOWED_JWT_ALGORITHMS
        assert "RS512" in ALLOWED_JWT_ALGORITHMS
        # ECDSA algorithms
        assert "ES256" in ALLOWED_JWT_ALGORITHMS
        assert "ES384" in ALLOWED_JWT_ALGORITHMS
        assert "ES512" in ALLOWED_JWT_ALGORITHMS
        # Insecure algorithms should NOT be present
        assert "none" not in ALLOWED_JWT_ALGORITHMS
        assert "HS256" not in ALLOWED_JWT_ALGORITHMS

    def test_max_jwks_response_size(self) -> None:
        """Test JWKS response size limit."""
        assert MAX_JWKS_RESPONSE_SIZE == 1024 * 1024  # 1MB


class TestJWKSClientSSRF:
    """Tests for JWKS client SSRF protection."""

    def test_jwks_client_rejects_private_ip(self) -> None:
        """Test that JWKSClient rejects private IP URLs."""
        with pytest.raises(AuthenticationError) as exc_info:
            JWKSClient("https://192.168.1.1/.well-known/jwks.json")
        assert "private IP" in str(exc_info.value)

    def test_jwks_client_rejects_localhost(self) -> None:
        """Test that JWKSClient rejects localhost URLs."""
        with pytest.raises(AuthenticationError) as exc_info:
            JWKSClient("https://localhost/.well-known/jwks.json")
        assert "private IP" in str(exc_info.value)

    def test_jwks_client_accepts_public_url(self) -> None:
        """Test that JWKSClient accepts public URLs."""
        client = JWKSClient("https://auth.example.com/.well-known/jwks.json")
        assert client._jwks_url == "https://auth.example.com/.well-known/jwks.json"


class TestJWKSClientFetch:
    """Tests for JWKS client fetch operations."""

    @pytest.mark.asyncio
    async def test_get_jwks_invalid_structure(self) -> None:
        """Test that invalid JWKS structure is rejected."""
        client = JWKSClient("https://auth.example.com/.well-known/jwks.json")

        mock_response = MagicMock()
        mock_response.headers = {}
        mock_response.json.return_value = {"invalid": "structure"}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.get.return_value = mock_response
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_client.return_value = mock_instance

            with pytest.raises(AuthenticationError) as exc_info:
                await client.get_jwks()

            assert "missing 'keys' field" in str(exc_info.value)

    @pytest.mark.asyncio
    async def test_get_jwks_response_too_large(self) -> None:
        """Test that oversized JWKS responses are rejected."""
        client = JWKSClient("https://auth.example.com/.well-known/jwks.json")

        mock_response = MagicMock()
        mock_response.headers = {"content-length": str(MAX_JWKS_RESPONSE_SIZE + 1)}
        mock_response.raise_for_status = MagicMock()

        with patch("httpx.AsyncClient") as mock_client:
            mock_instance = AsyncMock()
            mock_instance.get.return_value = mock_response
            mock_instance.__aenter__.return_value = mock_instance
            mock_instance.__aexit__.return_value = None
            mock_client.return_value = mock_instance

            with pytest.raises(AuthenticationError) as exc_info:
                await client.get_jwks()

            assert "too large" in str(exc_info.value)


class TestOAuthValidatorAudience:
    """Tests for audience validation in OAuthValidator."""

    def test_audience_single_string(self, tmp_path: Path) -> None:
        """Test handling of single string audience in config."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",
            oauth_audience="api",
            audit_log_file=tmp_path / "audit.log",
        )
        validator = OAuthValidator(config)

        # Should normalize to list
        assert validator._audience == ["api"]

    def test_audience_list(self, tmp_path: Path) -> None:
        """Test handling of list audience in config."""
        config = SecurityConfig(
            oauth_enabled=True,
            oauth_issuer="https://auth.example.com",
            oauth_audience=["api", "service"],
            audit_log_file=tmp_path / "audit.log",
        )
        validator = OAuthValidator(config)

        assert validator._audience == ["api", "service"]
