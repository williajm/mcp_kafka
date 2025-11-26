"""Configuration management for MCP Kafka server."""

import json
import warnings
from pathlib import Path

from pydantic import Field, HttpUrl, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from mcp_kafka.version import __version__


def _parse_comma_separated_list(value: str | list[str] | None) -> list[str]:
    """Parse comma-separated string or JSON array into list of strings.

    Args:
        value: Input value (string, list, or None)

    Returns:
        List of strings
    """
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        value_stripped = value.strip()
        if value_stripped.startswith("[") and value_stripped.endswith("]"):
            try:
                parsed = json.loads(value_stripped)
                if isinstance(parsed, list):
                    return [str(item) for item in parsed]
            except json.JSONDecodeError:
                # Intentional fallback: if JSON parsing fails, treat as comma-separated
                pass
        return [item.strip() for item in value.split(",") if item.strip()]
    return []


class KafkaConfig(BaseSettings):
    """Kafka client configuration."""

    model_config = SettingsConfigDict(
        env_prefix="KAFKA_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Connection
    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Comma-separated list of Kafka broker addresses",
    )
    client_id: str = Field(
        default="mcp-kafka",
        description="Client identifier for Kafka connections",
    )
    timeout: int = Field(
        default=30,
        description="Default timeout for Kafka operations in seconds",
        gt=0,
    )

    # Security Protocol
    security_protocol: str = Field(
        default="PLAINTEXT",
        description="Security protocol: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL",
    )

    # SASL Authentication
    sasl_mechanism: str | None = Field(
        default=None,
        description="SASL mechanism: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI",
    )
    sasl_username: str | None = Field(
        default=None,
        description="SASL username for PLAIN/SCRAM",
    )
    sasl_password: SecretStr | None = Field(
        default=None,
        description="SASL password for PLAIN/SCRAM (sensitive)",
    )

    # Kerberos/GSSAPI
    sasl_kerberos_service_name: str = Field(
        default="kafka",
        description="Kerberos service name",
    )
    sasl_kerberos_keytab: Path | None = Field(
        default=None,
        description="Path to Kerberos keytab file",
    )
    sasl_kerberos_principal: str | None = Field(
        default=None,
        description="Kerberos principal name",
    )

    # SSL/TLS
    ssl_ca_location: Path | None = Field(
        default=None,
        description="Path to CA certificate file",
    )
    ssl_certificate_location: Path | None = Field(
        default=None,
        description="Path to client certificate file",
    )
    ssl_key_location: Path | None = Field(
        default=None,
        description="Path to client private key file",
    )
    ssl_key_password: SecretStr | None = Field(
        default=None,
        description="Password for encrypted private key (sensitive)",
    )

    @field_validator("security_protocol")
    @classmethod
    def validate_security_protocol(cls, v: str) -> str:
        """Validate security protocol."""
        valid = {"PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"}
        if v not in valid:
            raise ValueError(f"Invalid security_protocol: {v}. Must be one of {valid}")
        return v

    @field_validator("sasl_mechanism")
    @classmethod
    def validate_sasl_mechanism(cls, v: str | None) -> str | None:
        """Validate SASL mechanism."""
        if v is None:
            return None
        valid = {"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512", "GSSAPI"}
        if v not in valid:
            raise ValueError(f"Invalid sasl_mechanism: {v}. Must be one of {valid}")
        return v

    @model_validator(mode="after")
    def validate_sasl_config(self) -> "KafkaConfig":
        """Validate SASL configuration consistency."""
        if self.sasl_mechanism:
            if self.sasl_mechanism in ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]:
                if not self.sasl_username or not self.sasl_password:
                    raise ValueError(
                        f"SASL mechanism {self.sasl_mechanism} requires "
                        "KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD"
                    )
            elif self.sasl_mechanism == "GSSAPI" and not self.sasl_kerberos_principal:
                raise ValueError("SASL mechanism GSSAPI requires KAFKA_SASL_KERBEROS_PRINCIPAL")
        return self


class SafetyConfig(BaseSettings):
    """Safety and operation control configuration."""

    model_config = SettingsConfigDict(
        env_prefix="SAFETY_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    allow_write_operations: bool = Field(
        default=False,
        description="Allow write operations (produce, create_topic, reset_offsets)",
    )
    max_consume_messages: int = Field(
        default=100,
        description="Maximum messages to return from consume operations",
        ge=1,
        le=10000,
    )
    max_message_size: int = Field(
        default=1048576,  # 1 MB
        description="Maximum message size for produce operations",
        ge=1,
        le=10485760,  # 10 MB
    )

    # Topic filtering
    topic_blocklist: str | list[str] = Field(
        default=["__consumer_offsets", "__transaction_state"],
        description="Topics to block from all operations",
    )

    # Tool filtering
    allowed_tools: str | list[str] | None = Field(
        default=None,
        description="Allowed tool names (None = allow all based on access level)",
    )
    denied_tools: str | list[str] | None = Field(
        default=None,
        description="Denied tool names (takes precedence over allowed_tools)",
    )

    @field_validator("topic_blocklist", mode="before")
    @classmethod
    def parse_topic_blocklist(cls, value: str | list[str] | None) -> list[str]:
        """Parse topic blocklist."""
        return _parse_comma_separated_list(value)

    @field_validator("allowed_tools", "denied_tools", mode="before")
    @classmethod
    def parse_filtering_list(cls, value: str | list[str] | None) -> list[str] | None:
        """Parse tool filtering list."""
        if value is None:
            return None
        return _parse_comma_separated_list(value)


class SecurityConfig(BaseSettings):
    """Security configuration for authentication, authorization, and audit."""

    model_config = SettingsConfigDict(
        env_prefix="SECURITY_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Rate Limiting
    rate_limit_enabled: bool = Field(
        default=True,
        description="Enable global rate limiting",
    )
    rate_limit_rpm: int = Field(
        default=60,
        description="Maximum requests per minute (global)",
        gt=0,
        le=1000,
    )

    # Audit Logging
    audit_log_enabled: bool = Field(
        default=True,
        description="Enable audit logging of all operations",
    )
    audit_log_file: Path = Field(
        default=Path("mcp_audit.log"),
        description="Path to audit log file",
    )

    # Network Security
    allowed_client_ips: str | list[str] = Field(
        default=[],
        description="Allowed client IP addresses (empty list = allow all)",
    )

    # OAuth/OIDC Authentication
    oauth_enabled: bool = Field(
        default=False,
        description="Enable OAuth/OIDC authentication for network transports",
    )
    oauth_issuer: HttpUrl | None = Field(
        default=None,
        description="OAuth/OIDC issuer URL",
    )
    oauth_audience: list[str] | str = Field(
        default=[],
        description="Expected audience values in JWT 'aud' claim",
    )
    oauth_jwks_url: HttpUrl | None = Field(
        default=None,
        description="JWKS endpoint URL for JWT signature verification",
    )

    @field_validator("allowed_client_ips", mode="before")
    @classmethod
    def parse_ip_list(cls, value: str | list[str] | None) -> list[str]:
        """Parse IP list."""
        return _parse_comma_separated_list(value)

    @field_validator("audit_log_file")
    @classmethod
    def validate_audit_log_path(cls, audit_log_path: Path) -> Path:
        """Ensure parent directory exists for audit log file."""
        if not audit_log_path.parent.exists():
            audit_log_path.parent.mkdir(parents=True, exist_ok=True)
        return audit_log_path

    @model_validator(mode="after")
    def parse_oauth_list_fields(self) -> "SecurityConfig":
        """Parse OAuth list fields from comma-separated strings."""
        if isinstance(self.oauth_audience, str):
            self.oauth_audience = _parse_comma_separated_list(self.oauth_audience)
        return self

    @model_validator(mode="after")
    def validate_oauth_config(self) -> "SecurityConfig":
        """Validate OAuth configuration consistency."""
        if self.oauth_enabled:
            if not self.oauth_issuer:
                raise ValueError(
                    "OAuth enabled but oauth_issuer not configured. "
                    "Set SECURITY_OAUTH_ISSUER to your OAuth provider's issuer URL."
                )
            if not self.oauth_jwks_url:
                warnings.warn(
                    "OAuth enabled but oauth_jwks_url not configured. "
                    "JWKS URL will be discovered from issuer.",
                    UserWarning,
                    stacklevel=2,
                )
        return self


class ServerConfig(BaseSettings):
    """MCP server configuration."""

    model_config = SettingsConfigDict(
        env_prefix="MCP_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    server_name: str = Field(
        default="mcp-kafka",
        description="MCP server name",
    )
    server_version: str = Field(
        default=__version__,
        description="MCP server version",
    )
    log_level: str = Field(
        default="INFO",
        description="Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)",
    )
    log_format: str = Field(
        default=(
            "<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
            "<level>{level: <8}</level> | "
            "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
            "<level>{message}</level>"
        ),
        description="Log format string for loguru",
    )
    json_logging: bool = Field(
        default=False,
        description="Enable JSON structured logging (for SIEM/production)",
    )
    debug_mode: bool = Field(
        default=False,
        description="Enable debug mode (shows detailed errors)",
    )

    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, level: str) -> str:
        """Validate log level."""
        valid_levels = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        level_upper = level.upper()
        if level_upper not in valid_levels:
            raise ValueError(f"Invalid log level: {level}. Must be one of {valid_levels}")
        return level_upper


class Config:
    """Main configuration container."""

    def __init__(self) -> None:
        """Initialize configuration from environment and .env file."""
        self.kafka = KafkaConfig()
        self.safety = SafetyConfig()
        self.security = SecurityConfig()
        self.server = ServerConfig()

    def __repr__(self) -> str:
        """Return string representation of config."""
        return (
            f"Config(kafka={self.kafka!r}, safety={self.safety!r}, "
            f"security={self.security!r}, server={self.server!r})"
        )
