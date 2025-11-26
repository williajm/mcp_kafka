"""Tests for version module."""

from mcp_kafka.version import __version__, get_version


class TestVersion:
    """Tests for version functions."""

    def test_get_version_returns_string(self) -> None:
        """Test that get_version returns a string."""
        version = get_version()
        assert isinstance(version, str)
        assert len(version) > 0

    def test_version_variable_exists(self) -> None:
        """Test that __version__ variable exists."""
        assert __version__ is not None
        assert isinstance(__version__, str)

    def test_version_format(self) -> None:
        """Test version has expected format (semver or dev)."""
        version = get_version()
        # Either a semver version or dev version
        assert "." in version or "dev" in version

    def test_get_version_direct_call(self) -> None:
        """Test get_version called directly."""
        # get_version uses try/except internally
        version = get_version()
        # Should return the installed version
        assert version == "0.1.0"
