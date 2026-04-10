"""Tests for acoharmony._config_loader."""

from unittest.mock import patch

import pytest

from acoharmony._config_loader import get_config_path, load_aco_config


class TestGetConfigPath:
    """Tests for get_config_path()."""

    @pytest.mark.unit
    def test_returns_absolute_path(self):
        """Returned path is absolute and points at the packaged aco.toml."""
        p = get_config_path()
        assert p.is_absolute()
        assert p.name == "aco.toml"
        assert p.parent.name == "_config"

    @pytest.mark.unit
    def test_points_inside_acoharmony_package(self):
        """Path lives under the installed acoharmony package directory."""
        p = get_config_path()
        # The path should have `acoharmony/_config/aco.toml` as its tail.
        assert p.parts[-3:] == ("acoharmony", "_config", "aco.toml")


class TestLoadAcoConfig:
    """Tests for load_aco_config()."""

    @pytest.mark.unit
    def test_loads_real_packaged_config(self):
        """Happy path: reads the real aco.toml shipped with the package."""
        config = load_aco_config()
        assert isinstance(config, dict)
        assert "default_profile" in config
        assert "profiles" in config
        # A few known profiles must be present.
        assert set(config["profiles"].keys()) >= {"dev", "local", "staging", "prod"}

    @pytest.mark.unit
    def test_raises_when_file_missing(self, monkeypatch, tmp_path):
        """FileNotFoundError when the packaged aco.toml cannot be found."""
        # Redirect the module-level _CONFIG_PATH to a missing file.
        import acoharmony._config_loader as loader

        missing = tmp_path / "nope" / "aco.toml"
        monkeypatch.setattr(loader, "_CONFIG_PATH", missing)
        with pytest.raises(FileNotFoundError, match="aco.toml not found"):
            load_aco_config()
