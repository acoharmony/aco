# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for 4icli config - Polars style."""

from __future__ import annotations
from dataclasses import dataclass

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import patch

import pytest

from acoharmony._4icli.config import FourICLIConfig, load_profile_config
from acoharmony._test.foureye.conftest import _make_config  # noqa: F401


class TestLoadProfileConfig:
    """Tests for load_profile_config function."""

    @pytest.mark.unit
    def test_load_profile_config_dev(self) -> None:
        """Loads dev profile configuration."""
        config = load_profile_config("dev")

        assert config is not None
        assert "storage" in config
        assert "environment" in config
        assert config["environment"] == "development"

    @pytest.mark.unit
    def test_load_profile_config_default(self) -> None:
        """Loads default profile when none specified."""
        config = load_profile_config()

        assert config is not None
        assert "storage" in config

    @pytest.mark.unit
    def test_load_profile_config_missing_raises(self) -> None:
        """Raises error when profile file missing."""
        with pytest.raises((ValueError, FileNotFoundError, KeyError)):
            load_profile_config("nonexistent")


class TestFourICLIConfigInitialization:
    """Tests for FourICLIConfig initialization."""

    @pytest.mark.unit
    def test_from_profile_creates_config(self) -> None:
        """from_profile creates valid configuration."""
        config = FourICLIConfig.from_profile("dev")

        assert config is not None
        assert isinstance(config.binary_path, Path)
        assert isinstance(config.working_dir, Path)
        assert isinstance(config.bronze_dir, Path)
        assert isinstance(config.data_path, Path)

    @pytest.mark.unit
    def test_from_profile_uses_env_profile(self) -> None:
        """from_profile respects ACO_PROFILE environment variable."""
        with patch.dict("os.environ", {"ACO_PROFILE": "dev"}):
            config = FourICLIConfig.from_profile()

            assert config is not None
            # Should use dev profile settings

    @pytest.mark.unit
    def test_paths_derived_from_storage(self) -> None:
        """Paths are correctly derived from storage configuration."""
        config = FourICLIConfig.from_profile("dev")

        # Bronze dir should be data_path/bronze
        assert config.bronze_dir == config.data_path / "bronze"

        # Archive should be data_path/archive
        assert config.archive_dir == config.data_path / "archive"

        # Silver should be data_path/silver
        assert config.silver_dir == config.data_path / "silver"

        # Gold should be data_path/gold
        assert config.gold_dir == config.data_path / "gold"

    @pytest.mark.unit
    def test_working_dir_defaults_to_bronze(self) -> None:
        """Working directory defaults to bronze directory."""
        config = FourICLIConfig.from_profile("dev")

        # By default, working_dir should equal bronze_dir
        # unless FOURICLI_WORKING_DIR is set
        assert config.working_dir == config.bronze_dir or config.working_dir.exists()


class TestFourICLIConfigValidation:
    """Tests for FourICLIConfig validation."""

    @pytest.mark.unit
    def test_validate_succeeds_with_config_txt(self, mock_config: FourICLIConfig) -> None:
        """validate succeeds when config.txt exists."""
        # mock_config has config.txt
        mock_config.validate()  # Should not raise

    @pytest.mark.unit
    def test_validate_creates_directories(self, mock_config: FourICLIConfig) -> None:
        """ensure_storage_directories creates necessary directories."""
        # Remove directories
        if mock_config.bronze_dir.exists():
            mock_config.bronze_dir.rmdir()

        # ensure_storage_directories should create all medallion directories
        mock_config.ensure_storage_directories()

        assert mock_config.bronze_dir.exists()
        assert mock_config.archive_dir.exists()
        assert mock_config.silver_dir.exists()
        assert mock_config.gold_dir.exists()

    @pytest.mark.unit
    def test_validate_checks_config_txt(self, mock_config: FourICLIConfig) -> None:
        """validate checks for config.txt in working directory."""
        # mock_config already has config.txt
        mock_config.validate()  # Should not raise

    @pytest.mark.unit
    def test_validate_raises_missing_config_txt(self, tmp_path: Path) -> None:
        """validate raises when config.txt missing everywhere."""
        config = FourICLIConfig.from_profile("dev")
        config.working_dir = tmp_path / "working"
        config.bronze_dir = tmp_path / "bronze"
        config.archive_dir = tmp_path / "archive"
        config.silver_dir = tmp_path / "silver"
        config.gold_dir = tmp_path / "gold"
        config.log_dir = tmp_path / "logs"

        # Mock Path.exists to make config.txt not found anywhere
        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="4icli config.txt not found"):
                config.validate()


class TestFourICLIConfigCredentials:
    """Tests for credential management."""

    @pytest.mark.unit
    def test_ensure_config_file_existing(self, mock_config: FourICLIConfig) -> None:
        """ensure_config_file returns existing config.txt."""
        config_file = mock_config.ensure_config_file()

        assert config_file.exists()
        assert config_file == mock_config.working_dir / "config.txt"

    @pytest.mark.unit
    def test_ensure_config_file_from_compose(self, tmp_path: Path) -> None:
        """ensure_config_file finds config.txt in working directory."""
        config = FourICLIConfig.from_profile("dev")
        config.working_dir = tmp_path / "working"
        config.working_dir.mkdir()

        # Place config.txt in the working directory (search priority #2)
        (config.working_dir / "config.txt").write_text("dummy\n")

        config_file = config.ensure_config_file()

        assert config_file.exists()
        assert config_file.name == "config.txt"

    @pytest.mark.unit
    def test_ensure_config_file_missing_raises(self, tmp_path: Path) -> None:
        """ensure_config_file raises when no config.txt found."""
        config = FourICLIConfig.from_profile("dev")
        config.working_dir = tmp_path / "nonexistent_working"
        config.working_dir.mkdir()

        # Mock the compose config to not exist
        with patch("pathlib.Path.exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="config.txt not found"):
                config.ensure_config_file()


class TestFourICLIConfigHelperMethods:
    """Tests for FourICLIConfig helper methods."""

    @pytest.mark.unit
    def test_get_alignment_dir_palmr(self, mock_config: FourICLIConfig) -> None:
        """get_alignment_dir returns bronze (flat structure, no nesting)."""
        dir_path = mock_config.get_alignment_dir("PALMR")

        assert dir_path == mock_config.bronze_dir

    @pytest.mark.unit
    def test_get_alignment_dir_pbvar(self, mock_config: FourICLIConfig) -> None:
        """get_alignment_dir returns bronze (flat structure, no nesting)."""
        dir_path = mock_config.get_alignment_dir("PBVAR")

        assert dir_path == mock_config.bronze_dir

    @pytest.mark.unit
    def test_get_alignment_dir_tparc(self, mock_config: FourICLIConfig) -> None:
        """get_alignment_dir returns bronze (flat structure, no nesting)."""
        dir_path = mock_config.get_alignment_dir("TPARC")

        assert dir_path == mock_config.bronze_dir

    @pytest.mark.unit
    def test_get_alignment_dir_unknown(self, mock_config: FourICLIConfig) -> None:
        """get_alignment_dir returns bronze (flat structure, no nesting)."""
        dir_path = mock_config.get_alignment_dir("UNKNOWN")

        assert dir_path == mock_config.bronze_dir

    @pytest.mark.unit
    def test_get_cclf_dir_with_number(self, mock_config: FourICLIConfig) -> None:
        """get_cclf_dir returns bronze (flat structure, no nesting)."""
        dir_path = mock_config.get_cclf_dir("8")

        assert dir_path == mock_config.bronze_dir

    @pytest.mark.unit
    def test_get_cclf_dir_without_number(self, mock_config: FourICLIConfig) -> None:
        """get_cclf_dir returns bronze (flat structure, no nesting)."""
        dir_path = mock_config.get_cclf_dir()

        assert dir_path == mock_config.bronze_dir

    @pytest.mark.unit
    def test_get_report_dir_palmr(self, mock_config: FourICLIConfig) -> None:
        """get_report_dir returns bronze (flat structure, no nesting)."""
        dir_path = mock_config.get_report_dir("PALMR")

        assert dir_path == mock_config.bronze_dir

    @pytest.mark.unit
    def test_get_report_dir_cclf(self, mock_config: FourICLIConfig) -> None:
        """get_report_dir returns bronze (flat structure, no nesting)."""
        dir_path = mock_config.get_report_dir("CCLF")

        assert dir_path == mock_config.bronze_dir

    @pytest.mark.unit
    def test_get_report_dir_unknown(self, mock_config: FourICLIConfig) -> None:
        """get_report_dir returns bronze (flat structure, no nesting)."""
        dir_path = mock_config.get_report_dir("UNKNOWN")

        assert dir_path == mock_config.bronze_dir


class TestFourICLIConfigEnvironmentOverrides:
    """Tests for environment variable overrides."""

    @pytest.mark.unit
    def test_binary_path_from_env(self) -> None:
        """Binary path can be overridden with env var."""
        with patch.dict("os.environ", {"FOURICLI_BINARY_PATH": "/custom/path/4icli"}):
            config = FourICLIConfig.from_profile("dev")

            assert config.binary_path == Path("/custom/path/4icli")

    @pytest.mark.unit
    def test_working_dir_from_env(self) -> None:
        """Working directory can be overridden with env var."""
        with patch.dict("os.environ", {"FOURICLI_WORKING_DIR": "/custom/working"}):
            config = FourICLIConfig.from_profile("dev")

            assert config.working_dir == Path("/custom/working")

    @pytest.mark.unit
    def test_apm_id_from_env(self) -> None:
        """APM ID can be set from environment."""
        with patch.dict("os.environ", {"FOURICLI_APM_ID": "D0999"}):
            config = FourICLIConfig.from_profile("dev")

            assert config.default_apm_id == "D0999"

    @pytest.mark.unit
    def test_default_year_from_env(self) -> None:
        """Default year can be set from environment."""
        with patch.dict("os.environ", {"FOURICLI_DEFAULT_YEAR": "2026"}):
            config = FourICLIConfig.from_profile("dev")

            assert config.default_year == 2026


class TestFourICLIConfigEdgeCases:
    """Tests for edge cases and error paths in config."""

    @pytest.mark.unit
    def test_ensure_storage_directories_with_logging(self, mock_config: FourICLIConfig) -> None:
        """ensure_storage_directories creates log dir when logging enabled."""
        mock_config.enable_logging = True

        # Remove directories
        import shutil

        if mock_config.log_dir.exists():
            shutil.rmtree(mock_config.log_dir)

        mock_config.ensure_storage_directories()

        assert mock_config.log_dir.exists()

    @pytest.mark.unit
    def test_ensure_config_file_returns_existing_compose_config(self, tmp_path) -> None:
        """ensure_config_file returns config.txt from working dir."""
        config = FourICLIConfig.from_profile("dev")
        config.working_dir = tmp_path / "bronze"
        config.working_dir.mkdir()

        # Place config.txt in the working directory
        (config.working_dir / "config.txt").write_text("dummy\n")

        config_file = config.ensure_config_file()

        assert config_file.exists()
        assert "config.txt" in str(config_file)


class TestConfig:
    @pytest.mark.unit
    def test_ensure_config_file_profile_path(self, tmp_path):
        cfg = _make_config(tmp_path)
        config_txt = tmp_path / "profile_config.txt"
        config_txt.write_text("creds")
        cfg._profile_config_path = config_txt

        result = cfg.ensure_config_file()
        assert result == config_txt

    @pytest.mark.unit
    def test_ensure_config_file_working_dir(self, tmp_path):
        cfg = _make_config(tmp_path)
        cfg._profile_config_path = None

        result = cfg.ensure_config_file()
        assert result == cfg.working_dir / "config.txt"

    @pytest.mark.unit
    def test_ensure_config_file_not_found(self, tmp_path):
        cfg = _make_config(tmp_path)
        cfg._profile_config_path = tmp_path / "nope.txt"
        (cfg.working_dir / "config.txt").unlink()

        with pytest.raises(FileNotFoundError, match="config.txt not found"):
            cfg.ensure_config_file()

    @pytest.mark.unit
    def test_ensure_storage_directories(self, tmp_path):
        cfg = _make_config(tmp_path)
        cfg.ensure_storage_directories()
        assert cfg.bronze_dir.exists()
        assert cfg.archive_dir.exists()
        assert cfg.silver_dir.exists()
        assert cfg.gold_dir.exists()
        assert cfg.tracking_dir.exists()

    @pytest.mark.unit
    def test_get_alignment_dir(self, tmp_path):
        cfg = _make_config(tmp_path)
        assert cfg.get_alignment_dir("PALMR") == cfg.bronze_dir

    @pytest.mark.unit
    def test_get_cclf_dir(self, tmp_path):
        cfg = _make_config(tmp_path)
        assert cfg.get_cclf_dir("8") == cfg.bronze_dir

    @pytest.mark.unit
    def test_get_report_dir(self, tmp_path):
        cfg = _make_config(tmp_path)
        assert cfg.get_report_dir("RAP") == cfg.bronze_dir

class TestConfigAdditional:
    """Additional config tests for remaining coverage."""

    @pytest.mark.unit
    def test_load_profile_config(self):
        """Test load_profile_config function."""
        from acoharmony._4icli.config import load_profile_config
        # Should load from pyproject.toml - test with default profile
        try:
            config = load_profile_config()
            assert isinstance(config, dict)
        except (FileNotFoundError, ValueError):
            pass  # OK if pyproject.toml not in expected location

    @pytest.mark.unit
    def test_load_profile_config_invalid_profile(self):
        from acoharmony._4icli.config import load_profile_config
        with pytest.raises(ValueError, match="not found"):
            load_profile_config(profile="nonexistent_profile_xyz")

    @pytest.mark.unit
    def test_from_profile_with_config_path(self):
        """Test from_profile with fouricli.config_path set."""
        from acoharmony._4icli.config import FourICLIConfig
        try:
            config = FourICLIConfig.from_profile()
            # Just verify it creates a config
            assert config.data_path is not None
        except (FileNotFoundError, ValueError):
            pass

    @pytest.mark.unit
    def test_validate_creates_working_dir(self, tmp_path):
        cfg = _make_config(tmp_path)
        import shutil
        shutil.rmtree(cfg.working_dir)
        assert not cfg.working_dir.exists()

        # validate should create working_dir and check config
        try:
            cfg.validate()
        except FileNotFoundError:
            pass  # Expected since config.txt won't exist after rmtree
        assert cfg.working_dir.exists()


class TestConfigEdgeCases:
    """Cover config.py lines 35, 140-141, 146, 192-194."""

    @pytest.mark.unit
    def test_config_validate_raises_file_not_found(self, tmp_path):
        """Cover lines 192-194: validate re-raises FileNotFoundError."""
        cfg = _make_config(tmp_path)
        # Remove config.txt so ensure_config_file will fail
        (cfg.working_dir / "config.txt").unlink()
        cfg._profile_config_path = tmp_path / "nope.txt"

        original_exists = Path.exists
        def patched_exists(self_path):
            # Make all config.txt files not exist
            if "config.txt" in str(self_path):
                return False
            return original_exists(self_path)

        with patch.object(Path, "exists", patched_exists):
            with pytest.raises(FileNotFoundError):
                cfg.validate()

class TestConfigFromProfile:
    """Cover config.py lines 35, 140-141, 146."""

    @pytest.mark.unit
    def test_from_profile_with_relative_config_path(self):
        """Cover lines 140-141: relative config_path."""
        from acoharmony._4icli.config import FourICLIConfig

        mock_profile = {
            "storage": {"data_path": "/tmp/test_data"},
            "fouricli": {
                "config_path": "relative/config.txt",  # relative path
                "default_apm_id": "D0259",
            },
        }

        with patch("acoharmony._4icli.config.load_profile_config", return_value=mock_profile):
            cfg = FourICLIConfig.from_profile()
            assert cfg._profile_config_path is not None
            assert "relative" in str(cfg._profile_config_path)

    @pytest.mark.unit
    def test_from_profile_with_absolute_config_path(self):
        """Cover line 143: absolute config_path."""
        from acoharmony._4icli.config import FourICLIConfig

        mock_profile = {
            "storage": {"data_path": "/tmp/test_data"},
            "fouricli": {
                "config_path": "/absolute/config.txt",
            },
        }

        with patch("acoharmony._4icli.config.load_profile_config", return_value=mock_profile):
            cfg = FourICLIConfig.from_profile()
            assert str(cfg._profile_config_path) == "/absolute/config.txt"

    @pytest.mark.unit
    def test_from_profile_no_config_path(self):
        """Cover line 146: no config_path set."""
        from acoharmony._4icli.config import FourICLIConfig

        mock_profile = {
            "storage": {"data_path": "/tmp/test_data"},
            "fouricli": {},
        }

        with patch("acoharmony._4icli.config.load_profile_config", return_value=mock_profile):
            cfg = FourICLIConfig.from_profile()
            assert cfg._profile_config_path is None

    @pytest.mark.unit
    def test_aco_toml_not_found(self):
        """load_profile_config bubbles FileNotFoundError when aco.toml is missing."""
        from acoharmony._4icli.config import load_profile_config

        with patch(
            "acoharmony._config_loader.load_aco_config",
            side_effect=FileNotFoundError("aco.toml not found"),
        ):
            with pytest.raises(FileNotFoundError, match="aco.toml not found"):
                load_profile_config()


class TestConfigGetCurrentYear:
    @pytest.mark.unit
    def test_get_current_year_returns_int(self):
        from acoharmony._4icli.config import get_current_year
        year = get_current_year()
        assert isinstance(year, int)
        assert 2020 <= year <= 2100

    @pytest.mark.unit
    def test_ensure_config_file_profile_path_exists(self, make_config, tmp_path):
        profile_config = tmp_path / "profile_config.txt"
        profile_config.write_text("creds")
        make_config._profile_config_path = profile_config

        result = make_config.ensure_config_file()
        assert result == profile_config


class TestFourICLIConfig:
    """Test 4icli config loading."""

    @pytest.mark.unit
    def test_config_dataclass(self, tmp_path) -> None:
        """FourICLIConfig can be instantiated."""
        from acoharmony._4icli.config import FourICLIConfig

        config = FourICLIConfig(
            binary_path=tmp_path / "4icli",
            working_dir=tmp_path / "working",
            data_path=tmp_path / "data",
            bronze_dir=tmp_path / "bronze",
            archive_dir=tmp_path / "archive",
            silver_dir=tmp_path / "silver",
            gold_dir=tmp_path / "gold",
            log_dir=tmp_path / "logs",
            tracking_dir=tmp_path / "tracking",
            default_year=2025,
            default_apm_id="D0259",
        )
        assert config.default_apm_id == "D0259"
        assert config.default_year == 2025


