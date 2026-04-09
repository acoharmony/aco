"""Unit tests for log config - Polars style."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from pathlib import Path
from unittest.mock import MagicMock, patch
import pytest

import acoharmony._log.config as log_config_mod
import acoharmony._log.config as log_config_module
from acoharmony._log.config import LogConfig, setup_logging, get_logger


class TestLogConfig:
    """Tests for LogConfig."""

    @pytest.mark.unit
    def test_log_config_initialization(self) -> None:
        """LogConfig initializes properly."""
        config = LogConfig()
        assert config is not None
        assert hasattr(config, "storage_config")

    @pytest.mark.unit
    def test_storage_non_import_exception(self) -> None:
        with patch("acoharmony._store.StorageBackend", side_effect=RuntimeError("bad")):
            with pytest.raises(Exception, match=r".*"):
                LogConfig()

    @pytest.mark.unit
    def test_from_env_storage_fails(self) -> None:
        with patch("acoharmony._store.StorageBackend", side_effect=RuntimeError("bad")):
            with pytest.raises(Exception, match=r".*"):
                LogConfig.from_env()

    @pytest.mark.unit
    def test_get_logger_when_config_none(self) -> None:
        old = log_config_mod._config
        try:
            log_config_mod._config = None
            logger = log_config_mod.get_logger("test_gap_module")
            assert logger is not None
        finally:
            log_config_mod._config = old


class TestLogConfigGaps:
    """Additional LogConfig coverage."""

    @pytest.mark.unit
    def test_get_base_path_with_explicit_base(self):
        """get_base_path returns explicit _base_path when set."""

        config = LogConfig(_base_path="/tmp/my_logs")
        result = config.get_base_path()
        assert result == "/tmp/my_logs"

    @pytest.mark.unit
    def test_get_base_path_fallback(self):
        """get_base_path returns temp dir when no storage config."""

        config = LogConfig(storage_config=None, _base_path=None)
        # Force storage_config to None after init
        config.storage_config = None
        config._base_path = None
        result = config.get_base_path()
        assert isinstance(result, Path)
        assert "acoharmony" in str(result)

    @pytest.mark.unit
    def test_from_storage(self):
        """from_storage() creates config with given storage."""

        mock_storage = MagicMock()
        config = LogConfig.from_storage(mock_storage)
        assert config.storage_config is mock_storage

    @pytest.mark.unit
    def test_setup_logging_returns_config(self):
        """setup_logging() returns a LogConfig."""

        # Reset global config
        old_config = log_config_module._config
        log_config_module._config = None

        try:
            config = LogConfig(_base_path="/tmp/test_logs")
            result = setup_logging(config=config)
            assert result is config
        finally:
            log_config_module._config = old_config

    @pytest.mark.unit
    def test_setup_logging_cached(self):
        """setup_logging() returns cached config on second call."""

        old_config = log_config_module._config
        log_config_module._config = None

        try:
            config = LogConfig(_base_path="/tmp/test_logs")
            result1 = setup_logging(config=config)
            result2 = setup_logging()  # Should return cached
            assert result2 is result1
        finally:
            log_config_module._config = old_config

    @pytest.mark.unit
    def test_get_logger(self):
        """get_logger() returns a Logger instance."""

        logger = get_logger("test_module")
        assert logger.name == "acoharmony.test_module"


class TestLogConfigBranches:
    """Cover branches 70->-47, 72->-47, 84->85, 87->91, 174->175,
    177->192, 178->180, 196->197, 224->225."""

    @pytest.mark.unit
    def test_post_init_local_storage_creates_dir(self, tmp_path):
        """Branch 70->local, 72->73: local storage, log_path is Path, mkdir called."""
        mock_storage = MagicMock()
        mock_storage.get_storage_type.return_value = "local"
        mock_storage.get_path.return_value = tmp_path / "logs"

        config = LogConfig(storage_config=mock_storage)
        assert (tmp_path / "logs").exists()

    @pytest.mark.unit
    def test_get_base_path_explicit(self):
        """Branch 84->85: _base_path is set, returned directly."""
        config = LogConfig(_base_path="/explicit/path")
        assert config.get_base_path() == "/explicit/path"

    @pytest.mark.unit
    def test_get_base_path_from_storage_config(self):
        """Branch 87->88: storage_config exists, uses get_path."""
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = Path("/storage/logs")

        config = LogConfig(storage_config=mock_storage, _base_path=None)
        # Force _base_path to None after init
        config._base_path = None
        result = config.get_base_path()
        assert result == Path("/storage/logs")

    @pytest.mark.unit
    def test_get_base_path_fallback_temp(self):
        """Branch 87->91: no storage_config, falls back to temp dir."""
        config = LogConfig(storage_config=None, _base_path=None)
        config.storage_config = None
        config._base_path = None
        result = config.get_base_path()
        assert isinstance(result, Path)
        assert "acoharmony" in str(result)

    @pytest.mark.unit
    def test_setup_logging_already_configured(self):
        """Branch 174->175: _config is not None, returns cached."""
        old_config = log_config_module._config
        try:
            cached = LogConfig(_base_path="/tmp/cached")
            log_config_module._config = cached
            result = setup_logging()  # config=None, returns cached
            assert result is cached
        finally:
            log_config_module._config = old_config

    @pytest.mark.unit
    def test_setup_logging_with_profile(self):
        """Branch 177->178->180: config is None with profile."""
        old_config = log_config_module._config
        log_config_module._config = None
        try:
            result = setup_logging(profile="local")
            assert result is not None
        finally:
            log_config_module._config = old_config

    @pytest.mark.unit
    def test_setup_logging_no_config_no_profile(self):
        """Branch 177->192: config=None, profile=None, from_env used."""
        old_config = log_config_module._config
        log_config_module._config = None
        try:
            result = setup_logging()
            assert result is not None
        finally:
            log_config_module._config = old_config

    @pytest.mark.unit
    def test_get_logger_when_not_configured(self):
        """Branch 224->225: _config is None, setup_logging called first."""
        old_config = log_config_module._config
        log_config_module._config = None
        try:
            logger = get_logger("auto_setup")
            assert logger.name == "acoharmony.auto_setup"
        finally:
            log_config_module._config = old_config

    @pytest.mark.unit
    def test_setup_logging_handler_exists(self):
        """Branch 196->197: root_logger already has handlers, basicConfig skipped."""
        import logging
        old_config = log_config_module._config
        log_config_module._config = None
        try:
            # root logger already has handlers from earlier setup
            root = logging.getLogger()
            had_handlers = len(root.handlers) > 0
            config = LogConfig(_base_path="/tmp/test_handlers")
            result = setup_logging(config=config)
            assert result is config
        finally:
            log_config_module._config = old_config
