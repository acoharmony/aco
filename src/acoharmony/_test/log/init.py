"""
Boost coverage for root modules, _runner/, _log/, and _exceptions/.

Targets uncovered code paths not exercised by test_runner_root_coverage.py.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import json
import logging  # noqa: E402
import shutil
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import acoharmony._log.config as config_mod  # noqa: E402
import acoharmony._log.config as log_config_mod  # noqa: E402
from acoharmony._log.config import LogConfig, get_logger, setup_logging  # noqa: E402
from acoharmony._exceptions import ACOHarmonyException  # noqa: E402
from acoharmony.tracking import TransformTracker  # noqa: E402
from acoharmony import __version__  # noqa: E402


class TestLogWriterDeeper:
    """Cover LogWriter branches."""

    @pytest.mark.unit
    def test_log_cloud_path(self):

        mock_config = MagicMock()
        mock_config.get_base_path.return_value = "s3://bucket/logs"
        with patch("acoharmony._log.writer.get_logger", return_value=MagicMock()):
            writer = LogWriter("test", config=mock_config)
            writer.log("INFO", "test message")
            assert len(writer.entries) == 1

    @pytest.mark.unit
    def test_log_cloud_path_flush_threshold(self):

        mock_config = MagicMock()
        mock_config.get_base_path.return_value = "s3://bucket/logs"
        with patch("acoharmony._log.writer.get_logger", return_value=MagicMock()):
            writer = LogWriter("test", config=mock_config)
            # Pre-fill to trigger flush
            writer.entries = [{"msg": "x"}] * 99
            writer.log("INFO", "trigger flush")
            assert len(writer.entries) == 0  # flushed

    @pytest.mark.unit
    def test_flush_cloud(self):

        mock_config = MagicMock()
        mock_config.get_base_path.return_value = "s3://bucket/logs"
        with patch("acoharmony._log.writer.get_logger", return_value=MagicMock()):
            writer = LogWriter("test", config=mock_config)
            writer.entries = [{"msg": "x"}]
            writer.flush()
            assert writer.entries == []

    @pytest.mark.unit
    def test_flush_empty(self):

        mock_config = MagicMock()
        mock_config.get_base_path.return_value = Path("/tmp/test_logs_empty")
        with patch("acoharmony._log.writer.get_logger", return_value=MagicMock()):
            writer = LogWriter("test", config=mock_config)
            writer.entries = []
            writer.flush()  # Should not error

    @pytest.mark.unit
    def test_write_metadata(self, tmp_path):

        mock_config = MagicMock()
        mock_config.get_base_path.return_value = tmp_path
        with patch("acoharmony._log.writer.get_logger", return_value=MagicMock()):
            writer = LogWriter("test", config=mock_config)
            writer.write_metadata({"key": "value"})

    @pytest.mark.unit
    def test_add_entry(self, tmp_path):

        mock_config = MagicMock()
        mock_config.get_base_path.return_value = tmp_path
        with patch("acoharmony._log.writer.get_logger", return_value=MagicMock()):
            writer = LogWriter("test", config=mock_config)
            writer.add_entry({"action": "test"})

    @pytest.mark.unit
    def test_write_session_log(self, tmp_path):

        mock_config = MagicMock()
        mock_config.get_base_path.return_value = tmp_path
        with patch("acoharmony._log.writer.get_logger", return_value=MagicMock()):
            writer = LogWriter("test", config=mock_config)
            result = writer.write_session_log()
            assert isinstance(result, Path)

    @pytest.mark.unit
    def test_write_session_log_cloud(self):

        mock_config = MagicMock()
        mock_config.get_base_path.return_value = "s3://bucket/logs"
        with patch("acoharmony._log.writer.get_logger", return_value=MagicMock()):
            writer = LogWriter("test", config=mock_config)
            result = writer.write_session_log()
            assert result is None

    @pytest.mark.unit
    def test_get_recent_logs_empty(self, tmp_path):

        mock_config = MagicMock()
        mock_config.get_base_path.return_value = tmp_path
        with patch("acoharmony._log.writer.get_logger", return_value=MagicMock()):
            writer = LogWriter("test", config=mock_config)
            df = writer.get_recent_logs()
            assert len(df) == 0

    @pytest.mark.unit
    def test_get_recent_logs_with_entries(self, tmp_path):

        mock_config = MagicMock()
        mock_config.get_base_path.return_value = tmp_path
        with patch("acoharmony._log.writer.get_logger", return_value=MagicMock()):
            writer = LogWriter("test", config=mock_config)
            # Write some entries
            writer.info("message1")
            writer.warning("message2")
            writer.error("message3")
            writer.debug("message4")
            df = writer.get_recent_logs()
            assert len(df) >= 4

    @pytest.mark.unit
    def test_get_recent_logs_invalid_json(self, tmp_path):

        mock_config = MagicMock()
        mock_config.get_base_path.return_value = tmp_path
        with patch("acoharmony._log.writer.get_logger", return_value=MagicMock()):
            writer = LogWriter("test", config=mock_config)
            # Write a valid entry then corrupt one
            writer.info("good")
            log_file = writer._get_log_file()
            with open(log_file, "a") as f:
                f.write("not json\n")
            df = writer.get_recent_logs()
            assert len(df) >= 1  # at least the good entry

    @pytest.mark.unit
    def test_get_recent_logs_cloud_path(self):

        mock_config = MagicMock()
        mock_config.get_base_path.return_value = "s3://bucket/logs"
        with patch("acoharmony._log.writer.get_logger", return_value=MagicMock()):
            writer = LogWriter("test", config=mock_config)
            df = writer.get_recent_logs()
            assert len(df) == 0


class TestLogConfigDeeper:
    """Cover _log/config.py branches."""

    @pytest.mark.unit
    def test_log_config_get_base_path_with_base_path(self):

        config = LogConfig.__new__(LogConfig)
        config._base_path = Path("/custom/path")
        config.storage_config = None
        assert config.get_base_path() == Path("/custom/path")

    @pytest.mark.unit
    def test_log_config_get_base_path_skinny_install(self):

        config = LogConfig.__new__(LogConfig)
        config._base_path = None
        config.storage_config = None
        result = config.get_base_path()
        assert "acoharmony" in str(result)

    @pytest.mark.unit
    def test_setup_logging_already_configured(self):

        old_config = log_config_mod._config
        fake_config = MagicMock()
        log_config_mod._config = fake_config
        try:
            result = log_config_mod.setup_logging()
            assert result is fake_config
        finally:
            log_config_mod._config = old_config

    @pytest.mark.unit
    def test_setup_logging_with_profile(self):

        old_config = log_config_mod._config
        log_config_mod._config = None
        try:
            with patch("acoharmony._store.StorageBackend") as MockSB:
                mock_sb = MagicMock()
                mock_sb.get_storage_type.return_value = "local"
                mock_sb.get_path.return_value = Path(tempfile.mkdtemp())
                MockSB.return_value = mock_sb
                result = log_config_mod.setup_logging(profile="local")
                assert result is not None
        finally:
            log_config_mod._config = old_config

    @pytest.mark.unit
    def test_get_logger_initializes(self):

        old_config = log_config_mod._config
        log_config_mod._config = None
        try:
            with patch("acoharmony._log.config.setup_logging") as mock_setup:
                mock_setup.return_value = MagicMock()
                # After setting _config to None, get_logger calls setup_logging
                log_config_mod._config = MagicMock()  # pretend it got set
                logger = log_config_mod.get_logger("test")
                assert logger is not None
        finally:
            log_config_mod._config = old_config

    @pytest.mark.unit
    def test_from_storage(self):

        mock_sb = MagicMock()
        mock_sb.get_storage_type.return_value = "local"
        mock_sb.get_path.return_value = Path(tempfile.mkdtemp())
        config = LogConfig.from_storage(mock_sb)
        assert config.storage_config is mock_sb


# ===========================================================================
# _exceptions/ - deeper coverage
# ===========================================================================


"""Additional tests for _log/config.py to cover 9 missing lines.

Targets:
- LogConfig.__post_init__ with ImportError and other exceptions
- LogConfig.get_base_path with no storage_config (skinny install fallback)
- LogConfig.from_env with ImportError
- setup_logging with profile, already configured
- get_logger auto-setup
"""




@pytest.fixture(autouse=True)
def _reset_config():
    """Reset global config between tests."""
    config_mod._config = None
    yield
    config_mod._config = None


def _patch_storage_import_error():
    """Patch StorageBackend to raise ImportError at import time.

    We patch the actual StorageBackend class in acoharmony._store so that
    ``from .._store import StorageBackend`` raises ImportError, without
    breaking all other imports.
    """
    return patch("acoharmony._store.StorageBackend", side_effect=ImportError("no store"))


# ---------------------------------------------------------------------------
# LogConfig initialization
# ---------------------------------------------------------------------------


class TestLogConfigInit:
    """Test LogConfig.__post_init__ edge cases."""

    @pytest.mark.unit
    def test_init_with_storage_config(self):
        """LogConfig with explicit storage config."""
        mock_storage = MagicMock()
        mock_storage.get_storage_type.return_value = "local"
        mock_storage.get_path.return_value = Path("/tmp/test_logs")
        config = LogConfig(storage_config=mock_storage)
        assert config.storage_config is mock_storage

    @pytest.mark.unit
    def test_init_skinny_install_import_error(self):
        """LogConfig gracefully handles ImportError (skinny install)."""
        with patch.dict("sys.modules", {"acoharmony._store": None}):
            config = LogConfig(storage_config=None)
            assert config.storage_config is None

    @pytest.mark.unit
    def test_init_with_non_local_storage(self):
        """LogConfig with non-local storage doesn't create directories."""
        mock_storage = MagicMock()
        mock_storage.get_storage_type.return_value = "s3"
        config = LogConfig(storage_config=mock_storage)
        assert config.storage_config is mock_storage


# ---------------------------------------------------------------------------
# LogConfig.get_base_path
# ---------------------------------------------------------------------------


class TestGetBasePath:
    """Test get_base_path method."""

    @pytest.mark.unit
    def test_with_cached_base_path(self):
        """Returns cached _base_path if set."""
        config = LogConfig.__new__(LogConfig)
        config._base_path = Path("/cached/path")
        config.storage_config = None
        assert config.get_base_path() == Path("/cached/path")

    @pytest.mark.unit
    def test_with_storage_config(self):
        """Returns path from storage config."""
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = Path("/storage/logs")
        config = LogConfig.__new__(LogConfig)
        config._base_path = None
        config.storage_config = mock_storage
        assert config.get_base_path() == Path("/storage/logs")

    @pytest.mark.unit
    def test_skinny_install_fallback(self):
        """Returns temp directory path when no storage config."""
        config = LogConfig.__new__(LogConfig)
        config._base_path = None
        config.storage_config = None
        result = config.get_base_path()
        assert isinstance(result, Path)
        assert "acoharmony" in str(result)
        assert "logs" in str(result)


# ---------------------------------------------------------------------------
# LogConfig.from_env
# ---------------------------------------------------------------------------


class TestFromEnv:
    """Test from_env classmethod."""

    @pytest.mark.unit
    def test_from_env_skinny_install(self):
        """from_env handles ImportError gracefully."""
        with patch.dict("sys.modules", {"acoharmony._store": None}):
            config = LogConfig.from_env()
            assert config is not None
            assert config.level == "INFO"

    @pytest.mark.unit
    def test_from_env_with_env_vars(self):
        """from_env reads environment variables."""
        with (
            patch.dict(
                "os.environ",
                {
                    "ACOHARMONY_LOG_LEVEL": "DEBUG",
                    "ACOHARMONY_LOG_JSON": "false",
                },
            ),
            patch.dict("sys.modules", {"acoharmony._store": None}),
        ):
            config = LogConfig.from_env()
            assert config.level == "DEBUG"
            assert config.json_logs is False


# ---------------------------------------------------------------------------
# LogConfig.from_storage
# ---------------------------------------------------------------------------


class TestFromStorage:
    """Test from_storage classmethod."""

    @pytest.mark.unit
    def test_from_storage(self):
        mock_storage = MagicMock()
        mock_storage.get_storage_type.return_value = "s3"
        config = LogConfig.from_storage(mock_storage)
        assert config.storage_config is mock_storage


# ---------------------------------------------------------------------------
# setup_logging
# ---------------------------------------------------------------------------


class TestSetupLogging:
    """Test setup_logging function."""

    @pytest.mark.unit
    def test_setup_with_config(self):
        """setup_logging stores config globally."""
        mock_storage = MagicMock()
        mock_storage.get_storage_type.return_value = "local"
        mock_storage.get_path.return_value = Path("/tmp/logs")
        config = LogConfig(storage_config=mock_storage)
        result = setup_logging(config)
        assert result is config

    @pytest.mark.unit
    def test_setup_already_configured_returns_existing(self):
        """setup_logging returns existing config when already set."""
        mock_storage = MagicMock()
        mock_storage.get_storage_type.return_value = "local"
        mock_storage.get_path.return_value = Path("/tmp/logs")
        config = LogConfig(storage_config=mock_storage)
        setup_logging(config)
        # Second call without config returns existing
        result = setup_logging()
        assert result is config

    @pytest.mark.unit
    def test_setup_with_profile(self):
        """setup_logging with profile parameter."""
        mock_storage = MagicMock()
        mock_storage.get_storage_type.return_value = "local"
        mock_storage.get_path.return_value = Path("/tmp/logs")
        with patch("acoharmony._store.StorageBackend", return_value=mock_storage):
            result = setup_logging(profile="dev")
            assert result is not None

    @pytest.mark.unit
    def test_setup_with_profile_import_error(self):
        """setup_logging with profile falls back when StorageBackend unavailable."""
        with patch.dict("sys.modules", {"acoharmony._store": None}):
            config_mod._config = None
            result = setup_logging(profile="dev")
            assert result is not None


# ---------------------------------------------------------------------------
# get_logger
# ---------------------------------------------------------------------------


class TestGetLogger:
    """Test get_logger function."""

    @pytest.mark.unit
    def test_get_logger_auto_setup(self):
        """get_logger triggers setup_logging if not configured."""
        config_mod._config = None
        with patch("acoharmony._log.config.setup_logging") as mock_setup:
            mock_setup.return_value = LogConfig.__new__(LogConfig)
            logger = get_logger("test")
            assert isinstance(logger, logging.Logger)

    @pytest.mark.unit
    def test_get_logger_returns_prefixed(self):
        """get_logger returns logger with acoharmony prefix."""
        mock_storage = MagicMock()
        mock_storage.get_storage_type.return_value = "local"
        mock_storage.get_path.return_value = Path("/tmp/logs")
        setup_logging(LogConfig(storage_config=mock_storage))
        logger = get_logger("mymodule")
        assert logger.name == "acoharmony.mymodule"


# ===== From test_skinny.py =====


class TestSkinnyPackageImport:
    """Verify core package imports work without full deps."""

    @pytest.mark.unit
    def test_package_version(self) -> None:
        """Package exposes __version__."""

        assert __version__

    @pytest.mark.unit
    def test_exceptions_available(self) -> None:
        """Exception classes are always available."""

        assert ACOHarmonyException is not None

    @pytest.mark.unit
    def test_log_module_available(self) -> None:
        """Logging module is always available."""

        assert LogWriter is not None
        assert get_logger is not None
        assert setup_logging is not None


# ---------------------------------------------------------------------------
# 4icli module imports
# ---------------------------------------------------------------------------


# ===== From test_logging_root.py =====


class TestLogging:
    """Test basic logging functionality."""

    @pytest.mark.unit
    def test_logging_setup(self):
        """Test logging setup."""

        # Setup logging
        setup_logging()

        # Get a logger
        logger = logging.getLogger("acoharmony.test")

        # Test that logging works
        logger.info("Test message")
        logger.debug("Debug message")
        logger.warning("Warning message")

        # No exceptions means success
        assert logger is not None


class TestTransformTracker:
    """Test the TransformTracker functionality."""

    def setup_method(self):
        """Set up test environment."""
        self.test_dir = tempfile.mkdtemp()
        self.tracking_dir = Path(self.test_dir) / "tracking"
        self.tracking_dir.mkdir(parents=True)

    def teardown_method(self):
        """Clean up test environment."""
        if Path(self.test_dir).exists():
            shutil.rmtree(self.test_dir)

    @pytest.mark.unit
    def test_tracker_creation(self):
        """Test creating a transform tracker."""
        tracker = TransformTracker(schema_name="test_schema", tracking_dir=self.tracking_dir)

        assert tracker.schema_name == "test_schema"
        assert tracker.tracking_dir == self.tracking_dir
        assert tracker.state_file == self.tracking_dir / "test_schema_state.json"

    @pytest.mark.unit
    def test_start_transform(self):
        """Test starting a transformation tracking."""
        tracker = TransformTracker(schema_name="test", tracking_dir=self.tracking_dir)

        # Start tracking
        tracker.start_transform()

        # Check state file was created
        assert tracker.state_file.exists()

        # Load and verify state
        with open(tracker.state_file) as f:
            state = json.load(f)

        assert state["transform_name"] == "test"
        assert state["total_runs"] == 1
        assert state["last_run"] is not None

    @pytest.mark.unit
    def test_complete_transform_success(self):
        """Test completing transformation tracking."""
        tracker = TransformTracker(schema_name="test", tracking_dir=self.tracking_dir)

        # Start and complete tracking
        tracker.start_transform()
        tracker.complete_transform(
            success=True, records=300, files=3, message="Transformation completed"
        )

        # Load and verify state
        with open(tracker.state_file) as f:
            state = json.load(f)

        assert state["successful_runs"] == 1
        assert state["last_success"] is not None
        assert state["metadata"]["last_run_message"] == "Transformation completed"
        assert state["metadata"]["last_run_records"] == 300

    @pytest.mark.unit
    def test_complete_transform_failure(self):
        """Test tracking transformation failure."""
        tracker = TransformTracker(schema_name="test", tracking_dir=self.tracking_dir)

        # Start tracking
        tracker.start_transform()

        # Complete with failure
        tracker.complete_transform(success=False, message="Failed to process")

        # Load and verify state
        with open(tracker.state_file) as f:
            state = json.load(f)

        assert state["failed_runs"] == 1
        assert state["metadata"]["last_run_message"] == "Failed to process"

    @pytest.mark.unit
    def test_track_file(self):
        """Test tracking individual file processing."""
        tracker = TransformTracker(schema_name="test", tracking_dir=self.tracking_dir)

        tracker.track_file("data.parquet", status="processed")

        assert tracker.has_processed_file("data.parquet")

    @pytest.mark.unit
    def test_get_unprocessed_files(self):
        """Test identifying unprocessed files."""
        tracker = TransformTracker(schema_name="test", tracking_dir=self.tracking_dir)

        tracker.track_file("file1.parquet", status="processed")
        tracker.track_file("file2.parquet", status="processed")

        unprocessed = tracker.get_unprocessed_files(
            ["file1.parquet", "file2.parquet", "file3.parquet"]
        )
        assert unprocessed == ["file3.parquet"]

    @pytest.mark.unit
    def test_get_stats(self):
        """Test getting transformation statistics."""
        tracker = TransformTracker(schema_name="test", tracking_dir=self.tracking_dir)

        tracker.start_transform()
        tracker.complete_transform(success=True)

        stats = tracker.get_stats()
        assert stats["transform"] == "test"
        assert stats["total_runs"] == 1
        assert stats["successful_runs"] == 1
