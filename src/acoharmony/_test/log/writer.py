# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for log writer - Polars style."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from pathlib import Path
import pytest
import polars as pl

from acoharmony._log.config import LogConfig
from acoharmony._log.writer import LogWriter


class TestLogWriter:
    """Tests for LogWriter."""

    @pytest.mark.unit
    def test_log_writer_initialization(self) -> None:
        """LogWriter initializes properly."""
        writer = LogWriter("test")
        assert writer is not None
        assert hasattr(writer, "logger")

    @pytest.mark.unit
    def test_log_writer_logging(self) -> None:
        """LogWriter can log messages."""
        writer = LogWriter("test")
        writer.info("Test message")
        writer.warning("Warning message")
        # If no exception, test passes
        assert True


class TestLogWriterGaps:
    """Additional LogWriter coverage."""

    @pytest.mark.unit
    def test_error_method(self, tmp_path):
        """error() writes ERROR level entry."""

        config = LogConfig(_base_path=tmp_path)
        writer = LogWriter("test", config=config)
        writer.error("bad thing happened")

        log_file = writer._get_log_file()
        assert log_file.exists()
        content = log_file.read_text()
        assert "ERROR" in content

    @pytest.mark.unit
    def test_debug_method(self, tmp_path):
        """debug() writes DEBUG level entry."""

        config = LogConfig(_base_path=tmp_path)
        writer = LogWriter("test", config=config)
        writer.debug("debug info")

        log_file = writer._get_log_file()
        content = log_file.read_text()
        assert "DEBUG" in content

    @pytest.mark.unit
    def test_write_metadata(self, tmp_path):
        """write_metadata() writes metadata as log entry."""

        config = LogConfig(_base_path=tmp_path)
        writer = LogWriter("test", config=config)
        writer.write_metadata({"key": "value"})

        log_file = writer._get_log_file()
        content = log_file.read_text()
        assert "Metadata" in content

    @pytest.mark.unit
    def test_add_entry(self, tmp_path):
        """add_entry() writes structured log entry."""

        config = LogConfig(_base_path=tmp_path)
        writer = LogWriter("test", config=config)
        writer.add_entry({"action": "test_action"})

        log_file = writer._get_log_file()
        content = log_file.read_text()
        assert "Entry" in content

    @pytest.mark.unit
    def test_write_session_log(self, tmp_path):
        """write_session_log() returns log file path."""

        config = LogConfig(_base_path=tmp_path)
        writer = LogWriter("test", config=config)
        writer.info("session test")

        path = writer.write_session_log()
        assert path is not None
        assert isinstance(path, Path)

    @pytest.mark.unit
    def test_get_recent_logs_empty(self, tmp_path):
        """get_recent_logs() returns empty DataFrame when no logs."""

        config = LogConfig(_base_path=tmp_path)
        writer = LogWriter("test", config=config)

        df = writer.get_recent_logs()
        assert isinstance(df, pl.DataFrame)
        assert df.height == 0

    @pytest.mark.unit
    def test_get_recent_logs_with_data(self, tmp_path):
        """get_recent_logs() returns DataFrame with recent entries."""

        config = LogConfig(_base_path=tmp_path)
        writer = LogWriter("test", config=config)
        writer.info("line 1")
        writer.warning("line 2")

        df = writer.get_recent_logs(limit=10)
        assert df.height == 2

    @pytest.mark.unit
    def test_cloud_path_log_file(self, tmp_path):
        """Cloud storage path returns string URL."""

        config = LogConfig(_base_path="s3://bucket/logs")
        writer = LogWriter("test", config=config)

        log_file = writer._get_log_file()
        assert isinstance(log_file, str)
        assert log_file.startswith("s3://")

    @pytest.mark.unit
    def test_cloud_log_batches(self, tmp_path):
        """Cloud storage log batches entries and flushes."""

        config = LogConfig(_base_path="s3://bucket/logs")
        writer = LogWriter("test", config=config)

        # Add entries but not enough to trigger auto-flush
        writer.info("msg1")
        assert len(writer.entries) == 1

        # Manual flush
        writer.flush()
        assert len(writer.entries) == 0

    @pytest.mark.unit
    def test_flush_no_entries(self, tmp_path):
        """flush() with no entries is a no-op."""

        config = LogConfig(_base_path=tmp_path)
        writer = LogWriter("test", config=config)
        writer.flush()  # Should not raise

    @pytest.mark.unit
    def test_write_session_log_cloud_returns_none(self):
        """write_session_log() returns None for cloud paths."""

        config = LogConfig(_base_path="s3://bucket/logs")
        writer = LogWriter("test", config=config)
        result = writer.write_session_log()
        assert result is None

    @pytest.mark.unit
    def test_get_recent_logs_nonexistent_file(self, tmp_path):
        """get_recent_logs() returns empty DataFrame for nonexistent local file."""

        config = LogConfig(_base_path=tmp_path / "subdir_that_has_no_logs")
        writer = LogWriter("test", config=config)
        df = writer.get_recent_logs()
        assert df.height == 0


class TestLogWriterFlush:
    """Tests for LogWriter.flush() branch coverage."""

    @pytest.mark.unit
    def test_flush_local_path_does_not_upload(self, tmp_path):
        """flush() with local log_file skips cloud upload (branch 125->-118).

        Manually inject entries (normally only populated for cloud paths),
        then flush with a local base_path. The cloud branch is not taken,
        so entries remain.
        """
        config = LogConfig(_base_path=tmp_path)
        writer = LogWriter("test", config=config)
        # Manually add entries to simulate batched cloud writes
        writer.entries = [{"message": "entry1"}, {"message": "entry2"}]
        writer.flush()
        # Entries NOT cleared because log_file is a local Path, not cloud URL
        assert len(writer.entries) == 2


class TestLogWriterBranches:
    """Cover branches 55->57 (cloud path), 88->94 (cloud write),
    95->96/99 (batch size)."""

    @pytest.mark.unit
    def test_get_log_file_cloud_path(self):
        """Branch 55->57: base_path is a cloud URL string."""
        from unittest.mock import MagicMock
        config = LogConfig(_base_path="s3://bucket/logs")
        writer = LogWriter("test", config=config)
        log_file = writer._get_log_file()
        assert isinstance(log_file, str)
        assert log_file.startswith("s3://")

    @pytest.mark.unit
    def test_get_log_file_local_path(self, tmp_path):
        """Branch 55->58: base_path is a local Path."""
        config = LogConfig(_base_path=tmp_path)
        writer = LogWriter("test", config=config)
        log_file = writer._get_log_file()
        assert isinstance(log_file, Path)

    @pytest.mark.unit
    def test_log_cloud_batches(self):
        """Branch 88->94: cloud storage, entries batched."""
        config = LogConfig(_base_path="s3://bucket/logs")
        writer = LogWriter("test", config=config)
        writer.log("INFO", "test message")
        assert len(writer.entries) == 1

    @pytest.mark.unit
    def test_log_cloud_batch_flush(self):
        """Branch 95->96: batch size reached, flush called."""
        from unittest.mock import patch as _p
        config = LogConfig(_base_path="s3://bucket/logs")
        writer = LogWriter("test", config=config)
        # Fill to batch size
        with _p.object(writer, 'flush') as mock_flush:
            for i in range(100):
                writer.log("INFO", f"msg {i}")
            mock_flush.assert_called()

    @pytest.mark.unit
    def test_log_local_writes_to_file(self, tmp_path):
        """Branch 88->89: local file, writes immediately."""
        config = LogConfig(_base_path=tmp_path)
        writer = LogWriter("test", config=config)
        writer.log("INFO", "hello local")
        log_file = writer._get_log_file()
        assert log_file.exists()

    @pytest.mark.unit
    def test_log_below_batch_size(self):
        """Branch 95->99: entries below batch size, no flush."""
        config = LogConfig(_base_path="s3://bucket/logs")
        writer = LogWriter("test", config=config)
        writer.log("INFO", "test")
        assert len(writer.entries) == 1  # No flush happened


class TestRedactSensitive:
    """Cover _redact_sensitive helper used to scrub PHI from log kwargs."""

    @pytest.mark.unit
    def test_redacts_sensitive_dict_keys(self):
        from acoharmony._log.writer import _redact_sensitive

        result = _redact_sensitive({"mbi": "1A2B3C", "count": 5})
        assert result == {"mbi": "<REDACTED>", "count": 5}

    @pytest.mark.unit
    def test_redacts_nested_dict_keys(self):
        from acoharmony._log.writer import _redact_sensitive

        result = _redact_sensitive({"outer": {"npi": "123", "ok": "yes"}})
        assert result == {"outer": {"npi": "<REDACTED>", "ok": "yes"}}

    @pytest.mark.unit
    def test_walks_into_lists_and_tuples(self):
        from acoharmony._log.writer import _redact_sensitive

        result = _redact_sensitive([{"mbi": "x"}, ({"npi": "y"},)])
        assert result == [{"mbi": "<REDACTED>"}, ({"npi": "<REDACTED>"},)]
        assert isinstance(result[1], tuple)

    @pytest.mark.unit
    def test_walks_into_sets(self):
        from acoharmony._log.writer import _redact_sensitive

        result = _redact_sensitive({"a", "b"})
        assert result == {"a", "b"}
        assert isinstance(result, set)

    @pytest.mark.unit
    def test_passthrough_for_scalars(self):
        from acoharmony._log.writer import _redact_sensitive

        assert _redact_sensitive(42) == 42
        assert _redact_sensitive("plain string") == "plain string"
        assert _redact_sensitive(None) is None

    @pytest.mark.unit
    def test_log_redacts_phi_kwargs_in_file_output(self, tmp_path):
        """End-to-end: PHI keys in kwargs do NOT appear in the persisted log line."""
        config = LogConfig(_base_path=tmp_path)
        writer = LogWriter("test", config=config)
        writer.log("INFO", "Processing claim", mbi="1A2B3C4D5E6", npi="1234567890", count=3)

        log_file = writer._get_log_file()
        content = log_file.read_text()
        assert "1A2B3C4D5E6" not in content
        assert "1234567890" not in content
        assert "<REDACTED>" in content
        assert '"count": 3' in content
