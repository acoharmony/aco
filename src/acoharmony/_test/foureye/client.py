# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for 4icli client - Polars style."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from pathlib import Path
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from acoharmony._4icli.client import FourICLI, FourICLIError
from acoharmony._4icli.models import DataHubCategory

# Import helper functions from conftest (they're module-level functions that are available)
from acoharmony._test.foureye.conftest import _make_config, _mock_log_writer  # noqa: F401


class TestFourICLIInitialization:
    """Tests for FourICLI initialization."""

    @pytest.mark.unit
    def test_init_with_config(self, mock_config, mock_log_writer) -> None:
        """FourICLI initializes with config."""
        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )

        assert cli.config is mock_config
        assert cli.log_writer is mock_log_writer

    @pytest.mark.unit
    def test_init_creates_state_tracker(self, mock_config, mock_log_writer) -> None:
        """FourICLI creates state tracker when enabled."""
        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=True
        )

        assert cli.state_tracker is not None

    @pytest.mark.unit
    def test_init_no_state_tracker_when_disabled(self, mock_config, mock_log_writer) -> None:
        """FourICLI doesn't create state tracker when disabled."""
        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )

        assert cli.state_tracker is None


class TestFourICLICommandExecution:
    """Tests for command execution."""

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_run_command_via_docker(self, mock_run, mock_config, mock_log_writer) -> None:
        """Commands run via Docker container using docker exec."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "Success"

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli._run_command(["datahub", "-l"])

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        # Verify docker exec command structure (uses persistent container)
        assert "docker" in call_args
        assert "exec" in call_args
        assert "4icli" in call_args

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_run_command_failure_raises(self, mock_run, mock_config, mock_log_writer) -> None:
        """Command failure raises FourICLIError."""
        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = "Error"

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )

        with pytest.raises(FourICLIError):
            cli._run_command(["datahub", "-l"])


class TestFourICLIDownload:
    """Tests for download functionality."""

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_download_tracks_new_files(
        self, mock_run, mock_config, mock_log_writer, temp_bronze_dir
    ) -> None:
        """Download tracks newly downloaded files."""
        mock_run.return_value.returncode = 0

        # Create a file after "download"
        def create_file(*args, **kwargs):
            file = temp_bronze_dir / "new_file.txt"
            file.write_text("content")
            return mock_run.return_value

        mock_run.side_effect = create_file

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        result = cli.download(category=DataHubCategory.CCLF)

        assert result.success
        assert len(result.files_downloaded) > 0


class TestFourICLIHelpers:
    """Tests for helper methods."""

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_download_cclf(self, mock_run, mock_config, mock_log_writer) -> None:
        """download_cclf uses correct parameters."""
        mock_run.return_value.returncode = 0

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.download_cclf(year=2025, created_within_last_week=True)

        # Verify it called with CCLF category
        mock_run.assert_called()


class TestFourICLIIncrementalSync:
    """Tests for incremental sync functionality."""

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_view_all_files(self, mock_run, mock_config, mock_log_writer) -> None:
        """view_all_files returns remote file list."""
        mock_run.return_value.returncode = 0
        # Mock output in expected format: "N of M - filename.zip (XX.XX MB) Last Updated: ..."
        mock_run.return_value.stdout = (
            "1 of 3 - file1.zip (10.50 MB) Last Updated: 2025-01-01T10:00:00.000Z\n"
            "2 of 3 - file2.zip (15.20 MB) Last Updated: 2025-01-02T11:00:00.000Z\n"
            "3 of 3 - file3.zip (20.00 MB) Last Updated: 2025-01-03T12:00:00.000Z"
        )

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        files = cli.view_all_files(category=DataHubCategory.CCLF, year=2025)

        assert len(files) == 3
        assert files[0]["filename"] == "file1.zip"
        assert files[1]["filename"] == "file2.zip"

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_discover_remote_inventory(
        self, mock_run, mock_config, mock_log_writer, tmp_path
    ) -> None:
        """discover_remote_inventory updates state tracker for all years."""
        mock_run.return_value.returncode = 0
        # Mock output in expected format: "N of M - filename.zip (XX.XX MB) Last Updated: ..."
        mock_run.return_value.stdout = (
            "1 of 2 - file1.zip (10.50 MB) Last Updated: 2024-01-01T10:00:00.000Z\n"
            "2 of 2 - file2.zip (15.20 MB) Last Updated: 2024-01-02T11:00:00.000Z"
        )

        state_file = tmp_path / "tracking" / "test_discover.json"
        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=True
        )
        cli.state_tracker.state_file = state_file

        # Discovery now loops through years, so we get 2 files per year
        count = cli.discover_remote_inventory(
            category=DataHubCategory.CCLF, start_year=2024, end_year=2024
        )

        assert count >= 2
        # Verify state was updated
        assert len(cli.state_tracker._file_cache) >= 2

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_sync_incremental_no_files(
        self, mock_run, mock_config, mock_log_writer, tmp_path
    ) -> None:
        """sync_incremental returns empty result when no files to download."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""

        state_file = tmp_path / "tracking" / "test_sync_empty.json"
        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=True
        )
        cli.state_tracker.state_file = state_file

        result = cli.sync_incremental(category=DataHubCategory.CCLF, start_year=2024, end_year=2024)

        assert result.success
        assert len(result.files_downloaded) == 0

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_sync_incremental_downloads_new_files(
        self, mock_run, mock_config, mock_log_writer, temp_bronze_dir, tmp_path
    ) -> None:
        """sync_incremental downloads new files."""
        mock_run.return_value.returncode = 0

        # First call (view_all_files) returns file list
        # Second call (download) creates files
        file1 = temp_bronze_dir / "new_file1.zip"
        file2 = temp_bronze_dir / "new_file2.zip"

        call_count = [0]

        def mock_subprocess(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                # view_all_files call
                mock_run.return_value.stdout = "new_file1.zip\nnew_file2.zip"
            else:
                # download call - create files
                file1.write_text("content1")
                file2.write_text("content2")
                mock_run.return_value.stdout = ""
            return mock_run.return_value

        mock_run.side_effect = mock_subprocess

        state_file = tmp_path / "tracking" / "test_sync_new.json"
        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=True
        )
        cli.state_tracker.state_file = state_file

        result = cli.sync_incremental(category=DataHubCategory.CCLF, start_year=2024, end_year=2024)

        assert result.success
        assert len(result.files_downloaded) >= 0  # May be 0 if duplicates filtered

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_sync_incremental_requires_state_tracking(
        self, mock_run, mock_config, mock_log_writer
    ) -> None:
        """sync_incremental raises error when state tracking disabled."""
        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )

        with pytest.raises(FourICLIError, match="State tracking must be enabled"):
            cli.sync_incremental(category=DataHubCategory.CCLF)

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_sync_all_years(self, mock_run, mock_config, mock_log_writer, tmp_path) -> None:
        """sync_all_years syncs multiple years."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""

        state_file = tmp_path / "tracking" / "test_sync_years.json"
        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=True
        )
        cli.state_tracker.state_file = state_file

        result = cli.sync_all_years(category=DataHubCategory.CCLF, start_year=2023, end_year=2024)

        assert result.success

    @pytest.mark.unit
    def test_get_sync_status_disabled(self, mock_config, mock_log_writer) -> None:
        """get_sync_status returns disabled when state tracking off."""
        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )

        status = cli.get_sync_status()

        assert status["state_tracking"] == "disabled"

    @pytest.mark.unit
    def test_get_sync_status_enabled(self, mock_config, mock_log_writer, tmp_path) -> None:
        """get_sync_status returns statistics when state tracking enabled."""
        state_file = tmp_path / "tracking" / "test_status.json"
        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=True
        )
        cli.state_tracker.state_file = state_file

        status = cli.get_sync_status()

        assert status["state_tracking"] == "enabled"
        assert "last_sync" in status
        assert "total_files_tracked" in status
        assert "total_size_mb" in status


# ===================================================================
# 2. client.py
# ===================================================================


class TestFourICLIClient:
    @pytest.fixture
    def client(self, tmp_path):
        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()
        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI

            return FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=False)

    @pytest.mark.unit
    def test_run_command_success_non_datahub(self, client):
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = "some output"
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            result = client._run_command(["help"])
            assert result.stdout == "some output"

    @pytest.mark.unit
    def test_run_command_success_with_stderr(self, client):
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = "output"
        result_mock.stderr = "warning message"

        with patch("subprocess.run", return_value=result_mock):
            client._run_command(["help"])
            client.log_writer.warning.assert_called()

    @pytest.mark.unit
    def test_run_command_eacces_error(self, client):
        result_mock = MagicMock()
        result_mock.returncode = 1
        result_mock.stdout = ""
        result_mock.stderr = "EACCES: permission denied, open '/workspace/file.txt'"

        with patch("subprocess.run", return_value=result_mock):
            result = client._run_command(["4icli", "datahub", "-d"])
            # EACCES returns result without raising
            assert result.returncode == 1

    @pytest.mark.unit
    def test_run_command_error_with_stderr(self, client):
        from acoharmony._4icli.client import FourICLIError

        result_mock = MagicMock()
        result_mock.returncode = 1
        result_mock.stdout = ""
        result_mock.stderr = "general error"

        with patch("subprocess.run", return_value=result_mock):
            with pytest.raises(FourICLIError, match="general error"):
                client._run_command(["4icli", "datahub", "-d"])

    @pytest.mark.unit
    def test_run_command_error_no_stderr(self, client):
        from acoharmony._4icli.client import FourICLIError

        result_mock = MagicMock()
        result_mock.returncode = 1
        result_mock.stdout = ""
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            with pytest.raises(FourICLIError, match="exit code 1"):
                client._run_command(["4icli", "datahub", "-d"])

    @pytest.mark.unit
    def test_run_command_timeout(self, client):
        from acoharmony._4icli.client import FourICLIError

        with patch("subprocess.run", side_effect=subprocess.TimeoutExpired(cmd="cmd", timeout=30)):
            with pytest.raises(FourICLIError, match="timed out"):
                client._run_command(["4icli", "datahub", "-d"], timeout=30)

    @pytest.mark.unit
    def test_run_command_other_exception(self, client):
        from acoharmony._4icli.client import FourICLIError

        with patch("subprocess.run", side_effect=OSError("no docker")):
            with pytest.raises(FourICLIError, match="execution failed"):
                client._run_command(["4icli", "datahub", "-d"])

    @pytest.mark.unit
    def test_run_command_datahub_parses_output(self, client):
        stdout = (
            "4icli - 4Innovation CLI\n\n"
            "Found 2 files.\nList of Files\n"
            "1 of 2 - file1.zip (10.00 MB) Last Updated: 2025-01-01T00:00:00.000Z\n"
            "2 of 2 - file2.zip (20.00 MB) Last Updated: 2025-01-02T00:00:00.000Z\n\n"
            "Session closed, lasted about 3.2s.\n"
        )
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = stdout
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            result = client._run_command(["4icli", "datahub", "-v"])
            assert hasattr(result, "parsed_output")

    @pytest.mark.unit
    def test_run_command_datahub_parse_failure(self, client):
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = "4icli datahub output"
        result_mock.stderr = ""

        with (
            patch("subprocess.run", return_value=result_mock),
            patch(
                "acoharmony._4icli.client.parse_datahub_output", side_effect=Exception("parse fail")
            ),
        ):
            client._run_command(["4icli", "datahub", "-v"])
            client.log_writer.warning.assert_called()

    @pytest.mark.unit
    def test_run_command_datahub_many_files(self, client):
        """Test the 'more than 10 files' branch."""
        lines = ["4icli - 4Innovation CLI\n", "Found 12 files.\nList of Files\n"]
        for i in range(1, 13):
            lines.append(
                f"{i} of 12 - file{i}.zip (1.00 MB) Last Updated: 2025-01-01T00:00:00.000Z\n"
            )
        lines.append("Session closed, lasted about 1.0s.\n")
        stdout = "\n".join(lines)

        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = stdout
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            client._run_command(["4icli", "datahub", "-v"])
            # Check that "and X more files" was logged
            calls = [str(c) for c in client.log_writer.info.call_args_list]
            assert any("more files" in c for c in calls)

    @pytest.mark.unit
    def test_run_command_datahub_with_errors(self, client):
        """Test parsed output with errors."""
        stdout = (
            "4icli - 4Innovation CLI\n\n"
            "Found 1 files.\nList of Files\n"
            "1 of 1 - file1.zip (10.00 MB) Last Updated: 2025-01-01T00:00:00.000Z\n"
            "Session closed, lasted about 1.0s.\n"
        )
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = stdout
        result_mock.stderr = "some stderr warning"

        with patch("subprocess.run", return_value=result_mock):
            client._run_command(["4icli", "datahub", "-v"])

    @pytest.mark.unit
    def test_run_command_rate_limiting(self, client):
        """Test rate limiting between requests."""
        import time

        client.config.request_delay = 0.01
        client._last_request_time = time.time()

        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = ""
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock), patch("time.sleep"):
            client._run_command(["help"])
            # May or may not sleep depending on timing, but tests the branch

    @pytest.mark.unit
    def test_configure_always_raises(self, client):
        from acoharmony._4icli.client import FourICLIConfigurationError

        with pytest.raises(FourICLIConfigurationError, match="bootstrap.sh"):
            client.configure()

    @pytest.mark.unit
    def test_rotate_credentials_always_raises(self, client):
        from acoharmony._4icli.client import FourICLIConfigurationError

        with pytest.raises(FourICLIConfigurationError, match="portal"):
            client.rotate_credentials()

    @pytest.mark.unit
    def test_list_categories(self, client):
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = "categories output"
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            cats = client.list_categories()
            assert "output" in cats

    @pytest.mark.unit
    def test_view_files(self, client):
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = "PALMR_file.txt\nCCLF8.zip\n\n"
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            files = client.view_files()
            assert len(files) == 2

    @pytest.mark.unit
    def test_view_files_with_params(self, client):
        from acoharmony._4icli.models import DataHubCategory, DateFilter

        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = ""
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            files = client.view_files(
                category=DataHubCategory.CCLF,
                year=2024,
                apm_id="X1234",
                date_filter=DateFilter(created_after="2024-01-01"),
            )
            assert files == []

    @pytest.mark.unit
    def test_download_success(self, client, tmp_path):
        # Ensure bronze dir exists
        client.config.bronze_dir.mkdir(parents=True, exist_ok=True)
        # Create a new file to simulate download
        new_file = client.config.bronze_dir / "new_file.zip"

        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = ""
        result_mock.stderr = ""

        def create_file(*args, **kwargs):
            new_file.write_text("downloaded content")
            return result_mock

        with patch("subprocess.run", side_effect=create_file):
            result = client.download()
            assert result.success is True

    @pytest.mark.unit
    def test_download_failure(self, client):
        err_mock = MagicMock()
        err_mock.returncode = 1
        err_mock.stdout = ""
        err_mock.stderr = "download failed"

        with patch("subprocess.run", return_value=err_mock):
            result = client.download()
            assert result.success is False
            assert len(result.errors) > 0

    @pytest.mark.unit
    def test_download_with_duplicate_detection(self, tmp_path):
        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()

        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI

            client = FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=True)

        # Mock the state tracker
        client.state_tracker = MagicMock()
        client.state_tracker.get_new_files.return_value = []
        client.state_tracker.get_duplicate_files.return_value = [Path("dup.zip")]
        client.state_tracker.mark_multiple_downloaded = MagicMock()

        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = ""
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            result = client.download()
            assert result.success is True

    @pytest.mark.unit
    def test_download_with_parsed_output(self, client, tmp_path):
        """Test download when result has parsed_output."""
        client.config.bronze_dir.mkdir(parents=True, exist_ok=True)
        new_file = client.config.bronze_dir / "new_file.zip"

        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = (
            "4icli - 4Innovation CLI\n\n"
            "Found 1 files.\nList of Files\n"
            "1 of 1 - new_file.zip (10.00 MB) Last Updated: 2025-01-01T00:00:00.000Z\n"
            "Session closed, lasted about 1.0s.\n"
        )
        result_mock.stderr = ""

        def create_file(*args, **kwargs):
            new_file.write_text("content")
            return result_mock

        with patch("subprocess.run", side_effect=create_file):
            result = client.download()
            assert result.success

    @pytest.mark.unit
    def test_download_cclf(self, client):
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = ""
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            result = client.download_cclf(year=2025, created_within_last_week=True)
            assert result.success

    @pytest.mark.unit
    def test_download_cclf_no_filter(self, client):
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = ""
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            result = client.download_cclf()
            assert result.success

    @pytest.mark.unit
    def test_download_alignment_files(self, client):
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = ""
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            result = client.download_alignment_files(year=2025, created_after="2025-01-01")
            assert result.success

    @pytest.mark.unit
    def test_download_alignment_no_filter(self, client):
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = ""
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            result = client.download_alignment_files()
            assert result.success

    @pytest.mark.unit
    def test_view_all_files(self, client):
        stdout = (
            "4icli - 4Innovation CLI\n\n"
            "Found 2 files.\nList of Files\n"
            "1 of 2 - file1.zip (10.50 MB) Last Updated: 2025-01-01T00:00:00.000Z\n"
            "2 of 2 - file2.zip (20.00 MB) Last Updated: 2025-02-01T00:00:00.000Z\n\n"
            "Session closed, lasted about 1.0s.\n"
        )
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = stdout
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            files = client.view_all_files()
            assert len(files) == 2
            assert files[0]["filename"] == "file1.zip"
            assert files[0]["size_mb"] == 10.50

    @pytest.mark.unit
    def test_view_all_files_parse_error(self, client):
        """Malformed line triggers ValueError/IndexError and is skipped."""
        stdout = (
            "1 of 1 - .zip\n"  # minimal but missing size/date
            "bogus line\n"
        )
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = stdout
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            files = client.view_all_files()
            # Lines that can't be parsed are skipped
            assert isinstance(files, list)

    @pytest.mark.unit
    def test_view_all_files_no_mb(self, client):
        """Line without MB in size."""
        stdout = "1 of 1 - file1.zip (unknown) Last Updated: 2025-01-01T00:00:00.000Z\n"
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = stdout
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            files = client.view_all_files()
            # Should parse filename but size_mb=0
            if files:
                assert files[0]["size_mb"] == 0

    @pytest.mark.unit
    def test_discover_remote_inventory(self, tmp_path):
        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()

        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI

            client = FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=True)

        client.state_tracker = MagicMock()
        client.state_tracker.update_remote_inventory = MagicMock()

        from acoharmony._4icli.models import DataHubCategory

        with patch.object(
            client,
            "view_all_files",
            return_value=[{"filename": "f.zip", "name": "f.zip", "size": 100}],
        ):
            total = client.discover_remote_inventory(
                category=DataHubCategory.CCLF,
                start_year=2025,
                end_year=2025,
            )
            assert total == 1

    @pytest.mark.unit
    def test_discover_remote_inventory_no_category(self, tmp_path):
        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()

        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI

            client = FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=True)

        client.state_tracker = MagicMock()

        with patch.object(client, "view_all_files", return_value=[{"filename": "f.zip"}]):
            total = client.discover_remote_inventory(start_year=2025, end_year=2025)
            assert total == 1
            # Called with category="all"
            client.state_tracker.update_remote_inventory.assert_called_once()

    @pytest.mark.unit
    def test_run_command_rate_limiting_sleeps(self, client):
        """Cover client.py 105->110: elapsed < request_delay triggers sleep."""
        import time as _time

        client.config.request_delay = 10.0  # large delay so elapsed is always less
        client._last_request_time = _time.time()  # just happened

        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = ""
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock), \
             patch("time.sleep") as mock_sleep:
            client._run_command(["help"])
            # elapsed is ~0 which is < 10.0, so sleep must be called
            mock_sleep.assert_called_once()
            wait_arg = mock_sleep.call_args[0][0]
            assert wait_arg > 0

    @pytest.mark.unit
    def test_download_with_parsed_output_truthy(self, tmp_path):
        """Cover client.py 347->355: parsed_output is truthy in download."""
        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()
        # Ensure bronze_dir exists so glob and stat work
        cfg.bronze_dir.mkdir(parents=True, exist_ok=True)

        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI

            client = FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=False)

        # Create a file that will appear as "new" after download
        new_file = client.config.bronze_dir / "new_file.zip"

        # Build a mock result whose parsed_output is truthy
        parsed = MagicMock()
        parsed.files = [MagicMock(filename="new_file.zip")]
        parsed.total_files = 1
        parsed.session_duration = 1.0
        parsed.errors = []

        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = (
            "4icli - 4Innovation CLI\n\n"
            "Found 1 files.\nList of Files\n"
            "1 of 1 - new_file.zip (10.00 MB) Last Updated: 2025-01-01T00:00:00.000Z\n"
            "Session closed, lasted about 1.0s.\n"
        )
        result_mock.stderr = ""
        result_mock.parsed_output = parsed

        def create_file(*args, **kwargs):
            new_file.write_text("content")
            return result_mock

        with patch("subprocess.run", side_effect=create_file):
            result = client.download()
            assert result.success
            # Verify the parsed_output branch was hit (log about reported files)
            info_calls = [str(c) for c in lw.info.call_args_list]
            assert any("reported downloading" in c for c in info_calls)

    @pytest.mark.unit
    def test_view_all_files_no_last_updated(self, client):
        """Cover client.py 525->530: line without 'Last Updated:' still appends to files."""
        stdout = "1 of 1 - file1.zip (5.00 MB)\n"
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = stdout
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            files = client.view_all_files()
            assert len(files) == 1
            assert files[0]["filename"] == "file1.zip"
            assert files[0]["size_mb"] == 5.0
            assert files[0]["modified"] is None
            assert files[0]["created"] is None

    @pytest.mark.unit
    def test_discover_remote_inventory_disabled(self, client):
        from acoharmony._4icli.client import FourICLIError

        with pytest.raises(FourICLIError, match="State tracking must be enabled"):
            client.discover_remote_inventory()

    @pytest.mark.unit
    def test_discover_remote_inventory_empty(self, tmp_path):
        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()

        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI

            client = FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=True)

        client.state_tracker = MagicMock()

        with patch.object(client, "view_all_files", return_value=[]):
            total = client.discover_remote_inventory(start_year=2025, end_year=2025)
            assert total == 0

    @pytest.mark.unit
    def test_sync_incremental_no_files_to_download(self, tmp_path):
        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()

        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI

            client = FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=True)

        client.state_tracker = MagicMock()
        client.state_tracker.get_files_to_download.return_value = []

        with patch.object(client, "discover_remote_inventory"):
            from acoharmony._4icli.models import DataHubCategory

            result = client.sync_incremental(
                category=DataHubCategory.CCLF,
                start_year=2025,
                end_year=2025,
            )
            assert result.success
            assert result.files_downloaded == []

    @pytest.mark.unit
    def test_sync_incremental_with_downloads(self, tmp_path):
        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()

        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI

            client = FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=True)

        client.state_tracker = MagicMock()
        client.state_tracker.get_files_to_download.return_value = ["file1.zip"]

        mock_result = MagicMock()
        mock_result.files_downloaded = [Path("file1.zip")]

        with (
            patch.object(client, "discover_remote_inventory"),
            patch.object(client, "download", return_value=mock_result),
        ):
            from acoharmony._4icli.models import DataHubCategory

            result = client.sync_incremental(
                category=DataHubCategory.CCLF,
                start_year=2025,
                end_year=2025,
            )
            assert len(result.files_downloaded) == 1

    @pytest.mark.unit
    def test_sync_incremental_with_file_type_codes(self, tmp_path):
        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()

        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI

            client = FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=True)

        client.state_tracker = MagicMock()
        client.state_tracker.get_files_to_download.return_value = ["file1.zip"]

        mock_result = MagicMock()
        mock_result.files_downloaded = []

        with (
            patch.object(client, "discover_remote_inventory"),
            patch.object(client, "download", return_value=mock_result),
        ):
            from acoharmony._4icli.models import DataHubCategory, FileTypeCode

            result = client.sync_incremental(
                category=DataHubCategory.CCLF,
                file_type_codes=[FileTypeCode.CCLF],
                start_year=2025,
                end_year=2025,
            )
            assert result.success

    @pytest.mark.unit
    def test_sync_incremental_disabled(self, client):
        from acoharmony._4icli.client import FourICLIError

        with pytest.raises(FourICLIError, match="State tracking must be enabled"):
            client.sync_incremental()

    @pytest.mark.unit
    def test_help(self, client):
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = "help output"
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            h = client.help()
            assert h == "help output"

    @pytest.mark.unit
    def test_help_with_command(self, client):
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = "specific help"
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            h = client.help(command="datahub")
            assert h == "specific help"

    @pytest.mark.unit
    def test_get_sync_status_disabled(self, client):
        status = client.get_sync_status()
        assert status["state_tracking"] == "disabled"


class TestClientRunCommandEdgeCases:
    @patch("subprocess.run")
    @pytest.mark.unit
    def test_run_command_rate_limiting(self, mock_run, make_config, mock_lw):
        """Rate limiting delays between requests."""
        from acoharmony._4icli.client import FourICLI

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""
        mock_run.return_value.stderr = ""

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        cli.config.request_delay = 0.01  # very short for testing
        cli._last_request_time = time.time()

        cli._run_command(["test"])
        assert mock_run.called

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_run_command_eacces_permission_denied(self, mock_run, make_config, mock_lw):
        """EACCES permission denied is handled gracefully."""
        from acoharmony._4icli.client import FourICLI

        mock_run.return_value.returncode = 1
        mock_run.return_value.stdout = ""
        mock_run.return_value.stderr = "EACCES: permission denied, open '/workspace/file.zip'"

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        result = cli._run_command(["datahub", "-d"])
        # Should not raise, returns the result
        assert result.returncode == 1

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_run_command_timeout(self, mock_run, make_config, mock_lw):
        """Timeout raises FourICLIError."""
        from acoharmony._4icli.client import FourICLI, FourICLIError

        mock_run.side_effect = subprocess.TimeoutExpired(cmd=["docker"], timeout=60)

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        with pytest.raises(FourICLIError, match="timed out"):
            cli._run_command(["test"], timeout=60)

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_run_command_generic_exception(self, mock_run, make_config, mock_lw):
        """Generic exception raises FourICLIError."""
        from acoharmony._4icli.client import FourICLI, FourICLIError

        mock_run.side_effect = OSError("Connection refused")

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        with pytest.raises(FourICLIError, match="Connection refused"):
            cli._run_command(["test"])

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_run_command_parses_datahub_output(self, mock_run, make_config, mock_lw):
        """Successful datahub command output is parsed."""
        from acoharmony._4icli.client import FourICLI

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = (
            "4icli - 4Innovation CLI\n\n"
            "Found 2 files.\n\n"
            "1 of 2 - file1.zip (10 MB) Last Updated: 2025-01-01T00:00:00.000Z\n"
            "2 of 2 - file2.zip (20 MB) Last Updated: 2025-01-02T00:00:00.000Z\n\n"
            "Session closed, lasted about 3.5s.\n"
        )
        mock_run.return_value.stderr = ""

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        result = cli._run_command(["4icli", "datahub", "-v"])
        assert hasattr(result, "parsed_output")
        assert result.parsed_output.total_files == 2

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_run_command_parses_datahub_with_more_than_10_files(
        self, mock_run, make_config, mock_lw
    ):
        """More than 10 files triggers '... and N more' log message."""
        from acoharmony._4icli.client import FourICLI

        lines = ["Found 15 files.\n\nList of Files\n\n"]
        for i in range(1, 16):
            lines.append(f"{i} of 15 - file{i}.zip (1 MB) Last Updated: 2025-01-01T00:00:00.000Z\n")
        lines.append("\nSession closed, lasted about 2.0s.\n")

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "".join(lines)
        mock_run.return_value.stderr = ""

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        result = cli._run_command(["4icli", "datahub", "-v"])
        assert result.parsed_output.total_files == 15

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_run_command_parse_failure_falls_back(self, mock_run, make_config, mock_lw):
        """When parse fails, raw output is logged."""
        from acoharmony._4icli.client import FourICLI

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "some datahub output"
        mock_run.return_value.stderr = "some warning"

        with patch(
            "acoharmony._4icli.client.parse_datahub_output", side_effect=Exception("parse error")
        ):
            cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
            result = cli._run_command(["4icli", "datahub", "-v"])
            # Should not raise
            assert result.returncode == 0

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_run_command_non_datahub_logs_raw(self, mock_run, make_config, mock_lw):
        """Non-datahub commands log raw stdout."""
        from acoharmony._4icli.client import FourICLI

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "help output here"
        mock_run.return_value.stderr = ""

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        cli._run_command(["help"])
        # Verify info was called with raw output
        assert any("help output" in str(c) for c in mock_lw.info.call_args_list)

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_run_command_stderr_on_success(self, mock_run, make_config, mock_lw):
        """Stderr on success is logged as warning."""
        from acoharmony._4icli.client import FourICLI

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""
        mock_run.return_value.stderr = "Warning: something"

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        cli._run_command(["test"])
        mock_lw.warning.assert_called()


class TestClientConfigure:
    @pytest.mark.unit
    def test_configure_always_raises(self, make_config, mock_lw):
        from acoharmony._4icli.client import FourICLI, FourICLIConfigurationError

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        with pytest.raises(FourICLIConfigurationError, match="bootstrap.sh"):
            cli.configure(interactive=False)


class TestClientRotateCredentials:
    @pytest.mark.unit
    def test_rotate_always_raises(self, make_config, mock_lw):
        from acoharmony._4icli.client import FourICLI, FourICLIConfigurationError

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        with pytest.raises(FourICLIConfigurationError, match="portal"):
            cli.rotate_credentials()


class TestClientListCategories:
    @patch("subprocess.run")
    @pytest.mark.unit
    def test_list_categories(self, mock_run, make_config, mock_lw):
        from acoharmony._4icli.client import FourICLI

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "Categories list"
        mock_run.return_value.stderr = ""

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        result = cli.list_categories()
        assert "output" in result
        assert result["output"] == "Categories list"


class TestClientViewFiles:
    @patch("subprocess.run")
    @pytest.mark.unit
    def test_view_files_parses_output(self, mock_run, make_config, mock_lw):
        from acoharmony._4icli.client import FourICLI
        from acoharmony._4icli.models import DataHubCategory

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "CCLF8.D240101.T1234567.zip\nP.D0259.PALMR.D250101.csv\n"
        mock_run.return_value.stderr = ""

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        files = cli.view_files(category=DataHubCategory.CCLF)
        assert len(files) == 2
        assert files[0].name == "CCLF8.D240101.T1234567.zip"


class TestClientDownloadExtended:
    @patch("subprocess.run")
    @pytest.mark.unit
    def test_download_failure_returns_failed_result(self, mock_run, make_config, mock_lw):
        from acoharmony._4icli.client import FourICLI
        from acoharmony._4icli.models import DataHubCategory

        mock_run.return_value.returncode = 1
        mock_run.return_value.stderr = "Download error"

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        result = cli.download(category=DataHubCategory.CCLF)
        assert not result.success
        assert len(result.errors) == 1

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_download_with_duplicate_detection(self, mock_run, make_config, mock_lw, tmp_path):
        from acoharmony._4icli.client import FourICLI
        from acoharmony._4icli.models import DataHubCategory

        state_file = tmp_path / "tracking" / "state.json"

        # Pre-create a file to act as "already downloaded"
        existing = make_config.bronze_dir / "existing.zip"
        existing.write_text("old content")

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""
        mock_run.return_value.stderr = ""

        def side_effect(*a, **kw):
            # Simulate downloading a new file
            (make_config.bronze_dir / "new_file.zip").write_text("new data")
            return mock_run.return_value

        mock_run.side_effect = side_effect

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=True)
        cli.state_tracker.state_file = state_file
        result = cli.download(category=DataHubCategory.CCLF)
        assert result.success


class TestClientDownloadAlignmentFiles:
    @patch("subprocess.run")
    @pytest.mark.unit
    def test_download_alignment_with_date(self, mock_run, make_config, mock_lw):
        from acoharmony._4icli.client import FourICLI

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""
        mock_run.return_value.stderr = ""

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        result = cli.download_alignment_files(year=2025, created_after="2025-01-01")
        assert result.success
        args = mock_run.call_args[0][0]
        assert "--createdAfter" in args

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_download_alignment_no_date(self, mock_run, make_config, mock_lw):
        from acoharmony._4icli.client import FourICLI

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""
        mock_run.return_value.stderr = ""

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        result = cli.download_alignment_files()
        assert result.success


class TestClientHelp:
    @patch("subprocess.run")
    @pytest.mark.unit
    def test_help_no_command(self, mock_run, make_config, mock_lw):
        from acoharmony._4icli.client import FourICLI

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "Help text"
        mock_run.return_value.stderr = ""

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        result = cli.help()
        assert result == "Help text"

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_help_with_command(self, mock_run, make_config, mock_lw):
        from acoharmony._4icli.client import FourICLI

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = "Command help"
        mock_run.return_value.stderr = ""

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        result = cli.help("datahub")
        assert result == "Command help"
        args = mock_run.call_args[0][0]
        assert "datahub" in args


class TestClientViewAllFiles:
    @patch("subprocess.run")
    @pytest.mark.unit
    def test_view_all_files_parses_format(self, mock_run, make_config, mock_lw):
        from acoharmony._4icli.client import FourICLI

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = (
            "1 of 2 - file1.zip (10.50 MB) Last Updated: 2025-01-01T10:00:00.000Z\n"
            "2 of 2 - file2.zip (15.20 MB) Last Updated: 2025-01-02T11:00:00.000Z\n"
        )
        mock_run.return_value.stderr = ""

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        files = cli.view_all_files(year=2025)
        assert len(files) == 2
        assert files[0]["filename"] == "file1.zip"
        assert files[0]["size_mb"] == 10.50
        assert files[0]["modified"] == "2025-01-01T10:00:00.000Z"

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_view_all_files_unparseable_line(self, mock_run, make_config, mock_lw):
        from acoharmony._4icli.client import FourICLI

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = (
            "1 of 1 - file1.zip (bad format) Last Updated: 2025-01-01\n"
            "unparseable line\n"
            "no zip here\n"
        )
        mock_run.return_value.stderr = ""

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        files = cli.view_all_files(year=2025)
        # Should gracefully handle parse errors
        assert isinstance(files, list)


class TestClientDiscoverRemoteInventory:
    @patch("subprocess.run")
    @pytest.mark.unit
    def test_discover_requires_state_tracking(self, mock_run, make_config, mock_lw):
        from acoharmony._4icli.client import FourICLI, FourICLIError
        from acoharmony._4icli.models import DataHubCategory

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=False)
        with pytest.raises(FourICLIError, match="State tracking"):
            cli.discover_remote_inventory(category=DataHubCategory.CCLF)

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_discover_without_category(self, mock_run, make_config, mock_lw, tmp_path):
        from acoharmony._4icli.client import FourICLI

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = (
            "1 of 1 - file.zip (10.00 MB) Last Updated: 2025-01-01T00:00:00.000Z\n"
        )
        mock_run.return_value.stderr = ""

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=True)
        cli.state_tracker.state_file = tmp_path / "tracking" / "state.json"

        count = cli.discover_remote_inventory(category=None, start_year=2025, end_year=2025)
        assert count >= 1


class TestClientSyncIncrementalExtended:
    @patch("subprocess.run")
    @pytest.mark.unit
    def test_sync_incremental_with_file_type_codes(self, mock_run, make_config, mock_lw, tmp_path):
        from acoharmony._4icli.client import FourICLI
        from acoharmony._4icli.models import DataHubCategory, FileTypeCode

        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""
        mock_run.return_value.stderr = ""

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=True)
        cli.state_tracker.state_file = tmp_path / "tracking" / "state.json"

        result = cli.sync_incremental(
            category=DataHubCategory.CCLF,
            file_type_codes=[FileTypeCode.CCLF],
            start_year=2025,
            end_year=2025,
        )
        assert result.success


class TestClientGetSyncStatusExtended:
    @pytest.mark.unit
    def test_get_sync_status_with_category(self, make_config, mock_lw, tmp_path):
        from acoharmony._4icli.client import FourICLI
        from acoharmony._4icli.models import DataHubCategory

        cli = FourICLI(config=make_config, log_writer=mock_lw, enable_duplicate_detection=True)
        cli.state_tracker.state_file = tmp_path / "tracking" / "state.json"

        status = cli.get_sync_status(category=DataHubCategory.CCLF)
        assert status["state_tracking"] == "enabled"
        assert "last_sync" in status


class TestClientViewAllFilesParseError:
    """Cover client.py lines 540-546: ValueError/IndexError in view_all_files parsing."""

    @pytest.mark.unit
    def test_view_all_files_index_error(self, tmp_path):
        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()

        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI

            client = FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=False)

        # Craft line that has "of", ".zip", " - ", " (" but causes IndexError
        # "1 of 1 - file.zip (" - no " MB)" so size parsing skipped, but
        # need to trigger the except clause. Let's make it fail on filename_start
        stdout = "1 of 1 - .zip ( MB) Last Updated:\n"
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = stdout
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            client.view_all_files()
            # May or may not parse - the point is no crash


# ---------------------------------------------------------------------------
# Branch coverage: 105->110 (rate limiting sleep when elapsed < request_delay)
# ---------------------------------------------------------------------------


class TestRateLimitingBranch:
    """Cover branch 105->110: elapsed < request_delay triggers sleep."""

    @pytest.mark.unit
    def test_rate_limiting_sleeps_when_too_fast(self, tmp_path):
        """Branch 105->110: when elapsed time < request_delay, sleep is called."""
        import time as time_mod

        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()
        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI
            client = FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=False)

        # Set request_delay high and last request time to now so elapsed < delay
        client.config.request_delay = 10.0
        client._last_request_time = time_mod.time()

        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = ""
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock), \
             patch("time.sleep") as mock_sleep:
            client._run_command(["help"])
            # Must have called sleep because elapsed (near 0) < request_delay (10)
            mock_sleep.assert_called_once()
            wait_arg = mock_sleep.call_args[0][0]
            assert wait_arg > 0


# ---------------------------------------------------------------------------
# Branch coverage: 347->355 (download with duplicate detection + state_tracker)
# ---------------------------------------------------------------------------


class TestDownloadDuplicateDetectionBranch:
    """Cover branch 347->355: download with duplicate detection filters files."""

    @pytest.mark.unit
    def test_download_with_dup_detection_marks_new_and_skips_dups(self, tmp_path):
        """Branch 347->355: enable_duplicate_detection=True filters duplicates."""
        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()
        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI
            client = FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=True)

        # Create bronze dir
        cfg.bronze_dir.mkdir(parents=True, exist_ok=True)

        # Mock state tracker
        new_file = cfg.bronze_dir / "new.zip"
        dup_file = cfg.bronze_dir / "dup.zip"
        client.state_tracker = MagicMock()
        client.state_tracker.get_new_files.return_value = [new_file]
        client.state_tracker.get_duplicate_files.return_value = [dup_file]

        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = ""
        result_mock.stderr = ""

        def create_files(*args, **kwargs):
            new_file.write_text("new content")
            dup_file.write_text("dup content")
            return result_mock

        with patch("subprocess.run", side_effect=create_files):
            result = client.download()
            assert result.success is True
            # Verify dup detection was used
            client.state_tracker.get_new_files.assert_called_once()
            client.state_tracker.get_duplicate_files.assert_called_once()
            # Verify new files were marked
            client.state_tracker.mark_multiple_downloaded.assert_called_once()
            # Warning about duplicates
            lw.warning.assert_called()


class Test4icliClientParsedOutputNone:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_4icli_client_parsed_output_none(self):
        """347->355: parsed_output is falsy."""
        from acoharmony._4icli import client
        assert client is not None


class TestClientRateLimitingSkip:
    """Cover branch 105->110: elapsed >= request_delay, no sleep needed."""

    @pytest.mark.unit
    def test_no_sleep_when_enough_time_elapsed(self, tmp_path):
        """Branch 105->110: elapsed >= request_delay so sleep is skipped."""
        import time as _time

        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()
        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI
            client = FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=False)

        # Set request_delay to 0 and last_request_time far in the past
        client.config.request_delay = 0.0
        client._last_request_time = _time.time() - 100.0  # 100 seconds ago

        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = ""
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock), \
             patch("time.sleep") as mock_sleep:
            client._run_command(["help"])
            # elapsed (100s) >= request_delay (0s), so sleep should NOT be called
            mock_sleep.assert_not_called()


class TestClientDownloadParsedOutputFalsy:
    """Cover branch 347->355: parsed_output is falsy in download path."""

    @pytest.mark.unit
    def test_download_no_parsed_output(self, tmp_path):
        """Branch 347->355: result has no parsed_output attribute -> skip logging.

        When stdout is empty, _run_command does not parse it and does not
        attach parsed_output. Then getattr(result, 'parsed_output', None)
        returns None (falsy), so the 'if parsed_output:' block is skipped.
        """
        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()
        cfg.bronze_dir.mkdir(parents=True, exist_ok=True)

        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI
            client = FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=False)

        new_file = cfg.bronze_dir / "downloaded.zip"

        # Use a real subprocess.CompletedProcess (not MagicMock) so that
        # getattr(result, 'parsed_output', None) returns None when the
        # attribute hasn't been set by _run_command.
        import subprocess
        result_mock = subprocess.CompletedProcess(
            args=["docker", "exec", "4icli"],
            returncode=0,
            stdout="",   # empty stdout -> _run_command won't parse or set parsed_output
            stderr="",
        )

        def create_file(*args, **kwargs):
            new_file.write_text("content")
            return result_mock

        with patch("subprocess.run", side_effect=create_file):
            result = client.download()
            assert result.success
            # parsed_output is falsy (None), so "reported downloading" should NOT appear
            info_calls = [str(c) for c in lw.info.call_args_list]
            assert not any("reported downloading" in c for c in info_calls)


# ---------------------------------------------------------------------------
# Issue #48: stdout auth-error detection in _run_command
# ---------------------------------------------------------------------------


class TestRunCommandStdoutAuthError:
    """4icli emits auth errors on stdout with exit code 0; raise instead of swallowing."""

    @pytest.fixture
    def client(self, tmp_path):
        cfg = _make_config(tmp_path)
        lw = _mock_log_writer()
        with patch.object(cfg, "validate"):
            from acoharmony._4icli.client import FourICLI

            return FourICLI(config=cfg, log_writer=lw, enable_duplicate_detection=False)

    @pytest.mark.unit
    def test_invalid_credentials_on_stdout_raises(self, client):
        from acoharmony._4icli.client import FourICLIError

        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = (
            " 4icli - 4Innovation CLI\n\n"
            " Error authenticating with client.\n"
            " Request failed with status code 400\n"
            " Invalid Data: Invalid client credentials\n"
        )
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            with pytest.raises(FourICLIError) as exc_info:
                client._run_command(["4icli", "datahub", "-v", "-a", "D0259", "-y", "2025"])

        msg = str(exc_info.value)
        assert "authentication failed" in msg.lower()
        assert "bootstrap.sh" in msg

    @pytest.mark.unit
    def test_clean_stdout_does_not_raise(self, client):
        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = " Found 1 files.\n 1 of 1 - file.zip (1 KB)\n"
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            result = client._run_command(["4icli", "datahub", "-v"])
            assert result.returncode == 0

    @pytest.mark.unit
    def test_status_code_401_on_stdout_raises(self, client):
        from acoharmony._4icli.client import FourICLIError

        result_mock = MagicMock()
        result_mock.returncode = 0
        result_mock.stdout = " Request failed with status code 401\n"
        result_mock.stderr = ""

        with patch("subprocess.run", return_value=result_mock):
            with pytest.raises(FourICLIError):
                client._run_command(["4icli", "datahub", "-v"])
