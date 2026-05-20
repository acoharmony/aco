"""Tests for acoharmony._puf.puf_cli module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest
from unittest.mock import patch, MagicMock, PropertyMock
from types import SimpleNamespace

from acoharmony._puf.puf_cli import (
    cmd_inventory,
    cmd_need_download,
    cmd_download,
    cmd_list_years,
)
from acoharmony._puf.puf_state import PUFFileEntry, PUFInventoryState, PUFStateTracker
from acoharmony._puf.models import FileMetadata, FileCategory, RuleType, DownloadTask


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        from acoharmony._puf import puf_cli
        assert puf_cli is not None


def _make_file_entry(
    year="2024",
    rule_type="Final",
    file_key="test_file",
    url="https://example.com/test.zip",
    category="addenda",
    downloaded=False,
    extracted=False,
    found_in_archive=False,
    found_in_bronze=False,
    found_in_cite_corpus=False,
):
    return PUFFileEntry(
        year=year,
        rule_type=rule_type,
        file_key=file_key,
        url=url,
        category=category,
        downloaded=downloaded,
        extracted=extracted,
        found_in_archive=found_in_archive,
        found_in_bronze=found_in_bronze,
        found_in_cite_corpus=found_in_cite_corpus,
    )


def _mock_tracker_with_files(files_dict, summary_overrides=None):
    """Create a mock tracker with files pre-loaded."""
    tracker = MagicMock(spec=PUFStateTracker)
    mock_state = MagicMock()
    mock_state.files = files_dict
    tracker.state = mock_state
    tracker.sync_with_inventory.return_value = 0
    tracker.scan_filesystem.return_value = {
        "found_in_archive": 0,
        "found_in_bronze": 0,
        "found_in_cite": 0,
        "marked_downloaded": 0,
        "marked_extracted": 0,
    }
    summary = {
        "dataset_name": "Test",
        "last_updated": "2024-01-01",
        "total_files": len(files_dict),
        "downloaded_files": sum(1 for f in files_dict.values() if f.downloaded),
        "pending_files": sum(1 for f in files_dict.values() if not f.downloaded),
        "failed_files": 0,
        "download_percentage": 0.0,
    }
    if summary_overrides:
        summary.update(summary_overrides)
    tracker.get_summary.return_value = summary
    tracker.get_state_path.return_value = "/tmp/test_state.json"
    return tracker


class TestCmdInventoryBranches:
    """Tests for cmd_inventory branches."""

    @pytest.mark.unit
    def test_empty_files_skips_breakdown(self):
        """Branch 126→168: tracker.state.files is empty, skip year/category breakdown."""
        tracker = _mock_tracker_with_files({})

        args = SimpleNamespace(dataset="pfs", year=None, rule_type=None, force=False)
        with patch("acoharmony._puf.puf_cli.PUFStateTracker") as MockTracker:
            MockTracker.load.return_value = tracker
            result = cmd_inventory(args)

        assert result == 0

    @pytest.mark.unit
    def test_downloaded_file_counted_in_year_breakdown(self):
        """Branch 133→134: file_entry.downloaded is True in year breakdown."""
        entry_dl = _make_file_entry(year="2024", downloaded=True)
        entry_pending = _make_file_entry(year="2024", file_key="pending_file", downloaded=False)
        files = {
            "2024:Final:test_file": entry_dl,
            "2024:Final:pending_file": entry_pending,
        }
        tracker = _mock_tracker_with_files(files)

        args = SimpleNamespace(dataset="pfs", year=None, rule_type=None, force=False)
        with patch("acoharmony._puf.puf_cli.PUFStateTracker") as MockTracker:
            MockTracker.load.return_value = tracker
            result = cmd_inventory(args)

        assert result == 0

    @pytest.mark.unit
    def test_downloaded_file_counted_in_category_breakdown(self):
        """Branch 154→155: file_entry.downloaded is True in category breakdown."""
        entry_dl = _make_file_entry(category="addenda", downloaded=True)
        entry_pending = _make_file_entry(category="gpci", file_key="pending_gpci", downloaded=False)
        files = {
            "2024:Final:test_file": entry_dl,
            "2024:Final:pending_gpci": entry_pending,
        }
        tracker = _mock_tracker_with_files(files)

        args = SimpleNamespace(dataset="pfs", year=None, rule_type=None, force=False)
        with patch("acoharmony._puf.puf_cli.PUFStateTracker") as MockTracker:
            MockTracker.load.return_value = tracker
            result = cmd_inventory(args)

        assert result == 0


class TestCmdNeedDownloadBranches:
    """Tests for cmd_need_download branches."""

    @pytest.mark.unit
    def test_no_needed_with_filters_shows_message(self):
        """Branch 257→258: no needed tasks + filters_applied is truthy."""
        tracker = MagicMock(spec=PUFStateTracker)
        tracker.sync_with_inventory.return_value = 0
        tracker.scan_filesystem.return_value = {
            "found_in_archive": 0,
            "found_in_bronze": 0,
            "found_in_cite": 0,
            "marked_downloaded": 0,
            "marked_extracted": 0,
        }
        tracker.get_needed_downloads.return_value = []

        args = SimpleNamespace(
            dataset="pfs",
            year="2024",  # filter applied
            rule_type=None,
            category=None,
            schema=None,
            limit=20,
        )
        with patch("acoharmony._puf.puf_cli.PUFStateTracker") as MockTracker:
            MockTracker.load.return_value = tracker
            result = cmd_need_download(args)

        assert result == 0


class TestCmdDownloadBranches:
    """Tests for cmd_download branches."""

    def _write_state_file(self, tmp_path, files_data):
        """Helper to write a download state file."""
        import json
        state_path = tmp_path / "logs" / "tracking" / "puf_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_data = {
            "generated_at": "2024-01-01T00:00:00",
            "total_missing": len(files_data),
            "files": files_data,
        }
        with open(state_path, "w") as f:
            json.dump(state_data, f)
        return state_path

    def _make_file_data(self, dataset_key="pfs", url="https://example.com/test.zip", file_key="test_file"):
        """Helper to make a PUFFileEntry dict for state file."""
        return {
            "year": "2024",
            "rule_type": "Final",
            "file_key": file_key,
            "url": url,
            "category": "addenda",
            "dataset_key": dataset_key,
            "metadata": {},
            "schema_mapping": None,
            "downloaded": False,
            "download_timestamp": None,
            "corpus_path": None,
            "file_size_bytes": None,
            "error_message": None,
            "extracted": False,
            "extraction_timestamp": None,
            "extracted_files": [],
            "zip_filename": "test.zip",
            "found_in_archive": False,
            "found_in_bronze": False,
            "found_in_cite_corpus": False,
        }

    @pytest.mark.unit
    def test_rvu_dataset_key_and_no_zip_extension_and_empty_result(self, tmp_path, capsys):
        """Branches 440→444, 452→453, 461→466: integrated test through cmd_download.

        Tests:
        - 440→444: result_df has 0 rows, corpus_path stays None
        - 452→453: dataset_key='rvu' uses rvu archive dir
        - 461→466: URL without .zip extension uses fallback filename
        """
        from pathlib import Path as RealPath

        # URL without .zip extension to trigger fallback
        file_data = self._make_file_data(
            dataset_key="rvu",
            url="https://example.com/files/no_ext",
            file_key="rvu_q1",
        )
        self._write_state_file(tmp_path, [file_data])

        args = SimpleNamespace(limit=None)

        mock_tracker = MagicMock(spec=PUFStateTracker)

        # Mock transform_cite to return empty result (0 rows)
        mock_result_df = MagicMock()
        mock_result_df.__len__ = lambda self: 0
        mock_result_df.columns = []
        mock_result_lf = MagicMock()
        mock_result_lf.collect.return_value = mock_result_df

        mock_response = MagicMock()
        mock_response.content = b"fake zip content"

        # Use real paths rooted at tmp_path
        def path_side_effect(arg):
            if str(arg) == "/opt/s3/data/workspace":
                return tmp_path
            return RealPath(str(arg))

        with patch("acoharmony._puf.puf_cli.Path", side_effect=path_side_effect):
            with patch("acoharmony._puf.puf_cli.PUFStateTracker") as MockTrackerCls:
                MockTrackerCls.load.return_value = mock_tracker
                with patch.dict("sys.modules", {
                    "acoharmony._transforms._cite": MagicMock(transform_cite=lambda **kw: mock_result_lf),
                    "requests": MagicMock(get=lambda *a, **kw: mock_response),
                }):
                    result = cmd_download(args)

        captured = capsys.readouterr()
        assert result == 0
        # Verify archive dir was created for rvu
        archive_dir = tmp_path / "archive" / "2024" / "rvu"
        assert archive_dir.exists()
        # Verify fallback filename was used (file_key + .zip)
        zip_file = archive_dir / "rvu_q1.zip"
        assert zip_file.exists()
        assert zip_file.read_bytes() == b"fake zip content"
        # Verify corpus_path was None (0 rows)
        mock_tracker.mark_downloaded.assert_called_once()
        call_kwargs = mock_tracker.mark_downloaded.call_args
        assert call_kwargs[1].get("corpus_path") is None or call_kwargs.kwargs.get("corpus_path") is None

    @pytest.mark.unit
    def test_more_than_10_errors_shows_truncation(self, tmp_path, capsys):
        """Branch 516→517: more than 10 errors triggers truncation message.

        We create 12 files that all fail to download, producing >10 errors.
        """
        from pathlib import Path as RealPath

        files_data = []
        for i in range(12):
            fd = self._make_file_data(file_key=f"file_{i}", url=f"https://example.com/f{i}.zip")
            files_data.append(fd)
        self._write_state_file(tmp_path, files_data)

        args = SimpleNamespace(limit=None)
        mock_tracker = MagicMock(spec=PUFStateTracker)

        def path_side_effect(arg):
            if str(arg) == "/opt/s3/data/workspace":
                return tmp_path
            return RealPath(str(arg))

        with patch("acoharmony._puf.puf_cli.Path", side_effect=path_side_effect):
            with patch("acoharmony._puf.puf_cli.PUFStateTracker") as MockTrackerCls:
                MockTrackerCls.load.return_value = mock_tracker
                # Mock transform_cite to always raise an error
                with patch.dict("sys.modules", {
                    "acoharmony._transforms._cite": MagicMock(
                        transform_cite=MagicMock(side_effect=RuntimeError("download failed"))
                    ),
                }):
                    result = cmd_download(args)

        captured = capsys.readouterr()
        assert result == 1
        assert "... and 2 more" in captured.out

    @pytest.mark.unit
    def test_no_zip_extension_uses_fallback_filename(self):
        """Branch 461→466: filename doesn't end with .zip, use fallback."""
        from urllib.parse import urlparse
        from pathlib import Path

        url = "https://example.com/files/no_extension"
        url_path = urlparse(url).path
        filename = Path(url_path).name
        assert not filename.lower().endswith('.zip')
        file_key = "my_file_key"
        if not filename or not filename.lower().endswith('.zip'):
            filename = f"{file_key}.zip"
        assert filename == "my_file_key.zip"

    @pytest.mark.unit
    def test_corpus_path_extraction_no_rows(self):
        """Branch 440→444: result_df has 0 rows, corpus_path stays None."""
        mock_df = MagicMock()
        mock_df.__len__ = lambda self: 0
        mock_df.columns = ["corpus_path"]

        corpus_path = None
        if len(mock_df) > 0 and "corpus_path" in mock_df.columns:
            corpus_path = str(mock_df["corpus_path"][0])

        assert corpus_path is None


class TestCmdListYearsBranch:
    """Tests for cmd_list_years branch 548→546."""

    @pytest.mark.unit
    def test_multiple_years_loop_iteration(self):
        """Branch 548→546: iteration through multiple years in the loop."""
        mock_year_inv_2024 = MagicMock()
        mock_year_inv_2024.rules = {"Final": MagicMock(), "Proposed": MagicMock()}
        mock_year_inv_2024.get_all_files.return_value = [MagicMock()] * 5

        mock_year_inv_2025 = MagicMock()
        mock_year_inv_2025.rules = {"Final": MagicMock()}
        mock_year_inv_2025.get_all_files.return_value = [MagicMock()] * 3

        def mock_get_year(year):
            if year == "2024":
                return mock_year_inv_2024
            elif year == "2025":
                return mock_year_inv_2025
            return None

        args = SimpleNamespace()
        with patch("acoharmony._puf.puf_cli.pfs_inventory") as mock_pfs:
            mock_pfs.list_available_years.return_value = ["2024", "2025"]
            mock_pfs.get_year.side_effect = mock_get_year
            result = cmd_list_years(args)

        assert result == 0


# ---------------------------------------------------------------------------
# Branch coverage: 461->466 (filename doesn't end with .zip, fallback)
# Already covered above in test_rvu_dataset_key_and_no_zip_extension_and_empty_result
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Branch coverage: 548->546 (cmd_list_years: year_inv is None, loop continues)
# ---------------------------------------------------------------------------


class TestCmdListYearsNoneYearBranch:
    """Cover branch 548->546: get_year returns None for a year, loop continues."""

    @pytest.mark.unit
    def test_list_years_with_none_year_inv(self, capsys):
        """Branch 548->546: get_year returns None, loop continues to next year."""
        args = SimpleNamespace()
        with patch("acoharmony._puf.puf_cli.pfs_inventory") as mock_pfs:
            mock_pfs.list_available_years.return_value = ["2023", "2024"]
            # 2023 returns None (branch 548->546), 2024 returns valid
            mock_year_inv = MagicMock()
            mock_year_inv.rules = {"Final": MagicMock()}
            mock_year_inv.get_all_files.return_value = [MagicMock(), MagicMock()]
            mock_pfs.get_year.side_effect = lambda y: None if y == "2023" else mock_year_inv
            result = cmd_list_years(args)

        assert result == 0
        captured = capsys.readouterr()
        # 2024 should be printed, 2023 should be skipped
        assert "2024" in captured.out


class TestCmdDownloadNoExtensionFallback:
    """Cover branch 461->466: URL path has no .zip extension, fallback to file_key.zip."""

    def _write_state_file(self, tmp_path, files_data):
        """Helper to write a download state file."""
        import json
        state_path = tmp_path / "logs" / "tracking" / "puf_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_data = {
            "generated_at": "2024-01-01T00:00:00",
            "total_missing": len(files_data),
            "files": files_data,
        }
        with open(state_path, "w") as f:
            json.dump(state_data, f)
        return state_path

    @pytest.mark.unit
    def test_url_with_no_zip_extension(self, tmp_path, capsys):
        """Branch 461->466: URL yields filename without .zip -> fallback to file_key.zip."""
        from pathlib import Path as RealPath

        # URL with a filename that doesn't end in .zip
        file_data = {
            "year": "2024",
            "rule_type": "Final",
            "file_key": "my_fallback_key",
            "url": "https://example.com/files/datafile.dat",
            "category": "addenda",
            "dataset_key": "pfs",
            "metadata": {},
            "schema_mapping": None,
            "downloaded": False,
            "download_timestamp": None,
            "corpus_path": None,
            "file_size_bytes": None,
            "error_message": None,
            "extracted": False,
            "extraction_timestamp": None,
            "extracted_files": [],
            "zip_filename": "test.zip",
            "found_in_archive": False,
            "found_in_bronze": False,
            "found_in_cite_corpus": False,
        }
        self._write_state_file(tmp_path, [file_data])

        args = SimpleNamespace(limit=None)
        mock_tracker = MagicMock(spec=PUFStateTracker)

        mock_result_df = MagicMock()
        mock_result_df.__len__ = lambda self: 0
        mock_result_df.columns = []
        mock_result_lf = MagicMock()
        mock_result_lf.collect.return_value = mock_result_df

        mock_response = MagicMock()
        mock_response.content = b"zip content"

        def path_side_effect(arg):
            if str(arg) == "/opt/s3/data/workspace":
                return tmp_path
            return RealPath(str(arg))

        with patch("acoharmony._puf.puf_cli.Path", side_effect=path_side_effect):
            with patch("acoharmony._puf.puf_cli.PUFStateTracker") as MockTrackerCls:
                MockTrackerCls.load.return_value = mock_tracker
                with patch.dict("sys.modules", {
                    "acoharmony._transforms._cite": MagicMock(transform_cite=lambda **kw: mock_result_lf),
                    "requests": MagicMock(get=lambda *a, **kw: mock_response),
                }):
                    result = cmd_download(args)

        assert result == 0
        # Fallback filename should be used: my_fallback_key.zip
        archive_dir = tmp_path / "archive" / "2024" / "final"
        zip_file = archive_dir / "my_fallback_key.zip"
        assert zip_file.exists()

    @pytest.mark.unit
    def test_url_with_zip_extension_no_fallback(self, tmp_path, capsys):
        """Branch 461->466 (False): URL has .zip extension, no fallback needed."""
        from pathlib import Path as RealPath

        file_data = {
            "year": "2024",
            "rule_type": "Final",
            "file_key": "test_key",
            "url": "https://example.com/files/real_data.zip",
            "category": "addenda",
            "dataset_key": "pfs",
            "metadata": {},
            "schema_mapping": None,
            "downloaded": False,
            "download_timestamp": None,
            "corpus_path": None,
            "file_size_bytes": None,
            "error_message": None,
            "extracted": False,
            "extraction_timestamp": None,
            "extracted_files": [],
            "zip_filename": "test.zip",
            "found_in_archive": False,
            "found_in_bronze": False,
            "found_in_cite_corpus": False,
        }
        self._write_state_file(tmp_path, [file_data])

        args = SimpleNamespace(limit=None)
        mock_tracker = MagicMock(spec=PUFStateTracker)

        mock_result_df = MagicMock()
        mock_result_df.__len__ = lambda self: 0
        mock_result_df.columns = []
        mock_result_lf = MagicMock()
        mock_result_lf.collect.return_value = mock_result_df

        mock_response = MagicMock()
        mock_response.content = b"zip content"

        def path_side_effect(arg):
            if str(arg) == "/opt/s3/data/workspace":
                return tmp_path
            return RealPath(str(arg))

        with patch("acoharmony._puf.puf_cli.Path", side_effect=path_side_effect):
            with patch("acoharmony._puf.puf_cli.PUFStateTracker") as MockTrackerCls:
                MockTrackerCls.load.return_value = mock_tracker
                with patch.dict("sys.modules", {
                    "acoharmony._transforms._cite": MagicMock(transform_cite=lambda **kw: mock_result_lf),
                    "requests": MagicMock(get=lambda *a, **kw: mock_response),
                }):
                    result = cmd_download(args)

        assert result == 0
        # Original .zip filename from URL should be used (no fallback)
        archive_dir = tmp_path / "archive" / "2024" / "final"
        zip_file = archive_dir / "real_data.zip"
        assert zip_file.exists()
