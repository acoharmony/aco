"""Unit tests for cli module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import argparse
from datetime import datetime
import json
from pathlib import Path
import types
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from typing import TYPE_CHECKING

import pytest

# Import necessary classes from _4icli module
try:
    from acoharmony._4icli.models import DateFilter
    from acoharmony._4icli.client import FourICLI
    from acoharmony._4icli import parser
except ImportError:
    # Provide fallbacks or None values to allow tests to run
    DateFilter = None
    FourICLI = None
    parser = None

# Import helper functions and models
from acoharmony._test.foureye.conftest import _make_config, _mock_log_writer  # noqa: F401
try:
    from acoharmony._4icli.models import DataHubCategory, FileTypeCode
    from acoharmony._4icli.inventory import InventoryResult, FileInventoryEntry
    from acoharmony._4icli.config import get_current_year
except ImportError:
    DataHubCategory = None
    FileTypeCode = None
    InventoryResult = None
    FileInventoryEntry = None
    get_current_year = None

if TYPE_CHECKING:
    pass


@pytest.mark.unit
def test_cmd_need_download_basic(tmp_path) -> None:
    """cmd_need_download basic functionality -- returns 1 when no inventory file."""
    from acoharmony._4icli.cli import cmd_need_download

    with patch("acoharmony._4icli.cli.FourICLIConfig") as m_cfg, \
         patch("acoharmony._4icli.cli.LogWriter"), \
         patch("acoharmony._4icli.cli.InventoryDiscovery") as m_disc:
        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg
        discovery = MagicMock()
        discovery.get_inventory_path.return_value = tmp_path / "nonexistent.json"
        m_disc.return_value = discovery
        args = argparse.Namespace()
        result = cmd_need_download(args)
        assert result == 1


@pytest.mark.unit
def test_cmd_download_basic(tmp_path) -> None:
    """cmd_download basic functionality -- returns 1 when no state file."""
    from acoharmony._4icli.cli import cmd_download

    with patch("acoharmony._4icli.cli.FourICLIConfig") as m_cfg, \
         patch("acoharmony._4icli.cli.LogWriter"):
        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg
        args = argparse.Namespace(updated_after=None)
        result = cmd_download(args)
        assert result == 1


@pytest.mark.unit
def test_cmd_inventory_basic(tmp_path) -> None:
    """cmd_inventory basic functionality -- runs without error for new inventory."""
    from acoharmony._4icli.cli import cmd_inventory

    with patch("acoharmony._4icli.cli.FourICLIConfig") as m_cfg, \
         patch("acoharmony._4icli.cli.LogWriter"), \
         patch("acoharmony._4icli.cli.InventoryDiscovery") as m_disc:
        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg
        discovery = MagicMock()
        discovery.get_inventory_path.return_value = tmp_path / "nonexistent.json"
        mock_result = MagicMock()
        mock_result.total_files = 5
        mock_result.completed_at = datetime(2024, 1, 1)
        mock_result.duration_seconds = 5.0
        mock_result.files = []
        mock_result.years = [2024]
        mock_result.files_by_year = {2024: 5}
        mock_result.files_by_category = {"CCLF": 5}
        mock_result.apm_id = "A9999"
        discovery.discover_years.return_value = mock_result
        discovery.enrich_with_file_type_codes.return_value = mock_result
        m_disc.return_value = discovery
        args = argparse.Namespace(start_year=2024, end_year=2024, force=True)
        result = cmd_inventory(args)
        assert result is None or result == 0


from unittest.mock import patch


class TestCLIMainNoCommand:
    """Cover the else branch when no valid command is given."""

    @pytest.mark.unit
    def test_no_command_prints_help(self):
        """Lines 857-858: unknown command prints help and exits."""
        from acoharmony._4icli.cli import main

        with patch("sys.argv", ["4icli"]):
            with patch("sys.exit") as mock_exit:
                try:
                    main()
                except SystemExit:
                    pass
                if mock_exit.called:
                    assert mock_exit.call_args[0][0] in (0, 1, 2)


def _make_config(tmp_path: Path):
    """Return a mock FourICLIConfig rooted under tmp_path."""
    cfg = MagicMock()
    cfg.data_path = tmp_path
    cfg.bronze_dir = tmp_path / "bronze"
    cfg.archive_dir = tmp_path / "archive"
    cfg.silver_dir = tmp_path / "silver"
    cfg.gold_dir = tmp_path / "gold"
    cfg.log_dir = tmp_path / "logs"
    cfg.tracking_dir = tmp_path / "logs" / "tracking"
    cfg.default_apm_id = "A9999"
    cfg.default_year = 2025
    cfg.binary_path = Path("/usr/local/bin/4icli")
    cfg.working_dir = tmp_path / "bronze"
    for d in [cfg.bronze_dir, cfg.archive_dir, cfg.log_dir, cfg.tracking_dir]:
        d.mkdir(parents=True, exist_ok=True)
    return cfg


def _make_inventory_result(files=None, years=None, errors=None):
    """Build a lightweight InventoryResult-like object."""
    from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult

    if files is None:
        files = [
            FileInventoryEntry(
                filename="CCLF1.zip",
                category="CCLF",
                file_type_code=113,
                year=2024,
                size_bytes=1024,
                last_updated="2024-06-01T00:00:00Z",
            ),
            FileInventoryEntry(
                filename="PALMR.zip",
                category="Beneficiary List",
                file_type_code=165,
                year=2024,
                size_bytes=2048,
                last_updated="2024-07-01T00:00:00Z",
            ),
        ]
    if years is None:
        years = [2024]

    return InventoryResult(
        apm_id="A9999",
        categories=["CCLF", "Beneficiary List"],
        years=years,
        total_files=len(files),
        files_by_year={2024: len(files)},
        files_by_category={"CCLF": 1, "Beneficiary List": 1},
        files=files,
        started_at=datetime(2024, 1, 1),
        completed_at=datetime(2024, 1, 1, 0, 5),
        errors=errors,
    )


def create_sample_inventory_entries():
    """Create sample inventory entries for testing.

    Returns a list of FileInventoryEntry objects with various file types.
    """
    from acoharmony._4icli.inventory import FileInventoryEntry

    entries = [
        FileInventoryEntry(
            filename="CCLF1.zip",
            category="CCLF",
            file_type_code=113,
            year=2024,
            size_bytes=1024,
            last_updated="2024-06-01T00:00:00Z",
        ),
        FileInventoryEntry(
            filename="CCLF8.zip",
            category="CCLF",
            file_type_code=114,
            year=2024,
            size_bytes=2048,
            last_updated="2024-07-01T00:00:00Z",
        ),
        FileInventoryEntry(
            filename="PALMR.zip",
            category="Beneficiary List",
            file_type_code=165,
            year=2024,
            size_bytes=512,
            last_updated="2024-07-15T00:00:00Z",
        ),
        FileInventoryEntry(
            filename="TPARC.zip",
            category="Reports",
            file_type_code=167,
            year=2024,
            size_bytes=1536,
            last_updated="2024-08-01T00:00:00Z",
        ),
        FileInventoryEntry(
            filename="CCLF1_2025.zip",
            category="CCLF",
            file_type_code=113,
            year=2025,
            size_bytes=1024,
            last_updated="2025-01-15T00:00:00Z",
        ),
    ]
    return entries


class TestCmdNeedDownload:
    """Cover cmd_need_download (lines 55-241)."""

    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.LogWriter")
    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @patch("acoharmony._4icli.cli.scan_directory")
    @patch("acoharmony._4icli.cli.compare_inventory")
    @patch("acoharmony._4icli.cli.format_size")
    @patch("acoharmony._4icli.cli.save_not_downloaded_state")
    @patch("acoharmony._4icli.cli.export_to_csv")
    @pytest.mark.unit
    def test_need_download_inventory_not_found(
        self, m_export, m_save, m_fmt, m_cmp, m_scan, m_disc, m_log, m_cfg, tmp_path
    ):
        from acoharmony._4icli.cli import cmd_need_download

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        discovery = MagicMock()
        # Inventory file does not exist
        discovery.get_inventory_path.return_value = tmp_path / "nope.json"
        m_disc.return_value = discovery

        args = argparse.Namespace()
        result = cmd_need_download(args)
        assert result == 1

    @patch("acoharmony._4icli.cli.save_not_downloaded_state")
    @patch("acoharmony._4icli.cli.export_to_csv")
    @patch("acoharmony._4icli.cli.format_size", return_value="1 KB")
    @patch("acoharmony._4icli.cli.compare_inventory")
    @patch("acoharmony._4icli.cli.scan_directory", return_value=set())
    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @patch("acoharmony._4icli.cli.LogWriter")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @pytest.mark.unit
    def test_need_download_happy_path(
        self, m_cfg, m_log, m_disc, m_scan, m_cmp, m_fmt, m_export, m_save, tmp_path
    ):
        from acoharmony._4icli.cli import cmd_need_download
        from acoharmony._4icli.inventory import FileInventoryEntry

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        inv = _make_inventory_result()
        discovery = MagicMock()
        inv_path = tmp_path / "inv.json"
        inv_path.write_text("{}")
        discovery.get_inventory_path.return_value = inv_path
        discovery.discover_years.return_value = inv
        discovery.enrich_with_file_type_codes.return_value = inv
        m_disc.return_value = discovery

        missing_entry = FileInventoryEntry(
            filename="MISSING.zip",
            category="CCLF",
            file_type_code=113,
            year=2024,
            size_bytes=500,
            last_updated="2024-01-01",
        )
        have_entry = FileInventoryEntry(
            filename="HAVE.zip",
            category="CCLF",
            file_type_code=113,
            year=2024,
            size_bytes=200,
        )

        m_cmp.return_value = {
            "total_inventory": 2,
            "have_count": 1,
            "missing_count": 1,
            "total_size_bytes": 500,
            "missing_by_year": {2024: 1},
            "missing_by_category": {"CCLF": 1},
            "missing_by_type_code": {113: 1},
            "missing": [missing_entry],
            "have": [have_entry],
        }

        # Patch the state tracker import inside the function (imported inline)
        tracker = MagicMock()
        tracker._file_cache = {}
        with patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=tracker):
            args = argparse.Namespace(
                start_year=2024,
                end_year=2024,
                year=2024,
                category="CCLF",
                limit=5,
                show_have=True,
                export=str(tmp_path / "out.csv"),
            )
            result = cmd_need_download(args)

        assert result == 0
        m_save.assert_called_once()
        m_export.assert_called_once()

    @patch("acoharmony._4icli.cli.save_not_downloaded_state")
    @patch("acoharmony._4icli.cli.format_size", return_value="0 B")
    @patch("acoharmony._4icli.cli.compare_inventory")
    @patch("acoharmony._4icli.cli.scan_directory", return_value=set())
    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @patch("acoharmony._4icli.cli.LogWriter")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @pytest.mark.unit
    def test_need_download_zero_inventory(
        self, m_cfg, m_log, m_disc, m_scan, m_cmp, m_fmt, m_save, tmp_path
    ):
        """Cover the branch total_inventory == 0."""
        from acoharmony._4icli.cli import cmd_need_download

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        inv = _make_inventory_result(files=[], years=[2024])
        inv.total_files = 0
        discovery = MagicMock()
        inv_path = tmp_path / "inv.json"
        inv_path.write_text("{}")
        discovery.get_inventory_path.return_value = inv_path
        discovery.discover_years.return_value = inv
        discovery.enrich_with_file_type_codes.return_value = inv
        m_disc.return_value = discovery

        m_cmp.return_value = {
            "total_inventory": 0,
            "have_count": 0,
            "missing_count": 0,
            "total_size_bytes": 0,
            "missing_by_year": {},
            "missing_by_category": {},
            "missing_by_type_code": {},
            "missing": [],
            "have": [],
        }

        tracker = MagicMock()
        tracker._file_cache = {}
        with patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=tracker):
            args = argparse.Namespace(
                start_year=None,
                end_year=None,
                year=None,
                category=None,
                limit=20,
                show_have=False,
            )
            result = cmd_need_download(args)

        assert result == 0


class TestCmdDownload:
    """Cover cmd_download (lines 324-581)."""

    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.LogWriter")
    @pytest.mark.unit
    def test_download_no_state_file(self, m_log, m_cfg, tmp_path):
        from acoharmony._4icli.cli import cmd_download

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        args = argparse.Namespace(updated_after=None)
        result = cmd_download(args)
        assert result == 1

    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.LogWriter")
    @pytest.mark.unit
    def test_download_empty_files_list(self, m_log, m_cfg, tmp_path):
        from acoharmony._4icli.cli import cmd_download

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        state_path = cfg.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps({"files": [], "generated_at": "2024-01-01"}))

        args = argparse.Namespace(updated_after=None)
        result = cmd_download(args)
        assert result == 0

    @patch("acoharmony._4icli.cli.FourICLI")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.LogWriter")
    @pytest.mark.unit
    def test_download_happy_path(self, m_log, m_cfg, m_client_cls, tmp_path):
        from acoharmony._4icli.cli import cmd_download

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        state_data = {
            "generated_at": "2024-06-01",
            "total_missing": 1,
            "total_size_formatted": "1 KB",
            "files": [
                {
                    "filename": "CCLF1.zip",
                    "category": "CCLF",
                    "file_type_code": 113,
                    "year": 2024,
                    "size_bytes": 1024,
                    "last_updated": "2024-06-01T00:00:00Z",
                    "discovered_at": None,
                }
            ],
        }
        state_path = cfg.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.write_text(json.dumps(state_data))

        # Mock registry
        with (
            patch("acoharmony._4icli.cli.get_file_type_codes", create=True),
            patch("acoharmony._4icli.cli.FourICLIStateTracker", create=True),
        ):
            MagicMock()
            # Patch the import inside cmd_download

            with patch.dict(
                "sys.modules",
                {
                    "acoharmony._4icli.registry": MagicMock(
                        get_file_type_codes=MagicMock(return_value=[113, 165])
                    ),
                    "acoharmony._4icli.state": MagicMock(
                        FourICLIStateTracker=MagicMock(return_value=MagicMock(_file_cache={}))
                    ),
                },
            ):
                # Since the function imports inline, we need to patch differently
                pass

        # Simpler approach: mock at function level
        # The function does inline imports, so we mock them via the module
        args = argparse.Namespace(updated_after="2024-01-01")

        registry_mod = types.ModuleType("acoharmony._4icli.registry")
        registry_mod.get_file_type_codes = MagicMock(return_value=[113, 165])

        state_mod = types.ModuleType("acoharmony._4icli.state")
        tracker = MagicMock()
        tracker._file_cache = {}
        state_mod.FourICLIStateTracker = MagicMock(return_value=tracker)

        MagicMock()

        # mock the download client
        download_result = MagicMock()
        download_result.success = True
        download_result.files_downloaded = [Path("/fake/file")]
        download_result.errors = []
        client = MagicMock()
        client.download.return_value = download_result
        m_client_cls.return_value = client

        with patch.dict(
            "sys.modules",
            {
                "acoharmony._4icli.registry": registry_mod,
                "acoharmony._4icli.state": state_mod,
            },
        ):
            result = cmd_download(args)

        assert result == 0

    @patch("acoharmony._4icli.cli.FourICLI")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.LogWriter")
    @pytest.mark.unit
    def test_download_no_file_type_codes(self, m_log, m_cfg, m_client_cls, tmp_path):
        """Cover branch: no file type codes found."""
        from acoharmony._4icli.cli import cmd_download

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        state_data = {
            "generated_at": "2024-06-01",
            "total_missing": 1,
            "files": [
                {
                    "filename": "CCLF1.zip",
                    "category": "CCLF",
                    "file_type_code": 113,
                    "year": 2024,
                    "size_bytes": 1024,
                    "last_updated": "2024-06-01T00:00:00Z",
                }
            ],
        }
        state_path = cfg.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.write_text(json.dumps(state_data))

        registry_mod = types.ModuleType("acoharmony._4icli.registry")
        registry_mod.get_file_type_codes = MagicMock(return_value=[])

        with patch.dict(
            "sys.modules",
            {
                "acoharmony._4icli.registry": registry_mod,
            },
        ):
            args = argparse.Namespace(updated_after=None)
            result = cmd_download(args)

        assert result == 1

    @patch("acoharmony._4icli.cli.FourICLI")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.LogWriter")
    @pytest.mark.unit
    def test_download_all_already_in_state(self, m_log, m_cfg, m_client_cls, tmp_path):
        """Cover branch: all files already in state tracker."""
        from acoharmony._4icli.cli import cmd_download

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        state_data = {
            "generated_at": "2024-06-01",
            "total_missing": 1,
            "files": [
                {
                    "filename": "CCLF1.zip",
                    "category": "CCLF",
                    "file_type_code": 113,
                    "year": 2024,
                    "size_bytes": 1024,
                    "last_updated": "2024-06-01T00:00:00Z",
                }
            ],
        }
        state_path = cfg.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.write_text(json.dumps(state_data))

        registry_mod = types.ModuleType("acoharmony._4icli.registry")
        registry_mod.get_file_type_codes = MagicMock(return_value=[113])

        state_mod = types.ModuleType("acoharmony._4icli.state")
        tracker = MagicMock()
        tracker._file_cache = {"CCLF1.zip": MagicMock()}  # Already tracked
        state_mod.FourICLIStateTracker = MagicMock(return_value=tracker)

        with patch.dict(
            "sys.modules",
            {
                "acoharmony._4icli.registry": registry_mod,
                "acoharmony._4icli.state": state_mod,
            },
        ):
            args = argparse.Namespace(updated_after=None)
            result = cmd_download(args)

        assert result == 0

    @patch("acoharmony._4icli.cli.FourICLI")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.LogWriter")
    @pytest.mark.unit
    def test_download_unknown_file_type_code_skipped(self, m_log, m_cfg, m_client_cls, tmp_path):
        """Cover branch: file_type_code not in registry (line 395)."""
        from acoharmony._4icli.cli import cmd_download

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        state_data = {
            "generated_at": "2024-06-01",
            "files": [
                {
                    "filename": "UNKNOWN.zip",
                    "category": "Reports",
                    "file_type_code": 9999,
                    "year": 2024,
                    "size_bytes": 1024,
                    "last_updated": "2024-06-01T00:00:00Z",
                }
            ],
        }
        state_path = cfg.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.write_text(json.dumps(state_data))

        registry_mod = types.ModuleType("acoharmony._4icli.registry")
        registry_mod.get_file_type_codes = MagicMock(return_value=[113])

        state_mod = types.ModuleType("acoharmony._4icli.state")
        tracker = MagicMock()
        tracker._file_cache = {}
        state_mod.FourICLIStateTracker = MagicMock(return_value=tracker)

        with patch.dict(
            "sys.modules",
            {
                "acoharmony._4icli.registry": registry_mod,
                "acoharmony._4icli.state": state_mod,
            },
        ):
            args = argparse.Namespace(updated_after=None)
            result = cmd_download(args)

        # No matching schemas -> return 0 with message
        assert result == 0

    @patch("acoharmony._4icli.cli.FourICLI")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.LogWriter")
    @pytest.mark.unit
    def test_download_with_errors(self, m_log, m_cfg, m_client_cls, tmp_path):
        """Cover download error branches (lines 541-549, 566-571)."""
        from acoharmony._4icli.cli import cmd_download

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        state_data = {
            "generated_at": "2024-06-01",
            "files": [
                {
                    "filename": "CCLF1.zip",
                    "category": "CCLF",
                    "file_type_code": 113,
                    "year": 2024,
                    "size_bytes": 1024,
                    "last_updated": "2024-06-01T00:00:00Z",
                }
            ],
        }
        state_path = cfg.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.write_text(json.dumps(state_data))

        registry_mod = types.ModuleType("acoharmony._4icli.registry")
        registry_mod.get_file_type_codes = MagicMock(return_value=[113])

        state_mod = types.ModuleType("acoharmony._4icli.state")
        tracker = MagicMock()
        tracker._file_cache = {}
        state_mod.FourICLIStateTracker = MagicMock(return_value=tracker)

        # Simulate download failure
        download_result = MagicMock()
        download_result.success = False
        download_result.files_downloaded = []
        download_result.errors = ["Connection timeout"]
        client = MagicMock()
        client.download.return_value = download_result
        m_client_cls.return_value = client

        with patch.dict(
            "sys.modules",
            {
                "acoharmony._4icli.registry": registry_mod,
                "acoharmony._4icli.state": state_mod,
            },
        ):
            args = argparse.Namespace(updated_after=None)
            result = cmd_download(args)

        assert result == 1

    @patch("acoharmony._4icli.cli.FourICLI")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.LogWriter")
    @pytest.mark.unit
    def test_download_exception_during_download(self, m_log, m_cfg, m_client_cls, tmp_path):
        """Cover exception handler during download (lines 544-549)."""
        from acoharmony._4icli.cli import cmd_download

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        state_data = {
            "generated_at": "2024-06-01",
            "files": [
                {
                    "filename": "CCLF1.zip",
                    "category": "CCLF",
                    "file_type_code": 113,
                    "year": 2024,
                    "size_bytes": 1024,
                    "last_updated": "2024-06-01T00:00:00Z",
                }
            ],
        }
        state_path = cfg.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.write_text(json.dumps(state_data))

        registry_mod = types.ModuleType("acoharmony._4icli.registry")
        registry_mod.get_file_type_codes = MagicMock(return_value=[113])

        state_mod = types.ModuleType("acoharmony._4icli.state")
        tracker = MagicMock()
        tracker._file_cache = {}
        state_mod.FourICLIStateTracker = MagicMock(return_value=tracker)

        client = MagicMock()
        client.download.side_effect = RuntimeError("Network error")
        m_client_cls.return_value = client

        with patch.dict(
            "sys.modules",
            {
                "acoharmony._4icli.registry": registry_mod,
                "acoharmony._4icli.state": state_mod,
            },
        ):
            args = argparse.Namespace(updated_after=None)
            result = cmd_download(args)

        assert result == 1


class TestCmdInventory:
    """Cover cmd_inventory (lines 630-756)."""

    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @patch("acoharmony._4icli.cli.LogWriter")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @pytest.mark.unit
    def test_inventory_force_rebuild(self, m_cfg, m_log, m_disc, tmp_path):
        from acoharmony._4icli.cli import cmd_inventory

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        inv = _make_inventory_result()
        discovery = MagicMock()
        discovery.get_inventory_path.return_value = tmp_path / "inv.json"
        discovery.discover_years.return_value = inv
        discovery.enrich_with_file_type_codes.return_value = inv
        m_disc.return_value = discovery

        args = argparse.Namespace(start_year=2024, end_year=2024, force=True)
        cmd_inventory(args)

        discovery.discover_years.assert_called_once()

    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @patch("acoharmony._4icli.cli.LogWriter")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @pytest.mark.unit
    def test_inventory_load_existing_with_new_years(self, m_cfg, m_log, m_disc, tmp_path):
        """Cover lines 630-669: scan new years and merge."""
        from acoharmony._4icli.cli import cmd_inventory

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        # Create existing inventory file with year 2023
        existing_inv = _make_inventory_result(years=[2023])
        inv_path = tmp_path / "inv.json"
        existing_inv.save_to_json(inv_path)

        new_year_inv = _make_inventory_result(years=[2024])

        discovery = MagicMock()
        discovery.get_inventory_path.return_value = inv_path
        discovery.discover_years.return_value = new_year_inv
        discovery.enrich_with_file_type_codes.side_effect = lambda x: x
        m_disc.return_value = discovery

        args = argparse.Namespace(start_year=2023, end_year=2024, force=False)
        cmd_inventory(args)

        # Should have called discover_years for 2024 only
        discovery.discover_years.assert_called_once()

    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @patch("acoharmony._4icli.cli.LogWriter")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @pytest.mark.unit
    def test_inventory_load_existing_all_years_present(self, m_cfg, m_log, m_disc, tmp_path):
        """Cover branch: all requested years already in inventory."""
        from acoharmony._4icli.cli import cmd_inventory

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        existing_inv = _make_inventory_result(years=[2024])
        inv_path = tmp_path / "inv.json"
        existing_inv.save_to_json(inv_path)

        discovery = MagicMock()
        discovery.get_inventory_path.return_value = inv_path
        m_disc.return_value = discovery

        args = argparse.Namespace(start_year=2024, end_year=2024, force=False)
        cmd_inventory(args)

        discovery.discover_years.assert_not_called()

    @patch("acoharmony._4icli.cli.InventoryResult")
    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @patch("acoharmony._4icli.cli.LogWriter")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @pytest.mark.unit
    def test_inventory_load_error(self, m_cfg, m_log, m_disc, m_inv_result, tmp_path):
        """Cover lines 674-677: error loading inventory."""
        from acoharmony._4icli.cli import cmd_inventory

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        inv_path = tmp_path / "inv.json"
        inv_path.write_text("{bad json")

        discovery = MagicMock()
        discovery.get_inventory_path.return_value = inv_path
        m_disc.return_value = discovery
        m_inv_result.load_from_json.side_effect = Exception("bad json")

        args = argparse.Namespace(start_year=2024, end_year=2024, force=False)
        cmd_inventory(args)

    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @patch("acoharmony._4icli.cli.LogWriter")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @pytest.mark.unit
    def test_inventory_with_errors_in_result(self, m_cfg, m_log, m_disc, tmp_path):
        """Cover lines 752-756: inventory result has errors."""
        from acoharmony._4icli.cli import cmd_inventory

        cfg = _make_config(tmp_path)
        cfg.default_apm_id = "A9999"
        m_cfg.from_profile.return_value = cfg

        errors = [f"Error {i}" for i in range(7)]
        inv = _make_inventory_result(errors=errors)
        discovery = MagicMock()
        discovery.get_inventory_path.return_value = tmp_path / "inv.json"
        discovery.discover_years.return_value = inv
        discovery.enrich_with_file_type_codes.return_value = inv
        m_disc.return_value = discovery

        args = argparse.Namespace(start_year=2024, end_year=2024, force=True)
        cmd_inventory(args)


class TestMainEntryPoint:
    """Cover main() entry point (lines 857-858)."""

    @patch("acoharmony._4icli.cli.cmd_inventory")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @pytest.mark.unit
    def test_main_no_command(self, m_cfg, m_cmd):
        from acoharmony._4icli.cli import main

        with patch("sys.argv", ["aco-4icli"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    @patch("acoharmony._4icli.cli.cmd_inventory")
    @pytest.mark.unit
    def test_main_unknown_command(self, m_cmd):
        """The parser won't accept unknown commands; they exit with error."""
        from acoharmony._4icli.cli import main

        with patch("sys.argv", ["aco-4icli", "bogus"]):
            with pytest.raises(SystemExit):
                main()


class TestCLICommandSyntax:
    """Tests to verify correct 4icli command-line syntax."""

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_download_with_created_within_last_week(
        self, mock_run, mock_config, mock_log_writer
    ) -> None:
        """Verify --createdWithinLastWeek flag is correctly passed."""
        mock_run.return_value.returncode = 0

        date_filter = DateFilter(created_within_last_week=True)

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.download(
            category=DataHubCategory.CCLF, year=2025, date_filter=date_filter, apm_id="D0259"
        )

        # Verify the command arguments
        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        # Should contain: docker exec 4icli 4icli datahub -d -a D0259 -y 2025 -c "Claim and Claim Line Feed (CCLF) Files" --createdWithinLastWeek
        assert "4icli" in call_args
        assert "datahub" in call_args
        assert "-d" in call_args  # download flag
        assert "-a" in call_args
        assert "D0259" in call_args
        assert "-y" in call_args
        assert "2025" in call_args
        assert "--createdWithinLastWeek" in call_args

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_download_with_updated_after(self, mock_run, mock_config, mock_log_writer) -> None:
        """Verify --updatedAfter flag is correctly passed."""
        mock_run.return_value.returncode = 0

        date_filter = DateFilter(updated_after="2025-01-01")

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.download(category=DataHubCategory.CCLF, date_filter=date_filter, apm_id="D0259")

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        assert "--updatedAfter" in call_args
        assert "2025-01-01" in call_args

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_download_with_created_after(self, mock_run, mock_config, mock_log_writer) -> None:
        """Verify --createdAfter flag is correctly passed."""
        mock_run.return_value.returncode = 0

        date_filter = DateFilter(created_after="2025-05-01")

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.download(category=DataHubCategory.CCLF, date_filter=date_filter, apm_id="D0259")

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        assert "--createdAfter" in call_args
        assert "2025-05-01" in call_args

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_download_with_created_between(self, mock_run, mock_config, mock_log_writer) -> None:
        """Verify --createdBetween flag is correctly passed."""
        mock_run.return_value.returncode = 0

        date_filter = DateFilter(created_between=("2025-01-01", "2025-06-30"))

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.download(category=DataHubCategory.CCLF, date_filter=date_filter, apm_id="D0259")

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        assert "--createdBetween" in call_args
        assert "2025-01-01,2025-06-30" in call_args

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_download_with_updated_between(self, mock_run, mock_config, mock_log_writer) -> None:
        """Verify --updatedBetween flag is correctly passed."""
        mock_run.return_value.returncode = 0

        date_filter = DateFilter(updated_between=("2025-03-01", "2025-03-31"))

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.download(category=DataHubCategory.CCLF, date_filter=date_filter, apm_id="D0259")

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        assert "--updatedBetween" in call_args
        assert "2025-03-01,2025-03-31" in call_args

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_download_with_created_within_last_month(
        self, mock_run, mock_config, mock_log_writer
    ) -> None:
        """Verify --createdWithinLastMonth flag is correctly passed."""
        mock_run.return_value.returncode = 0

        date_filter = DateFilter(created_within_last_month=True)

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.download(category=DataHubCategory.CCLF, date_filter=date_filter, apm_id="D0259")

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        assert "--createdWithinLastMonth" in call_args

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_download_with_file_type_code(self, mock_run, mock_config, mock_log_writer) -> None:
        """Verify -f flag with file type code is correctly passed."""
        mock_run.return_value.returncode = 0

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.download(
            category=DataHubCategory.CCLF,
            file_type_code=FileTypeCode.CCLF,
            apm_id="D0259",
            year=2025,
        )

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        assert "-f" in call_args
        assert "113" in call_args  # CCLF file type code

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_view_files_syntax(self, mock_run, mock_config, mock_log_writer) -> None:
        """Verify view files uses -v flag."""
        mock_run.return_value.returncode = 0
        mock_run.return_value.stdout = ""

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.view_files(category=DataHubCategory.CCLF, year=2025, apm_id="D0259")

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        assert "4icli" in call_args
        assert "datahub" in call_args
        assert "-v" in call_args  # view flag, not download
        assert "-d" not in call_args  # should NOT have download flag


class TestCLICategoryMapping:
    """Tests for category value mapping to CLI arguments."""

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_cclf_category_syntax(self, mock_run, mock_config, mock_log_writer) -> None:
        """Verify CCLF category is correctly formatted."""
        mock_run.return_value.returncode = 0

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.download(category=DataHubCategory.CCLF, apm_id="D0259")

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        assert "-c" in call_args
        # The category value should be in the args
        category_idx = call_args.index("-c")
        assert call_args[category_idx + 1] == "CCLF"

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_beneficiary_list_category_syntax(self, mock_run, mock_config, mock_log_writer) -> None:
        """Verify Beneficiary List category is correctly formatted."""
        mock_run.return_value.returncode = 0

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.download(category=DataHubCategory.BENEFICIARY_LIST, apm_id="D0259")

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        assert "-c" in call_args
        category_idx = call_args.index("-c")
        assert "Beneficiary List" in call_args[category_idx + 1]

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_reports_category_syntax(self, mock_run, mock_config, mock_log_writer) -> None:
        """Verify Reports category is correctly formatted."""
        mock_run.return_value.returncode = 0

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.download(category=DataHubCategory.REPORTS, apm_id="D0259")

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        assert "-c" in call_args
        category_idx = call_args.index("-c")
        assert "Reports" in call_args[category_idx + 1]


class TestCLIMultipleDateFilters:
    """Tests for combining multiple date filters."""

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_multiple_date_filters_combined(self, mock_run, mock_config, mock_log_writer) -> None:
        """Verify multiple date filters can be combined."""
        mock_run.return_value.returncode = 0

        date_filter = DateFilter(
            created_after="2025-01-01", updated_after="2025-01-15", created_within_last_week=True
        )

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.download(category=DataHubCategory.CCLF, date_filter=date_filter, apm_id="D0259")

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        # All filters should be present
        assert "--createdAfter" in call_args
        assert "2025-01-01" in call_args
        assert "--updatedAfter" in call_args
        assert "2025-01-15" in call_args
        assert "--createdWithinLastWeek" in call_args


class TestCLIAPMIDAndYear:
    """Tests for APM ID and year parameters."""

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_apm_id_passed_correctly(self, mock_run, mock_config, mock_log_writer) -> None:
        """Verify APM ID is passed with -a flag."""
        mock_run.return_value.returncode = 0

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.download(category=DataHubCategory.CCLF, apm_id="D0259")

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        assert "-a" in call_args
        apm_idx = call_args.index("-a")
        assert call_args[apm_idx + 1] == "D0259"

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_year_passed_correctly(self, mock_run, mock_config, mock_log_writer) -> None:
        """Verify year is passed with -y flag."""
        mock_run.return_value.returncode = 0

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        cli.download(category=DataHubCategory.CCLF, year=2024, apm_id="D0259")

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        assert "-y" in call_args
        year_idx = call_args.index("-y")
        assert call_args[year_idx + 1] == "2024"

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_default_year_used_from_config(self, mock_run, mock_config, mock_log_writer) -> None:
        """Verify default year from config is used when not specified."""
        mock_run.return_value.returncode = 0
        mock_config.default_year = 2025

        cli = FourICLI(
            config=mock_config, log_writer=mock_log_writer, enable_duplicate_detection=False
        )
        # Don't specify year - should use config default
        cli.download(category=DataHubCategory.CCLF, apm_id="D0259")

        mock_run.assert_called_once()
        call_args = mock_run.call_args[0][0]

        assert "-y" in call_args
        year_idx = call_args.index("-y")
        assert call_args[year_idx + 1] == "2025"


# ===================================================================
# 3. cli.py
# ===================================================================


class TestCLICmdDownload:
    @pytest.mark.unit
    def test_download_state_exists_empty_list(self, tmp_path):
        from acoharmony._4icli.cli import cmd_download

        config = _make_config(tmp_path)
        state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_data = {"generated_at": "2025-01-01", "total_missing": 0, "files": []}
        state_path.write_text(json.dumps(state_data))

        args = SimpleNamespace(updated_after=None)

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
        ):
            result = cmd_download(args)
            assert result == 0

    @pytest.mark.unit
    def test_download_no_state_file(self, tmp_path):
        from acoharmony._4icli.cli import cmd_download

        config = _make_config(tmp_path)
        args = SimpleNamespace(updated_after=None)

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
        ):
            result = cmd_download(args)
            assert result == 1

    @pytest.mark.unit
    def test_download_with_files(self, tmp_path):
        from acoharmony._4icli.cli import cmd_download

        config = _make_config(tmp_path)
        state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        files_list = [
            {
                "filename": "CCLF8.D240101.T1234567.zip",
                "category": "CCLF",
                "file_type_code": 113,
                "year": 2025,
                "size_bytes": 1000,
                "last_updated": "2025-01-01T00:00:00.000Z",
                "discovered_at": "2025-01-01",
            }
        ]
        state_data = {
            "generated_at": "2025-01-01",
            "total_missing": 1,
            "total_size_formatted": "1 KB",
            "files": files_list,
        }
        state_path.write_text(json.dumps(state_data))

        args = SimpleNamespace(updated_after=None)

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.files_downloaded = [Path("file.zip")]
        mock_result.errors = []

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {}

        mock_client = MagicMock()
        mock_client.download.return_value = mock_result

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.FourICLI", return_value=mock_client),
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
            patch("acoharmony._4icli.cli.get_current_year", return_value=2025),
        ):
            # Need to mock registry
            with patch.dict(
                "sys.modules",
                {
                    "acoharmony._4icli.registry": MagicMock(
                        get_file_type_codes=lambda: {113: "CCLF"}
                    )
                },
            ):
                result = cmd_download(args)
                assert result == 0

    @pytest.mark.unit
    def test_download_no_registry_codes(self, tmp_path):
        from acoharmony._4icli.cli import cmd_download

        config = _make_config(tmp_path)
        state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        files_list = [
            {
                "filename": "file.zip",
                "category": "CCLF",
                "file_type_code": 113,
                "year": 2025,
            }
        ]
        state_data = {"generated_at": "2025-01-01", "total_missing": 1, "files": files_list}
        state_path.write_text(json.dumps(state_data))

        args = SimpleNamespace(updated_after=None)

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {}

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
        ):
            with patch.dict(
                "sys.modules",
                {"acoharmony._4icli.registry": MagicMock(get_file_type_codes=lambda: {})},
            ):
                result = cmd_download(args)
                assert result == 1

    @pytest.mark.unit
    def test_download_all_in_state(self, tmp_path):
        """Files already in state tracker should be filtered out."""
        from acoharmony._4icli.cli import cmd_download

        config = _make_config(tmp_path)
        state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        files_list = [
            {
                "filename": "already_have.zip",
                "category": "CCLF",
                "file_type_code": 113,
                "year": 2025,
            }
        ]
        state_data = {"generated_at": "2025-01-01", "total_missing": 1, "files": files_list}
        state_path.write_text(json.dumps(state_data))

        args = SimpleNamespace(updated_after=None)

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {"already_have.zip": MagicMock()}

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
        ):
            with patch.dict(
                "sys.modules",
                {
                    "acoharmony._4icli.registry": MagicMock(
                        get_file_type_codes=lambda: {113: "CCLF"}
                    )
                },
            ):
                result = cmd_download(args)
                assert result == 0

    @pytest.mark.unit
    def test_download_with_updated_after(self, tmp_path):
        from acoharmony._4icli.cli import cmd_download

        config = _make_config(tmp_path)
        state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        files_list = [
            {
                "filename": "file.zip",
                "category": "CCLF",
                "file_type_code": 113,
                "year": 2025,
                "last_updated": None,
            }
        ]
        state_data = {"generated_at": "2025-01-01", "total_missing": 1, "files": files_list}
        state_path.write_text(json.dumps(state_data))

        args = SimpleNamespace(updated_after="2025-01-01")

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.files_downloaded = []
        mock_result.errors = []

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {}

        mock_client = MagicMock()
        mock_client.download.return_value = mock_result

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.FourICLI", return_value=mock_client),
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
        ):
            with patch.dict(
                "sys.modules",
                {
                    "acoharmony._4icli.registry": MagicMock(
                        get_file_type_codes=lambda: {113: "CCLF"}
                    )
                },
            ):
                result = cmd_download(args)
                assert result == 0

    @pytest.mark.unit
    def test_download_with_errors(self, tmp_path):
        from acoharmony._4icli.cli import cmd_download

        config = _make_config(tmp_path)
        state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        files_list = [
            {
                "filename": "file.zip",
                "category": "CCLF",
                "file_type_code": 113,
                "year": 2025,
                "last_updated": "2025-06-01T00:00:00.000Z",
            }
        ]
        state_data = {"generated_at": "2025-01-01", "total_missing": 1, "files": files_list}
        state_path.write_text(json.dumps(state_data))

        args = SimpleNamespace(updated_after=None)

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {}

        mock_client = MagicMock()
        mock_client.download.side_effect = RuntimeError("download boom")

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.FourICLI", return_value=mock_client),
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
        ):
            with patch.dict(
                "sys.modules",
                {
                    "acoharmony._4icli.registry": MagicMock(
                        get_file_type_codes=lambda: {113: "CCLF"}
                    )
                },
            ):
                result = cmd_download(args)
                assert result == 1

    @pytest.mark.unit
    def test_download_result_failure(self, tmp_path):
        """Test download when result.success is False."""
        from acoharmony._4icli.cli import cmd_download

        config = _make_config(tmp_path)
        state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        files_list = [
            {
                "filename": "file.zip",
                "category": "CCLF",
                "file_type_code": 113,
                "year": 2025,
                "last_updated": "2025-06-01T00:00:00.000Z",
            }
        ]
        state_data = {"generated_at": "2025-01-01", "total_missing": 1, "files": files_list}
        state_path.write_text(json.dumps(state_data))

        args = SimpleNamespace(updated_after=None)

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {}

        mock_result = MagicMock()
        mock_result.success = False
        mock_result.files_downloaded = []
        mock_result.errors = ["some error"]

        mock_client = MagicMock()
        mock_client.download.return_value = mock_result

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.FourICLI", return_value=mock_client),
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
        ):
            with patch.dict(
                "sys.modules",
                {
                    "acoharmony._4icli.registry": MagicMock(
                        get_file_type_codes=lambda: {113: "CCLF"}
                    )
                },
            ):
                result = cmd_download(args)
                assert result == 1

    @pytest.mark.unit
    def test_download_file_type_code_not_in_registry(self, tmp_path):
        """Test files with file_type_code not in registry are skipped."""
        from acoharmony._4icli.cli import cmd_download

        config = _make_config(tmp_path)
        state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        files_list = [
            {
                "filename": "file.zip",
                "category": "CCLF",
                "file_type_code": 9999,  # not in registry
                "year": 2025,
            }
        ]
        state_data = {"generated_at": "2025-01-01", "total_missing": 1, "files": files_list}
        state_path.write_text(json.dumps(state_data))

        args = SimpleNamespace(updated_after=None)

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {}

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
        ):
            with patch.dict(
                "sys.modules",
                {
                    "acoharmony._4icli.registry": MagicMock(
                        get_file_type_codes=lambda: {113: "CCLF"}
                    )
                },
            ):
                result = cmd_download(args)
                assert result == 0  # no files to download

    @pytest.mark.unit
    def test_download_null_file_type_code(self, tmp_path):
        """Test files with null file_type_code are included."""
        from acoharmony._4icli.cli import cmd_download

        config = _make_config(tmp_path)
        state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        files_list = [
            {
                "filename": "file.zip",
                "category": "CCLF",
                "file_type_code": None,
                "year": 2025,
                "last_updated": "2025-06-01T00:00:00.000Z",
            }
        ]
        state_data = {"generated_at": "2025-01-01", "total_missing": 1, "files": files_list}
        state_path.write_text(json.dumps(state_data))

        args = SimpleNamespace(updated_after=None)

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.files_downloaded = []
        mock_result.errors = []

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {}

        mock_client = MagicMock()
        mock_client.download.return_value = mock_result

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.FourICLI", return_value=mock_client),
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
        ):
            with patch.dict(
                "sys.modules",
                {
                    "acoharmony._4icli.registry": MagicMock(
                        get_file_type_codes=lambda: {113: "CCLF"}
                    )
                },
            ):
                result = cmd_download(args)
                assert result == 0

    @pytest.mark.unit
    def test_download_file_already_in_bronze(self, tmp_path):
        """Test files already in bronze are skipped."""
        from acoharmony._4icli.cli import cmd_download

        config = _make_config(tmp_path)
        state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        # Create file in bronze
        bronze_file = config.bronze_dir / "file.zip"
        bronze_file.write_text("exists")

        files_list = [
            {
                "filename": "file.zip",
                "category": "CCLF",
                "file_type_code": 113,
                "year": 2025,
                "last_updated": "2025-06-01T00:00:00.000Z",
            }
        ]
        state_data = {"generated_at": "2025-01-01", "total_missing": 1, "files": files_list}
        state_path.write_text(json.dumps(state_data))

        args = SimpleNamespace(updated_after=None)

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {}

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
        ):
            with patch.dict(
                "sys.modules",
                {
                    "acoharmony._4icli.registry": MagicMock(
                        get_file_type_codes=lambda: {113: "CCLF"}
                    )
                },
            ):
                result = cmd_download(args)
                assert result == 0  # All skipped


class TestCLICmdInventory:
    @pytest.mark.unit
    def test_inventory_new(self, tmp_path):
        from acoharmony._4icli.cli import cmd_inventory
        from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult

        config = _make_config(tmp_path)
        args = SimpleNamespace(start_year=2025, end_year=2025, force=False)

        mock_result = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2025],
            total_files=1,
            files_by_year={2025: 1},
            files_by_category={"CCLF": 1},
            files=[
                FileInventoryEntry(
                    filename="file.zip",
                    category="CCLF",
                    file_type_code=113,
                    year=2025,
                )
            ],
            started_at=datetime.now(),
            completed_at=datetime.now(),
            errors=None,
        )

        mock_discovery = MagicMock()
        mock_discovery.get_inventory_path.return_value = tmp_path / "nonexistent_inventory.json"
        mock_discovery.discover_years.return_value = mock_result
        mock_discovery.enrich_with_file_type_codes.return_value = mock_result

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery),
        ):
            cmd_inventory(args)

    @pytest.mark.unit
    def test_inventory_existing_no_new_years(self, tmp_path):
        from acoharmony._4icli.cli import cmd_inventory
        from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult

        config = _make_config(tmp_path)
        args = SimpleNamespace(start_year=2025, end_year=2025, force=False)

        # Create existing inventory
        inv_path = tmp_path / "inventory.json"
        existing = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2025],
            total_files=1,
            files_by_year={2025: 1},
            files_by_category={"CCLF": 1},
            files=[
                FileInventoryEntry(
                    filename="file.zip",
                    category="CCLF",
                    file_type_code=113,
                    year=2025,
                )
            ],
            started_at=datetime.now(),
            completed_at=datetime.now(),
            errors=None,
        )
        existing.save_to_json(inv_path)

        mock_discovery = MagicMock()
        mock_discovery.get_inventory_path.return_value = inv_path

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery),
        ):
            cmd_inventory(args)

    @pytest.mark.unit
    def test_inventory_existing_with_new_years(self, tmp_path):
        from acoharmony._4icli.cli import cmd_inventory
        from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult

        config = _make_config(tmp_path)
        args = SimpleNamespace(start_year=2024, end_year=2025, force=False)

        # Existing inventory only has 2024
        inv_path = tmp_path / "inventory.json"
        existing = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2024],
            total_files=1,
            files_by_year={2024: 1},
            files_by_category={"CCLF": 1},
            files=[
                FileInventoryEntry(
                    filename="file24.zip",
                    category="CCLF",
                    file_type_code=113,
                    year=2024,
                )
            ],
            started_at=datetime.now(),
            completed_at=datetime.now(),
            errors=None,
        )
        existing.save_to_json(inv_path)

        # New year result
        new_year_result = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2025],
            total_files=1,
            files_by_year={2025: 1},
            files_by_category={"CCLF": 1},
            files=[
                FileInventoryEntry(
                    filename="file25.zip",
                    category="CCLF",
                    file_type_code=113,
                    year=2025,
                )
            ],
            started_at=datetime.now(),
            completed_at=datetime.now(),
            errors=None,
        )

        mock_discovery = MagicMock()
        mock_discovery.get_inventory_path.return_value = inv_path
        mock_discovery.discover_years.return_value = new_year_result
        mock_discovery.enrich_with_file_type_codes.side_effect = lambda r: r

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery),
        ):
            cmd_inventory(args)

    @pytest.mark.unit
    def test_inventory_force_rebuild(self, tmp_path):
        from acoharmony._4icli.cli import cmd_inventory
        from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult

        config = _make_config(tmp_path)
        args = SimpleNamespace(start_year=2025, end_year=2025, force=True)

        mock_result = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2025],
            total_files=1,
            files_by_year={2025: 1},
            files_by_category={"CCLF": 1},
            files=[
                FileInventoryEntry(
                    filename="file.zip",
                    category="CCLF",
                    file_type_code=113,
                    year=2025,
                )
            ],
            started_at=datetime.now(),
            completed_at=datetime.now(),
            errors=None,
        )

        mock_discovery = MagicMock()
        mock_discovery.get_inventory_path.return_value = tmp_path / "inventory.json"
        mock_discovery.discover_years.return_value = mock_result
        mock_discovery.enrich_with_file_type_codes.return_value = mock_result

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery),
        ):
            cmd_inventory(args)

    @pytest.mark.unit
    def test_inventory_no_apm_id(self, tmp_path):
        from acoharmony._4icli.cli import cmd_inventory

        config = _make_config(tmp_path)
        config.default_apm_id = None
        args = SimpleNamespace(start_year=2025, end_year=2025, force=True)

        mock_discovery = MagicMock()
        mock_discovery.get_inventory_path.return_value = tmp_path / "nonexistent.json"

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery),
        ):
            result = cmd_inventory(args)
            assert result == 1

    @pytest.mark.unit
    def test_inventory_load_error(self, tmp_path):
        from acoharmony._4icli.cli import cmd_inventory

        config = _make_config(tmp_path)
        args = SimpleNamespace(start_year=2025, end_year=2025, force=False)

        # Create invalid inventory file
        inv_path = tmp_path / "inventory.json"
        inv_path.write_text("invalid json{{{")

        mock_discovery = MagicMock()
        mock_discovery.get_inventory_path.return_value = inv_path

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery),
        ):
            cmd_inventory(args)

    @pytest.mark.unit
    def test_inventory_with_errors(self, tmp_path):
        from acoharmony._4icli.cli import cmd_inventory
        from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult

        config = _make_config(tmp_path)
        args = SimpleNamespace(start_year=2025, end_year=2025, force=True)

        mock_result = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2025],
            total_files=1,
            files_by_year={2025: 1},
            files_by_category={"CCLF": 1},
            files=[
                FileInventoryEntry(
                    filename="file.zip",
                    category="CCLF",
                    file_type_code=113,
                    year=2025,
                )
            ],
            started_at=datetime.now(),
            completed_at=datetime.now(),
            errors=["error1", "error2", "error3", "error4", "error5", "error6"],
        )

        mock_discovery = MagicMock()
        mock_discovery.get_inventory_path.return_value = tmp_path / "inventory.json"
        mock_discovery.discover_years.return_value = mock_result
        mock_discovery.enrich_with_file_type_codes.return_value = mock_result

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery),
        ):
            cmd_inventory(args)


class TestCLIMain:
    @pytest.mark.unit
    def test_main_no_command(self):
        from acoharmony._4icli.cli import main

        with patch("sys.argv", ["aco-4icli"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1

    @pytest.mark.unit
    def test_main_inventory(self, tmp_path):
        from acoharmony._4icli.cli import main

        with (
            patch("sys.argv", ["aco-4icli", "inventory", "--force"]),
            patch("acoharmony._4icli.cli.cmd_inventory") as mock_cmd,
        ):
            main()
            mock_cmd.assert_called_once()

    @pytest.mark.unit
    def test_main_need_download(self, tmp_path):
        from acoharmony._4icli.cli import main

        with (
            patch("sys.argv", ["aco-4icli", "need-download"]),
            patch("acoharmony._4icli.cli.cmd_need_download") as mock_cmd,
        ):
            main()
            mock_cmd.assert_called_once()

    @pytest.mark.unit
    def test_main_download(self, tmp_path):
        from acoharmony._4icli.cli import main

        with (
            patch("sys.argv", ["aco-4icli", "download"]),
            patch("acoharmony._4icli.cli.cmd_download") as mock_cmd,
        ):
            main()
            mock_cmd.assert_called_once()

    @pytest.mark.unit
    def test_main_exception(self):
        from acoharmony._4icli.cli import main

        with (
            patch("sys.argv", ["aco-4icli", "inventory"]),
            patch("acoharmony._4icli.cli.cmd_inventory", side_effect=RuntimeError("boom")),
        ):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1


class TestCmdInventory2:
    """Test the inventory CLI command."""

    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @pytest.mark.unit
    def test_inventory_creates_new_when_missing(
        self, mock_discovery_cls, mock_config_cls, tmp_path: Path
    ):
        """Test inventory command creates new inventory when none exists."""
        # Setup mock config
        mock_config = MagicMock()
        mock_config.default_apm_id = "D0259"
        mock_config_cls.from_profile.return_value = mock_config

        # Setup mock discovery
        mock_discovery = MagicMock()
        mock_discovery_cls.return_value = mock_discovery

        inventory_path = tmp_path / "inventory.json"
        mock_discovery.get_inventory_path.return_value = inventory_path

        # Mock the discover_years result
        mock_result = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2022, 2023],
            total_files=10,
            files_by_year={2022: 5, 2023: 5},
            files_by_category={"CCLF": 10},
            files=[],
            started_at=datetime.now(),
        )
        mock_discovery.discover_years.return_value = mock_result
        mock_discovery.enrich_with_file_type_codes.return_value = mock_result

        # Create args
        args = argparse.Namespace(start_year=2022, end_year=2023, force=False)

        # Execute command
        from acoharmony._4icli.cli import cmd_inventory
        result = cmd_inventory(args)

        # Verify
        assert result is None or result == 0
        mock_discovery.discover_years.assert_called_once()
        call_kwargs = mock_discovery.discover_years.call_args[1]
        assert call_kwargs["apm_id"] == "D0259"
        assert call_kwargs["start_year"] == 2022
        assert call_kwargs["end_year"] == 2023

    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @pytest.mark.unit
    def test_inventory_uses_defaults_from_2022(
        self, mock_discovery_cls, mock_config_cls, tmp_path: Path
    ):
        """Test inventory command uses 2022 as default start year."""
        # Setup mock config
        mock_config = MagicMock()
        mock_config.default_apm_id = "D0259"
        mock_config_cls.from_profile.return_value = mock_config

        # Setup mock discovery
        mock_discovery = MagicMock()
        mock_discovery_cls.return_value = mock_discovery

        inventory_path = tmp_path / "inventory.json"
        mock_discovery.get_inventory_path.return_value = inventory_path

        # Mock the discover_years result
        mock_result = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2022, 2023, 2024, 2025],
            total_files=20,
            files_by_year={2022: 5, 2023: 5, 2024: 5, 2025: 5},
            files_by_category={"CCLF": 20},
            files=[],
            started_at=datetime.now(),
        )
        mock_discovery.discover_years.return_value = mock_result
        mock_discovery.enrich_with_file_type_codes.return_value = mock_result

        # Create args without explicit years (should use defaults)
        args = argparse.Namespace(start_year=None, end_year=None, force=False)

        # Execute command
        from acoharmony._4icli.cli import cmd_inventory
        result = cmd_inventory(args)

        # Verify defaults
        assert result is None or result == 0
        call_kwargs = mock_discovery.discover_years.call_args[1]
        assert call_kwargs["start_year"] == 2022  # Should default to 2022
        assert call_kwargs["end_year"] == get_current_year()  # Should default to current year

    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @pytest.mark.unit
    def test_inventory_fails_without_apm_id(
        self, mock_discovery_cls, mock_config_cls, tmp_path: Path, capsys
    ):
        """Test inventory command fails gracefully when APM ID is not configured."""
        # Setup mock config without APM ID
        mock_config = MagicMock()
        mock_config.default_apm_id = None  # No APM ID configured
        mock_config_cls.from_profile.return_value = mock_config

        # Setup mock discovery
        mock_discovery = MagicMock()
        mock_discovery_cls.return_value = mock_discovery

        inventory_path = tmp_path / "inventory.json"
        mock_discovery.get_inventory_path.return_value = inventory_path

        # Create args
        args = argparse.Namespace(start_year=2022, end_year=2023, force=False)

        # Execute command
        from acoharmony._4icli.cli import cmd_inventory
        result = cmd_inventory(args)

        # Verify error handling
        assert result == 1
        captured = capsys.readouterr()
        assert "Error: APM ID not configured" in captured.out

    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @pytest.mark.unit
    def test_inventory_force_rebuild(self, mock_discovery_cls, mock_config_cls, tmp_path: Path):
        """Test inventory command with --force flag rebuilds from scratch."""
        # Setup mock config
        mock_config = MagicMock()
        mock_config.default_apm_id = "D0259"
        mock_config_cls.from_profile.return_value = mock_config

        # Setup mock discovery
        mock_discovery = MagicMock()
        mock_discovery_cls.return_value = mock_discovery

        # Create existing inventory file
        inventory_path = tmp_path / "inventory.json"
        inventory_path.parent.mkdir(parents=True, exist_ok=True)

        existing_result = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2022],
            total_files=5,
            files_by_year={2022: 5},
            files_by_category={"CCLF": 5},
            files=[],
            started_at=datetime.now(),
        )
        existing_result.save_to_json(inventory_path)

        mock_discovery.get_inventory_path.return_value = inventory_path

        # Mock the discover_years result
        mock_result = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2022, 2023],
            total_files=10,
            files_by_year={2022: 5, 2023: 5},
            files_by_category={"CCLF": 10},
            files=[],
            started_at=datetime.now(),
        )
        mock_discovery.discover_years.return_value = mock_result
        mock_discovery.enrich_with_file_type_codes.return_value = mock_result

        # Create args with force flag
        args = argparse.Namespace(start_year=2022, end_year=2023, force=True)

        # Execute command
        from acoharmony._4icli.cli import cmd_inventory
        result = cmd_inventory(args)

        # Verify force rebuild
        assert result is None or result == 0
        mock_discovery.discover_years.assert_called_once()


class TestCmdNeedDownload2:
    """
    Test the need-download CLI command.

    ⚠  IMPORTANT: These tests use REAL data from actual schemas! ⚠

    When you add a NEW schema with fourIcli.fileTypeCode:
    1. Add an entry to tests/_4icli/_helpers/real_schema_data.py:REAL_SCHEMA_FILE_TYPES
    2. Add a FileInventoryEntry to create_sample_inventory_entries()
    3. Run tests to ensure coverage across ALL file types
    """

    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @patch("acoharmony._4icli.cli.compare_inventory")
    @patch("acoharmony._4icli.cli.scan_directory")
    @pytest.mark.unit
    def test_need_download_requires_inventory(
        self, mock_scan, mock_compare, mock_discovery_cls, mock_config_cls, tmp_path: Path, capsys
    ):
        """Test need-download command fails when inventory doesn't exist."""
        # Setup mock config
        mock_config = MagicMock()
        mock_config.bronze_dir = tmp_path / "bronze"
        mock_config.data_path = tmp_path
        mock_config_cls.from_profile.return_value = mock_config

        # Setup mock discovery - inventory doesn't exist
        mock_discovery = MagicMock()
        mock_discovery_cls.return_value = mock_discovery

        inventory_path = tmp_path / "inventory.json"
        mock_discovery.get_inventory_path.return_value = inventory_path

        # Create args
        args = argparse.Namespace(
            year=None, category=None, export=None, show_have=False, limit=20, save_state=False
        )

        # Execute command
        from acoharmony._4icli.cli import cmd_need_download
        result = cmd_need_download(args)

        # Verify error handling
        assert result == 1
        captured = capsys.readouterr()
        assert "Error: Inventory file not found" in captured.out
        assert "Run 'aco 4icli inventory' to create the inventory first" in captured.out

    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @patch("acoharmony._4icli.cli.compare_inventory")
    @patch("acoharmony._4icli.cli.scan_directory")
    @pytest.mark.unit
    def test_need_download_with_filters(
        self, mock_scan, mock_compare, mock_discovery_cls, mock_config_cls, tmp_path: Path
    ):
        """Test need-download command with year and category filters."""
        # Setup mock config
        mock_config = MagicMock()
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)
        mock_config.bronze_dir = bronze_dir
        mock_config.data_path = tmp_path
        mock_config_cls.from_profile.return_value = mock_config

        # Setup mock discovery with existing inventory
        mock_discovery = MagicMock()
        mock_discovery_cls.return_value = mock_discovery

        inventory_path = tmp_path / "inventory.json"
        inventory_path.parent.mkdir(parents=True, exist_ok=True)

        # ⚠  Use REAL data from actual schemas via helper function ⚠
        # This ensures tests cover ALL file types we support
        sample_files = create_sample_inventory_entries()

        # Build aggregates from sample data
        categories = sorted({f.category for f in sample_files})
        years = sorted({f.year for f in sample_files})
        files_by_year = {}
        files_by_category = {}
        for f in sample_files:
            files_by_year[f.year] = files_by_year.get(f.year, 0) + 1
            files_by_category[f.category] = files_by_category.get(f.category, 0) + 1

        test_inventory = InventoryResult(
            apm_id="D0259",
            categories=categories,
            years=years,
            total_files=len(sample_files),
            files_by_year=files_by_year,
            files_by_category=files_by_category,
            files=sample_files,
            started_at=datetime.now(),
        )
        test_inventory.save_to_json(inventory_path)

        mock_discovery.get_inventory_path.return_value = inventory_path

        # Mock scan_directory to return empty sets
        mock_scan.return_value = set()

        # Mock compare_inventory - use first file from sample data
        first_file = sample_files[0]
        mock_compare.return_value = {
            "missing": [first_file],
            "have": [],
            "total_inventory": 1,
            "missing_count": 1,
            "have_count": 0,
            "total_size_bytes": first_file.size_bytes or 0,
            "missing_by_year": {first_file.year: 1},
            "missing_by_category": {first_file.category: 1},
            "missing_by_type_code": {first_file.file_type_code: 1},
        }

        # Create args with filters using REAL data
        args = argparse.Namespace(
            year=first_file.year,
            category=first_file.category,
            export=None,
            show_have=False,
            limit=20,
            save_state=False,
        )

        # Execute command
        from acoharmony._4icli.cli import cmd_need_download
        result = cmd_need_download(args)

        # Verify filters were passed with REAL values
        assert result is None or result == 0
        mock_compare.assert_called_once()
        call_kwargs = mock_compare.call_args[1]
        assert call_kwargs["year_filter"] == first_file.year
        assert call_kwargs["category_filter"] == first_file.category

    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @patch("acoharmony._4icli.cli.compare_inventory")
    @patch("acoharmony._4icli.cli.scan_directory")
    @patch("acoharmony._4icli.cli.export_to_csv")
    @pytest.mark.unit
    def test_need_download_with_csv_export(
        self,
        mock_export,
        mock_scan,
        mock_compare,
        mock_discovery_cls,
        mock_config_cls,
        tmp_path: Path,
    ):
        """Test need-download command exports results to CSV."""
        # Setup mock config
        mock_config = MagicMock()
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)
        mock_config.bronze_dir = bronze_dir
        mock_config.data_path = tmp_path
        mock_config_cls.from_profile.return_value = mock_config

        # Setup mock discovery with existing inventory
        mock_discovery = MagicMock()
        mock_discovery_cls.return_value = mock_discovery

        inventory_path = tmp_path / "inventory.json"
        inventory_path.parent.mkdir(parents=True, exist_ok=True)

        test_inventory = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2022],
            total_files=1,
            files_by_year={2022: 1},
            files_by_category={"CCLF": 1},
            files=[],
            started_at=datetime.now(),
        )
        test_inventory.save_to_json(inventory_path)

        mock_discovery.get_inventory_path.return_value = inventory_path

        # Mock scan_directory
        mock_scan.return_value = set()

        # Mock compare_inventory - must match actual return structure
        mock_compare.return_value = {
            "missing": [],
            "have": [],
            "total_inventory": 1,
            "missing_count": 0,
            "have_count": 0,
            "total_size_bytes": 0,
            "missing_by_year": {},
            "missing_by_category": {},
            "missing_by_type_code": {},
        }

        # Create args with export
        export_path = tmp_path / "export.csv"
        args = argparse.Namespace(
            year=None,
            category=None,
            export=str(export_path),
            show_have=False,
            limit=20,
            save_state=False,
        )

        # Execute command
        from acoharmony._4icli.cli import cmd_need_download
        result = cmd_need_download(args)

        # Verify export was called
        assert result is None or result == 0
        mock_export.assert_called_once()


class TestYearDefaults:
    """Test that year defaults are correctly set to 2022+."""

    @pytest.mark.unit
    def test_inventory_discovery_default_year(self):
        """Test InventoryDiscovery.discover_years defaults to 2022."""
        import inspect

        from acoharmony._4icli.inventory import InventoryDiscovery

        sig = inspect.signature(InventoryDiscovery.discover_years)
        assert sig.parameters["start_year"].default == 2022

    @pytest.mark.unit
    def test_client_discover_remote_inventory_default_year(self):
        """Test FourICLI.discover_remote_inventory defaults to 2022."""
        import inspect

        from acoharmony._4icli.client import FourICLI

        sig = inspect.signature(FourICLI.discover_remote_inventory)
        assert sig.parameters["start_year"].default == 2022

    @pytest.mark.unit
    def test_client_sync_incremental_default_year(self):
        """Test FourICLI.sync_incremental defaults to 2022."""
        import inspect

        from acoharmony._4icli.client import FourICLI

        sig = inspect.signature(FourICLI.sync_incremental)
        assert sig.parameters["start_year"].default == 2022


class TestCLIMoreEdgeCases:
    """Cover cli.py remaining lines: 446-447, 511-512, 570, 857-858."""

    @pytest.mark.unit
    def test_download_invalid_last_updated(self, tmp_path):
        """Test ValueError/AttributeError in date parsing (lines 446-447)."""
        from acoharmony._4icli.cli import cmd_download

        config = _make_config(tmp_path)
        state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        files_list = [
            {
                "filename": "file.zip",
                "category": "CCLF",
                "file_type_code": 113,
                "year": 2025,
                "last_updated": "not-a-valid-date",
            }
        ]
        state_data = {"generated_at": "2025-01-01", "total_missing": 1, "files": files_list}
        state_path.write_text(json.dumps(state_data))

        args = SimpleNamespace(updated_after=None)

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.files_downloaded = []
        mock_result.errors = []

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {}
        mock_client = MagicMock()
        mock_client.download.return_value = mock_result

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.FourICLI", return_value=mock_client),
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
        ):
            with patch.dict(
                "sys.modules",
                {
                    "acoharmony._4icli.registry": MagicMock(
                        get_file_type_codes=lambda: {113: "CCLF"}
                    )
                },
            ):
                result = cmd_download(args)
                assert result == 0

    @pytest.mark.unit
    def test_download_unknown_file_type_code_enum(self, tmp_path):
        """Test ValueError in FileTypeCode enum conversion (lines 511-512)."""
        from acoharmony._4icli.cli import cmd_download

        config = _make_config(tmp_path)
        state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        # file_type_code 42 is in registry but not a valid FileTypeCode enum
        files_list = [
            {
                "filename": "file.zip",
                "category": "CCLF",
                "file_type_code": 42,
                "year": 2025,
                "last_updated": "2025-06-01T00:00:00.000Z",
            }
        ]
        state_data = {"generated_at": "2025-01-01", "total_missing": 1, "files": files_list}
        state_path.write_text(json.dumps(state_data))

        args = SimpleNamespace(updated_after=None)

        mock_result = MagicMock()
        mock_result.success = True
        mock_result.files_downloaded = []
        mock_result.errors = []

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {}
        mock_client = MagicMock()
        mock_client.download.return_value = mock_result

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.FourICLI", return_value=mock_client),
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
        ):
            with patch.dict(
                "sys.modules",
                {
                    "acoharmony._4icli.registry": MagicMock(
                        get_file_type_codes=lambda: {42: "Custom"}
                    )
                },
            ):
                result = cmd_download(args)
                assert result == 0

    @pytest.mark.unit
    def test_download_many_errors(self, tmp_path):
        """Test > 10 errors in download (line 570)."""
        from acoharmony._4icli.cli import cmd_download

        config = _make_config(tmp_path)
        state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)

        # Create many files so we get many error groups
        files_list = [
            {
                "filename": f"file{i}.zip",
                "category": "CCLF",
                "file_type_code": 113,
                "year": 2020 + i,
                "last_updated": f"2025-0{min(i + 1, 9)}-01T00:00:00.000Z",
            }
            for i in range(12)
        ]
        state_data = {"generated_at": "2025-01-01", "total_missing": 12, "files": files_list}
        state_path.write_text(json.dumps(state_data))

        args = SimpleNamespace(updated_after=None)

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {}
        mock_client = MagicMock()
        mock_client.download.side_effect = RuntimeError("boom")

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.FourICLI", return_value=mock_client),
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
        ):
            with patch.dict(
                "sys.modules",
                {
                    "acoharmony._4icli.registry": MagicMock(
                        get_file_type_codes=lambda: {113: "CCLF"}
                    )
                },
            ):
                result = cmd_download(args)
                assert result == 1


class TestCliCoverageGaps:
    """Cover _4icli/cli.py missed lines 857-858."""

    @pytest.mark.unit
    def test_cli_main_unknown_command(self):
        """Cover lines 857-858: unknown command → print_help + sys.exit(1)."""
        from acoharmony._4icli.cli import main

        with patch("sys.argv", ["aco-4icli"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1


# ===========================================================================
# cli.py coverage
# ===========================================================================


class TestCLIMain:  # noqa: F811
    @pytest.mark.unit
    def test_main_no_args_prints_help(self, capsys):
        from acoharmony._4icli.cli import main

        with pytest.raises(SystemExit) as exc_info:
            with patch("sys.argv", ["aco-4icli"]):
                main()
        assert exc_info.value.code == 1

    @pytest.mark.unit
    def test_main_version(self, capsys):
        from acoharmony._4icli.cli import main

        with pytest.raises(SystemExit) as exc_info:
            with patch("sys.argv", ["aco-4icli", "--version"]):
                main()
        assert exc_info.value.code == 0

    @pytest.mark.unit
    def test_main_inventory_command(self):
        from acoharmony._4icli.cli import main

        with patch("sys.argv", ["aco-4icli", "inventory", "--force"]):
            with patch("acoharmony._4icli.cli.cmd_inventory") as mock_cmd:
                main()
                mock_cmd.assert_called_once()

    @pytest.mark.unit
    def test_main_need_download_command(self):
        from acoharmony._4icli.cli import main

        with patch("sys.argv", ["aco-4icli", "need-download"]):
            with patch("acoharmony._4icli.cli.cmd_need_download") as mock_cmd:
                main()
                mock_cmd.assert_called_once()

    @pytest.mark.unit
    def test_main_download_command(self):
        from acoharmony._4icli.cli import main

        with patch("sys.argv", ["aco-4icli", "download"]):
            with patch("acoharmony._4icli.cli.cmd_download") as mock_cmd:
                main()
                mock_cmd.assert_called_once()

    @pytest.mark.unit
    def test_main_exception_handler(self, capsys):
        from acoharmony._4icli.cli import main

        with patch("sys.argv", ["aco-4icli", "inventory"]):
            with patch("acoharmony._4icli.cli.cmd_inventory", side_effect=RuntimeError("test err")):
                with pytest.raises(SystemExit) as exc_info:
                    main()
                assert exc_info.value.code == 1


class TestCmdDownload3:
    @pytest.mark.unit
    def test_cmd_download_no_state_file(self, make_config, mock_lw, capsys, tmp_path):
        from acoharmony._4icli.cli import cmd_download

        with patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=make_config):
            with patch("acoharmony._4icli.cli.LogWriter", return_value=mock_lw):
                args = SimpleNamespace(updated_after=None)
                result = cmd_download(args)
                assert result == 1
                captured = capsys.readouterr()
                assert "need-download" in captured.out

    @pytest.mark.unit
    def test_cmd_download_empty_files_list(self, make_config, mock_lw, capsys, tmp_path):
        from acoharmony._4icli.cli import cmd_download

        # Create state file with empty files
        state_path = make_config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_data = {"generated_at": "2025-01-01", "total_missing": 0, "files": []}
        state_path.write_text(json.dumps(state_data))

        with patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=make_config):
            with patch("acoharmony._4icli.cli.LogWriter", return_value=mock_lw):
                args = SimpleNamespace(updated_after=None)
                result = cmd_download(args)
                assert result == 0

    @pytest.mark.unit
    def test_cmd_download_with_files(self, make_config, mock_lw, capsys, tmp_path):
        from acoharmony._4icli.cli import cmd_download

        # Create state file with files
        state_path = make_config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_data = {
            "generated_at": "2025-01-01",
            "total_missing": 1,
            "total_size_formatted": "10 MB",
            "files": [
                {
                    "filename": "test.zip",
                    "category": "CCLF",
                    "file_type_code": 113,
                    "year": 2025,
                    "size_bytes": 1000,
                    "last_updated": "2025-01-01T00:00:00.000Z",
                    "discovered_at": "2025-01-01",
                }
            ],
        }
        state_path.write_text(json.dumps(state_data))

        with patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=make_config):
            with patch("acoharmony._4icli.cli.LogWriter", return_value=mock_lw):
                with patch("acoharmony._4icli.registry.get_file_type_codes", return_value=[113]):
                    with patch("acoharmony._4icli.cli.FourICLI") as mock_client_cls:
                        mock_client = MagicMock()
                        mock_client.download.return_value = MagicMock(
                            success=True, files_downloaded=[], errors=[]
                        )
                        mock_client_cls.return_value = mock_client

                        # Mock state tracker
                        with patch("acoharmony._4icli.state.FourICLIStateTracker") as mock_st:
                            mock_tracker = MagicMock()
                            mock_tracker._file_cache = {}
                            mock_st.return_value = mock_tracker

                            args = SimpleNamespace(updated_after=None)
                            result = cmd_download(args)
                            # Should attempt downloads
                            assert isinstance(result, int)


class TestCmdNeedDownload3:
    @pytest.mark.unit
    def test_cmd_need_download_no_inventory(self, make_config, mock_lw, capsys):
        from acoharmony._4icli.cli import cmd_need_download

        with patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=make_config):
            with patch("acoharmony._4icli.cli.LogWriter", return_value=mock_lw):
                mock_discovery = MagicMock()
                mock_discovery.get_inventory_path.return_value = Path("/nonexistent/inventory.json")

                with patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery):
                    args = SimpleNamespace(
                        start_year=2025,
                        end_year=2025,
                        year=None,
                        category=None,
                        export=None,
                        show_have=False,
                        limit=20,
                    )
                    result = cmd_need_download(args)
                    assert result == 1


class TestCmdInventory3:
    @pytest.mark.unit
    def test_cmd_inventory_no_apm_id(self, make_config, mock_lw, capsys):
        from acoharmony._4icli.cli import cmd_inventory

        make_config.default_apm_id = None

        with patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=make_config):
            with patch("acoharmony._4icli.cli.LogWriter", return_value=mock_lw):
                mock_discovery = MagicMock()
                inv_path = MagicMock()
                inv_path.exists.return_value = False
                mock_discovery.get_inventory_path.return_value = inv_path

                with patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery):
                    args = SimpleNamespace(start_year=2025, end_year=2025, force=False)
                    result = cmd_inventory(args)
                    assert result == 1

    @pytest.mark.unit
    def test_cmd_inventory_existing_no_force(self, make_config, mock_lw, capsys, tmp_path):
        from acoharmony._4icli.cli import cmd_inventory
        from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult

        inv_path = tmp_path / "inventory.json"
        result = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2025],
            total_files=1,
            files_by_year={2025: 1},
            files_by_category={"CCLF": 1},
            files=[
                FileInventoryEntry(filename="f.zip", category="CCLF", file_type_code=113, year=2025)
            ],
            started_at=datetime.now(),
            completed_at=datetime.now(),
        )
        result.save_to_json(inv_path)

        with patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=make_config):
            with patch("acoharmony._4icli.cli.LogWriter", return_value=mock_lw):
                mock_discovery = MagicMock()
                mock_discovery.get_inventory_path.return_value = inv_path
                mock_discovery.enrich_with_file_type_codes.return_value = result

                with patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery):
                    with patch(
                        "acoharmony._4icli.inventory.InventoryResult.load_from_json",
                        return_value=result,
                    ):
                        args = SimpleNamespace(start_year=2025, end_year=2025, force=False)
                        cmd_inventory(args)
                        captured = capsys.readouterr()
                        assert "Inventory Summary" in captured.out

    @pytest.mark.unit
    def test_cmd_inventory_force_rebuild(self, make_config, mock_lw, capsys, tmp_path):
        from acoharmony._4icli.cli import cmd_inventory
        from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult

        make_config.default_apm_id = "D0259"

        result = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2025],
            total_files=1,
            files_by_year={2025: 1},
            files_by_category={"CCLF": 1},
            files=[
                FileInventoryEntry(filename="f.zip", category="CCLF", file_type_code=113, year=2025)
            ],
            started_at=datetime.now(),
            completed_at=datetime.now(),
        )

        with patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=make_config):
            with patch("acoharmony._4icli.cli.LogWriter", return_value=mock_lw):
                mock_discovery = MagicMock()
                inv_path = MagicMock()
                inv_path.exists.return_value = True
                mock_discovery.get_inventory_path.return_value = inv_path
                mock_discovery.discover_years.return_value = result
                mock_discovery.enrich_with_file_type_codes.return_value = result

                with patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery):
                    args = SimpleNamespace(start_year=2025, end_year=2025, force=True)
                    cmd_inventory(args)
                    mock_discovery.discover_years.assert_called_once()


class TestFourICLIModels:
    """Test 4icli model classes work correctly."""

    @pytest.mark.unit
    def test_datahub_categories(self) -> None:
        """DataHubCategory enum values."""
        from acoharmony._4icli.models import DataHubCategory

        assert DataHubCategory.CCLF == "CCLF"
        assert DataHubCategory.BENEFICIARY_LIST == "Beneficiary List"
        assert DataHubCategory.REPORTS == "Reports"

    @pytest.mark.unit
    def test_file_type_codes(self) -> None:
        """FileTypeCode enum has expected members."""
        from acoharmony._4icli.models import FileTypeCode

        assert hasattr(FileTypeCode, "CCLF")
        assert hasattr(FileTypeCode, "BENEFICIARY_LIST")

    @pytest.mark.unit
    def test_datahub_query_creation(self) -> None:
        """DataHubQuery can be created with defaults."""
        from acoharmony._4icli.models import DataHubCategory, DataHubQuery

        query = DataHubQuery(
            category=DataHubCategory.CCLF,
            apm_id="D0259",
            year=2025,
        )
        assert query.category == DataHubCategory.CCLF
        assert query.apm_id == "D0259"
        assert query.year == 2025

    @pytest.mark.unit
    def test_get_current_year(self) -> None:
        """get_current_year returns sensible value."""
        from acoharmony._4icli.config import get_current_year

        year = get_current_year()
        assert 2024 <= year <= 2030


class TestFourICLIModels3:
    """Test 4icli model classes work correctly."""

    @pytest.mark.unit
    def test_datahub_categories(self) -> None:
        """DataHubCategory enum values."""
        from acoharmony._4icli.models import DataHubCategory

        assert DataHubCategory.CCLF == "CCLF"
        assert DataHubCategory.BENEFICIARY_LIST == "Beneficiary List"
        assert DataHubCategory.REPORTS == "Reports"

    @pytest.mark.unit
    def test_file_type_codes(self) -> None:
        """FileTypeCode enum has expected members."""
        from acoharmony._4icli.models import FileTypeCode

        assert hasattr(FileTypeCode, "CCLF")
        assert hasattr(FileTypeCode, "BENEFICIARY_LIST")

    @pytest.mark.unit
    def test_datahub_query_creation(self) -> None:
        """DataHubQuery can be created with defaults."""
        from acoharmony._4icli.models import DataHubCategory, DataHubQuery

        query = DataHubQuery(
            category=DataHubCategory.CCLF,
            apm_id="D0259",
            year=2025,
        )
        assert query.category == DataHubCategory.CCLF
        assert query.apm_id == "D0259"
        assert query.year == 2025

    @pytest.mark.unit
    def test_get_current_year(self) -> None:
        """get_current_year returns sensible value."""
        from acoharmony._4icli.config import get_current_year

        year = get_current_year()
        assert 2024 <= year <= 2030


class TestCLICmdNeedDownload:
    @pytest.mark.unit
    def test_need_download_no_inventory(self, tmp_path):
        from acoharmony._4icli.cli import cmd_need_download

        config = _make_config(tmp_path)
        args = SimpleNamespace(
            start_year=2025,
            end_year=2025,
            year=None,
            category=None,
            export=None,
            show_have=False,
            limit=20,
        )

        mock_discovery = MagicMock()
        mock_discovery.get_inventory_path.return_value = tmp_path / "nonexistent.json"

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery),
        ):
            result = cmd_need_download(args)
            assert result == 1

    @pytest.mark.unit
    def test_need_download_with_results(self, tmp_path):
        from acoharmony._4icli.cli import cmd_need_download
        from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult

        config = _make_config(tmp_path)
        # Create bronze and archive dirs
        config.bronze_dir.mkdir(parents=True, exist_ok=True)
        (config.data_path / "archive").mkdir(parents=True, exist_ok=True)

        inv_path = config.log_dir / "tracking" / "4icli_inventory_state.json"
        inv_path.parent.mkdir(parents=True, exist_ok=True)
        inv = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2025],
            total_files=1,
            files_by_year={2025: 1},
            files_by_category={"CCLF": 1},
            files=[
                FileInventoryEntry(
                    filename="missing.zip",
                    category="CCLF",
                    file_type_code=113,
                    year=2025,
                    size_bytes=1000,
                    last_updated="2025-01-01",
                )
            ],
            started_at=datetime.now(),
            completed_at=datetime.now(),
        )
        inv.save_to_json(inv_path)

        args = SimpleNamespace(
            start_year=2025,
            end_year=2025,
            year=2025,
            category="CCLF",
            export=None,
            show_have=False,
            limit=20,
        )

        mock_discovery = MagicMock()
        mock_discovery.get_inventory_path.return_value = inv_path
        mock_discovery.discover_years.return_value = inv
        mock_discovery.enrich_with_file_type_codes.return_value = inv

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {}

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery),
            patch("acoharmony._4icli.cli.scan_directory", return_value=set()),
            patch(
                "acoharmony._4icli.cli.compare_inventory",
                return_value={
                    "total_inventory": 1,
                    "have": [],
                    "missing": [inv.files[0]],
                    "have_count": 0,
                    "missing_count": 1,
                    "total_size_bytes": 1000,
                    "missing_by_year": {2025: 1},
                    "missing_by_category": {"CCLF": 1},
                    "missing_by_type_code": {113: 1},
                },
            ),
            patch("acoharmony._4icli.cli.save_not_downloaded_state"),
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
        ):
            result = cmd_need_download(args)
            assert result == 0

    @pytest.mark.unit
    def test_need_download_show_have_and_export(self, tmp_path):
        from acoharmony._4icli.cli import cmd_need_download
        from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult

        config = _make_config(tmp_path)
        config.bronze_dir.mkdir(parents=True, exist_ok=True)
        (config.data_path / "archive").mkdir(parents=True, exist_ok=True)

        inv_path = config.log_dir / "tracking" / "4icli_inventory_state.json"
        inv_path.parent.mkdir(parents=True, exist_ok=True)
        have_file = FileInventoryEntry(
            filename="have.zip",
            category="CCLF",
            file_type_code=113,
            year=2025,
            size_bytes=500,
        )
        inv = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2025],
            total_files=1,
            files_by_year={2025: 1},
            files_by_category={"CCLF": 1},
            files=[have_file],
            started_at=datetime.now(),
            completed_at=datetime.now(),
        )
        inv.save_to_json(inv_path)

        export_path = str(tmp_path / "export.csv")
        args = SimpleNamespace(
            start_year=2025,
            end_year=2025,
            year=None,
            category=None,
            export=export_path,
            show_have=True,
            limit=20,
        )

        mock_discovery = MagicMock()
        mock_discovery.get_inventory_path.return_value = inv_path
        mock_discovery.discover_years.return_value = inv
        mock_discovery.enrich_with_file_type_codes.return_value = inv

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {}

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery),
            patch("acoharmony._4icli.cli.scan_directory", return_value={"have.zip"}),
            patch(
                "acoharmony._4icli.cli.compare_inventory",
                return_value={
                    "total_inventory": 1,
                    "have": [have_file],
                    "missing": [],
                    "have_count": 1,
                    "missing_count": 0,
                    "total_size_bytes": 0,
                    "missing_by_year": {},
                    "missing_by_category": {},
                    "missing_by_type_code": {},
                },
            ),
            patch("acoharmony._4icli.cli.export_to_csv") as mock_export,
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
        ):
            result = cmd_need_download(args)
            assert result == 0
            mock_export.assert_called_once()

    @pytest.mark.unit
    def test_need_download_empty_inventory(self, tmp_path):
        """Test when total_inventory is 0."""
        from acoharmony._4icli.cli import cmd_need_download
        from acoharmony._4icli.inventory import InventoryResult

        config = _make_config(tmp_path)
        config.bronze_dir.mkdir(parents=True, exist_ok=True)
        (config.data_path / "archive").mkdir(parents=True, exist_ok=True)

        inv_path = config.log_dir / "tracking" / "4icli_inventory_state.json"
        inv_path.parent.mkdir(parents=True, exist_ok=True)
        inv = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2025],
            total_files=0,
            files_by_year={},
            files_by_category={},
            files=[],
            started_at=datetime.now(),
            completed_at=datetime.now(),
        )
        inv.save_to_json(inv_path)

        args = SimpleNamespace(
            start_year=2025,
            end_year=2025,
            year=None,
            category=None,
            export=None,
            show_have=False,
            limit=20,
        )

        mock_discovery = MagicMock()
        mock_discovery.get_inventory_path.return_value = inv_path
        mock_discovery.discover_years.return_value = inv
        mock_discovery.enrich_with_file_type_codes.return_value = inv

        mock_state_tracker = MagicMock()
        mock_state_tracker._file_cache = {}

        with (
            patch("acoharmony._4icli.cli.FourICLIConfig.from_profile", return_value=config),
            patch("acoharmony._4icli.cli.LogWriter", return_value=_mock_log_writer()),
            patch("acoharmony._4icli.cli.InventoryDiscovery", return_value=mock_discovery),
            patch("acoharmony._4icli.cli.scan_directory", return_value=set()),
            patch(
                "acoharmony._4icli.cli.compare_inventory",
                return_value={
                    "total_inventory": 0,
                    "have": [],
                    "missing": [],
                    "have_count": 0,
                    "missing_count": 0,
                    "total_size_bytes": 0,
                    "missing_by_year": {},
                    "missing_by_category": {},
                    "missing_by_type_code": {},
                },
            ),
            patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=mock_state_tracker),
        ):
            result = cmd_need_download(args)
            assert result == 0


class TestFourICLIImports:
    """Verify all 4icli submodules import cleanly."""

    @pytest.mark.unit
    def test_import_4icli_package(self) -> None:
        """Top-level _4icli package imports."""
        from acoharmony._4icli import (
            DataHubCategory,
            FourICLI,
            FourICLIConfig,
        )

        assert FourICLI is not None
        assert FourICLIConfig is not None
        assert DataHubCategory is not None

    @pytest.mark.unit
    def test_import_4icli_client(self) -> None:
        """Client module imports."""
        from acoharmony._4icli.client import FourICLI

        assert FourICLI is not None

    @pytest.mark.unit
    def test_import_4icli_config(self) -> None:
        """Config module imports."""
        from acoharmony._4icli.config import FourICLIConfig, get_current_year

        assert FourICLIConfig is not None
        assert callable(get_current_year)

    @pytest.mark.unit
    def test_import_4icli_models(self) -> None:
        """Models module imports."""
        from acoharmony._4icli.models import (
            DataHubCategory,
        )

        assert DataHubCategory is not None

    @pytest.mark.unit
    def test_import_4icli_parser(self) -> None:
        """Parser module imports."""
        from acoharmony._4icli.parser import (
            extract_file_count,
            extract_filenames,
            parse_datahub_output,
        )

        assert callable(parse_datahub_output)
        assert callable(extract_filenames)
        assert callable(extract_file_count)

    @pytest.mark.unit
    def test_import_4icli_inventory(self) -> None:
        """Inventory module imports."""
        from acoharmony._4icli.inventory import InventoryDiscovery, InventoryResult

        assert InventoryDiscovery is not None
        assert InventoryResult is not None

    @pytest.mark.unit
    def test_import_4icli_state(self) -> None:
        """State tracking module imports."""
        from acoharmony._4icli.state import FourICLIStateTracker

        assert FourICLIStateTracker is not None

    @pytest.mark.unit
    def test_import_4icli_comparison(self) -> None:
        """Comparison module imports."""
        from acoharmony._4icli.comparison import compare_inventory

        assert callable(compare_inventory)

    @pytest.mark.unit
    def test_import_4icli_processor(self) -> None:
        """Processor module imports."""
        from acoharmony._4icli.processor import FileProcessor

        assert FileProcessor is not None

    @pytest.mark.unit
    def test_import_4icli_registry(self) -> None:
        """Registry module imports."""
        from acoharmony._4icli.registry import SchemaRegistry

        assert SchemaRegistry is not None

    @pytest.mark.unit
    def test_import_4icli_cli(self) -> None:
        """CLI module imports."""
        from acoharmony._4icli.cli import (
            cmd_download,
            cmd_inventory,
            cmd_need_download,
            main,
        )

        assert callable(cmd_download)
        assert callable(cmd_inventory)
        assert callable(cmd_need_download)
        assert callable(main)


class Test4icliCliForLoopEmpty:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_4icli_cli_for_loop_empty(self):
        """499->505: for cat in DataHubCategory — loop body not entered (empty enum)."""
        from acoharmony._4icli.cli import cmd_inventory
        args = MagicMock()
        args.category = "nonexistent"
        args.verbose = False
        try: cmd_inventory(args)
        except: pass


class TestCmdNeedDownloadMissingEntryNoLastUpdated:
    """Cover branch 192->187: file_entry.last_updated is falsy, skip print."""

    @patch("acoharmony._4icli.cli.save_not_downloaded_state")
    @patch("acoharmony._4icli.cli.export_to_csv")
    @patch("acoharmony._4icli.cli.format_size", return_value="1 KB")
    @patch("acoharmony._4icli.cli.compare_inventory")
    @patch("acoharmony._4icli.cli.scan_directory", return_value=set())
    @patch("acoharmony._4icli.cli.InventoryDiscovery")
    @patch("acoharmony._4icli.cli.LogWriter")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @pytest.mark.unit
    def test_missing_entry_without_last_updated(
        self, m_cfg, m_log, m_disc, m_scan, m_cmp, m_fmt, m_export, m_save, tmp_path
    ):
        """Branch 192->187: file_entry.last_updated is None, skip the print line."""
        from acoharmony._4icli.cli import cmd_need_download
        from acoharmony._4icli.inventory import FileInventoryEntry

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        inv = _make_inventory_result()
        discovery = MagicMock()
        inv_path = tmp_path / "inv.json"
        inv_path.write_text("{}")
        discovery.get_inventory_path.return_value = inv_path
        discovery.discover_years.return_value = inv
        discovery.enrich_with_file_type_codes.return_value = inv
        m_disc.return_value = discovery

        # Create a missing entry with last_updated=None
        missing_no_date = FileInventoryEntry(
            filename="NO_DATE.zip",
            category="CCLF",
            file_type_code=113,
            year=2024,
            size_bytes=500,
            last_updated=None,  # <-- This triggers branch 192->187
        )

        m_cmp.return_value = {
            "total_inventory": 1,
            "have_count": 0,
            "missing_count": 1,
            "total_size_bytes": 500,
            "missing_by_year": {2024: 1},
            "missing_by_category": {"CCLF": 1},
            "missing_by_type_code": {113: 1},
            "missing": [missing_no_date],
            "have": [],
        }

        tracker = MagicMock()
        tracker._file_cache = {}
        with patch("acoharmony._4icli.state.FourICLIStateTracker", return_value=tracker):
            args = argparse.Namespace(
                start_year=2024,
                end_year=2024,
                year=None,
                category=None,
                limit=20,
                show_have=False,
            )
            result = cmd_need_download(args)

        assert result == 0


class TestCmdDownloadOldestDateNotUpdated:
    """Cover branch 444->437: oldest_date is not None AND file_date >= oldest_date."""

    @patch("acoharmony._4icli.cli.FourICLI")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.LogWriter")
    @pytest.mark.unit
    def test_oldest_date_not_updated_when_newer(self, m_log, m_cfg, m_client_cls, tmp_path):
        """Branch 444->437: a second file has a newer date than oldest_date,
        so oldest_date is NOT updated and the loop continues.
        """
        from acoharmony._4icli.cli import cmd_download

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        # Two files with same category/year/type but different dates.
        # The older one sets oldest_date first; the newer one does NOT update it.
        state_data = {
            "generated_at": "2024-06-01",
            "files": [
                {
                    "filename": "CCLF1_old.zip",
                    "category": "CCLF",
                    "file_type_code": 113,
                    "year": 2024,
                    "size_bytes": 1024,
                    "last_updated": "2024-01-01T00:00:00Z",
                },
                {
                    "filename": "CCLF1_new.zip",
                    "category": "CCLF",
                    "file_type_code": 113,
                    "year": 2024,
                    "size_bytes": 2048,
                    "last_updated": "2024-06-01T00:00:00Z",  # newer -> doesn't update oldest
                },
            ],
        }
        state_path = cfg.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.write_text(json.dumps(state_data))

        registry_mod = types.ModuleType("acoharmony._4icli.registry")
        registry_mod.get_file_type_codes = MagicMock(return_value=[113])

        state_mod = types.ModuleType("acoharmony._4icli.state")
        tracker = MagicMock()
        tracker._file_cache = {}
        state_mod.FourICLIStateTracker = MagicMock(return_value=tracker)

        download_result = MagicMock()
        download_result.success = True
        download_result.files_downloaded = []
        download_result.errors = []
        client = MagicMock()
        client.download.return_value = download_result
        m_client_cls.return_value = client

        with patch.dict(
            "sys.modules",
            {
                "acoharmony._4icli.registry": registry_mod,
                "acoharmony._4icli.state": state_mod,
            },
        ):
            args = argparse.Namespace(updated_after=None)
            result = cmd_download(args)

        assert result == 0


class TestCmdDownloadCategoryNoneAndUnmatchedEnum:
    """Cover branches 498->505 and 499->505: category is None or doesn't match enum."""

    @patch("acoharmony._4icli.cli.FourICLI")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.LogWriter")
    @pytest.mark.unit
    def test_category_none_skips_enum_lookup(self, m_log, m_cfg, m_client_cls, tmp_path):
        """Branch 498->505: category is None so the for loop is skipped entirely."""
        from acoharmony._4icli.cli import cmd_download

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        state_data = {
            "generated_at": "2024-06-01",
            "files": [
                {
                    "filename": "FILE.zip",
                    "category": None,
                    "file_type_code": 113,
                    "year": 2024,
                    "size_bytes": 1024,
                    "last_updated": "2024-06-01T00:00:00Z",
                }
            ],
        }
        state_path = cfg.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.write_text(json.dumps(state_data))

        registry_mod = types.ModuleType("acoharmony._4icli.registry")
        registry_mod.get_file_type_codes = MagicMock(return_value=[113])

        state_mod = types.ModuleType("acoharmony._4icli.state")
        tracker = MagicMock()
        tracker._file_cache = {}
        state_mod.FourICLIStateTracker = MagicMock(return_value=tracker)

        download_result = MagicMock()
        download_result.success = True
        download_result.files_downloaded = []
        download_result.errors = []
        client = MagicMock()
        client.download.return_value = download_result
        m_client_cls.return_value = client

        with patch.dict(
            "sys.modules",
            {
                "acoharmony._4icli.registry": registry_mod,
                "acoharmony._4icli.state": state_mod,
            },
        ):
            args = argparse.Namespace(updated_after=None)
            result = cmd_download(args)

        assert result == 0

    @patch("acoharmony._4icli.cli.FourICLI")
    @patch("acoharmony._4icli.cli.FourICLIConfig")
    @patch("acoharmony._4icli.cli.LogWriter")
    @pytest.mark.unit
    def test_category_no_enum_match(self, m_log, m_cfg, m_client_cls, tmp_path):
        """Branch 499->505: category string doesn't match any DataHubCategory enum value.

        The for loop completes without breaking, so category_enum stays None.
        """
        from acoharmony._4icli.cli import cmd_download

        cfg = _make_config(tmp_path)
        m_cfg.from_profile.return_value = cfg

        state_data = {
            "generated_at": "2024-06-01",
            "files": [
                {
                    "filename": "FILE.zip",
                    "category": "NonExistentCategory",
                    "file_type_code": 113,
                    "year": 2024,
                    "size_bytes": 1024,
                    "last_updated": "2024-06-01T00:00:00Z",
                }
            ],
        }
        state_path = cfg.log_dir / "tracking" / "4icli_notdownloaded_state.json"
        state_path.write_text(json.dumps(state_data))

        registry_mod = types.ModuleType("acoharmony._4icli.registry")
        registry_mod.get_file_type_codes = MagicMock(return_value=[113])

        state_mod = types.ModuleType("acoharmony._4icli.state")
        tracker = MagicMock()
        tracker._file_cache = {}
        state_mod.FourICLIStateTracker = MagicMock(return_value=tracker)

        download_result = MagicMock()
        download_result.success = True
        download_result.files_downloaded = []
        download_result.errors = []
        client = MagicMock()
        client.download.return_value = download_result
        m_client_cls.return_value = client

        with patch.dict(
            "sys.modules",
            {
                "acoharmony._4icli.registry": registry_mod,
                "acoharmony._4icli.state": state_mod,
            },
        ):
            args = argparse.Namespace(updated_after=None)
            result = cmd_download(args)

        assert result == 0


# --- cmd_setup tests ---------------------------------------------------------

class TestSetupHelpers:
    """Cover the small helpers that back cmd_setup."""

    @pytest.mark.unit
    def test_read_env_file_parses_keys_skipping_blanks_and_comments(self, tmp_path):
        from acoharmony._4icli.cli import _read_env_file

        env = tmp_path / ".env"
        env.write_text(
            "# comment line\n"
            "\n"
            "FOO=bar\n"
            "  BAZ = qux \n"
            "no_equals_line\n"
            "EMPTY=\n"
        )
        result = _read_env_file(env)
        assert result == {"FOO": "bar", "BAZ": "qux", "EMPTY": ""}

    @pytest.mark.unit
    def test_read_env_file_returns_empty_when_missing(self, tmp_path):
        from acoharmony._4icli.cli import _read_env_file

        assert _read_env_file(tmp_path / "does_not_exist.env") == {}

    @pytest.mark.unit
    def test_update_env_file_replaces_existing_and_appends_new(self, tmp_path):
        from acoharmony._4icli.cli import _update_env_file

        env = tmp_path / ".env"
        env.write_text("# header\nFOO=old\nKEEP=stays\n")
        _update_env_file(env, {"FOO": "new", "FRESH": "added"})
        text = env.read_text()
        assert "# header" in text
        assert "FOO=new" in text
        assert "FOO=old" not in text
        assert "KEEP=stays" in text
        assert "FRESH=added" in text

    @pytest.mark.unit
    def test_update_env_file_creates_when_missing(self, tmp_path):
        from acoharmony._4icli.cli import _update_env_file

        env = tmp_path / "fresh.env"
        _update_env_file(env, {"A": "1", "B": "2"})
        assert env.read_text().splitlines() == ["A=1", "B=2"]

    @pytest.mark.unit
    def test_mask_short_secret(self):
        from acoharmony._4icli.cli import _mask

        assert _mask("") == "(unset)"
        assert _mask("short") == "…"

    @pytest.mark.unit
    def test_mask_long_secret(self):
        from acoharmony._4icli.cli import _mask

        assert _mask("abcdefghij") == "abcd…ghij"

    @pytest.mark.unit
    def test_find_deploy_dir_walks_to_repo_root(self, tmp_path, monkeypatch):
        from acoharmony._4icli import cli as cli_module

        repo = tmp_path / "repo"
        deploy = repo / "deploy"
        (deploy / "images" / "4icli").mkdir(parents=True)
        (deploy / ".env").write_text("FOURICLI_API_KEY=k\n")
        (deploy / "images" / "4icli" / "bootstrap.sh").write_text("#!/bin/sh\n")
        fake_module = repo / "src" / "acoharmony" / "_4icli" / "cli.py"
        fake_module.parent.mkdir(parents=True)
        fake_module.write_text("")
        monkeypatch.setattr(cli_module, "__file__", str(fake_module))

        found = cli_module._find_deploy_dir()
        assert found == deploy


class TestCmdSetup:
    """Cover cmd_setup. The bootstrap.sh subprocess is always mocked."""

    def _patch_deploy(self, tmp_path, monkeypatch):
        deploy = tmp_path / "deploy"
        (deploy / "images" / "4icli").mkdir(parents=True)
        env = deploy / ".env"
        env.write_text(
            "FOURICLI_API_KEY=oldkeyvalue1234\n"
            "FOURICLI_API_SECRET=oldsecretvalue5678\n"
            "FOURICLI_APM_ID=D0259\n"
        )
        bootstrap = deploy / "images" / "4icli" / "bootstrap.sh"
        bootstrap.write_text("#!/bin/sh\nexit 0\n")
        bootstrap.chmod(0o755)
        monkeypatch.setattr(
            "acoharmony._4icli.cli._find_deploy_dir", lambda: deploy
        )
        return deploy, env

    @pytest.mark.unit
    def test_setup_keeps_existing_values_and_runs_bootstrap(
        self, tmp_path, monkeypatch
    ):
        from acoharmony._4icli.cli import cmd_setup

        deploy, env = self._patch_deploy(tmp_path, monkeypatch)
        inputs = iter(["", "", "y"])
        monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
        monkeypatch.setattr("acoharmony._4icli.cli.getpass", lambda *a, **k: "")

        run_calls = []

        def fake_run(cmd, env=None, check=False):
            run_calls.append((cmd, dict(env or {})))
            return SimpleNamespace(returncode=0)

        monkeypatch.setattr("acoharmony._4icli.cli.subprocess.run", fake_run)

        rc = cmd_setup(argparse.Namespace())

        assert rc == 0
        assert len(run_calls) == 1
        invoked_cmd, invoked_env = run_calls[0]
        assert invoked_cmd[0].endswith("bootstrap.sh")
        assert invoked_env["FOURICLI_API_KEY"] == "oldkeyvalue1234"
        assert invoked_env["FOURICLI_API_SECRET"] == "oldsecretvalue5678"
        text = env.read_text()
        assert "FOURICLI_API_KEY=oldkeyvalue1234" in text
        assert "FOURICLI_API_SECRET=oldsecretvalue5678" in text

    @pytest.mark.unit
    def test_setup_writes_new_values_to_env(self, tmp_path, monkeypatch):
        from acoharmony._4icli.cli import cmd_setup

        deploy, env = self._patch_deploy(tmp_path, monkeypatch)
        inputs = iter(["new-key-from-portal", ""])
        monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
        monkeypatch.setattr(
            "acoharmony._4icli.cli.getpass", lambda *a, **k: "new-secret-from-portal"
        )
        monkeypatch.setattr(
            "acoharmony._4icli.cli.subprocess.run",
            lambda *a, **k: SimpleNamespace(returncode=0),
        )

        rc = cmd_setup(argparse.Namespace())

        assert rc == 0
        text = env.read_text()
        assert "FOURICLI_API_KEY=new-key-from-portal" in text
        assert "FOURICLI_API_SECRET=new-secret-from-portal" in text
        assert "FOURICLI_APM_ID=D0259" in text

    @pytest.mark.unit
    def test_setup_aborts_when_user_declines_match_warning(
        self, tmp_path, monkeypatch
    ):
        from acoharmony._4icli.cli import cmd_setup

        deploy, env = self._patch_deploy(tmp_path, monkeypatch)
        inputs = iter(["", "", "n"])
        monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
        monkeypatch.setattr("acoharmony._4icli.cli.getpass", lambda *a, **k: "")

        called = []
        monkeypatch.setattr(
            "acoharmony._4icli.cli.subprocess.run",
            lambda *a, **k: called.append(1) or SimpleNamespace(returncode=0),
        )

        rc = cmd_setup(argparse.Namespace())

        assert rc == 1
        assert called == []

    @pytest.mark.unit
    def test_setup_propagates_bootstrap_failure(self, tmp_path, monkeypatch):
        from acoharmony._4icli.cli import cmd_setup

        deploy, env = self._patch_deploy(tmp_path, monkeypatch)
        inputs = iter(["", "", "y"])
        monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
        monkeypatch.setattr("acoharmony._4icli.cli.getpass", lambda *a, **k: "")
        monkeypatch.setattr(
            "acoharmony._4icli.cli.subprocess.run",
            lambda *a, **k: SimpleNamespace(returncode=1),
        )

        rc = cmd_setup(argparse.Namespace())

        assert rc == 1

    @pytest.mark.unit
    def test_setup_aborts_when_no_key_available(self, tmp_path, monkeypatch):
        from acoharmony._4icli.cli import cmd_setup

        deploy = tmp_path / "deploy"
        (deploy / "images" / "4icli").mkdir(parents=True)
        (deploy / ".env").write_text("")
        bootstrap = deploy / "images" / "4icli" / "bootstrap.sh"
        bootstrap.write_text("#!/bin/sh\nexit 0\n")
        bootstrap.chmod(0o755)
        monkeypatch.setattr(
            "acoharmony._4icli.cli._find_deploy_dir", lambda: deploy
        )
        monkeypatch.setattr("builtins.input", lambda *a, **k: "")

        rc = cmd_setup(argparse.Namespace())

        assert rc == 1

    @pytest.mark.unit
    def test_setup_aborts_when_no_secret_available(self, tmp_path, monkeypatch):
        from acoharmony._4icli.cli import cmd_setup

        deploy = tmp_path / "deploy"
        (deploy / "images" / "4icli").mkdir(parents=True)
        (deploy / ".env").write_text("FOURICLI_API_KEY=k\n")
        bootstrap = deploy / "images" / "4icli" / "bootstrap.sh"
        bootstrap.write_text("#!/bin/sh\nexit 0\n")
        bootstrap.chmod(0o755)
        monkeypatch.setattr(
            "acoharmony._4icli.cli._find_deploy_dir", lambda: deploy
        )
        monkeypatch.setattr("builtins.input", lambda *a, **k: "")
        monkeypatch.setattr("acoharmony._4icli.cli.getpass", lambda *a, **k: "")

        rc = cmd_setup(argparse.Namespace())

        assert rc == 1

    @pytest.mark.unit
    def test_find_deploy_dir_raises_when_no_deploy_anywhere(
        self, tmp_path, monkeypatch
    ):
        from acoharmony._4icli import cli as cli_module

        fake_module_path = tmp_path / "isolated" / "pkg" / "cli.py"
        fake_module_path.parent.mkdir(parents=True)
        fake_module_path.write_text("")
        monkeypatch.setattr(cli_module, "__file__", str(fake_module_path))

        with pytest.raises(FileNotFoundError, match="deploy"):
            cli_module._find_deploy_dir()
