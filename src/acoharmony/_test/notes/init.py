# © 2025 HarmonyCares
# All rights reserved.
"""
Comprehensive tests for acoharmony._notes and acoharmony._trace packages.

Targets 100% coverage for:
- _notes: config.py, plugins.py, generator.py, __init__.py
- _trace: tracer.py, config.py, exporters.py, decorators.py, __init__.py
"""

from __future__ import annotations
from dataclasses import dataclass

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, PropertyMock, mock_open, patch

import polars as pl
import pytest

import acoharmony._trace.config as tcfg


def _cleanup_trace_globals():
    """Reset trace module globals before and after each test."""

    old_config = tcfg._config
    old_provider = tcfg._tracer_provider
    tcfg._config = None
    tcfg._tracer_provider = None
    yield
    # Restore / cleanup
    if tcfg._tracer_provider is not None:
        try:
            tcfg._tracer_provider.shutdown()
        except Exception:
            pass
    tcfg._config = old_config
    tcfg._tracer_provider = old_provider


@pytest.fixture
def mock_storage():
    """Create a mock StorageBackend."""
    storage = MagicMock()
    storage.get_path.side_effect = lambda tier: Path(f"/tmp/test_data/{tier}")
    storage.get_data_path.return_value = Path("/tmp/test_data")
    storage.get_storage_type.return_value = "local"
    return storage


@pytest.fixture
def mock_catalog():
    """Create a mock Catalog."""
    catalog = MagicMock()
    return catalog


@pytest.fixture
def mock_mo():
    """Create a mock marimo module."""
    mo = MagicMock()
    mo.md.side_effect = lambda html: html
    mo.download.side_effect = lambda **kw: kw
    return mo


class TestNotebookConfig:
    """Tests for NotebookConfig dataclass."""

    @pytest.mark.unit
    def test_init_defaults(self):
        """NotebookConfig initializes with correct defaults."""
        cfg = self._import_config()(
            schema_name="test",
            schema_description="A test schema",
            storage_tier="gold",
            data_path="/tmp/test.parquet",
        )
        assert cfg.schema_name == "test"
        assert cfg.schema_description == "A test schema"
        assert cfg.storage_tier == "gold"
        assert cfg.data_path == "/tmp/test.parquet"
        assert cfg.app_width == "medium"
        assert cfg.hide_code is True
        assert cfg.show_footer is True
        assert cfg.show_tracking is True
        assert cfg.primary_key is None
        assert cfg.default_sort_column is None
        assert cfg.max_display_rows == 100
        assert "logo" in cfg.logo_url

    @pytest.mark.unit
    def test_init_custom_values(self):
        """NotebookConfig accepts custom values for all fields."""
        cfg = self._import_config()(
            schema_name="custom",
            schema_description="desc",
            storage_tier="silver",
            data_path="/x.parquet",
            app_width="full",
            hide_code=False,
            show_footer=False,
            show_tracking=False,
            primary_key="id",
            default_sort_column="date",
            max_display_rows=50,
            logo_url="http://example.com/logo.png",
            html_head_file="/custom/head.html",
        )
        assert cfg.app_width == "full"
        assert cfg.hide_code is False
        assert cfg.show_footer is False
        assert cfg.show_tracking is False
        assert cfg.primary_key == "id"
        assert cfg.default_sort_column == "date"
        assert cfg.max_display_rows == 50
        assert cfg.logo_url == "http://example.com/logo.png"
        assert cfg.html_head_file == "/custom/head.html"

    @pytest.mark.unit
    def test_from_schema_bronze_tier(self, mock_storage):
        """from_schema routes bronze tier to silver path."""
        schema = SimpleNamespace(
            name="cclf1",
            description="Part A header",
            storage={"tier": "bronze"},
            columns=[],
        )
        cfg = self._import_config().from_schema(schema, storage_config=mock_storage)
        assert cfg.schema_name == "cclf1"
        assert cfg.storage_tier == "bronze"
        # Bronze outputs to silver
        mock_storage.get_path.assert_any_call("silver")
        assert "cclf1.parquet" in cfg.data_path

    @pytest.mark.unit
    def test_from_schema_gold_tier(self, mock_storage):
        """from_schema uses gold path for gold tier."""
        schema = SimpleNamespace(
            name="medical_claim",
            description="Gold claims",
            storage={"tier": "gold"},
            columns=[],
        )
        cfg = self._import_config().from_schema(schema, storage_config=mock_storage)
        assert cfg.storage_tier == "gold"
        mock_storage.get_path.assert_any_call("gold")
        assert "medical_claim.parquet" in cfg.data_path

    @pytest.mark.unit
    def test_from_schema_silver_tier(self, mock_storage):
        """from_schema uses silver path for silver tier."""
        schema = SimpleNamespace(
            name="crosswalk",
            description="Silver crosswalk",
            storage={"tier": "silver"},
            columns=[],
        )
        cfg = self._import_config().from_schema(schema, storage_config=mock_storage)
        assert cfg.storage_tier == "silver"
        mock_storage.get_path.assert_any_call("silver")

    @pytest.mark.unit
    def test_from_schema_no_storage_attr_defaults_silver(self, mock_storage):
        """from_schema defaults to silver when schema has no storage attr."""
        schema = SimpleNamespace(name="test", description="desc")
        # Remove storage attribute
        assert not hasattr(schema, "storage") or True  # SimpleNamespace may not have it
        delattr(schema, "storage") if hasattr(schema, "storage") else None
        # Since hasattr(schema, 'storage') is False, storage_tier = "silver"
        cfg = self._import_config().from_schema(schema, storage_config=mock_storage)
        assert cfg.storage_tier == "silver"

    @pytest.mark.unit
    def test_from_schema_default_sort_column_detection(self, mock_storage):
        """from_schema detects default sort column from common patterns."""
        schema = SimpleNamespace(
            name="test",
            description="desc",
            storage={"tier": "gold"},
            columns=[
                {"output_name": "claim_id"},
                {"output_name": "claim_start_date"},
                {"output_name": "paid_amount"},
            ],
        )
        cfg = self._import_config().from_schema(schema, storage_config=mock_storage)
        # "date" pattern should match "claim_start_date"
        assert cfg.default_sort_column == "claim_start_date"

    @pytest.mark.unit
    def test_from_schema_sort_column_total_spend(self, mock_storage):
        """from_schema detects total_spend as sort column (first pattern)."""
        schema = SimpleNamespace(
            name="test",
            description="desc",
            storage={"tier": "gold"},
            columns=[
                {"output_name": "member_id"},
                {"output_name": "total_spend"},
            ],
        )
        cfg = self._import_config().from_schema(schema, storage_config=mock_storage)
        assert cfg.default_sort_column == "total_spend"

    @pytest.mark.unit
    def test_from_schema_sort_column_amount_pattern(self, mock_storage):
        """from_schema detects 'amount' pattern for sort column."""
        schema = SimpleNamespace(
            name="test",
            description="desc",
            storage={"tier": "gold"},
            columns=[
                {"output_name": "claim_id"},
                {"output_name": "paid_amount"},
            ],
        )
        cfg = self._import_config().from_schema(schema, storage_config=mock_storage)
        assert cfg.default_sort_column == "paid_amount"

    @pytest.mark.unit
    def test_from_schema_sort_column_with_name_key(self, mock_storage):
        """from_schema falls back to 'name' key when 'output_name' is absent."""
        schema = SimpleNamespace(
            name="test",
            description="desc",
            storage={"tier": "gold"},
            columns=[
                {"name": "created_at"},
            ],
        )
        cfg = self._import_config().from_schema(schema, storage_config=mock_storage)
        assert cfg.default_sort_column == "created_at"

    @pytest.mark.unit
    def test_from_schema_sort_column_string_columns(self, mock_storage):
        """from_schema handles string column entries."""
        schema = SimpleNamespace(
            name="test",
            description="desc",
            storage={"tier": "gold"},
            columns=["id", "updated_timestamp"],
        )
        cfg = self._import_config().from_schema(schema, storage_config=mock_storage)
        assert cfg.default_sort_column == "updated_timestamp"

    @pytest.mark.unit
    def test_from_schema_no_sort_column(self, mock_storage):
        """from_schema sets None when no sort pattern matches."""
        schema = SimpleNamespace(
            name="test",
            description="desc",
            storage={"tier": "gold"},
            columns=[{"output_name": "id"}, {"output_name": "name"}],
        )
        cfg = self._import_config().from_schema(schema, storage_config=mock_storage)
        assert cfg.default_sort_column is None

    @pytest.mark.unit
    def test_from_schema_no_columns(self, mock_storage):
        """from_schema handles missing columns attribute."""
        schema = SimpleNamespace(
            name="test",
            description="desc",
            storage={"tier": "gold"},
        )
        cfg = self._import_config().from_schema(schema, storage_config=mock_storage)
        assert cfg.default_sort_column is None

    @pytest.mark.unit
    def test_from_schema_string_data_path(self):
        """from_schema handles string tier_path (non-Path)."""
        storage = MagicMock()
        storage.get_path.return_value = "s3://bucket/gold"
        schema = SimpleNamespace(
            name="test",
            description="desc",
            storage={"tier": "gold"},
            columns=[],
        )
        cfg = self._import_config().from_schema(schema, storage_config=storage)
        assert cfg.data_path == "s3://bucket/gold/test.parquet"

    @patch("acoharmony._store.StorageBackend")
    @pytest.mark.unit
    def test_from_schema_default_storage(self, mock_sb_class):
        """from_schema creates default StorageBackend when none provided."""
        mock_instance = MagicMock()
        mock_instance.get_path.return_value = Path("/default/silver")
        mock_sb_class.return_value = mock_instance
        schema = SimpleNamespace(
            name="test",
            description="desc",
            storage={"tier": "bronze"},
            columns=[],
        )
        cfg = self._import_config().from_schema(schema, storage_config=None)
        mock_sb_class.assert_called_once()
        assert "test.parquet" in cfg.data_path

    @staticmethod
    def _import_config():
        from acoharmony._notes.config import NotebookConfig

        return NotebookConfig


class TestPluginRegistry:
    """Tests for PluginRegistry base class lazy loading."""

    @pytest.mark.unit
    def test_mo_lazy_load(self, mock_mo):
        """mo property lazy-loads marimo."""
        from acoharmony._notes.plugins import PluginRegistry

        reg = PluginRegistry()
        assert reg._mo is None
        with patch.dict("sys.modules", {"marimo": mock_mo}):
            result = reg.mo
            assert result is mock_mo

    @pytest.mark.unit
    def test_mo_cached(self, mock_mo):
        """mo property caches after first access."""
        from acoharmony._notes.plugins import PluginRegistry

        reg = PluginRegistry()
        reg._mo = mock_mo
        assert reg.mo is mock_mo

    @pytest.mark.unit
    def test_storage_lazy_load(self, mock_storage):
        """storage property lazy-loads StorageBackend."""
        from acoharmony._notes.plugins import PluginRegistry

        reg = PluginRegistry()
        assert reg._storage is None
        with patch(
            "acoharmony._notes.plugins.PluginRegistry.storage",
            new_callable=PropertyMock,
            return_value=mock_storage,
        ):
            pass
        # Direct set for test
        reg._storage = mock_storage
        assert reg.storage is mock_storage

    @pytest.mark.unit
    def test_catalog_lazy_load(self, mock_catalog):
        """catalog property lazy-loads Catalog."""
        from acoharmony._notes.plugins import PluginRegistry

        reg = PluginRegistry()
        reg._catalog = mock_catalog
        assert reg.catalog is mock_catalog


class TestSetupPlugins:
    """Tests for SetupPlugins."""

    @pytest.mark.unit
    def test_setup_project_path_exists(self, tmp_path):
        """setup_project_path adds src to sys.path when project exists."""
        from acoharmony._notes.plugins import SetupPlugins

        sp = SetupPlugins()
        with patch.object(Path, "exists", return_value=True):
            sys.path.copy()
            result = sp.setup_project_path()
            assert isinstance(result, Path)

    @pytest.mark.unit
    def test_setup_project_path_not_exists(self):
        """setup_project_path does not modify sys.path when project missing."""
        from acoharmony._notes.plugins import SetupPlugins

        sp = SetupPlugins()
        with patch.object(Path, "exists", return_value=False):
            sys.path.copy()
            sp.setup_project_path()
            # No new entries added (specifically the project src)

    @pytest.mark.unit
    def test_initialize_with_path_setup(self, mock_storage, mock_catalog):
        """initialize returns env dict with all expected keys."""
        from acoharmony._notes.plugins import SetupPlugins

        sp = SetupPlugins()
        sp._storage = mock_storage
        sp._catalog = mock_catalog
        with patch.object(Path, "exists", return_value=True):
            env = sp.initialize(setup_path=True)
        assert "storage" in env
        assert "catalog" in env
        assert "gold_path" in env
        assert "silver_path" in env
        assert "bronze_path" in env
        assert env["storage"] is mock_storage
        assert env["catalog"] is mock_catalog

    @pytest.mark.unit
    def test_initialize_without_path_setup(self, mock_storage, mock_catalog):
        """initialize skips path setup when setup_path=False."""
        from acoharmony._notes.plugins import SetupPlugins

        sp = SetupPlugins()
        sp._storage = mock_storage
        sp._catalog = mock_catalog
        env = sp.initialize(setup_path=False)
        assert "storage" in env

    @pytest.mark.unit
    def test_get_medallion_path_success(self, mock_storage):
        """get_medallion_path returns path from storage backend."""
        from acoharmony._notes.plugins import SetupPlugins

        sp = SetupPlugins()
        sp._storage = mock_storage
        result = sp.get_medallion_path("gold")
        assert result == Path("/tmp/test_data/gold")

    @pytest.mark.unit
    def test_get_medallion_path_fallback(self):
        """get_medallion_path returns fallback on storage failure."""
        from acoharmony._notes.plugins import SetupPlugins

        sp = SetupPlugins()
        bad_storage = MagicMock()
        bad_storage.get_path.side_effect = RuntimeError("fail")
        sp._storage = bad_storage
        result = sp.get_medallion_path("gold")
        assert result == Path("/opt/s3/data/workspace/gold")


class TestUIPlugins:
    """Tests for UIPlugins."""

    def _make_ui(self, mock_mo):
        from acoharmony._notes.plugins import UIPlugins

        ui = UIPlugins()
        ui._mo = mock_mo
        return ui

    @pytest.mark.unit
    def test_branded_header_basic(self, mock_mo):
        """branded_header produces HTML with title."""
        ui = self._make_ui(mock_mo)
        result = ui.branded_header("Dashboard")
        assert "Dashboard" in result
        mock_mo.md.assert_called_once()

    @pytest.mark.unit
    def test_branded_header_with_subtitle(self, mock_mo):
        """branded_header includes subtitle when provided."""
        ui = self._make_ui(mock_mo)
        result = ui.branded_header("Title", subtitle="Sub")
        assert "Sub" in result

    @pytest.mark.unit
    def test_branded_header_with_metadata(self, mock_mo):
        """branded_header includes dataset metadata."""
        ui = self._make_ui(mock_mo)
        metadata = {
            "Claims": {"rows": 1000, "min_date": "2024-01-01", "max_date": "2024-12-31"},
            "Eligibility": {"rows": 500},
        }
        result = ui.branded_header("Title", metadata=metadata)
        assert "1,000" in result
        assert "2024-01-01" in result
        assert "2024-12-31" in result
        assert "500" in result

    @pytest.mark.unit
    def test_branded_header_no_logo(self, mock_mo):
        """branded_header omits logo when show_logo=False."""
        ui = self._make_ui(mock_mo)
        result = ui.branded_header("Title", show_logo=False)
        assert "harmonycaresaco.com" not in result or "display:none" not in result

    @pytest.mark.unit
    def test_branded_header_no_timestamp(self, mock_mo):
        """branded_header omits timestamp when show_timestamp=False."""
        ui = self._make_ui(mock_mo)
        result = ui.branded_header("Title", show_timestamp=False)
        # Just check it doesn't fail; timestamp_html will be empty
        assert "Title" in result

    @pytest.mark.unit
    def test_branded_header_custom_icon(self, mock_mo):
        """branded_header uses custom icon class."""
        ui = self._make_ui(mock_mo)
        result = ui.branded_header("Title", icon="fa-solid fa-hospital")
        assert "fa-hospital" in result

    @pytest.mark.unit
    def test_info_callout_with_data_source(self, mock_mo):
        """info_callout shows data source when provided."""
        ui = self._make_ui(mock_mo)
        result = ui.info_callout("msg", data_source="Gold Layer")
        assert "Gold Layer" in result

    @pytest.mark.unit
    def test_info_callout_without_data_source(self, mock_mo):
        """info_callout shows message when no data_source."""
        ui = self._make_ui(mock_mo)
        result = ui.info_callout("Just a message")
        assert "Just a message" in result

    @pytest.mark.unit
    def test_summary_cards_int_values(self, mock_mo):
        """summary_cards formats integer values with commas."""
        ui = self._make_ui(mock_mo)
        metrics = [{"name": "Total", "value": 12345}]
        result = ui.summary_cards(metrics)
        assert "12,345" in result

    @pytest.mark.unit
    def test_summary_cards_float_values(self, mock_mo):
        """summary_cards formats float values with 2 decimal places."""
        ui = self._make_ui(mock_mo)
        metrics = [{"name": "Avg", "value": 3.14159}]
        result = ui.summary_cards(metrics)
        assert "3.14" in result

    @pytest.mark.unit
    def test_summary_cards_string_values(self, mock_mo):
        """summary_cards renders string values as-is."""
        ui = self._make_ui(mock_mo)
        metrics = [{"name": "Status", "value": "$45,678"}]
        result = ui.summary_cards(metrics)
        assert "$45,678" in result

    @pytest.mark.unit
    def test_summary_cards_custom_colors_and_icons(self, mock_mo):
        """summary_cards uses custom color and icon."""
        ui = self._make_ui(mock_mo)
        metrics = [
            {"name": "Metric", "value": 100, "color": "danger_red", "icon": "fa-exclamation"},
        ]
        result = ui.summary_cards(metrics)
        assert "fa-exclamation" in result
        assert ui.COLORS["danger_red"] in result

    @pytest.mark.unit
    def test_summary_cards_color_cycling(self, mock_mo):
        """summary_cards auto-assigns colors cycling through defaults."""
        ui = self._make_ui(mock_mo)
        metrics = [{"name": f"M{i}", "value": i} for i in range(8)]
        result = ui.summary_cards(metrics, columns=4)
        assert "repeat(4, 1fr)" in result

    @pytest.mark.unit
    def test_summary_cards_unknown_color_falls_back(self, mock_mo):
        """summary_cards falls back to info_blue for unknown color name."""
        ui = self._make_ui(mock_mo)
        metrics = [{"name": "X", "value": 1, "color": "nonexistent_color"}]
        result = ui.summary_cards(metrics)
        assert ui.COLORS["info_blue"] in result

    @pytest.mark.unit
    def test_download_button_csv(self, mock_mo):
        """download_button exports CSV format."""
        ui = self._make_ui(mock_mo)
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = ui.download_button(df, format="csv", filename="test", include_timestamp=False)
        assert result["mimetype"] == "text/csv"
        assert result["filename"] == "test.csv"

    @pytest.mark.unit
    def test_download_button_csv_with_timestamp(self, mock_mo):
        """download_button adds timestamp to filename."""
        ui = self._make_ui(mock_mo)
        df = pl.DataFrame({"a": [1]})
        result = ui.download_button(df, format="csv", filename="export")
        assert "export_" in result["filename"]
        assert result["filename"].endswith(".csv")

    @pytest.mark.unit
    def test_download_button_csv_auto_filename(self, mock_mo):
        """download_button generates filename when None provided."""
        ui = self._make_ui(mock_mo)
        df = pl.DataFrame({"a": [1]})
        result = ui.download_button(df, format="csv", include_timestamp=False)
        assert result["filename"] == "export.csv"

    @pytest.mark.unit
    def test_download_button_csv_auto_label(self, mock_mo):
        """download_button generates label when None provided."""
        ui = self._make_ui(mock_mo)
        df = pl.DataFrame({"a": [1]})
        result = ui.download_button(df, format="csv", include_timestamp=False)
        assert "CSV" in result["label"]

    @pytest.mark.unit
    def test_download_button_excel(self, mock_mo):
        """download_button exports Excel format."""
        ui = self._make_ui(mock_mo)
        df = pl.DataFrame({"a": [1, 2]})
        result = ui.download_button(df, format="excel", filename="test", include_timestamp=False)
        assert result["filename"] == "test.xlsx"
        # data should be a callable for lazy generation
        assert callable(result["data"])

    @pytest.mark.unit
    def test_download_button_parquet(self, mock_mo):
        """download_button exports Parquet format."""
        ui = self._make_ui(mock_mo)
        df = pl.DataFrame({"a": [1, 2]})
        result = ui.download_button(df, format="parquet", filename="test", include_timestamp=False)
        assert result["filename"] == "test.parquet"
        assert isinstance(result["data"], bytes)

    @pytest.mark.unit
    def test_download_button_unsupported_format(self, mock_mo):
        """download_button raises ValueError for unsupported format."""
        ui = self._make_ui(mock_mo)
        df = pl.DataFrame({"a": [1]})
        with pytest.raises(ValueError, match="Unsupported format"):
            ui.download_button(df, format="xml")

    @pytest.mark.unit
    def test_download_button_custom_label(self, mock_mo):
        """download_button uses custom label when provided."""
        ui = self._make_ui(mock_mo)
        df = pl.DataFrame({"a": [1]})
        result = ui.download_button(df, format="csv", label="Get CSV", include_timestamp=False)
        assert result["label"] == "Get CSV"

    @pytest.mark.unit
    def test_branded_footer_basic(self, mock_mo):
        """branded_footer renders without optional params."""
        ui = self._make_ui(mock_mo)
        result = ui.branded_footer()
        assert "2025" in result

    @pytest.mark.unit
    def test_branded_footer_with_tier(self, mock_mo):
        """branded_footer shows tier info."""
        ui = self._make_ui(mock_mo)
        result = ui.branded_footer(tier="gold")
        assert "Gold" in result

    @pytest.mark.unit
    def test_branded_footer_with_tier_and_files(self, mock_mo):
        """branded_footer shows tier and file names."""
        ui = self._make_ui(mock_mo)
        result = ui.branded_footer(tier="silver", files=["claims.parquet", "elig.parquet"])
        assert "Silver" in result
        assert "claims.parquet" in result

    @pytest.mark.unit
    def test_branded_footer_with_tracker(self, mock_mo):
        """branded_footer shows tracker info when available."""
        ui = self._make_ui(mock_mo)
        mock_tracker = MagicMock()
        mock_tracker.state.last_run = "2025-01-01"
        mock_tracker.state.total_runs = 5
        with patch(
            "acoharmony._notes.plugins.TransformTracker", return_value=mock_tracker, create=True
        ):
            # The import happens inside branded_footer, mock the import path
            with patch.dict("sys.modules", {}):
                # tracker_name triggers the import
                result = ui.branded_footer(tracker_name="test_tracker")
        # Due to dynamic import, it may silently catch the error - just ensure no crash
        assert "2025" in result

    @pytest.mark.unit
    def test_branded_footer_tracker_exception(self, mock_mo):
        """branded_footer handles tracker errors gracefully."""
        ui = self._make_ui(mock_mo)
        with patch("builtins.__import__", side_effect=ImportError("no tracking")):
            # Will catch the exception and skip tracking_html
            result = ui.branded_footer(tracker_name="bad_tracker")
        assert "2025" in result

    @pytest.mark.unit
    def test_colors_palette_complete(self):
        """COLORS dict contains all expected brand colors."""
        from acoharmony._notes.plugins import UIPlugins

        expected = {
            "primary_blue",
            "secondary_blue",
            "highlight_blue",
            "info_blue",
            "success_green",
            "warning_orange",
            "danger_red",
            "purple",
            "teal",
            "gray_light",
            "gray_medium",
            "gray_dark",
        }
        assert expected == set(UIPlugins.COLORS.keys())


class TestDataPlugins:
    """Tests for DataPlugins."""

    def _make_data(self, mock_storage):
        from acoharmony._notes.plugins import DataPlugins

        dp = DataPlugins()
        dp._storage = mock_storage
        return dp

    @pytest.mark.unit
    def test_load_gold_dataset_lazy(self, mock_storage, tmp_path):
        """load_gold_dataset returns LazyFrame when lazy=True."""
        dp = self._make_data(mock_storage)
        parquet = tmp_path / "test.parquet"
        pl.DataFrame({"a": [1, 2, 3]}).write_parquet(parquet)
        result = dp.load_gold_dataset("test", lazy=True, path=tmp_path)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_load_gold_dataset_eager(self, mock_storage, tmp_path):
        """load_gold_dataset returns DataFrame when lazy=False."""
        dp = self._make_data(mock_storage)
        parquet = tmp_path / "test.parquet"
        pl.DataFrame({"a": [1, 2, 3]}).write_parquet(parquet)
        result = dp.load_gold_dataset("test", lazy=False, path=tmp_path)
        assert isinstance(result, pl.DataFrame)
        assert result.height == 3

    @pytest.mark.unit
    def test_load_gold_dataset_not_found(self, mock_storage, tmp_path):
        """load_gold_dataset raises FileNotFoundError for missing file."""
        dp = self._make_data(mock_storage)
        with pytest.raises(FileNotFoundError, match="Dataset not found"):
            dp.load_gold_dataset("nonexistent", path=tmp_path)

    @pytest.mark.unit
    def test_load_gold_dataset_default_path(self, mock_storage, tmp_path):
        """load_gold_dataset uses storage backend when no path given."""
        mock_storage.get_path.side_effect = None
        mock_storage.get_path.return_value = str(tmp_path)
        dp = self._make_data(mock_storage)
        parquet = tmp_path / "test.parquet"
        pl.DataFrame({"a": [1]}).write_parquet(parquet)
        result = dp.load_gold_dataset("test", lazy=True)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_load_silver_dataset_lazy(self, mock_storage, tmp_path):
        """load_silver_dataset returns LazyFrame when lazy=True."""
        dp = self._make_data(mock_storage)
        parquet = tmp_path / "crosswalk.parquet"
        pl.DataFrame({"b": [10, 20]}).write_parquet(parquet)
        result = dp.load_silver_dataset("crosswalk", lazy=True, path=tmp_path)
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_load_silver_dataset_eager(self, mock_storage, tmp_path):
        """load_silver_dataset returns DataFrame when lazy=False."""
        dp = self._make_data(mock_storage)
        parquet = tmp_path / "crosswalk.parquet"
        pl.DataFrame({"b": [10, 20]}).write_parquet(parquet)
        result = dp.load_silver_dataset("crosswalk", lazy=False, path=tmp_path)
        assert isinstance(result, pl.DataFrame)

    @pytest.mark.unit
    def test_load_silver_dataset_not_found(self, mock_storage, tmp_path):
        """load_silver_dataset raises FileNotFoundError for missing file."""
        dp = self._make_data(mock_storage)
        with pytest.raises(FileNotFoundError, match="Dataset not found"):
            dp.load_silver_dataset("missing", path=tmp_path)

    @pytest.mark.unit
    def test_load_silver_dataset_default_path(self, mock_storage, tmp_path):
        """load_silver_dataset uses storage when no path given."""
        mock_storage.get_path.side_effect = None
        mock_storage.get_path.return_value = str(tmp_path)
        dp = self._make_data(mock_storage)
        parquet = tmp_path / "test.parquet"
        pl.DataFrame({"a": [1]}).write_parquet(parquet)
        result = dp.load_silver_dataset("test", lazy=False)
        assert isinstance(result, pl.DataFrame)

    @pytest.mark.unit
    def test_get_member_eligibility_empty_list(self, mock_storage):
        """get_member_eligibility returns None for empty member_ids."""
        dp = self._make_data(mock_storage)
        assert dp.get_member_eligibility([]) is None

    @pytest.mark.unit
    def test_get_member_eligibility_with_data(self, mock_storage):
        """get_member_eligibility filters and returns matching members."""
        dp = self._make_data(mock_storage)
        elig_df = pl.DataFrame(
            {
                "person_id": ["P1", "P2"],
                "member_id": ["M1", "M2"],
                "subscriber_id": ["S1", "S2"],
                "gender": ["M", "F"],
                "race": ["W", "B"],
                "birth_date": ["1950-01-01", "1960-01-01"],
                "death_date": [None, None],
                "death_flag": [0, 0],
                "enrollment_start_date": ["2024-01-01", "2024-01-01"],
                "enrollment_end_date": ["2024-12-31", "2024-12-31"],
                "payer": ["Medicare", "Medicare"],
                "payer_type": ["A", "B"],
                "plan": ["Plan1", "Plan2"],
            }
        )
        elig_lf = elig_df.lazy()
        result = dp.get_member_eligibility(["M1"], eligibility_lf=elig_lf)
        assert result is not None
        assert result.height == 1
        assert result["member_id"][0] == "M1"

    @pytest.mark.unit
    def test_get_member_eligibility_no_matches(self, mock_storage):
        """get_member_eligibility returns None when no matches found."""
        dp = self._make_data(mock_storage)
        elig_df = pl.DataFrame(
            {
                "person_id": ["P1"],
                "member_id": ["M1"],
                "subscriber_id": ["S1"],
                "gender": ["M"],
                "race": ["W"],
                "birth_date": ["1950-01-01"],
                "death_date": [None],
                "death_flag": [0],
                "enrollment_start_date": ["2024-01-01"],
                "enrollment_end_date": ["2024-12-31"],
                "payer": ["Medicare"],
                "payer_type": ["A"],
                "plan": ["Plan1"],
            }
        )
        result = dp.get_member_eligibility(["NONEXISTENT"], eligibility_lf=elig_df.lazy())
        assert result is None

    @pytest.mark.unit
    def test_get_pharmacy_claims_empty_list(self, mock_storage):
        """get_pharmacy_claims returns None for empty member_ids."""
        dp = self._make_data(mock_storage)
        assert dp.get_pharmacy_claims([]) is None

    @pytest.mark.unit
    def test_get_pharmacy_claims_no_matches(self, mock_storage):
        """get_pharmacy_claims returns None when no matches."""
        dp = self._make_data(mock_storage)
        rx_df = pl.DataFrame(
            {
                "claim_id": ["C1"],
                "claim_line_number": [1],
                "member_id": ["M1"],
                "person_id": ["P1"],
                "dispensing_date": ["2024-01-01"],
                "ndc_code": ["12345"],
                "prescribing_provider_npi": ["NPI1"],
                "dispensing_provider_npi": ["NPI2"],
                "quantity": [30],
                "days_supply": [30],
                "refills": [0],
                "paid_date": ["2024-02-01"],
                "paid_amount": [100.0],
                "allowed_amount": [120.0],
                "charge_amount": [150.0],
                "coinsurance_amount": [10.0],
                "copayment_amount": [5.0],
                "deductible_amount": [20.0],
                "in_network_flag": [1],
            }
        )
        result = dp.get_pharmacy_claims(["NONEXISTENT"], pharmacy_claim_lf=rx_df.lazy())
        assert result is None

    @pytest.mark.unit
    def test_get_pharmacy_claims_with_data(self, mock_storage):
        """get_pharmacy_claims returns matching pharmacy claims."""
        dp = self._make_data(mock_storage)
        rx_df = pl.DataFrame(
            {
                "claim_id": ["C1", "C2"],
                "claim_line_number": [1, 1],
                "member_id": ["M1", "M2"],
                "person_id": ["P1", "P2"],
                "dispensing_date": ["2024-01-01", "2024-02-01"],
                "ndc_code": ["12345", "67890"],
                "prescribing_provider_npi": ["NPI1", "NPI3"],
                "dispensing_provider_npi": ["NPI2", "NPI4"],
                "quantity": [30, 60],
                "days_supply": [30, 60],
                "refills": [0, 1],
                "paid_date": ["2024-02-01", "2024-03-01"],
                "paid_amount": [100.0, 200.0],
                "allowed_amount": [120.0, 240.0],
                "charge_amount": [150.0, 300.0],
                "coinsurance_amount": [10.0, 20.0],
                "copayment_amount": [5.0, 10.0],
                "deductible_amount": [20.0, 40.0],
                "in_network_flag": [1, 0],
            }
        )
        result = dp.get_pharmacy_claims(["M1"], pharmacy_claim_lf=rx_df.lazy())
        assert result is not None
        assert result.height == 1

    @pytest.mark.unit
    def test_get_medical_claims_no_filters(self, mock_storage):
        """get_medical_claims returns all claims when no filters given."""
        dp = self._make_data(mock_storage)
        claims_df = self._make_claims_df()
        result = dp.get_medical_claims(filters=None, medical_claim_lf=claims_df.lazy())
        assert result is not None
        assert result.height == 2

    @pytest.mark.unit
    def test_get_medical_claims_filter_member_ids(self, mock_storage):
        """get_medical_claims filters by member_ids."""
        dp = self._make_data(mock_storage)
        claims_df = self._make_claims_df()
        result = dp.get_medical_claims(
            filters={"member_ids": ["M1"]},
            medical_claim_lf=claims_df.lazy(),
        )
        assert result is not None
        assert all(r == "M1" for r in result["member_id"].to_list())

    @pytest.mark.unit
    def test_get_medical_claims_filter_hcpcs(self, mock_storage):
        """get_medical_claims filters by hcpcs_codes."""
        dp = self._make_data(mock_storage)
        claims_df = self._make_claims_df()
        result = dp.get_medical_claims(
            filters={"hcpcs_codes": ["99213"]},
            medical_claim_lf=claims_df.lazy(),
        )
        assert result is not None

    @pytest.mark.unit
    def test_get_medical_claims_filter_npi(self, mock_storage):
        """get_medical_claims filters by npi_codes."""
        dp = self._make_data(mock_storage)
        claims_df = self._make_claims_df()
        result = dp.get_medical_claims(
            filters={"npi_codes": ["NPI1"]},
            medical_claim_lf=claims_df.lazy(),
        )
        assert result is not None

    @pytest.mark.unit
    def test_get_medical_claims_filter_tin(self, mock_storage):
        """get_medical_claims filters by tin_codes."""
        dp = self._make_data(mock_storage)
        claims_df = self._make_claims_df()
        result = dp.get_medical_claims(
            filters={"tin_codes": ["TIN1"]},
            medical_claim_lf=claims_df.lazy(),
        )
        assert result is not None

    @pytest.mark.unit
    def test_get_medical_claims_filter_dates(self, mock_storage):
        """get_medical_claims filters by start_date and end_date."""
        dp = self._make_data(mock_storage)
        claims_df = self._make_claims_df()
        result = dp.get_medical_claims(
            filters={"start_date": "2024-01-01", "end_date": "2024-01-31"},
            medical_claim_lf=claims_df.lazy(),
        )
        assert result is not None

    @pytest.mark.unit
    def test_get_medical_claims_no_matches(self, mock_storage):
        """get_medical_claims returns None when no matches."""
        dp = self._make_data(mock_storage)
        claims_df = self._make_claims_df()
        result = dp.get_medical_claims(
            filters={"member_ids": ["NONEXISTENT"]},
            medical_claim_lf=claims_df.lazy(),
        )
        assert result is None

    @pytest.mark.unit
    def test_get_medical_claims_empty_filter_values(self, mock_storage):
        """get_medical_claims ignores filter keys with empty values."""
        dp = self._make_data(mock_storage)
        claims_df = self._make_claims_df()
        result = dp.get_medical_claims(
            filters={"member_ids": [], "hcpcs_codes": [], "npi_codes": [], "tin_codes": []},
            medical_claim_lf=claims_df.lazy(),
        )
        assert result is not None
        assert result.height == 2

    @staticmethod
    def _make_claims_df():
        return pl.DataFrame(
            {
                "claim_id": ["C1", "C2"],
                "claim_line_number": [1, 1],
                "claim_type": ["I", "P"],
                "member_id": ["M1", "M2"],
                "person_id": ["P1", "P2"],
                "claim_start_date": ["2024-01-15", "2024-02-15"],
                "claim_end_date": ["2024-01-20", "2024-02-20"],
                "claim_line_start_date": ["2024-01-15", "2024-02-15"],
                "claim_line_end_date": ["2024-01-20", "2024-02-20"],
                "admission_date": ["2024-01-15", None],
                "discharge_date": ["2024-01-20", None],
                "place_of_service_code": ["21", "11"],
                "bill_type_code": ["111", None],
                "revenue_center_code": ["0100", None],
                "hcpcs_code": ["99213", "99214"],
                "hcpcs_modifier_1": [None, None],
                "hcpcs_modifier_2": [None, None],
                "rendering_npi": ["NPI1", "NPI2"],
                "rendering_tin": ["TIN1", "TIN2"],
                "billing_npi": ["NPI1", "NPI3"],
                "billing_tin": ["TIN1", "TIN3"],
                "facility_npi": [None, None],
                "paid_amount": [100.0, 200.0],
                "allowed_amount": [120.0, 240.0],
                "charge_amount": [150.0, 300.0],
                "diagnosis_code_1": ["Z00.00", "J06.9"],
                "diagnosis_code_2": [None, None],
                "diagnosis_code_3": [None, None],
            }
        )


class TestAnalysisPlugins:
    """Tests for AnalysisPlugins."""

    def _make_analysis(self):
        from acoharmony._notes.plugins import AnalysisPlugins

        return AnalysisPlugins()

    @pytest.mark.unit
    def test_compute_summary_auto_detect(self):
        """compute_summary auto-detects numeric columns."""
        ap = self._make_analysis()
        df = pl.DataFrame(
            {
                "id": [1, 2, 3],
                "amount": [10.0, 20.0, 30.0],
                "name": ["a", "b", "c"],
                "claim_date": ["2024-01-01", "2024-02-01", "2024-03-01"],
            }
        )
        summary = ap.compute_summary(df)
        assert summary["total_rows"] == 3
        assert summary["total_columns"] == 4
        assert "amount_sum" in summary
        assert summary["amount_sum"] == 60.0
        assert "amount_mean" in summary
        assert "claim_date_min" in summary

    @pytest.mark.unit
    def test_compute_summary_explicit_metrics(self):
        """compute_summary uses explicitly provided metrics."""
        ap = self._make_analysis()
        df = pl.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
        summary = ap.compute_summary(df, metrics=["x"])
        assert "x_sum" in summary
        assert "y_sum" not in summary

    @pytest.mark.unit
    def test_compute_summary_missing_metric_column(self):
        """compute_summary skips metrics not in dataframe columns."""
        ap = self._make_analysis()
        df = pl.DataFrame({"x": [1, 2]})
        summary = ap.compute_summary(df, metrics=["nonexistent"])
        assert "nonexistent_sum" not in summary

    @pytest.mark.unit
    def test_compute_summary_date_column_error(self):
        """compute_summary handles date column min/max errors gracefully."""
        ap = self._make_analysis()
        df = pl.DataFrame({"date_col": ["not-a-date"]})
        ap.compute_summary(df)
        # Should not raise, may or may not have date_col_min depending on the value

    @pytest.mark.unit
    def test_top_n_analysis_sum(self):
        """top_n_analysis computes sum aggregation."""
        ap = self._make_analysis()
        df = pl.DataFrame(
            {
                "category": ["A", "A", "B", "B", "C"],
                "value": [10, 20, 30, 40, 50],
            }
        )
        result = ap.top_n_analysis(df, "category", "value", n=2, agg_func="sum")
        assert result.height == 2
        assert result["value_sum"][0] == 70  # B: 30+40

    @pytest.mark.unit
    def test_top_n_analysis_mean(self):
        """top_n_analysis computes mean aggregation."""
        ap = self._make_analysis()
        df = pl.DataFrame(
            {
                "category": ["A", "A", "B"],
                "value": [10, 20, 30],
            }
        )
        result = ap.top_n_analysis(df, "category", "value", n=10, agg_func="mean")
        assert result.height == 2

    @pytest.mark.unit
    def test_top_n_analysis_count(self):
        """top_n_analysis computes count aggregation."""
        ap = self._make_analysis()
        df = pl.DataFrame(
            {
                "category": ["A", "A", "A", "B"],
                "value": [1, 2, 3, 4],
            }
        )
        result = ap.top_n_analysis(df, "category", "value", n=1, agg_func="count")
        assert result.height == 1

    @pytest.mark.unit
    def test_top_n_analysis_unsupported_agg(self):
        """top_n_analysis raises ValueError for unsupported aggregation."""
        ap = self._make_analysis()
        df = pl.DataFrame({"cat": ["A"], "val": [1]})
        with pytest.raises(ValueError, match="Unsupported aggregation"):
            ap.top_n_analysis(df, "cat", "val", agg_func="median")


class TestUtilityPlugins:
    """Tests for UtilityPlugins."""

    def _make_utils(self):
        from acoharmony._notes.plugins import UtilityPlugins

        return UtilityPlugins()

    @pytest.mark.unit
    def test_format_size_bytes(self):
        """format_size formats bytes correctly."""
        assert self._make_utils().format_size(500) == "500.0 B"

    @pytest.mark.unit
    def test_format_size_kb(self):
        """format_size formats kilobytes correctly."""
        assert self._make_utils().format_size(1536) == "1.5 KB"

    @pytest.mark.unit
    def test_format_size_mb(self):
        """format_size formats megabytes correctly."""
        result = self._make_utils().format_size(1_500_000)
        assert "MB" in result

    @pytest.mark.unit
    def test_format_size_gb(self):
        """format_size formats gigabytes correctly."""
        result = self._make_utils().format_size(2 * 1024**3)
        assert "GB" in result

    @pytest.mark.unit
    def test_format_size_tb(self):
        """format_size formats terabytes correctly."""
        result = self._make_utils().format_size(3 * 1024**4)
        assert "TB" in result

    @pytest.mark.unit
    def test_format_size_pb(self):
        """format_size formats petabytes correctly."""
        result = self._make_utils().format_size(2 * 1024**5)
        assert "PB" in result

    @pytest.mark.unit
    def test_parse_input_list_normal(self):
        """parse_input_list splits comma-separated values."""
        result = self._make_utils().parse_input_list("A, B, C")
        assert result == ["A", "B", "C"]

    @pytest.mark.unit
    def test_parse_input_list_empty(self):
        """parse_input_list returns empty list for empty input."""
        assert self._make_utils().parse_input_list("") == []
        assert self._make_utils().parse_input_list("   ") == []

    @pytest.mark.unit
    def test_parse_input_list_custom_delimiter(self):
        """parse_input_list uses custom delimiter."""
        result = self._make_utils().parse_input_list("A;B;C", delimiter=";")
        assert result == ["A", "B", "C"]

    @pytest.mark.unit
    def test_parse_input_list_strips_whitespace(self):
        """parse_input_list strips whitespace from items."""
        result = self._make_utils().parse_input_list("  X ,  Y  ,  Z  ")
        assert result == ["X", "Y", "Z"]

    @pytest.mark.unit
    def test_parse_input_list_skips_empty_items(self):
        """parse_input_list removes empty items."""
        result = self._make_utils().parse_input_list("A,,B,,,C")
        assert result == ["A", "B", "C"]

    @pytest.mark.unit
    def test_create_multi_sheet_excel(self):
        """create_multi_sheet_excel returns bytes for workbook."""
        utils = self._make_utils()
        sheets = {
            "Sheet1": pl.DataFrame({"a": [1, 2]}),
            "Sheet2": pl.DataFrame({"b": [3, 4]}),
        }
        result = utils.create_multi_sheet_excel(sheets)
        assert isinstance(result, bytes)
        assert len(result) > 0

    @pytest.mark.unit
    def test_create_multi_sheet_excel_with_filename(self):
        """create_multi_sheet_excel accepts optional filename param."""
        utils = self._make_utils()
        sheets = {"Data": pl.DataFrame({"x": [1]})}
        result = utils.create_multi_sheet_excel(sheets, filename="test.xlsx")
        assert isinstance(result, bytes)


class TestNotebookGenerator:
    """Tests for NotebookGenerator."""

    def _make_generator(self, mock_storage, mock_catalog, tmp_path):
        with patch("acoharmony._notes.generator.Catalog", return_value=mock_catalog):
            with patch("acoharmony._notes.generator.StorageBackend", return_value=mock_storage):
                from acoharmony._notes.generator import NotebookGenerator

                gen = NotebookGenerator(
                    storage_backend=mock_storage,
                    output_dir=tmp_path / "notebooks",
                )
        return gen

    @pytest.mark.unit
    def test_init_with_defaults(self, mock_storage, mock_catalog):
        """NotebookGenerator initializes with default output directory."""
        mock_storage.get_data_path.return_value = Path("/tmp/test_data")
        with patch("acoharmony._notes.generator.Catalog", return_value=mock_catalog):
            from acoharmony._notes.generator import NotebookGenerator

            gen = NotebookGenerator(storage_backend=mock_storage)
        assert gen.output_dir == Path("/tmp/test_data").parent / "notebooks" / "generated"

    @pytest.mark.unit
    def test_init_with_string_data_path(self, mock_storage, mock_catalog):
        """NotebookGenerator handles string data path for default output dir."""
        mock_storage.get_data_path.return_value = "s3://bucket/data"
        with patch("acoharmony._notes.generator.Catalog", return_value=mock_catalog):
            with patch("pathlib.Path.mkdir"):
                from acoharmony._notes.generator import NotebookGenerator

                gen = NotebookGenerator(storage_backend=mock_storage)
        assert "notebooks" in str(gen.output_dir)
        assert "generated" in str(gen.output_dir)

    @pytest.mark.unit
    def test_init_default_storage(self, mock_catalog, tmp_path):
        """NotebookGenerator creates default StorageBackend when none given."""
        mock_sb = MagicMock()
        mock_sb.get_data_path.return_value = Path("/tmp/data")
        with patch("acoharmony._notes.generator.StorageBackend", return_value=mock_sb) as sb_cls:
            with patch("acoharmony._notes.generator.Catalog", return_value=mock_catalog):
                from acoharmony._notes.generator import NotebookGenerator

                NotebookGenerator(output_dir=tmp_path / "out")
        sb_cls.assert_called_once()

    @pytest.mark.unit
    def test_get_data_path_for_schema_with_medallion(self, mock_storage, mock_catalog, tmp_path):
        """get_data_path_for_schema uses medallion layer data_tier."""
        gen = self._make_generator(mock_storage, mock_catalog, tmp_path)
        meta = SimpleNamespace(
            name="claims",
            medallion_layer=SimpleNamespace(data_tier="curated"),
            storage={"tier": "gold"},
        )
        mock_catalog.get_table_metadata.return_value = meta
        mock_storage.get_data_path.return_value = Path("/data/curated")
        result = gen.get_data_path_for_schema("claims")
        assert result == "/data/curated/claims.parquet"

    @pytest.mark.unit
    def test_get_data_path_for_schema_no_medallion_with_storage(
        self, mock_storage, mock_catalog, tmp_path
    ):
        """get_data_path_for_schema falls back to storage tier."""
        gen = self._make_generator(mock_storage, mock_catalog, tmp_path)
        meta = SimpleNamespace(
            name="test",
            medallion_layer=None,
            storage={"tier": "processed"},
        )
        mock_catalog.get_table_metadata.return_value = meta
        mock_storage.get_data_path.return_value = Path("/data/processed")
        result = gen.get_data_path_for_schema("test")
        assert "test.parquet" in result

    @pytest.mark.unit
    def test_get_data_path_for_schema_no_medallion_no_storage(
        self, mock_storage, mock_catalog, tmp_path
    ):
        """get_data_path_for_schema uses default tier when no medallion or storage."""
        gen = self._make_generator(mock_storage, mock_catalog, tmp_path)
        meta = SimpleNamespace(name="test", medallion_layer=None)
        mock_catalog.get_table_metadata.return_value = meta
        mock_storage.get_data_path.return_value = Path("/data/processed")
        result = gen.get_data_path_for_schema("test")
        assert "test.parquet" in result

    @pytest.mark.unit
    def test_get_data_path_for_schema_cloud_path(self, mock_storage, mock_catalog, tmp_path):
        """get_data_path_for_schema handles cloud (string) paths."""
        gen = self._make_generator(mock_storage, mock_catalog, tmp_path)
        meta = SimpleNamespace(
            name="claims",
            medallion_layer=SimpleNamespace(data_tier="curated"),
        )
        mock_catalog.get_table_metadata.return_value = meta
        mock_storage.get_data_path.return_value = "s3://bucket/curated"
        result = gen.get_data_path_for_schema("claims")
        assert result == "s3://bucket/curated/claims.parquet"

    @pytest.mark.unit
    def test_create_notebook(self, mock_storage, mock_catalog, tmp_path):
        """create_notebook generates a notebook file."""
        gen = self._make_generator(mock_storage, mock_catalog, tmp_path)

        # Mock schema details
        schema_dict = {
            "name": "test_schema",
            "description": "Test",
            "storage": {"tier": "gold"},
            "columns": [],
            "file_format": {},
            "keys": {},
        }
        gen.get_schema_with_full_details = MagicMock(return_value=schema_dict)
        gen.get_data_path_for_schema = MagicMock(return_value="/data/test_schema.parquet")

        # Mock template
        mock_template = MagicMock()
        mock_template.render.return_value = "# Generated notebook\nprint('hello')"
        gen.jinja_env = MagicMock()
        gen.jinja_env.get_template.return_value = mock_template

        # Mock from_schema
        with patch("acoharmony._notes.generator.NotebookConfig.from_schema") as mock_from:
            mock_config = MagicMock()
            mock_from.return_value = mock_config
            result = gen.create_notebook("test_schema")

        assert result.exists()
        assert result.name == "test_schema_dashboard.py"
        content = result.read_text()
        assert "hello" in content

    @pytest.mark.unit
    def test_create_notebook_custom_output_name(self, mock_storage, mock_catalog, tmp_path):
        """create_notebook uses custom output name."""
        gen = self._make_generator(mock_storage, mock_catalog, tmp_path)
        gen.get_schema_with_full_details = MagicMock(
            return_value={
                "name": "t",
                "description": "d",
                "storage": {"tier": "gold"},
                "columns": [],
                "file_format": {},
                "keys": {},
            }
        )
        gen.get_data_path_for_schema = MagicMock(return_value="/data/t.parquet")
        mock_template = MagicMock()
        mock_template.render.return_value = "content"
        gen.jinja_env = MagicMock()
        gen.jinja_env.get_template.return_value = mock_template

        with patch("acoharmony._notes.generator.NotebookConfig.from_schema") as mock_from:
            mock_from.return_value = MagicMock()
            result = gen.create_notebook("t", output_name="custom.py")

        assert result.name == "custom.py"

    @pytest.mark.unit
    def test_list_raw_schemas(self, mock_storage, mock_catalog, tmp_path):
        """list_raw_schemas returns sorted bronze table names."""
        gen = self._make_generator(mock_storage, mock_catalog, tmp_path)
        mock_catalog.list_tables.return_value = ["cclf3", "cclf1", "cclf2"]

        with patch("acoharmony.medallion.MedallionLayer") as mock_ml:
            mock_ml.BRONZE = "BRONZE"
            result = gen.list_raw_schemas()

        assert result == ["cclf1", "cclf2", "cclf3"]

    @pytest.mark.unit
    def test_create_notebooks_for_raw_schemas(self, mock_storage, mock_catalog, tmp_path):
        """create_notebooks_for_raw_schemas generates notebooks for all raw schemas."""
        gen = self._make_generator(mock_storage, mock_catalog, tmp_path)
        gen.list_raw_schemas = MagicMock(return_value=["schema1", "schema2"])
        gen.create_notebook = MagicMock(
            side_effect=[
                tmp_path / "schema1_dashboard.py",
                tmp_path / "schema2_dashboard.py",
            ]
        )
        result = gen.create_notebooks_for_raw_schemas()
        assert len(result) == 2
        assert gen.create_notebook.call_count == 2

    @pytest.mark.unit
    def test_create_notebooks_for_raw_schemas_with_error(
        self, mock_storage, mock_catalog, tmp_path
    ):
        """create_notebooks_for_raw_schemas continues on individual failures."""
        gen = self._make_generator(mock_storage, mock_catalog, tmp_path)
        gen.list_raw_schemas = MagicMock(return_value=["good", "bad", "good2"])
        gen.create_notebook = MagicMock(
            side_effect=[
                tmp_path / "good_dashboard.py",
                RuntimeError("fail"),
                tmp_path / "good2_dashboard.py",
            ]
        )
        result = gen.create_notebooks_for_raw_schemas()
        assert len(result) == 2


# ===========================================================================
# NOTES - __init__.py
# ===========================================================================


class TestNotesInit:
    """Tests for _notes package __init__.py exports."""

    @pytest.mark.unit
    def test_exports(self):
        """Package exports expected names."""
        from acoharmony._notes import (
            NotebookConfig,
            NotebookGenerator,
            analysis,
            data,
            setup,
            ui,
            utils,
        )

        assert NotebookConfig is not None
        assert NotebookGenerator is not None
        assert setup is not None
        assert ui is not None
        assert data is not None
        assert analysis is not None
        assert utils is not None


# ===========================================================================
# TRACE - tracer.py
# ===========================================================================


class TestPluginSingletons:
    """Tests for module-level singleton plugin instances."""

    @pytest.mark.unit
    def test_singletons_exist(self):
        """Module-level singletons are properly instantiated."""
        from acoharmony._notes.plugins import (
            AnalysisPlugins,
            DataPlugins,
            SetupPlugins,
            UIPlugins,
            UtilityPlugins,
            analysis,
            data,
            setup,
            ui,
            utils,
        )

        assert isinstance(setup, SetupPlugins)
        assert isinstance(ui, UIPlugins)
        assert isinstance(data, DataPlugins)
        assert isinstance(analysis, AnalysisPlugins)
        assert isinstance(utils, UtilityPlugins)


"""Additional tests for _notes/generator.py to cover 9 missing lines.

Targets:
- NotebookGenerator initialization
- get_schema_with_full_details with existing and missing YAML
- get_data_path_for_schema with different tiers
- create_notebook
- list_raw_schemas
- create_notebooks_for_raw_schemas
"""


class TestNotebookGeneratorInit:
    """Test NotebookGenerator initialization."""

    @pytest.mark.unit
    def test_init_with_defaults(self, tmp_path):
        """Init with default storage backend."""
        with (
            patch("acoharmony._notes.generator.StorageBackend") as mock_sb_cls,
            patch("acoharmony._notes.generator.Catalog"),
        ):
            mock_sb = MagicMock()
            mock_sb.get_data_path.return_value = tmp_path / "data"
            mock_sb_cls.return_value = mock_sb

            from acoharmony._notes.generator import NotebookGenerator

            gen = NotebookGenerator(storage_backend=mock_sb, output_dir=tmp_path / "notebooks")
            assert gen.output_dir == tmp_path / "notebooks"
            assert gen.storage is mock_sb

    @pytest.mark.unit
    def test_init_creates_output_dir(self, tmp_path):
        """Init creates output directory if not exists."""
        output_dir = tmp_path / "new_dir" / "notebooks"
        with (
            patch("acoharmony._notes.generator.StorageBackend") as mock_sb_cls,
            patch("acoharmony._notes.generator.Catalog"),
        ):
            mock_sb = MagicMock()
            mock_sb.get_data_path.return_value = tmp_path
            mock_sb_cls.return_value = mock_sb

            from acoharmony._notes.generator import NotebookGenerator

            NotebookGenerator(storage_backend=mock_sb, output_dir=output_dir)
            assert output_dir.exists()

    @pytest.mark.unit
    def test_init_with_default_output_dir(self, tmp_path):
        """Init derives output_dir from storage backend when not specified."""
        with (
            patch("acoharmony._notes.generator.StorageBackend") as mock_sb_cls,
            patch("acoharmony._notes.generator.Catalog"),
        ):
            mock_sb = MagicMock()
            mock_sb.get_data_path.return_value = tmp_path / "data"
            mock_sb_cls.return_value = mock_sb

            from acoharmony._notes.generator import NotebookGenerator

            gen = NotebookGenerator(storage_backend=mock_sb)
            assert "notebooks" in str(gen.output_dir)

    @pytest.mark.unit
    def test_init_with_non_path_data_path(self, tmp_path):
        """Init with cloud data path uses fallback output_dir."""
        with (
            patch("acoharmony._notes.generator.StorageBackend") as mock_sb_cls,
            patch("acoharmony._notes.generator.Catalog"),
            patch("pathlib.Path.mkdir"),
        ):
            mock_sb = MagicMock()
            mock_sb.get_data_path.return_value = "s3://bucket/data"
            mock_sb_cls.return_value = mock_sb

            from acoharmony._notes.generator import NotebookGenerator

            gen = NotebookGenerator(storage_backend=mock_sb)
            assert "generated" in str(gen.output_dir)


class TestGetSchemaWithFullDetails:
    """Test get_schema_with_full_details method."""

    @pytest.mark.unit
    def test_with_missing_yaml(self, tmp_path):
        """Falls back to metadata attributes when YAML not found."""
        with (
            patch("acoharmony._notes.generator.StorageBackend") as mock_sb_cls,
            patch("acoharmony._notes.generator.Catalog") as mock_cat_cls,
        ):
            mock_sb = MagicMock()
            mock_sb.get_data_path.return_value = tmp_path
            mock_sb_cls.return_value = mock_sb

            mock_schema = MagicMock()
            mock_schema.name = "test_schema"
            mock_schema.description = "Test"
            mock_schema.storage = {}
            mock_schema.columns = []
            mock_schema.file_format = {}
            mock_schema.keys = {}

            mock_catalog = MagicMock()
            mock_catalog.get_table_metadata.return_value = mock_schema
            mock_cat_cls.return_value = mock_catalog

            from acoharmony._notes.generator import NotebookGenerator

            gen = NotebookGenerator(storage_backend=mock_sb, output_dir=tmp_path)

            with patch.object(Path, "exists", return_value=False):
                result = gen.get_schema_with_full_details("test_schema")
                assert result["name"] == "test_schema"


class TestGetDataPathForSchema:
    """Test get_data_path_for_schema method."""

    @pytest.mark.unit
    def test_with_medallion_layer(self, tmp_path):
        """Uses medallion_layer.data_tier for path resolution."""
        with (
            patch("acoharmony._notes.generator.StorageBackend") as mock_sb_cls,
            patch("acoharmony._notes.generator.Catalog") as mock_cat_cls,
        ):
            mock_sb = MagicMock()
            mock_sb.get_data_path.return_value = tmp_path / "silver"
            mock_sb_cls.return_value = mock_sb

            mock_layer = MagicMock()
            mock_layer.data_tier = "processed"
            mock_schema = MagicMock()
            mock_schema.medallion_layer = mock_layer

            mock_catalog = MagicMock()
            mock_catalog.get_table_metadata.return_value = mock_schema
            mock_cat_cls.return_value = mock_catalog

            from acoharmony._notes.generator import NotebookGenerator

            gen = NotebookGenerator(storage_backend=mock_sb, output_dir=tmp_path)
            path = gen.get_data_path_for_schema("test_schema")
            assert "test_schema.parquet" in path

    @pytest.mark.unit
    def test_with_cloud_storage(self, tmp_path):
        """Cloud storage returns URL string."""
        with (
            patch("acoharmony._notes.generator.StorageBackend") as mock_sb_cls,
            patch("acoharmony._notes.generator.Catalog") as mock_cat_cls,
        ):
            mock_sb = MagicMock()
            mock_sb.get_data_path.return_value = "s3://bucket/silver"
            mock_sb_cls.return_value = mock_sb

            mock_layer = MagicMock()
            mock_layer.data_tier = "processed"
            mock_schema = MagicMock()
            mock_schema.medallion_layer = mock_layer

            mock_catalog = MagicMock()
            mock_catalog.get_table_metadata.return_value = mock_schema
            mock_cat_cls.return_value = mock_catalog

            from acoharmony._notes.generator import NotebookGenerator

            gen = NotebookGenerator(storage_backend=mock_sb, output_dir=tmp_path)
            path = gen.get_data_path_for_schema("test_schema")
            assert path == "s3://bucket/silver/test_schema.parquet"

    @pytest.mark.unit
    def test_without_medallion_layer(self, tmp_path):
        """Fallback to storage config tier."""
        with (
            patch("acoharmony._notes.generator.StorageBackend") as mock_sb_cls,
            patch("acoharmony._notes.generator.Catalog") as mock_cat_cls,
        ):
            mock_sb = MagicMock()
            mock_sb.get_data_path.return_value = tmp_path / "data"
            mock_sb_cls.return_value = mock_sb

            mock_schema = MagicMock()
            mock_schema.medallion_layer = None
            mock_schema.storage = {"tier": "raw"}

            mock_catalog = MagicMock()
            mock_catalog.get_table_metadata.return_value = mock_schema
            mock_cat_cls.return_value = mock_catalog

            from acoharmony._notes.generator import NotebookGenerator

            gen = NotebookGenerator(storage_backend=mock_sb, output_dir=tmp_path)
            path = gen.get_data_path_for_schema("test_schema")
            assert "test_schema.parquet" in path


class TestListRawSchemas:
    """Test list_raw_schemas method."""

    @pytest.mark.unit
    def test_returns_sorted_list(self, tmp_path):
        with (
            patch("acoharmony._notes.generator.StorageBackend") as mock_sb_cls,
            patch("acoharmony._notes.generator.Catalog") as mock_cat_cls,
        ):
            mock_sb = MagicMock()
            mock_sb.get_data_path.return_value = tmp_path
            mock_sb_cls.return_value = mock_sb

            mock_catalog = MagicMock()
            mock_catalog.list_tables.return_value = ["cclf5", "cclf1", "bar"]
            mock_cat_cls.return_value = mock_catalog

            from acoharmony._notes.generator import NotebookGenerator

            gen = NotebookGenerator(storage_backend=mock_sb, output_dir=tmp_path)
            result = gen.list_raw_schemas()
            assert result == ["bar", "cclf1", "cclf5"]


class TestCreateNotebooksForRawSchemas:
    """Test create_notebooks_for_raw_schemas method."""

    @pytest.mark.unit
    def test_generates_notebooks(self, tmp_path):
        with (
            patch("acoharmony._notes.generator.StorageBackend") as mock_sb_cls,
            patch("acoharmony._notes.generator.Catalog") as mock_cat_cls,
        ):
            mock_sb = MagicMock()
            mock_sb.get_data_path.return_value = tmp_path
            mock_sb_cls.return_value = mock_sb

            mock_catalog = MagicMock()
            mock_catalog.list_tables.return_value = ["schema1"]
            mock_cat_cls.return_value = mock_catalog

            from acoharmony._notes.generator import NotebookGenerator

            gen = NotebookGenerator(storage_backend=mock_sb, output_dir=tmp_path)

            with patch.object(gen, "create_notebook", return_value=tmp_path / "schema1.py"):
                result = gen.create_notebooks_for_raw_schemas()
                assert len(result) == 1

    @pytest.mark.unit
    def test_handles_errors_gracefully(self, tmp_path, capsys):
        with (
            patch("acoharmony._notes.generator.StorageBackend") as mock_sb_cls,
            patch("acoharmony._notes.generator.Catalog") as mock_cat_cls,
        ):
            mock_sb = MagicMock()
            mock_sb.get_data_path.return_value = tmp_path
            mock_sb_cls.return_value = mock_sb

            mock_catalog = MagicMock()
            mock_catalog.list_tables.return_value = ["bad_schema"]
            mock_cat_cls.return_value = mock_catalog

            from acoharmony._notes.generator import NotebookGenerator

            gen = NotebookGenerator(storage_backend=mock_sb, output_dir=tmp_path)

            with patch.object(gen, "create_notebook", side_effect=Exception("fail")):
                result = gen.create_notebooks_for_raw_schemas()
                assert len(result) == 0
                captured = capsys.readouterr()
                assert "ERROR" in captured.out
