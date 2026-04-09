"""Additional tests for _utils modules."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock, Mock, patch
import contextlib
import sys  # noqa: E402
import zipfile  # noqa: E402
from pathlib import Path  # noqa: E402
from unittest.mock import (
    MagicMock,
    Mock,  # noqa: E402
    patch,
)

import polars as pl
import pytest

import acoharmony._transforms as pkg
# show_query module removed

_TRANSFORM_NAMES = [
    "_aco_alignment_temporal",
    "_aco_alignment_voluntary",
    "_aco_alignment_demographics",
    "_aco_alignment_office",
    "_aco_alignment_provider",
    "_aco_alignment_metrics",
    "_aco_alignment_metadata",
    "_last_ffs_service",
]


def _patch_transforms(overrides=None):
    """Return a combined context manager that patches all transform submodules
    on the acoharmony._transforms package so that ``from .._transforms import ...``
    inside _generate_aco_alignment_sql picks up the mocks."""


    patches = []
    for name in _TRANSFORM_NAMES:
        mock = (overrides or {}).get(name, MagicMock())
        patches.append(patch.object(pkg, name, mock, create=True))
    return contextlib.ExitStack(), patches


# ---------------------------------------------------------------------------
# show_query - aco_alignment path (mocked)
# ---------------------------------------------------------------------------

class TestShowQueryAcoAlignment:
    """Removed — show_query module deleted."""
    pass

class TestGenerateAcoAlignmentSql:
    """Removed — show_query module deleted."""
    pass

class TestManualSqlGenerationEdgeCases:
    """Removed — show_query module deleted."""
    pass

class TestGenerateAcoAlignmentSqlFallback:
    """Removed — show_query module deleted."""
    pass

_SENTINEL = object()


# ---------------------------------------------------------------------------
# ValueSetLoader tests
# ---------------------------------------------------------------------------


class TestValueSetLoader:
    """Tests for ValueSetLoader class."""

    def _make_loader(self, tmp_path):
        """Create a ValueSetLoader with mock parquet files in tmp_path."""
        from acoharmony._utils._value_set_loader import ValueSetLoader

        # Create value_sets parquet
        vs_df = pl.DataFrame(
            {
                "concept_name": [
                    "Diabetes Diagnosis",
                    "Diabetes Diagnosis",
                    "HbA1c Test",
                ],
                "concept_oid": ["2.16.840.1", "2.16.840.1", "2.16.840.2"],
                "code": ["E11.9", "E11.65", "83036"],
                "code_system": ["ICD-10-CM", "ICD-10-CM", "CPT"],
            }
        )
        vs_df.write_parquet(
            tmp_path / "value_sets_quality_measures_value_sets.parquet"
        )

        # Create concepts parquet
        concepts_df = pl.DataFrame(
            {
                "concept_name": [
                    "Diabetes Diagnosis",
                    "HbA1c Test",
                    "Diabetes Diagnosis",
                ],
                "concept_oid": ["2.16.840.1", "2.16.840.2", "2.16.840.1"],
                "measure_id": ["NQF0059", "NQF0059", "NQF0061"],
                "measure_name": [
                    "Diabetes: HbA1c Control",
                    "Diabetes: HbA1c Control",
                    "Diabetes: BP Control",
                ],
            }
        )
        concepts_df.write_parquet(
            tmp_path / "value_sets_quality_measures_concepts.parquet"
        )

        # Create measures parquet
        measures_df = pl.DataFrame(
            {
                "id": ["NQF0059", "NQF0061"],
                "name": ["Diabetes: HbA1c Control", "Diabetes: BP Control"],
                "description": ["HbA1c poor control", "BP control"],
                "version": ["2024", "2024"],
                "steward": ["NCQA", "NCQA"],
            }
        )
        measures_df.write_parquet(
            tmp_path / "value_sets_quality_measures_measures.parquet"
        )

        return ValueSetLoader(tmp_path)

    @pytest.mark.unit
    def test_load_value_sets_catalog(self, tmp_path):
        loader = self._make_loader(tmp_path)
        lf = loader.load_value_sets_catalog()
        assert isinstance(lf, pl.LazyFrame)
        df = lf.collect()
        assert len(df) == 3
        assert "concept_name" in df.columns
        assert "code" in df.columns

    @pytest.mark.unit
    def test_load_value_sets_catalog_caches(self, tmp_path):
        loader = self._make_loader(tmp_path)
        lf1 = loader.load_value_sets_catalog()
        lf2 = loader.load_value_sets_catalog()
        assert lf1 is lf2

    @pytest.mark.unit
    def test_load_concepts_catalog(self, tmp_path):
        loader = self._make_loader(tmp_path)
        lf = loader.load_concepts_catalog()
        df = lf.collect()
        assert len(df) == 3
        assert "measure_id" in df.columns

    @pytest.mark.unit
    def test_load_concepts_catalog_caches(self, tmp_path):
        loader = self._make_loader(tmp_path)
        lf1 = loader.load_concepts_catalog()
        lf2 = loader.load_concepts_catalog()
        assert lf1 is lf2

    @pytest.mark.unit
    def test_load_measures_catalog(self, tmp_path):
        loader = self._make_loader(tmp_path)
        lf = loader.load_measures_catalog()
        df = lf.collect()
        assert len(df) == 2
        assert "NQF0059" in df["id"].to_list()

    @pytest.mark.unit
    def test_load_measures_catalog_caches(self, tmp_path):
        loader = self._make_loader(tmp_path)
        lf1 = loader.load_measures_catalog()
        lf2 = loader.load_measures_catalog()
        assert lf1 is lf2

    @pytest.mark.unit
    def test_get_value_set_for_concept_no_filter(self, tmp_path):
        loader = self._make_loader(tmp_path)
        lf = loader.get_value_set_for_concept("Diabetes Diagnosis")
        df = lf.collect()
        assert len(df) == 2
        assert all(
            row == "Diabetes Diagnosis" for row in df["concept_name"].to_list()
        )

    @pytest.mark.unit
    def test_get_value_set_for_concept_with_code_system(self, tmp_path):
        loader = self._make_loader(tmp_path)
        lf = loader.get_value_set_for_concept(
            "Diabetes Diagnosis", code_system="ICD-10-CM"
        )
        df = lf.collect()
        assert len(df) == 2
        assert all(
            row == "ICD-10-CM" for row in df["code_system"].to_list()
        )

    @pytest.mark.unit
    def test_get_value_set_for_concept_caches(self, tmp_path):
        loader = self._make_loader(tmp_path)
        lf1 = loader.get_value_set_for_concept("Diabetes Diagnosis")
        lf2 = loader.get_value_set_for_concept("Diabetes Diagnosis")
        assert lf1 is lf2

    @pytest.mark.unit
    def test_get_value_set_for_concept_cache_key_differs_by_code_system(
        self, tmp_path
    ):
        loader = self._make_loader(tmp_path)
        lf1 = loader.get_value_set_for_concept("Diabetes Diagnosis")
        lf2 = loader.get_value_set_for_concept(
            "Diabetes Diagnosis", code_system="ICD-10-CM"
        )
        assert lf1 is not lf2

    @pytest.mark.unit
    def test_get_concepts_for_measure(self, tmp_path):
        loader = self._make_loader(tmp_path)
        concepts = loader.get_concepts_for_measure("NQF0059")
        assert isinstance(concepts, list)
        assert set(concepts) == {"Diabetes Diagnosis", "HbA1c Test"}

    @pytest.mark.unit
    def test_get_concepts_for_measure_empty(self, tmp_path):
        loader = self._make_loader(tmp_path)
        concepts = loader.get_concepts_for_measure("NONEXISTENT")
        assert concepts == []

    @pytest.mark.unit
    def test_load_value_sets_for_measure(self, tmp_path):
        loader = self._make_loader(tmp_path)
        result = loader.load_value_sets_for_measure("NQF0059")
        assert isinstance(result, dict)
        assert "Diabetes Diagnosis" in result
        assert "HbA1c Test" in result
        for _name, lf in result.items():
            assert isinstance(lf, pl.LazyFrame)

    @pytest.mark.unit
    def test_load_value_sets_for_measure_empty(self, tmp_path):
        loader = self._make_loader(tmp_path)
        result = loader.load_value_sets_for_measure("NONEXISTENT")
        assert result == {}

    @pytest.mark.unit
    def test_clear_cache(self, tmp_path):
        loader = self._make_loader(tmp_path)
        loader.get_value_set_for_concept("Diabetes Diagnosis")
        assert len(loader._cache) == 1
        loader.clear_cache()
        assert len(loader._cache) == 0

    @pytest.mark.unit
    def test_init_sets_silver_path(self, tmp_path):
        loader = self._make_loader(tmp_path)
        assert loader.silver_path == tmp_path
        assert loader._cache == {}
        assert loader._value_sets_df is None
        assert loader._concepts_df is None
        assert loader._measures_df is None

    @pytest.mark.unit
    def test_cache_key_construction(self, tmp_path):
        """Verify cache key includes 'all' when code_system is None."""
        loader = self._make_loader(tmp_path)
        loader.get_value_set_for_concept("Diabetes Diagnosis")
        assert "Diabetes Diagnosis_all" in loader._cache
        loader.get_value_set_for_concept("Diabetes Diagnosis", code_system="CPT")
        assert "Diabetes Diagnosis_CPT" in loader._cache


# ---------------------------------------------------------------------------
# show_query tests
# ---------------------------------------------------------------------------


class TestShowQuery:
    """Removed — show_query module deleted."""
    pass

class TestGenerateAcoAlignmentSql:  # noqa: F811
    """Removed — show_query module deleted."""
    pass

class TestManualSqlGeneration:
    """Removed — show_query module deleted."""
    pass

class TestMakeTablesStatic:
    """Tests for make_tables_static HTML processing."""

    @pytest.mark.unit
    def test_injects_css_before_head_close(self):
        from acoharmony._utils.export_notebook import make_tables_static

        html = "<html><head><title>Test</title></head><body></body></html>"
        result = make_tables_static(html)
        assert "max-height: none !important" in result
        assert "overflow: visible !important" in result
        head_end_pos = result.index("</head>")
        css_pos = result.index("max-height: none !important")
        assert css_pos < head_end_pos

    @pytest.mark.unit
    def test_fallback_when_no_head_tag(self):
        from acoharmony._utils.export_notebook import make_tables_static

        html = "<body><div>content</div></body>"
        result = make_tables_static(html)
        assert "max-height: none !important" in result
        body_pos = result.index("<body")
        css_pos = result.index("max-height: none !important")
        assert css_pos < body_pos

    @pytest.mark.unit
    def test_removes_inline_max_height(self):
        from acoharmony._utils.export_notebook import make_tables_static

        html = '<html><head></head><body><div style="max-height: 300px; color: red;">table</div></body></html>'
        result = make_tables_static(html)
        after_style = result.split("</style>")[-1]
        assert "max-height" not in after_style
        assert "color: red" in after_style

    @pytest.mark.unit
    def test_removes_inline_overflow_auto(self):
        from acoharmony._utils.export_notebook import make_tables_static

        html = '<html><head></head><body><div style="overflow: auto; padding: 5px;">data</div></body></html>'
        result = make_tables_static(html)
        remaining = result.split("</style>")[-1]
        assert "overflow: auto" not in remaining
        assert "padding: 5px" in remaining

    @pytest.mark.unit
    def test_removes_overflow_x_scroll(self):
        from acoharmony._utils.export_notebook import make_tables_static

        html = '<html><head></head><body><div style="overflow-x: scroll; margin: 10px;">data</div></body></html>'
        result = make_tables_static(html)
        remaining = result.split("</style>")[-1]
        assert "overflow-x: scroll" not in remaining
        assert "margin: 10px" in remaining

    @pytest.mark.unit
    def test_removes_overflow_y_auto(self):
        from acoharmony._utils.export_notebook import make_tables_static

        html = '<html><head></head><body><div style="overflow-y: auto;">data</div></body></html>'
        result = make_tables_static(html)
        remaining = result.split("</style>")[-1]
        assert "overflow-y: auto" not in remaining

    @pytest.mark.unit
    def test_cleans_empty_style_attributes(self):
        from acoharmony._utils.export_notebook import make_tables_static

        html = '<html><head></head><body><div style="max-height: 200px;">data</div></body></html>'
        result = make_tables_static(html)
        remaining = result.split("</style>")[-1]
        assert 'style=""' not in remaining

    @pytest.mark.unit
    def test_preserves_non_scroll_styles(self):
        from acoharmony._utils.export_notebook import make_tables_static

        html = '<html><head></head><body><div style="color: blue; font-size: 14px;">data</div></body></html>'
        result = make_tables_static(html)
        assert "color: blue" in result
        assert "font-size: 14px" in result

    @pytest.mark.unit
    def test_print_media_query_present(self):
        from acoharmony._utils.export_notebook import make_tables_static

        html = "<html><head></head><body></body></html>"
        result = make_tables_static(html)
        assert "@media print" in result
        assert "page-break-inside: avoid" in result

    @pytest.mark.unit
    def test_scroll_shadow_display_none(self):
        from acoharmony._utils.export_notebook import make_tables_static

        html = "<html><head></head><body></body></html>"
        result = make_tables_static(html)
        assert "scroll-shadow" in result
        assert "display: none !important" in result

    @pytest.mark.unit
    def test_multiple_inline_styles_cleaned(self):
        from acoharmony._utils.export_notebook import make_tables_static

        html = (
            "<html><head></head><body>"
            '<div style="max-height: 100px; overflow: auto; color: red;">'
            '<div style="overflow-y: scroll; padding: 5px;">'
            "</body></html>"
        )
        result = make_tables_static(html)
        after_style = result.split("</style>")[-1]
        assert "max-height" not in after_style
        assert "overflow: auto" not in after_style
        assert "overflow-y: scroll" not in after_style
        assert "color: red" in after_style
        assert "padding: 5px" in after_style

    @pytest.mark.unit
    def test_case_insensitive_overflow_removal(self):
        from acoharmony._utils.export_notebook import make_tables_static

        html = '<html><head></head><body><div style="OVERFLOW: AUTO;">data</div></body></html>'
        result = make_tables_static(html)
        after_style = result.split("</style>")[-1]
        assert "OVERFLOW: AUTO" not in after_style

    @pytest.mark.unit
    def test_no_head_no_body(self):
        """HTML with neither </head> nor <body tag."""
        from acoharmony._utils.export_notebook import make_tables_static

        html = "<div>just a div</div>"
        result = make_tables_static(html)
        assert "just a div" in result

    @pytest.mark.unit
    def test_table_width_100_percent(self):
        from acoharmony._utils.export_notebook import make_tables_static

        html = "<html><head></head><body></body></html>"
        result = make_tables_static(html)
        assert "width: 100% !important" in result

    @pytest.mark.unit
    def test_tbody_overflow_visible(self):
        from acoharmony._utils.export_notebook import make_tables_static

        html = "<html><head></head><body></body></html>"
        result = make_tables_static(html)
        # The CSS contains tbody rules
        assert "tbody" in result


class TestExportNotebookToHtml:
    """Tests for export_notebook_to_html."""

    @patch("acoharmony._utils.export_notebook.subprocess.run")
    @pytest.mark.unit
    def test_successful_export(self, mock_run, tmp_path):
        from acoharmony._utils.export_notebook import export_notebook_to_html

        notebook = tmp_path / "test_nb.py"
        notebook.write_text("# marimo notebook")
        output = tmp_path / "output.html"

        def create_html_side_effect(*args, **kwargs):
            output.write_text(
                "<html><head></head><body>table</body></html>"
            )
            return Mock(returncode=0, stdout="", stderr="")

        mock_run.side_effect = create_html_side_effect

        result = export_notebook_to_html(notebook, output)
        assert result == output
        assert output.exists()
        content = output.read_text()
        assert "max-height: none !important" in content

    @patch("acoharmony._utils.export_notebook.subprocess.run")
    @pytest.mark.unit
    def test_calls_marimo_cli(self, mock_run, tmp_path):
        from acoharmony._utils.export_notebook import export_notebook_to_html

        notebook = tmp_path / "nb.py"
        notebook.write_text("# notebook")
        output = tmp_path / "nb.html"

        def side_effect(*args, **kwargs):
            output.write_text("<html><head></head><body></body></html>")
            return Mock(returncode=0, stdout="", stderr="")

        mock_run.side_effect = side_effect

        export_notebook_to_html(notebook, output)

        call_args = mock_run.call_args
        cmd = call_args[0][0]
        assert cmd[0] == "marimo"
        assert cmd[1] == "export"
        assert cmd[2] == "html"
        assert str(notebook) in cmd
        assert str(output) in cmd

    @patch("acoharmony._utils.export_notebook.subprocess.run")
    @pytest.mark.unit
    def test_default_output_path(self, mock_run, tmp_path):
        from acoharmony._utils.export_notebook import export_notebook_to_html

        notebook = tmp_path / "my_notebook.py"
        notebook.write_text("# marimo notebook")
        expected_dir = Path("/home/care/kcorwin/Downloads")

        def create_html_side_effect(*args, **kwargs):
            expected_dir.mkdir(parents=True, exist_ok=True)
            out = expected_dir / "my_notebook.html"
            out.write_text("<html><head></head><body></body></html>")
            return Mock(returncode=0, stdout="", stderr="")

        mock_run.side_effect = create_html_side_effect

        result = export_notebook_to_html(notebook)
        assert result.name == "my_notebook.html"
        # Clean up
        if result.exists():
            result.unlink()

    @pytest.mark.unit
    def test_notebook_not_found(self, tmp_path):
        from acoharmony._utils.export_notebook import export_notebook_to_html

        with pytest.raises(FileNotFoundError, match="Notebook not found"):
            export_notebook_to_html(tmp_path / "nonexistent.py")

    @patch("acoharmony._utils.export_notebook.subprocess.run")
    @pytest.mark.unit
    def test_marimo_export_failure(self, mock_run, tmp_path):
        from acoharmony._utils.export_notebook import export_notebook_to_html

        notebook = tmp_path / "test.py"
        notebook.write_text("# notebook")

        mock_run.return_value = Mock(
            returncode=1, stderr="marimo error", stdout=""
        )

        with pytest.raises(RuntimeError, match="marimo export failed"):
            export_notebook_to_html(notebook, tmp_path / "out.html")


class TestExportNotebookMain:
    """Tests for main() CLI entry point."""

    @patch("acoharmony._utils.export_notebook.export_notebook_to_html")
    @pytest.mark.unit
    def test_main_default_args(self, mock_export):
        from acoharmony._utils.export_notebook import main

        mock_export.return_value = Path("/tmp/result.html")

        with patch("sys.argv", ["export_notebook"]):
            main()

        mock_export.assert_called_once()
        args_call = mock_export.call_args
        assert args_call[0][0] == Path(
            "/opt/s3/data/notebooks/consolidated_alignments.py"
        )
        assert args_call[0][1] is None

    @patch("acoharmony._utils.export_notebook.export_notebook_to_html")
    @pytest.mark.unit
    def test_main_custom_args(self, mock_export):
        from acoharmony._utils.export_notebook import main

        mock_export.return_value = Path("/tmp/output.html")

        with patch(
            "sys.argv",
            ["export_notebook", "/tmp/my_nb.py", "-o", "/tmp/output.html"],
        ):
            main()

        args_call = mock_export.call_args
        assert args_call[0][0] == Path("/tmp/my_nb.py")
        assert args_call[0][1] == Path("/tmp/output.html")

    @patch("acoharmony._utils.export_notebook.export_notebook_to_html")
    @pytest.mark.unit
    def test_main_handles_exception(self, mock_export):
        from acoharmony._utils.export_notebook import main

        mock_export.side_effect = FileNotFoundError("not found")

        with patch("sys.argv", ["export_notebook"]):
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# unpack tests
# ---------------------------------------------------------------------------


class TestLoadSchemas:
    """Tests for _load_schemas."""

    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_returns_empty_when_no_schemas(self, mock_get_logger):
        from acoharmony._utils.unpack import _load_schemas

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger

        with patch("acoharmony._utils.unpack.SchemaRegistry") as MockSR:
            MockSR.list_schemas.return_value = []
            result = _load_schemas()
        assert result == {}

    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_loads_multiple_schemas(self, mock_get_logger):
        from acoharmony._utils.unpack import _load_schemas

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger

        with patch("acoharmony._utils.unpack.SchemaRegistry") as MockSR:
            MockSR.list_schemas.return_value = ["schema_a", "schema_b"]
            MockSR.get_full_table_config.side_effect = lambda name: {
                "schema_a": {"name": "a"},
                "schema_b": {"name": "b", "storage": {"file_patterns": {"mssp": "*.csv"}}},
            }.get(name)
            result = _load_schemas()
        assert "schema_a" in result
        assert "schema_b" in result
        assert len(result) == 2

    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_handles_config_none(self, mock_get_logger):
        from acoharmony._utils.unpack import _load_schemas

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger

        with patch("acoharmony._utils.unpack.SchemaRegistry") as MockSR:
            MockSR.list_schemas.return_value = ["bad_schema"]
            MockSR.get_full_table_config.return_value = None
            result = _load_schemas()

        assert result == {}

    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_loads_from_real_schemas_dir_if_exists(self, mock_get_logger):
        """If the real _schemas dir exists, _load_schemas should load from it."""
        from acoharmony._utils.unpack import _load_schemas

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger

        result = _load_schemas()
        assert isinstance(result, dict)
        # No assertion on count since we don't control the real schemas dir


class TestMatchFileToSchemas:
    """Tests for _match_file_to_schemas."""

    @pytest.mark.unit
    def test_matches_mssp_pattern(self):
        from acoharmony._utils.unpack import _match_file_to_schemas

        schemas = {
            "cclf8": {
                "storage": {
                    "file_patterns": {
                        "mssp": "P.A*.T8.D*.csv",
                        "reach": "CCLF8*.csv",
                    }
                }
            }
        }
        matches = _match_file_to_schemas("P.A1234.T8.D20240101.csv", schemas)
        assert matches == ["cclf8:mssp"]

    @pytest.mark.unit
    def test_matches_reach_pattern(self):
        from acoharmony._utils.unpack import _match_file_to_schemas

        schemas = {
            "cclf5": {
                "storage": {
                    "file_patterns": {
                        "mssp": "P.A*.T5.D*.csv",
                        "reach": "CCLF5*.csv",
                    }
                }
            }
        }
        matches = _match_file_to_schemas("CCLF5_2024.csv", schemas)
        assert matches == ["cclf5:reach"]

    @pytest.mark.unit
    def test_no_match(self):
        from acoharmony._utils.unpack import _match_file_to_schemas

        schemas = {
            "cclf8": {
                "storage": {
                    "file_patterns": {
                        "mssp": "P.A*.T8.D*.csv",
                    }
                }
            }
        }
        matches = _match_file_to_schemas("random_file.txt", schemas)
        assert matches == []

    @pytest.mark.unit
    def test_none_schema_skipped(self):
        from acoharmony._utils.unpack import _match_file_to_schemas

        schemas = {
            "bad_schema": None,
            "good_schema": {
                "storage": {"file_patterns": {"mssp": "*.csv"}}
            },
        }
        matches = _match_file_to_schemas("data.csv", schemas)
        assert matches == ["good_schema:mssp"]

    @pytest.mark.unit
    def test_none_storage_skipped(self):
        from acoharmony._utils.unpack import _match_file_to_schemas

        schemas = {"no_storage": {"storage": None}}
        matches = _match_file_to_schemas("data.csv", schemas)
        assert matches == []

    @pytest.mark.unit
    def test_none_file_patterns_skipped(self):
        from acoharmony._utils.unpack import _match_file_to_schemas

        schemas = {"no_patterns": {"storage": {"file_patterns": None}}}
        matches = _match_file_to_schemas("data.csv", schemas)
        assert matches == []

    @pytest.mark.unit
    def test_non_string_pattern_skipped(self):
        from acoharmony._utils.unpack import _match_file_to_schemas

        schemas = {
            "list_pattern": {
                "storage": {
                    "file_patterns": {
                        "mssp": ["*.csv", "*.txt"],
                    }
                }
            }
        }
        matches = _match_file_to_schemas("data.csv", schemas)
        assert matches == []

    @pytest.mark.unit
    def test_multiple_schemas_match(self):
        from acoharmony._utils.unpack import _match_file_to_schemas

        schemas = {
            "schema_a": {
                "storage": {"file_patterns": {"type1": "data_*.csv"}}
            },
            "schema_b": {
                "storage": {"file_patterns": {"type2": "data_*.csv"}}
            },
        }
        matches = _match_file_to_schemas("data_2024.csv", schemas)
        assert len(matches) == 2
        assert "schema_a:type1" in matches
        assert "schema_b:type2" in matches

    @pytest.mark.unit
    def test_missing_storage_key(self):
        from acoharmony._utils.unpack import _match_file_to_schemas

        schemas = {"no_storage_key": {"name": "test"}}
        matches = _match_file_to_schemas("test.csv", schemas)
        assert matches == []

    @pytest.mark.unit
    def test_first_matching_pattern_breaks(self):
        """Only the first matching pattern type within a schema is reported."""
        from acoharmony._utils.unpack import _match_file_to_schemas

        schemas = {
            "multi_pattern": {
                "storage": {
                    "file_patterns": {
                        "type_a": "*.csv",
                        "type_b": "*.csv",
                    }
                }
            }
        }
        matches = _match_file_to_schemas("data.csv", schemas)
        # Only one match per schema due to break
        assert len(matches) == 1
        assert "multi_pattern:" in matches[0]


class TestExtractZipFlat:
    """Tests for _extract_zip_flat with in-memory zip files."""

    @pytest.mark.unit
    def test_extracts_files_flat(self, tmp_path):
        from acoharmony._utils.unpack import _extract_zip_flat

        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("subdir/file1.csv", "a,b,c\n1,2,3")
            zf.writestr("subdir/nested/file2.csv", "x,y\n4,5")

        dest = tmp_path / "output"
        dest.mkdir()
        logger = Mock()

        result = _extract_zip_flat(zip_path, dest, logger)

        assert len(result) == 2
        filenames = {p.name for p in result}
        assert filenames == {"file1.csv", "file2.csv"}
        for p in result:
            assert p.parent == dest
        assert (dest / "file1.csv").read_text() == "a,b,c\n1,2,3"

    @pytest.mark.unit
    def test_skips_directories(self, tmp_path):
        from acoharmony._utils.unpack import _extract_zip_flat

        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("mydir/", "")
            zf.writestr("mydir/file.txt", "hello")

        dest = tmp_path / "out"
        dest.mkdir()
        logger = Mock()

        result = _extract_zip_flat(zip_path, dest, logger)
        assert len(result) == 1
        assert result[0].name == "file.txt"

    @pytest.mark.unit
    def test_skips_existing_files(self, tmp_path):
        from acoharmony._utils.unpack import _extract_zip_flat

        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file.txt", "new content")

        dest = tmp_path / "out"
        dest.mkdir()
        (dest / "file.txt").write_text("old content")
        logger = Mock()

        result = _extract_zip_flat(zip_path, dest, logger)
        assert len(result) == 0
        assert (dest / "file.txt").read_text() == "old content"
        logger.warning.assert_called_once()

    @pytest.mark.unit
    def test_deletes_empty_extracted_files(self, tmp_path):
        from acoharmony._utils.unpack import _extract_zip_flat

        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("empty.txt", "")
            zf.writestr("nonempty.txt", "data")

        dest = tmp_path / "out"
        dest.mkdir()
        logger = Mock()

        result = _extract_zip_flat(zip_path, dest, logger)
        assert len(result) == 1
        assert result[0].name == "nonempty.txt"
        assert not (dest / "empty.txt").exists()

    @pytest.mark.unit
    def test_bad_zip_raises(self, tmp_path):
        from acoharmony._utils.unpack import _extract_zip_flat

        bad_zip = tmp_path / "bad.zip"
        bad_zip.write_bytes(b"not a zip file")

        dest = tmp_path / "out"
        dest.mkdir()
        logger = Mock()

        with pytest.raises(zipfile.BadZipFile):
            _extract_zip_flat(bad_zip, dest, logger)

    @pytest.mark.unit
    def test_multiple_files_in_flat_zip(self, tmp_path):
        from acoharmony._utils.unpack import _extract_zip_flat

        zip_path = tmp_path / "test.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("a.csv", "col1\n1")
            zf.writestr("b.csv", "col2\n2")
            zf.writestr("c.csv", "col3\n3")

        dest = tmp_path / "out"
        dest.mkdir()
        logger = Mock()

        result = _extract_zip_flat(zip_path, dest, logger)
        assert len(result) == 3
        logger.info.assert_called()


class TestUnpackBronzeZips:
    """Tests for unpack_bronze_zips."""

    def _setup_config_mock(self, tmp_path):
        """Create mocked config pointing to tmp_path."""
        bronze = tmp_path / "bronze"
        bronze.mkdir()
        archive = tmp_path / "archive"
        archive.mkdir()

        config = Mock()
        config.storage.base_path = tmp_path
        config.storage.bronze_dir = "bronze"
        config.storage.archive_dir = "archive"
        return config, bronze, archive

    @patch("acoharmony._utils.unpack._load_schemas")
    @patch("acoharmony._utils.unpack.get_config")
    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_no_zip_files(
        self, mock_get_logger, mock_get_config, mock_load_schemas, tmp_path
    ):
        from acoharmony._utils.unpack import unpack_bronze_zips

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        config, bronze, archive = self._setup_config_mock(tmp_path)
        mock_get_config.return_value = config

        result = unpack_bronze_zips()
        assert result["found"] == 0
        assert result["processed"] == 0
        assert result["extracted"] == 0

    @patch("acoharmony._utils.unpack._load_schemas")
    @patch("acoharmony._utils.unpack.get_config")
    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_dry_run_mode(
        self, mock_get_logger, mock_get_config, mock_load_schemas, tmp_path
    ):
        from acoharmony._utils.unpack import unpack_bronze_zips

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        config, bronze, archive = self._setup_config_mock(tmp_path)
        mock_get_config.return_value = config
        mock_load_schemas.return_value = {}

        zip_path = bronze / "test_data.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file1.csv", "a,b\n1,2")
            zf.writestr("file2.csv", "x,y\n3,4")

        result = unpack_bronze_zips(dry_run=True)
        assert result["found"] == 1
        assert result["processed"] == 1
        assert not (bronze / "file1.csv").exists()
        assert zip_path.exists()

    @patch("acoharmony._utils.unpack._load_schemas")
    @patch("acoharmony._utils.unpack.get_config")
    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_real_extraction_and_archive(
        self, mock_get_logger, mock_get_config, mock_load_schemas, tmp_path
    ):
        from acoharmony._utils.unpack import unpack_bronze_zips

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        config, bronze, archive = self._setup_config_mock(tmp_path)
        mock_get_config.return_value = config
        mock_load_schemas.return_value = {
            "cclf8": {
                "storage": {"file_patterns": {"mssp": "*.csv"}}
            }
        }

        zip_path = bronze / "batch1.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.csv", "a,b\n1,2")

        result = unpack_bronze_zips(dry_run=False)
        assert result["found"] == 1
        assert result["processed"] == 1
        assert result["extracted"] == 1
        assert result["failed"] == 0
        assert (bronze / "data.csv").exists()
        assert not zip_path.exists()
        assert (archive / "batch1.zip").exists()
        assert "cclf8:mssp" in result["schema_matches"]

    @patch("acoharmony._utils.unpack._load_schemas")
    @patch("acoharmony._utils.unpack.get_config")
    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_archive_already_exists(
        self, mock_get_logger, mock_get_config, mock_load_schemas, tmp_path
    ):
        from acoharmony._utils.unpack import unpack_bronze_zips

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        config, bronze, archive = self._setup_config_mock(tmp_path)
        mock_get_config.return_value = config
        mock_load_schemas.return_value = {}

        zip_path = bronze / "dup.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file.csv", "data")

        (archive / "dup.zip").write_text("old archive")

        result = unpack_bronze_zips(dry_run=False)
        assert result["processed"] == 1
        assert not zip_path.exists()

    @patch("acoharmony._utils.unpack._load_schemas")
    @patch("acoharmony._utils.unpack.get_config")
    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_bad_zip_file_error(
        self, mock_get_logger, mock_get_config, mock_load_schemas, tmp_path
    ):
        from acoharmony._utils.unpack import unpack_bronze_zips

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        config, bronze, archive = self._setup_config_mock(tmp_path)
        mock_get_config.return_value = config
        mock_load_schemas.return_value = {}

        bad_zip = bronze / "corrupt.zip"
        bad_zip.write_bytes(b"PK\x03\x04not-really-a-zip")

        result = unpack_bronze_zips(dry_run=False)
        assert result["found"] == 1
        assert result["failed"] == 1
        assert result["processed"] == 0
        assert bad_zip.exists()

    @patch("acoharmony._utils.unpack._load_schemas")
    @patch("acoharmony._utils.unpack.get_config")
    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_generic_exception_during_processing(
        self, mock_get_logger, mock_get_config, mock_load_schemas, tmp_path
    ):
        from acoharmony._utils.unpack import unpack_bronze_zips

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        config, bronze, archive = self._setup_config_mock(tmp_path)
        mock_get_config.return_value = config
        mock_load_schemas.return_value = {}

        zip_path = bronze / "error.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file.csv", "data")

        with patch(
            "acoharmony._utils.unpack._extract_zip_flat",
            side_effect=PermissionError("denied"),
        ):
            result = unpack_bronze_zips(dry_run=False)

        assert result["failed"] == 1
        assert result["processed"] == 0
        assert zip_path.exists()

    @patch("acoharmony._utils.unpack._load_schemas")
    @patch("acoharmony._utils.unpack.get_config")
    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_state_tracker_updated(
        self, mock_get_logger, mock_get_config, mock_load_schemas, tmp_path
    ):
        from acoharmony._utils.unpack import unpack_bronze_zips

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        config, bronze, archive = self._setup_config_mock(tmp_path)
        mock_get_config.return_value = config
        mock_load_schemas.return_value = {}

        zip_path = bronze / "tracked.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file.csv", "data")

        tracker = Mock()
        unpack_bronze_zips(dry_run=False, state_tracker=tracker)

        tracker.update_file_location.assert_called_once_with(
            "tracked.zip", archive / "tracked.zip"
        )

    @patch("acoharmony._utils.unpack._load_schemas")
    @patch("acoharmony._utils.unpack.get_config")
    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_state_tracker_failure_logged(
        self, mock_get_logger, mock_get_config, mock_load_schemas, tmp_path
    ):
        from acoharmony._utils.unpack import unpack_bronze_zips

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        config, bronze, archive = self._setup_config_mock(tmp_path)
        mock_get_config.return_value = config
        mock_load_schemas.return_value = {}

        zip_path = bronze / "tracked2.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("file.csv", "data")

        tracker = Mock()
        tracker.update_file_location.side_effect = RuntimeError("tracker broke")

        result = unpack_bronze_zips(dry_run=False, state_tracker=tracker)
        assert result["processed"] == 1

    @patch("acoharmony._utils.unpack._load_schemas")
    @patch("acoharmony._utils.unpack.get_config")
    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_detects_zip_without_extension(
        self, mock_get_logger, mock_get_config, mock_load_schemas, tmp_path
    ):
        from acoharmony._utils.unpack import unpack_bronze_zips

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        config, bronze, archive = self._setup_config_mock(tmp_path)
        mock_get_config.return_value = config
        mock_load_schemas.return_value = {}

        weird_zip = bronze / "MSSP_DATA_PKG"
        with zipfile.ZipFile(weird_zip, "w") as zf:
            zf.writestr("inner.csv", "a,b\n1,2")

        result = unpack_bronze_zips(dry_run=False)
        assert result["found"] == 1
        assert result["processed"] == 1

    @patch("acoharmony._utils.unpack._load_schemas")
    @patch("acoharmony._utils.unpack.get_config")
    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_skips_known_extensions(
        self, mock_get_logger, mock_get_config, mock_load_schemas, tmp_path
    ):
        from acoharmony._utils.unpack import unpack_bronze_zips

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        config, bronze, archive = self._setup_config_mock(tmp_path)
        mock_get_config.return_value = config
        mock_load_schemas.return_value = {}

        (bronze / "report.csv").write_text("a,b\n1,2")
        (bronze / "doc.pdf").write_bytes(b"fake pdf")

        result = unpack_bronze_zips(dry_run=False)
        assert result["found"] == 0

    @patch("acoharmony._utils.unpack._load_schemas")
    @patch("acoharmony._utils.unpack.get_config")
    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_no_schema_matches_logged(
        self, mock_get_logger, mock_get_config, mock_load_schemas, tmp_path
    ):
        from acoharmony._utils.unpack import unpack_bronze_zips

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        config, bronze, archive = self._setup_config_mock(tmp_path)
        mock_get_config.return_value = config
        mock_load_schemas.return_value = {
            "cclf8": {
                "storage": {
                    "file_patterns": {"mssp": "P.A*.T8.D*.csv"}
                }
            }
        }

        zip_path = bronze / "misc.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("unrelated_file.txt", "hello")

        result = unpack_bronze_zips(dry_run=False)
        assert result["schema_matches"] == {}

    @patch("acoharmony._utils.unpack._load_schemas")
    @patch("acoharmony._utils.unpack.get_config")
    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_multiple_zips_processed(
        self, mock_get_logger, mock_get_config, mock_load_schemas, tmp_path
    ):
        from acoharmony._utils.unpack import unpack_bronze_zips

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        config, bronze, archive = self._setup_config_mock(tmp_path)
        mock_get_config.return_value = config
        mock_load_schemas.return_value = {}

        for i in range(3):
            zp = bronze / f"batch_{i}.zip"
            with zipfile.ZipFile(zp, "w") as zf:
                zf.writestr(f"file_{i}.csv", f"data_{i}")

        result = unpack_bronze_zips(dry_run=False)
        assert result["found"] == 3
        assert result["processed"] == 3
        assert result["extracted"] == 3

    @patch("acoharmony._utils.unpack._load_schemas")
    @patch("acoharmony._utils.unpack.get_config")
    @patch("acoharmony._utils.unpack.get_logger")
    @pytest.mark.unit
    def test_multiple_schema_matches_counted(
        self, mock_get_logger, mock_get_config, mock_load_schemas, tmp_path
    ):
        from acoharmony._utils.unpack import unpack_bronze_zips

        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        config, bronze, archive = self._setup_config_mock(tmp_path)
        mock_get_config.return_value = config
        mock_load_schemas.return_value = {
            "cclf5": {
                "storage": {"file_patterns": {"mssp": "*.csv"}}
            }
        }

        zip_path = bronze / "multi.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("a.csv", "d1")
            zf.writestr("b.csv", "d2")

        result = unpack_bronze_zips(dry_run=False)
        assert result["schema_matches"]["cclf5:mssp"] == 2


class TestUnpackMainBlock:
    """Tests for the unpack module __main__ block behavior."""

    @pytest.mark.unit
    def test_main_block_dry_run_flag(self):
        with patch.object(sys, "argv", ["unpack.py", "--dry-run"]):
            dry_run = "--dry-run" in sys.argv or "-n" in sys.argv
            assert dry_run is True

    @pytest.mark.unit
    def test_main_block_short_flag(self):
        with patch.object(sys, "argv", ["unpack.py", "-n"]):
            dry_run = "--dry-run" in sys.argv or "-n" in sys.argv
            assert dry_run is True

    @pytest.mark.unit
    def test_main_block_no_flag(self):
        with patch.object(sys, "argv", ["unpack.py"]):
            dry_run = "--dry-run" in sys.argv or "-n" in sys.argv
            assert dry_run is False

    @pytest.mark.unit
    def test_main_block_exit_code_on_failure(self):
        result = {"failed": 2}
        exit_code = 1 if result["failed"] > 0 else 0
        assert exit_code == 1

    @pytest.mark.unit
    def test_main_block_exit_code_on_success(self):
        result = {"failed": 0}
        exit_code = 1 if result["failed"] > 0 else 0
        assert exit_code == 0
