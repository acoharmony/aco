"""Tests for acoharmony._dev.analysis.schemas module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._dev.analysis.schemas is not None


class TestGenerateSchemaTemplateUnknownFileType:
    """Test generate_schema_template with a file_type that is not excel/csv/delimited."""

    @pytest.mark.unit
    def test_unknown_file_type_skips_format_details_uses_else_columns(self):
        """file_type='parquet' falls through all elif blocks (247->253)
        and uses the else branch for column generation (268)."""
        from acoharmony._dev.analysis.schemas import generate_schema_template

        metadata = {
            "file_path": "/data/test.parquet",
            "file_type": "parquet",
            "columns": ["id", "value"],
            "dtypes": {"id": "Int64", "value": "Utf8"},
        }
        result = generate_schema_template(metadata, schema_name="test_parquet")
        assert result["name"] == "test_parquet"
        assert result["file_format"]["type"] == "parquet"
        # No delimiter/encoding keys added (skipped format-specific blocks)
        assert "delimiter" not in result["file_format"]
        assert "encoding" not in result["file_format"]
        # Columns generated via else branch
        assert len(result["columns"]) == 2
        assert result["columns"][0]["name"] == "id"
        assert result["columns"][1]["output_name"] == "value"

    @pytest.mark.unit
    def test_excel_with_empty_sheets_produces_no_columns(self):
        """Excel metadata with empty sheets dict skips column generation (258->279)."""
        from acoharmony._dev.analysis.schemas import generate_schema_template

        metadata = {
            "file_path": "/data/test.xlsx",
            "file_type": "excel",
            "sheets": {},
        }
        result = generate_schema_template(metadata, schema_name="empty_excel")
        assert result["name"] == "empty_excel"
        assert result["file_format"]["type"] == "excel"
        assert result["columns"] == []
