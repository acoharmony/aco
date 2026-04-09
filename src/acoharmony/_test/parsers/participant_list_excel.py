"""Unit tests for Participant List Excel parser coverage gaps."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path

import pytest
try:
    import openpyxl
except ImportError:
    openpyxl = None
import acoharmony

from .conftest import _schema


class TestParticipantListExcelCoverage:
    """Coverage tests for Participant List Excel parser."""

    @pytest.mark.unit
    def test_column_renaming_within_bounds(self, tmp_path: Path) -> None:
        """Test column renaming when index is within dataframe column count."""


        # Create Excel file with 27 columns (HarmonyCares format)
        excel_file = tmp_path / "D0259 Provider List - 01-15-2024 12.00.00.xlsx"
        wb = openpyxl.Workbook()
        ws = wb.active

        # Add header and data rows with 27 columns
        headers = [f"col_{i}" for i in range(27)]
        ws.append(headers)
        ws.append([f"val_0_{i}" for i in range(27)])
        ws.append([f"val_1_{i}" for i in range(27)])

        wb.save(excel_file)

        # Mock schema with 27 output names
        class MockSchema:
            file_format = {"header_row": 0}
            columns = [{"output_name": f"output_{i}"} for i in range(27)]

        result = parse_participant_list_excel(excel_file, MockSchema())
        df = result.collect()

        # Should rename columns successfully
        assert "output_0" in df.columns
        assert len(df) == 2

    @pytest.mark.unit
    def test_adding_missing_schema_columns(self, tmp_path: Path) -> None:
        """Test adding missing columns from schema with null values."""


        # Create Excel with 27 columns (HarmonyCares format)
        excel_file = tmp_path / "D0259 Provider List - 01-15-2024 12.00.00.xlsx"
        wb = openpyxl.Workbook()
        ws = wb.active

        # Add header and data rows with 27 columns
        headers = [f"col_{i}" for i in range(27)]
        ws.append(headers)
        ws.append([f"val_{i}" for i in range(27)])

        wb.save(excel_file)

        # Schema with 27 output names
        class MockSchema:
            file_format = {"header_row": 0}
            columns = [{"output_name": f"output_{i}"} for i in range(27)]

        result = parse_participant_list_excel(excel_file, MockSchema())
        df = result.collect()

        # Should process all columns
        assert "output_0" in df.columns
        assert "output_26" in df.columns
        assert len(df) == 1


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._participant_list_excel is not None

class TestParticipantListExcel:
    """Tests for acoharmony._parsers._participant_list_excel."""

    @pytest.mark.unit
    def test_parse_participant_list_excel_51_cols(self, tmp_path):
        from acoharmony._parsers._participant_list_excel import parse_participant_list_excel

        data = {f"col_{i}": [f"val_{i}"] for i in range(51)}
        p = tmp_path / "reach.xlsx"
        pl.DataFrame(data).write_excel(p)
        output_names = [f"output_{i}" for i in range(51)]
        schema = _schema([{"output_name": name} for name in output_names])
        df = parse_participant_list_excel(p, schema).collect()
        assert df.columns == output_names

    @pytest.mark.unit
    def test_parse_participant_list_excel_27_cols(self, tmp_path):
        from acoharmony._parsers._participant_list_excel import parse_participant_list_excel

        data = {f"col_{i}": [f"val_{i}"] for i in range(27)}
        p = tmp_path / "D0259 Provider List - 1-30-2026 15.27.44.xlsx"
        pl.DataFrame(data).write_excel(p)
        output_names = [f"output_{i}" for i in range(51)]
        schema = _schema([{"output_name": name} for name in output_names])
        df = parse_participant_list_excel(p, schema).collect()
        assert "entity_id" in df.columns
        assert "performance_year" in df.columns

    @pytest.mark.unit
    def test_parse_participant_list_excel_wrong_cols_raises(self, tmp_path):
        from acoharmony._parsers._participant_list_excel import parse_participant_list_excel

        data = {f"col_{i}": [f"val_{i}"] for i in range(10)}
        p = tmp_path / "unknown.xlsx"
        pl.DataFrame(data).write_excel(p)
        schema = _schema([{"output_name": f"o_{i}"} for i in range(10)])
        with pytest.raises(ValueError, match="Unknown participant list format"):
            parse_participant_list_excel(p, schema)

    @pytest.mark.unit
    def test_parse_participant_list_excel_27_no_date(self, tmp_path):
        from acoharmony._parsers._participant_list_excel import parse_participant_list_excel

        data = {f"col_{i}": [f"val_{i}"] for i in range(27)}
        p = tmp_path / "D0259 Provider List.xlsx"
        pl.DataFrame(data).write_excel(p)
        output_names = [f"output_{i}" for i in range(51)]
        schema = _schema([{"output_name": name} for name in output_names])
        df = parse_participant_list_excel(p, schema).collect()
        assert "entity_id" in df.columns

    @pytest.mark.unit
    def test_parse_participant_list_excel_limit(self, tmp_path):
        from acoharmony._parsers._participant_list_excel import parse_participant_list_excel

        data = {f"col_{i}": [f"val_{i}_{r}" for r in range(10)] for i in range(51)}
        p = tmp_path / "reach.xlsx"
        pl.DataFrame(data).write_excel(p)
        output_names = [f"output_{i}" for i in range(51)]
        schema = _schema([{"output_name": name} for name in output_names])
        df = parse_participant_list_excel(p, schema, limit=3).collect()
        assert df.height == 3

    @pytest.mark.unit
    def test_normalize_reach_no_schema_raises(self, tmp_path):
        from acoharmony._parsers._participant_list_excel import _normalize_reach_format

        df = pl.DataFrame({f"col_{i}": ["val"] for i in range(51)})
        with pytest.raises(ValueError, match="Schema is required"):
            _normalize_reach_format(df, None)

    @pytest.mark.unit
    def test_normalize_harmonycares_no_schema_raises(self, tmp_path):
        from acoharmony._parsers._participant_list_excel import _normalize_harmonycares_format

        df = pl.DataFrame({f"col_{i}": ["val"] for i in range(27)})
        with pytest.raises(ValueError, match="Schema is required"):
            _normalize_harmonycares_format(df, None, Path("test.xlsx"))

    @pytest.mark.unit
    def test_normalize_reach_extra_columns(self, tmp_path):
        """Test that extra columns beyond schema are kept with original name."""
        from acoharmony._parsers._participant_list_excel import _normalize_reach_format

        data = {f"col_{i}": ["val"] for i in range(53)}
        df = pl.DataFrame(data)
        output_names = [f"output_{i}" for i in range(51)]
        schema = _schema([{"output_name": name} for name in output_names])
        result = _normalize_reach_format(df, schema)
        assert "output_0" in result.columns
        assert "col_51" in result.columns

    @pytest.mark.unit
    def test_harmonycares_rename_loop_exceeds_columns(self):
        """Branch 141->140: mapping entries exceed df column count, so the
        if-check is False and the loop continues to the next iteration."""
        from acoharmony._parsers._participant_list_excel import _normalize_harmonycares_format

        # Only 2 columns, but harmonycares_mapping has 27 entries.
        # Iterations i >= 2 will skip the rename (if i < len(df.columns) is False).
        df = pl.DataFrame({"a": ["x"], "b": ["y"]})
        output_names = [f"output_{i}" for i in range(51)]
        schema = _schema([{"output_name": name} for name in output_names])
        result = _normalize_harmonycares_format(df, schema, Path("D0259 Provider List - 1-30-2026 15.27.44.xlsx"))
        # First 2 columns renamed per mapping: provider_type, provider_class
        assert "provider_type" in result.columns
        assert "provider_class" in result.columns
        # Column "a" and "b" no longer present (renamed)
        assert "a" not in result.columns

    @pytest.mark.unit
    def test_missing_schema_columns_skips_existing(self):
        """Branch 176->175: schema output_name already in df columns, so
        the if-check is False and the loop continues to the next iteration."""
        from acoharmony._parsers._participant_list_excel import _normalize_harmonycares_format

        # 27 columns so harmonycares path works; use column names that will
        # become the harmonycares mapping output names after rename.
        df = pl.DataFrame({f"col_{i}": ["val"] for i in range(27)})
        # Schema with output_names that include one already present after rename
        # After rename, the df will have "provider_type" (from col 0).
        # Put "provider_type" in the schema so the loop hits an existing column.
        schema_cols = [{"output_name": "provider_type"}] + [
            {"output_name": f"extra_{i}"} for i in range(50)
        ]
        schema = _schema(schema_cols)
        result = _normalize_harmonycares_format(df, schema, Path("D0259 Provider List - 1-30-2026 15.27.44.xlsx"))
        # "provider_type" existed after rename, so it should not be overwritten with null
        assert result["provider_type"][0] == "val"
        # "extra_0" should be added as null since it was missing
        assert result["extra_0"][0] is None

