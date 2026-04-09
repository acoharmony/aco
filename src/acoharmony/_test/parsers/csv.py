# © 2025 HarmonyCares
# All rights reserved.

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from pathlib import Path
from io import StringIO
from typing import TYPE_CHECKING
from types import SimpleNamespace
import tempfile

import polars as pl
import pytest

import acoharmony

from .conftest import create_mock_metadata, _schema, _schema_with_file_format

"""Tests for acoharmony._parsers._csv module."""


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._csv is not None


if TYPE_CHECKING:
    pass


class TestCsvParser:
    """Tests for CSV parsing."""

    @pytest.mark.unit
    def test_parse_csv_basic(self) -> None:
        """Parse basic CSV data."""
        csv_data = "id,name,value\n1,Alice,100\n2,Bob,200\n"

        df = pl.read_csv(StringIO(csv_data))

        assert len(df) == 2
        assert list(df.columns) == ["id", "name", "value"]
        assert df["id"].to_list() == [1, 2]

    @pytest.mark.unit
    def test_parse_csv_with_nulls(self) -> None:
        """Parse CSV with null values."""
        csv_data = "id,name,value\n1,Alice,\n2,,200\n"

        df = pl.read_csv(StringIO(csv_data))

        assert len(df) == 2
        assert df["value"].null_count() == 1

    @pytest.mark.unit
    def test_parse_csv_with_headers(self) -> None:
        """Parse CSV respects column headers."""
        csv_data = "custom_id,custom_name\n1,test\n2,data\n"

        df = pl.read_csv(StringIO(csv_data))

        assert "custom_id" in df.columns
        assert "custom_name" in df.columns
        assert len(df) == 2


class TestParseCSV:
    """Test parse_csv function."""

    @pytest.mark.unit
    def test_parse_csv_basic(self):
        """Test basic CSV parsing."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("id,name,value\n")
            f.write("1,test,100\n")
            f.write("2,data,200\n")
            csv_path = Path(f.name)
        try:
            mock_schema = create_mock_metadata(
                "test",
                columns=[
                    {"name": "id", "output_name": "id", "data_type": "integer"},
                    {"name": "name", "output_name": "name", "data_type": "string"},
                    {"name": "value", "output_name": "value", "data_type": "integer"},
                ],
                file_format={"type": "csv"},
            )
            lf = parse_csv(csv_path, mock_schema)
            result = lf.collect()
            assert len(result) == 2
            assert "id" in result.columns
            assert "name" in result.columns
            assert "value" in result.columns
        finally:
            csv_path.unlink()

    @pytest.mark.unit
    def test_parse_csv_with_limit(self):
        """Test CSV parsing with row limit."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("col1,col2\n")
            for i in range(100):
                f.write(f"{i},value{i}\n")
            csv_path = Path(f.name)
        try:
            mock_schema = create_mock_metadata("test", [], {"type": "csv"})
            lf = parse_csv(csv_path, mock_schema, limit=10)
            result = lf.collect()
            assert len(result) == 10
        finally:
            csv_path.unlink()


class TestCsvCoverageGaps:
    """Additional CSV tests to cover branch gaps."""

    @pytest.mark.unit
    def test_csv_no_schema(self, tmp_path):
        """Cover schema=None path (no rename, no cast)."""
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("a,b\n1,2\n")
        lf = parse_csv(p, None)
        df = lf.collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_csv_empty_columns(self, tmp_path):
        """Cover schema with empty columns list (cast_exprs and rename_map empty)."""
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("a,b\n1,2\n")
        schema = _schema([])
        lf = parse_csv(p, schema)
        df = lf.collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_csv_integer_alias(self, tmp_path):
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("h1\n42\n")
        schema = _schema([{"name": "x", "output_name": "x", "data_type": "integer"}])
        df = parse_csv(p, schema).collect()
        assert df["x"].dtype == pl.Int64

    @pytest.mark.unit
    def test_csv_float64_alias(self, tmp_path):
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("h1\n1.5\n")
        schema = _schema([{"name": "x", "output_name": "x", "data_type": "float64"}])
        df = parse_csv(p, schema).collect()
        assert df["x"].dtype == pl.Float64


class TestCsv:
    """Tests for acoharmony._parsers._csv.parse_csv."""

    @pytest.mark.unit
    def test_parse_csv_with_schema(self, tmp_path):
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("h1,h2,h3\n1,2.5,hello\n3,4.5,world\n")
        schema = _schema(
            [
                {"name": "col_a", "output_name": "col_a", "data_type": "int"},
                {"name": "col_b", "output_name": "col_b", "data_type": "float"},
                {"name": "col_c", "output_name": "col_c", "data_type": "string"},
            ]
        )
        lf = parse_csv(p, schema)
        df = lf.collect()
        assert df.columns == ["col_a", "col_b", "col_c"]
        assert df["col_a"].dtype == pl.Int64
        assert df["col_b"].dtype == pl.Float64
        assert df["col_c"].dtype == pl.Utf8

    @pytest.mark.unit
    def test_parse_csv_with_limit(self, tmp_path):
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("h1,h2\na,b\nc,d\ne,f\n")
        schema = _schema(
            [
                {"name": "x", "output_name": "x", "data_type": "string"},
                {"name": "y", "output_name": "y", "data_type": "string"},
            ]
        )
        df = parse_csv(p, schema, limit=1).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_parse_csv_date_type(self, tmp_path):
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("dt\n2024-01-01\n2024-02-15\n")
        schema = _schema([{"name": "dt", "output_name": "dt", "data_type": "date"}])
        df = parse_csv(p, schema).collect()
        assert df["dt"].dtype == pl.Utf8

    @pytest.mark.unit
    def test_parse_csv_boolean_type(self, tmp_path):
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("flag\ntrue\nfalse\n1\n0\nyes\nno\n")
        schema = _schema([{"name": "flag", "output_name": "flag", "data_type": "boolean"}])
        df = parse_csv(p, schema).collect()
        assert df["flag"].to_list() == [True, False, True, False, True, False]

    @pytest.mark.unit
    def test_parse_csv_more_schema_cols_than_file(self, tmp_path):
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("h1\na\n")
        schema = _schema(
            [
                {"name": "x", "output_name": "x", "data_type": "string"},
                {"name": "y", "output_name": "y", "data_type": "string"},
            ]
        )
        df = parse_csv(p, schema).collect()
        assert df.columns == ["x"]

    @pytest.mark.unit
    def test_parse_csv_no_schema_columns(self, tmp_path):
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("h1,h2\na,b\n")
        schema = SimpleNamespace()
        lf = parse_csv(p, schema)
        df = lf.collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_parse_csv_decimal_type(self, tmp_path):
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("val\n1.5\n2.5\n")
        schema = _schema([{"name": "val", "output_name": "val", "data_type": "decimal"}])
        df = parse_csv(p, schema).collect()
        assert df["val"].dtype == pl.Float64

    @pytest.mark.unit
    def test_parse_csv_name_fallback(self, tmp_path):
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("h1\nval\n")
        schema = _schema([{"name": "col_name"}])
        df = parse_csv(p, schema).collect()
        assert df.columns == ["col_name"]


class TestCsvParseBranches:
    """Cover branches 177-238 in parse_csv."""

    @pytest.mark.unit
    def test_no_schema_columns(self, tmp_path):
        """Branch 177->238: schema has no columns attr, returns raw."""
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("a,b\n1,2\n")
        df = parse_csv(p, schema=None).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_schema_more_cols_than_file(self, tmp_path):
        """Branch 182->183, 183->185: schema has more columns than file."""
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("h1\nval\n")
        schema = _schema([
            {"name": "col1"},
            {"name": "col2"},
            {"name": "col3"},  # More than file has
        ])
        df = parse_csv(p, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_schema_exact_cols(self, tmp_path):
        """Branch 183->188: schema columns match file columns."""
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("h1,h2\nval1,val2\n")
        schema = _schema([
            {"name": "col1"},
            {"name": "col2"},
        ])
        df = parse_csv(p, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_date_type_column(self, tmp_path):
        """Branch 197->198: data_type is 'date'."""
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("dt\n2024-01-01\n")
        schema = _schema([{"name": "dt", "data_type": "date"}])
        df = parse_csv(p, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_int_type_column(self, tmp_path):
        """Branch 199->200: data_type is 'int'."""
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("val\n42\n")
        schema = _schema([{"name": "val", "data_type": "int"}])
        df = parse_csv(p, schema).collect()
        assert df["val"].dtype == pl.Int64

    @pytest.mark.unit
    def test_float_type_column(self, tmp_path):
        """Branch 201->202: data_type is 'float'."""
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("val\n3.14\n")
        schema = _schema([{"name": "val", "data_type": "float"}])
        df = parse_csv(p, schema).collect()
        assert df["val"].dtype == pl.Float64

    @pytest.mark.unit
    def test_boolean_type_column(self, tmp_path):
        """Branch 203->207: data_type is 'boolean'."""
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("flag\ntrue\nfalse\n1\n0\n")
        schema = _schema([{"name": "flag", "data_type": "boolean"}])
        df = parse_csv(p, schema).collect()
        assert df["flag"].dtype == pl.Boolean
        assert df["flag"][0] is True
        assert df["flag"][1] is False

    @pytest.mark.unit
    def test_string_type_column(self, tmp_path):
        """Branch 203->228: data_type is default 'string'."""
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("name\nhello\n")
        schema = _schema([{"name": "name", "data_type": "string"}])
        df = parse_csv(p, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_rename_map_applied(self, tmp_path):
        """Branch 231->232: rename_map is non-empty, applied."""
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("h1\nval\n")
        schema = _schema([{"name": "output_col"}])
        df = parse_csv(p, schema).collect()
        assert "output_col" in df.columns

    @pytest.mark.unit
    def test_cast_exprs_applied(self, tmp_path):
        """Branch 235->236: cast_exprs is non-empty, applied."""
        from acoharmony._parsers._csv import parse_csv

        p = tmp_path / "test.csv"
        p.write_text("val\n99\n")
        schema = _schema([{"name": "val", "data_type": "integer"}])
        df = parse_csv(p, schema).collect()
        assert df["val"].dtype == pl.Int64


class TestCoreParsers:
    """Test core CSV parser functionality."""

    @pytest.mark.integration
    def test_csv_parse(self, tmp_path) -> None:
        """CSV parser can read a file."""
        import polars as pl

        csv_file = tmp_path / "test.csv"
        csv_file.write_text("name,age\nAlice,30\nBob,25\n")
        df = pl.read_csv(csv_file)
        assert len(df) == 2
        assert "name" in df.columns
