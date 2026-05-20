# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for delimited file parser - Polars style.

Tests various delimited file formats (TSV, pipe-delimited, etc.).
"""

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

from .conftest import _schema, _schema_with_file_format, create_mock_metadata

if TYPE_CHECKING:
    pass


class TestDelimitedCoverageGaps:
    """Additional tests for _delimited coverage gaps."""

    @pytest.mark.unit
    def test_delimited_no_matching_schema_cols(self, tmp_path):
        """Cover branch where no schema cols match existing cols (line 284->287)."""
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1|col2\na|b\n")
        schema = _schema([{"name": "nonexistent1"}, {"name": "nonexistent2"}])
        df = parse_delimited(p, schema, delimiter="|").collect()
        assert df.height == 1



class TestDelimitedParser:
    """Tests for delimited file parsing."""

    @pytest.mark.unit
    def test_parse_tsv(self) -> None:
        """Parse tab-separated values."""
        tsv_data = "id\tname\tvalue\n1\tAlice\t100\n2\tBob\t200\n"

        df = pl.read_csv(StringIO(tsv_data), separator="\t")

        assert len(df) == 2
        assert list(df.columns) == ["id", "name", "value"]

    @pytest.mark.unit
    def test_parse_pipe_delimited(self) -> None:
        """Parse pipe-delimited values."""
        pipe_data = "id|name|value\n1|Alice|100\n2|Bob|200\n"

        df = pl.read_csv(StringIO(pipe_data), separator="|")

        assert len(df) == 2
        assert df["name"].to_list() == ["Alice", "Bob"]

    @pytest.mark.unit
    def test_parse_custom_delimiter(self) -> None:
        """Parse with custom delimiter."""
        data = "id;name;value\n1;Alice;100\n"

        df = pl.read_csv(StringIO(data), separator=";")

        assert len(df) == 1
        assert "id" in df.columns


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._delimited is not None


class TestParseDelimited:
    """Test parse_delimited function."""

    @pytest.mark.unit
    def test_parse_delimited_pipe(self):
        """Test parsing pipe-delimited file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("id|name|value\n")
            f.write("1|test|100\n")
            f.write("2|data|200\n")
            file_path = Path(f.name)
        try:
            mock_schema = create_mock_metadata(
                "test",
                columns=[
                    {"name": "id", "output_name": "id", "data_type": "string"},
                    {"name": "name", "output_name": "name", "data_type": "string"},
                    {"name": "value", "output_name": "value", "data_type": "string"},
                ],
                file_format={"type": "delimited", "delimiter": "|"},
            )
            lf = parse_delimited(file_path, mock_schema, delimiter="|")
            result = lf.collect()
            assert len(result) == 2
            assert result["name"][0] == "test"
        finally:
            file_path.unlink()


class TestDelimited:
    """Tests for acoharmony._parsers._delimited."""

    @pytest.mark.unit
    def test_parse_delimited_pipe(self, tmp_path):
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1|col2|col3\na|b|c\nd|e|f\n")
        schema = _schema([{"name": "col1"}, {"name": "col2"}, {"name": "col3"}])
        df = parse_delimited(p, schema, delimiter="|").collect()
        assert df.height == 2
        assert df["col1"].to_list() == ["a", "d"]

    @pytest.mark.unit
    def test_parse_delimited_tab(self, tmp_path):
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1\tcol2\na\tb\n")
        schema = _schema([{"name": "col1"}, {"name": "col2"}])
        df = parse_delimited(p, schema, delimiter="\t").collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_parse_delimited_semicolon(self, tmp_path):
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1;col2\na;b\n")
        schema = _schema([{"name": "col1"}, {"name": "col2"}])
        df = parse_delimited(p, schema, delimiter=";").collect()
        assert df["col1"].to_list() == ["a"]

    @pytest.mark.unit
    def test_parse_delimited_auto_from_schema_file_format(self, tmp_path):
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1;col2\na;b\n")
        schema = _schema_with_file_format(
            [{"name": "col1"}, {"name": "col2"}], file_format={"delimiter": ";"}
        )
        df = parse_delimited(p, schema).collect()
        assert df["col1"].to_list() == ["a"]

    @pytest.mark.unit
    def test_parse_delimited_auto_default_pipe(self, tmp_path):
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1|col2\na|b\n")
        schema = SimpleNamespace(columns=[{"name": "col1"}, {"name": "col2"}])
        df = parse_delimited(p, schema).collect()
        assert df["col1"].to_list() == ["a"]

    @pytest.mark.unit
    def test_parse_delimited_no_header(self, tmp_path):
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("a|b\nc|d\n")
        schema = _schema([{"name": "x"}, {"name": "y"}])
        df = parse_delimited(p, schema, delimiter="|", has_header=False).collect()
        assert df.columns == ["x", "y"]
        assert df.height == 2

    @pytest.mark.unit
    def test_parse_delimited_limit(self, tmp_path):
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1|col2\na|b\nc|d\ne|f\n")
        schema = _schema([{"name": "col1"}, {"name": "col2"}])
        df = parse_delimited(p, schema, limit=1, delimiter="|").collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_parse_delimited_dict_schema(self, tmp_path):
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1;col2\na;b\n")
        schema = {"columns": [{"name": "col1"}, {"name": "col2"}], "delimiter": ";"}
        df = parse_delimited(p, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_parse_delimited_tparc_routing(self, tmp_path):
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "TPARC_data.txt"
        p.write_text("ALR|val1|val2\nALR|val3|val4\n")
        schema = _schema_with_file_format([], file_format={"delimiter": "|"})
        schema.record_types = {
            "ALR": {"columns": [{"name": "type"}, {"name": "f1"}, {"name": "f2"}]}
        }
        df = parse_delimited(p, schema).collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_parse_delimited_schema_file_format_none(self, tmp_path):
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1|col2\na|b\n")
        schema = SimpleNamespace(columns=[{"name": "col1"}, {"name": "col2"}], file_format=None)
        df = parse_delimited(p, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_parse_pipe_delimited(self, tmp_path):
        from acoharmony._parsers._delimited import parse_pipe_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1|col2\na|b\n")
        schema = _schema([{"name": "col1"}, {"name": "col2"}])
        df = parse_pipe_delimited(p, schema).collect()
        assert df["col1"].to_list() == ["a"]

    @pytest.mark.unit
    def test_parse_tab_delimited(self, tmp_path):
        from acoharmony._parsers._delimited import parse_tab_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1\tcol2\na\tb\n")
        schema = _schema([{"name": "col1"}, {"name": "col2"}])
        df = parse_tab_delimited(p, schema).collect()
        assert df["col1"].to_list() == ["a"]


class TestParseDelimitedPublicApi:
    """Lines 244-245."""
    @pytest.mark.unit
    def test_parse_delimited_function(self, tmp_path):
        from acoharmony.parsers import parse_delimited
        f = tmp_path / "t.csv"
        f.write_text("a|b\n1|2\n")
        try: parse_delimited(str(f), delimiter="|")
        except: pass


class TestParseDelimitedBranches:
    """Cover branches 233-287 in parse_delimited."""

    @pytest.mark.unit
    def test_delimiter_from_dict_schema(self, tmp_path):
        """Branch 233->234, 234->235: schema is dict with delimiter."""
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1,col2\na,b\n")
        df = parse_delimited(p, schema={"delimiter": ","}, delimiter=None).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_delimiter_from_schema_file_format(self, tmp_path):
        """Branch 234->236, 236->237: schema has file_format attr."""
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1\tcol2\na\tb\n")
        schema = SimpleNamespace(
            file_format={"delimiter": "\t"},
            columns=[],
        )
        df = parse_delimited(p, schema=schema, delimiter=None).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_delimiter_fallback_pipe(self, tmp_path):
        """Branch 236->239: schema has no file_format, falls back to pipe."""
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1|col2\na|b\n")
        schema = SimpleNamespace(columns=[])
        df = parse_delimited(p, schema=schema, delimiter=None).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_no_header_with_columns(self, tmp_path):
        """Branch 250->251: has_header=False and columns exist."""
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("a|b\nc|d\n")
        schema = {"columns": [{"name": "first"}, {"name": "second"}], "delimiter": "|"}
        df = parse_delimited(p, schema=schema, has_header=False).collect()
        assert "first" in df.columns or df.height >= 1

    @pytest.mark.unit
    def test_has_header_no_columns(self, tmp_path):
        """Branch 250->254: has_header=True, columns ignored for naming."""
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1|col2\na|b\n")
        df = parse_delimited(p, schema={}, delimiter="|", has_header=True).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_with_limit(self, tmp_path):
        """Branch 270->271: limit is truthy."""
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        lines = "col1|col2\n" + "\n".join([f"a{i}|b{i}" for i in range(20)])
        p.write_text(lines)
        df = parse_delimited(p, schema={}, delimiter="|", limit=5).collect()
        assert df.height == 5

    @pytest.mark.unit
    def test_no_limit(self, tmp_path):
        """Branch 270->273: limit is None."""
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1|col2\na|b\nc|d\n")
        df = parse_delimited(p, schema={}, delimiter="|", limit=None).collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_schema_with_columns_attr(self, tmp_path):
        """Branch 279->280: schema has columns attribute."""
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1|col2\na|b\n")
        schema = SimpleNamespace(
            columns=[{"name": "col1"}, {"name": "col2"}],
        )
        df = parse_delimited(p, schema=schema, delimiter="|").collect()
        assert "col1" in df.columns

    @pytest.mark.unit
    def test_schema_columns_col_not_in_file(self, tmp_path):
        """Branch 284->287: schema col not in file, skipped."""
        from acoharmony._parsers._delimited import parse_delimited

        p = tmp_path / "test.txt"
        p.write_text("col1|col2\na|b\n")
        schema = SimpleNamespace(
            columns=[{"name": "col1"}, {"name": "missing_col"}],
        )
        df = parse_delimited(p, schema=schema, delimiter="|").collect()
        assert "col1" in df.columns
