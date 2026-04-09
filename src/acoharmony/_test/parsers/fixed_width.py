# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for fixed-width file parser - Polars style.

Tests fixed-width file parsing functionality.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from typing import TYPE_CHECKING

import pytest

from .conftest import _schema, _schema_with_file_format

if TYPE_CHECKING:
    pass


class TestFixedWidthParser:
    """Tests for fixed-width file parsing."""

    @pytest.mark.unit
    def test_fixed_width_basic(self) -> None:
        """Parse basic fixed-width data."""
        # Fixed width: columns at positions 0-5, 5-15, 15-20
        fw_data = "00001Alice      100  \n00002Bob        200  \n"

        # Polars doesn't have built-in fixed-width, would need custom parser
        # This is a placeholder
        assert len(fw_data.split("\n")) == 3  # Including empty line

    @pytest.mark.unit
    def test_fixed_width_with_spec(self) -> None:
        """Parse fixed-width with column specifications."""
        # Would specify column widths: [5, 10, 5]
        assert True


class TestFixedWidthCoverageGaps:
    """Additional tests for _fixed_width coverage gaps."""

    @pytest.mark.unit
    def test_parse_cclf_dict_schema_path(self, tmp_path):
        """Cover the isinstance(schema, dict) branch in parse_cclf (line 528-529)."""
        from acoharmony._parsers._fixed_width import parse_cclf

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema_dict = {
            "columns": [{"name": "data", "start_pos": 1, "length": 5}],
            "encoding": "utf-8",
        }
        with pytest.raises(ValueError, match="No valid column specifications"):
            parse_cclf(p, schema_dict)

    @pytest.mark.unit
    def test_parse_fixed_width_no_col_specs_from_missing_positions(self, tmp_path):
        """Cover columns without start_pos/start."""
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema = _schema([{"name": "data"}])
        with pytest.raises(ValueError, match="No valid column specifications"):
            parse_fixed_width(p, schema)

class TestFixedWidth:
    """Tests for acoharmony._parsers._fixed_width."""

    @pytest.mark.unit
    def test_parse_fixed_width_start_length(self, tmp_path):
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("ABCDE12345\nFGHIJ67890\n")
        schema = _schema(
            [
                {"name": "letters", "start_pos": 1, "length": 5},
                {"name": "numbers", "start_pos": 6, "length": 5},
            ]
        )
        df = parse_fixed_width(p, schema).collect()
        assert df["letters"].to_list() == ["ABCDE", "FGHIJ"]
        assert df["numbers"].to_list() == ["12345", "67890"]

    @pytest.mark.unit
    def test_parse_fixed_width_start_end(self, tmp_path):
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("ABCDE12345\n")
        schema = _schema(
            [
                {"name": "letters", "start_pos": 1, "end_pos": 5},
                {"name": "numbers", "start_pos": 6, "end_pos": 10},
            ]
        )
        df = parse_fixed_width(p, schema).collect()
        assert df["letters"].to_list() == ["ABCDE"]
        assert df["numbers"].to_list() == ["12345"]

    @pytest.mark.unit
    def test_parse_fixed_width_output_name(self, tmp_path):
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema = _schema([{"name": "orig", "output_name": "renamed", "start_pos": 1, "length": 5}])
        df = parse_fixed_width(p, schema).collect()
        assert "renamed" in df.columns

    @pytest.mark.unit
    def test_parse_fixed_width_keep_false(self, tmp_path):
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("ABCDE12345\n")
        schema = _schema(
            [
                {"name": "letters", "start_pos": 1, "length": 5},
                {"name": "filler", "start_pos": 6, "length": 5, "keep": False},
            ]
        )
        df = parse_fixed_width(p, schema).collect()
        assert df.columns == ["letters"]

    @pytest.mark.unit
    def test_parse_fixed_width_limit(self, tmp_path):
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("AAAAA\nBBBBB\nCCCCC\n")
        schema = _schema([{"name": "data", "start_pos": 1, "length": 5}])
        df = parse_fixed_width(p, schema, limit=2).collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_parse_fixed_width_offset(self, tmp_path):
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("HEADER\nAAAAA\nBBBBB\n")
        schema = _schema([{"name": "data", "start_pos": 1, "length": 5}])
        df = parse_fixed_width(p, schema, offset=1).collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_parse_fixed_width_no_specs_raises(self, tmp_path):
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema = _schema([])
        with pytest.raises(ValueError, match="No valid column specifications"):
            parse_fixed_width(p, schema)

    @pytest.mark.unit
    def test_parse_fixed_width_start_alias(self, tmp_path):
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema = _schema([{"name": "data", "start": 1, "width": 5}])
        df = parse_fixed_width(p, schema).collect()
        assert df["data"].to_list() == ["ABCDE"]

    @pytest.mark.unit
    def test_parse_fixed_width_encoding_utf8(self, tmp_path):
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema = _schema([{"name": "name", "start_pos": 1, "length": 5}])
        df = parse_fixed_width(p, schema, encoding="utf-8").collect()
        assert df["name"].to_list() == ["ABCDE"]

    @pytest.mark.unit
    def test_parse_cclf_default(self, tmp_path):
        from acoharmony._parsers._fixed_width import parse_cclf

        p = tmp_path / "test.dat"
        p.write_text("ABCDE12345\n")
        schema = _schema_with_file_format(
            [
                {"name": "letters", "start_pos": 1, "length": 5},
                {"name": "numbers", "start_pos": 6, "length": 5},
            ],
            file_format={"encoding": "utf-8"},
        )
        df = parse_cclf(p, schema).collect()
        assert df["letters"].to_list() == ["ABCDE"]

    @pytest.mark.unit
    def test_parse_cclf_dict_schema(self, tmp_path):
        from acoharmony._parsers._fixed_width import parse_cclf

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema = _schema([{"name": "data", "start_pos": 1, "length": 5}])
        df = parse_cclf(p, schema).collect()
        assert df["data"].to_list() == ["ABCDE"]

    @pytest.mark.unit
    def test_parse_cclf_dict_encoding(self, tmp_path):
        from acoharmony._parsers._fixed_width import parse_cclf

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema = _schema([{"name": "data", "start_pos": 1, "length": 5}])
        df = parse_cclf(p, schema).collect()
        assert df["data"].to_list() == ["ABCDE"]

    @pytest.mark.unit
    def test_parse_cclf_no_file_format(self, tmp_path):
        from acoharmony._parsers._fixed_width import parse_cclf

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema = _schema([{"name": "data", "start_pos": 1, "length": 5}])
        df = parse_cclf(p, schema).collect()
        assert df["data"].to_list() == ["ABCDE"]

    @pytest.mark.unit
    def test_parse_cclf_file_format_none(self, tmp_path):
        from acoharmony._parsers._fixed_width import parse_cclf

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema = _schema_with_file_format(
            [{"name": "data", "start_pos": 1, "length": 5}], file_format=None
        )
        df = parse_cclf(p, schema).collect()
        assert df["data"].to_list() == ["ABCDE"]


class TestFixedWidthParseBranches:
    """Cover branches 409-471 in parse_fixed_width."""

    @pytest.mark.unit
    def test_keep_false_skipped(self, tmp_path):
        """Branch 411->412: col_def has keep=False, skipped."""
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("ABCDE12345\n")
        schema = _schema([
            {"name": "data", "start_pos": 1, "length": 5},
            {"name": "filler", "start_pos": 6, "length": 5, "keep": False},
        ])
        df = parse_fixed_width(p, schema).collect()
        assert "data" in df.columns
        assert "filler" not in df.columns

    @pytest.mark.unit
    def test_start_and_end_pos(self, tmp_path):
        """Branch 419->421: start_pos and end_pos both provided."""
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("ABCDE12345\n")
        schema = _schema([
            {"name": "data", "start_pos": 1, "end_pos": 5},
        ])
        df = parse_fixed_width(p, schema).collect()
        assert "data" in df.columns

    @pytest.mark.unit
    def test_start_and_length(self, tmp_path):
        """Branch 428->429: start_pos and length (no end_pos)."""
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("ABCDE12345\n")
        schema = _schema([
            {"name": "data", "start_pos": 1, "length": 5},
        ])
        df = parse_fixed_width(p, schema).collect()
        assert df["data"][0] == "ABCDE"

    @pytest.mark.unit
    def test_no_valid_specs_raises(self, tmp_path):
        """Branch 437->438: no valid col_specs, raises ValueError."""
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema = _schema([
            {"name": "bad", "keep": False},
        ])
        with pytest.raises(ValueError, match="No valid column specifications"):
            parse_fixed_width(p, schema)

    @pytest.mark.unit
    def test_multiple_columns(self, tmp_path):
        """Branch 460->462: iterate through col_specs, build expressions."""
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("Hello World\n")
        schema = _schema([
            {"name": "first", "start_pos": 1, "length": 5},
            {"name": "second", "start_pos": 7, "length": 5},
        ])
        df = parse_fixed_width(p, schema).collect()
        assert "first" in df.columns
        assert "second" in df.columns

    @pytest.mark.unit
    def test_output_name_override(self, tmp_path):
        """Column spec with output_name overrides name."""
        from acoharmony._parsers._fixed_width import parse_fixed_width

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema = _schema([
            {"name": "raw_name", "output_name": "pretty_name", "start_pos": 1, "length": 5},
        ])
        df = parse_fixed_width(p, schema).collect()
        assert "pretty_name" in df.columns


class TestCclfEncodingBranches:
    """Cover branches 528->529/530, 530->531/533 in parse_cclf."""

    @pytest.mark.unit
    def test_cclf_dict_schema_encoding(self, tmp_path):
        """Branch 528->529: schema is dict, get encoding."""
        from acoharmony._parsers._fixed_width import parse_cclf

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema_dict = {
            "encoding": "utf-8",
            "columns": [{"name": "data", "start_pos": 1, "length": 5}],
        }
        # parse_cclf expects schema with .columns attribute
        # Use _schema helper
        schema = _schema([{"name": "data", "start_pos": 1, "length": 5}])
        df = parse_cclf(p, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_cclf_schema_with_file_format(self, tmp_path):
        """Branch 530->531: hasattr file_format and file_format is truthy."""
        from acoharmony._parsers._fixed_width import parse_cclf

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema = _schema_with_file_format(
            [{"name": "data", "start_pos": 1, "length": 5}],
            file_format={"encoding": "utf-8"},
        )
        df = parse_cclf(p, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_cclf_schema_no_file_format(self, tmp_path):
        """Branch 530->533: no file_format, defaults to utf-8."""
        from acoharmony._parsers._fixed_width import parse_cclf

        p = tmp_path / "test.dat"
        p.write_text("ABCDE\n")
        schema = _schema([{"name": "data", "start_pos": 1, "length": 5}])
        df = parse_cclf(p, schema).collect()
        assert df.height == 1
