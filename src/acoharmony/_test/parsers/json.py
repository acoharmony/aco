from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from io import StringIO
from typing import TYPE_CHECKING
from types import SimpleNamespace
import json
import tempfile
from pathlib import Path

import polars as pl
import pytest

import acoharmony

from .conftest import create_mock_metadata, _schema, _schema_with_file_format

# © 2025 HarmonyCares
# All rights reserved.


"""
Unit tests for JSON parser - Polars style.

Tests JSON file parsing functionality.
"""


if TYPE_CHECKING:
    pass


@pytest.mark.unit
def test_json_parse(tmp_path) -> None:
    """JSON parser can read a file."""
    json_file = tmp_path / "test.json"
    data = [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]
    json_file.write_text(json.dumps(data))
    df = pl.read_json(json_file)
    assert len(df) == 2


class TestJsonParser:
    """Tests for JSON parsing."""

    @pytest.mark.unit
    def test_parse_json_basic(self) -> None:
        """Parse basic JSON data."""
        json_data = '[{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]'

        df = pl.read_json(StringIO(json_data))

        assert len(df) == 2
        assert "id" in df.columns
        assert "name" in df.columns

    @pytest.mark.unit
    def test_parse_json_ndjson(self) -> None:
        """Parse newline-delimited JSON."""
        ndjson_data = '{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}\n'

        df = pl.read_ndjson(StringIO(ndjson_data))

        assert len(df) == 2
        assert df["id"].to_list() == [1, 2]

    @pytest.mark.unit
    def test_parse_json_nested(self) -> None:
        """Parse JSON with nested structures."""
        json_data = '[{"id": 1, "data": {"value": 100}}]'

        df = pl.read_json(StringIO(json_data))

        assert len(df) == 1
        assert "id" in df.columns
        assert "data" in df.columns


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._json is not None


class TestJsonCoverageGaps:
    """Additional tests for _json coverage gaps."""

    @pytest.mark.unit
    def test_json_array_no_schema_cols(self, tmp_path):
        """Cover array path with no schema columns."""
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text(json.dumps([{"a": 1}]))
        schema = SimpleNamespace()
        df = parse_json(p, schema, json_format="array").collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_json_array_schema_cols_not_in_data(self, tmp_path):
        """Cover schema cols selection where some cols don't exist."""
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text(json.dumps([{"a": 1}]))
        schema = _schema([{"name": "a"}, {"name": "nonexistent"}])
        df = parse_json(p, schema, json_format="array").collect()
        assert df.columns == ["a"]

    @pytest.mark.unit
    def test_json_ndjson_schema_cols(self, tmp_path):
        """Cover ndjson path with schema column selection."""
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.ndjson"
        p.write_text('{"a": 1, "b": 2}\n{"a": 3, "b": 4}\n')
        schema = _schema([{"name": "a"}])
        df = parse_json(p, schema).collect()
        assert df.columns == ["a"]

    @pytest.mark.unit
    def test_json_array_no_rename_needed(self, tmp_path):
        """Cover source_name same as name (no rename)."""
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text(json.dumps([{"a": 1}]))
        schema = _schema([{"name": "a", "source_name": "a"}])
        df = parse_json(p, schema, json_format="array").collect()
        assert df.columns == ["a"]

    @pytest.mark.unit
    def test_json_schema_no_file_format(self, tmp_path):
        """Cover array path when schema has no file_format."""
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text(json.dumps([{"a": 1}]))
        schema = _schema([{"name": "a"}])
        df = parse_json(p, schema, json_format="array").collect()
        assert df.height == 1

class TestJson:
    """Tests for acoharmony._parsers._json."""

    @pytest.mark.unit
    def test_parse_json_array(self, tmp_path):
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text(json.dumps([{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]))
        schema = _schema([{"name": "a"}, {"name": "b"}])
        df = parse_json(p, schema).collect()
        assert df.height == 2
        assert set(df.columns) == {"a", "b"}

    @pytest.mark.unit
    def test_parse_json_ndjson_extension(self, tmp_path):
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.ndjson"
        p.write_text('{"a": 1}\n{"a": 2}\n')
        schema = _schema([{"name": "a"}])
        df = parse_json(p, schema).collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_parse_json_jsonl_extension(self, tmp_path):
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.jsonl"
        p.write_text('{"a": 1}\n{"a": 2}\n')
        schema = _schema([{"name": "a"}])
        df = parse_json(p, schema).collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_parse_json_auto_ndjson_from_content(self, tmp_path):
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text('{"a": 1}\n{"a": 2}\n')
        schema = _schema([{"name": "a"}])
        df = parse_json(p, schema).collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_parse_json_single_object(self, tmp_path):
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text(json.dumps({"a": 1, "b": "hello"}))
        schema = _schema([{"name": "a"}, {"name": "b"}])
        df = parse_json(p, schema, json_format="array").collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_parse_json_limit(self, tmp_path):
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text(json.dumps([{"a": i} for i in range(10)]))
        schema = _schema([{"name": "a"}])
        df = parse_json(p, schema, limit=3).collect()
        assert df.height == 3

    @pytest.mark.unit
    def test_parse_json_source_name_mapping(self, tmp_path):
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text(json.dumps([{"original_name": "val1"}]))
        schema = _schema([{"name": "mapped_name", "source_name": "original_name"}])
        df = parse_json(p, schema).collect()
        assert "mapped_name" in df.columns

    @pytest.mark.unit
    def test_parse_json_explicit_ndjson(self, tmp_path):
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text('{"a": 1}\n{"a": 2}\n')
        schema = _schema([{"name": "a"}])
        df = parse_json(p, schema, json_format="ndjson").collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_parse_json_explicit_jsonl(self, tmp_path):
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text('{"a": 1}\n{"a": 2}\n')
        schema = _schema([{"name": "a"}])
        df = parse_json(p, schema, json_format="jsonl").collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_parse_json_unsupported_type(self, tmp_path):
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text('"just a string"')
        schema = _schema([])
        with pytest.raises(ValueError, match="Unsupported JSON structure"):
            parse_json(p, schema, json_format="array").collect()

    @pytest.mark.unit
    def test_parse_json_no_schema(self, tmp_path):
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text(json.dumps([{"x": 1}]))
        df = parse_json(p, None).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_parse_json_schema_encoding(self, tmp_path):
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text(json.dumps([{"a": 1}]))
        schema = _schema_with_file_format([{"name": "a"}], file_format={"encoding": "utf-8"})
        df = parse_json(p, schema).collect()
        assert df.height == 1

    @pytest.mark.unit
    def test_parse_ndjson(self, tmp_path):
        from acoharmony._parsers._json import parse_ndjson

        p = tmp_path / "test.json"
        p.write_text('{"a": 1}\n{"a": 2}\n')
        schema = _schema([{"name": "a"}])
        df = parse_ndjson(p, schema).collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_parse_jsonl(self, tmp_path):
        from acoharmony._parsers._json import parse_jsonl

        p = tmp_path / "test.json"
        p.write_text('{"a": 1}\n{"a": 2}\n')
        schema = _schema([{"name": "a"}])
        df = parse_jsonl(p, schema).collect()
        assert df.height == 2


class TestJsonBranchCoverage:
    """Tests covering specific uncovered branches in parse_json."""

    @pytest.mark.unit
    def test_auto_detect_first_char_neither_bracket_nor_brace(self, tmp_path):
        """Branch 239->243: auto-detect when first char is neither '[' nor '{'.

        When the first character is whitespace (e.g. a space before a JSON array),
        json_format stays 'auto' which is not in ['ndjson', 'jsonl'], so it falls
        through to the array/eager parsing path.
        """
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        # Leading space before valid JSON array -- json.load tolerates whitespace
        p.write_text(' [{"a": 1}]')
        schema = _schema([{"name": "a"}])
        df = parse_json(p, schema, json_format="auto").collect()
        assert df.height == 1
        assert "a" in df.columns

    @pytest.mark.unit
    def test_array_path_empty_columns_skips_rename(self, tmp_path):
        """Branch 268->279: schema.columns is an empty list so rename block is skipped."""
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text(json.dumps([{"x": 10, "y": 20}]))
        # columns=[] is falsy, so `if columns:` at line 268 is False
        schema = _schema([])
        df = parse_json(p, schema, json_format="array").collect()
        assert df.height == 1
        assert set(df.columns) == {"x", "y"}

    @pytest.mark.unit
    def test_schema_columns_empty_skips_selection(self, tmp_path):
        """Branch 288->296: schema.columns is empty so column selection is skipped."""
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text('{"a": 1, "b": 2}\n{"a": 3, "b": 4}\n')
        # Use ndjson path, then schema.columns=[] means `if columns:` at 288 is False
        schema = _schema([])
        df = parse_json(p, schema, json_format="ndjson").collect()
        assert df.height == 2
        assert set(df.columns) == {"a", "b"}

    @pytest.mark.unit
    def test_schema_cols_none_match_existing_skips_select(self, tmp_path):
        """Branch 293->296: cols_to_select is empty because no schema cols exist in data."""
        from acoharmony._parsers._json import parse_json

        p = tmp_path / "test.json"
        p.write_text('{"a": 1}\n{"a": 2}\n')
        # Schema requests columns that do not exist in data
        schema = _schema([{"name": "nonexistent"}, {"name": "also_missing"}])
        df = parse_json(p, schema, json_format="ndjson").collect()
        # No matching columns, so no selection applied; all original columns remain
        assert df.height == 2
        assert "a" in df.columns
