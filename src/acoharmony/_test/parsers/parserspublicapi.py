"""
Tests for medium-coverage-gap modules: _deploy/_manager, _runner/_pipeline_executor,
_runner/_schema_transformer (additional), parsers.py, _tuva/_depends/setup.py
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock

import polars as pl
import pytest

from acoharmony.parsers import parse_file, parse_csv, parse_json, parse_parquet


class TestParsersPublicAPI:
    def _make_schema(self, file_format=None, columns=None):
        schema = MagicMock()
        schema.file_format = file_format or {}
        schema.columns = columns or []
        schema.name = "test_schema"
        return schema

    @pytest.mark.unit
    def test_parse_file_csv(self, tmp_path):

        csv = tmp_path / "test.csv"
        csv.write_text("a,b\n1,2\n3,4")
        schema = self._make_schema({"type": "csv"})
        result = parse_file(str(csv), schema)
        assert result is not None

    @pytest.mark.unit
    def test_parse_file_nonexistent(self):
        schema = self._make_schema()
        with pytest.raises(FileNotFoundError):
            parse_file("/nonexistent/file.csv", schema)

    @pytest.mark.unit
    def test_parse_csv_function(self, tmp_path):
        csv = tmp_path / "test.csv"
        csv.write_text("a,b\n1,2\n3,4")
        result = parse_csv(str(csv))
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_parse_json_function(self, tmp_path):
        jf = tmp_path / "test.json"
        jf.write_text('[{"a": 1}]')
        result = parse_json(str(jf))
        assert result is not None

    @pytest.mark.unit
    def test_parse_parquet_function(self, tmp_path):
        pf = tmp_path / "test.parquet"
        pl.DataFrame({"x": [1]}).write_parquet(pf)
        result = parse_parquet(str(pf))
        assert isinstance(result, pl.LazyFrame)

    @pytest.mark.unit
    def test_parse_file_parquet(self, tmp_path):
        pf = tmp_path / "test.parquet"
        pl.DataFrame({"a": [1, 2]}).write_parquet(pf)
        schema = self._make_schema({"type": "parquet"})
        result = parse_file(str(pf), schema)
        assert result is not None

    @pytest.mark.unit
    def test_parse_file_auto_detect_csv(self, tmp_path):
        csv = tmp_path / "test.csv"
        csv.write_text("a,b\n1,2")
        schema = self._make_schema({"type": "auto"})
        result = parse_file(str(csv), schema)
        assert result is not None

    @pytest.mark.unit
    def test_parse_file_with_medallion_layer(self, tmp_path):
        """Branch 118→121: schema has medallion_layer attribute."""
        from acoharmony.medallion import MedallionLayer

        csv = tmp_path / "test.csv"
        csv.write_text("a,b\n1,2\n3,4")
        schema = self._make_schema({"type": "csv"})
        schema.medallion_layer = MedallionLayer.SILVER
        result = parse_file(str(csv), schema, add_tracking=True)
        assert result is not None

    @pytest.mark.unit
    def test_parse_file_without_medallion_layer(self, tmp_path):
        """Branch 118→121 (False path): schema lacks medallion_layer attribute."""

        # Use a plain class so hasattr(schema, "medallion_layer") returns False
        class _Schema:
            file_format = {"type": "csv"}
            columns: list = []
            name = "test_schema"

        csv = tmp_path / "test.csv"
        csv.write_text("a,b\n1,2\n3,4")
        result = parse_file(str(csv), _Schema(), add_tracking=True)
        assert result is not None


# ---------------------------------------------------------------------------
# _tuva/_depends/setup.py
# ---------------------------------------------------------------------------
