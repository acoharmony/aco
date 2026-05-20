from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import tempfile
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest
from polars.testing import assert_frame_equal

import acoharmony

from .conftest import create_mock_metadata

# © 2025 HarmonyCares
# All rights reserved.


"""
Unit tests for Parquet parser - Polars style.

Tests Parquet file parsing functionality.
"""


if TYPE_CHECKING:
    pass


class TestParquetParser:
    """Tests for Parquet parsing."""

    @pytest.mark.unit
    def test_parse_parquet_basic(self) -> None:
        """Parse basic Parquet file."""
        df_original = pl.DataFrame(
            {"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"], "value": [100, 200, 300]}
        )

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            df_original.write_parquet(f.name)
            df_read = pl.read_parquet(f.name)

        assert_frame_equal(df_original, df_read)
        Path(f.name).unlink()

    @pytest.mark.unit
    def test_parse_parquet_lazy(self) -> None:
        """Parse Parquet file lazily."""
        df_original = pl.DataFrame({"id": [1, 2, 3], "value": [10, 20, 30]})

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            df_original.write_parquet(f.name)
            lf = pl.scan_parquet(f.name)
            df_read = lf.collect()

        assert_frame_equal(df_original, df_read)
        Path(f.name).unlink()

    @pytest.mark.unit
    def test_parse_parquet_schema_preserved(self) -> None:
        """Parquet preserves data types."""
        df_original = pl.DataFrame(
            {"int_col": [1, 2, 3], "float_col": [1.1, 2.2, 3.3], "str_col": ["a", "b", "c"]}
        )

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            df_original.write_parquet(f.name)
            df_read = pl.read_parquet(f.name)

        assert df_read["int_col"].dtype == pl.Int64
        assert df_read["float_col"].dtype == pl.Float64
        assert df_read["str_col"].dtype == pl.Utf8
        Path(f.name).unlink()


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._parquet is not None


class TestParseParquet:
    """Test parse_parquet function."""

    @pytest.mark.unit
    def test_parse_parquet_basic(self):
        """Test basic Parquet parsing."""
        df = pl.DataFrame({"id": [1, 2, 3], "value": ["a", "b", "c"]})
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            parquet_path = Path(f.name)
        try:
            df.write_parquet(parquet_path)
            mock_schema = create_mock_metadata("test", [], {"type": "parquet"})
            lf = parse_parquet(parquet_path, mock_schema)
            result = lf.collect()
            assert len(result) == 3
            assert "id" in result.columns
            assert "value" in result.columns
        finally:
            if parquet_path.exists():
                parquet_path.unlink()


class TestParquet:
    """Tests for acoharmony._parsers._parquet."""

    @pytest.mark.unit
    def test_parse_parquet_basic(self, tmp_path):
        from acoharmony._parsers._parquet import parse_parquet

        p = tmp_path / "test.parquet"
        pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]}).write_parquet(p)
        df = parse_parquet(p, None).collect()
        assert df.height == 3

    @pytest.mark.unit
    def test_parse_parquet_with_limit(self, tmp_path):
        from acoharmony._parsers._parquet import parse_parquet

        p = tmp_path / "test.parquet"
        pl.DataFrame({"a": list(range(100))}).write_parquet(p)
        df = parse_parquet(p, None, limit=5).collect()
        assert df.height == 5


class TestCoreParquet:
    """Test core parquet functionality."""

    @pytest.mark.integration
    def test_parquet_roundtrip(self, tmp_path) -> None:
        """Parquet write and read works."""
        import polars as pl

        df = pl.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        parquet_file = tmp_path / "test.parquet"
        df.write_parquet(parquet_file)
        result = pl.read_parquet(parquet_file)
        assert len(result) == 3
        assert result.columns == ["a", "b"]


class TestParseParquetLimitBranches:
    """Cover branches 45->46 (limit truthy) and 45->47 (limit falsy)."""

    @pytest.mark.unit
    def test_parse_parquet_no_limit(self, tmp_path):
        """Branch 45->47: limit is None so lf.head() is NOT called."""
        from acoharmony._parsers._parquet import parse_parquet

        p = tmp_path / "test.parquet"
        pl.DataFrame({"x": [1, 2, 3]}).write_parquet(p)
        df = parse_parquet(p, None, limit=None).collect()
        assert df.height == 3

    @pytest.mark.unit
    def test_parse_parquet_limit_zero(self, tmp_path):
        """Branch 45->47: limit=0 is falsy so head() is NOT called."""
        from acoharmony._parsers._parquet import parse_parquet

        p = tmp_path / "test.parquet"
        pl.DataFrame({"x": [1, 2, 3]}).write_parquet(p)
        df = parse_parquet(p, None, limit=0).collect()
        # limit=0 is falsy, so no head() applied, all rows returned
        assert df.height == 3

    @pytest.mark.unit
    def test_parse_parquet_with_positive_limit(self, tmp_path):
        """Branch 45->46: limit > 0 is truthy so head(limit) IS called."""
        from acoharmony._parsers._parquet import parse_parquet

        p = tmp_path / "test.parquet"
        pl.DataFrame({"x": list(range(50))}).write_parquet(p)
        df = parse_parquet(p, None, limit=10).collect()
        assert df.height == 10
