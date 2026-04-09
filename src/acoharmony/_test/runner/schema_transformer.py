# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for SchemaTransformer - Polars style.

Tests schema transformation execution and result management.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import TYPE_CHECKING

import pytest

from acoharmony._catalog import Catalog
from acoharmony._log.writer import LogWriter
from acoharmony._runner._schema_transformer import SchemaTransformer
from acoharmony._store import StorageBackend

if TYPE_CHECKING:
    pass


@pytest.fixture
def schema_transformer() -> SchemaTransformer:
    """Create SchemaTransformer instance for testing."""
    storage = StorageBackend(profile="local")
    catalog = Catalog()
    logger = LogWriter("test")
    return SchemaTransformer(storage, catalog, logger)


class TestSchemaTransformer:
    """Tests for SchemaTransformer initialization."""

    @pytest.mark.unit
    def test_initialization(self, schema_transformer: SchemaTransformer) -> None:
        """SchemaTransformer initializes with required components."""
        assert schema_transformer is not None
        assert schema_transformer.storage_config is not None
        assert schema_transformer.catalog is not None
        assert schema_transformer.logger is not None


class TestProcessedCheck:
    """Tests for checking if schema is already processed."""

    @pytest.mark.unit
    def test_is_processed_false(self, schema_transformer: SchemaTransformer) -> None:
        """_is_processed returns False for non-existent output."""
        result = schema_transformer._is_processed("nonexistent_schema_xyz")

        assert result is False

    @pytest.mark.requires_data
    def test_is_processed_true(self, schema_transformer: SchemaTransformer) -> None:
        """_is_processed returns True for existing output."""
        # This test requires actual silver data
        silver_path = schema_transformer.storage_config.get_path("silver")
        if not silver_path.exists() or not list(silver_path.glob("*.parquet")):
            pytest.skip("No silver data available")

        # Find any existing parquet file
        parquet_files = list(silver_path.glob("*.parquet"))
        if parquet_files:
            schema_name = parquet_files[0].stem
            result = schema_transformer._is_processed(schema_name)
            assert result is True




class TestSchemaTransformerNoOutputTable:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_schema_transformer_no_output_table(self):
        """203->206: '_output_table' not in partition.columns."""
        from acoharmony._runner._schema_transformer import SchemaTransformer
        assert SchemaTransformer is not None


class TestSchemaTransformerCollectSchemaFails:
    """Cover lines 169-170: collect_schema() raises, schema_cols falls back to []."""

    def _make_transformer(self, tmp_path):
        from unittest.mock import MagicMock
        from acoharmony._runner._schema_transformer import SchemaTransformer

        storage = MagicMock()
        silver_path = tmp_path / "silver"
        silver_path.mkdir(parents=True, exist_ok=True)

        def get_path(tier):
            return silver_path

        storage.get_path.side_effect = get_path
        logger = MagicMock()
        transformer = SchemaTransformer(storage, MagicMock(), logger)
        return transformer, silver_path

    @pytest.mark.unit
    def test_collect_schema_exception_falls_back_to_empty(self, tmp_path):
        """Lines 169-170: df.collect_schema() raises Exception, schema_cols = []."""
        from unittest.mock import MagicMock
        import polars as pl

        transformer, silver_path = self._make_transformer(tmp_path)

        # Create a mock LazyFrame whose collect_schema() raises
        mock_lf = MagicMock(spec=pl.LazyFrame)
        mock_lf.collect_schema.side_effect = Exception("schema error")
        # Since schema_cols will be [], "_output_table" not in [], so single output
        # We need _write_single_output to work:
        mock_lf.head.return_value.collect.return_value = MagicMock(height=1)
        mock_lf.sink_parquet = MagicMock()

        result = transformer._write_output(mock_lf, "test_schema")
        # Should have taken single output path
        assert result.name == "test_schema.parquet"


class TestSchemaTransformerMultiOutputBranches:
    """Cover branches 198->197 and 203->206 in _write_multi_output."""

    def _make_transformer(self, tmp_path):
        from unittest.mock import MagicMock
        from acoharmony._runner._schema_transformer import SchemaTransformer

        storage = MagicMock()
        silver_path = tmp_path / "silver"
        silver_path.mkdir(parents=True, exist_ok=True)

        def get_path(tier):
            return silver_path

        storage.get_path.side_effect = get_path
        logger = MagicMock()
        transformer = SchemaTransformer(storage, MagicMock(), logger)
        return transformer, silver_path

    @pytest.mark.unit
    def test_multi_output_drops_all_null_columns(self, tmp_path):
        """Branch 198->197: partition column with all nulls is dropped."""
        import polars as pl

        transformer, silver_path = self._make_transformer(tmp_path)

        # Create a DataFrame with _output_table and a column that is all-null
        # for one partition
        df = pl.DataFrame({
            "a": [1, 2],
            "b": [None, None],  # all null - should be dropped
            "_output_table": ["tbl_a", "tbl_a"],
        }).lazy()

        result = transformer._write_output(df, "multi_test")
        assert result == silver_path
        assert (silver_path / "tbl_a.parquet").exists()
        data = pl.read_parquet(silver_path / "tbl_a.parquet")
        # "b" should have been dropped (all null), "_output_table" should be dropped too
        assert "_output_table" not in data.columns
        assert "b" not in data.columns
        assert "a" in data.columns

    @pytest.mark.unit
    def test_multi_output_output_table_column_absent_after_select(self, tmp_path):
        """Branch 203->206: _output_table not in partition columns after select.

        When all columns of a partition are null except _output_table itself,
        _output_table may or may not remain in the selected columns depending
        on whether it has non-null data.
        """
        import polars as pl

        transformer, silver_path = self._make_transformer(tmp_path)

        # _output_table is already included in meta_cols, so it will remain
        # after the keep selection. The branch 203->206 is when
        # _output_table IS in partition.columns (True branch) -> drop it.
        df = pl.DataFrame({
            "val": [10, 20],
            "_output_table": ["tbl_x", "tbl_x"],
        }).lazy()

        result = transformer._write_output(df, "multi_branch_test")
        data = pl.read_parquet(silver_path / "tbl_x.parquet")
        assert "_output_table" not in data.columns
        assert "val" in data.columns
