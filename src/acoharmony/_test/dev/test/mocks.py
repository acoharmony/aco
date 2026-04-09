"""Tests for acoharmony._dev.test.mocks module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony
from acoharmony._dev.test.mocks import ColumnMetadata, MockDataGenerator


import tempfile
from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock

import polars as pl


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._dev.test.mocks is not None


class TestScanTableMetadataDateColumn:
    """Cover branch 120->129: Date/Datetime columns skip sample_vals extraction."""

    @pytest.mark.unit
    def test_date_column_skips_sample_values(self):
        """Date columns skip sample_vals but still compute unique_count."""
        df = pl.DataFrame({
            "event_date": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)],
            "name": ["Alice", "Bob", "Carol"],
        })
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            df.write_parquet(f.name)
            storage = MagicMock()
            gen = MockDataGenerator(storage=storage, sample_size=100)
            meta = gen.scan_table_metadata(Path(f.name), "silver", "test_table")
        assert meta is not None
        date_col = meta.columns["event_date"]
        # Date column should have empty sample_values since it's skipped
        assert date_col.sample_values == []
        # But unique_count should still be computed
        assert date_col.unique_count == 3

    @pytest.mark.unit
    def test_datetime_column_skips_sample_values(self):
        """Datetime columns also skip sample_vals extraction."""
        df = pl.DataFrame({
            "created_at": [datetime(2024, 1, 1, 10, 0), datetime(2024, 2, 1, 11, 0)],
        })
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            df.write_parquet(f.name)
            storage = MagicMock()
            gen = MockDataGenerator(storage=storage, sample_size=100)
            meta = gen.scan_table_metadata(Path(f.name), "gold", "test_dt")
        assert meta is not None
        dt_col = meta.columns["created_at"]
        assert dt_col.sample_values == []
        assert dt_col.unique_count == 2


class TestInferDependenciesProvider:
    """Cover branch 192->195: provider/prvdr column triggers provider dependency."""

    @pytest.mark.unit
    def test_provider_column_adds_dependency(self):
        """Columns with 'provider' in name add 'provider' dependency."""
        storage = MagicMock()
        gen = MockDataGenerator(storage=storage)
        columns = {
            "provider_npi": ColumnMetadata(name="provider_npi", dtype="String", null_count=0, null_percentage=0.0),
            "amount": ColumnMetadata(name="amount", dtype="Float64", null_count=0, null_percentage=0.0),
        }
        deps = gen._infer_dependencies(columns)
        assert "provider" in deps

    @pytest.mark.unit
    def test_prvdr_column_adds_dependency(self):
        """Columns with 'prvdr' in name add 'provider' dependency."""
        storage = MagicMock()
        gen = MockDataGenerator(storage=storage)
        columns = {
            "prvdr_id": ColumnMetadata(name="prvdr_id", dtype="String", null_count=0, null_percentage=0.0),
        }
        deps = gen._infer_dependencies(columns)
        assert "provider" in deps

    @pytest.mark.unit
    def test_clm_column_without_provider(self):
        """Cover 192->195: clm column present but no provider columns -> claims dep added, then falls through to provider check which is false."""
        storage = MagicMock()
        gen = MockDataGenerator(storage=storage)
        columns = {
            "clm_id": ColumnMetadata(name="clm_id", dtype="String", null_count=0, null_percentage=0.0),
            "amount": ColumnMetadata(name="amount", dtype="Float64", null_count=0, null_percentage=0.0),
        }
        deps = gen._infer_dependencies(columns)
        assert "claims" in deps
        assert "provider" not in deps

    @pytest.mark.unit
    def test_no_dependencies_at_all(self):
        """No matching patterns -> empty dependencies list."""
        storage = MagicMock()
        gen = MockDataGenerator(storage=storage)
        columns = {
            "amount": ColumnMetadata(name="amount", dtype="Float64", null_count=0, null_percentage=0.0),
        }
        deps = gen._infer_dependencies(columns)
        assert deps == []


class TestGenerateSyntheticValueDatetime:
    """Cover branch 295->305: Datetime type generates datetime value."""

    @pytest.mark.unit
    def test_datetime_column_generates_datetime(self):
        """A date column with Datetime dtype generates a datetime object."""
        storage = MagicMock()
        gen = MockDataGenerator(storage=storage)
        col_meta = ColumnMetadata(
            name="event_date",
            dtype="Datetime",
            null_count=0,
            null_percentage=0.0,
            sample_values=[],
            unique_count=10,
        )
        # Run many times to ensure we hit the branch (null_percentage=0 so never None)
        results = [gen.generate_synthetic_value(col_meta, "event_date") for _ in range(10)]
        for r in results:
            assert isinstance(r, datetime)

    @pytest.mark.unit
    def test_date_column_without_min_max(self):
        """A date column with Date dtype but no min/max generates a date."""
        storage = MagicMock()
        gen = MockDataGenerator(storage=storage)
        col_meta = ColumnMetadata(
            name="start_date",
            dtype="Date",
            null_count=0,
            null_percentage=0.0,
            sample_values=[],
            unique_count=5,
            min_value=None,
            max_value=None,
        )
        results = [gen.generate_synthetic_value(col_meta, "start_date") for _ in range(10)]
        for r in results:
            assert isinstance(r, date)

    @pytest.mark.unit
    def test_date_named_column_with_non_date_dtype(self):
        """Cover 295->305: col name has 'date' but dtype is Int64 (not Date/Datetime).
        Falls through date pattern matching and reaches Int dtype branch."""
        storage = MagicMock()
        gen = MockDataGenerator(storage=storage)
        col_meta = ColumnMetadata(
            name="date_count",
            dtype="Int64",
            null_count=0,
            null_percentage=0.0,
            sample_values=[],
            unique_count=5,
        )
        results = [gen.generate_synthetic_value(col_meta, "date_count") for _ in range(10)]
        for r in results:
            assert isinstance(r, int)


class TestInferDependenciesProviderNotMatched:
    """Cover branch 192->195: provider check doesn't match any columns."""

    @pytest.mark.unit
    def test_no_provider_or_prvdr_columns(self):
        """Branch 192->195: no columns have 'provider' or 'prvdr', so provider dep NOT added."""
        storage = MagicMock()
        gen = MockDataGenerator(storage=storage)
        columns = {
            "bene_mbi_id": ColumnMetadata(name="bene_mbi_id", dtype="String", null_count=0, null_percentage=0.0),
            "clm_id": ColumnMetadata(name="clm_id", dtype="String", null_count=0, null_percentage=0.0),
            "amount": ColumnMetadata(name="amount", dtype="Float64", null_count=0, null_percentage=0.0),
        }
        deps = gen._infer_dependencies(columns)
        assert "beneficiary" in deps
        assert "claims" in deps
        assert "provider" not in deps

    @pytest.mark.unit
    def test_claims_already_present_not_duplicated(self):
        """Branch 192: 'claims' already in dependencies from beneficiary check, not duplicated.

        This exercises the `if 'claims' not in dependencies` guard on line 192.
        """
        storage = MagicMock()
        gen = MockDataGenerator(storage=storage)
        # bene_mbi_id adds 'beneficiary', clm_line adds 'claims'
        # A second clm column shouldn't duplicate 'claims'
        columns = {
            "clm_id": ColumnMetadata(name="clm_id", dtype="String", null_count=0, null_percentage=0.0),
            "clm_line_num": ColumnMetadata(name="clm_line_num", dtype="Int64", null_count=0, null_percentage=0.0),
        }
        deps = gen._infer_dependencies(columns)
        assert deps.count("claims") == 1
