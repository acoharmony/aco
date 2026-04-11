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


class TestSeedDeterminism:
    """Seed support: same seed → identical fixtures across runs."""

    def _scan_fixture(self, tmp_path: Path, seed: int | None) -> pl.DataFrame:
        """Build a generator, seed it, scan a synthetic parquet, generate a frame."""
        # Write a source parquet with a known schema so scan_table_metadata has
        # something to populate the metadata cache with.
        src = tmp_path / f"fix_{seed}.parquet"
        pl.DataFrame(
            {
                "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
                "name": ["alpha", "beta", "gamma", "delta", "eps", "zeta",
                         "eta", "theta", "iota", "kappa"],
                "amount": [1.0, 2.5, 3.0, 4.25, 5.5, 6.75, 7.0, 8.5, 9.25, 10.0],
            }
        ).write_parquet(src)

        gen = MockDataGenerator(storage=MagicMock(), seed=seed)
        meta = gen.scan_table_metadata(src, "silver", "fix")
        gen.metadata_cache["fix"] = meta
        return gen.generate_synthetic_dataframe("fix", n_rows=25)

    @pytest.mark.unit
    def test_same_seed_produces_identical_output(self, tmp_path):
        """Two generators seeded with the same integer produce identical frames."""
        a = self._scan_fixture(tmp_path, seed=42)
        b = self._scan_fixture(tmp_path, seed=42)
        assert a.to_dicts() == b.to_dicts()

    @pytest.mark.unit
    def test_different_seeds_produce_different_output(self, tmp_path):
        """Distinct seeds produce distinct frames (overwhelmingly likely)."""
        a = self._scan_fixture(tmp_path, seed=1)
        b = self._scan_fixture(tmp_path, seed=2)
        assert a.to_dicts() != b.to_dicts()

    @pytest.mark.unit
    def test_unseeded_generator_still_works(self, tmp_path):
        """seed=None uses the shared random state and the generator still runs."""
        gen = MockDataGenerator(storage=MagicMock(), seed=None)
        src = tmp_path / "unseeded.parquet"
        pl.DataFrame({"id": [1, 2, 3], "x": ["a", "b", "c"]}).write_parquet(src)
        meta = gen.scan_table_metadata(src, "silver", "unseeded")
        gen.metadata_cache["unseeded"] = meta
        df = gen.generate_synthetic_dataframe("unseeded", n_rows=5)
        assert df.height == 5
        assert set(df.columns) == {"id", "x"}

    @pytest.mark.unit
    def test_seed_does_not_mutate_global_random_state(self, tmp_path):
        """Seeding the generator must not clobber the global random module.

        We rely on an isolated random.Random instance. To prove it, snapshot
        a value from the global random stream, run a seeded generator, and
        confirm the next value from the global stream is unaffected.
        """
        import random as _random

        _random.seed(12345)  # prime the global stream
        before = _random.random()

        _random.seed(12345)  # rewind
        _ = self._scan_fixture(tmp_path, seed=999)
        after = _random.random()

        assert before == after, (
            "MockDataGenerator(seed=...) leaked into the global random state"
        )

    @pytest.mark.unit
    def test_seed_affects_generate_synthetic_value(self):
        """generate_synthetic_value is deterministic per-instance given a seed."""
        col = ColumnMetadata(
            name="amount",
            dtype="Float64",
            null_count=0,
            null_percentage=0.0,
            min_value=1.0,
            max_value=1000.0,
        )
        g1 = MockDataGenerator(storage=MagicMock(), seed=7)
        g2 = MockDataGenerator(storage=MagicMock(), seed=7)
        seq1 = [g1.generate_synthetic_value(col, "amount") for _ in range(20)]
        seq2 = [g2.generate_synthetic_value(col, "amount") for _ in range(20)]
        assert seq1 == seq2


class TestGenerateTestMocksSeedArgument:
    """generate_test_mocks() forwards the seed argument to MockDataGenerator."""

    @pytest.mark.unit
    def test_generate_test_mocks_accepts_seed(self, tmp_path, monkeypatch):
        """The top-level entry point accepts and forwards seed=."""
        from acoharmony._dev.test import mocks as mocks_mod

        captured = {}

        class _StubGenerator:
            def __init__(self, storage=None, sample_size=1000,
                         sample_values_per_column=20, seed=None):
                captured["seed"] = seed
                self.metadata_cache = {}

            def scan_all_tables(self, layers=None):
                return {}

        monkeypatch.setattr(mocks_mod, "MockDataGenerator", _StubGenerator)
        mocks_mod.generate_test_mocks(
            layers=["silver"],
            output_dir=str(tmp_path),
            dry_run=True,
            seed=777,
        )
        assert captured["seed"] == 777
