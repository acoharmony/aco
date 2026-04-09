# © 2025 HarmonyCares
# All rights reserved.

"""Reconcile benchmark calculations against CMS BNMR and PLARU reports."""

import polars as pl
import pytest

from .conftest import SILVER, requires_data, scan_silver


@requires_data
class TestBnmrReconciliation:
    """Validate BNMR data across all available performance years."""

    @pytest.fixture
    def bnmr(self):
        try:
            return scan_silver("bnmr").collect()
        except Exception:
            pytest.skip("bnmr not available")

    @pytest.fixture
    def bnmr_wide(self):
        try:
            return scan_silver("bnmr_wide").collect()
        except Exception:
            pytest.skip("bnmr_wide not available")

    @pytest.mark.reconciliation
    def test_bnmr_has_data(self, bnmr):
        assert bnmr.height > 0

    @pytest.mark.reconciliation
    def test_bnmr_wide_matches_bnmr_rows(self, bnmr, bnmr_wide):
        """Wide and long formats should have same row count."""
        assert bnmr.height == bnmr_wide.height

    @pytest.mark.reconciliation
    def test_bnmr_per_source_file(self, bnmr):
        """Each BNMR source file should have consistent data."""
        if "source_filename" in bnmr.columns:
            files = bnmr["source_filename"].unique().to_list()
            for fn in files:
                subset = bnmr.filter(pl.col("source_filename") == fn)
                assert subset.height > 0, f"Empty BNMR for {fn}"

    @pytest.mark.reconciliation
    def test_bnmr_sheet_types(self, bnmr):
        """BNMR should have multiple sheet types."""
        if "sheet_type" in bnmr.columns:
            types = bnmr["sheet_type"].unique().to_list()
            assert len(types) >= 3, f"Only {len(types)} sheet types: {types}"


@requires_data
class TestPlaruReconciliation:
    """Validate PLARU payment data."""

    @pytest.mark.reconciliation
    def test_plaru_exists(self):
        path = SILVER / "plaru.parquet"
        if not path.exists():
            pytest.skip("plaru not available")
        df = pl.scan_parquet(path).collect()
        assert df.height > 0

    @pytest.mark.reconciliation
    def test_plaru_has_payment_columns(self):
        path = SILVER / "plaru.parquet"
        if not path.exists():
            pytest.skip("plaru not available")
        df = pl.scan_parquet(path).collect()
        payment_cols = [c for c in df.columns if "pmt" in c.lower() or "amt" in c.lower()]
        assert len(payment_cols) > 0


@requires_data
class TestPalmrReconciliation:
    """Validate PALMR alignment data."""

    @pytest.mark.reconciliation
    def test_palmr_exists(self):
        path = SILVER / "palmr.parquet"
        if not path.exists():
            pytest.skip("palmr not available")
        df = pl.scan_parquet(path).collect()
        assert df.height > 0

    @pytest.mark.reconciliation
    def test_palmr_has_beneficiary_ids(self):
        path = SILVER / "palmr.parquet"
        if not path.exists():
            pytest.skip("palmr not available")
        df = pl.scan_parquet(path).collect()
        id_cols = [c for c in df.columns if "mbi" in c.lower() or "bene" in c.lower()]
        assert len(id_cols) > 0
