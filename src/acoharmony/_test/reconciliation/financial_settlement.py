# © 2025 HarmonyCares
# All rights reserved.

"""Reconcile financial calculations against CMS reports."""

import polars as pl
import pytest

from .conftest import GOLD, SILVER, requires_data, scan_gold, scan_silver


@requires_data
class TestReconReport:
    """Validate reconciliation report data."""

    @pytest.mark.reconciliation
    def test_recon_exists(self):
        for name in ["recon", "reconciliation_report"]:
            path = SILVER / f"{name}.parquet"
            if path.exists():
                df = pl.scan_parquet(path).collect()
                assert df.height > 0
                return
        path = GOLD / "reconciliation_report.parquet"
        if path.exists():
            df = pl.scan_parquet(path).collect()
            assert df.height > 0
            return
        pytest.xfail("Reconciliation report not yet generated")


@requires_data
class TestPfsRatesReconciliation:
    """Validate PFS payment rates."""

    @pytest.mark.reconciliation
    def test_pfs_rates_exist(self):
        path = GOLD / "pfs_rates.parquet"
        if not path.exists():
            pytest.xfail("pfs_rates not yet generated in gold")
        df = pl.scan_parquet(path).collect()
        assert df.height > 0
        assert "payment_rate" in df.columns

    @pytest.mark.reconciliation
    def test_pfs_rates_positive(self):
        path = GOLD / "pfs_rates.parquet"
        if not path.exists():
            pytest.xfail("pfs_rates not yet generated in gold")
        df = pl.scan_parquet(path).collect()
        negative = df.filter(pl.col("payment_rate") < 0)
        assert negative.height == 0

    @pytest.mark.reconciliation
    def test_pfs_conversion_factor_range(self):
        path = GOLD / "pfs_rates.parquet"
        if not path.exists():
            pytest.xfail("pfs_rates not yet generated in gold")
        df = pl.scan_parquet(path).collect()
        if "conversion_factor" in df.columns:
            cf = df["conversion_factor"].drop_nulls().mean()
            assert 30 < cf < 40, f"CF {cf:.2f} outside expected $30-$40"


@requires_data
class TestFinancialPmpm:
    """Validate financial PMPM calculations."""

    @pytest.mark.reconciliation
    @pytest.mark.parametrize("table", ["financial_pmpm_by_category", "financial_pmpm_by_payer"])
    def test_pmpm_tables_exist(self, table):
        path = GOLD / f"{table}.parquet"
        if not path.exists():
            pytest.skip(f"{table} not in gold")
        df = pl.scan_parquet(path).collect()
        assert df.height > 0

    @pytest.mark.reconciliation
    def test_pmpm_values_reasonable(self):
        path = GOLD / "financial_pmpm_by_category.parquet"
        if not path.exists():
            pytest.skip("financial_pmpm_by_category not in gold")
        df = pl.scan_parquet(path).collect()
        if "pmpm" in df.columns:
            pmpm = df["pmpm"].drop_nulls()
            if pmpm.len() > 0:
                assert pmpm.mean() > 0, "Average PMPM should be positive"
                assert pmpm.mean() < 50000, "Average PMPM unreasonably high"
