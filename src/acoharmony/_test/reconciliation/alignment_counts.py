# © 2025 HarmonyCares
# All rights reserved.

"""Reconcile alignment counts across BAR, ALR, PAER, and consolidated alignment."""

import polars as pl
import pytest

from .conftest import GOLD, SILVER, requires_data, scan_gold, scan_silver


@requires_data
class TestBarReconciliation:
    """Reconcile BAR beneficiary counts."""

    @pytest.fixture
    def bar(self):
        try:
            return scan_silver("bar").collect()
        except Exception:
            pytest.skip("bar not available")

    @pytest.mark.reconciliation
    def test_bar_has_beneficiaries(self, bar):
        assert bar.height > 0
        mbi_col = next((c for c in bar.columns if "mbi" in c.lower()), None)
        assert mbi_col is not None

    @pytest.mark.reconciliation
    def test_bar_unique_mbi_per_delivery(self, bar):
        """Within each delivery file, each MBI should appear exactly once."""
        mbi_col = next(c for c in bar.columns if "mbi" in c.lower())
        if "source_filename" not in bar.columns:
            pytest.skip("No source_filename to group by")
        # Check per-delivery uniqueness (BAR has one row per bene per delivery)
        dups = (
            bar.group_by("source_filename", mbi_col)
            .len()
            .filter(pl.col("len") > 1)
        )
        assert dups.height == 0, (
            f"{dups.height} duplicate MBIs within same delivery file"
        )

    @pytest.mark.reconciliation
    def test_bar_has_alignment_dates(self, bar):
        date_cols = [c for c in bar.columns if "date" in c.lower() or "start" in c.lower()]
        assert len(date_cols) > 0


@requires_data
class TestAlrReconciliation:
    """Reconcile ALR assignment counts."""

    @pytest.mark.reconciliation
    def test_alr_has_beneficiaries(self):
        try:
            alr = scan_silver("alr").collect()
        except Exception:
            pytest.skip("alr not available")
        assert alr.height > 0
        mbi_col = next((c for c in alr.columns if "mbi" in c.lower()), None)
        assert mbi_col is not None


@requires_data
class TestConsolidatedAlignmentReconciliation:
    """Reconcile consolidated alignment against source files."""

    @pytest.fixture
    def consolidated(self):
        try:
            return scan_gold("consolidated_alignment").collect()
        except Exception:
            pytest.skip("consolidated_alignment not available")

    @pytest.mark.reconciliation
    def test_consolidated_has_data(self, consolidated):
        assert consolidated.height > 0

    @pytest.mark.reconciliation
    def test_consolidated_no_duplicate_mbis(self, consolidated):
        mbi_col = next((c for c in consolidated.columns if c in ("current_mbi", "bene_mbi")), None)
        if mbi_col is None:
            pytest.skip("No MBI column found")
        total = consolidated.height
        unique = consolidated[mbi_col].n_unique()
        assert total == unique, f"{total - unique} duplicate MBIs"

    @pytest.mark.reconciliation
    def test_consolidated_has_program_column(self, consolidated):
        assert "current_program" in consolidated.columns

    @pytest.mark.reconciliation
    def test_consolidated_program_distribution(self, consolidated):
        dist = consolidated.group_by("current_program").len().sort("len", descending=True)
        programs = dist["current_program"].to_list()
        assert len(programs) >= 1

    @pytest.mark.reconciliation
    def test_bar_mbis_in_consolidated(self, consolidated):
        """Most BAR MBIs from the latest delivery should appear in consolidated."""
        try:
            bar = scan_silver("bar").collect()
        except Exception:
            pytest.skip("bar not available")
        bar_col = next(c for c in bar.columns if "mbi" in c.lower())
        cons_col = next(c for c in consolidated.columns if c in ("current_mbi", "bene_mbi"))
        # Use only the latest BAR delivery for comparison (avoids historical churn)
        if "source_filename" in bar.columns:
            latest_file = bar["source_filename"].sort().to_list()[-1]
            bar = bar.filter(pl.col("source_filename") == latest_file)
        bar_mbis = set(bar[bar_col].drop_nulls().to_list())
        cons_mbis = set(consolidated[cons_col].drop_nulls().to_list())
        missing_pct = len(bar_mbis - cons_mbis) / len(bar_mbis) * 100 if bar_mbis else 0
        # Latest BAR delivery MBIs should mostly appear in consolidated
        assert missing_pct < 15, f"{missing_pct:.1f}% of latest BAR MBIs missing from consolidated"


@requires_data
class TestCclf8VsConsolidated:
    """Reconcile CCLF8 demographics against consolidated alignment."""

    @pytest.mark.reconciliation
    def test_cclf8_mbis_coverage(self):
        try:
            cclf8 = scan_silver("cclf8").collect()
            consolidated = scan_gold("consolidated_alignment").collect()
        except Exception:
            pytest.skip("Required tables not available")
        c8_col = next((c for c in cclf8.columns if "mbi" in c.lower()), None)
        ca_col = next((c for c in consolidated.columns if c in ("current_mbi", "bene_mbi")), None)
        if not c8_col or not ca_col:
            pytest.skip("MBI columns not found")
        c8_mbis = set(cclf8[c8_col].drop_nulls().to_list())
        ca_mbis = set(consolidated[ca_col].drop_nulls().to_list())
        missing_pct = len(c8_mbis - ca_mbis) / len(c8_mbis) * 100 if c8_mbis else 0
        assert missing_pct < 20, f"{missing_pct:.1f}% of CCLF8 MBIs missing"
