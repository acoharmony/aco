# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._transforms._bnmr_reconciliation_view."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from acoharmony._transforms._bnmr_reconciliation_view import (
    build_bnmr_reconciliation_view,
)


def _bnmr_claims_frame(rows: list[dict]) -> pl.LazyFrame:
    """Build a reach_bnmr_claims-shaped LazyFrame."""
    defaults = {
        "perf_yr": "2024",
        "clndr_yr": "2024",
        "clndr_mnth": "3",
        "bnmrk": "AD",
        "align_type": "C",
        "bnmrk_type": "RATEBOOK",
        "aco_id": "D0259",
        "clm_type_cd": "60",
        "clm_pmt_amt_agg": 1000.0,
        "sqstr_amt_agg": 0.0,
        "apa_rdctn_amt_agg": 0.0,
        "ucc_amt_agg": 0.0,
        "op_dsh_amt_agg": 0.0,
        "cp_dsh_amt_agg": 0.0,
        "op_ime_amt_agg": 0.0,
        "cp_ime_amt_agg": 0.0,
        "nonpbp_rdct_amt_agg": 0.0,
        "aco_amt_agg_apa": 0.0,
        "file_date": "2024-12-31",
    }
    return pl.LazyFrame([{**defaults, **r} for r in rows])


class TestOutputShape:
    @pytest.mark.unit
    def test_derived_columns_present(self):
        """Emits net_expenditure, claim_type_label, claim_type_part,
        srvc_month_date, program alongside the original dim columns.
        """
        bnmr = _bnmr_claims_frame([{}])
        result = build_bnmr_reconciliation_view(bnmr).collect()
        for col in [
            "net_expenditure",
            "claim_type_label",
            "claim_type_part",
            "srvc_month_date",
            "program",
        ]:
            assert col in result.columns, f"missing derived col: {col}"

    @pytest.mark.unit
    def test_row_count_matches_input(self):
        """Transform is row-preserving — no join explosion or filter."""
        bnmr = _bnmr_claims_frame(
            [
                {"clm_type_cd": "60"},
                {"clm_type_cd": "20"},
                {"clm_type_cd": "71"},
            ]
        )
        result = build_bnmr_reconciliation_view(bnmr).collect()
        assert result.height == 3


class TestNetExpenditureComputation:
    """net_expenditure is derived from components, not from total_exp_amt_agg."""

    @pytest.mark.unit
    def test_reductions_applied(self):
        """Gross $1000, $100 in reductions → net $900."""
        bnmr = _bnmr_claims_frame(
            [
                {
                    "clm_pmt_amt_agg": 1000.0,
                    "sqstr_amt_agg": 30.0,
                    "apa_rdctn_amt_agg": 40.0,
                    "ucc_amt_agg": 10.0,
                    "op_dsh_amt_agg": 5.0,
                    "cp_dsh_amt_agg": 5.0,
                    "op_ime_amt_agg": 5.0,
                    "cp_ime_amt_agg": 5.0,
                    "nonpbp_rdct_amt_agg": 0.0,
                    "aco_amt_agg_apa": 0.0,
                }
            ]
        )
        result = build_bnmr_reconciliation_view(bnmr).collect()
        assert float(result["net_expenditure"][0]) == pytest.approx(900.0)

    @pytest.mark.unit
    def test_aco_apa_adjustment_adds(self):
        """aco_amt_agg_apa is ADDED to the net."""
        bnmr = _bnmr_claims_frame(
            [
                {
                    "clm_pmt_amt_agg": 1000.0,
                    "aco_amt_agg_apa": 250.0,
                }
            ]
        )
        result = build_bnmr_reconciliation_view(bnmr).collect()
        assert float(result["net_expenditure"][0]) == pytest.approx(1250.0)


class TestDerivedColumnsReuseMerExpressions:
    """The shared label/part/srvc_month/program expressions work for BNMR."""

    @pytest.mark.unit
    def test_label_and_part_for_inpatient(self):
        bnmr = _bnmr_claims_frame([{"clm_type_cd": "60"}])
        row = build_bnmr_reconciliation_view(bnmr).collect().row(0, named=True)
        assert row["claim_type_label"] == "Inpatient"
        assert row["claim_type_part"] == "Part A"

    @pytest.mark.unit
    def test_label_and_part_for_dmerc(self):
        bnmr = _bnmr_claims_frame([{"clm_type_cd": "82"}])
        row = build_bnmr_reconciliation_view(bnmr).collect().row(0, named=True)
        assert row["claim_type_label"] == "DMERC/DMEPOS"
        assert row["claim_type_part"] == "Part B"

    @pytest.mark.unit
    def test_srvc_month_date_built_from_clndr_yr_mnth(self):
        bnmr = _bnmr_claims_frame(
            [{"clndr_yr": "2025", "clndr_mnth": "4"}]
        )
        row = build_bnmr_reconciliation_view(bnmr).collect().row(0, named=True)
        assert row["srvc_month_date"] == date(2025, 4, 1)

    @pytest.mark.unit
    def test_program_from_aco_id(self):
        bnmr = _bnmr_claims_frame([{"aco_id": "D0259"}])
        row = build_bnmr_reconciliation_view(bnmr).collect().row(0, named=True)
        assert row["program"] == "REACH"


class TestPointInTimeFilter:
    """``as_of_delivery_date`` filters BNMR rows by file_date."""

    @pytest.mark.unit
    def test_cutoff_drops_future_rows(self):
        bnmr = _bnmr_claims_frame(
            [
                {"file_date": "2024-06-30", "clm_pmt_amt_agg": 100.0},
                {"file_date": "2024-12-31", "clm_pmt_amt_agg": 999.0},
            ]
        )
        result = build_bnmr_reconciliation_view(
            bnmr, as_of_delivery_date="2024-06-30"
        ).collect()
        assert result.height == 1
        assert float(result["net_expenditure"][0]) == pytest.approx(100.0)

    @pytest.mark.unit
    def test_no_cutoff_keeps_all(self):
        bnmr = _bnmr_claims_frame(
            [
                {"file_date": "2024-06-30"},
                {"file_date": "2024-12-31"},
            ]
        )
        result = build_bnmr_reconciliation_view(bnmr).collect()
        assert result.height == 2


class TestRealFixtureSmoke:
    FIXTURE_DIR = Path(__file__).parent.parent / "_fixtures" / "reconciliation"

    @pytest.mark.unit
    def test_runs_against_committed_bnmr_fixture(self):
        p = self.FIXTURE_DIR / "reach_bnmr_claims.parquet"
        if not p.exists():
            pytest.skip("reach_bnmr_claims fixture not generated")
        bnmr = pl.scan_parquet(p)
        result = build_bnmr_reconciliation_view(bnmr).collect()
        assert result.height > 0
        assert "net_expenditure" in result.columns
        assert "claim_type_label" in result.columns
