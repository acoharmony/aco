# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._transforms._mer_reconciliation_view."""

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

from acoharmony._transforms._mer_reconciliation_view import (
    build_mer_reconciliation_view,
)

# Canonical small-fixture shapes used by the tests below. Two files:
# mexpr_data_claims (dims + amounts) and mexpr_data_enroll (dims + member months).
# Column set matches the real silver parquet schema for the fields the
# transform actually uses; anything unused is omitted to keep tests focused.


def _claims_frame(rows: list[dict]) -> pl.LazyFrame:
    """Build a LazyFrame that matches the mexpr_data_claims dim+measure schema."""
    default_cols = {
        "perf_yr": "2025",
        "clndr_yr": "2025",
        "clndr_mnth": "1",
        "bnmrk": "AD",
        "align_type": "C",
        "bnmrk_type": "RATEBOOK",
        "aco_id": "D0259",
        "clm_type_cd": "60",
        "total_exp_amt_agg": 1000.00,
        "clm_pmt_amt_agg": 1000.00,
        "file_date": "2025-11-30",
        "source_filename": "REACH.D0259.MEXPR.11.PY2025.D20251130.Tnnnnnn.xlsx",
    }
    filled = [{**default_cols, **row} for row in rows]
    return pl.LazyFrame(filled)


def _enroll_frame(rows: list[dict]) -> pl.LazyFrame:
    """Build a LazyFrame that matches the mexpr_data_enroll dim+measure schema."""
    default_cols = {
        "perf_yr": "2025",
        "clndr_yr": "2025",
        "clndr_mnth": "1",
        "bnmrk": "AD",
        "align_type": "C",
        "bnmrk_type": "RATEBOOK",
        "aco_id": "D0259",
        "elig_mnths": 100,
        "bene_dcnt": 100,
        "file_date": "2025-11-30",
        "source_filename": "REACH.D0259.MEXPR.11.PY2025.D20251130.Tnnnnnn.xlsx",
    }
    filled = [{**default_cols, **row} for row in rows]
    return pl.LazyFrame(filled)


class TestLongFormatShape:
    """The view produces one row per (dim-tuple, claim-type) with derived cols."""

    @pytest.mark.unit
    def test_one_row_per_input_claim_row(self):
        """No row duplication or loss — a 3-row claims frame stays 3 rows."""
        claims = _claims_frame(
            [
                {"clm_type_cd": "60", "total_exp_amt_agg": 1000.00},
                {"clm_type_cd": "20", "total_exp_amt_agg": 500.00},
                {"clm_type_cd": "40", "total_exp_amt_agg": 250.00},
            ]
        )
        enroll = _enroll_frame([{"elig_mnths": 100}])
        result = build_mer_reconciliation_view(claims, enroll).collect()
        assert result.height == 3

    @pytest.mark.unit
    def test_derived_columns_present(self):
        """Output carries label, part, net_expenditure, pbpm, srvc_month_date, program."""
        claims = _claims_frame([{"clm_type_cd": "60", "total_exp_amt_agg": 1000.00}])
        enroll = _enroll_frame([{"elig_mnths": 100}])
        result = build_mer_reconciliation_view(claims, enroll).collect()

        for col in [
            "claim_type_label",
            "claim_type_part",
            "net_expenditure",
            "pbpm",
            "srvc_month_date",
            "program",
        ]:
            assert col in result.columns, f"missing derived column: {col}"

    @pytest.mark.unit
    def test_derived_values_correct_for_known_row(self):
        """Single-row sanity check on every derived column."""
        claims = _claims_frame(
            [
                {
                    "clm_type_cd": "60",
                    "total_exp_amt_agg": 12000.00,
                    "aco_id": "D0259",
                    "clndr_yr": "2025",
                    "clndr_mnth": "3",
                }
            ]
        )
        enroll = _enroll_frame(
            [
                {
                    "elig_mnths": 120,
                    "aco_id": "D0259",
                    "clndr_yr": "2025",
                    "clndr_mnth": "3",
                }
            ]
        )
        row = build_mer_reconciliation_view(claims, enroll).collect().row(0, named=True)
        assert row["claim_type_label"] == "Inpatient"
        assert row["claim_type_part"] == "Part A"
        assert row["program"] == "REACH"
        assert row["srvc_month_date"] == date(2025, 3, 1)
        assert row["net_expenditure"] == pytest.approx(12000.00)
        assert row["pbpm"] == pytest.approx(100.00)


class TestJoinKey:
    """Join key is the full dim tuple: (perf_yr, clndr_yr, clndr_mnth, bnmrk,
    align_type, bnmrk_type, aco_id). Wrong enroll row must not bind to a claim."""

    @pytest.mark.unit
    def test_matching_dims_produce_populated_elig_mnths(self):
        claims = _claims_frame(
            [{"clndr_yr": "2025", "clndr_mnth": "1", "total_exp_amt_agg": 500.0}]
        )
        enroll = _enroll_frame(
            [{"clndr_yr": "2025", "clndr_mnth": "1", "elig_mnths": 50}]
        )
        result = build_mer_reconciliation_view(claims, enroll).collect()
        assert result["eligible_member_months"][0] == 50
        assert result["pbpm"][0] == pytest.approx(10.0)

    @pytest.mark.unit
    def test_mismatched_month_yields_null_pbpm(self):
        """Claim in Jan but enrollment only for Feb → left-join null → null PBPM."""
        claims = _claims_frame(
            [{"clndr_yr": "2025", "clndr_mnth": "1", "total_exp_amt_agg": 500.0}]
        )
        enroll = _enroll_frame(
            [{"clndr_yr": "2025", "clndr_mnth": "2", "elig_mnths": 50}]
        )
        result = build_mer_reconciliation_view(claims, enroll).collect()
        assert result["eligible_member_months"][0] is None
        assert result["pbpm"][0] is None

    @pytest.mark.unit
    def test_mismatched_bnmrk_does_not_bleed_between_rows(self):
        """AD and ESRD rows must be scoped separately on the same month."""
        claims = _claims_frame(
            [
                {"bnmrk": "AD", "total_exp_amt_agg": 1000.0},
                {"bnmrk": "ESRD", "total_exp_amt_agg": 2000.0},
            ]
        )
        enroll = _enroll_frame(
            [
                {"bnmrk": "AD", "elig_mnths": 100},
                {"bnmrk": "ESRD", "elig_mnths": 50},
            ]
        )
        result = (
            build_mer_reconciliation_view(claims, enroll)
            .collect()
            .sort("bnmrk")
        )
        # AD: 1000 / 100 = 10
        ad = result.filter(pl.col("bnmrk") == "AD").row(0, named=True)
        assert ad["pbpm"] == pytest.approx(10.0)
        # ESRD: 2000 / 50 = 40
        esrd = result.filter(pl.col("bnmrk") == "ESRD").row(0, named=True)
        assert esrd["pbpm"] == pytest.approx(40.0)


class TestPointInTimeFilter:
    """`as_of_delivery_date` keyword filters both claims and enroll to rows
    whose source file_date ≤ cutoff, enforcing no-future-data-leak."""

    @pytest.mark.unit
    def test_as_of_cutoff_drops_future_rows(self):
        """Rows from deliveries after the cutoff are dropped from BOTH inputs."""
        claims = _claims_frame(
            [
                {"clm_type_cd": "60", "file_date": "2025-06-30", "total_exp_amt_agg": 100.0},
                {"clm_type_cd": "20", "file_date": "2025-12-31", "total_exp_amt_agg": 999.0},
            ]
        )
        enroll = _enroll_frame(
            [
                {"file_date": "2025-06-30", "elig_mnths": 50},
                {"file_date": "2025-12-31", "elig_mnths": 50},
            ]
        )
        result = build_mer_reconciliation_view(
            claims, enroll, as_of_delivery_date="2025-06-30"
        ).collect()
        assert result.height == 1
        assert result["clm_type_cd"][0] == "60"
        assert result["net_expenditure"][0] == pytest.approx(100.0)

    @pytest.mark.unit
    def test_no_cutoff_keeps_everything(self):
        claims = _claims_frame(
            [
                {"clm_type_cd": "60", "file_date": "2025-06-30"},
                {"clm_type_cd": "20", "file_date": "2025-12-31"},
            ]
        )
        enroll = _enroll_frame([{"file_date": "2025-06-30"}, {"file_date": "2025-12-31"}])
        result = build_mer_reconciliation_view(claims, enroll).collect()
        assert result.height == 2


class TestRealFixtureSmoke:
    """End-to-end smoke test against the committed generate-mocks fixtures.

    These fixtures have realistic distributions but no business-semantic
    coherence (dim values are sampled independently per column). So we only
    assert the pipeline does not crash, produces the right column set, and
    emits a nonzero row count — not specific values.
    """

    FIXTURE_DIR = (
        Path(__file__).parent.parent / "_fixtures" / "reconciliation"
    )

    @pytest.mark.unit
    def test_runs_against_committed_fixtures(self):
        if not (self.FIXTURE_DIR / "mexpr_data_claims.parquet").exists():
            pytest.skip("reconciliation fixtures not generated")
        claims = pl.scan_parquet(self.FIXTURE_DIR / "mexpr_data_claims.parquet")
        enroll = pl.scan_parquet(self.FIXTURE_DIR / "mexpr_data_enroll.parquet")
        result = build_mer_reconciliation_view(claims, enroll).collect()
        assert result.height > 0
        assert "net_expenditure" in result.columns
        assert "pbpm" in result.columns
        assert "claim_type_label" in result.columns
