# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._transforms._financial_expenditure_by_cms_claim_type."""

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

from acoharmony._transforms._financial_expenditure_by_cms_claim_type import (
    build_bene_attribution_asof,
    build_financial_expenditure_by_cms_claim_type,
)


# ---------------------------------------------------------------------------
# Fixture builders: small, schemaed frames for each input source.
# ---------------------------------------------------------------------------

_CCLF1_SCHEMA = {
    "bene_mbi_id": pl.Utf8,
    "clm_type_cd": pl.Utf8,
    "clm_from_dt": pl.Date,
    "clm_pmt_amt": pl.Float64,
    "clm_adjsmt_type_cd": pl.Utf8,
    "file_date": pl.Utf8,
}

_CCLF_LINE_SCHEMA = {
    "bene_mbi_id": pl.Utf8,
    "clm_type_cd": pl.Utf8,
    "clm_from_dt": pl.Date,
    "clm_line_cvrd_pd_amt": pl.Float64,
    "clm_adjsmt_type_cd": pl.Utf8,
    "file_date": pl.Utf8,
}

_BAR_SCHEMA = {
    "bene_mbi": pl.Utf8,
    "start_date": pl.Date,
    "end_date": pl.Date,
    "source_filename": pl.Utf8,
    "file_date": pl.Utf8,
}

_ALR_SCHEMA = {
    "bene_mbi": pl.Utf8,
    "source_filename": pl.Utf8,
    "file_date": pl.Utf8,
}


def _cclf1_frame(rows: list[dict]) -> pl.LazyFrame:
    defaults = {
        "bene_mbi_id": "1AAA",
        "clm_type_cd": "60",
        "clm_from_dt": date(2024, 3, 15),
        "clm_pmt_amt": 1000.0,
        "clm_adjsmt_type_cd": "0",
        "file_date": "2024-06-01",
    }
    return pl.LazyFrame([{**defaults, **r} for r in rows], schema=_CCLF1_SCHEMA)


def _cclf5_frame(rows: list[dict]) -> pl.LazyFrame:
    defaults = {
        "bene_mbi_id": "1AAA",
        "clm_type_cd": "71",
        "clm_from_dt": date(2024, 3, 15),
        "clm_line_cvrd_pd_amt": 100.0,
        "clm_adjsmt_type_cd": "0",
        "file_date": "2024-06-01",
    }
    return pl.LazyFrame([{**defaults, **r} for r in rows], schema=_CCLF_LINE_SCHEMA)


def _cclf6_frame(rows: list[dict]) -> pl.LazyFrame:
    defaults = {
        "bene_mbi_id": "1AAA",
        "clm_type_cd": "82",
        "clm_from_dt": date(2024, 3, 15),
        "clm_line_cvrd_pd_amt": 50.0,
        "clm_adjsmt_type_cd": "0",
        "file_date": "2024-06-01",
    }
    return pl.LazyFrame([{**defaults, **r} for r in rows], schema=_CCLF_LINE_SCHEMA)


def _bar_frame(rows: list[dict]) -> pl.LazyFrame:
    defaults = {
        "bene_mbi": "1AAA",
        "start_date": date(2024, 1, 1),
        "end_date": None,
        "source_filename": "P.D0259.ALGC24.RP.D240601.T1111111.xlsx",
        "file_date": "2024-06-01",
    }
    return pl.LazyFrame([{**defaults, **r} for r in rows], schema=_BAR_SCHEMA)


def _alr_frame(rows: list[dict]) -> pl.LazyFrame:
    defaults = {
        "bene_mbi": "1BBB",
        "source_filename": "P.A2671.ACO.QALR.2024Q1.D240101.T0000000_1-2.csv",
        "file_date": "2024-01-01",
    }
    return pl.LazyFrame([{**defaults, **r} for r in rows], schema=_ALR_SCHEMA)


# ===========================================================================
# build_bene_attribution_asof — per-bene-month attribution (REACH + MSSP)
# ===========================================================================


class TestBeneAttributionOutputShape:
    @pytest.mark.unit
    def test_output_columns(self):
        bar = _bar_frame([{}])
        alr = _alr_frame([{}])
        result = build_bene_attribution_asof(bar, alr, as_of_cutoff="2024-12-31").collect()
        assert set(result.columns) == {
            "bene_mbi",
            "aco_id",
            "program",
            "performance_year",
            "year_month",
        }


class TestReachBeneAttribution:
    @pytest.mark.unit
    def test_group_with_no_active_months_emits_empty_frame(self):
        """BAR has a delivery for (D0259, PY2024) but the bene's alignment
        window (Jan 1-1) is narrower than any single-month-end check, so
        no month produces rows. Covers the _reach_attribution_for_group
        empty-frames fallback.
        """
        bar = _bar_frame(
            [
                {
                    "bene_mbi": "R1",
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 1, 1),  # same day → never 'active at month_end'
                    "source_filename": "P.D0259.ALGC24.RP.D240601.T1111111.xlsx",
                    "file_date": "2024-06-01",
                }
            ]
        )
        alr = _alr_frame([])
        result = (
            build_bene_attribution_asof(bar, alr, as_of_cutoff="2024-06-30")
            .collect()
            .filter(pl.col("program") == "REACH")
        )
        assert result.height == 0

    @pytest.mark.unit
    def test_open_ended_bene_attributed_every_month(self):
        """One REACH bene, active Jan onward, cutoff June 30 → 6 attribution rows."""
        bar = _bar_frame(
            [
                {
                    "bene_mbi": "R1",
                    "start_date": date(2024, 1, 1),
                    "end_date": None,
                    "source_filename": "P.D0259.ALGC24.RP.D240601.T1111111.xlsx",
                    "file_date": "2024-06-01",
                }
            ]
        )
        alr = _alr_frame([])
        result = (
            build_bene_attribution_asof(bar, alr, as_of_cutoff="2024-06-30")
            .collect()
            .filter(pl.col("program") == "REACH")
            .sort("year_month")
        )
        assert result["bene_mbi"].to_list() == ["R1"] * 6
        assert result["aco_id"].to_list() == ["D0259"] * 6
        assert result["year_month"].to_list() == [
            202401, 202402, 202403, 202404, 202405, 202406,
        ]

    @pytest.mark.unit
    def test_churned_bene_only_attributed_until_end_date(self):
        bar = _bar_frame(
            [
                {
                    "bene_mbi": "R1",
                    "start_date": date(2024, 1, 1),
                    "end_date": date(2024, 4, 1),  # out starting April
                    "source_filename": "P.D0259.ALGC24.RP.D240601.T1111111.xlsx",
                    "file_date": "2024-06-01",
                }
            ]
        )
        alr = _alr_frame([])
        result = (
            build_bene_attribution_asof(bar, alr, as_of_cutoff="2024-06-30")
            .collect()
            .filter(pl.col("program") == "REACH")
            .sort("year_month")
        )
        assert result["year_month"].to_list() == [202401, 202402, 202403]


class TestMsspBeneAttribution:
    @pytest.mark.unit
    def test_cutoff_before_py_start_yields_empty_group(self):
        """MSSP ALR delivery exists for PY2024 but cutoff is 2023-12-31
        (before any PY2024 month exists). The group is consulted but the
        month-end grid is empty → no attribution rows emitted for that group.
        """
        alr = _alr_frame(
            [
                {
                    "bene_mbi": "M1",
                    "source_filename": "P.A2671.ACO.QALR.2024Q1.D231201.T0000000_1-2.csv",
                    "file_date": "2023-12-01",
                }
            ]
        )
        bar = _bar_frame([])
        result = (
            build_bene_attribution_asof(bar, alr, as_of_cutoff="2023-12-31")
            .collect()
        )
        # Group existed but produced zero rows — the fallback branch.
        assert result.height == 0

    @pytest.mark.unit
    def test_roster_bene_attributed_every_py_month_through_cutoff(self):
        alr = _alr_frame(
            [
                {
                    "bene_mbi": "M1",
                    "source_filename": "P.A2671.ACO.QALR.2024Q1.D240101.T0000000_1-2.csv",
                    "file_date": "2024-01-01",
                }
            ]
        )
        bar = _bar_frame([])
        result = (
            build_bene_attribution_asof(bar, alr, as_of_cutoff="2024-04-30")
            .collect()
            .filter(pl.col("program") == "MSSP")
            .sort("year_month")
        )
        assert result["bene_mbi"].to_list() == ["M1"] * 4
        assert result["aco_id"].to_list() == ["A2671"] * 4
        assert result["year_month"].to_list() == [202401, 202402, 202403, 202404]


# ===========================================================================
# build_financial_expenditure_by_cms_claim_type — the full gold transform
# ===========================================================================


class TestOutputShape:
    @pytest.mark.unit
    def test_output_columns(self):
        """Emits (aco_id, program, performance_year, year_month, clm_type_cd,
        total_spend, member_months, pbpm)."""
        cclf1 = _cclf1_frame([{}])
        cclf5 = _cclf5_frame([])
        cclf6 = _cclf6_frame([])
        bar = _bar_frame([{}])
        alr = _alr_frame([])
        result = build_financial_expenditure_by_cms_claim_type(
            cclf1, cclf5, cclf6, bar, alr, as_of_cutoff="2024-12-31"
        ).collect()
        assert set(result.columns) == {
            "aco_id",
            "program",
            "performance_year",
            "year_month",
            "clm_type_cd",
            "total_spend",
            "member_months",
            "pbpm",
        }


class TestAttributionAndSpendJoin:
    """Core logic: spend lands in the right ACO bucket and PBPM is correct."""

    @pytest.mark.unit
    def test_single_bene_single_claim_produces_single_bucket(self):
        """One bene, one $1000 inpatient claim in March, aligned to D0259 → one row."""
        cclf1 = _cclf1_frame(
            [
                {
                    "bene_mbi_id": "R1",
                    "clm_type_cd": "60",
                    "clm_from_dt": date(2024, 3, 15),
                    "clm_pmt_amt": 1000.0,
                    "clm_adjsmt_type_cd": "0",
                    "file_date": "2024-06-01",
                }
            ]
        )
        cclf5 = _cclf5_frame([])
        cclf6 = _cclf6_frame([])
        bar = _bar_frame(
            [
                {
                    "bene_mbi": "R1",
                    "start_date": date(2024, 1, 1),
                    "end_date": None,
                    "source_filename": "P.D0259.ALGC24.RP.D240601.T1111111.xlsx",
                    "file_date": "2024-06-01",
                }
            ]
        )
        alr = _alr_frame([])
        result = build_financial_expenditure_by_cms_claim_type(
            cclf1, cclf5, cclf6, bar, alr, as_of_cutoff="2024-06-30"
        ).collect()
        # Single row: D0259 / REACH / PY2024 / 202403 / 60
        assert result.height == 1
        row = result.row(0, named=True)
        assert row["aco_id"] == "D0259"
        assert row["program"] == "REACH"
        assert row["performance_year"] == 2024
        assert row["year_month"] == 202403
        assert row["clm_type_cd"] == "60"
        assert float(row["total_spend"]) == pytest.approx(1000.0)
        assert row["member_months"] == 1
        assert float(row["pbpm"]) == pytest.approx(1000.0)

    @pytest.mark.unit
    def test_unaligned_bene_spend_is_dropped(self):
        """A bene with claims but no alignment in BAR/ALR is dropped, not
        silently attributed to some random ACO.
        """
        cclf1 = _cclf1_frame(
            [
                {"bene_mbi_id": "NOBODY", "clm_pmt_amt": 9999.0},
            ]
        )
        bar = _bar_frame([])  # no alignment at all
        alr = _alr_frame([])
        result = build_financial_expenditure_by_cms_claim_type(
            cclf1, _cclf5_frame([]), _cclf6_frame([]), bar, alr,
            as_of_cutoff="2024-12-31",
        ).collect()
        assert result.height == 0

    @pytest.mark.unit
    def test_multiple_benes_same_aco_sum(self):
        """Two benes in the same ACO with claims in the same month and bucket
        should sum: $1000 + $500 = $1500 total_spend, 2 member_months, $750 PBPM.
        """
        cclf1 = _cclf1_frame(
            [
                {"bene_mbi_id": "R1", "clm_pmt_amt": 1000.0, "clm_from_dt": date(2024, 3, 10)},
                {"bene_mbi_id": "R2", "clm_pmt_amt": 500.0, "clm_from_dt": date(2024, 3, 20)},
            ]
        )
        bar = _bar_frame(
            [
                {"bene_mbi": "R1", "start_date": date(2024, 1, 1), "end_date": None,
                 "source_filename": "P.D0259.ALGC24.RP.D240601.T1111111.xlsx",
                 "file_date": "2024-06-01"},
                {"bene_mbi": "R2", "start_date": date(2024, 1, 1), "end_date": None,
                 "source_filename": "P.D0259.ALGC24.RP.D240601.T1111111.xlsx",
                 "file_date": "2024-06-01"},
            ]
        )
        result = build_financial_expenditure_by_cms_claim_type(
            cclf1, _cclf5_frame([]), _cclf6_frame([]), bar, _alr_frame([]),
            as_of_cutoff="2024-06-30",
        ).collect().filter(pl.col("year_month") == 202403)
        assert result.height == 1
        row = result.row(0, named=True)
        assert float(row["total_spend"]) == pytest.approx(1500.0)
        assert row["member_months"] == 2
        assert float(row["pbpm"]) == pytest.approx(750.0)

    @pytest.mark.unit
    def test_reach_and_mssp_benes_do_not_mix(self):
        """A REACH bene and an MSSP bene with the same month/bucket stay in
        their own ACO rows — no cross-program contamination."""
        cclf1 = _cclf1_frame(
            [
                {"bene_mbi_id": "R1", "clm_pmt_amt": 1000.0, "clm_from_dt": date(2024, 3, 10)},
                {"bene_mbi_id": "M1", "clm_pmt_amt": 500.0, "clm_from_dt": date(2024, 3, 20)},
            ]
        )
        bar = _bar_frame(
            [
                {"bene_mbi": "R1", "start_date": date(2024, 1, 1), "end_date": None,
                 "source_filename": "P.D0259.ALGC24.RP.D240601.T1111111.xlsx",
                 "file_date": "2024-06-01"},
            ]
        )
        alr = _alr_frame(
            [
                {"bene_mbi": "M1",
                 "source_filename": "P.A2671.ACO.QALR.2024Q1.D240101.T0000000_1-2.csv",
                 "file_date": "2024-01-01"},
            ]
        )
        result = (
            build_financial_expenditure_by_cms_claim_type(
                cclf1, _cclf5_frame([]), _cclf6_frame([]), bar, alr,
                as_of_cutoff="2024-06-30",
            )
            .collect()
            .filter(pl.col("year_month") == 202403)
            .sort("aco_id")
        )
        assert result.height == 2
        assert result["aco_id"].to_list() == ["A2671", "D0259"]
        assert result["program"].to_list() == ["MSSP", "REACH"]
        spends = [float(v) for v in result["total_spend"].to_list()]
        assert spends == [pytest.approx(500.0), pytest.approx(1000.0)]


class TestPointInTimeConsistency:
    """Point-in-time cutoff applies consistently to CCLF, BAR, and ALR."""

    @pytest.mark.unit
    def test_cclf_row_from_future_delivery_is_dropped(self):
        """A claim that exists in the silver CCLF but whose file_date is AFTER
        the cutoff must not appear in a historical reconciliation.
        """
        cclf1 = _cclf1_frame(
            [
                {
                    "bene_mbi_id": "R1",
                    "clm_pmt_amt": 1000.0,
                    "clm_from_dt": date(2024, 3, 10),
                    "file_date": "2024-01-01",
                },
                {
                    "bene_mbi_id": "R1",
                    "clm_pmt_amt": 999.0,
                    "clm_from_dt": date(2024, 3, 20),
                    "file_date": "2024-12-31",  # after cutoff
                },
            ]
        )
        bar = _bar_frame(
            [
                {
                    "bene_mbi": "R1",
                    "start_date": date(2024, 1, 1),
                    "end_date": None,
                    "source_filename": "P.D0259.ALGC24.RP.D240101.T1111111.xlsx",
                    "file_date": "2024-01-01",
                }
            ]
        )
        result = build_financial_expenditure_by_cms_claim_type(
            cclf1, _cclf5_frame([]), _cclf6_frame([]), bar, _alr_frame([]),
            as_of_cutoff="2024-03-31",
        ).collect()
        # Only the 2024-01-01 delivery's $1000 claim survives
        assert result.height == 1
        assert float(result["total_spend"][0]) == pytest.approx(1000.0)


class TestRealFixtureSmoke:
    """End-to-end smoke test against committed fixtures."""

    FIXTURE_DIR = Path(__file__).parent.parent / "_fixtures" / "reconciliation"

    @pytest.mark.unit
    def test_runs_against_committed_fixtures(self):
        paths = {
            "cclf1": self.FIXTURE_DIR / "cclf1.parquet",
            "cclf5": self.FIXTURE_DIR / "cclf5.parquet",
            "cclf6": self.FIXTURE_DIR / "cclf6.parquet",
            "bar": self.FIXTURE_DIR / "bar.parquet",
            "alr": self.FIXTURE_DIR / "alr.parquet",
        }
        for name, p in paths.items():
            if not p.exists():
                pytest.skip(f"reconciliation fixture {name} not generated")
        result = build_financial_expenditure_by_cms_claim_type(
            pl.scan_parquet(paths["cclf1"]),
            pl.scan_parquet(paths["cclf5"]),
            pl.scan_parquet(paths["cclf6"]),
            pl.scan_parquet(paths["bar"]),
            pl.scan_parquet(paths["alr"]),
            as_of_cutoff="2099-12-31",
        ).collect()
        # Output schema invariant; the synthetic fixtures aren't business-
        # semantically coherent so we don't assert specific values. The
        # smoke test is really checking "the pipeline does not crash".
        assert set(result.columns) == {
            "aco_id",
            "program",
            "performance_year",
            "year_month",
            "clm_type_cd",
            "total_spend",
            "member_months",
            "pbpm",
        }
