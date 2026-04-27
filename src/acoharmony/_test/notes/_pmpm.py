# © 2025 HarmonyCares
"""Tests for acoharmony._notes._pmpm (PmpmPlugins)."""

from __future__ import annotations

import polars as pl
import pytest

from acoharmony._notes import PmpmPlugins
from acoharmony._notes._pmpm import PROGRAM_NAMES


def _pmpm_df() -> pl.DataFrame:
    """Tiny synthetic PMPM table."""
    return pl.DataFrame(
        {
            "year_month": [202401, 202401, 202402, 202402, 202501, 202501],
            "program": ["ffs", "mssp", "ffs", "mssp", "ffs", "mssp"],
            "category": ["IP", "IP", "OP", "OP", "IP", "IP"],
            "total_spend": [1000.0, 500.0, 200.0, 100.0, 1500.0, 800.0],
            "member_months": [100, 50, 100, 50, 110, 55],
        }
    )


# ---------------------------------------------------------------------------
# deduplicate_member_months
# ---------------------------------------------------------------------------


class TestDeduplicateMemberMonths:
    @pytest.mark.unit
    def test_unique_keys(self) -> None:
        df = pl.DataFrame(
            {
                "year_month": [202401, 202401, 202401],
                "program": ["ffs", "ffs", "mssp"],
                "category": ["IP", "OP", "IP"],
                "member_months": [100, 100, 50],
            }
        )
        out = PmpmPlugins().deduplicate_member_months(df)
        # 2 distinct (year_month, program) keys
        assert out.height == 2


# ---------------------------------------------------------------------------
# overall_summary
# ---------------------------------------------------------------------------


class TestOverallSummary:
    @pytest.mark.unit
    def test_totals(self) -> None:
        out = PmpmPlugins().overall_summary(_pmpm_df())
        # total_spend = 1000+500+200+100+1500+800 = 4100
        assert out["total_spend"] == 4100.0
        # member-months: 4 distinct (ym,prog) → 100+50+100+50 = 300 (and PY2025 110+55 = 165) = 465
        assert out["total_member_months"] == 465
        assert out["overall_pmpm"] == pytest.approx(4100.0 / 465)
        assert out["unique_programs"] == 2
        assert out["unique_categories"] == 2

    @pytest.mark.unit
    def test_zero_member_months(self) -> None:
        df = pl.DataFrame(
            {
                "year_month": [202401],
                "program": ["ffs"],
                "category": ["IP"],
                "total_spend": [100.0],
                "member_months": [0],
            }
        )
        out = PmpmPlugins().overall_summary(df)
        assert out["overall_pmpm"] == 0


# ---------------------------------------------------------------------------
# rollups
# ---------------------------------------------------------------------------


class TestByProgram:
    @pytest.mark.unit
    def test_pmpm_per_program(self) -> None:
        out = PmpmPlugins().by_program(_pmpm_df())
        as_dict = {row["program"]: row for row in out.iter_rows(named=True)}
        # ffs: spend = 1000+200+1500 = 2700, mm = 100+100+110 = 310 → 8.7
        assert as_dict["ffs"]["pmpm"] == pytest.approx(2700.0 / 310)


class TestByCategory:
    @pytest.mark.unit
    def test_top_n_with_total_mm_denominator(self) -> None:
        out = PmpmPlugins().by_category(_pmpm_df(), n=10)
        # IP rows: 1000+500+1500+800 = 3800; OP: 200+100 = 300
        as_dict = {row["category"]: row for row in out.iter_rows(named=True)}
        assert as_dict["IP"]["total_spend"] == 3800.0


class TestByProgramCategory:
    @pytest.mark.unit
    def test_combos(self) -> None:
        out = PmpmPlugins().by_program_category(_pmpm_df(), n=30)
        # 4 (program, category) combos in fixture
        assert out.height == 4
        # PMPM uses program-scoped member-months
        ip_ffs = out.filter(
            (pl.col("program") == "ffs") & (pl.col("category") == "IP")
        ).row(0, named=True)
        # ffs IP spend = 1000+1500 = 2500, ffs mm = 100+100+110 = 310
        assert ip_ffs["pmpm"] == pytest.approx(2500.0 / 310)


# ---------------------------------------------------------------------------
# year-over-year
# ---------------------------------------------------------------------------


class TestYoyPmpmByProgram:
    @pytest.mark.unit
    def test_pivots(self) -> None:
        out = PmpmPlugins().yoy_pmpm_by_program(_pmpm_df())
        assert "ffs" in out.columns and "mssp" in out.columns
        years = out["year"].to_list()
        assert sorted(years, reverse=True) == years


class TestYoySpendByCategory:
    @pytest.mark.unit
    def test_pivots_with_2025_sort(self) -> None:
        out = PmpmPlugins().yoy_spend_by_category(_pmpm_df(), top_n=15)
        assert "category" in out.columns

    @pytest.mark.unit
    def test_no_2025_column(self) -> None:
        df = pl.DataFrame(
            {
                "year_month": [202401, 202401],
                "program": ["ffs", "mssp"],
                "category": ["IP", "OP"],
                "total_spend": [100.0, 50.0],
                "member_months": [10, 5],
            }
        )
        out = PmpmPlugins().yoy_spend_by_category(df, top_n=5)
        # Pivot still works; sort_year column simply doesn't exist
        assert out.height >= 1


# ---------------------------------------------------------------------------
# program_year_metrics
# ---------------------------------------------------------------------------


class TestProgramYearMetrics:
    @pytest.mark.unit
    def test_no_match_returns_none(self) -> None:
        out = PmpmPlugins().program_year_metrics(_pmpm_df(), 2099, "ffs")
        assert out is None

    @pytest.mark.unit
    def test_with_match(self) -> None:
        out = PmpmPlugins().program_year_metrics(_pmpm_df(), 2024, "ffs")
        # ffs 2024: 1000 (Jan IP) + 200 (Feb OP) = 1200; mm: 100 + 100 = 200
        assert out["total_spend"] == 1200.0
        assert out["total_member_months"] == 200
        assert out["overall_pmpm"] == 6.0
        assert out["num_categories"] == 2


# ---------------------------------------------------------------------------
# readmissions / service categories
# ---------------------------------------------------------------------------


class TestReadmissionsSummary:
    @pytest.mark.unit
    def test_summary(self) -> None:
        df = pl.DataFrame(
            {
                "patient_id": ["A", "B", "A"],
                "days_to_readmission": [10, 20, 30],
            }
        )
        out = PmpmPlugins().readmissions_summary(df)
        assert out["total"] == 3
        assert out["unique_patients"] == 2
        assert out["avg_days"] == pytest.approx(20.0)
        assert out["median_days"] == 20

    @pytest.mark.unit
    def test_empty_summary(self) -> None:
        df = pl.DataFrame(
            {
                "patient_id": pl.Series([], dtype=pl.Utf8),
                "days_to_readmission": pl.Series([], dtype=pl.Int64),
            }
        )
        out = PmpmPlugins().readmissions_summary(df)
        assert out["total"] == 0
        assert out["unique_patients"] == 0
        assert out["avg_days"] == 0


class TestTopReadmittedPatients:
    @pytest.mark.unit
    def test_topn(self) -> None:
        df = pl.DataFrame(
            {
                "patient_id": ["A", "B", "A", "A"],
                "days_to_readmission": [5, 10, 7, 6],
            }
        )
        out = PmpmPlugins().top_readmitted_patients(df, n=10)
        as_dict = {row["patient_id"]: row for row in out.iter_rows(named=True)}
        assert as_dict["A"]["readmission_count"] == 3
        assert as_dict["A"]["avg_days_to_readmission"] == pytest.approx(6.0)


class TestServiceCategorySummary:
    @pytest.mark.unit
    def test_groups(self) -> None:
        lf = pl.LazyFrame(
            {
                "service_category_2": ["A", "A", "B"],
                "paid": [100, 200, 50],
                "person_id": ["p1", "p2", "p1"],
            }
        )
        out = PmpmPlugins().service_category_summary(lf, n=5)
        as_dict = {row["service_category_2"]: row for row in out.iter_rows(named=True)}
        assert as_dict["A"]["claim_count"] == 2
        assert as_dict["A"]["unique_patients"] == 2
        assert as_dict["A"]["total_paid"] == 300


class TestProgramNames:
    @pytest.mark.unit
    def test_lookup_table_present(self) -> None:
        assert PROGRAM_NAMES["ffs"] == "Fee-for-Service"
