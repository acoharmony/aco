# © 2025 HarmonyCares
"""Tests for acoharmony._notes._hedr (HedrPlugins)."""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from acoharmony._notes import HedrPlugins


def _hedr_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mbi": ["M1", "M2", "M3", "M4"],
            "performance_year": [2025, 2025, 2025, 2025],
            "bar_file_date": [
                date(2025, 1, 1),
                date(2025, 1, 1),
                date(2025, 1, 1),
                date(2025, 1, 1),
            ],
            "hedr_denominator": [True, True, False, True],
            "hedr_numerator": [True, False, False, False],
            "hedr_status": ["complete", "incomplete", "ineligible", "incomplete"],
            "missing_data_fields": [None, "race,ethnicity", None, "language"],
            "reach_months_2025": [12, 8, 2, 6],
            "is_alive": [True, True, True, True],
            "first_reach_date": [
                date(2024, 1, 1),
                date(2025, 11, 1),
                date(2024, 1, 1),
                date(2025, 5, 1),
            ],
            "bene_first_name": ["A", "B", "C", "D"],
            "bene_last_name": ["S", "T", "U", "V"],
            "bene_city": ["X", "Y", "Z", "W"],
            "bene_state": ["MA", "MA", "NY", "NY"],
        }
    )


# ---------------------------------------------------------------------------
# summary_metrics
# ---------------------------------------------------------------------------


class TestSummaryMetrics:
    @pytest.mark.unit
    def test_basic(self) -> None:
        out = HedrPlugins().summary_metrics(_hedr_df())
        assert out["performance_year"] == 2025
        assert out["total_beneficiaries"] == 4
        assert out["denominator_count"] == 3
        assert out["numerator_count"] == 1
        assert out["hedr_rate"] == pytest.approx(33.33, rel=1e-2)
        assert out["incomplete_count"] == 2
        assert out["ineligible_count"] == 1

    @pytest.mark.unit
    def test_zero_denominator(self) -> None:
        df = pl.DataFrame(
            {
                "performance_year": [2025],
                "bar_file_date": [date(2025, 1, 1)],
                "hedr_denominator": [False],
                "hedr_numerator": [False],
            }
        )
        out = HedrPlugins().summary_metrics(df)
        assert out["hedr_rate"] == 0.0


# ---------------------------------------------------------------------------
# rollups
# ---------------------------------------------------------------------------


class TestStatusBreakdown:
    @pytest.mark.unit
    def test_groups(self) -> None:
        out = HedrPlugins().status_breakdown(_hedr_df())
        as_dict = {row["hedr_status"]: row for row in out.iter_rows(named=True)}
        assert as_dict["incomplete"]["count"] == 2
        assert as_dict["incomplete"]["percentage"] == 50.0


class TestMonthsDistribution:
    @pytest.mark.unit
    def test_groups(self) -> None:
        out = HedrPlugins().months_distribution(_hedr_df())
        # 4 distinct month counts in fixture
        assert out.height == 4


class TestMissingFieldCounts:
    @pytest.mark.unit
    def test_explodes(self) -> None:
        out = HedrPlugins().missing_field_counts(_hedr_df())
        as_dict = {row["missing_data_fields"]: row for row in out.iter_rows(named=True)}
        assert as_dict["race"]["count"] == 1
        assert as_dict["ethnicity"]["count"] == 1
        assert as_dict["language"]["count"] == 1

    @pytest.mark.unit
    def test_no_incomplete(self) -> None:
        df = pl.DataFrame(
            {
                "hedr_denominator": [True],
                "hedr_numerator": [True],
                "missing_data_fields": [None],
            }
        )
        out = HedrPlugins().missing_field_counts(df)
        assert out.is_empty()


class TestAlignmentTiming:
    @pytest.mark.unit
    def test_categorizes(self) -> None:
        out = HedrPlugins().alignment_timing(_hedr_df())
        as_dict = {row["alignment_timing"]: row for row in out.iter_rows(named=True)}
        # M1, M3, M4 < Oct 1; M2 > Oct 1
        assert as_dict["Started before Oct 1"]["count"] == 3
        assert as_dict["Started after Oct 1"]["count"] == 1


class TestIncompleteBeneficiaries:
    @pytest.mark.unit
    def test_filters(self) -> None:
        out = HedrPlugins().incomplete_beneficiaries(_hedr_df())
        # M2 and M4 are denom & not numerator
        assert sorted(out["mbi"].to_list()) == ["M2", "M4"]
        assert "missing_data_fields" in out.columns


# ---------------------------------------------------------------------------
# SDOH template check
# ---------------------------------------------------------------------------


class TestSdohTemplateCheck:
    @pytest.mark.unit
    def test_missing_template(self, tmp_path: Path) -> None:
        out = HedrPlugins().sdoh_template_check(tmp_path, _hedr_df())
        assert out is None

    @pytest.mark.unit
    def test_template_present(self, tmp_path: Path) -> None:
        # Template has M1 (denom-eligible), M3 (not denom-eligible), and M99 (not in BAR)
        pl.DataFrame({"mbi": ["M1", "M3", "M99"]}).write_parquet(
            tmp_path / "reach_sdoh.parquet"
        )
        out = HedrPlugins().sdoh_template_check(tmp_path, _hedr_df())
        assert out is not None
        as_dict = {row["status"]: row for row in out["summary"].iter_rows(named=True)}
        assert as_dict["✓ Denominator Eligible"]["count"] == 1  # M1
        assert as_dict["✗ NOT Denominator Eligible"]["count"] == 1  # M3
        assert as_dict["✗ Not in BAR at all"]["count"] == 1  # M99
        assert out["total_issues"] == 2

    @pytest.mark.unit
    def test_empty_template(self, tmp_path: Path) -> None:
        pl.DataFrame({"mbi": pl.Series([], dtype=pl.Utf8)}).write_parquet(
            tmp_path / "reach_sdoh.parquet"
        )
        out = HedrPlugins().sdoh_template_check(tmp_path, _hedr_df())
        assert out["issue_pct"] == 0


# ---------------------------------------------------------------------------
# status_label
# ---------------------------------------------------------------------------


class TestStatusLabel:
    @pytest.mark.unit
    @pytest.mark.parametrize(
        "rate,expected",
        [(96, "ok"), (90, "warn"), (50, "critical")],
    )
    def test_thresholds(self, rate, expected):
        severity, _ = HedrPlugins.status_label(rate)
        assert severity == expected
