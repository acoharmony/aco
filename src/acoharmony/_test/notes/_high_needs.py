# © 2025 HarmonyCares
"""Tests for acoharmony._notes._high_needs (HighNeedsPlugins)."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from acoharmony._notes import HighNeedsPlugins


def _recon_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "mbi": ["M1", "M2", "M3", "M4"],
            "performance_year": [2024, 2024, 2024, 2024],
            "criterion_a_met_ever": [True, False, False, None],
            "criterion_b_met_ever": [True, True, False, False],
            "criterion_c_met_ever": [False, False, True, False],
            "criterion_d_met_ever": [False, False, False, False],
            "criterion_e_met_ever": [False, True, False, False],
            "criterion_a_met": [True, False, False, False],
            "criterion_b_met": [True, False, False, False],
            "criterion_c_met": [False, False, True, False],
            "criterion_d_met": [False, False, False, False],
            "criterion_e_met": [False, False, False, False],
            "high_needs_eligible_sticky": [True, True, True, False],
            "high_needs_eligible_this_py": [True, False, True, False],
            "first_eligible_py": [2023, 2024, 2024, None],
            "first_eligible_check_date": [
                date(2023, 4, 1),
                date(2024, 1, 1),
                date(2024, 4, 1),
                None,
            ],
            "bar_file_date": [date(2024, 6, 1), None, date(2024, 6, 1), date(2024, 6, 1)],
            "bar_mobility_impairment_flag": ["Y", None, None, None],
            "bar_high_risk_flag": ["Y", None, "Y", None],
            "bar_medium_risk_unplanned_flag": [None, None, "Y", None],
            "bar_frailty_flag": [None, None, None, None],
            "bar_claims_based_flag": ["Y", None, "Y", "Y"],
            "pbvar_a2_present": [False, True, False, True],
            "pbvar_a2_file_date": [None, date(2024, 6, 1), None, date(2024, 6, 1)],
            "pbvar_response_codes": [None, "A2", None, "A2"],
        }
    )


class TestLoadRecon:
    @pytest.mark.unit
    def test_missing_raises(self, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            HighNeedsPlugins().load_recon(tmp_path)

    @pytest.mark.unit
    def test_loads(self, tmp_path: Path) -> None:
        _recon_df().write_parquet(tmp_path / "high_needs_reconciliation.parquet")
        out = HighNeedsPlugins().load_recon(tmp_path)
        assert out.height == 4


class TestCriterionCounts:
    @pytest.mark.unit
    def test_counts(self) -> None:
        out = HighNeedsPlugins().criterion_counts(_recon_df())
        assert out["total"] == 4
        assert out["a"] == 1
        assert out["b"] == 2


class TestPerCriterionTable:
    @pytest.mark.unit
    def test_table(self) -> None:
        out = HighNeedsPlugins().per_criterion_table(_recon_df())
        assert out.height == 5
        assert set(out.columns) == {
            "criterion",
            "description",
            "benes_ever",
            "share_ever",
            "benes_latest_check",
            "share_latest",
        }

    @pytest.mark.unit
    def test_zero_population(self) -> None:
        empty = pl.DataFrame(
            schema={
                **{f"criterion_{l}_met_ever": pl.Boolean for l in "abcde"},
                **{f"criterion_{l}_met": pl.Boolean for l in "abcde"},
            }
        )
        out = HighNeedsPlugins().per_criterion_table(empty)
        assert out.height == 5
        assert out["share_ever"].to_list() == [0.0] * 5


class TestCompositeBreakdown:
    @pytest.mark.unit
    def test_breakdown(self) -> None:
        out = HighNeedsPlugins().composite_breakdown(_recon_df())
        assert out.height == 4
        as_dict = {row["composite"]: row for row in out.iter_rows(named=True)}
        assert as_dict["eligible (sticky, cross-PY)"]["n_benes"] == 3
        assert as_dict["not eligible (sticky)"]["n_benes"] == 1

    @pytest.mark.unit
    def test_zero_population(self) -> None:
        empty = pl.DataFrame(
            schema={
                "high_needs_eligible_sticky": pl.Boolean,
                "high_needs_eligible_this_py": pl.Boolean,
            }
        )
        out = HighNeedsPlugins().composite_breakdown(empty)
        assert out["share_of_population"].to_list() == [0.0] * 4


class TestFirstEligibleBreakdown:
    @pytest.mark.unit
    def test_with_col(self) -> None:
        out = HighNeedsPlugins().first_eligible_breakdown(_recon_df())
        assert out is not None
        breakdown, never = out
        assert never == 1
        assert breakdown.height == 2

    @pytest.mark.unit
    def test_missing_col_returns_none(self) -> None:
        df = pl.DataFrame({"mbi": ["M1"]})
        assert HighNeedsPlugins().first_eligible_breakdown(df) is None


class TestFilterByFlag:
    @pytest.mark.unit
    def test_filters(self) -> None:
        out = HighNeedsPlugins().filter_by_flag(_recon_df(), "high_needs_eligible_sticky")
        assert out.height == 3


class TestIneligibleMbis:
    @pytest.mark.unit
    def test_returns_only_ineligible(self) -> None:
        out = HighNeedsPlugins().ineligible_mbis(_recon_df())
        assert out == ["M4"]

    @pytest.mark.unit
    def test_limit(self) -> None:
        df = pl.DataFrame(
            {
                "mbi": [f"M{i}" for i in range(10)],
                "high_needs_eligible_sticky": [False] * 10,
            }
        )
        out = HighNeedsPlugins().ineligible_mbis(df, limit=3)
        assert len(out) == 3


class TestBarTieout:
    @pytest.mark.unit
    def test_stats(self) -> None:
        out = HighNeedsPlugins().bar_tieout(_recon_df())
        # M1, M3, M4 have bar_claims_based_flag set
        assert out["total_on_bar"] == 3
        assert out["agree_eligible"] == 2  # M1, M3 eligible
        assert out["missed"] == 1  # M4
        assert out["recall"] == pytest.approx(2 / 3)
        assert out["missed_rows"].height == 1

    @pytest.mark.unit
    def test_empty(self) -> None:
        empty = pl.DataFrame(
            schema={
                "bar_claims_based_flag": pl.Utf8,
                "high_needs_eligible_sticky": pl.Boolean,
                "mbi": pl.Utf8,
                "bar_file_date": pl.Date,
                "bar_mobility_impairment_flag": pl.Utf8,
                "bar_high_risk_flag": pl.Utf8,
                "bar_medium_risk_unplanned_flag": pl.Utf8,
                "bar_frailty_flag": pl.Utf8,
                "criterion_a_met_ever": pl.Boolean,
                "criterion_b_met_ever": pl.Boolean,
                "criterion_c_met_ever": pl.Boolean,
                "criterion_d_met_ever": pl.Boolean,
                "criterion_e_met_ever": pl.Boolean,
                "first_eligible_py": pl.Int32,
            }
        )
        out = HighNeedsPlugins().bar_tieout(empty)
        assert out["total_on_bar"] == 0
        assert out["recall"] == 0.0


class TestPbvarA2Tieout:
    @pytest.mark.unit
    def test_stats(self) -> None:
        out = HighNeedsPlugins().pbvar_a2_tieout(_recon_df())
        assert out["total"] == 2  # M2, M4
        assert out["agree_not_eligible"] == 1  # M4
        assert out["overmatch"] == 1  # M2 sticky=True
        assert out["rows"].height == 2


class TestRecallResidualBuckets:
    @pytest.mark.unit
    def test_no_silver_files(self, tmp_path: Path) -> None:
        # All silver/gold lookups raise → everything ends up in "Other"
        out = HighNeedsPlugins().recall_residual_buckets(
            _recon_df(), tmp_path, tmp_path
        )
        assert out.height == 5
        # M4 is the only BAR-recall miss → 1 in "no data" bucket
        as_dict = {row["bucket"]: row for row in out.iter_rows(named=True)}
        no_data_row = next(
            row
            for label, row in as_dict.items()
            if label.startswith("1.")
        )
        assert no_data_row["benes"] == 1

    @pytest.mark.unit
    def test_with_partial_data(self, tmp_path: Path) -> None:
        # Stage cclf8 with M4 → bucket 2 (cclf8-only) candidate
        pl.DataFrame({"bene_mbi_id": ["M4"]}).write_parquet(
            tmp_path / "cclf8.parquet"
        )
        # Empty cclf1
        pl.DataFrame({"bene_mbi_id": []}, schema={"bene_mbi_id": pl.Utf8}).write_parquet(
            tmp_path / "cclf1.parquet"
        )
        out = HighNeedsPlugins().recall_residual_buckets(
            _recon_df(), tmp_path, tmp_path
        )
        as_dict = {row["bucket"]: row for row in out.iter_rows(named=True)}
        cclf8_only = next(
            row for label, row in as_dict.items() if label.startswith("2.")
        )
        assert cclf8_only["benes"] == 1

    @pytest.mark.unit
    def test_zero_misses_handles_zero_total(self, tmp_path: Path) -> None:
        df = _recon_df().with_columns(
            pl.lit(True).alias("high_needs_eligible_sticky")
        )
        out = HighNeedsPlugins().recall_residual_buckets(df, tmp_path, tmp_path)
        # No misses → all shares 0
        assert out["share"].to_list() == [0.0] * 5

    @pytest.mark.unit
    def test_scored_below_bucket(self, tmp_path: Path) -> None:
        # gold/hcc_risk_scores with M4 score = 1.5 → bucket 3
        pl.DataFrame(
            {
                "mbi": ["M4"],
                "total_risk_score": [1.5],
            }
        ).write_parquet(tmp_path / "hcc_risk_scores.parquet")
        out = HighNeedsPlugins().recall_residual_buckets(
            _recon_df(), tmp_path, tmp_path
        )
        as_dict = {row["bucket"]: row for row in out.iter_rows(named=True)}
        scored_below = next(
            row for label, row in as_dict.items() if label.startswith("3.")
        )
        assert scored_below["benes"] == 1

    @pytest.mark.unit
    def test_bnex_only_bucket(self, tmp_path: Path) -> None:
        # bnex with M4 only → bucket 4
        pl.DataFrame({"MBI": ["M4"]}).write_parquet(tmp_path / "bnex.parquet")
        out = HighNeedsPlugins().recall_residual_buckets(
            _recon_df(), tmp_path, tmp_path
        )
        as_dict = {row["bucket"]: row for row in out.iter_rows(named=True)}
        bnex_only = next(
            row for label, row in as_dict.items() if label.startswith("4.")
        )
        assert bnex_only["benes"] == 1


class TestPerMbiSummary:
    @pytest.mark.unit
    def test_eligible(self) -> None:
        out = HighNeedsPlugins().per_mbi_summary(_recon_df(), "M1")
        assert "First qualified" in out["first_line"]
        assert "✅ yes" in out["criteria_table_md"]
        assert "Eligible" in out["composite_md"]

    @pytest.mark.unit
    def test_never_eligible(self) -> None:
        out = HighNeedsPlugins().per_mbi_summary(_recon_df(), "M4")
        assert "Never qualified" in out["first_line"]

    @pytest.mark.unit
    def test_null_flag_label(self) -> None:
        # M1's criterion_a_met_ever for a null path: build a row where
        # one ever-flag is null
        df = _recon_df().with_columns(
            pl.when(pl.col("mbi") == "M2")
            .then(None)
            .otherwise(pl.col("criterion_a_met_ever"))
            .alias("criterion_a_met_ever")
        )
        out = HighNeedsPlugins().per_mbi_summary(df, "M2")
        assert "? null" in out["criteria_table_md"]
