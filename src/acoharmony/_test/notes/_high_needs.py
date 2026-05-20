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


class TestPerCriterionRecall:
    """``HighNeedsPlugins.per_criterion_recall`` long-form output."""

    def _bar_df(self) -> pl.DataFrame:
        # Two PYs, four BAR-flagged benes, mixed criterion flags.
        return pl.DataFrame({
            "performance_year": [2024, 2024, 2024, 2024, 2025, 2025],
            "resolved_mbi":     ["X1", "X2", "X3", "X4", "X5", "X6"],
            "bar_a":            [True,  False, True,  False, True,  True],
            "bar_b":            [True,  True,  False, True,  False, True],
            "bar_c":            [False, False, False, True,  False, False],
            "bar_d":            [False, True,  False, False, False, False],
        })

    def _ever_df(self, mbis: list[str]) -> pl.DataFrame:
        return pl.DataFrame({"mbi": mbis})

    @pytest.mark.unit
    def test_full_recall_when_all_found(self) -> None:
        bar = self._bar_df()
        ever = self._ever_df(["X1", "X2", "X3", "X4", "X5", "X6"])
        out = HighNeedsPlugins().per_criterion_recall(bar, ever)
        # Every cell should report recall 1.0 with n_missed == 0.
        assert (out["recall"] == 1.0).all()
        assert (out["n_missed"] == 0).all()

    @pytest.mark.unit
    def test_partial_recall_per_criterion(self) -> None:
        bar = self._bar_df()
        # Drop X1 — affects PY2024 (a) and (b)
        ever = self._ever_df(["X2", "X3", "X4", "X5", "X6"])
        out = HighNeedsPlugins().per_criterion_recall(bar, ever)
        cell = lambda py, c: out.filter(
            (pl.col("performance_year") == py) & (pl.col("criterion") == c)
        ).row(0, named=True)
        # PY2024 (a) denom 2 (X1, X3); X1 missed → 1/2
        assert cell(2024, "a")["n_missed"] == 1
        assert cell(2024, "a")["recall"] == 0.5
        # PY2024 (b) denom 3 (X1, X2, X4); X1 missed → 2/3
        assert cell(2024, "b")["n_missed"] == 1
        # PY2024 (d) denom 1 (X2); X2 found → 1.0
        assert cell(2024, "d")["recall"] == 1.0
        # PY2025 unaffected — X5, X6 found
        assert (out.filter(pl.col("performance_year") == 2025)["recall"] == 1.0).all()

    @pytest.mark.unit
    def test_zero_denom_criterion_skipped(self) -> None:
        """A criterion with no BAR-flagged benes in a PY is omitted from
        the output rather than emitting a divide-by-zero or recall=1.0
        row that would mislead the reader."""
        bar = pl.DataFrame({
            "performance_year": [2024],
            "resolved_mbi": ["X1"],
            "bar_a": [True],
            "bar_b": [False],
            "bar_c": [False],
            "bar_d": [False],
        })
        ever = self._ever_df(["X1"])
        out = HighNeedsPlugins().per_criterion_recall(bar, ever)
        # Only criterion (a) should appear.
        assert out["criterion"].to_list() == ["a"]


class TestCriterionABranchSimulation:
    """``HighNeedsPlugins.criterion_a_branch_simulation``."""

    def _make_codes(self) -> pl.LazyFrame:
        return pl.LazyFrame({"icd10_code": ["G35", "G800"], "category": ["MS", "CP"]})

    def _make_window_callable(self):
        from acoharmony._expressions._high_needs_lookback import LookbackWindow
        return lambda cd: LookbackWindow(begin=date(2024, 1, 1), end=date(2024, 12, 31))

    @pytest.mark.unit
    def test_inpatient_branch_dominates_combined_match(self) -> None:
        """A bene with both an inpatient and a 2-DOS non-inpatient match
        should land in the 'inpatient' bucket (mutually-exclusive in
        order)."""
        mc = pl.LazyFrame({
            "person_id": ["A", "A", "A"],
            "claim_type": ["institutional", "professional", "professional"],
            "bill_type_code": ["111", None, None],
            "claim_start_date": [date(2024, 6, 1), date(2024, 3, 1), date(2024, 8, 1)],
            "diagnosis_code_1": ["G35", "G35", "G35"],
            **{f"diagnosis_code_{i}": [None, None, None] for i in range(2, 26)},
        }, schema_overrides={"claim_start_date": pl.Date})
        out = HighNeedsPlugins().criterion_a_branch_simulation(
            ["A"], mc, self._make_codes(),
            check_dates=[date(2024, 1, 1)],
            py_table_c_window=self._make_window_callable(),
        )
        d = {r["bucket"]: r["benes"] for r in out.to_dicts()}
        assert d["inpatient (current branch — 1+ inpatient B.6.1 claim)"] == 1
        assert d["non_inpatient_2dos (FIX adds these — 2+ DOS)"] == 0

    @pytest.mark.unit
    def test_non_inpatient_2dos_branch_picked_up(self) -> None:
        mc = pl.LazyFrame({
            "person_id": ["A", "A"],
            "claim_type": ["professional", "professional"],
            "bill_type_code": [None, None],
            "claim_start_date": [date(2024, 3, 1), date(2024, 8, 15)],
            "diagnosis_code_1": ["G35", "G800"],
            **{f"diagnosis_code_{i}": [None, None] for i in range(2, 26)},
        }, schema_overrides={"claim_start_date": pl.Date})
        out = HighNeedsPlugins().criterion_a_branch_simulation(
            ["A"], mc, self._make_codes(),
            check_dates=[date(2024, 1, 1)],
            py_table_c_window=self._make_window_callable(),
        )
        d = {r["bucket"]: r["benes"] for r in out.to_dicts()}
        assert d["inpatient (current branch — 1+ inpatient B.6.1 claim)"] == 0
        assert d["non_inpatient_2dos (FIX adds these — 2+ DOS)"] == 1

    @pytest.mark.unit
    def test_one_dos_lands_in_1dos_bucket(self) -> None:
        mc = pl.LazyFrame({
            "person_id": ["A"],
            "claim_type": ["professional"],
            "bill_type_code": [None],
            "claim_start_date": [date(2024, 3, 1)],
            "diagnosis_code_1": ["G35"],
            **{f"diagnosis_code_{i}": [None] for i in range(2, 26)},
        }, schema_overrides={"claim_start_date": pl.Date})
        out = HighNeedsPlugins().criterion_a_branch_simulation(
            ["A"], mc, self._make_codes(),
            check_dates=[date(2024, 1, 1)],
            py_table_c_window=self._make_window_callable(),
        )
        d = {r["bucket"]: r["benes"] for r in out.to_dicts()}
        assert d["non_inpatient_1dos (still won't qualify after fix)"] == 1

    @pytest.mark.unit
    def test_no_match_at_all(self) -> None:
        # Bene B has claims but no B.6.1 dx; bene C has no claims at all.
        mc = pl.LazyFrame({
            "person_id": ["B"],
            "claim_type": ["professional"],
            "bill_type_code": [None],
            "claim_start_date": [date(2024, 3, 1)],
            "diagnosis_code_1": ["Z99.0"],
            **{f"diagnosis_code_{i}": [None] for i in range(2, 26)},
        }, schema_overrides={"claim_start_date": pl.Date})
        out = HighNeedsPlugins().criterion_a_branch_simulation(
            ["B", "C"], mc, self._make_codes(),
            check_dates=[date(2024, 1, 1)],
            py_table_c_window=self._make_window_callable(),
        )
        d = {r["bucket"]: r["benes"] for r in out.to_dicts()}
        assert d["no_match_in_claims (data gap, no B.6.1 dx in any claim)"] == 2

    @pytest.mark.unit
    def test_empty_input_returns_zero_share(self) -> None:
        mc = pl.LazyFrame(schema={
            "person_id": pl.String, "claim_type": pl.String,
            "bill_type_code": pl.String, "claim_start_date": pl.Date,
            **{f"diagnosis_code_{i}": pl.String for i in range(1, 26)},
        })
        out = HighNeedsPlugins().criterion_a_branch_simulation(
            [], mc, self._make_codes(),
            check_dates=[date(2024, 1, 1)],
            py_table_c_window=self._make_window_callable(),
        )
        # All buckets should have share=0.0 when total is zero.
        assert (out["share"] == 0.0).all()


class TestCriterionBScoreDistribution:
    """``HighNeedsPlugins.criterion_b_score_distribution``."""

    def _seed_scores(self, tmp_path: Path) -> Path:
        gold = tmp_path / "gold"
        gold.mkdir()
        pl.DataFrame({
            "mbi":              ["M1", "M2", "M3", "M4", "M5", "M6"],
            "performance_year": [2026] * 6,
            "model_version":    ["cms_hcc_v24"] * 6,
            "check_date":       [date(2026, 1, 1)] * 6,
            "total_risk_score": [3.5, 2.7, 2.2, 1.5, 0.5, 1.0],
            # PY2025 row for M1 should not contribute when we ask PY2026
            "cohort":           ["AD"] * 6,
        }).write_parquet(gold / "hcc_risk_scores.parquet")
        return gold

    @pytest.mark.unit
    def test_buckets_each_threshold(self, tmp_path: Path) -> None:
        gold = self._seed_scores(tmp_path)
        # M7 has no score row → "no score"
        out = HighNeedsPlugins().criterion_b_score_distribution(
            ["M1", "M2", "M3", "M4", "M5", "M6", "M7"],
            gold,
            performance_year=2026,
        )
        d = {r["bucket"]: r["benes"] for r in out.to_dicts()}
        assert d["scored ≥ 3.0"] == 1                  # M1
        assert d["2.5-3.0 (within 0.5)"] == 1          # M2
        assert d["2.0-2.5"] == 1                       # M3
        assert d["1.0-2.0"] == 2                       # M4 (1.5), M6 (1.0)
        assert d["< 1.0"] == 1                         # M5
        assert d["no score"] == 1                      # M7

    @pytest.mark.unit
    def test_zero_input_no_division(self, tmp_path: Path) -> None:
        gold = self._seed_scores(tmp_path)
        out = HighNeedsPlugins().criterion_b_score_distribution(
            [], gold, performance_year=2026,
        )
        assert (out["share"] == 0.0).all()


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
