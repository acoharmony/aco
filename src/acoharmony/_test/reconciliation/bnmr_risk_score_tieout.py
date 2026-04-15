# © 2025 HarmonyCares
# All rights reserved.

"""
1:1 reconciliation tie-out: mean of per-bene HCC scores == BNMR
``raw_risk_score`` (milestone M2b).

Tolerance: 1e-4 (4 decimal places).

Scoring responsibility belongs to the caller — we test the tie-out
arithmetic, not the HCC model. Two flavors of test:

- **Pure-arithmetic scenarios** use hand-picked risk_score values so
  the expected mean is obvious by inspection.
- **HCCEngine-backed scenarios** compute a couple of per-bene scores
  via the real engine at fixture-build time, then feed them in, to
  prove the pipeline works on realistic RAFs.
"""

from __future__ import annotations

import polars as pl
import pytest

from acoharmony._risk import HCCEngine
from acoharmony._transforms._bnmr_risk_score_reconciliation_view import (
    RISK_SCORE_TOLERANCE,
    aggregate_scored_benes,
    build_bnmr_risk_score_reconciliation_view,
)

ACO_ID = "D0259"


# ---------------------------------------------------------------------------
# Schemas & builders
# ---------------------------------------------------------------------------

_SCORED_BENES_SCHEMA = {
    "member_id": pl.Utf8,
    "perf_yr": pl.Utf8,
    "clndr_yr": pl.Utf8,
    "clndr_mnth": pl.Utf8,
    "bnmrk": pl.Utf8,
    "align_type": pl.Utf8,
    "va_cat": pl.Utf8,
    "bnmrk_type": pl.Utf8,
    "aco_id": pl.Utf8,
    "risk_score": pl.Float64,
}

_BNMR_RISK_SCHEMA = {
    "perf_yr": pl.Utf8,
    "clndr_yr": pl.Utf8,
    "clndr_mnth": pl.Utf8,
    "bnmrk": pl.Utf8,
    "align_type": pl.Utf8,
    "va_cat": pl.Utf8,
    "bnmrk_type": pl.Utf8,
    "aco_id": pl.Utf8,
    "raw_risk_score": pl.Float64,
    "file_date": pl.Utf8,
}

_SCORED_DEFAULTS = {
    "perf_yr": "2026",
    "clndr_yr": "2026",
    "clndr_mnth": "3",
    "bnmrk": "AD",
    "align_type": "C",
    "va_cat": "N",
    "bnmrk_type": "RATEBOOK",
    "aco_id": ACO_ID,
}

_BNMR_DEFAULTS = {
    "perf_yr": "2026",
    "clndr_yr": "2026",
    "clndr_mnth": "3",
    "bnmrk": "AD",
    "align_type": "C",
    "va_cat": "N",
    "bnmrk_type": "RATEBOOK",
    "aco_id": ACO_ID,
    "raw_risk_score": 0.0,
    "file_date": "2026-05-01",
}


def _scored(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(
        [{**_SCORED_DEFAULTS, **r} for r in rows], schema=_SCORED_BENES_SCHEMA
    )


def _bnmr(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(
        [{**_BNMR_DEFAULTS, **r} for r in rows], schema=_BNMR_RISK_SCHEMA
    )


# ---------------------------------------------------------------------------
# Pure-arithmetic scenarios
# ---------------------------------------------------------------------------


class TestPureArithmeticTieOut:
    @pytest.mark.reconciliation
    def test_single_month_mean(self):
        """Three benes with scores 1.0, 1.2, 0.8 → mean 1.0."""
        scored = _scored(
            [
                {"member_id": "B1", "risk_score": 1.0},
                {"member_id": "B2", "risk_score": 1.2},
                {"member_id": "B3", "risk_score": 0.8},
            ]
        )
        bnmr = _bnmr([{"raw_risk_score": 1.0}])
        diff = build_bnmr_risk_score_reconciliation_view(bnmr, scored).collect()
        assert diff.height == 1
        row = diff.row(0, named=True)
        assert float(row["expected_raw_risk_score"]) == pytest.approx(1.0)
        assert float(row["raw_risk_score_diff"]) < RISK_SCORE_TOLERANCE

    @pytest.mark.reconciliation
    def test_multi_month_separate_buckets(self):
        """Two months with different bene sets produce independent means."""
        scored = _scored(
            [
                {"member_id": "B1", "clndr_mnth": "3", "risk_score": 1.0},
                {"member_id": "B2", "clndr_mnth": "3", "risk_score": 2.0},
                {"member_id": "B1", "clndr_mnth": "4", "risk_score": 0.5},
                {"member_id": "B2", "clndr_mnth": "4", "risk_score": 1.5},
            ]
        )
        bnmr = _bnmr(
            [
                {"clndr_mnth": "3", "raw_risk_score": 1.5},
                {"clndr_mnth": "4", "raw_risk_score": 1.0},
            ]
        )
        diff = build_bnmr_risk_score_reconciliation_view(bnmr, scored).collect()
        bad = diff.filter(pl.col("raw_risk_score_diff") > RISK_SCORE_TOLERANCE)
        assert bad.height == 0, f"Monthly score tie-out failed:\n{bad}"

    @pytest.mark.reconciliation
    def test_va_cat_stratifies(self):
        scored = _scored(
            [
                {"member_id": "B1", "va_cat": "N", "risk_score": 1.0},
                {"member_id": "B2", "va_cat": "N", "risk_score": 1.0},
                {"member_id": "B3", "va_cat": "C", "risk_score": 2.0},
                {"member_id": "B4", "va_cat": "C", "risk_score": 2.0},
            ]
        )
        bnmr = _bnmr(
            [
                {"va_cat": "N", "raw_risk_score": 1.0},
                {"va_cat": "C", "raw_risk_score": 2.0},
            ]
        )
        diff = build_bnmr_risk_score_reconciliation_view(bnmr, scored).collect()
        bad = diff.filter(pl.col("raw_risk_score_diff") > RISK_SCORE_TOLERANCE)
        assert bad.height == 0

    @pytest.mark.reconciliation
    def test_four_decimal_tolerance_is_tight(self):
        """Off by 0.001 should be caught; off by 0.00001 should pass."""
        scored = _scored(
            [
                {"member_id": "B1", "risk_score": 1.00000},
                {"member_id": "B2", "risk_score": 1.00000},
            ]
        )
        # Passes: well within 1e-4 tolerance
        bnmr_ok = _bnmr([{"raw_risk_score": 1.00001}])
        diff_ok = build_bnmr_risk_score_reconciliation_view(bnmr_ok, scored).collect()
        assert (
            float(diff_ok.row(0, named=True)["raw_risk_score_diff"])
            < RISK_SCORE_TOLERANCE
        )

        # Fails: 0.001 is above 1e-4 tolerance
        bnmr_bad = _bnmr([{"raw_risk_score": 1.001}])
        diff_bad = build_bnmr_risk_score_reconciliation_view(bnmr_bad, scored).collect()
        assert (
            float(diff_bad.row(0, named=True)["raw_risk_score_diff"])
            > RISK_SCORE_TOLERANCE
        )


# ---------------------------------------------------------------------------
# HCCEngine-backed scenarios (realistic RAFs)
# ---------------------------------------------------------------------------


class TestHCCEngineBackedTieOut:
    """
    Use the real HCCEngine to score two canonical benes, then tie out.
    This proves the reconciliation math works with actual V28 RAFs, not
    just rounded test values.
    """

    def _engine(self) -> HCCEngine:
        return HCCEngine()

    def _score_bene(
        self, engine: HCCEngine, diagnosis_codes: list[str], age: int, sex: str
    ) -> float:
        return engine.score_patient(
            diagnosis_codes=diagnosis_codes, age=age, sex=sex
        ).risk_score

    @pytest.mark.reconciliation
    def test_hcc_backed_two_bene_mean(self):
        """Two benes with real HCCEngine scores average correctly."""
        engine = self._engine()
        bene_a_score = self._score_bene(engine, ["E11.9"], age=72, sex="F")
        bene_b_score = self._score_bene(
            engine, ["E11.9", "I50.1", "N18.6"], age=72, sex="M"
        )
        expected_mean = (bene_a_score + bene_b_score) / 2

        scored = _scored(
            [
                {"member_id": "A", "risk_score": bene_a_score},
                {"member_id": "B", "risk_score": bene_b_score},
            ]
        )
        bnmr = _bnmr([{"raw_risk_score": expected_mean}])
        diff = build_bnmr_risk_score_reconciliation_view(bnmr, scored).collect()
        assert (
            float(diff.row(0, named=True)["raw_risk_score_diff"])
            < RISK_SCORE_TOLERANCE
        )


# ---------------------------------------------------------------------------
# Mismatch detection
# ---------------------------------------------------------------------------


class TestDeliberateMismatchFailsLoudly:
    @pytest.mark.reconciliation
    def test_inflated_bnmr_score_is_caught(self):
        """BNMR reports 1.50 but recon yields 1.00. Diff must fire."""
        scored = _scored(
            [
                {"member_id": "B1", "risk_score": 1.0},
                {"member_id": "B2", "risk_score": 1.0},
            ]
        )
        bnmr = _bnmr([{"raw_risk_score": 1.5}])
        diff = build_bnmr_risk_score_reconciliation_view(bnmr, scored).collect()
        bad = diff.filter(pl.col("raw_risk_score_diff") > RISK_SCORE_TOLERANCE)
        assert bad.height == 1
        assert float(bad.row(0, named=True)["raw_risk_score_diff"]) == pytest.approx(
            0.5
        )

    @pytest.mark.reconciliation
    def test_missing_bnmr_bucket_is_caught(self):
        """Scored benes exist for April but BNMR has no April row.
        The outer join surfaces the orphan."""
        scored = _scored(
            [
                {"member_id": "B1", "clndr_mnth": "3", "risk_score": 1.0},
                {"member_id": "B1", "clndr_mnth": "4", "risk_score": 1.0},
            ]
        )
        bnmr = _bnmr([{"clndr_mnth": "3", "raw_risk_score": 1.0}])
        diff = build_bnmr_risk_score_reconciliation_view(bnmr, scored).collect()
        orphan = diff.filter(pl.col("clndr_mnth") == "4")
        assert orphan.height == 1
        assert orphan["raw_risk_score"][0] is None
        assert float(orphan["expected_raw_risk_score"][0]) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Point-in-time filter
# ---------------------------------------------------------------------------


class TestPointInTimeFiltering:
    @pytest.mark.reconciliation
    def test_post_cutoff_bnmr_row_excluded(self):
        scored = _scored([{"member_id": "B1", "risk_score": 1.0}])
        bnmr_rows = [{"raw_risk_score": 1.0, "file_date": "2026-05-01"}]
        bnmr_rows.append(
            {
                **_BNMR_DEFAULTS,
                "raw_risk_score": 9.999,  # would blow up the tie-out
                "file_date": "2026-12-31",
            }
        )
        bnmr = _bnmr(
            [{k: v for k, v in r.items() if k in _BNMR_RISK_SCHEMA} for r in bnmr_rows]
        )
        diff = build_bnmr_risk_score_reconciliation_view(
            bnmr, scored, as_of_delivery_date="2026-06-30"
        ).collect()
        bad = diff.filter(pl.col("raw_risk_score_diff") > RISK_SCORE_TOLERANCE)
        assert bad.height == 0


# ---------------------------------------------------------------------------
# Aggregate helper
# ---------------------------------------------------------------------------


class TestAggregateHelper:
    @pytest.mark.reconciliation
    def test_aggregate_scored_benes_emits_count_and_mean(self):
        scored = _scored(
            [
                {"member_id": "B1", "risk_score": 1.0},
                {"member_id": "B2", "risk_score": 2.0},
                {"member_id": "B3", "risk_score": 3.0},
            ]
        )
        agg = aggregate_scored_benes(scored).collect()
        assert agg.height == 1
        row = agg.row(0, named=True)
        assert int(row["expected_bene_dcnt"]) == 3
        assert float(row["expected_raw_risk_score"]) == pytest.approx(2.0)
