# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for the per-beneficiary × month risk score matrix transform.

The transform mirrors ``consolidated_alignment``'s pattern: pivot
long-form scored beneficiaries to a wide bene × ``ym_YYYYMM_risk_score``
matrix with horizontal aggregates (mean-PY, max-ever, months-above-RAF,
etc.). Point-in-time semantics: each ym column is the most-recent score
with ``score_date <= month_end``.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from acoharmony._transforms._risk_score_matrix import (
    _month_end,
    build_risk_score_matrix,
    year_month_range,
)


_SCHEMA = {
    "member_id": pl.Utf8,
    "score_date": pl.Date,
    "risk_score": pl.Float64,
}


def _scored(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(rows, schema=_SCHEMA)


class TestYearMonthHelpers:
    @pytest.mark.unit
    def test_year_month_range_inclusive(self):
        months = year_month_range(date(2026, 1, 1), date(2026, 3, 15))
        assert months == ["202601", "202602", "202603"]

    @pytest.mark.unit
    def test_year_month_range_single_month(self):
        months = year_month_range(date(2026, 4, 1), date(2026, 4, 30))
        assert months == ["202604"]

    @pytest.mark.unit
    def test_month_end_december_wraparound(self):
        assert _month_end("202612") == date(2026, 12, 31)

    @pytest.mark.unit
    def test_month_end_february_non_leap(self):
        assert _month_end("202602") == date(2026, 2, 28)


class TestMatrixPivot:
    @pytest.mark.unit
    def test_single_bene_single_score_spreads_forward(self):
        """A score on Jan 15 should fill Jan, Feb, Mar columns (each is the
        most-recent score ≤ month-end)."""
        scored = _scored(
            [{"member_id": "A", "score_date": date(2026, 1, 15), "risk_score": 0.9}]
        )
        out = build_risk_score_matrix(
            scored,
            window_start=date(2026, 1, 1),
            window_end=date(2026, 3, 31),
            performance_year=2026,
        ).collect()
        assert out.height == 1
        row = out.row(0, named=True)
        assert float(row["ym_202601_risk_score"]) == pytest.approx(0.9)
        assert float(row["ym_202602_risk_score"]) == pytest.approx(0.9)
        assert float(row["ym_202603_risk_score"]) == pytest.approx(0.9)

    @pytest.mark.unit
    def test_score_refresh_uses_most_recent(self):
        """Jan score 0.8, then Feb refresh to 1.2 — Mar should see 1.2."""
        scored = _scored(
            [
                {"member_id": "A", "score_date": date(2026, 1, 15), "risk_score": 0.8},
                {"member_id": "A", "score_date": date(2026, 2, 15), "risk_score": 1.2},
            ]
        )
        out = build_risk_score_matrix(
            scored,
            window_start=date(2026, 1, 1),
            window_end=date(2026, 3, 31),
            performance_year=2026,
        ).collect()
        row = out.row(0, named=True)
        assert float(row["ym_202601_risk_score"]) == pytest.approx(0.8)
        assert float(row["ym_202602_risk_score"]) == pytest.approx(1.2)
        assert float(row["ym_202603_risk_score"]) == pytest.approx(1.2)

    @pytest.mark.unit
    def test_pre_window_score_absent(self):
        """A score dated before the window start should leave early
        months null (not retroactively populate)."""
        scored = _scored(
            [
                # Score on Feb 20 — so Jan should be null, Feb and Mar should see 1.0
                {"member_id": "A", "score_date": date(2026, 2, 20), "risk_score": 1.0},
            ]
        )
        out = build_risk_score_matrix(
            scored,
            window_start=date(2026, 1, 1),
            window_end=date(2026, 3, 31),
            performance_year=2026,
        ).collect()
        row = out.row(0, named=True)
        assert row["ym_202601_risk_score"] is None
        assert float(row["ym_202602_risk_score"]) == pytest.approx(1.0)
        assert float(row["ym_202603_risk_score"]) == pytest.approx(1.0)


class TestHorizontalAggregates:
    @pytest.mark.unit
    def test_avg_max_min_across_window(self):
        scored = _scored(
            [
                {"member_id": "A", "score_date": date(2026, 1, 15), "risk_score": 0.5},
                {"member_id": "A", "score_date": date(2026, 2, 15), "risk_score": 1.5},
                {"member_id": "A", "score_date": date(2026, 3, 15), "risk_score": 1.0},
            ]
        )
        out = build_risk_score_matrix(
            scored,
            window_start=date(2026, 1, 1),
            window_end=date(2026, 3, 31),
            performance_year=2026,
        ).collect()
        row = out.row(0, named=True)
        # 0.5 + 1.5 + 1.0 → mean 1.0
        assert float(row["avg_risk_score_py"]) == pytest.approx(1.0)
        assert float(row["max_risk_score_ever"]) == pytest.approx(1.5)
        assert float(row["min_risk_score_ever"]) == pytest.approx(0.5)
        assert float(row["latest_risk_score"]) == pytest.approx(1.0)
        assert int(row["months_scored"]) == 3
        assert int(row["months_scored_py"]) == 3

    @pytest.mark.unit
    def test_months_above_raf_counts(self):
        scored = _scored(
            [
                {"member_id": "A", "score_date": date(2026, 1, 15), "risk_score": 0.8},
                {"member_id": "A", "score_date": date(2026, 2, 15), "risk_score": 1.1},
                {"member_id": "A", "score_date": date(2026, 3, 15), "risk_score": 1.3},
            ]
        )
        out = build_risk_score_matrix(
            scored,
            window_start=date(2026, 1, 1),
            window_end=date(2026, 3, 31),
            performance_year=2026,
            raf_threshold=1.0,
        ).collect()
        row = out.row(0, named=True)
        # Jan 0.8 (not above), Feb 1.1, Mar 1.3 (both above) → 2 months
        assert int(row["months_above_raf"]) == 2

    @pytest.mark.unit
    def test_risk_change_6mo(self):
        """Score in Jan is 1.0, score in Jul is 1.3. Window Jan–Jul with
        lookback 6 months → change = 1.3 - 1.0 = 0.3."""
        scored = _scored(
            [
                {"member_id": "A", "score_date": date(2026, 1, 15), "risk_score": 1.0},
                {"member_id": "A", "score_date": date(2026, 7, 15), "risk_score": 1.3},
            ]
        )
        out = build_risk_score_matrix(
            scored,
            window_start=date(2026, 1, 1),
            window_end=date(2026, 7, 31),
            performance_year=2026,
            change_lookback_months=6,
        ).collect()
        row = out.row(0, named=True)
        assert float(row["risk_change_6_mo"]) == pytest.approx(0.3)

    @pytest.mark.unit
    def test_null_months_skipped_in_avg(self):
        """A bene with scores only in two of three months should compute
        avg over the populated two (null-skipping)."""
        scored = _scored(
            [
                {"member_id": "A", "score_date": date(2026, 1, 15), "risk_score": 1.0},
                # Feb: no score → Feb column null
                {"member_id": "A", "score_date": date(2026, 3, 15), "risk_score": 3.0},
            ]
        )
        out = build_risk_score_matrix(
            scored,
            window_start=date(2026, 1, 1),
            window_end=date(2026, 3, 31),
            performance_year=2026,
        ).collect()
        row = out.row(0, named=True)
        # Feb propagates 1.0 (most-recent ≤ Feb-end), Mar 3.0 → mean = (1+1+3)/3
        assert int(row["months_scored_py"]) == 3
        assert float(row["avg_risk_score_py"]) == pytest.approx((1.0 + 1.0 + 3.0) / 3)


class TestMultiBene:
    @pytest.mark.unit
    def test_multiple_benes_get_independent_rows(self):
        scored = _scored(
            [
                {"member_id": "A", "score_date": date(2026, 1, 15), "risk_score": 1.0},
                {"member_id": "B", "score_date": date(2026, 1, 15), "risk_score": 2.0},
            ]
        )
        out = build_risk_score_matrix(
            scored,
            window_start=date(2026, 1, 1),
            window_end=date(2026, 1, 31),
            performance_year=2026,
        ).collect()
        assert out.height == 2
        a = out.filter(pl.col("member_id") == "A").row(0, named=True)
        b = out.filter(pl.col("member_id") == "B").row(0, named=True)
        assert float(a["ym_202601_risk_score"]) == pytest.approx(1.0)
        assert float(b["ym_202601_risk_score"]) == pytest.approx(2.0)


class TestDuplicateScores:
    @pytest.mark.unit
    def test_duplicate_score_rows_deduplicated_by_max(self):
        """Two rows on the same bene/date — max wins (conservative
        reconstruction against restated claims)."""
        scored = _scored(
            [
                {"member_id": "A", "score_date": date(2026, 1, 15), "risk_score": 1.0},
                {"member_id": "A", "score_date": date(2026, 1, 15), "risk_score": 1.5},
            ]
        )
        out = build_risk_score_matrix(
            scored,
            window_start=date(2026, 1, 1),
            window_end=date(2026, 1, 31),
            performance_year=2026,
        ).collect()
        row = out.row(0, named=True)
        assert float(row["ym_202601_risk_score"]) == pytest.approx(1.5)
