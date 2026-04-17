# © 2025 HarmonyCares
# All rights reserved.

"""
1:1 reconciliation tie-out: reconstructed eligibility counts == BNMR risk
sheet counts (milestone M2a).

Given a per-beneficiary-month eligibility frame stamped with BNMR dims
``(perf_yr, clndr_yr, clndr_mnth, bnmrk, align_type, va_cat, bnmrk_type,
aco_id)``, distinct-counting ``member_id`` at that grain must equal
BNMR ``bene_dcnt``. The annual rollup grain
``(perf_yr, bnmrk, align_type, va_cat, bnmrk_type, aco_id)`` must equal
BNMR ``bene_dcnt_annual``.

Tolerance is zero: a single-beneficiary drift means the eligibility
stamping is wrong.

Fixture strategy
----------------
Hand-crafted inline frames, same as M0/M1. Both sides constructed from
the same underlying story so matches are deterministic. Deliberate
mismatch scenarios prove the assertion fires.
"""

from __future__ import annotations

import polars as pl
import pytest

from acoharmony._transforms._bnmr_risk_reconciliation_view import (
    aggregate_eligibility_annual,
    aggregate_eligibility_monthly,
    build_bnmr_risk_annual_count_reconciliation_view,
    build_bnmr_risk_count_reconciliation_view,
)

ACO_ID = "D0259"


# ---------------------------------------------------------------------------
# Frame schemas & builders
# ---------------------------------------------------------------------------

_ELIGIBILITY_SCHEMA = {
    "member_id": pl.Utf8,
    "perf_yr": pl.Utf8,
    "clndr_yr": pl.Utf8,
    "clndr_mnth": pl.Utf8,
    "bnmrk": pl.Utf8,
    "align_type": pl.Utf8,
    "va_cat": pl.Utf8,
    "bnmrk_type": pl.Utf8,
    "aco_id": pl.Utf8,
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
    "bene_dcnt": pl.Int64,
    "bene_dcnt_annual": pl.Int64,
    "file_date": pl.Utf8,
}

_ELIG_DEFAULTS = {
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
    "bene_dcnt": 0,
    "bene_dcnt_annual": 0,
    "file_date": "2026-05-01",
}


def _elig(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(
        [{**_ELIG_DEFAULTS, **r} for r in rows], schema=_ELIGIBILITY_SCHEMA
    )


def _bnmr(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(
        [{**_BNMR_DEFAULTS, **r} for r in rows], schema=_BNMR_RISK_SCHEMA
    )


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def _basic_monthly_scenario() -> dict:
    """
    Two months, claims-aligned benes (va_cat = 'N' for non-voluntary).

    - 2026-03: B1, B2, B3  → bene_dcnt = 3
    - 2026-04: B1, B2      → bene_dcnt = 2
    - Annual distinct:     B1, B2, B3 → bene_dcnt_annual = 3
    """
    elig = _elig(
        [
            {"member_id": "B1", "clndr_mnth": "3"},
            {"member_id": "B2", "clndr_mnth": "3"},
            {"member_id": "B3", "clndr_mnth": "3"},
            {"member_id": "B1", "clndr_mnth": "4"},
            {"member_id": "B2", "clndr_mnth": "4"},
        ]
    )
    bnmr = _bnmr(
        [
            {"clndr_mnth": "3", "bene_dcnt": 3, "bene_dcnt_annual": 3},
            {"clndr_mnth": "4", "bene_dcnt": 2, "bene_dcnt_annual": 3},
        ]
    )
    return {"elig": elig, "bnmr": bnmr}


def _va_cat_stratified_scenario() -> dict:
    """
    Same month, two va_cat buckets. Distinct count must partition by va_cat.

    - 2026-03 / va_cat='N' (newly voluntary): B1, B2   → 2
    - 2026-03 / va_cat='C' (continuously voluntary): B3, B4, B5 → 3
    - Annual per va_cat: same, since only one month
    """
    elig = _elig(
        [
            {"member_id": "B1", "clndr_mnth": "3", "va_cat": "N"},
            {"member_id": "B2", "clndr_mnth": "3", "va_cat": "N"},
            {"member_id": "B3", "clndr_mnth": "3", "va_cat": "C"},
            {"member_id": "B4", "clndr_mnth": "3", "va_cat": "C"},
            {"member_id": "B5", "clndr_mnth": "3", "va_cat": "C"},
        ]
    )
    bnmr = _bnmr(
        [
            {"clndr_mnth": "3", "va_cat": "N", "bene_dcnt": 2, "bene_dcnt_annual": 2},
            {"clndr_mnth": "3", "va_cat": "C", "bene_dcnt": 3, "bene_dcnt_annual": 3},
        ]
    )
    return {"elig": elig, "bnmr": bnmr}


# ---------------------------------------------------------------------------
# Tests: monthly count tie-out
# ---------------------------------------------------------------------------


class TestMonthlyCountsMatch:
    @pytest.mark.reconciliation
    def test_basic_two_months(self):
        scenario = _basic_monthly_scenario()
        diff = build_bnmr_risk_count_reconciliation_view(
            scenario["bnmr"], scenario["elig"]
        ).collect()
        bad = diff.filter(pl.col("bene_dcnt_diff") > 0)
        assert bad.height == 0, f"Monthly count tie-out failed:\n{bad}"
        # Sanity: both months are present in the join
        assert diff.height == 2
        assert set(diff["clndr_mnth"].to_list()) == {"3", "4"}

    @pytest.mark.reconciliation
    def test_va_cat_stratification(self):
        """A single calendar month split across va_cat buckets must tie
        out each bucket independently."""
        scenario = _va_cat_stratified_scenario()
        diff = build_bnmr_risk_count_reconciliation_view(
            scenario["bnmr"], scenario["elig"]
        ).collect()
        bad = diff.filter(pl.col("bene_dcnt_diff") > 0)
        assert bad.height == 0, f"va_cat-stratified tie-out failed:\n{bad}"
        assert diff.height == 2  # two va_cat rows for the same month

    @pytest.mark.reconciliation
    def test_distinct_counting_ignores_duplicate_rows(self):
        """Duplicate eligibility rows for the same bene-month must not
        inflate the count — we count distinct member_id, not rows."""
        scenario = _basic_monthly_scenario()
        dup_elig_rows = scenario["elig"].collect().to_dicts()
        # Duplicate B1 in March
        dup_elig_rows.append({**_ELIG_DEFAULTS, "member_id": "B1", "clndr_mnth": "3"})
        scenario["elig"] = _elig(
            [{k: v for k, v in r.items() if k in _ELIGIBILITY_SCHEMA} for r in dup_elig_rows]
        )
        diff = build_bnmr_risk_count_reconciliation_view(
            scenario["bnmr"], scenario["elig"]
        ).collect()
        bad = diff.filter(pl.col("bene_dcnt_diff") > 0)
        assert bad.height == 0, (
            f"Duplicate eligibility row inflated the count — distinct "
            f"counting is broken:\n{bad}"
        )


# ---------------------------------------------------------------------------
# Tests: annual count tie-out
# ---------------------------------------------------------------------------


class TestAnnualCountsMatch:
    @pytest.mark.reconciliation
    def test_annual_distinct_across_months(self):
        """B1 and B2 appear in March and April; B3 only in March. Annual
        distinct across the perf_yr = {B1, B2, B3} = 3."""
        scenario = _basic_monthly_scenario()
        diff = build_bnmr_risk_annual_count_reconciliation_view(
            scenario["bnmr"], scenario["elig"]
        ).collect()
        bad = diff.filter(pl.col("bene_dcnt_annual_diff") > 0)
        assert bad.height == 0, f"Annual count tie-out failed:\n{bad}"
        # One row (one (perf_yr, bnmrk, align_type, va_cat, bnmrk_type, aco_id) key)
        assert diff.height == 1
        assert int(diff["expected_bene_dcnt_annual"][0]) == 3

    @pytest.mark.reconciliation
    def test_annual_bnmr_deduplicates_repeated_row(self):
        """BNMR ships bene_dcnt_annual repeated on every monthly row — the
        view must dedupe before comparing."""
        scenario = _basic_monthly_scenario()
        # Both monthly BNMR rows already carry bene_dcnt_annual=3; ensure
        # the annual view produces exactly one output row, not two.
        diff = build_bnmr_risk_annual_count_reconciliation_view(
            scenario["bnmr"], scenario["elig"]
        ).collect()
        assert diff.height == 1


# ---------------------------------------------------------------------------
# Tests: annual PIT filter
# ---------------------------------------------------------------------------


class TestAnnualPointInTimeFiltering:
    @pytest.mark.reconciliation
    def test_post_cutoff_annual_rows_excluded(self):
        """A BNMR row with file_date after cutoff must be dropped from
        the annual view."""
        scenario = _basic_monthly_scenario()
        bnmr_rows = scenario["bnmr"].collect().to_dicts()
        bnmr_rows.append(
            {
                **_BNMR_DEFAULTS,
                "clndr_mnth": "3",
                "bene_dcnt": 9999,
                "bene_dcnt_annual": 9999,
                "file_date": "2026-12-31",
            }
        )
        scenario["bnmr"] = _bnmr(
            [{k: v for k, v in r.items() if k in _BNMR_RISK_SCHEMA} for r in bnmr_rows]
        )
        diff = build_bnmr_risk_annual_count_reconciliation_view(
            scenario["bnmr"], scenario["elig"], as_of_delivery_date="2026-06-30"
        ).collect()
        bad = diff.filter(pl.col("bene_dcnt_annual_diff") > 0)
        assert bad.height == 0


# ---------------------------------------------------------------------------
# Tests: deliberate mismatch detection
# ---------------------------------------------------------------------------


class TestDeliberateMismatchFailsLoudly:
    @pytest.mark.reconciliation
    def test_undercount_in_bnmr_is_caught(self):
        """BNMR under-reports March by one bene. Diff must fire."""
        scenario = _basic_monthly_scenario()
        scenario["bnmr"] = _bnmr(
            [
                {"clndr_mnth": "3", "bene_dcnt": 2, "bene_dcnt_annual": 3},  # -1
                {"clndr_mnth": "4", "bene_dcnt": 2, "bene_dcnt_annual": 3},
            ]
        )
        diff = build_bnmr_risk_count_reconciliation_view(
            scenario["bnmr"], scenario["elig"]
        ).collect()
        bad = diff.filter(pl.col("bene_dcnt_diff") > 0)
        assert bad.height == 1
        row = bad.filter(pl.col("clndr_mnth") == "3").row(0, named=True)
        assert int(row["bene_dcnt"]) == 2
        assert int(row["expected_bene_dcnt"]) == 3
        assert int(row["bene_dcnt_diff"]) == 1

    @pytest.mark.reconciliation
    def test_missing_bnmr_month_is_caught(self):
        """BNMR drops the April row; the view must surface an
        eligibility-only bucket."""
        scenario = _basic_monthly_scenario()
        scenario["bnmr"] = _bnmr(
            [
                {"clndr_mnth": "3", "bene_dcnt": 3, "bene_dcnt_annual": 3},
            ]
        )
        diff = build_bnmr_risk_count_reconciliation_view(
            scenario["bnmr"], scenario["elig"]
        ).collect()
        orphan = diff.filter(pl.col("clndr_mnth") == "4")
        assert orphan.height == 1
        assert int(orphan["bene_dcnt"][0]) == 0
        assert int(orphan["expected_bene_dcnt"][0]) == 2
        assert int(orphan["bene_dcnt_diff"][0]) == 2

    @pytest.mark.reconciliation
    def test_annual_overcount_is_caught(self):
        """BNMR over-reports annual count by 1. Every monthly row carries
        the inflated number, but the annual view reconciles once."""
        scenario = _basic_monthly_scenario()
        scenario["bnmr"] = _bnmr(
            [
                {"clndr_mnth": "3", "bene_dcnt": 3, "bene_dcnt_annual": 4},  # +1
                {"clndr_mnth": "4", "bene_dcnt": 2, "bene_dcnt_annual": 4},
            ]
        )
        diff = build_bnmr_risk_annual_count_reconciliation_view(
            scenario["bnmr"], scenario["elig"]
        ).collect()
        bad = diff.filter(pl.col("bene_dcnt_annual_diff") > 0)
        assert bad.height == 1
        assert int(bad["bene_dcnt_annual"][0]) == 4
        assert int(bad["expected_bene_dcnt_annual"][0]) == 3


# ---------------------------------------------------------------------------
# Tests: point-in-time filter
# ---------------------------------------------------------------------------


class TestPointInTimeFiltering:
    @pytest.mark.reconciliation
    def test_post_cutoff_bnmr_rows_are_excluded(self):
        """A BNMR row with file_date after cutoff must be dropped."""
        scenario = _basic_monthly_scenario()
        bnmr_rows = scenario["bnmr"].collect().to_dicts()
        bnmr_rows.append(
            {
                **_BNMR_DEFAULTS,
                "clndr_mnth": "3",
                "bene_dcnt": 9999,  # would blow up the tie-out
                "bene_dcnt_annual": 9999,
                "file_date": "2026-12-31",
            }
        )
        scenario["bnmr"] = _bnmr(
            [{k: v for k, v in r.items() if k in _BNMR_RISK_SCHEMA} for r in bnmr_rows]
        )
        diff = build_bnmr_risk_count_reconciliation_view(
            scenario["bnmr"], scenario["elig"], as_of_delivery_date="2026-06-30"
        ).collect()
        bad = diff.filter(pl.col("bene_dcnt_diff") > 0)
        assert bad.height == 0, (
            f"Post-cutoff BNMR row leaked past PIT filter:\n{bad}"
        )


# ---------------------------------------------------------------------------
# Tests: aggregator helpers
# ---------------------------------------------------------------------------


class TestAggregateHelpers:
    @pytest.mark.reconciliation
    def test_monthly_aggregator_counts_distinct(self):
        elig = _elig(
            [
                {"member_id": "B1", "clndr_mnth": "3"},
                {"member_id": "B1", "clndr_mnth": "3"},  # duplicate
                {"member_id": "B2", "clndr_mnth": "3"},
            ]
        )
        agg = aggregate_eligibility_monthly(elig).collect()
        assert agg.height == 1
        assert int(agg["expected_bene_dcnt"][0]) == 2

    @pytest.mark.reconciliation
    def test_annual_aggregator_collapses_months(self):
        elig = _elig(
            [
                {"member_id": "B1", "clndr_mnth": "3"},
                {"member_id": "B1", "clndr_mnth": "4"},
                {"member_id": "B2", "clndr_mnth": "5"},
            ]
        )
        agg = aggregate_eligibility_annual(elig).collect()
        assert agg.height == 1
        assert int(agg["expected_bene_dcnt_annual"][0]) == 2
