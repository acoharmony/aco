# © 2025 HarmonyCares
# All rights reserved.

"""
1:1 reconciliation tie-out: PLARU payment_history == BNMR cap totals.

The BNMR ``reach_bnmr_cap`` sheet reports per-capita-cost totals at a
``(perf_yr, aco_id, bnmrk, pmt_mnth, align_type)`` grain. PLARU's
``payment_history`` sheet ledgers every capitation payment as a row
keyed on ``payment_date`` with ``base_pcc_total``, ``enhanced_pcc_total``,
``apo_total``. Aggregating PLARU by payment month should tie out 1:1
against the corresponding BNMR cap rollup at ``(aco_id, pmt_mnth)``.

Format variance
---------------
CMS changed the BNMR cap layout around April 2025:

- **Old format**: single ``_total`` / ``_total_seq`` columns per metric.
- **New format**: three variants (``_pre_seq_actual``,
  ``_post_seq_actual``, ``_post_seq_paid``) per metric.

A row is strictly one format or the other; the two are NOT coalescible
by addition because the methodology differs. The reconcilable-amount
expressions select the old ``_total`` when present, else the new
``_post_seq_paid``.

Fixture strategy
----------------
Hand-crafted matched inline frames (mirroring ``financial_bnmr_tieout.py``):
both sides constructed from the same underlying story so matches are
deterministic. A deliberate-mismatch scenario proves the assertion fires.
"""

from __future__ import annotations

import polars as pl
import pytest

from acoharmony._transforms._bnmr_cap_reconciliation_view import (
    aggregate_bnmr_cap,
    aggregate_plaru_payment_history,
    build_bnmr_cap_reconciliation_view,
)

TOLERANCE = 0.01
ACO_ID = "D0259"


# ---------------------------------------------------------------------------
# Frame schemas & builders
# ---------------------------------------------------------------------------

_PLARU_PAYMENT_HISTORY_SCHEMA = {
    "payment_date": pl.Utf8,
    "base_pcc_total": pl.Utf8,
    "enhanced_pcc_total": pl.Utf8,
    "apo_total": pl.Utf8,
}

_BNMR_CAP_SCHEMA = {
    "perf_yr": pl.Utf8,
    "aco_id": pl.Utf8,
    "bnmrk": pl.Utf8,
    "pmt_mnth": pl.Utf8,
    "align_type": pl.Utf8,
    # Old-format columns
    "aco_tcc_amt_total": pl.Float64,
    "aco_bpcc_amt_total": pl.Float64,
    "aco_epcc_amt_total_seq": pl.Float64,
    "aco_apo_amt_total_seq": pl.Float64,
    # New-format columns
    "aco_tcc_amt_pre_seq_actual": pl.Float64,
    "aco_tcc_amt_post_seq_actual": pl.Float64,
    "aco_tcc_amt_post_seq_paid": pl.Float64,
    "aco_bpcc_amt_pre_seq_actual": pl.Float64,
    "aco_bpcc_amt_post_seq_actual": pl.Float64,
    "aco_bpcc_amt_post_seq_paid": pl.Float64,
    "aco_apo_amt_pre_seq_actual": pl.Float64,
    "aco_apo_amt_post_seq_actual": pl.Float64,
    "aco_apo_amt_post_seq_paid": pl.Float64,
    "aco_epcc_amt_post_seq_paid": pl.Float64,
    "file_date": pl.Utf8,
}

_OLD_FORMAT_DEFAULTS = {
    "perf_yr": "2024",
    "aco_id": ACO_ID,
    "bnmrk": "AD",
    "pmt_mnth": "2024-03",
    "align_type": "C",
    "aco_tcc_amt_total": 0.0,
    "aco_bpcc_amt_total": 0.0,
    "aco_epcc_amt_total_seq": 0.0,
    "aco_apo_amt_total_seq": 0.0,
    "aco_tcc_amt_pre_seq_actual": None,
    "aco_tcc_amt_post_seq_actual": None,
    "aco_tcc_amt_post_seq_paid": None,
    "aco_bpcc_amt_pre_seq_actual": None,
    "aco_bpcc_amt_post_seq_actual": None,
    "aco_bpcc_amt_post_seq_paid": None,
    "aco_apo_amt_pre_seq_actual": None,
    "aco_apo_amt_post_seq_actual": None,
    "aco_apo_amt_post_seq_paid": None,
    "aco_epcc_amt_post_seq_paid": None,
    "file_date": "2024-05-01",
}

_NEW_FORMAT_DEFAULTS = {
    "perf_yr": "2026",
    "aco_id": ACO_ID,
    "bnmrk": "AD",
    "pmt_mnth": "2026-03",
    "align_type": "C",
    "aco_tcc_amt_total": None,
    "aco_bpcc_amt_total": None,
    "aco_epcc_amt_total_seq": None,
    "aco_apo_amt_total_seq": None,
    "aco_tcc_amt_pre_seq_actual": 0.0,
    "aco_tcc_amt_post_seq_actual": 0.0,
    "aco_tcc_amt_post_seq_paid": 0.0,
    "aco_bpcc_amt_pre_seq_actual": 0.0,
    "aco_bpcc_amt_post_seq_actual": 0.0,
    "aco_bpcc_amt_post_seq_paid": 0.0,
    "aco_apo_amt_pre_seq_actual": 0.0,
    "aco_apo_amt_post_seq_actual": 0.0,
    "aco_apo_amt_post_seq_paid": 0.0,
    "aco_epcc_amt_post_seq_paid": 0.0,
    "file_date": "2026-05-01",
}


def _plaru(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(rows, schema=_PLARU_PAYMENT_HISTORY_SCHEMA)


def _bnmr_cap_old(rows: list[dict]) -> pl.LazyFrame:
    """Build a BNMR cap LazyFrame with old-format defaults (pre-April 2025)."""
    return pl.LazyFrame(
        [{**_OLD_FORMAT_DEFAULTS, **r} for r in rows], schema=_BNMR_CAP_SCHEMA
    )


def _bnmr_cap_new(rows: list[dict]) -> pl.LazyFrame:
    """Build a BNMR cap LazyFrame with new-format defaults (April 2025+)."""
    return pl.LazyFrame(
        [{**_NEW_FORMAT_DEFAULTS, **r} for r in rows], schema=_BNMR_CAP_SCHEMA
    )


# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------


def _old_format_matched_scenario() -> dict:
    """
    Single ACO, two payment months, old-format BNMR cap matches PLARU
    aggregation exactly.

    - 2024-03: BPCC $1000 + EPCC $200 + APO $50  → TCC $1250
    - 2024-04: BPCC $1100 + EPCC $250 + APO $60  → TCC $1410
    """
    plaru = _plaru(
        [
            {
                "payment_date": "03/15/2024",
                "base_pcc_total": "600.00",
                "enhanced_pcc_total": "120.00",
                "apo_total": "30.00",
            },
            {
                "payment_date": "03/30/2024",
                "base_pcc_total": "400.00",
                "enhanced_pcc_total": "80.00",
                "apo_total": "20.00",
            },
            {
                "payment_date": "04/15/2024",
                "base_pcc_total": "1100.00",
                "enhanced_pcc_total": "250.00",
                "apo_total": "60.00",
            },
        ]
    )
    bnmr = _bnmr_cap_old(
        [
            {
                "pmt_mnth": "2024-03",
                "aco_bpcc_amt_total": 1000.0,
                "aco_epcc_amt_total_seq": 200.0,
                "aco_apo_amt_total_seq": 50.0,
                "aco_tcc_amt_total": 1250.0,
            },
            {
                "pmt_mnth": "2024-04",
                "aco_bpcc_amt_total": 1100.0,
                "aco_epcc_amt_total_seq": 250.0,
                "aco_apo_amt_total_seq": 60.0,
                "aco_tcc_amt_total": 1410.0,
            },
        ]
    )
    return {"plaru": plaru, "bnmr": bnmr}


def _new_format_matched_scenario() -> dict:
    """
    Single ACO, one payment month, new-format BNMR cap matches PLARU.
    """
    plaru = _plaru(
        [
            {
                "payment_date": "03/15/2026",
                "base_pcc_total": "500.00",
                "enhanced_pcc_total": "100.00",
                "apo_total": "25.00",
            },
        ]
    )
    bnmr = _bnmr_cap_new(
        [
            {
                "pmt_mnth": "2026-03",
                "aco_bpcc_amt_pre_seq_actual": 520.0,
                "aco_bpcc_amt_post_seq_actual": 510.0,
                "aco_bpcc_amt_post_seq_paid": 500.0,
                "aco_epcc_amt_post_seq_paid": 100.0,
                "aco_apo_amt_pre_seq_actual": 26.0,
                "aco_apo_amt_post_seq_actual": 25.5,
                "aco_apo_amt_post_seq_paid": 25.0,
                "aco_tcc_amt_pre_seq_actual": 646.0,
                "aco_tcc_amt_post_seq_actual": 635.5,
                "aco_tcc_amt_post_seq_paid": 625.0,
            },
        ]
    )
    return {"plaru": plaru, "bnmr": bnmr}


# ---------------------------------------------------------------------------
# Tests: old-format tie-out
# ---------------------------------------------------------------------------


class TestOldFormatTiesOut:
    @pytest.mark.reconciliation
    def test_bpcc_matches(self):
        scenario = _old_format_matched_scenario()
        diff = build_bnmr_cap_reconciliation_view(
            scenario["bnmr"], scenario["plaru"], aco_id=ACO_ID
        ).collect()
        out = diff.filter(pl.col("bpcc_diff") > TOLERANCE)
        assert out.height == 0, f"BPCC tie-out failed:\n{out}"

    @pytest.mark.reconciliation
    def test_epcc_matches(self):
        scenario = _old_format_matched_scenario()
        diff = build_bnmr_cap_reconciliation_view(
            scenario["bnmr"], scenario["plaru"], aco_id=ACO_ID
        ).collect()
        out = diff.filter(pl.col("epcc_diff") > TOLERANCE)
        assert out.height == 0, f"EPCC tie-out failed:\n{out}"

    @pytest.mark.reconciliation
    def test_apo_matches(self):
        scenario = _old_format_matched_scenario()
        diff = build_bnmr_cap_reconciliation_view(
            scenario["bnmr"], scenario["plaru"], aco_id=ACO_ID
        ).collect()
        out = diff.filter(pl.col("apo_diff") > TOLERANCE)
        assert out.height == 0, f"APO tie-out failed:\n{out}"

    @pytest.mark.reconciliation
    def test_tcc_matches_plaru_sum(self):
        scenario = _old_format_matched_scenario()
        diff = build_bnmr_cap_reconciliation_view(
            scenario["bnmr"], scenario["plaru"], aco_id=ACO_ID
        ).collect()
        out = diff.filter(pl.col("tcc_diff") > TOLERANCE)
        assert out.height == 0, f"TCC tie-out failed:\n{out}"

    @pytest.mark.reconciliation
    def test_tcc_internal_consistency(self):
        """TCC on BNMR side must equal BPCC + EPCC + APO on BNMR side."""
        scenario = _old_format_matched_scenario()
        diff = build_bnmr_cap_reconciliation_view(
            scenario["bnmr"], scenario["plaru"], aco_id=ACO_ID
        ).collect()
        out = diff.filter(pl.col("tcc_internal_diff") > TOLERANCE)
        assert out.height == 0, f"BNMR internal TCC inconsistency:\n{out}"


# ---------------------------------------------------------------------------
# Tests: new-format tie-out
# ---------------------------------------------------------------------------


class TestNewFormatTiesOut:
    @pytest.mark.reconciliation
    def test_new_format_all_components_match(self):
        scenario = _new_format_matched_scenario()
        diff = build_bnmr_cap_reconciliation_view(
            scenario["bnmr"], scenario["plaru"], aco_id=ACO_ID
        ).collect()
        problems = diff.filter(
            (pl.col("bpcc_diff") > TOLERANCE)
            | (pl.col("epcc_diff") > TOLERANCE)
            | (pl.col("apo_diff") > TOLERANCE)
            | (pl.col("tcc_diff") > TOLERANCE)
        )
        assert problems.height == 0, f"New-format tie-out failed:\n{problems}"

    @pytest.mark.reconciliation
    def test_new_format_reconcilable_picks_post_seq_paid(self):
        """Reconcilable amount must be the ``_post_seq_paid`` field, not
        the pre-seq or post-seq-actual. A row with paid=$500 and
        pre-seq=$520 must reconcile at $500."""
        bnmr = _bnmr_cap_new(
            [
                {
                    "pmt_mnth": "2026-03",
                    "aco_bpcc_amt_pre_seq_actual": 520.0,
                    "aco_bpcc_amt_post_seq_actual": 510.0,
                    "aco_bpcc_amt_post_seq_paid": 500.0,
                }
            ]
        )
        agg = aggregate_bnmr_cap(bnmr).collect()
        assert float(agg["bnmr_bpcc"][0]) == pytest.approx(500.0)


# ---------------------------------------------------------------------------
# Tests: format fork exclusivity
# ---------------------------------------------------------------------------


class TestFormatForkExclusivity:
    """A single row must be old format XOR new format, never both populated.

    The expression that picks reconcilable values uses ``pl.coalesce`` —
    a row that mistakenly has both populated would silently prefer the
    old-format value. This test proves that in practice our fixtures
    only populate one side at a time.
    """

    @pytest.mark.reconciliation
    def test_old_format_fixture_has_no_new_fields_set(self):
        scenario = _old_format_matched_scenario()
        bnmr = scenario["bnmr"].collect()
        new_fields = [
            "aco_tcc_amt_pre_seq_actual",
            "aco_tcc_amt_post_seq_actual",
            "aco_tcc_amt_post_seq_paid",
            "aco_bpcc_amt_pre_seq_actual",
            "aco_bpcc_amt_post_seq_actual",
            "aco_bpcc_amt_post_seq_paid",
            "aco_apo_amt_pre_seq_actual",
            "aco_apo_amt_post_seq_actual",
            "aco_apo_amt_post_seq_paid",
            "aco_epcc_amt_post_seq_paid",
        ]
        for f in new_fields:
            vals = bnmr[f].drop_nulls()
            assert vals.len() == 0, (
                f"Old-format fixture unexpectedly has {f} populated"
            )

    @pytest.mark.reconciliation
    def test_new_format_fixture_has_no_old_fields_set(self):
        scenario = _new_format_matched_scenario()
        bnmr = scenario["bnmr"].collect()
        old_fields = [
            "aco_tcc_amt_total",
            "aco_bpcc_amt_total",
            "aco_epcc_amt_total_seq",
            "aco_apo_amt_total_seq",
        ]
        for f in old_fields:
            vals = bnmr[f].drop_nulls()
            assert vals.len() == 0, (
                f"New-format fixture unexpectedly has {f} populated"
            )


# ---------------------------------------------------------------------------
# Tests: deliberate mismatch detection
# ---------------------------------------------------------------------------


class TestDeliberateMismatchFailsLoudly:
    @pytest.mark.reconciliation
    def test_inflated_bnmr_bpcc_is_caught(self):
        """Inflate BNMR BPCC by $75 and confirm the diff fires."""
        scenario = _old_format_matched_scenario()
        scenario["bnmr"] = _bnmr_cap_old(
            [
                {
                    "pmt_mnth": "2024-03",
                    "aco_bpcc_amt_total": 1075.0,  # +$75
                    "aco_epcc_amt_total_seq": 200.0,
                    "aco_apo_amt_total_seq": 50.0,
                    "aco_tcc_amt_total": 1325.0,
                },
                {
                    "pmt_mnth": "2024-04",
                    "aco_bpcc_amt_total": 1100.0,
                    "aco_epcc_amt_total_seq": 250.0,
                    "aco_apo_amt_total_seq": 60.0,
                    "aco_tcc_amt_total": 1410.0,
                },
            ]
        )
        diff = build_bnmr_cap_reconciliation_view(
            scenario["bnmr"], scenario["plaru"], aco_id=ACO_ID
        ).collect()
        bad = diff.filter(pl.col("bpcc_diff") > TOLERANCE)
        assert bad.height >= 1, (
            "Deliberate $75 BPCC inflation was not caught — reconciliation "
            "is not actually firing"
        )
        row = bad.filter(pl.col("pmt_mnth") == "2024-03").row(0, named=True)
        assert float(row["bpcc_diff"]) == pytest.approx(75.0)

    @pytest.mark.reconciliation
    def test_missing_bnmr_month_is_caught(self):
        """BNMR drops the 2024-04 row; diff must flag a PLARU-only bucket."""
        scenario = _old_format_matched_scenario()
        scenario["bnmr"] = _bnmr_cap_old(
            [
                {
                    "pmt_mnth": "2024-03",
                    "aco_bpcc_amt_total": 1000.0,
                    "aco_epcc_amt_total_seq": 200.0,
                    "aco_apo_amt_total_seq": 50.0,
                    "aco_tcc_amt_total": 1250.0,
                },
            ]
        )
        diff = build_bnmr_cap_reconciliation_view(
            scenario["bnmr"], scenario["plaru"], aco_id=ACO_ID
        ).collect()
        orphan = diff.filter(pl.col("pmt_mnth") == "2024-04")
        assert orphan.height == 1
        assert float(orphan["bnmr_bpcc"][0]) == pytest.approx(0.0)
        assert float(orphan["plaru_bpcc"][0]) == pytest.approx(1100.0)
        assert float(orphan["bpcc_diff"][0]) == pytest.approx(1100.0)


# ---------------------------------------------------------------------------
# Tests: point-in-time filter
# ---------------------------------------------------------------------------


class TestPointInTimeFiltering:
    @pytest.mark.reconciliation
    def test_post_cutoff_bnmr_rows_are_excluded(self):
        """A BNMR row with file_date after the cutoff must be dropped so
        later-delivered data can't inflate a historical reconciliation."""
        scenario = _old_format_matched_scenario()
        # Add a post-cutoff BNMR row that would otherwise break the tie-out.
        bnmr_rows = scenario["bnmr"].collect().to_dicts()
        bnmr_rows.append(
            {
                **_OLD_FORMAT_DEFAULTS,
                "pmt_mnth": "2024-03",
                "aco_bpcc_amt_total": 99999.0,  # would blow up the tie-out
                "file_date": "2024-12-31",
            }
        )
        scenario["bnmr"] = _bnmr_cap_old(
            [{k: v for k, v in r.items() if k in _BNMR_CAP_SCHEMA} for r in bnmr_rows]
        )
        diff = build_bnmr_cap_reconciliation_view(
            scenario["bnmr"],
            scenario["plaru"],
            aco_id=ACO_ID,
            as_of_delivery_date="2024-06-30",
        ).collect()
        out = diff.filter(
            (pl.col("bpcc_diff") > TOLERANCE)
            | (pl.col("epcc_diff") > TOLERANCE)
            | (pl.col("apo_diff") > TOLERANCE)
        )
        assert out.height == 0, (
            f"Post-cutoff BNMR row leaked past the PIT filter:\n{out}"
        )


# ---------------------------------------------------------------------------
# Tests: aggregate helpers
# ---------------------------------------------------------------------------


class TestAggregateHelpers:
    @pytest.mark.reconciliation
    def test_plaru_aggregation_by_month(self):
        """Two PLARU rows in the same month must sum to one output row."""
        plaru = _plaru(
            [
                {
                    "payment_date": "03/15/2024",
                    "base_pcc_total": "600.00",
                    "enhanced_pcc_total": "120.00",
                    "apo_total": "30.00",
                },
                {
                    "payment_date": "03/30/2024",
                    "base_pcc_total": "400.00",
                    "enhanced_pcc_total": "80.00",
                    "apo_total": "20.00",
                },
            ]
        )
        agg = aggregate_plaru_payment_history(plaru, aco_id=ACO_ID).collect()
        assert agg.height == 1
        row = agg.row(0, named=True)
        assert row["pmt_mnth"] == "2024-03"
        assert float(row["plaru_bpcc"]) == pytest.approx(1000.0)
        assert float(row["plaru_epcc"]) == pytest.approx(200.0)
        assert float(row["plaru_apo"]) == pytest.approx(50.0)
        assert float(row["plaru_tcc"]) == pytest.approx(1250.0)
