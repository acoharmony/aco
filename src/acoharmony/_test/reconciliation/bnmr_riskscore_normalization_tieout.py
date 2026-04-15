# © 2025 HarmonyCares
# All rights reserved.

"""
1:1 reconciliation tie-out: normalization + capping + benchmark chain
on the BNMR ``riskscore_ad`` / ``riskscore_esrd`` sheets (milestone M9).

CMS documents the chain in the sheets' own line descriptions:

    Normalized = Raw × Normalization Factor
    Floor      = 0.97 × RY Normalized
    Ceiling    = 1.03 × RY Normalized
    Capped     = clamp(Normalized, Floor, Ceiling)
    Benchmark  = Capped × CIF

When the reference-year normalized score is missing (new ACO, no prior
baseline), the cap bypasses and Capped = Normalized.

Fixture strategy mirrors M0/M1/M2a/M2b: hand-crafted inline frames with
known inputs and computed expected outputs so matches are deterministic.
Deliberate-mismatch scenarios prove the assertions fire.

Both AD and ESRD use identical math — one set of scenarios covers both,
parameterized where it matters.
"""

from __future__ import annotations

import polars as pl
import pytest

from acoharmony._transforms._bnmr_risk_normalization_view import (
    RISK_NORMALIZATION_TOLERANCE,
    build_bnmr_risk_normalization_reconciliation_view,
    prepare_riskscore_inputs,
)

ACO_ID = "D0259"

_RISKSCORE_SCHEMA = {
    "source_filename": pl.Utf8,
    "aco_id": pl.Utf8,
    "performance_year": pl.Utf8,
    "raw_risk_score": pl.Utf8,
    "normalization_factor": pl.Utf8,
    "ry_normalized_risk_score": pl.Utf8,
    "cif": pl.Utf8,
    "normalized_risk_score_claims_py": pl.Utf8,
    "capped_risk_score_claims_py": pl.Utf8,
    "benchmark_risk_score_claims_py": pl.Utf8,
    "file_date": pl.Utf8,
}

_DEFAULTS: dict[str, object] = {
    "aco_id": ACO_ID,
    "performance_year": "2024",
    "file_date": "2024-06-30",
}


def _sheet(rows: list[dict]) -> pl.LazyFrame:
    """Build a riskscore-sheet LazyFrame. Every numeric field arrives as
    a string since that's how silver lands it; the transform casts on
    the way in."""
    filled = []
    for r in rows:
        merged = {**_DEFAULTS, **r}
        # Stringify any scalar the caller passed as float/None for convenience
        for k in (
            "raw_risk_score",
            "normalization_factor",
            "ry_normalized_risk_score",
            "cif",
            "normalized_risk_score_claims_py",
            "capped_risk_score_claims_py",
            "benchmark_risk_score_claims_py",
        ):
            v = merged.get(k)
            merged[k] = None if v is None else str(v)
        filled.append(merged)
    return pl.LazyFrame(filled, schema=_RISKSCORE_SCHEMA)


# ---------------------------------------------------------------------------
# Normalization tie-out
# ---------------------------------------------------------------------------


class TestNormalizationArithmetic:
    @pytest.mark.reconciliation
    def test_normalized_equals_raw_times_factor(self):
        """Raw 1.00 × NF 1.15 = Normalized 1.15."""
        sheet = _sheet(
            [
                {
                    "source_filename": "DELIVERY1.xlsx",
                    "raw_risk_score": 1.0,
                    "normalization_factor": 1.15,
                    "ry_normalized_risk_score": None,  # no prior baseline
                    "cif": 1.0,
                    "normalized_risk_score_claims_py": 1.15,
                    "capped_risk_score_claims_py": 1.15,  # cap bypasses
                    "benchmark_risk_score_claims_py": 1.15,
                },
            ]
        )
        diff = build_bnmr_risk_normalization_reconciliation_view(sheet).collect()
        row = diff.row(0, named=True)
        assert float(row["expected_normalized_risk_score"]) == pytest.approx(1.15)
        assert float(row["normalized_risk_score_diff"]) < RISK_NORMALIZATION_TOLERANCE

    @pytest.mark.reconciliation
    def test_multiple_deliveries_each_tie_out_independently(self):
        sheet = _sheet(
            [
                {
                    "source_filename": "D1.xlsx",
                    "raw_risk_score": 1.0,
                    "normalization_factor": 1.10,
                    "ry_normalized_risk_score": None,
                    "cif": 1.0,
                    "normalized_risk_score_claims_py": 1.10,
                    "capped_risk_score_claims_py": 1.10,
                    "benchmark_risk_score_claims_py": 1.10,
                },
                {
                    "source_filename": "D2.xlsx",
                    "raw_risk_score": 0.8,
                    "normalization_factor": 1.05,
                    "ry_normalized_risk_score": None,
                    "cif": 1.0,
                    "normalized_risk_score_claims_py": 0.84,
                    "capped_risk_score_claims_py": 0.84,
                    "benchmark_risk_score_claims_py": 0.84,
                },
            ]
        )
        diff = build_bnmr_risk_normalization_reconciliation_view(sheet).collect()
        bad = diff.filter(
            pl.col("normalized_risk_score_diff") > RISK_NORMALIZATION_TOLERANCE
        )
        assert bad.height == 0, f"Normalization tie-out failed:\n{bad}"


# ---------------------------------------------------------------------------
# Capping logic
# ---------------------------------------------------------------------------


class TestCapping:
    @pytest.mark.reconciliation
    def test_cap_bypass_when_ry_normalized_missing(self):
        """No RY baseline → cap does nothing → capped == normalized."""
        sheet = _sheet(
            [
                {
                    "source_filename": "D.xlsx",
                    "raw_risk_score": 2.0,
                    "normalization_factor": 1.15,
                    "ry_normalized_risk_score": None,
                    "cif": 1.0,
                    "normalized_risk_score_claims_py": 2.3,
                    "capped_risk_score_claims_py": 2.3,
                    "benchmark_risk_score_claims_py": 2.3,
                },
            ]
        )
        diff = build_bnmr_risk_normalization_reconciliation_view(sheet).collect()
        row = diff.row(0, named=True)
        assert float(row["expected_capped_risk_score"]) == pytest.approx(2.3)
        assert float(row["capped_risk_score_diff"]) < RISK_NORMALIZATION_TOLERANCE

    @pytest.mark.reconciliation
    def test_cap_binds_at_ceiling(self):
        """Score grew 10% but cap caps at +3% of RY normalized."""
        # RY normalized = 1.00, so floor = 0.97, ceiling = 1.03
        # Normalized = 1.10 (huge jump) → capped at 1.03
        sheet = _sheet(
            [
                {
                    "source_filename": "D.xlsx",
                    "raw_risk_score": 1.0,
                    "normalization_factor": 1.10,
                    "ry_normalized_risk_score": 1.0,
                    "cif": 1.0,
                    "normalized_risk_score_claims_py": 1.10,
                    "capped_risk_score_claims_py": 1.03,
                    "benchmark_risk_score_claims_py": 1.03,
                },
            ]
        )
        diff = build_bnmr_risk_normalization_reconciliation_view(sheet).collect()
        row = diff.row(0, named=True)
        assert float(row["expected_cap_ceiling"]) == pytest.approx(1.03)
        assert float(row["expected_capped_risk_score"]) == pytest.approx(1.03)
        assert float(row["capped_risk_score_diff"]) < RISK_NORMALIZATION_TOLERANCE

    @pytest.mark.reconciliation
    def test_cap_binds_at_floor(self):
        """Score dropped 10%; cap floors at -3% of RY normalized."""
        sheet = _sheet(
            [
                {
                    "source_filename": "D.xlsx",
                    "raw_risk_score": 1.0,
                    "normalization_factor": 0.90,
                    "ry_normalized_risk_score": 1.0,
                    "cif": 1.0,
                    "normalized_risk_score_claims_py": 0.90,
                    "capped_risk_score_claims_py": 0.97,
                    "benchmark_risk_score_claims_py": 0.97,
                },
            ]
        )
        diff = build_bnmr_risk_normalization_reconciliation_view(sheet).collect()
        row = diff.row(0, named=True)
        assert float(row["expected_cap_floor"]) == pytest.approx(0.97)
        assert float(row["expected_capped_risk_score"]) == pytest.approx(0.97)
        assert float(row["capped_risk_score_diff"]) < RISK_NORMALIZATION_TOLERANCE

    @pytest.mark.reconciliation
    def test_cap_does_not_bind_inside_corridor(self):
        """Score moved 2% — inside the ±3% corridor → capped = normalized."""
        sheet = _sheet(
            [
                {
                    "source_filename": "D.xlsx",
                    "raw_risk_score": 1.0,
                    "normalization_factor": 1.02,
                    "ry_normalized_risk_score": 1.0,
                    "cif": 1.0,
                    "normalized_risk_score_claims_py": 1.02,
                    "capped_risk_score_claims_py": 1.02,
                    "benchmark_risk_score_claims_py": 1.02,
                },
            ]
        )
        diff = build_bnmr_risk_normalization_reconciliation_view(sheet).collect()
        row = diff.row(0, named=True)
        assert float(row["expected_capped_risk_score"]) == pytest.approx(1.02)
        assert float(row["capped_risk_score_diff"]) < RISK_NORMALIZATION_TOLERANCE


# ---------------------------------------------------------------------------
# Benchmark (CIF multiplication)
# ---------------------------------------------------------------------------


class TestBenchmark:
    @pytest.mark.reconciliation
    def test_benchmark_equals_capped_times_cif(self):
        """Benchmark = Capped × CIF. With CIF = 1.009, capped 1.03 → 1.03927."""
        sheet = _sheet(
            [
                {
                    "source_filename": "D.xlsx",
                    "raw_risk_score": 1.0,
                    "normalization_factor": 1.10,
                    "ry_normalized_risk_score": 1.0,
                    "cif": 1.009,
                    "normalized_risk_score_claims_py": 1.10,
                    "capped_risk_score_claims_py": 1.03,
                    "benchmark_risk_score_claims_py": 1.03 * 1.009,
                },
            ]
        )
        diff = build_bnmr_risk_normalization_reconciliation_view(sheet).collect()
        row = diff.row(0, named=True)
        assert float(row["expected_benchmark_risk_score"]) == pytest.approx(
            1.03 * 1.009
        )
        assert float(row["benchmark_risk_score_diff"]) < RISK_NORMALIZATION_TOLERANCE


# ---------------------------------------------------------------------------
# Deliberate mismatch — assertions must fire
# ---------------------------------------------------------------------------


class TestDeliberateMismatchFailsLoudly:
    @pytest.mark.reconciliation
    def test_wrong_normalization_factor_is_caught(self):
        """CMS sheet reports Normalized 1.15 but Raw × NF = 1.25 → diff = 0.10."""
        sheet = _sheet(
            [
                {
                    "source_filename": "D.xlsx",
                    "raw_risk_score": 1.0,
                    "normalization_factor": 1.25,  # real
                    "ry_normalized_risk_score": None,
                    "cif": 1.0,
                    "normalized_risk_score_claims_py": 1.15,  # reported (wrong)
                    "capped_risk_score_claims_py": 1.15,
                    "benchmark_risk_score_claims_py": 1.15,
                },
            ]
        )
        diff = build_bnmr_risk_normalization_reconciliation_view(sheet).collect()
        row = diff.row(0, named=True)
        assert float(row["normalized_risk_score_diff"]) == pytest.approx(0.10)
        assert float(row["normalized_risk_score_diff"]) > RISK_NORMALIZATION_TOLERANCE

    @pytest.mark.reconciliation
    def test_miscomputed_capped_value_is_caught(self):
        """Normalized 1.20 with RY = 1.00 should cap to 1.03, but sheet
        reports 1.10 (no cap applied). Diff should flag."""
        sheet = _sheet(
            [
                {
                    "source_filename": "D.xlsx",
                    "raw_risk_score": 1.0,
                    "normalization_factor": 1.20,
                    "ry_normalized_risk_score": 1.0,
                    "cif": 1.0,
                    "normalized_risk_score_claims_py": 1.20,
                    "capped_risk_score_claims_py": 1.10,  # wrong; should be 1.03
                    "benchmark_risk_score_claims_py": 1.10,
                },
            ]
        )
        diff = build_bnmr_risk_normalization_reconciliation_view(sheet).collect()
        row = diff.row(0, named=True)
        # expected capped = 1.03, reported = 1.10, diff = 0.07
        assert float(row["capped_risk_score_diff"]) == pytest.approx(0.07)


# ---------------------------------------------------------------------------
# Tolerance boundary
# ---------------------------------------------------------------------------


class TestToleranceBoundary:
    @pytest.mark.reconciliation
    def test_tiny_rounding_passes_but_one_bp_fails(self):
        """Off by 5e-6 passes (well inside 1e-4); off by 1e-3 fails."""
        # Passes
        sheet_ok = _sheet(
            [
                {
                    "source_filename": "D.xlsx",
                    "raw_risk_score": 1.0,
                    "normalization_factor": 1.15,
                    "ry_normalized_risk_score": None,
                    "cif": 1.0,
                    "normalized_risk_score_claims_py": 1.150005,
                    "capped_risk_score_claims_py": 1.150005,
                    "benchmark_risk_score_claims_py": 1.150005,
                },
            ]
        )
        diff_ok = build_bnmr_risk_normalization_reconciliation_view(sheet_ok).collect()
        assert (
            float(diff_ok.row(0, named=True)["normalized_risk_score_diff"])
            < RISK_NORMALIZATION_TOLERANCE
        )

        # Fails
        sheet_bad = _sheet(
            [
                {
                    "source_filename": "D.xlsx",
                    "raw_risk_score": 1.0,
                    "normalization_factor": 1.15,
                    "ry_normalized_risk_score": None,
                    "cif": 1.0,
                    "normalized_risk_score_claims_py": 1.151,
                    "capped_risk_score_claims_py": 1.151,
                    "benchmark_risk_score_claims_py": 1.151,
                },
            ]
        )
        diff_bad = build_bnmr_risk_normalization_reconciliation_view(sheet_bad).collect()
        assert (
            float(diff_bad.row(0, named=True)["normalized_risk_score_diff"])
            > RISK_NORMALIZATION_TOLERANCE
        )


# ---------------------------------------------------------------------------
# Point-in-time filter
# ---------------------------------------------------------------------------


class TestPointInTimeFiltering:
    @pytest.mark.reconciliation
    def test_post_cutoff_delivery_excluded(self):
        sheet = _sheet(
            [
                {
                    "source_filename": "OLD.xlsx",
                    "raw_risk_score": 1.0,
                    "normalization_factor": 1.10,
                    "ry_normalized_risk_score": None,
                    "cif": 1.0,
                    "normalized_risk_score_claims_py": 1.10,
                    "capped_risk_score_claims_py": 1.10,
                    "benchmark_risk_score_claims_py": 1.10,
                    "file_date": "2024-06-30",
                },
                {
                    "source_filename": "FUTURE.xlsx",
                    "raw_risk_score": 99.0,  # would blow up tie-out
                    "normalization_factor": 1.0,
                    "ry_normalized_risk_score": None,
                    "cif": 1.0,
                    "normalized_risk_score_claims_py": 50.0,  # deliberately wrong
                    "capped_risk_score_claims_py": 50.0,
                    "benchmark_risk_score_claims_py": 50.0,
                    "file_date": "2025-12-31",  # after cutoff
                },
            ]
        )
        diff = build_bnmr_risk_normalization_reconciliation_view(
            sheet, as_of_delivery_date="2024-12-31"
        ).collect()
        # FUTURE row filtered out → only the clean OLD row should appear
        assert diff.height == 1
        assert diff.row(0, named=True)["source_filename"] == "OLD.xlsx"
        bad = diff.filter(
            pl.col("normalized_risk_score_diff") > RISK_NORMALIZATION_TOLERANCE
        )
        assert bad.height == 0


# ---------------------------------------------------------------------------
# Aggregate helper
# ---------------------------------------------------------------------------


class TestPrepareRiskscoreInputs:
    @pytest.mark.reconciliation
    def test_deduplicates_repeated_broadcast_rows(self):
        """BNMR parser broadcasts named fields onto every row of the
        riskscore sheet (~36 per delivery). prepare_riskscore_inputs
        must dedupe to one row per delivery."""
        rows = [
            {
                "source_filename": "D.xlsx",
                "raw_risk_score": 1.0,
                "normalization_factor": 1.15,
                "ry_normalized_risk_score": None,
                "cif": 1.0,
                "normalized_risk_score_claims_py": 1.15,
                "capped_risk_score_claims_py": 1.15,
                "benchmark_risk_score_claims_py": 1.15,
            }
        ] * 36
        sheet = _sheet(rows)
        prepped = prepare_riskscore_inputs(sheet).collect()
        assert prepped.height == 1

    @pytest.mark.reconciliation
    def test_casts_numeric_cols_to_float(self):
        sheet = _sheet(
            [
                {
                    "source_filename": "D.xlsx",
                    "raw_risk_score": 1.5,
                    "normalization_factor": 1.15,
                    "ry_normalized_risk_score": None,
                    "cif": 1.009,
                    "normalized_risk_score_claims_py": 1.725,
                    "capped_risk_score_claims_py": 1.725,
                    "benchmark_risk_score_claims_py": 1.741,
                },
            ]
        )
        prepped = prepare_riskscore_inputs(sheet).collect()
        for c in (
            "raw_risk_score",
            "normalization_factor",
            "cif",
            "normalized_risk_score_claims_py",
        ):
            assert prepped.schema[c] == pl.Float64
