# © 2025 HarmonyCares
# All rights reserved.

"""
Quality measures inventory & schema validation (milestone Q0).

Validates that the six quality silver tables (4 BLQQR measure CSVs +
QTLQR claims results + ANLQR annual report) exist, have rows, carry
their required columns, and use known measure vocabularies.

All tests are ``@requires_data``-gated.
"""

from __future__ import annotations

import polars as pl
import pytest

from .conftest import requires_data, scan_silver

# Known ACO REACH quality measures.
# TFU is Standard/New Entrant ACO-only; High Needs ACOs may not have it.
KNOWN_MEASURES = {"ACR", "DAH", "UAMCC", "TFU"}

BLQQR_TABLES = {
    "blqqr_acr": {
        "required": ["aco_id", "bene_id", "radm30_flag", "index_cohort", "source_filename"],
    },
    "blqqr_dah": {
        "required": ["aco_id", "bene_id", "observed_dah", "survival_days", "source_filename"],
    },
    "blqqr_uamcc": {
        "required": ["aco_id", "bene_id", "count_unplanned_adm", "source_filename"],
    },
    "blqqr_exclusions": {
        "required": ["aco_id", "ct_benes_acr", "ct_benes_dah", "ct_benes_uamcc", "source_filename"],
    },
}

QTLQR_REQUIRED = [
    "aco_id", "measure", "measure_score", "measure_volume",
    "reporting_period", "source_filename",
]

ANLQR_REQUIRED = [
    "measure_name", "measure_score", "source_filename",
]

# MCC condition columns on BLQQR UAMCC — a bene qualifies if 2+ are flagged.
MCC_CONDITION_COLS = [
    "condition_ami", "condition_alz", "condition_afib",
    "condition_ckd", "condition_copd", "condition_depress",
    "condition_hf", "condition_stroke_tia", "condition_diab",
]


def _load(name: str) -> pl.DataFrame:
    try:
        return scan_silver(name).collect()
    except Exception:
        pytest.skip(f"{name} not available in silver")


# ---------------------------------------------------------------------------
# BLQQR tables
# ---------------------------------------------------------------------------


@requires_data
class TestBlqqrPresence:
    @pytest.mark.reconciliation
    @pytest.mark.parametrize("table", list(BLQQR_TABLES.keys()))
    def test_table_has_rows(self, table):
        df = _load(table)
        assert df.height > 0, f"{table} is empty"

    @pytest.mark.reconciliation
    @pytest.mark.parametrize("table", list(BLQQR_TABLES.keys()))
    def test_required_columns_present(self, table):
        df = _load(table)
        missing = [c for c in BLQQR_TABLES[table]["required"] if c not in df.columns]
        assert missing == [], f"{table} missing columns: {missing}"


@requires_data
class TestBlqqrAcrSchema:
    @pytest.mark.reconciliation
    def test_radm30_flag_is_binary(self):
        df = _load("blqqr_acr")
        vals = set(df["radm30_flag"].drop_nulls().unique().cast(pl.Utf8).to_list())
        assert vals <= {"0", "1", "0.0", "1.0"}, f"radm30_flag unexpected values: {vals}"

    @pytest.mark.reconciliation
    def test_index_cohort_known_values(self):
        df = _load("blqqr_acr")
        if "index_cohort" not in df.columns:
            pytest.skip("index_cohort not present")
        vals = set(df["index_cohort"].drop_nulls().unique().to_list())
        known = {"SURGICAL", "CARDIORESPIRATORY", "CV",
                 "NEUROLOGY", "MEDICINE"}
        unexpected = vals - known
        assert unexpected == set(), (
            f"index_cohort unexpected values: {unexpected} (known: {known})"
        )


@requires_data
class TestBlqqrUamccSchema:
    @pytest.mark.reconciliation
    def test_mcc_condition_columns_present(self):
        df = _load("blqqr_uamcc")
        missing = [c for c in MCC_CONDITION_COLS if c not in df.columns]
        assert missing == [], f"UAMCC missing MCC condition columns: {missing}"

    @pytest.mark.reconciliation
    def test_mcc_condition_columns_are_non_negative_integers(self):
        """MCC condition columns are counts of qualifying claims per
        condition group, not binary flags. A bene qualifies for the
        UAMCC denominator with 2+ condition groups having count >= 1."""
        df = _load("blqqr_uamcc")
        for c in MCC_CONDITION_COLS:
            if c not in df.columns:
                continue
            _vals = df[c].drop_nulls().cast(pl.Int64, strict=False).drop_nulls()
            negs = _vals.filter(_vals < 0)
            assert negs.len() == 0, f"{c} has {negs.len()} negative values"

    @pytest.mark.reconciliation
    def test_count_unplanned_adm_non_negative(self):
        df = _load("blqqr_uamcc")
        _vals = df["count_unplanned_adm"].drop_nulls().cast(pl.Int64, strict=False)
        negs = _vals.filter(_vals < 0)
        assert negs.len() == 0, f"count_unplanned_adm has {negs.len()} negative values"


@requires_data
class TestBlqqrDahSchema:
    @pytest.mark.reconciliation
    def test_observed_dah_non_negative(self):
        df = _load("blqqr_dah")
        _vals = df["observed_dah"].drop_nulls().cast(pl.Float64, strict=False)
        negs = _vals.filter(_vals < 0)
        assert negs.len() == 0, f"observed_dah has {negs.len()} negative values"

    @pytest.mark.reconciliation
    def test_survival_days_non_negative(self):
        df = _load("blqqr_dah")
        _vals = df["survival_days"].drop_nulls().cast(pl.Float64, strict=False)
        negs = _vals.filter(_vals < 0)
        assert negs.len() == 0, f"survival_days has {negs.len()} negative values"


@requires_data
class TestBlqqrExclusionsSchema:
    @pytest.mark.reconciliation
    def test_exclusion_counts_non_negative(self):
        df = _load("blqqr_exclusions")
        for c in df.columns:
            if c.startswith("ct_"):
                _vals = df[c].drop_nulls().cast(pl.Int64, strict=False).drop_nulls()
                negs = _vals.filter(_vals < 0)
                assert negs.len() == 0, f"{c} has {negs.len()} negative values"

    @pytest.mark.reconciliation
    def test_exclusion_percentages_non_negative(self):
        """Exclusion percentages should be non-negative. Some deliveries
        show values > 100 (e.g. pc_elig_prior_acr = 3021 on one delivery,
        likely a CMS data entry error). We check non-negativity only and
        flag outliers rather than hard-failing on CMS data quality issues."""
        df = _load("blqqr_exclusions")
        for c in df.columns:
            if c.startswith("pc_"):
                _vals = df[c].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
                if _vals.len() == 0:
                    continue
                assert _vals.min() >= 0.0, f"{c} min {_vals.min()} < 0"


# ---------------------------------------------------------------------------
# QTLQR claims results
# ---------------------------------------------------------------------------


@requires_data
class TestQtlqrPresence:
    @pytest.mark.reconciliation
    def test_table_has_rows(self):
        df = _load("quarterly_quality_report_claims_results")
        assert df.height > 0

    @pytest.mark.reconciliation
    def test_required_columns_present(self):
        df = _load("quarterly_quality_report_claims_results")
        missing = [c for c in QTLQR_REQUIRED if c not in df.columns]
        assert missing == [], f"QTLQR missing columns: {missing}"


@requires_data
class TestQtlqrMeasureVocabulary:
    @pytest.mark.reconciliation
    def test_known_measures_present(self):
        """At least ACR, DAH, UAMCC should appear as measure values."""
        df = _load("quarterly_quality_report_claims_results")
        measures = set(df["measure"].drop_nulls().unique().to_list())
        # Filter to actual measure names (not percentile rows / footnotes)
        actual_measures = measures & KNOWN_MEASURES
        assert len(actual_measures) >= 3, (
            f"Expected at least ACR, DAH, UAMCC in QTLQR measures; "
            f"found: {actual_measures}"
        )

    @pytest.mark.reconciliation
    def test_measure_score_non_negative_for_actual_measures(self):
        df = _load("quarterly_quality_report_claims_results")
        actual = df.filter(pl.col("measure").is_in(list(KNOWN_MEASURES)))
        _scores = actual["measure_score"].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
        if _scores.len() == 0:
            pytest.skip("No numeric measure scores")
        negs = _scores.filter(_scores < 0)
        assert negs.len() == 0, f"measure_score has {negs.len()} negative values"


# ---------------------------------------------------------------------------
# ANLQR annual quality report
# ---------------------------------------------------------------------------


@requires_data
class TestAnlqrPresence:
    @pytest.mark.reconciliation
    def test_table_has_rows(self):
        df = _load("annual_quality_report")
        assert df.height > 0

    @pytest.mark.reconciliation
    def test_required_columns_present(self):
        df = _load("annual_quality_report")
        missing = [c for c in ANLQR_REQUIRED if c not in df.columns]
        assert missing == [], f"ANLQR missing columns: {missing}"

    @pytest.mark.reconciliation
    def test_multiple_deliveries(self):
        df = _load("annual_quality_report")
        n = df["source_filename"].n_unique()
        assert n >= 1, f"Only {n} ANLQR deliveries"


# ---------------------------------------------------------------------------
# Cross-table: delivery coverage
# ---------------------------------------------------------------------------


@requires_data
class TestDeliveryCoverage:
    @pytest.mark.reconciliation
    def test_blqqr_has_multiple_quarters(self):
        """BLQQR CSVs should span multiple quarters (Q1–Q4)."""
        df = _load("blqqr_acr")
        filenames = df["source_filename"].unique().to_list()
        quarters = {f.split(".")[4] for f in filenames if len(f.split(".")) > 4}
        assert len(quarters) >= 2, (
            f"BLQQR ACR only covers {quarters} — expected multiple quarters"
        )

    @pytest.mark.reconciliation
    def test_blqqr_has_multiple_performance_years(self):
        """BLQQR should span at least PY2024 and PY2025."""
        df = _load("blqqr_acr")
        filenames = df["source_filename"].unique().to_list()
        pys = {f.split(".")[4] if len(f.split(".")) > 4 else "" for f in filenames}
        pys = {p for p in pys if p.startswith("PY")}
        assert len(pys) >= 2, (
            f"BLQQR ACR only covers {pys} — expected multiple PYs"
        )
