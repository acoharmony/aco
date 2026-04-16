# © 2025 HarmonyCares
# All rights reserved.

"""
DAH + UAMCC tie-out: BLQQR ↔ Exclusions ↔ QTLQR (milestones Q3 + Q4).

DAH (Days at Home)
------------------
- BLQQR DAH: 1 row per bene per quarter. ``observed_dah`` is actually
  **days in care** (facility days), not days at home. Raw DAH =
  ``survival_days - observed_dah``.
- QTLQR measure_score is the **risk-adjusted annual DAH** (~320–330
  days/year for a well-performing ACO).
- QTLQR measure_volume is **person-years** (fractional), not bene count.

UAMCC (Unplanned Admissions for Multiple Chronic Conditions)
------------------------------------------------------------
- BLQQR UAMCC: 1 row per bene per quarter. 9 MCC condition columns
  (integer claim counts). Bene qualifies with 2+ condition groups.
- ``count_unplanned_adm`` is the numerator.
- QTLQR measure_volume is **person-years**; measure_score is
  unplanned admissions per 100 person-years.

All tests are ``@requires_data``-gated.
"""

from __future__ import annotations

import polars as pl
import pytest

from .conftest import requires_data, scan_silver

MCC_CONDITION_COLS = [
    "condition_ami", "condition_alz", "condition_afib",
    "condition_ckd", "condition_copd", "condition_depress",
    "condition_hf", "condition_stroke_tia", "condition_diab",
]


def _parse_quarter(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(
        pl.col("source_filename").str.extract(r"\.(Q\d)\.(PY\d{4})\.", 1).alias("quarter"),
        pl.col("source_filename").str.extract(r"\.(Q\d)\.(PY\d{4})\.", 2).alias("perf_year"),
    )


@pytest.fixture
def blqqr_dah():
    try:
        return _parse_quarter(scan_silver("blqqr_dah").collect())
    except Exception:
        pytest.skip("blqqr_dah not available")


@pytest.fixture
def blqqr_uamcc():
    try:
        return _parse_quarter(scan_silver("blqqr_uamcc").collect())
    except Exception:
        pytest.skip("blqqr_uamcc not available")


@pytest.fixture
def exclusions():
    try:
        return _parse_quarter(scan_silver("blqqr_exclusions").collect())
    except Exception:
        pytest.skip("blqqr_exclusions not available")


@pytest.fixture
def qtlqr():
    try:
        return scan_silver("quarterly_quality_report_claims_results").collect()
    except Exception:
        pytest.skip("quarterly_quality_report_claims_results not available")


# ---------------------------------------------------------------------------
# Q3: DAH denominator + score tie-out
# ---------------------------------------------------------------------------


@requires_data
class TestDahDenominatorTieOut:
    @pytest.mark.reconciliation
    def test_bene_count_matches_exclusions(self, blqqr_dah, exclusions):
        """BLQQR DAH unique benes == exclusions ct_benes_dah per quarter."""
        _dah_benes = (
            blqqr_dah.group_by("quarter", "perf_year")
            .agg(pl.col("bene_id").n_unique().alias("blqqr_benes"))
        )
        _excl = exclusions.select(
            "quarter", "perf_year",
            pl.col("ct_benes_dah").cast(pl.Int64, strict=False).alias("excl_benes"),
        )
        _joined = _dah_benes.join(_excl, on=["quarter", "perf_year"], how="inner")
        if _joined.height == 0:
            pytest.skip("No matching quarters")
        _bad = _joined.filter(pl.col("blqqr_benes") != pl.col("excl_benes"))
        assert _bad.height == 0, (
            f"{_bad.height} quarters where DAH bene count != exclusions:\n{_bad}"
        )

    @pytest.mark.reconciliation
    def test_blqqr_benes_exceed_qtlqr_person_years(self, blqqr_dah, qtlqr):
        """Bene count > person-years since not every bene contributes
        a full year of observation."""
        _qtlqr_dah = qtlqr.filter(pl.col("measure") == "DAH")
        _max_vol = _qtlqr_dah["measure_volume"].cast(pl.Float64, strict=False).max()
        _total_benes = blqqr_dah["bene_id"].n_unique()
        if _max_vol is None:
            pytest.skip("No DAH measure_volume")
        assert _total_benes >= _max_vol, (
            f"BLQQR DAH benes ({_total_benes}) < QTLQR person-years ({_max_vol})"
        )


@requires_data
class TestDahScoreTieOut:
    @pytest.mark.reconciliation
    def test_raw_dah_in_ballpark_of_qtlqr(self, blqqr_dah, qtlqr):
        """Raw DAH (survival - facility days) should be within 50 days
        of the risk-adjusted QTLQR score. Wide tolerance because CMS
        risk-adjusts for demographics and dual-eligibility."""
        _raw_dah = (
            blqqr_dah["survival_days"].cast(pl.Float64, strict=False)
            - blqqr_dah["observed_dah"].cast(pl.Float64, strict=False)
        ).mean()
        _qtlqr_dah = qtlqr.filter(pl.col("measure") == "DAH")
        _qtlqr_score = _qtlqr_dah["measure_score"].cast(pl.Float64, strict=False).mean()
        if _raw_dah is None or _qtlqr_score is None:
            pytest.skip("Missing DAH data")
        _diff = abs(_raw_dah - _qtlqr_score)
        assert _diff <= 50.0, (
            f"Raw DAH ({_raw_dah:.1f}) vs QTLQR score ({_qtlqr_score:.1f}) "
            f"differ by {_diff:.1f} days (threshold 50)"
        )

    @pytest.mark.reconciliation
    def test_raw_dah_between_250_and_365(self, blqqr_dah):
        """Raw mean DAH should be 250–365 days/year. Below 250 means
        benes spend >100 days/yr in facilities; above 365 is impossible."""
        _raw_dah = (
            blqqr_dah["survival_days"].cast(pl.Float64, strict=False)
            - blqqr_dah["observed_dah"].cast(pl.Float64, strict=False)
        ).mean()
        assert 250.0 <= _raw_dah <= 365.0, (
            f"Raw mean DAH {_raw_dah:.1f} outside [250, 365]"
        )

    @pytest.mark.reconciliation
    def test_survival_days_reasonable(self, blqqr_dah):
        """Survival days should be 0–366 (one year max)."""
        _vals = blqqr_dah["survival_days"].cast(pl.Float64, strict=False).drop_nulls()
        assert _vals.min() >= 0.0, f"survival_days min {_vals.min()} < 0"
        assert _vals.max() <= 366.0, f"survival_days max {_vals.max()} > 366"


# ---------------------------------------------------------------------------
# Q4: UAMCC denominator + MCC cohort tie-out
# ---------------------------------------------------------------------------


@requires_data
class TestUamccDenominatorTieOut:
    @pytest.mark.reconciliation
    def test_bene_count_matches_exclusions(self, blqqr_uamcc, exclusions):
        """BLQQR UAMCC unique benes == exclusions ct_benes_uamcc.

        Tolerates up to 1 mismatched quarter: the Q3 PY2024 exclusions
        file has known parse artifacts (decimal values in integer fields)
        that produce nonsensical counts."""
        _uamcc_benes = (
            blqqr_uamcc.group_by("quarter", "perf_year")
            .agg(pl.col("bene_id").n_unique().alias("blqqr_benes"))
        )
        _excl = exclusions.select(
            "quarter", "perf_year",
            pl.col("ct_benes_uamcc").cast(pl.Int64, strict=False).alias("excl_benes"),
        )
        _joined = _uamcc_benes.join(_excl, on=["quarter", "perf_year"], how="inner")
        if _joined.height == 0:
            pytest.skip("No matching quarters")
        _bad = _joined.filter(pl.col("blqqr_benes") != pl.col("excl_benes"))
        assert _bad.height <= 1, (
            f"{_bad.height} quarters where UAMCC bene count != exclusions "
            f"(threshold 1):\n{_bad}"
        )

    @pytest.mark.reconciliation
    def test_blqqr_benes_exceed_qtlqr_person_years(self, blqqr_uamcc, qtlqr):
        _qtlqr_uamcc = qtlqr.filter(pl.col("measure") == "UAMCC")
        _max_vol = _qtlqr_uamcc["measure_volume"].cast(pl.Float64, strict=False).max()
        _total_benes = blqqr_uamcc["bene_id"].n_unique()
        if _max_vol is None:
            pytest.skip("No UAMCC measure_volume")
        assert _total_benes >= _max_vol, (
            f"BLQQR UAMCC benes ({_total_benes}) < QTLQR person-years ({_max_vol})"
        )


@requires_data
class TestUamccMccCohort:
    @pytest.mark.reconciliation
    def test_all_benes_have_2_plus_conditions(self, blqqr_uamcc):
        """UAMCC denominator requires 2+ MCC condition groups with at
        least one qualifying claim. Every bene in BLQQR UAMCC should
        meet this criterion."""
        _present = [c for c in MCC_CONDITION_COLS if c in blqqr_uamcc.columns]
        if len(_present) < 2:
            pytest.skip("Not enough MCC condition columns")
        _condition_count = blqqr_uamcc.select(
            pl.sum_horizontal(
                [(pl.col(c).cast(pl.Int64, strict=False) > 0).cast(pl.Int64) for c in _present]
            ).alias("n_conditions")
        )
        _under2 = _condition_count.filter(pl.col("n_conditions") < 2)
        assert _under2.height == 0, (
            f"{_under2.height} benes with < 2 MCC condition groups "
            f"(UAMCC denominator requires 2+)"
        )

    @pytest.mark.reconciliation
    def test_count_unplanned_adm_non_negative(self, blqqr_uamcc):
        _vals = blqqr_uamcc["count_unplanned_adm"].cast(pl.Int64, strict=False).drop_nulls()
        _negs = _vals.filter(_vals < 0)
        assert _negs.len() == 0


@requires_data
class TestUamccDataQuality:
    @pytest.mark.reconciliation
    def test_flag_all_zero_unplanned_admissions(self, blqqr_uamcc):
        """Data quality finding: if count_unplanned_adm is 0 for every
        bene across all quarters, the numerator field may not be
        populated yet. Flag but don't fail — this is a known data
        evolution issue (PY2025 schema change)."""
        _total = blqqr_uamcc["count_unplanned_adm"].cast(pl.Int64, strict=False).sum()
        if _total == 0:
            import warnings
            warnings.warn(
                "count_unplanned_adm is 0 across all BLQQR UAMCC rows — "
                "the numerator field may not be populated in this data vintage",
                stacklevel=1,
            )
