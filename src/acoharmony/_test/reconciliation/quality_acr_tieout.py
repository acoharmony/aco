# © 2025 HarmonyCares
# All rights reserved.

"""
ACR (All-Cause Readmission) tie-out: BLQQR ↔ Exclusions ↔ QTLQR
(milestones Q1 + Q2).

Three-way reconciliation:

1. **BLQQR ACR** — per-beneficiary, per-index-admission rows. Each row
   is one hospital stay; ``radm30_flag`` marks whether an unplanned
   readmission occurred within 30 days. One bene can have multiple
   index stays.

2. **BLQQR Exclusions** — per-quarter ACO-level aggregate counts:
   ``ct_benes_acr`` (total eligible benes), ``ct_opting_out_acr``
   (opted out of quality measurement), ``ct_elig_prior_acr`` (not
   eligible in prior period). These are **bene counts**, not index stays.

3. **QTLQR Claims Results** — per-quarter summary for ACR:
   ``measure_volume`` (index stays, not benes), ``measure_score``
   (risk-adjusted readmission rate, %).

Tie-out relationships verified:

- Q1: ``count(distinct bene_id in BLQQR ACR)`` == ``ct_benes_acr`` on
  the matching-quarter Exclusions row. Tolerance: 0 (exact).

- Q2: ``sum(radm30_flag) / count(rows)`` on BLQQR ACR approximates
  the raw (unadjusted) readmission rate. The QTLQR ``measure_score``
  is **risk-adjusted**, so an exact match isn't expected — but they
  should be in the same ballpark (within 5 percentage points).

All tests are ``@requires_data``-gated.
"""

from __future__ import annotations

import polars as pl
import pytest

from .conftest import requires_data, scan_silver


def _parse_blqqr_quarter(df: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame:
    """Add quarter + perf_year columns parsed from BLQQR source_filename."""
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    return df.with_columns(
        pl.col("source_filename").str.extract(r"\.(Q\d)\.(PY\d{4})\.", 1).alias("quarter"),
        pl.col("source_filename").str.extract(r"\.(Q\d)\.(PY\d{4})\.", 2).alias("perf_year"),
    )


def _parse_qtlqr_quarter(df: pl.DataFrame) -> pl.DataFrame:
    """Add quarter column parsed from QTLQR source_filename."""
    return df.with_columns(
        pl.col("source_filename").str.extract(r"QTLQR\.(Q\d)\.", 1).alias("quarter"),
    )


@pytest.fixture
def blqqr_acr():
    try:
        return _parse_blqqr_quarter(scan_silver("blqqr_acr"))
    except Exception:
        pytest.skip("blqqr_acr not available")


@pytest.fixture
def exclusions():
    try:
        return _parse_blqqr_quarter(scan_silver("blqqr_exclusions"))
    except Exception:
        pytest.skip("blqqr_exclusions not available")


@pytest.fixture
def qtlqr_acr():
    try:
        _df = scan_silver("quarterly_quality_report_claims_results").collect()
        return _parse_qtlqr_quarter(_df.filter(pl.col("measure") == "ACR"))
    except Exception:
        pytest.skip("quarterly_quality_report_claims_results not available")


# ---------------------------------------------------------------------------
# Q1: BLQQR ACR bene count == Exclusions ct_benes_acr
# ---------------------------------------------------------------------------


@requires_data
class TestAcrDenominatorTieOut:
    """Q1: unique benes in BLQQR ACR == ct_benes_acr on the exclusions file,
    per quarter. Tolerance: 0 (exact integer match)."""

    @pytest.mark.reconciliation
    def test_unique_benes_match_exclusion_count(self, blqqr_acr, exclusions):
        _acr_benes = (
            blqqr_acr.group_by("quarter", "perf_year")
            .agg(pl.col("bene_id").n_unique().alias("blqqr_benes"))
        )
        _excl = exclusions.select(
            "quarter", "perf_year",
            pl.col("ct_benes_acr").cast(pl.Int64, strict=False).alias("excl_benes"),
        )
        _joined = _acr_benes.join(_excl, on=["quarter", "perf_year"], how="inner")
        if _joined.height == 0:
            pytest.skip("No matching quarters between BLQQR ACR and Exclusions")

        _mismatches = _joined.filter(pl.col("blqqr_benes") != pl.col("excl_benes"))
        assert _mismatches.height == 0, (
            f"{_mismatches.height} quarters where BLQQR ACR bene count != "
            f"Exclusions ct_benes_acr:\n{_mismatches}"
        )

    @pytest.mark.reconciliation
    def test_at_least_4_quarters_tie_out(self, blqqr_acr, exclusions):
        """We should have tie-out data for at least 4 quarters."""
        _acr_benes = (
            blqqr_acr.group_by("quarter", "perf_year")
            .agg(pl.col("bene_id").n_unique().alias("blqqr_benes"))
        )
        _excl = exclusions.select(
            "quarter", "perf_year",
            pl.col("ct_benes_acr").cast(pl.Int64, strict=False).alias("excl_benes"),
        )
        _joined = _acr_benes.join(_excl, on=["quarter", "perf_year"], how="inner")
        assert _joined.height >= 4, (
            f"Only {_joined.height} quarters with both BLQQR ACR and Exclusions "
            f"(expected at least 4)"
        )


# ---------------------------------------------------------------------------
# Q2: ACR readmission rate vs QTLQR measure_score
# ---------------------------------------------------------------------------


@requires_data
class TestAcrNumeratorTieOut:
    """Q2: raw readmission rate from BLQQR ACR vs risk-adjusted
    measure_score on QTLQR. Not an exact match (CMS risk-adjusts
    across specialty cohorts), but should be within 5 percentage points."""

    @pytest.mark.reconciliation
    def test_raw_rate_in_ballpark_of_qtlqr_score(self, blqqr_acr, qtlqr_acr):
        _acr_rates = (
            blqqr_acr.group_by("quarter", "perf_year")
            .agg(
                pl.len().alias("index_stays"),
                pl.col("radm30_flag").cast(pl.Int64, strict=False).sum().alias("readmissions"),
            )
            .with_columns(
                (pl.col("readmissions") / pl.col("index_stays") * 100).alias("raw_rate_pct")
            )
        )
        # Match QTLQR quarter to BLQQR quarter. QTLQR uses rolling
        # 12-month periods; the quarter extracted from the source_filename
        # (Q1/Q2/Q3/Q4) aligns the deliveries.
        _joined = _acr_rates.join(
            qtlqr_acr.select("quarter", pl.col("measure_score").cast(pl.Float64, strict=False).alias("qtlqr_score")),
            on="quarter",
            how="inner",
        )
        if _joined.height == 0:
            pytest.skip("No matching quarters between BLQQR ACR and QTLQR")

        _ballpark_violations = _joined.filter(
            (pl.col("raw_rate_pct") - pl.col("qtlqr_score")).abs() > 5.0
        )
        assert _ballpark_violations.height == 0, (
            f"{_ballpark_violations.height} quarters where raw ACR rate is "
            f">5pp away from QTLQR risk-adjusted score:\n"
            f"{_ballpark_violations.select('quarter', 'perf_year', 'raw_rate_pct', 'qtlqr_score')}"
        )

    @pytest.mark.reconciliation
    def test_readmission_count_positive(self, blqqr_acr):
        """Sanity: at least some readmissions should exist across all data."""
        _total = blqqr_acr["radm30_flag"].cast(pl.Int64, strict=False).sum()
        assert _total > 0, "Zero readmissions across all BLQQR ACR data — suspicious"

    @pytest.mark.reconciliation
    def test_raw_rate_between_10_and_30_percent(self, blqqr_acr):
        """National ACR rates run 15–20%. Our ACO's rate should be in
        a generous 10–30% band; outside that suggests a parse error."""
        _total_stays = blqqr_acr.height
        _readmissions = blqqr_acr["radm30_flag"].cast(pl.Int64, strict=False).sum()
        _rate = _readmissions / _total_stays * 100
        assert 10.0 <= _rate <= 30.0, (
            f"Overall raw ACR rate {_rate:.1f}% outside [10%, 30%] band"
        )


# ---------------------------------------------------------------------------
# Cross-check: exclusion arithmetic
# ---------------------------------------------------------------------------


@requires_data
class TestExclusionArithmetic:
    """The exclusion counts should be internally consistent: opted-out
    and prior-period-ineligible benes should be a small fraction of
    the total eligible bene pool."""

    @pytest.mark.reconciliation
    def test_opted_out_less_than_10_percent(self, exclusions):
        """Opt-out is rare — if >10% of benes opted out, something is wrong."""
        for r in exclusions.iter_rows(named=True):
            _total = int(float(r["ct_benes_acr"])) if r["ct_benes_acr"] else 0
            _opted = int(float(r["ct_opting_out_acr"])) if r["ct_opting_out_acr"] else 0
            if _total == 0:
                continue
            _pct = _opted / _total * 100
            assert _pct <= 10.0, (
                f"{r['quarter']} {r['perf_year']}: {_pct:.1f}% opted out "
                f"(threshold 10%)"
            )

    @pytest.mark.reconciliation
    def test_exclusion_counts_non_negative(self, exclusions):
        for c in ["ct_benes_acr", "ct_opting_out_acr", "ct_elig_prior_acr"]:
            _vals = exclusions[c].drop_nulls().cast(pl.Float64, strict=False).drop_nulls()
            _negs = _vals.filter(_vals < 0)
            assert _negs.len() == 0, f"{c} has negative values"


# ---------------------------------------------------------------------------
# DAH + UAMCC volume sanity (lightweight Q3/Q4 pre-checks)
# ---------------------------------------------------------------------------


@requires_data
class TestDahUamccVolumeSanity:
    """Lightweight checks that DAH and UAMCC BLQQR bene counts are
    in the right ballpark vs QTLQR measure_volume (person-years).
    Full tie-out is Q3/Q4; these are guardrails."""

    @pytest.mark.reconciliation
    def test_dah_blqqr_benes_gt_qtlqr_person_years(self):
        _dah = _parse_blqqr_quarter(scan_silver("blqqr_dah"))
        _qtlqr = scan_silver("quarterly_quality_report_claims_results").collect()
        _qtlqr_dah = _qtlqr.filter(pl.col("measure") == "DAH")

        _dah_benes = _dah["bene_id"].n_unique()
        _qtlqr_vol = _qtlqr_dah["measure_volume"].cast(pl.Float64, strict=False).max()
        if _qtlqr_vol is None:
            pytest.skip("No DAH measure_volume")
        assert _dah_benes >= _qtlqr_vol, (
            f"BLQQR DAH benes ({_dah_benes}) < QTLQR DAH person-years "
            f"({_qtlqr_vol}) — bene count should exceed person-years"
        )

    @pytest.mark.reconciliation
    def test_uamcc_blqqr_benes_gt_qtlqr_person_years(self):
        _uamcc = _parse_blqqr_quarter(scan_silver("blqqr_uamcc"))
        _qtlqr = scan_silver("quarterly_quality_report_claims_results").collect()
        _qtlqr_uamcc = _qtlqr.filter(pl.col("measure") == "UAMCC")

        _uamcc_benes = _uamcc["bene_id"].n_unique()
        _qtlqr_vol = _qtlqr_uamcc["measure_volume"].cast(pl.Float64, strict=False).max()
        if _qtlqr_vol is None:
            pytest.skip("No UAMCC measure_volume")
        assert _uamcc_benes >= _qtlqr_vol, (
            f"BLQQR UAMCC benes ({_uamcc_benes}) < QTLQR UAMCC person-years "
            f"({_qtlqr_vol}) — bene count should exceed person-years"
        )
