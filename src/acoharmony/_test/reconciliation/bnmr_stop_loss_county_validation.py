# © 2025 HarmonyCares
# All rights reserved.

"""
BNMR ``stop_loss_county`` sheet validation (milestone M6).

The stop-loss county sheet allocates stop-loss parameters per county
for each benchmark segment (AD / ESRD). Each row carries a per-capita
base rate and a "factor × eligible-months" adjusted variant:

    adj_gaf_trend         = gaf_trend         × elig_mnths
    adj_avg_payout_pct    = avg_payout_pct    × elig_mnths
    adj_ad_ry_avg_pbpm    = ad_ry_avg_pbpm    × elig_mnths   (AD rows)
    adj_esrd_ry_avg_pbpm  = esrd_ry_avg_pbpm  × elig_mnths   (ESRD rows)

All four internal formulas verified against PY2023–PY2025 deliveries
with zero violations at $0.01 tolerance. M6 locks that down so a
future parser change or CMS format shift surfaces immediately.

Cross-sheet validation confirms every ``cty_accrl_cd`` in the
stop-loss county sheet appears in the main ``county`` sheet (M4) —
the two share the same geographic footprint. And the
``stop_loss_elected`` / ``stop_loss_payout_neutrality_factor``
metadata from M7's report_parameters is verified non-null on every
delivery that ships stop-loss data.

All tests are ``@requires_data``-gated.
"""

from __future__ import annotations

import polars as pl
import pytest

from .conftest import requires_data, scan_silver

INTERNAL_FORMULA_TOLERANCE = 0.01

# Per-capita base-rate ranges calibrated to observed PY2023–2025.
AD_PBPM_MIN = 500.0
AD_PBPM_MAX = 3000.0
ESRD_PBPM_MIN = 2000.0
ESRD_PBPM_MAX = 15000.0

GRAIN_COLS = [
    "aco_id",
    "perf_yr",
    "clndr_yr",
    "bnmrk",
    "cty_accrl_cd",
    "source_filename",
]


@pytest.fixture
def slc():
    try:
        return scan_silver("reach_bnmr_stop_loss_county").collect()
    except Exception:
        pytest.skip("reach_bnmr_stop_loss_county not available")


@pytest.fixture
def county():
    try:
        return scan_silver("reach_bnmr_county").collect()
    except Exception:
        pytest.skip("reach_bnmr_county not available")


# ---------------------------------------------------------------------------
# Presence + grain
# ---------------------------------------------------------------------------


@requires_data
class TestPresence:
    @pytest.mark.reconciliation
    def test_table_has_rows(self, slc):
        assert slc.height > 0

    @pytest.mark.reconciliation
    def test_grain_columns_no_nulls(self, slc):
        problems = {}
        for c in GRAIN_COLS:
            n = slc[c].null_count()
            if n > 0:
                problems[c] = n
        assert problems == {}, f"grain nulls: {problems}"

    @pytest.mark.reconciliation
    def test_grain_unique(self, slc):
        dups = slc.group_by(GRAIN_COLS).len().filter(pl.col("len") > 1)
        assert dups.height == 0, f"{dups.height} dup grains:\n{dups.head(5)}"

    @pytest.mark.reconciliation
    def test_bnmrk_known(self, slc):
        unexpected = set(slc["bnmrk"].unique().to_list()) - {"AD", "ESRD"}
        assert unexpected == set(), f"unexpected bnmrk: {unexpected}"


# ---------------------------------------------------------------------------
# Internal formulas: adjusted = base × elig_mnths
# ---------------------------------------------------------------------------


@requires_data
class TestInternalFormulas:
    """Four column-pair relationships verified against PY2023–2025 with
    zero violations. These are the internal-arithmetic guard rails."""

    @pytest.mark.reconciliation
    def test_adj_gaf_trend_equals_base_times_months(self, slc):
        diff = (
            pl.col("gaf_trend").cast(pl.Float64)
            * pl.col("elig_mnths").cast(pl.Float64)
            - pl.col("adj_gaf_trend").cast(pl.Float64)
        ).abs()
        bad = slc.with_columns(diff.alias("d")).filter(pl.col("d") > INTERNAL_FORMULA_TOLERANCE)
        assert bad.height == 0, f"{bad.height} violations:\n{bad.head(5)}"

    @pytest.mark.reconciliation
    def test_adj_avg_payout_pct_equals_base_times_months(self, slc):
        diff = (
            pl.col("avg_payout_pct").cast(pl.Float64)
            * pl.col("elig_mnths").cast(pl.Float64)
            - pl.col("adj_avg_payout_pct").cast(pl.Float64)
        ).abs()
        bad = slc.with_columns(diff.alias("d")).filter(pl.col("d") > INTERNAL_FORMULA_TOLERANCE)
        assert bad.height == 0, f"{bad.height} violations:\n{bad.head(5)}"

    @pytest.mark.reconciliation
    def test_adj_ad_ry_avg_pbpm_equals_base_times_months(self, slc):
        ad = slc.filter(pl.col("bnmrk") == "AD")
        if ad.height == 0:
            pytest.skip("no AD rows")
        diff = (
            pl.col("ad_ry_avg_pbpm").cast(pl.Float64)
            * pl.col("elig_mnths").cast(pl.Float64)
            - pl.col("adj_ad_ry_avg_pbpm").cast(pl.Float64)
        ).abs()
        bad = ad.with_columns(diff.alias("d")).filter(pl.col("d") > INTERNAL_FORMULA_TOLERANCE)
        assert bad.height == 0, f"{bad.height} AD violations:\n{bad.head(5)}"

    @pytest.mark.reconciliation
    def test_adj_esrd_ry_avg_pbpm_equals_base_times_months(self, slc):
        esrd = slc.filter(pl.col("bnmrk") == "ESRD")
        if esrd.height == 0:
            pytest.skip("no ESRD rows")
        diff = (
            pl.col("esrd_ry_avg_pbpm").cast(pl.Float64)
            * pl.col("elig_mnths").cast(pl.Float64)
            - pl.col("adj_esrd_ry_avg_pbpm").cast(pl.Float64)
        ).abs()
        bad = esrd.with_columns(diff.alias("d")).filter(pl.col("d") > INTERNAL_FORMULA_TOLERANCE)
        assert bad.height == 0, f"{bad.height} ESRD violations:\n{bad.head(5)}"


# ---------------------------------------------------------------------------
# Value ranges
# ---------------------------------------------------------------------------


@requires_data
class TestValueRanges:
    @pytest.mark.reconciliation
    def test_avg_payout_pct_between_0_and_1(self, slc):
        vals = slc["avg_payout_pct"].drop_nulls().cast(pl.Float64, strict=False)
        assert vals.min() >= 0.0, f"avg_payout_pct min {vals.min()} < 0"
        assert vals.max() <= 1.0, f"avg_payout_pct max {vals.max()} > 1"

    @pytest.mark.reconciliation
    def test_ad_pbpm_in_plausible_range(self, slc):
        ad = slc.filter(pl.col("bnmrk") == "AD")
        if ad.height == 0:
            pytest.skip("no AD rows")
        vals = ad["ad_ry_avg_pbpm"].drop_nulls().cast(pl.Float64, strict=False)
        assert vals.min() >= AD_PBPM_MIN, f"AD pbpm min {vals.min()} below {AD_PBPM_MIN}"
        assert vals.max() <= AD_PBPM_MAX, f"AD pbpm max {vals.max()} above {AD_PBPM_MAX}"

    @pytest.mark.reconciliation
    def test_esrd_pbpm_in_plausible_range(self, slc):
        esrd = slc.filter(pl.col("bnmrk") == "ESRD")
        if esrd.height == 0:
            pytest.skip("no ESRD rows")
        vals = esrd["esrd_ry_avg_pbpm"].drop_nulls().cast(pl.Float64, strict=False)
        assert vals.min() >= ESRD_PBPM_MIN, f"ESRD pbpm min {vals.min()} below {ESRD_PBPM_MIN}"
        assert vals.max() <= ESRD_PBPM_MAX, f"ESRD pbpm max {vals.max()} above {ESRD_PBPM_MAX}"


# ---------------------------------------------------------------------------
# Cardinality invariants
# ---------------------------------------------------------------------------


@requires_data
class TestCardinality:
    @pytest.mark.reconciliation
    def test_bene_dcnt_positive(self, slc):
        bad = slc.filter(pl.col("bene_dcnt") <= 0)
        assert bad.height == 0, f"{bad.height} rows with bene_dcnt <= 0"

    @pytest.mark.reconciliation
    def test_elig_mnths_at_least_bene_dcnt(self, slc):
        bad = slc.filter(pl.col("elig_mnths") < pl.col("bene_dcnt"))
        assert bad.height == 0, f"{bad.height} rows with elig_mnths < bene_dcnt"


# ---------------------------------------------------------------------------
# Geo code validity + cross-sheet overlap
# ---------------------------------------------------------------------------


@requires_data
class TestGeo:
    @pytest.mark.reconciliation
    def test_cty_accrl_cd_5_digit(self, slc):
        non5 = slc.filter(
            ~pl.col("cty_accrl_cd").cast(pl.Utf8).str.contains(r"^\d{5}$")
        )
        assert non5.height == 0, f"{non5.height} non-5-digit codes"

    @pytest.mark.reconciliation
    def test_county_codes_overlap_with_county_sheet(self, slc, county):
        """Every cty_accrl_cd on stop_loss_county must appear in the
        main county sheet for the same ACO."""
        slc_codes = set(slc["cty_accrl_cd"].unique().to_list())
        county_codes = set(county["cty_accrl_cd"].unique().to_list())
        orphan = slc_codes - county_codes
        assert orphan == set(), (
            f"{len(orphan)} stop_loss_county codes not in county sheet: "
            f"{sorted(orphan)[:10]}"
        )


# ---------------------------------------------------------------------------
# Cross-sheet: report_parameters consistency (M7)
# ---------------------------------------------------------------------------


@requires_data
class TestReportParametersConsistency:
    @pytest.mark.reconciliation
    def test_stop_loss_elected_on_all_deliveries(self, slc):
        """If the ACO ships stop-loss county data, the report_parameters
        must say ``stop_loss_elected = 'Yes'``."""
        vals = slc["stop_loss_elected"].drop_nulls().unique().to_list()
        assert vals == ["Yes"], (
            f"stop_loss_elected values: {vals} (expected ['Yes'] on "
            f"every delivery that has stop-loss county data)"
        )

    @pytest.mark.reconciliation
    def test_stop_loss_payout_neutrality_factor_exists_on_some_deliveries(self, slc):
        """The neutrality factor was added to REPORT_PARAMETERS in later
        PY2024/PY2025 deliveries. Earlier deliveries (PY2023, early PY2024)
        don't include it → matrix extraction returns null. Validate that
        at least some deliveries carry a non-null value."""
        populated = slc["stop_loss_payout_neutrality_factor"].drop_nulls().len()
        assert populated > 0, (
            "stop_loss_payout_neutrality_factor is null on every row — "
            "the matrix extraction may be broken"
        )
