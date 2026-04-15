# © 2025 HarmonyCares
# All rights reserved.

"""
Ingestion-level validation of the BNMR USPCC sheet (milestone M3).

USPCC (US Per Capita Cost) is a CMS-published national benchmark —
there is no upstream pipeline data to reconstruct it from. M3 scope is
therefore narrow: prove the sheet parses cleanly, the grain is unique,
no nulls where nulls would break downstream joins, and numeric values
sit in plausible ranges for a monthly per-capita FFS benchmark.

All tests are ``@requires_data``-gated: they run against
``silver/reach_bnmr_uspcc.parquet`` and skip in CI where the workspace
isn't mounted.

Grain
-----
The authoritative grain in the silver table is
``(perf_yr, clndr_yr, bnmrk, aco_id, source_filename)``. Multiple
deliveries per performance year each re-ship the full historical
benchmark table, so grain at the narrower ``(perf_yr, clndr_yr, bnmrk)``
key is intentionally not unique — we only enforce uniqueness at the
full delivery key.

Value ranges
------------
Calibrated against observed data (PY2023–PY2025 deliveries):

- AD (Aged/Disabled): ~$770–$1,100/month
- ESRD: ~$6,000–$9,700/month

We widen those to generous bounds (AD $500–$3000, ESRD $2000–$15000)
so a modest CMS policy shift doesn't false-flag the tests; anything
outside those bounds is almost certainly a parse error.
"""

from __future__ import annotations

import polars as pl
import pytest

from .conftest import requires_data, scan_silver

# Value-range bounds. Generous by design — anything outside is a parse
# failure or a CMS schema shift worth flagging.
AD_USPCC_MIN = 500.0
AD_USPCC_MAX = 3000.0
ESRD_USPCC_MIN = 2000.0
ESRD_USPCC_MAX = 15000.0

# Reasonable year range for the historical USPCC series. Extended back
# to 2013 to cover any historical look-back, forward to 2030 for a
# few-years-out performance year.
MIN_PLAUSIBLE_YEAR = 2013
MAX_PLAUSIBLE_YEAR = 2030

REQUIRED_COLUMNS = [
    "perf_yr",
    "clndr_yr",
    "bnmrk",
    "uspcc",
    "ucc_hosp_adj",
    "adj_ffs_uspcc",
    "aco_id",
    "source_filename",
]

# Grain columns: the full delivery-level key.
GRAIN_COLS = ["perf_yr", "clndr_yr", "bnmrk", "aco_id", "source_filename"]


@requires_data
class TestUspccPresence:
    """Sheet is parsed and landed in silver."""

    @pytest.fixture
    def uspcc(self):
        try:
            return scan_silver("reach_bnmr_uspcc").collect()
        except Exception:
            pytest.skip("reach_bnmr_uspcc not available in silver")

    @pytest.mark.reconciliation
    def test_table_has_rows(self, uspcc):
        assert uspcc.height > 0, "reach_bnmr_uspcc has no rows"

    @pytest.mark.reconciliation
    def test_required_columns_present(self, uspcc):
        missing = [c for c in REQUIRED_COLUMNS if c not in uspcc.columns]
        assert missing == [], f"missing columns: {missing}"


@requires_data
class TestUspccGrain:
    """Grain integrity: keys non-null and unique at the full delivery key."""

    @pytest.fixture
    def uspcc(self):
        try:
            return scan_silver("reach_bnmr_uspcc").collect()
        except Exception:
            pytest.skip("reach_bnmr_uspcc not available in silver")

    @pytest.mark.reconciliation
    @pytest.mark.xfail(
        reason="Known parser bug: aco_id null in 3 PY2024 deliveries (issue #29)",
        strict=False,
    )
    def test_grain_columns_have_no_nulls(self, uspcc):
        """Any null in a grain column breaks downstream joins.

        Currently xfail due to issue #29: 66 rows from three PY2024
        deliveries have ``aco_id`` null even though the ACO ID is in the
        source filename. Flipping to xpass signals the parser has been
        fixed.
        """
        problems = {}
        for col in GRAIN_COLS:
            null_count = uspcc[col].null_count()
            if null_count > 0:
                problems[col] = null_count
        assert problems == {}, f"grain columns with nulls: {problems}"

    @pytest.mark.reconciliation
    def test_grain_is_unique_at_delivery_key(self, uspcc):
        """(perf_yr, clndr_yr, bnmrk, aco_id, source_filename) must be unique.
        Duplicates here indicate the parser double-counted a sheet."""
        dups = (
            uspcc.group_by(GRAIN_COLS)
            .len()
            .filter(pl.col("len") > 1)
        )
        assert dups.height == 0, (
            f"{dups.height} duplicate grain keys found:\n{dups.head(10)}"
        )

    @pytest.mark.reconciliation
    def test_bnmrk_values_known(self, uspcc):
        """Bnmrk must be AD or ESRD — any other value means CMS added a
        population segment and the schema assumptions need revisiting."""
        bnmrks = set(uspcc["bnmrk"].drop_nulls().unique().to_list())
        unexpected = bnmrks - {"AD", "ESRD"}
        assert unexpected == set(), (
            f"unexpected bnmrk values: {unexpected} "
            "(known values are AD, ESRD)"
        )


@requires_data
class TestUspccValueRanges:
    """Dollar amounts pass sanity checks for a monthly per-capita benchmark."""

    @pytest.fixture
    def uspcc(self):
        try:
            return scan_silver("reach_bnmr_uspcc").collect()
        except Exception:
            pytest.skip("reach_bnmr_uspcc not available in silver")

    @pytest.mark.reconciliation
    def test_uspcc_non_negative(self, uspcc):
        """uspcc is a gross per-capita cost — must be ≥ 0. (ucc_hosp_adj
        can legitimately be negative; see class docstring.)"""
        negs = uspcc.filter(pl.col("uspcc") < 0)
        assert negs.height == 0, (
            f"{negs.height} rows with negative uspcc:\n{negs.head(5)}"
        )

    @pytest.mark.reconciliation
    def test_adj_ffs_uspcc_non_negative(self, uspcc):
        """adj_ffs_uspcc = uspcc + ucc_hosp_adj; after adjustment the
        number is still a per-capita cost and must be ≥ 0."""
        negs = uspcc.filter(pl.col("adj_ffs_uspcc") < 0)
        assert negs.height == 0, (
            f"{negs.height} rows with negative adj_ffs_uspcc:\n{negs.head(5)}"
        )

    @pytest.mark.reconciliation
    def test_uspcc_in_plausible_range_by_bnmrk(self, uspcc):
        """AD values in $500–$3000, ESRD in $2000–$15000 per month."""
        ad = uspcc.filter(pl.col("bnmrk") == "AD")
        esrd = uspcc.filter(pl.col("bnmrk") == "ESRD")

        if ad.height > 0:
            ad_vals = ad["uspcc"].drop_nulls()
            assert ad_vals.min() >= AD_USPCC_MIN, (
                f"AD uspcc min {ad_vals.min()} below ${AD_USPCC_MIN}"
            )
            assert ad_vals.max() <= AD_USPCC_MAX, (
                f"AD uspcc max {ad_vals.max()} above ${AD_USPCC_MAX}"
            )

        if esrd.height > 0:
            esrd_vals = esrd["uspcc"].drop_nulls()
            assert esrd_vals.min() >= ESRD_USPCC_MIN, (
                f"ESRD uspcc min {esrd_vals.min()} below ${ESRD_USPCC_MIN}"
            )
            assert esrd_vals.max() <= ESRD_USPCC_MAX, (
                f"ESRD uspcc max {esrd_vals.max()} above ${ESRD_USPCC_MAX}"
            )

    @pytest.mark.reconciliation
    def test_adj_ffs_uspcc_equals_uspcc_plus_ucc_hosp_adj(self, uspcc):
        """Internal-consistency: adj_ffs_uspcc should equal
        uspcc + ucc_hosp_adj within rounding tolerance."""
        check = uspcc.with_columns(
            (
                pl.col("uspcc")
                + pl.col("ucc_hosp_adj")
                - pl.col("adj_ffs_uspcc")
            )
            .abs()
            .alias("internal_diff")
        )
        bad = check.filter(pl.col("internal_diff") > 0.01)
        assert bad.height == 0, (
            f"{bad.height} rows where adj_ffs_uspcc != uspcc + ucc_hosp_adj:\n"
            f"{bad.head(5)}"
        )


@requires_data
class TestUspccYearFields:
    """Year fields parse and sit in plausible bounds."""

    @pytest.fixture
    def uspcc(self):
        try:
            return scan_silver("reach_bnmr_uspcc").collect()
        except Exception:
            pytest.skip("reach_bnmr_uspcc not available in silver")

    @pytest.mark.reconciliation
    def test_perf_yr_parses_as_year(self, uspcc):
        try:
            years = uspcc["perf_yr"].cast(pl.Int32, strict=True)
        except Exception as e:
            pytest.fail(f"perf_yr does not parse as integer: {e}")
        out_of_range = years.filter(
            (years < MIN_PLAUSIBLE_YEAR) | (years > MAX_PLAUSIBLE_YEAR)
        )
        assert out_of_range.len() == 0, (
            f"perf_yr values outside [{MIN_PLAUSIBLE_YEAR}, "
            f"{MAX_PLAUSIBLE_YEAR}]: {out_of_range.to_list()}"
        )

    @pytest.mark.reconciliation
    def test_clndr_yr_parses_as_year(self, uspcc):
        try:
            years = uspcc["clndr_yr"].cast(pl.Int32, strict=True)
        except Exception as e:
            pytest.fail(f"clndr_yr does not parse as integer: {e}")
        out_of_range = years.filter(
            (years < MIN_PLAUSIBLE_YEAR) | (years > MAX_PLAUSIBLE_YEAR)
        )
        assert out_of_range.len() == 0, (
            f"clndr_yr values outside [{MIN_PLAUSIBLE_YEAR}, "
            f"{MAX_PLAUSIBLE_YEAR}]: {out_of_range.to_list()}"
        )

    @pytest.mark.reconciliation
    def test_clndr_yr_at_or_before_perf_yr(self, uspcc):
        """USPCC is a look-back benchmark — the historical calendar year
        used for computing the per-capita cost should not postdate the
        performance year it's being used to score."""
        check = uspcc.with_columns(
            pl.col("perf_yr").cast(pl.Int32).alias("_perf"),
            pl.col("clndr_yr").cast(pl.Int32).alias("_clndr"),
        )
        future = check.filter(pl.col("_clndr") > pl.col("_perf"))
        assert future.height == 0, (
            f"{future.height} rows have clndr_yr > perf_yr:\n"
            f"{future.select('perf_yr', 'clndr_yr', 'bnmrk').head(5)}"
        )
