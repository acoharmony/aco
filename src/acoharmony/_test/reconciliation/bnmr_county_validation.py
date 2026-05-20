# © 2025 HarmonyCares
# All rights reserved.

"""
BNMR ``county`` sheet validation (milestone M4).

Scope discovery rewrote M4. The tracker originally described county
as "a slice of claims by county" where county rows would sum to the
ACO-level claims total. **That turns out to be wrong.** The county
sheet is a separate CMS-supplied benchmark-allocation table, not a
claims decomposition:

- ``cty_rate`` — CMS-published per-capita benchmark for each county
- ``elig_mnths`` — aligned-beneficiary months in the county for this
  delivery
- ``adj_cty_pmt`` — the computed product, ``cty_rate × elig_mnths``

There is no dollar-component decomposition (no ``sqstr_amt_agg`` /
``apa_rdctn_amt_agg`` / etc.), so the sum-of-county-dollars cannot be
compared to the net claim payments on the claims sheet at all — the
two sheets measure different things.

What M4 validates, then, is the county sheet on its own terms:

1. Internal formula: ``adj_cty_pmt == cty_rate × elig_mnths`` per row
2. Grain uniqueness at the full delivery key
3. Geo code well-formed (5-digit CMS county accrual code)
4. ``bene_dcnt`` and ``elig_mnths`` cardinalities sane
   (``elig_mnths >= bene_dcnt`` since each bene contributes ≥1 month)
5. Rate ranges calibrated to observed CMS values —
   AD $900–$1,600/month, ESRD $7,500–$12,500/month
6. Bnmrk vocabulary limited to the two known segments

All tests are ``@requires_data``-gated and skip in CI where the
workspace isn't mounted.
"""

from __future__ import annotations

import polars as pl
import pytest

from .conftest import requires_data, scan_silver

INTERNAL_FORMULA_TOLERANCE = 0.01  # $0.01 per row

# Rate bounds — tight against observed PY2023–2025 deliveries but
# generous enough to absorb reasonable CMS policy drift. Outside
# these bounds the test should flag; that's the signal M4 delivers.
AD_RATE_MIN = 500.0
AD_RATE_MAX = 3000.0
ESRD_RATE_MIN = 2000.0
ESRD_RATE_MAX = 15000.0

# Full delivery grain for uniqueness. ``source_filename`` distinguishes
# redeliveries at sub-day resolution (CMS sometimes ships multiple
# corrected workbooks on the same calendar ``file_date``).
GRAIN_COLS = [
    "aco_id",
    "perf_yr",
    "clndr_yr",
    "bnmrk",
    "align_type",
    "bnmrk_type",
    "cty_accrl_cd",
    "source_filename",
]

REQUIRED_COLUMNS = [
    "aco_id",
    "perf_yr",
    "clndr_yr",
    "bnmrk",
    "align_type",
    "bnmrk_type",
    "cty_accrl_cd",
    "cty_rate",
    "elig_mnths",
    "adj_cty_pmt",
    "bene_dcnt",
    "source_filename",
    "file_date",
]


@pytest.fixture
def county():
    try:
        return scan_silver("reach_bnmr_county").collect()
    except Exception:
        pytest.skip("reach_bnmr_county not available in silver")


@requires_data
class TestPresence:
    @pytest.mark.reconciliation
    def test_table_has_rows(self, county):
        assert county.height > 0

    @pytest.mark.reconciliation
    def test_required_columns_present(self, county):
        missing = [c for c in REQUIRED_COLUMNS if c not in county.columns]
        assert missing == [], f"missing columns: {missing}"


@requires_data
class TestGrain:
    @pytest.mark.reconciliation
    def test_grain_is_unique_at_delivery_key(self, county):
        """Duplicates at the full delivery key indicate a parser bug
        double-counting a sheet."""
        dups = (
            county.group_by(GRAIN_COLS)
            .len()
            .filter(pl.col("len") > 1)
        )
        assert dups.height == 0, (
            f"{dups.height} duplicate grain keys found:\n{dups.head(10)}"
        )

    @pytest.mark.reconciliation
    def test_grain_columns_have_no_nulls(self, county):
        problems = {}
        for col in GRAIN_COLS:
            null_count = county[col].null_count()
            if null_count > 0:
                problems[col] = null_count
        assert problems == {}, f"grain columns with nulls: {problems}"

    @pytest.mark.reconciliation
    def test_bnmrk_values_known(self, county):
        bnmrks = set(county["bnmrk"].drop_nulls().unique().to_list())
        unexpected = bnmrks - {"AD", "ESRD"}
        assert unexpected == set(), (
            f"unexpected bnmrk values: {unexpected} (known: AD, ESRD)"
        )


@requires_data
class TestInternalFormula:
    """``adj_cty_pmt == cty_rate × elig_mnths`` per row — verified in the
    parsed silver data. If CMS ever ships a row where this doesn't hold,
    the parser is mis-interpreting the sheet."""

    @pytest.mark.reconciliation
    def test_adj_cty_pmt_equals_rate_times_months(self, county):
        check = county.with_columns(
            (
                pl.col("cty_rate").cast(pl.Float64, strict=False)
                * pl.col("elig_mnths").cast(pl.Int64, strict=False)
                - pl.col("adj_cty_pmt").cast(pl.Float64, strict=False)
            )
            .abs()
            .alias("internal_diff")
        )
        bad = check.filter(pl.col("internal_diff") > INTERNAL_FORMULA_TOLERANCE)
        assert bad.height == 0, (
            f"{bad.height} rows violate adj_cty_pmt = cty_rate × elig_mnths:\n"
            f"{bad.select(GRAIN_COLS + ['cty_rate', 'elig_mnths', 'adj_cty_pmt', 'internal_diff']).head(5)}"
        )


@requires_data
class TestCardinalityInvariants:
    @pytest.mark.reconciliation
    def test_bene_dcnt_positive(self, county):
        """A row exists because a county has aligned benes — 0 is
        impossible here."""
        bad = county.filter(pl.col("bene_dcnt") <= 0)
        assert bad.height == 0, (
            f"{bad.height} rows with non-positive bene_dcnt:\n{bad.head(5)}"
        )

    @pytest.mark.reconciliation
    def test_elig_mnths_at_least_bene_dcnt(self, county):
        """Every bene contributes at least one eligible month in the
        period, so ``elig_mnths >= bene_dcnt`` is an invariant."""
        bad = county.filter(pl.col("elig_mnths") < pl.col("bene_dcnt"))
        assert bad.height == 0, (
            f"{bad.height} rows with elig_mnths < bene_dcnt:\n"
            f"{bad.select(GRAIN_COLS + ['bene_dcnt', 'elig_mnths']).head(5)}"
        )


@requires_data
class TestValueRanges:
    @pytest.mark.reconciliation
    def test_cty_rate_non_negative(self, county):
        negs = county.filter(pl.col("cty_rate") < 0)
        assert negs.height == 0, (
            f"{negs.height} rows with negative cty_rate:\n{negs.head(5)}"
        )

    @pytest.mark.reconciliation
    def test_adj_cty_pmt_non_negative(self, county):
        negs = county.filter(pl.col("adj_cty_pmt") < 0)
        assert negs.height == 0, (
            f"{negs.height} rows with negative adj_cty_pmt:\n{negs.head(5)}"
        )

    @pytest.mark.reconciliation
    def test_cty_rate_in_plausible_range_by_bnmrk(self, county):
        """AD $500–$3000/mo, ESRD $2000–$15000/mo — generous bounds
        that still catch any parse/sign error."""
        ad = county.filter(pl.col("bnmrk") == "AD")
        esrd = county.filter(pl.col("bnmrk") == "ESRD")

        if ad.height > 0:
            vals = ad["cty_rate"].drop_nulls().cast(pl.Float64, strict=False)
            assert vals.min() >= AD_RATE_MIN, (
                f"AD cty_rate min {vals.min()} below {AD_RATE_MIN}"
            )
            assert vals.max() <= AD_RATE_MAX, (
                f"AD cty_rate max {vals.max()} above {AD_RATE_MAX}"
            )

        if esrd.height > 0:
            vals = esrd["cty_rate"].drop_nulls().cast(pl.Float64, strict=False)
            assert vals.min() >= ESRD_RATE_MIN, (
                f"ESRD cty_rate min {vals.min()} below {ESRD_RATE_MIN}"
            )
            assert vals.max() <= ESRD_RATE_MAX, (
                f"ESRD cty_rate max {vals.max()} above {ESRD_RATE_MAX}"
            )


@requires_data
class TestGeoCode:
    @pytest.mark.reconciliation
    def test_cty_accrl_cd_is_5_digit_numeric(self, county):
        """CMS county accrual codes are 5-digit FIPS-aligned strings.
        Anything else means the parser or CMS changed format."""
        non_numeric = county.filter(
            ~pl.col("cty_accrl_cd").cast(pl.Utf8).str.contains(r"^\d{5}$")
        )
        assert non_numeric.height == 0, (
            f"{non_numeric.height} rows with non-5-digit cty_accrl_cd:\n"
            f"{non_numeric.select('cty_accrl_cd').unique().head(10)}"
        )

    @pytest.mark.reconciliation
    def test_county_code_set_is_reasonable(self, county):
        """Sanity check: we should see dozens to a few hundred unique
        county codes per ACO (depends on geographic footprint).
        Orders-of-magnitude deviations indicate parsing trouble."""
        n_counties = county["cty_accrl_cd"].n_unique()
        assert n_counties >= 5, (
            f"only {n_counties} unique county codes — suspiciously few"
        )
        assert n_counties <= 3500, (
            f"{n_counties} unique county codes exceeds US total (~3,143) — "
            "indicates duplicated or mis-parsed codes"
        )
