# © 2025 HarmonyCares
# All rights reserved.

"""
1:1 reconciliation tie-out: CCLF-derived expenditure == MER-reported expenditure.

This is the end-to-end assertion that PR A (MER view) and PR B1/B2/B3
(CCLF → MER-taxonomy gold) produce matching dollar amounts for every
bucket. If our calcs diverge from CMS's reported MER by even a cent in
any (aco_id, program, performance_year, year_month, clm_type_cd) tuple,
this test fails loudly.

Fixture strategy
----------------
Earlier reconciliation PRs committed per-table synthetic fixtures from
``aco dev generate-mocks``. Those fixtures have realistic per-column
distributions but NO business-semantic coherence — the MER fixture and
the CCLF fixtures describe different benes, different months, different
ACOs, so you cannot reconcile them against each other.

This test builds hand-crafted **matched** input pairs: a tiny MER
fixture and a tiny matching set of CCLF/BAR/ALR fixtures that were
constructed from the same underlying story. Each scenario states
explicitly "bene X had $Y spend in month M for clm_type Z, aligned to
ACO A" and then builds BOTH sides of the reconciliation from that
single source of truth. When the transforms work correctly, the
left and right sides agree by construction.

A deliberate-mismatch scenario is also included to prove the assertion
actually fires when the two sides disagree — because a test that only
ever passes is a test that might be silently broken.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from acoharmony._transforms._financial_expenditure_by_cms_claim_type import (
    build_financial_expenditure_by_cms_claim_type,
)
from acoharmony._transforms._mer_reconciliation_view import (
    build_mer_reconciliation_view,
)

TOLERANCE = 0.01  # $0.01 per-bucket tolerance per our standing rule


# ---------------------------------------------------------------------------
# Frame builders — one function per source, with defaults that cover the
# columns the transforms read. All schemas match the real silver shapes.
# ---------------------------------------------------------------------------

_CCLF1_SCHEMA = {
    "bene_mbi_id": pl.Utf8,
    "clm_type_cd": pl.Utf8,
    "clm_from_dt": pl.Date,
    "clm_pmt_amt": pl.Float64,
    "clm_adjsmt_type_cd": pl.Utf8,
    "file_date": pl.Utf8,
}

_CCLF_LINE_SCHEMA = {
    "bene_mbi_id": pl.Utf8,
    "clm_type_cd": pl.Utf8,
    "clm_from_dt": pl.Date,
    "clm_line_cvrd_pd_amt": pl.Float64,
    "clm_adjsmt_type_cd": pl.Utf8,
    "file_date": pl.Utf8,
}

_BAR_SCHEMA = {
    "bene_mbi": pl.Utf8,
    "start_date": pl.Date,
    "end_date": pl.Date,
    "source_filename": pl.Utf8,
    "file_date": pl.Utf8,
}

_ALR_SCHEMA = {
    "bene_mbi": pl.Utf8,
    "source_filename": pl.Utf8,
    "file_date": pl.Utf8,
}

# MER sheets carry string columns for the dim values — match that.
_MEXPR_CLAIMS_SCHEMA = {
    "perf_yr": pl.Utf8,
    "clndr_yr": pl.Utf8,
    "clndr_mnth": pl.Utf8,
    "bnmrk": pl.Utf8,
    "align_type": pl.Utf8,
    "bnmrk_type": pl.Utf8,
    "aco_id": pl.Utf8,
    "clm_type_cd": pl.Utf8,
    "total_exp_amt_agg": pl.Float64,
    "file_date": pl.Utf8,
}

_MEXPR_ENROLL_SCHEMA = {
    "perf_yr": pl.Utf8,
    "clndr_yr": pl.Utf8,
    "clndr_mnth": pl.Utf8,
    "bnmrk": pl.Utf8,
    "align_type": pl.Utf8,
    "bnmrk_type": pl.Utf8,
    "aco_id": pl.Utf8,
    "elig_mnths": pl.Int64,
    "file_date": pl.Utf8,
}


def _cclf1(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(rows, schema=_CCLF1_SCHEMA)


def _cclf5(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(rows, schema=_CCLF_LINE_SCHEMA)


def _cclf6(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(rows, schema=_CCLF_LINE_SCHEMA)


def _bar(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(rows, schema=_BAR_SCHEMA)


def _alr(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(rows, schema=_ALR_SCHEMA)


def _mexpr_claims(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(rows, schema=_MEXPR_CLAIMS_SCHEMA)


def _mexpr_enroll(rows: list[dict]) -> pl.LazyFrame:
    return pl.LazyFrame(rows, schema=_MEXPR_ENROLL_SCHEMA)


# ---------------------------------------------------------------------------
# Scenario 1: a matched pair where MER and CCLF tie exactly
# ---------------------------------------------------------------------------

CUTOFF = "2024-06-30"


def _matched_scenario() -> dict:
    """One ACO, one PY, three benes, claims across a few months/buckets.

    Hand-computed expected values:
      - D0259 REACH PY2024, with 3 benes aligned since Jan 1, no churn
      - Month 202403: R1 has $1000 inpatient (60) + R2 has $500 inpatient (60)
                    → total $1500 spend, 3 member_months, $500 PBPM
      - Month 202404: R1 has $200 physician (71) via CCLF5
                    → total $200 spend, 3 member_months, $66.67 PBPM
      - Other month/bucket combos = 0 (not emitted in either side)
    """
    cclf1 = _cclf1(
        [
            {
                "bene_mbi_id": "R1",
                "clm_type_cd": "60",
                "clm_from_dt": date(2024, 3, 10),
                "clm_pmt_amt": 1000.0,
                "clm_adjsmt_type_cd": "0",
                "file_date": "2024-05-01",
            },
            {
                "bene_mbi_id": "R2",
                "clm_type_cd": "60",
                "clm_from_dt": date(2024, 3, 20),
                "clm_pmt_amt": 500.0,
                "clm_adjsmt_type_cd": "0",
                "file_date": "2024-05-01",
            },
        ]
    )
    cclf5 = _cclf5(
        [
            {
                "bene_mbi_id": "R1",
                "clm_type_cd": "71",
                "clm_from_dt": date(2024, 4, 12),
                "clm_line_cvrd_pd_amt": 200.0,
                "clm_adjsmt_type_cd": "0",
                "file_date": "2024-05-01",
            },
        ]
    )
    cclf6 = _cclf6([])
    bar = _bar(
        [
            {
                "bene_mbi": "R1",
                "start_date": date(2024, 1, 1),
                "end_date": None,
                "source_filename": "P.D0259.ALGC24.RP.D240501.T1111111.xlsx",
                "file_date": "2024-05-01",
            },
            {
                "bene_mbi": "R2",
                "start_date": date(2024, 1, 1),
                "end_date": None,
                "source_filename": "P.D0259.ALGC24.RP.D240501.T1111111.xlsx",
                "file_date": "2024-05-01",
            },
            {
                "bene_mbi": "R3",
                "start_date": date(2024, 1, 1),
                "end_date": None,
                "source_filename": "P.D0259.ALGC24.RP.D240501.T1111111.xlsx",
                "file_date": "2024-05-01",
            },
        ]
    )
    alr = _alr([])

    # MER side: same story, but expressed in the MER's aggregated grain.
    # One row per (aco, py, month, clm_type_cd, bnmrk, align_type, bnmrk_type).
    # We use bnmrk='AD', align_type='C', bnmrk_type='RATEBOOK' because those
    # match what the generated BAR data would emit for claims-based alignment.
    dim_defaults = {
        "perf_yr": "2024",
        "clndr_yr": "2024",
        "bnmrk": "AD",
        "align_type": "C",
        "bnmrk_type": "RATEBOOK",
        "aco_id": "D0259",
        "file_date": "2024-05-01",
    }
    mer_claims = _mexpr_claims(
        [
            # Month 202403: $1500 inpatient (60)
            {**dim_defaults, "clndr_mnth": "3", "clm_type_cd": "60",
             "total_exp_amt_agg": 1500.0},
            # Month 202404: $200 physician (71)
            {**dim_defaults, "clndr_mnth": "4", "clm_type_cd": "71",
             "total_exp_amt_agg": 200.0},
        ]
    )
    mer_enroll = _mexpr_enroll(
        [
            # 3 eligible members in every month from Jan–June
            {**{k: v for k, v in dim_defaults.items() if k != "file_date"},
             "clndr_mnth": str(m), "elig_mnths": 3, "file_date": "2024-05-01"}
            for m in range(1, 7)
        ]
    )

    return {
        "cclf1": cclf1,
        "cclf5": cclf5,
        "cclf6": cclf6,
        "bar": bar,
        "alr": alr,
        "mer_claims": mer_claims,
        "mer_enroll": mer_enroll,
    }


def _compute_tieout(
    scenario: dict, cutoff: str = CUTOFF
) -> pl.DataFrame:
    """Run both pipelines against a scenario and return the tie-out diff frame.

    Returns a DataFrame with one row per bucket, containing MER-side and
    CCLF-side amounts plus their absolute difference. The test asserts
    the max diff is below TOLERANCE.
    """
    cclf_side = (
        build_financial_expenditure_by_cms_claim_type(
            scenario["cclf1"],
            scenario["cclf5"],
            scenario["cclf6"],
            scenario["bar"],
            scenario["alr"],
            as_of_cutoff=cutoff,
        )
        .collect()
        .with_columns(
            pl.col("total_spend").cast(pl.Float64).alias("cclf_amount"),
        )
        .select(
            "aco_id",
            "program",
            "performance_year",
            "year_month",
            "clm_type_cd",
            "cclf_amount",
        )
    )

    mer_side = (
        build_mer_reconciliation_view(
            scenario["mer_claims"],
            scenario["mer_enroll"],
            as_of_delivery_date=cutoff,
        )
        .collect()
        .with_columns(
            # Convert MER string dim cols to integer grain matching CCLF side
            (pl.col("clndr_yr").cast(pl.Int32) * 100 +
             pl.col("clndr_mnth").cast(pl.Int32)).alias("year_month"),
            pl.col("perf_yr").cast(pl.Int32).alias("performance_year"),
            pl.col("net_expenditure").cast(pl.Float64).alias("mer_amount"),
        )
        .select(
            "aco_id",
            "program",
            "performance_year",
            "year_month",
            "clm_type_cd",
            "mer_amount",
        )
    )

    # Full outer join so we see left-only and right-only buckets as diffs.
    joined = mer_side.join(
        cclf_side,
        on=[
            "aco_id",
            "program",
            "performance_year",
            "year_month",
            "clm_type_cd",
        ],
        how="full",
        coalesce=True,
    ).with_columns(
        pl.col("mer_amount").fill_null(0.0),
        pl.col("cclf_amount").fill_null(0.0),
    ).with_columns(
        (pl.col("mer_amount") - pl.col("cclf_amount")).abs().alias("abs_diff")
    )

    return joined


class TestMatchedScenarioTiesOut:
    """A matched MER + CCLF scenario must reconcile within TOLERANCE."""

    @pytest.mark.reconciliation
    def test_no_bucket_exceeds_tolerance(self):
        scenario = _matched_scenario()
        diff = _compute_tieout(scenario)

        # Every bucket should reconcile exactly.
        out_of_tolerance = diff.filter(pl.col("abs_diff") > TOLERANCE)
        if out_of_tolerance.height > 0:
            pytest.fail(
                f"Reconciliation failed: {out_of_tolerance.height} buckets "
                f"exceed ${TOLERANCE} tolerance:\n{out_of_tolerance}"
            )

    @pytest.mark.reconciliation
    def test_every_mer_bucket_has_a_cclf_counterpart(self):
        """No MER-only buckets — every MER row must have a matching CCLF row."""
        scenario = _matched_scenario()
        diff = _compute_tieout(scenario)
        # A MER-only bucket would have cclf_amount=0 with mer_amount>0
        mer_only = diff.filter(
            (pl.col("mer_amount") > 0) & (pl.col("cclf_amount") == 0)
        )
        assert mer_only.height == 0, (
            f"{mer_only.height} MER buckets have no CCLF counterpart:\n{mer_only}"
        )

    @pytest.mark.reconciliation
    def test_every_cclf_bucket_has_a_mer_counterpart(self):
        """No CCLF-only buckets — every CCLF row must have a matching MER row."""
        scenario = _matched_scenario()
        diff = _compute_tieout(scenario)
        cclf_only = diff.filter(
            (pl.col("cclf_amount") > 0) & (pl.col("mer_amount") == 0)
        )
        assert cclf_only.height == 0, (
            f"{cclf_only.height} CCLF buckets have no MER counterpart:\n{cclf_only}"
        )


class TestDeliberateMismatchFailsLoudly:
    """A deliberately-broken scenario MUST fail the reconciliation assertion.

    This is the canary for test integrity: if the reconciliation ever
    silently passes when the two sides disagree, this test catches it.
    """

    @pytest.mark.reconciliation
    def test_inflated_mer_bucket_is_caught(self):
        """Inflate one MER bucket by $50 and confirm the diff flags it."""
        scenario = _matched_scenario()
        # Mutate: change the 202403 inpatient bucket from $1500 to $1550.
        inflated = [
            {
                "perf_yr": "2024", "clndr_yr": "2024", "clndr_mnth": "3",
                "bnmrk": "AD", "align_type": "C", "bnmrk_type": "RATEBOOK",
                "aco_id": "D0259", "clm_type_cd": "60",
                "total_exp_amt_agg": 1550.0,  # +$50
                "file_date": "2024-05-01",
            },
            {
                "perf_yr": "2024", "clndr_yr": "2024", "clndr_mnth": "4",
                "bnmrk": "AD", "align_type": "C", "bnmrk_type": "RATEBOOK",
                "aco_id": "D0259", "clm_type_cd": "71",
                "total_exp_amt_agg": 200.0,
                "file_date": "2024-05-01",
            },
        ]
        scenario["mer_claims"] = _mexpr_claims(inflated)

        diff = _compute_tieout(scenario)
        out_of_tolerance = diff.filter(pl.col("abs_diff") > TOLERANCE)
        assert out_of_tolerance.height >= 1, (
            "Deliberate $50 MER inflation was not caught — reconciliation "
            "assertion is not actually working"
        )
        # The offending bucket should be the March 2024 inpatient one
        bad = out_of_tolerance.filter(pl.col("year_month") == 202403).row(0, named=True)
        assert bad["clm_type_cd"] == "60"
        assert bad["abs_diff"] == pytest.approx(50.0)

    @pytest.mark.reconciliation
    def test_missing_cclf_bucket_is_caught(self):
        """A MER bucket that has no CCLF counterpart should show as a diff."""
        scenario = _matched_scenario()
        # Remove the CCLF5 physician claim entirely — MER still reports it.
        scenario["cclf5"] = _cclf5([])

        diff = _compute_tieout(scenario)
        out_of_tolerance = diff.filter(pl.col("abs_diff") > TOLERANCE)
        assert out_of_tolerance.height >= 1, (
            "Missing CCLF bucket (CCLF5 physician claim removed) was not caught "
            "— reconciliation assertion is not actually working"
        )
        # The offending bucket should be the April 2024 physician one
        bad = out_of_tolerance.filter(pl.col("year_month") == 202404).row(0, named=True)
        assert bad["clm_type_cd"] == "71"
        assert float(bad["mer_amount"]) == pytest.approx(200.0)
        assert float(bad["cclf_amount"]) == pytest.approx(0.0)


class TestPointInTimeIsolation:
    """The as_of cutoff must be respected by BOTH sides identically."""

    @pytest.mark.reconciliation
    def test_post_cutoff_rows_do_not_break_tieout(self):
        """Adding rows on both sides that are strictly after the cutoff must
        not change the tie-out result — they are filtered symmetrically.
        """
        scenario = _matched_scenario()
        # Append a post-cutoff CCLF row
        cclf1_rows = scenario["cclf1"].collect().to_dicts()
        cclf1_rows.append(
            {
                "bene_mbi_id": "R1",
                "clm_type_cd": "60",
                "clm_from_dt": date(2024, 7, 10),
                "clm_pmt_amt": 99999.0,
                "clm_adjsmt_type_cd": "0",
                "file_date": "2024-12-31",  # after cutoff 2024-06-30
            }
        )
        scenario["cclf1"] = _cclf1(cclf1_rows)

        # Append a post-cutoff MER row
        mer_rows = scenario["mer_claims"].collect().to_dicts()
        mer_rows.append(
            {
                "perf_yr": "2024", "clndr_yr": "2024", "clndr_mnth": "7",
                "bnmrk": "AD", "align_type": "C", "bnmrk_type": "RATEBOOK",
                "aco_id": "D0259", "clm_type_cd": "60",
                "total_exp_amt_agg": 99999.0,
                "file_date": "2024-12-31",  # after cutoff
            }
        )
        scenario["mer_claims"] = _mexpr_claims(mer_rows)

        diff = _compute_tieout(scenario)
        out_of_tolerance = diff.filter(pl.col("abs_diff") > TOLERANCE)
        # Both sides are filtered symmetrically → tie-out still clean
        assert out_of_tolerance.height == 0, (
            "Post-cutoff rows leaked into reconciliation — point-in-time "
            "filter is broken on one side:\n"
            f"{out_of_tolerance}"
        )
