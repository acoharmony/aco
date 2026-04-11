# © 2025 HarmonyCares
# All rights reserved.

"""
1:1 reconciliation tie-out: CCLF-derived expenditure == BNMR-reported net claims.

The BNMR (Benchmark Report) ``reach_bnmr_claims`` sheet reports claim
aggregates per ``(aco_id, perf_yr, clndr_yr, clndr_mnth, bnmrk, align_type,
bnmrk_type, clm_type_cd)`` bucket, net of sequestration, APA reductions,
DSH/IME, and non-PBP reductions. Our CCLF-derived calc side
(``build_financial_expenditure_by_cms_claim_type``) must match those
amounts within $0.01 per bucket or the benchmark calculations downstream
are standing on broken inputs.

Because BNMR shares MER's claim-type taxonomy and dim schema, the
CCLF-side transform is the **same one** used in the MER tie-out — no
new calc-side transform was needed for BNMR. Only the left-hand side
of the join is different (PR D's BNMR view instead of PR A's MER view).

Fixture strategy
----------------
As in the MER tie-out, hand-crafted matched pairs rather than the
generate-mocks synthetic fixtures (which have no business-semantic
coherence). Each scenario constructs both sides of the reconciliation
from the same underlying story.

A deliberate-mismatch scenario proves the assertion actually fires.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from acoharmony._transforms._bnmr_reconciliation_view import (
    build_bnmr_reconciliation_view,
)
from acoharmony._transforms._financial_expenditure_by_cms_claim_type import (
    build_financial_expenditure_by_cms_claim_type,
)

TOLERANCE = 0.01


# ---------------------------------------------------------------------------
# Frame builders — same CCLF/BAR/ALR schemas as the MER tie-out, plus a
# BNMR claims builder with component reduction columns (no total_exp_amt_agg).
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

_BNMR_CLAIMS_SCHEMA = {
    "perf_yr": pl.Utf8,
    "clndr_yr": pl.Utf8,
    "clndr_mnth": pl.Utf8,
    "bnmrk": pl.Utf8,
    "align_type": pl.Utf8,
    "bnmrk_type": pl.Utf8,
    "aco_id": pl.Utf8,
    "clm_type_cd": pl.Utf8,
    "clm_pmt_amt_agg": pl.Float64,
    "sqstr_amt_agg": pl.Float64,
    "apa_rdctn_amt_agg": pl.Float64,
    "ucc_amt_agg": pl.Float64,
    "op_dsh_amt_agg": pl.Float64,
    "cp_dsh_amt_agg": pl.Float64,
    "op_ime_amt_agg": pl.Float64,
    "cp_ime_amt_agg": pl.Float64,
    "nonpbp_rdct_amt_agg": pl.Float64,
    "aco_amt_agg_apa": pl.Float64,
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


def _bnmr_claims(rows: list[dict]) -> pl.LazyFrame:
    """Build a BNMR claims frame. Defaults every reduction column to 0.0 so
    individual test rows only need to specify the fields that matter."""
    defaults = {
        "perf_yr": "2024",
        "clndr_yr": "2024",
        "clndr_mnth": "3",
        "bnmrk": "AD",
        "align_type": "C",
        "bnmrk_type": "RATEBOOK",
        "aco_id": "D0259",
        "clm_type_cd": "60",
        "clm_pmt_amt_agg": 0.0,
        "sqstr_amt_agg": 0.0,
        "apa_rdctn_amt_agg": 0.0,
        "ucc_amt_agg": 0.0,
        "op_dsh_amt_agg": 0.0,
        "cp_dsh_amt_agg": 0.0,
        "op_ime_amt_agg": 0.0,
        "cp_ime_amt_agg": 0.0,
        "nonpbp_rdct_amt_agg": 0.0,
        "aco_amt_agg_apa": 0.0,
        "file_date": "2024-12-31",
    }
    return pl.LazyFrame(
        [{**defaults, **r} for r in rows], schema=_BNMR_CLAIMS_SCHEMA
    )


# ---------------------------------------------------------------------------
# Scenario: single ACO, single PY, simple matched story.
# ---------------------------------------------------------------------------

CUTOFF = "2024-06-30"


def _matched_scenario() -> dict:
    """One REACH ACO, three benes aligned since Jan 1, claims across two
    months. All reductions are zero, so BNMR net = BNMR gross = CCLF total.

    Expected buckets:
      - 202403 / 60 / $1500 (R1 $1000 + R2 $500)
      - 202404 / 71 / $200  (R1 CCLF5)
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
            }
        ]
    )
    cclf6 = _cclf6([])
    bar = _bar(
        [
            {
                "bene_mbi": mbi,
                "start_date": date(2024, 1, 1),
                "end_date": None,
                "source_filename": "P.D0259.ALGC24.RP.D240501.T1111111.xlsx",
                "file_date": "2024-05-01",
            }
            for mbi in ("R1", "R2", "R3")
        ]
    )
    alr = _alr([])

    bnmr = _bnmr_claims(
        [
            {
                "clndr_mnth": "3",
                "clm_type_cd": "60",
                "clm_pmt_amt_agg": 1500.0,
                "file_date": "2024-05-01",
            },
            {
                "clndr_mnth": "4",
                "clm_type_cd": "71",
                "clm_pmt_amt_agg": 200.0,
                "file_date": "2024-05-01",
            },
        ]
    )

    return {
        "cclf1": cclf1,
        "cclf5": cclf5,
        "cclf6": cclf6,
        "bar": bar,
        "alr": alr,
        "bnmr": bnmr,
    }


def _compute_tieout(scenario: dict, cutoff: str = CUTOFF) -> pl.DataFrame:
    """Run both pipelines and return the tie-out diff frame.

    Returns rows keyed by (aco_id, program, performance_year, year_month,
    clm_type_cd) with bnmr_amount, cclf_amount, and abs_diff columns.
    Uses a full outer join so left-only and right-only buckets show up
    as diffs instead of being silently dropped.
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

    bnmr_side = (
        build_bnmr_reconciliation_view(
            scenario["bnmr"], as_of_delivery_date=cutoff
        )
        .collect()
        .with_columns(
            (
                pl.col("clndr_yr").cast(pl.Int32) * 100
                + pl.col("clndr_mnth").cast(pl.Int32)
            ).alias("year_month"),
            pl.col("perf_yr").cast(pl.Int32).alias("performance_year"),
            pl.col("net_expenditure").cast(pl.Float64).alias("bnmr_amount"),
        )
        .select(
            "aco_id",
            "program",
            "performance_year",
            "year_month",
            "clm_type_cd",
            "bnmr_amount",
        )
    )

    joined = bnmr_side.join(
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
        pl.col("bnmr_amount").fill_null(0.0),
        pl.col("cclf_amount").fill_null(0.0),
    ).with_columns(
        (pl.col("bnmr_amount") - pl.col("cclf_amount")).abs().alias("abs_diff")
    )
    return joined


class TestMatchedScenarioTiesOut:
    @pytest.mark.reconciliation
    def test_no_bucket_exceeds_tolerance(self):
        scenario = _matched_scenario()
        diff = _compute_tieout(scenario)
        out = diff.filter(pl.col("abs_diff") > TOLERANCE)
        if out.height > 0:
            pytest.fail(
                f"BNMR reconciliation failed: {out.height} buckets exceed "
                f"${TOLERANCE} tolerance:\n{out}"
            )

    @pytest.mark.reconciliation
    def test_every_bnmr_bucket_has_a_cclf_counterpart(self):
        scenario = _matched_scenario()
        diff = _compute_tieout(scenario)
        bnmr_only = diff.filter(
            (pl.col("bnmr_amount") > 0) & (pl.col("cclf_amount") == 0)
        )
        assert bnmr_only.height == 0, (
            f"{bnmr_only.height} BNMR buckets have no CCLF counterpart:\n{bnmr_only}"
        )

    @pytest.mark.reconciliation
    def test_every_cclf_bucket_has_a_bnmr_counterpart(self):
        scenario = _matched_scenario()
        diff = _compute_tieout(scenario)
        cclf_only = diff.filter(
            (pl.col("cclf_amount") > 0) & (pl.col("bnmr_amount") == 0)
        )
        assert cclf_only.height == 0, (
            f"{cclf_only.height} CCLF buckets have no BNMR counterpart:\n{cclf_only}"
        )


class TestBnmrNettingIsHonored:
    """BNMR's component-based net must flow through correctly.

    A BNMR row with gross $1500 and $100 in reductions has net $1400.
    The CCLF side must also show $1400 total_spend to tie out — so we
    adjust the CCLF scenario to match the BNMR's net (as if the gross
    claims included $100 of sequestration/DSH/etc. that CMS netted).
    """

    @pytest.mark.reconciliation
    def test_reductions_applied_net_matches_cclf(self):
        scenario = _matched_scenario()
        # Rewrite the BNMR 202403 / 60 row to have gross $1600 with $100
        # in reductions → net $1500, matching CCLF side.
        scenario["bnmr"] = _bnmr_claims(
            [
                {
                    "clndr_mnth": "3",
                    "clm_type_cd": "60",
                    "clm_pmt_amt_agg": 1600.0,
                    "sqstr_amt_agg": 30.0,
                    "apa_rdctn_amt_agg": 30.0,
                    "ucc_amt_agg": 10.0,
                    "op_dsh_amt_agg": 5.0,
                    "cp_dsh_amt_agg": 5.0,
                    "op_ime_amt_agg": 10.0,
                    "cp_ime_amt_agg": 10.0,
                    # 1600 - 30 - 30 - 10 - 5 - 5 - 10 - 10 = 1500 ✓
                    "file_date": "2024-05-01",
                },
                {
                    "clndr_mnth": "4",
                    "clm_type_cd": "71",
                    "clm_pmt_amt_agg": 200.0,
                    "file_date": "2024-05-01",
                },
            ]
        )
        diff = _compute_tieout(scenario)
        out = diff.filter(pl.col("abs_diff") > TOLERANCE)
        if out.height > 0:
            pytest.fail(
                f"BNMR component netting broken — {out.height} buckets off:\n{out}"
            )


class TestDeliberateMismatchFailsLoudly:
    @pytest.mark.reconciliation
    def test_inflated_bnmr_bucket_is_caught(self):
        """Inflate a BNMR bucket by $75 and confirm the assertion fires."""
        scenario = _matched_scenario()
        scenario["bnmr"] = _bnmr_claims(
            [
                {
                    "clndr_mnth": "3",
                    "clm_type_cd": "60",
                    "clm_pmt_amt_agg": 1575.0,  # +$75
                    "file_date": "2024-05-01",
                },
                {
                    "clndr_mnth": "4",
                    "clm_type_cd": "71",
                    "clm_pmt_amt_agg": 200.0,
                    "file_date": "2024-05-01",
                },
            ]
        )
        diff = _compute_tieout(scenario)
        out = diff.filter(pl.col("abs_diff") > TOLERANCE)
        assert out.height >= 1, (
            "Deliberate $75 BNMR inflation was not caught — the reconciliation "
            "assertion is not actually working"
        )
        bad = out.filter(pl.col("year_month") == 202403).row(0, named=True)
        assert bad["clm_type_cd"] == "60"
        assert bad["abs_diff"] == pytest.approx(75.0)

    @pytest.mark.reconciliation
    def test_missing_bnmr_bucket_is_caught(self):
        """If BNMR drops a bucket CCLF has, the diff must flag it."""
        scenario = _matched_scenario()
        # BNMR only has the Mar inpatient row — Apr physician is missing
        scenario["bnmr"] = _bnmr_claims(
            [
                {
                    "clndr_mnth": "3",
                    "clm_type_cd": "60",
                    "clm_pmt_amt_agg": 1500.0,
                    "file_date": "2024-05-01",
                },
            ]
        )
        diff = _compute_tieout(scenario)
        out = diff.filter(pl.col("abs_diff") > TOLERANCE)
        assert out.height >= 1, (
            "Missing BNMR bucket was not caught by the reconciliation"
        )
        bad = out.filter(pl.col("year_month") == 202404).row(0, named=True)
        assert bad["clm_type_cd"] == "71"
        assert float(bad["bnmr_amount"]) == pytest.approx(0.0)
        assert float(bad["cclf_amount"]) == pytest.approx(200.0)


class TestPointInTimeIsolation:
    @pytest.mark.reconciliation
    def test_post_cutoff_rows_do_not_break_tieout(self):
        """Post-cutoff rows on both sides must be dropped symmetrically."""
        scenario = _matched_scenario()
        # Add a post-cutoff CCLF row
        cclf_rows = scenario["cclf1"].collect().to_dicts()
        cclf_rows.append(
            {
                "bene_mbi_id": "R1",
                "clm_type_cd": "60",
                "clm_from_dt": date(2024, 7, 10),
                "clm_pmt_amt": 99999.0,
                "clm_adjsmt_type_cd": "0",
                "file_date": "2024-12-31",  # after cutoff
            }
        )
        scenario["cclf1"] = _cclf1(cclf_rows)
        # Add a post-cutoff BNMR row
        bnmr_rows = scenario["bnmr"].collect().to_dicts()
        bnmr_rows.append(
            {
                "perf_yr": "2024",
                "clndr_yr": "2024",
                "clndr_mnth": "7",
                "bnmrk": "AD",
                "align_type": "C",
                "bnmrk_type": "RATEBOOK",
                "aco_id": "D0259",
                "clm_type_cd": "60",
                "clm_pmt_amt_agg": 99999.0,
                "sqstr_amt_agg": 0.0,
                "apa_rdctn_amt_agg": 0.0,
                "ucc_amt_agg": 0.0,
                "op_dsh_amt_agg": 0.0,
                "cp_dsh_amt_agg": 0.0,
                "op_ime_amt_agg": 0.0,
                "cp_ime_amt_agg": 0.0,
                "nonpbp_rdct_amt_agg": 0.0,
                "aco_amt_agg_apa": 0.0,
                "file_date": "2024-12-31",  # after cutoff
            }
        )
        scenario["bnmr"] = _bnmr_claims(bnmr_rows)

        diff = _compute_tieout(scenario)
        out = diff.filter(pl.col("abs_diff") > TOLERANCE)
        assert out.height == 0, (
            "Post-cutoff rows leaked into BNMR reconciliation — point-in-time "
            f"filter is broken on one side:\n{out}"
        )
