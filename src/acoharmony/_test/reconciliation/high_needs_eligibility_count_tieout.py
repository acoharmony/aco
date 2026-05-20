# © 2025 HarmonyCares
# All rights reserved.

"""
High-Needs eligibility **recall** tie-out vs. the BAR.

What this test does
-------------------

For each performance year present in the BAR, we take the *latest*
``P.<aco>.ALG[CR]<yy>.RP.*`` file for that PY and collect the set of
benes CMS flagged as High-Needs (any of ``mobility_impairment_flag``,
``frailty_flag``, ``medium_risk_unplanned_flag``, ``high_risk_flag`` =
``'Y'``). We then assert that **≥ 99%** of those benes appear as
eligible (``first_ever_eligible_check_date IS NOT NULL``, i.e. met ≥ 1
criterion at *any* check date in history) in our
``gold/high_needs_eligibility`` output.

MBI rotation handling
---------------------

BAR carries each bene under the MBI in CMS's roster at delivery time.
Our ``gold/high_needs_eligibility`` keys benes on ``person_id``, the
canonical MBI assigned by ``current_mbi_lookup_lazy`` from
``_transforms/_identity_timeline``. When CMS rotates a bene's MBI, BAR
shows the old MBI for some snapshots and the new one for others, but
all our gold rows live under that canonical. A naive ``BAR.bene_mbi
== gold.mbi`` join therefore mis-counts every rotated bene as a
recall miss even though we correctly identified them as eligible
under their canonical MBI.

We resolve this by joining BAR through the **same** lookup function
the production transform uses, so the test always asks gold for the
exact MBI the dedup layer wrote under. (An earlier version of this
test re-derived canonicals from ``gold/identity_timeline.hop_index ==
0`` and got it wrong on a small set of chains where multiple MBIs
share hop_index=0; reusing the production function eliminates the
divergence.) This is responsible for ~5-10 percentage points of the
otherwise-apparent recall gap.

Newly-aligned benes with no CCLF data
-------------------------------------

CMS aligns benes to the ACO via the alignment file (BAR) before the
first CCLF batch with their claims/demographics arrives. The current
BAR delivery typically runs ~6 days ahead of the latest CCLF, and
benes flagged ``newly_aligned_flag = Y`` whose first BAR appearance
is the most recent file frequently have **zero** rows in any CCLF
table — we cannot evaluate eligibility for them, so they are
out-of-scope for a recall measurement. We exclude them from the
denominator. Once a CCLF batch arrives carrying their data, they
naturally re-enter scope.

Why recall, not count-equality
------------------------------

CMS's BAR is restricted to ACO-aligned benes on a specific issuance
date. Our eligibility transform evaluates the full beneficiary
universe (everyone with claims and eligibility) across every quarterly
check date in history. It is legitimate — and expected — that our
"eligible at some point" count exceeds BAR's count:

    - BAR drops benes who were never aligned (we keep them).
    - BAR captures a snapshot; we accumulate via sticky alignment
      (``eligible_sticky_across_pys`` is never cleared once set).
    - BAR may omit benes whose claim feed reaches us after the BAR
      cutoff.

What would be a bug is the reverse: a bene CMS flags as High-Needs on
the BAR (so CMS observed qualifying claims in their feed) whom our
algorithm *never* finds eligible. That's a missed criterion — the
class of defect this test is designed to surface.

Threshold
---------

99% recall per PY. We'll ratchet toward 100% as the remaining
mismatches get attributed and fixed. Failures print the bene count we
miss plus a 20-row sample so the delta is investigable without
re-running the query.

Data scope
----------

Reads real parquets from ``/opt/s3/data/workspace/{silver,gold}``;
skipped automatically when that workspace is unavailable (e.g. CI
build without S3 mount). No fixtures — this test is specifically about
production data semantics, not the expression arithmetic (which
``_test/expressions/`` already covers).
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl
import pytest

from .conftest import SILVER, requires_data, scan_gold, scan_silver

# Minimum fraction of BAR-flagged benes (per PY) that must appear as
# "ever eligible" in our high_needs_eligibility gold. Start at 0.99 and
# ratchet toward 1.0 as we close the remaining gaps.
RECALL_THRESHOLD = 0.99

# Sample size for the mismatch preview on assertion failure. Large
# enough to catch patterns (e.g. all missing benes share the same
# criterion flag), small enough to fit in a CI log.
MISMATCH_SAMPLE_SIZE = 20

# BAR filename pattern captures: ``P.<aco>.ALGC26.RP.D260421.T...`` →
# ALG[CR] followed by 2-digit PY. ALGC = cohort, ALGR = runout; both
# count as BAR deliveries for the same PY.
_BAR_PY_EXTRACT_PATTERN = r"ALG[CR](\d{2})"


@dataclass(frozen=True)
class PyRecallResult:
    performance_year: int
    n_bar_flagged: int
    n_found_eligible: int
    n_missed: int
    recall: float
    # A small DataFrame of missed benes (up to MISMATCH_SAMPLE_SIZE)
    # along with the BAR flags they carried, so failure output shows
    # *which* criterion CMS credited that we didn't.
    missed_sample: pl.DataFrame


def _latest_bar_per_py_lazy() -> pl.LazyFrame:
    """Lazy form of :func:`_latest_bar_per_py`."""
    bar = scan_silver("bar").with_columns(
        pl.col("source_filename")
        .str.extract(_BAR_PY_EXTRACT_PATTERN, 1)
        .cast(pl.Int64)
        .add(2000)
        .alias("performance_year")
    ).filter(pl.col("performance_year").is_not_null())

    latest_per_py = bar.group_by("performance_year").agg(
        pl.col("file_date").max().alias("_latest_file_date")
    )
    return bar.join(
        latest_per_py,
        left_on=["performance_year", "file_date"],
        right_on=["performance_year", "_latest_file_date"],
    )


def _latest_bar_per_py() -> pl.DataFrame:
    """Collect the BAR frame, tagged with ``performance_year``, filtered
    to the latest ``file_date`` per PY.

    PY is parsed from the ALGC/ALGR source filename rather than
    ``start_date``/``end_date``: the silver BAR parquet stores those as
    nulls for most rows, but the filename PY suffix is always present.
    """
    return _latest_bar_per_py_lazy().collect()


def _canonical_mbi_lookup() -> pl.DataFrame:
    """Map every historical MBI to the canonical MBI the pipeline
    actually keys gold rows under, by reusing the production
    ``current_mbi_lookup_lazy`` function.

    Earlier versions of this lookup re-derived the canonical from
    ``gold/identity_timeline.hop_index == 0``, but a small number of
    chains contain multiple MBIs at hop_index=0 — picking the wrong one
    sends the test looking for benes under an MBI the pipeline never
    used. Using the production lookup function guarantees the test
    resolves BAR's ``bene_mbi`` to the same canonical the dedup
    transform writes under.

    MBIs not in the lookup (no rotation history) pass through unchanged
    via ``coalesce`` at the call site.
    """
    from acoharmony._transforms._identity_timeline import (
        current_mbi_lookup_lazy,
    )

    return (
        current_mbi_lookup_lazy(SILVER)
        .rename({"prvs_num": "mbi", "crnt_num": "canonical_mbi"})
        .collect()
    )


def _benes_with_any_cclf_data() -> set[str]:
    """Set of canonical MBIs for which we have at least one row in any
    CCLF claims/demographics feed. Used to filter out newly-aligned-but-
    no-data-yet benes from the recall denominator."""
    sources = [
        ("cclf1", "bene_mbi_id"),
        ("cclf2", "bene_mbi_id"),
        ("cclf3", "bene_mbi_id"),
        ("cclf4", "bene_mbi_id"),
        ("cclf5", "bene_mbi_id"),
        ("cclf6", "bene_mbi_id"),
        ("cclf7", "bene_mbi_id"),
        ("cclf8", "bene_mbi_id"),
    ]
    seen: set[str] = set()
    for table, col in sources:
        seen.update(
            scan_silver(table)
            .select(pl.col(col).cast(pl.String, strict=False).alias("mbi"))
            .filter(pl.col("mbi").is_not_null())
            .unique()
            .collect()["mbi"]
            .to_list()
        )
    # Also resolve through CCLF9 so old MBIs whose chain has data anywhere
    # count too.
    canonical = _canonical_mbi_lookup()
    seen.update(
        canonical.filter(pl.col("mbi").is_in(list(seen)))["canonical_mbi"].to_list()
    )
    return seen


def _bar_high_needs_benes_lazy(
    bar_latest: pl.DataFrame | pl.LazyFrame,
) -> pl.LazyFrame:
    """Lazy form of :func:`_bar_high_needs_benes`.

    Same shape and semantics, but returns a ``LazyFrame`` so callers
    that compose it into a larger pipeline (e.g. the per-criterion
    recall test, which unpivots the four ``bar_<letter>`` flags and
    joins against ``ever_eligible``) can let the polars optimizer
    handle projection/predicate pushdown and avoid materializing the
    full BAR-bene table once per (PY, criterion) cell.

    Accepts either a ``DataFrame`` (legacy callers that already
    materialised the latest-BAR slice) or a ``LazyFrame`` (preferred —
    keeps the entire pipeline lazy from parquet through aggregation).
    """
    any_flag = (
        (pl.col("mobility_impairment_flag").fill_null("") == "Y")
        | (pl.col("frailty_flag").fill_null("") == "Y")
        | (pl.col("medium_risk_unplanned_flag").fill_null("") == "Y")
        | (pl.col("high_risk_flag").fill_null("") == "Y")
    )
    canonical = _canonical_mbi_lookup()
    benes_with_data = _benes_with_any_cclf_data()
    bar_lazy = (
        bar_latest if isinstance(bar_latest, pl.LazyFrame) else bar_latest.lazy()
    )
    return (
        bar_lazy
        .filter(any_flag)
        .join(canonical.lazy(), left_on="bene_mbi", right_on="mbi", how="left")
        .with_columns(
            pl.coalesce(["canonical_mbi", "bene_mbi"]).alias("resolved_mbi")
        )
        .group_by("performance_year", "resolved_mbi")
        .agg(
            # Keep the original BAR mbi for diagnostics — a recall miss
            # surfaces under the bene_mbi CMS issued, not the canonical
            # we resolved to.
            pl.col("bene_mbi").first().alias("bene_mbi"),
            (pl.col("newly_aligned_flag") == "Y").any().alias("newly_aligned"),
            (pl.col("mobility_impairment_flag") == "Y").any().alias("bar_a"),
            (pl.col("high_risk_flag") == "Y").any().alias("bar_b"),
            (pl.col("medium_risk_unplanned_flag") == "Y").any().alias("bar_c"),
            (pl.col("frailty_flag") == "Y").any().alias("bar_d"),
        )
        # Out-of-scope: newly aligned in latest BAR with no CCLF data yet.
        .filter(
            ~(
                pl.col("newly_aligned")
                & ~pl.col("resolved_mbi").is_in(list(benes_with_data))
            )
        )
        .drop("newly_aligned")
    )


def _bar_high_needs_benes(bar_latest: pl.DataFrame) -> pl.DataFrame:
    """From the latest-BAR frame, keep one row per (PY, resolved_mbi)
    with any HN flag set, plus the per-criterion flag values for
    mismatch diagnostics. ``resolved_mbi`` is BAR's ``bene_mbi`` rotated
    through ``identity_timeline`` to its canonical so the downstream
    join lines up with our gold ``mbi`` key.

    Drops benes flagged ``newly_aligned_flag = Y`` whose canonical MBI
    has no rows in any CCLF table — they are aligned in BAR but their
    claims have not yet been delivered, and we cannot compute
    eligibility without claims.
    """
    return _bar_high_needs_benes_lazy(bar_latest).collect()


def _ever_eligible_benes_lazy() -> pl.LazyFrame:
    """Lazy form of :func:`_ever_eligible_benes`."""
    return (
        scan_gold("high_needs_eligibility")
        .filter(pl.col("first_ever_eligible_check_date").is_not_null())
        .select("mbi")
        .unique()
    )


def _ever_eligible_benes() -> pl.DataFrame:
    """Distinct benes with ``first_ever_eligible_check_date IS NOT
    NULL`` — i.e. our algo flagged them eligible at some point in
    history (any PY, any check date).

    We pull the full sticky flag here rather than per-PY, because the
    BAR represents CMS's "have they ever met the bar" judgment; their
    claim feed has deeper history than the PY window a single BAR file
    is anchored to.
    """
    return _ever_eligible_benes_lazy().collect()


def _recall_for_py(
    py: int,
    bar_benes: pl.DataFrame,
    ever_eligible: pl.DataFrame,
) -> PyRecallResult:
    """Compute recall for one PY: fraction of BAR-flagged benes that
    our algo ever marked eligible. The join is on ``resolved_mbi`` (BAR's
    ``bene_mbi`` rotated through ``identity_timeline``) so MBI rotations
    don't show up as false-negative misses."""
    py_benes = bar_benes.filter(pl.col("performance_year") == py)
    n_bar = py_benes.height

    hits = py_benes.join(
        ever_eligible, left_on="resolved_mbi", right_on="mbi", how="inner"
    )
    n_found = hits.height
    missed = py_benes.join(
        ever_eligible, left_on="resolved_mbi", right_on="mbi", how="anti"
    )
    n_missed = missed.height
    recall = 1.0 if n_bar == 0 else n_found / n_bar

    return PyRecallResult(
        performance_year=py,
        n_bar_flagged=n_bar,
        n_found_eligible=n_found,
        n_missed=n_missed,
        recall=recall,
        missed_sample=missed.head(MISMATCH_SAMPLE_SIZE),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@requires_data
class TestHighNeedsEligibilityRecall:
    """Per-PY recall of our eligibility output against the latest BAR.

    One test per PY so a regression in PY2024 doesn't mask PY2026
    progress. The per-PY recall values are parametrised from the BAR
    itself at collect time, so whatever PYs are present in
    ``silver/bar`` get a test automatically.
    """

    @pytest.fixture(scope="class")
    def bar_high_needs(self) -> pl.DataFrame:
        return _bar_high_needs_benes(_latest_bar_per_py())

    @pytest.fixture(scope="class")
    def ever_eligible(self) -> pl.DataFrame:
        return _ever_eligible_benes()

    @pytest.fixture(scope="class")
    def performance_years(self, bar_high_needs) -> list[int]:
        pys = sorted(bar_high_needs["performance_year"].unique().to_list())
        if not pys:
            pytest.skip("No BAR performance years found")
        return pys

    @pytest.mark.reconciliation
    def test_recall_meets_threshold_per_py(
        self,
        bar_high_needs: pl.DataFrame,
        ever_eligible: pl.DataFrame,
        performance_years: list[int],
    ):
        """For each PY, ≥ RECALL_THRESHOLD of BAR-flagged HN benes are
        flagged eligible at some check date by our algorithm.

        Reports every PY's recall on failure (not just the first) so
        one run surfaces the full state. Missed-bene sample includes
        the BAR flag columns to help attribute misses to a specific
        criterion.
        """
        results = [
            _recall_for_py(py, bar_high_needs, ever_eligible)
            for py in performance_years
        ]

        lines = ["Per-PY recall (ours vs. latest BAR):"]
        for r in results:
            status = "OK " if r.recall >= RECALL_THRESHOLD else "FAIL"
            lines.append(
                f"  [{status}] PY{r.performance_year}: "
                f"{r.n_found_eligible:,}/{r.n_bar_flagged:,} = "
                f"{r.recall:.4f} (missed {r.n_missed:,})"
            )
        print("\n".join(lines))

        failures = [r for r in results if r.recall < RECALL_THRESHOLD]
        if failures:
            detail = ["Recall threshold violations:"]
            for r in failures:
                detail.append(
                    f"PY{r.performance_year}: recall={r.recall:.4f} "
                    f"(< {RECALL_THRESHOLD}), missed {r.n_missed:,} / "
                    f"{r.n_bar_flagged:,}. Sample of missed benes:"
                )
                detail.append(str(r.missed_sample))
            pytest.fail("\n".join(detail))
