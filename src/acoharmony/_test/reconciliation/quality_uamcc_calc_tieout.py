# © 2025 HarmonyCares
# All rights reserved.

"""UAMCC calc-vs-CMS tie-out: our computed admission counts vs BLQQR.

Where existing UAMCC reconciliation tests cross-check CMS reports against
each other (BLQQR ↔ Exclusions ↔ QTLQR), this test runs **our** UAMCC
calculation against silver claims and compares per-beneficiary
``count_unplanned_adm`` to what CMS reports in BLQQR-UAMCC. The only
known way to validate that the calculation logic itself is correct.

For each performance year we have BLQQR data for, the test:
  1. Pulls the latest available quarter snapshot (most-up-to-date counts).
  2. Maps BLQQR ``mbi`` → silver ``person_id`` via eligibility.
  3. Runs ``AllCauseUnplannedAdmissions.calculate(...)`` against silver
     claims/eligibility and Tuva/UAMCC value sets.
  4. Per beneficiary: compares CMS ``count_unplanned_adm`` to ours.

The test reports agreement rate (both = 0, both > 0 with same count,
both > 0 with different count, or one-sided). Tolerances are deliberately
wide — exact tie-out is unrealistic given:
  - We may run on a slightly different claim cutoff than the CMS report.
  - PAA classification is an algorithmic approximation (CMS uses the
    same algorithm but on their reference claim history).
  - MBI ↔ person_id mapping is imperfect (~88% covered).

If a year's recall vs CMS drops below ``MIN_RECALL_PER_YEAR`` or the
admission-count delta exceeds ``MAX_TOTAL_DELTA_PCT``, the test fails
with diagnostics naming the worst-mismatched beneficiaries.
"""

from __future__ import annotations

import polars as pl
import pytest

from acoharmony._expressions._uamcc import UamccExpression
from acoharmony._transforms._quality_uamcc import AllCauseUnplannedAdmissions

from .conftest import SILVER, requires_data, scan_gold, scan_silver

# Per-PY recall threshold: % of CMS BLQQR benes that map to one of our
# person_ids AND end up in our calculated denominator. Below this, either
# our cohort identification is missing benes or our MBI mapping is broken.
MIN_RECALL_PER_YEAR = 0.70

# Max % difference between CMS-reported total unplanned admissions and
# our calculated total. Across all benes, summed.
MAX_TOTAL_DELTA_PCT = 0.30

# Per-bene exact-match rate (both 0, or both same positive count).
MIN_EXACT_MATCH_RATE = 0.60


def _latest_quarter_for_py(blqqr: pl.DataFrame, perf_year: str) -> pl.DataFrame:
    """Pick the most recent quarter's snapshot for a given PY string (e.g. PY2024)."""
    py_rows = blqqr.filter(
        pl.col("source_filename").str.contains(rf"\.{perf_year}\.")
    )
    if py_rows.is_empty():
        return py_rows
    quarters = (
        py_rows.select(
            pl.col("source_filename")
            .str.extract(r"\.(Q\d)\.", 1)
            .alias("q")
        )
        .filter(pl.col("q").is_not_null())
        ["q"]
        .unique()
        .sort()
        .to_list()
    )
    if not quarters:
        return py_rows
    latest = quarters[-1]
    return py_rows.filter(pl.col("source_filename").str.contains(rf"\.{latest}\."))


def _mbi_to_person_id() -> pl.DataFrame:
    """In gold, person_id is the MBI itself. Provide a unique per-MBI
    set of person_ids for the join — kept as a function so a future
    crosswalk (e.g. via identity_timeline) is a single edit away."""
    elig = scan_gold("eligibility").select(
        pl.col("person_id").alias("mbi"),
        "person_id",
    )
    return elig.collect().unique(subset="mbi")


def _run_our_calc(performance_year: int, aco_id: str) -> pl.DataFrame:
    """Execute AllCauseUnplannedAdmissions for the given PY/ACO and
    return a per-person frame with count_unplanned_adm.

    Gates the denominator on the PY-aligned REACH population for
    ``aco_id`` so the cohort matches what CMS BLQQR-UAMCC reports on.
    """
    # The calc consumes gold-tier inputs (person_id-keyed,
    # standardized column names like birth_date). silver eligibility has
    # raw bene_* names and isn't usable here.
    claims = scan_gold("medical_claim")
    eligibility = scan_gold("eligibility")
    value_sets = UamccExpression.load_uamcc_value_sets(SILVER)

    transform = AllCauseUnplannedAdmissions(
        config={
            "measurement_year": performance_year,
            "program": "REACH",
            "aco_id": aco_id,
            "silver_path": str(SILVER),
        }
    )
    denom = transform.calculate_denominator(claims, eligibility, value_sets)
    numer = transform.calculate_numerator(denom, claims, value_sets)
    return (
        denom.join(numer, on="person_id", how="left")
        .with_columns(
            pl.col("count_unplanned_adm").fill_null(0).cast(pl.Int64)
        )
        .select("person_id", "count_unplanned_adm")
        .collect()
    )


def _compare(cms: pl.DataFrame, ours: pl.DataFrame, perf_year: str) -> dict:
    """Beneficiary-level join + agreement metrics."""
    # CMS: mbi, count_unplanned_adm  →  add person_id
    mbi_map = _mbi_to_person_id()
    cms_with_pid = (
        cms.join(mbi_map, on="mbi", how="left")
        .with_columns(
            pl.col("count_unplanned_adm").cast(pl.Int64, strict=False).alias("cms_count")
        )
        .drop("count_unplanned_adm")
    )

    cms_total_benes = cms.height
    cms_mapped_benes = cms_with_pid.filter(pl.col("person_id").is_not_null()).height

    # Inner join ours ↔ cms on person_id
    joined = cms_with_pid.filter(pl.col("person_id").is_not_null()).join(
        ours.with_columns(pl.col("count_unplanned_adm").alias("our_count")).drop(
            "count_unplanned_adm"
        ),
        on="person_id",
        how="inner",
    )

    # in_both is the real recall denominator: CMS benes that survive
    # into our calculated denom. ``cms_mapped_benes`` only checks gold
    # presence, which silently passes when the calc filters most CMS
    # benes out (e.g. wrong ACO scoping).
    in_both = joined.height
    cms_total_admits = (
        cms_with_pid["cms_count"].fill_null(0).sum()
    )
    our_total_admits_for_overlap = joined["our_count"].fill_null(0).sum()
    cms_total_admits_for_overlap = joined["cms_count"].fill_null(0).sum()

    exact_match = joined.filter(pl.col("cms_count") == pl.col("our_count")).height
    both_zero = joined.filter(
        (pl.col("cms_count") == 0) & (pl.col("our_count") == 0)
    ).height
    both_positive_same = joined.filter(
        (pl.col("cms_count") > 0) & (pl.col("cms_count") == pl.col("our_count"))
    ).height
    cms_only = joined.filter(
        (pl.col("cms_count") > 0) & (pl.col("our_count") == 0)
    ).height
    ours_only = joined.filter(
        (pl.col("cms_count") == 0) & (pl.col("our_count") > 0)
    ).height
    diff_count = joined.filter(
        (pl.col("cms_count") > 0)
        & (pl.col("our_count") > 0)
        & (pl.col("cms_count") != pl.col("our_count"))
    ).height

    delta_pct = (
        abs(our_total_admits_for_overlap - cms_total_admits_for_overlap)
        / max(cms_total_admits_for_overlap, 1)
    )

    return {
        "perf_year": perf_year,
        "cms_total_benes": cms_total_benes,
        "cms_mapped_benes": cms_mapped_benes,
        "in_both": in_both,
        # Recall now measures denom-overlap (CMS benes that landed in
        # our calculated denominator), not just MBI presence in gold.
        "recall": in_both / max(cms_total_benes, 1),
        "gold_presence": cms_mapped_benes / max(cms_total_benes, 1),
        "exact_match": exact_match,
        "exact_match_rate": exact_match / max(in_both, 1),
        "both_zero": both_zero,
        "both_positive_same": both_positive_same,
        "cms_only": cms_only,
        "ours_only": ours_only,
        "diff_count": diff_count,
        "cms_total_admits": int(cms_total_admits),
        "cms_total_admits_for_overlap": int(cms_total_admits_for_overlap),
        "our_total_admits_for_overlap": int(our_total_admits_for_overlap),
        "delta_pct": delta_pct,
    }


@pytest.fixture(scope="module")
def blqqr_uamcc():
    try:
        return scan_silver("blqqr_uamcc").collect()
    except Exception:
        pytest.skip("blqqr_uamcc not available")


@pytest.fixture(scope="module")
def tieout_results(blqqr_uamcc):
    """Run our calc + comparison for every PY present in BLQQR."""
    results = {}
    perf_years = (
        blqqr_uamcc.select(
            pl.col("source_filename").str.extract(r"\.(PY\d{4})\.", 1).alias("py")
        )["py"]
        .drop_nulls()
        .unique()
        .sort()
        .to_list()
    )
    for py_str in perf_years:
        cms = _latest_quarter_for_py(blqqr_uamcc, py_str)
        if cms.is_empty():
            continue
        py_int = int(py_str.replace("PY", ""))
        # BLQQR is a single-ACO file; pull aco_id from the data so the
        # gating helper looks up the correct alignment slice.
        aco_ids = cms["aco_id"].drop_nulls().unique().to_list()
        if len(aco_ids) != 1:
            continue
        ours = _run_our_calc(py_int, aco_ids[0])
        results[py_str] = _compare(
            cms.select("mbi", "count_unplanned_adm"), ours, py_str
        )
    return results


@requires_data
class TestUamccCalcTieOut:
    @pytest.mark.reconciliation
    def test_at_least_one_py_compared(self, tieout_results):
        assert tieout_results, "No PY snapshots produced — BLQQR has no source_filename data?"

    @pytest.mark.reconciliation
    def test_recall_meets_threshold_per_py(self, tieout_results):
        bad = {py: r for py, r in tieout_results.items() if r["recall"] < MIN_RECALL_PER_YEAR}
        msg = "\n".join(
            f"  [FAIL] {py}: {r['in_both']}/{r['cms_total_benes']} = "
            f"{r['recall']:.2%} in our denom "
            f"(gold presence: {r['gold_presence']:.2%})"
            for py, r in tieout_results.items()
        )
        assert not bad, (
            f"Per-PY recall (CMS benes that survive into our calculated "
            f"denominator) below {MIN_RECALL_PER_YEAR:.0%}:\n{msg}"
        )

    @pytest.mark.reconciliation
    def test_exact_match_rate_per_py(self, tieout_results):
        bad = {
            py: r
            for py, r in tieout_results.items()
            if r["in_both"] > 0 and r["exact_match_rate"] < MIN_EXACT_MATCH_RATE
        }
        msg = "\n".join(
            f"  {py}: {r['exact_match']}/{r['in_both']} = {r['exact_match_rate']:.2%}"
            f"  (both=0:{r['both_zero']} both>0_same:{r['both_positive_same']} "
            f"cms_only:{r['cms_only']} ours_only:{r['ours_only']} diff:{r['diff_count']})"
            for py, r in tieout_results.items()
        )
        assert not bad, (
            f"Per-bene exact match rate below {MIN_EXACT_MATCH_RATE:.0%}:\n{msg}"
        )

    @pytest.mark.reconciliation
    def test_total_admissions_delta_per_py(self, tieout_results):
        bad = {
            py: r
            for py, r in tieout_results.items()
            if r["cms_total_admits_for_overlap"] > 0 and r["delta_pct"] > MAX_TOTAL_DELTA_PCT
        }
        msg = "\n".join(
            f"  {py}: ours={r['our_total_admits_for_overlap']} "
            f"cms={r['cms_total_admits_for_overlap']} "
            f"(Δ{r['delta_pct']:+.2%})"
            for py, r in tieout_results.items()
        )
        assert not bad, (
            f"Total admission delta exceeds {MAX_TOTAL_DELTA_PCT:.0%}:\n{msg}"
        )
