# © 2025 HarmonyCares
# All rights reserved.

"""
Per-criterion High-Needs eligibility recall vs. the BAR.

Why a separate test from ``high_needs_eligibility_count_tieout``
-----------------------------------------------------------------

The aggregate recall test asks: "of all benes BAR flagged as HN,
how many did we ever flag?". A single number — informative for
trending, useless for attribution. When the pipeline misses BAR-
flagged benes, the per-criterion breakdown tells you *which*
qualifying logic is the leak: a coding bug in criterion (a)
manifests very differently from a model-coefficient drift in
criterion (b).

The aggregate test cannot serve as a regression guard for either
of those because *both* show up as the same percentage delta in
the rolled-up number. This test fixes that by tracking each of
the four BAR-emitted criterion flags independently:

    bar.mobility_impairment_flag      ↔ our criterion (a)
    bar.high_risk_flag                ↔ our criterion (b)
    bar.medium_risk_unplanned_flag    ↔ our criterion (c)
    bar.frailty_flag                  ↔ our criterion (d)

(BAR does not emit a flag for criterion (e) — it's a check-date
SNF/HH count, not a one-shot indicator.)

A bene flagged under multiple criteria contributes to each of
their per-criterion denominators independently — that's
intentional. We want to know the recall *for that criterion's
qualifying population*, not the share of misses attributable to
each criterion.

Threshold mechanics
-------------------

Per-PY × per-criterion floors below. **The floors lag actual
recall**: each is set 1-2 percentage points below the most
recently observed value, so a regression of >1pp fails the test
but normal noise doesn't. When recall climbs, ratchet the floor
up by editing this dict — that locks in the gain so it can't
silently regress.

The floors below were captured 2026-05-07 against:
  - latest BAR file in silver/bar
  - gold/high_needs_eligibility built from medical_claim with
    criterion (a) using ONLY the inpatient branch of FOG line
    1503 (the non-inpatient distinct-DOS branch was not yet
    implemented). Once the pipeline regenerates against the
    fixed criterion-a code, (a) recall is expected to jump
    from ~0.88-0.92 to ~0.95+; ratchet (a) floors then.

Sample-on-failure behaviour mirrors the aggregate test: failures
include a small DataFrame of missed benes plus the BAR per-
criterion flag columns, so a CI failure surfaces *which* criterion
fired AND a sample of the affected benes in one log line.
"""

from __future__ import annotations

from dataclasses import dataclass

import polars as pl
import pytest

from .conftest import requires_data
from .high_needs_eligibility_count_tieout import (
    MISMATCH_SAMPLE_SIZE,
    _bar_high_needs_benes,
    _ever_eligible_benes,
    _latest_bar_per_py,
)


# Per-PY × per-criterion recall floors. Keys: ``(performance_year,
# criterion_letter)`` — letter is one of "a", "b", "c", "d".
#
# A PY/criterion pair missing from the dict is skipped (e.g. PYs
# not yet present in BAR). Adding a new PY is therefore as simple as
# adding entries; the test will pick them up automatically.
#
# Values are conservative floors (1-2pp below observed recall on
# 2026-05-07). Bumping a floor signals "this is the new minimum we
# expect to maintain" — only ratchet up after a code change that
# justifies the new level lands.
PER_CRITERION_RECALL_FLOORS: dict[tuple[int, str], float] = {
    # PY2023 ----------------------------------------------------
    (2023, "a"): 0.91,   # observed 0.9184 — bumps to ~0.95+ after criterion-a fix
    (2023, "b"): 0.95,   # observed 0.9553
    (2023, "c"): 0.95,   # observed 0.9688 (denom 32 — small, noisier)
    (2023, "d"): 0.97,   # observed 0.9746
    # PY2024 ----------------------------------------------------
    (2024, "a"): 0.90,   # observed 0.9087
    (2024, "b"): 0.93,   # observed 0.9357
    (2024, "c"): 0.95,   # observed 0.9577 (denom 71)
    (2024, "d"): 0.96,   # observed 0.9667
    # PY2025 ----------------------------------------------------
    (2025, "a"): 0.89,   # observed 0.9009
    (2025, "b"): 0.91,   # observed 0.9189
    (2025, "c"): 0.91,   # observed 0.9192 (denom 99)
    (2025, "d"): 0.96,   # observed 0.9644
    # PY2026 ----------------------------------------------------
    (2026, "a"): 0.88,   # observed 0.8824 — biggest gap, fix lands ratchets to ~0.95+
    (2026, "b"): 0.89,   # observed 0.8962 — upstream score divergence; needs data fix
    (2026, "c"): 0.91,   # observed 0.9167 (denom 96)
    (2026, "d"): 0.95,   # observed 0.9557
}


# Map from criterion letter to the BAR per-criterion flag column name
# emitted by ``_bar_high_needs_benes``.
_BAR_FLAG_BY_LETTER = {
    "a": "bar_a",   # mobility_impairment_flag
    "b": "bar_b",   # high_risk_flag
    "c": "bar_c",   # medium_risk_unplanned_flag
    "d": "bar_d",   # frailty_flag
}


@dataclass(frozen=True)
class PerCriterionRecallResult:
    performance_year: int
    criterion: str
    n_bar_flagged: int
    n_found_eligible: int
    n_missed: int
    recall: float
    floor: float
    missed_sample: pl.DataFrame


def _recall_for_py_criterion(
    py: int,
    letter: str,
    bar_benes: pl.DataFrame,
    ever_eligible: pl.DataFrame,
    floor: float,
) -> PerCriterionRecallResult:
    """Compute recall for one (PY, criterion) cell.

    Denominator: BAR-flagged benes for this PY whose ``bar_<letter>``
    flag is True. Numerator: those who appear with
    ``first_ever_eligible_check_date IS NOT NULL`` in our gold output.
    """
    flag_col = _BAR_FLAG_BY_LETTER[letter]
    py_letter_benes = bar_benes.filter(
        (pl.col("performance_year") == py) & pl.col(flag_col)
    )
    n_bar = py_letter_benes.height

    hits = py_letter_benes.join(
        ever_eligible, left_on="resolved_mbi", right_on="mbi", how="inner"
    )
    n_found = hits.height
    missed = py_letter_benes.join(
        ever_eligible, left_on="resolved_mbi", right_on="mbi", how="anti"
    )
    n_missed = missed.height

    recall = 1.0 if n_bar == 0 else n_found / n_bar

    return PerCriterionRecallResult(
        performance_year=py,
        criterion=letter,
        n_bar_flagged=n_bar,
        n_found_eligible=n_found,
        n_missed=n_missed,
        recall=recall,
        floor=floor,
        missed_sample=missed.head(MISMATCH_SAMPLE_SIZE),
    )


@requires_data
class TestHighNeedsPerCriterionRecall:
    """Per-PY × per-criterion recall against ``PER_CRITERION_RECALL_FLOORS``.

    The floors form a 2D ratchet: criterion (a) can climb to 0.95+
    without affecting criterion (b)'s 0.89 floor, and vice versa.
    Run once after every pipeline regeneration; if a cell fails,
    the failure message identifies the criterion and a sample of
    missed benes so attribution does not require a follow-up query.
    """

    @pytest.fixture(scope="class")
    def bar_high_needs(self) -> pl.DataFrame:
        return _bar_high_needs_benes(_latest_bar_per_py())

    @pytest.fixture(scope="class")
    def ever_eligible(self) -> pl.DataFrame:
        return _ever_eligible_benes()

    @pytest.mark.reconciliation
    def test_each_per_criterion_recall_meets_floor(
        self,
        bar_high_needs: pl.DataFrame,
        ever_eligible: pl.DataFrame,
    ):
        present_pys = set(bar_high_needs["performance_year"].unique().to_list())
        if not present_pys:
            pytest.skip("No BAR performance years found")

        results: list[PerCriterionRecallResult] = []
        for (py, letter), floor in sorted(PER_CRITERION_RECALL_FLOORS.items()):
            if py not in present_pys:
                continue
            results.append(
                _recall_for_py_criterion(
                    py, letter, bar_high_needs, ever_eligible, floor
                )
            )

        lines = ["Per-PY × per-criterion recall (ours vs. latest BAR):"]
        for r in results:
            ok = r.recall >= r.floor
            status = "OK " if ok else "FAIL"
            lines.append(
                f"  [{status}] PY{r.performance_year} ({r.criterion}): "
                f"{r.n_found_eligible:,}/{r.n_bar_flagged:,} = "
                f"{r.recall:.4f}  (floor {r.floor:.2f}; missed {r.n_missed:,})"
            )
        print("\n".join(lines))

        failures = [r for r in results if r.recall < r.floor]
        if failures:
            detail = ["Per-criterion recall floor violations:"]
            for r in failures:
                detail.append(
                    f"PY{r.performance_year} criterion ({r.criterion}): "
                    f"recall={r.recall:.4f} (< {r.floor:.2f}), missed "
                    f"{r.n_missed:,} / {r.n_bar_flagged:,}. Sample:"
                )
                detail.append(str(r.missed_sample))
            pytest.fail("\n".join(detail))
