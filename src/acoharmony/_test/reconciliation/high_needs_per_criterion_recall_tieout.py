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

Lazy execution
--------------

The full recall table — all (PY × letter) cells at once — is built
as a single lazy pipeline: unpivot the four ``bar_<letter>`` flags
to long form, left-join against ever-eligible MBIs once, then
group_by (PY, criterion) and aggregate counts. Polars then
materialises only the small (≤ 16-row) result frame; nothing along
the way holds the cartesian product of BAR-benes × ever-eligible
that an eager per-cell loop would build. An earlier eager
implementation OOM'd on full-PY data because each cell ran two
hash joins against the full ever-eligible frame.

Missed-bene samples (for failure diagnostics) are computed
eagerly, but only for cells that actually breach their floor — in
the steady-state-green case zero cells, so zero extra work.

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
    _bar_high_needs_benes_lazy,
    _ever_eligible_benes_lazy,
    _latest_bar_per_py_lazy,
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

_BAR_FLAG_COLS = ("bar_a", "bar_b", "bar_c", "bar_d")


@dataclass(frozen=True)
class PerCriterionRecallResult:
    performance_year: int
    criterion: str
    n_bar_flagged: int
    n_found_eligible: int
    n_missed: int
    recall: float
    floor: float


def _per_criterion_recall_table(
    bar_high_needs: pl.LazyFrame,
    ever_eligible: pl.LazyFrame,
) -> pl.DataFrame:
    """Build the full (PY × criterion) recall table in one lazy pass.

    Steps, all on LazyFrames so polars can stream the join + group_by
    without materialising the full ever-eligible × bar-bene cross:

      1. Mark whether each BAR bene is in ``ever_eligible`` via a
         left semi-join projection (``is_eligible``) — done **once**,
         before unpivoting, so each bene's eligibility is looked up
         exactly one time even though they may carry multiple flags.
      2. Unpivot the four ``bar_<letter>`` boolean flag columns to
         long form, dropping rows where the flag is False — leaves
         one row per (resolved_mbi, PY, letter) the bene actually
         qualifies under.
      3. Group by (performance_year, criterion) and aggregate
         ``n_bar_flagged`` and ``n_found_eligible``.

    Returns an eager DataFrame with one row per cell; the per-cell
    miss samples (for failure diagnostics) are computed separately
    and only for cells that breach their floor.
    """
    bar_with_eligibility = bar_high_needs.join(
        ever_eligible.select(pl.col("mbi").alias("resolved_mbi"))
        .with_columns(pl.lit(True).alias("is_eligible")),
        on="resolved_mbi",
        how="left",
    ).with_columns(pl.col("is_eligible").fill_null(False))

    long = bar_with_eligibility.unpivot(
        on=list(_BAR_FLAG_COLS),
        index=["performance_year", "resolved_mbi", "is_eligible"],
        variable_name="flag_col",
        value_name="qualified",
    ).filter(pl.col("qualified"))

    return (
        long.with_columns(
            pl.col("flag_col").str.slice(-1, 1).alias("criterion")
        )
        .group_by("performance_year", "criterion")
        .agg(
            pl.len().alias("n_bar_flagged"),
            pl.col("is_eligible").sum().alias("n_found_eligible"),
        )
        .with_columns(
            (pl.col("n_bar_flagged") - pl.col("n_found_eligible")).alias(
                "n_missed"
            ),
            (pl.col("n_found_eligible") / pl.col("n_bar_flagged")).alias(
                "recall"
            ),
        )
        .collect()
    )


def _missed_sample(
    py: int,
    letter: str,
    bar_high_needs: pl.LazyFrame,
    ever_eligible: pl.LazyFrame,
) -> pl.DataFrame:
    """Eagerly materialise up to ``MISMATCH_SAMPLE_SIZE`` BAR-flagged
    misses for one (PY, letter) cell. Called only on test failure, so
    the eager join is bounded to a single failing slice — no risk of
    the all-cells × all-eligible memory blow-up the original
    implementation hit."""
    flag_col = f"bar_{letter}"
    return (
        bar_high_needs.filter(
            (pl.col("performance_year") == py) & pl.col(flag_col)
        )
        .join(
            ever_eligible.select(pl.col("mbi").alias("resolved_mbi"))
            .with_columns(pl.lit(True).alias("_eligible")),
            on="resolved_mbi",
            how="left",
        )
        .filter(pl.col("_eligible").is_null())
        .drop("_eligible")
        .head(MISMATCH_SAMPLE_SIZE)
        .collect()
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
    def bar_high_needs(self) -> pl.LazyFrame:
        # Fully lazy chain: silver/bar → latest-per-PY → flag filter →
        # canonical-MBI resolve → group-by-(PY, MBI). Cached at class
        # scope as the resulting LazyFrame plan; polars re-runs the
        # plan each time it's collected, but in practice the recall-
        # table call collects it once and the failure-sample calls
        # are skipped unless something breaks.
        return _bar_high_needs_benes_lazy(_latest_bar_per_py_lazy())

    @pytest.fixture(scope="class")
    def ever_eligible(self) -> pl.LazyFrame:
        return _ever_eligible_benes_lazy()

    @pytest.mark.reconciliation
    def test_each_per_criterion_recall_meets_floor(
        self,
        bar_high_needs: pl.LazyFrame,
        ever_eligible: pl.LazyFrame,
    ):
        recall_df = _per_criterion_recall_table(bar_high_needs, ever_eligible)

        # Pivot the eager result into PerCriterionRecallResult records,
        # joined to floors. Cells with no BAR data for a PY are simply
        # absent from recall_df and we skip them.
        rows_by_key = {
            (int(r["performance_year"]), r["criterion"]): r
            for r in recall_df.iter_rows(named=True)
        }

        results: list[PerCriterionRecallResult] = []
        for (py, letter), floor in sorted(PER_CRITERION_RECALL_FLOORS.items()):
            row = rows_by_key.get((py, letter))
            if row is None:
                continue
            results.append(
                PerCriterionRecallResult(
                    performance_year=py,
                    criterion=letter,
                    n_bar_flagged=int(row["n_bar_flagged"]),
                    n_found_eligible=int(row["n_found_eligible"]),
                    n_missed=int(row["n_missed"]),
                    recall=float(row["recall"]),
                    floor=floor,
                )
            )

        if not results:
            pytest.skip("No BAR/floor cells matched")

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
                sample = _missed_sample(
                    r.performance_year, r.criterion, bar_high_needs, ever_eligible
                )
                detail.append(
                    f"PY{r.performance_year} criterion ({r.criterion}): "
                    f"recall={r.recall:.4f} (< {r.floor:.2f}), missed "
                    f"{r.n_missed:,} / {r.n_bar_flagged:,}. Sample:"
                )
                detail.append(str(sample))
            pytest.fail("\n".join(detail))
