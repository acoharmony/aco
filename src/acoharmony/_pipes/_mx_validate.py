# © 2025 HarmonyCares
# All rights reserved.

"""
mx_validate: greedy measure-validation pipeline.

Generates one persisted table per (measure × performance_year × quarter)
that we have BLQQR reference data for, then a single tieout table
joining computed values to CMS reference at per-beneficiary granularity.

Stages
------
1. ``mx_validate_scope`` (bronze)
   Walks ``silver/blqqr_*.parquet``, parses ``\\.Q(\\d)\\.PY(\\d{4})\\.``
   from each file's ``source_filename``, intersects with the set of
   measures registered in ``MeasureFactory``, and writes one row per
   ``(measure, py, quarter)`` with status: ``ready`` if a transform
   exists, ``skip:no_transform`` if the BLQQR ref exists but no measure
   class is registered. Schema-aware: records ``ref_row_count`` and
   ``ref_columns_hash`` so downstream sees if BLQQR shape changes.

2. ``mx_validate_compute`` (silver, per-scope)
   For every ``status=ready`` scope, runs the registered measure
   against gold claims/eligibility and writes
   ``silver/mx_validate_{measure}_PY{py}_Q{q}.parquet``. Per-file so
   reruns can checkpoint individual scopes.

3. ``mx_validate_tieout`` (gold, single table)
   Left-joins each per-scope silver compute table to its BLQQR
   reference on (aco_id, bene_id), writes one consolidated
   ``gold/mx_validate_tieout.parquet`` row per scope with bene counts,
   agreement_pct, mean_abs_diff, and a JSON sample of worst mismatches.

The pipeline is greedy: it discovers the full matrix from on-disk silver
BLQQR parquets — first available PY/quarter through latest — and runs
every (measure, py, quarter) combo that has both a transform and a ref.
No filters; rerun the whole matrix every time.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import polars as pl

from .._log import LogWriter
from ._registry import register_pipeline

logger = LogWriter("pipes.mx_validate")

# Matches ".Q1.PY2024." inside BLQQR filenames.
_PY_QUARTER_RE = re.compile(r"\.Q(\d)\.PY(\d{4})\.")

# Map BLQQR ref-table suffix → registered MeasureFactory id.
# Add a row here when you register a new measure transform.
_MEASURE_REGISTRY: dict[str, str] = {
    "acr": "NQF1789",
    "uamcc": "NQF2888",
    "dah": "REACH_DAH",
    # "exclusions" is a reference-only aggregate (not per-bene), no transform.
}

# Per-measure column carrying the value to tie out at the bene level.
_MEASURE_TIEOUT_COLUMN: dict[str, str] = {
    "acr": "radm30_flag",      # 0/1 readmission within 30 days
    "uamcc": "count_unplanned_adm",
    "dah": "observed_dah",
}

# Per-measure column on the computed frame that maps to the ref column above.
_COMPUTED_TIEOUT_COLUMN: dict[str, str] = {
    "acr": "numerator_flag",   # cast to int
    "uamcc": "count_unplanned_adm",
    "dah": "observed_dah",
}

# Per-measure value-set parquet filenames (in silver/) keyed by the short
# names the transform expects in value_sets[]. Matches the dicts in
# _expressions/_acr_readmission.py and _expressions/_uamcc.py.
_VALUE_SET_FILES: dict[str, dict[str, str]] = {
    "acr": {
        "ccs_icd10_cm": "value_sets_acr_ccs_icd10_cm.parquet",
        "exclusions": "value_sets_acr_exclusions.parquet",
        "cohort_icd10": "value_sets_acr_cohort_icd10.parquet",
        "cohort_ccs": "value_sets_acr_cohort_ccs.parquet",
        "paa2": "value_sets_acr_paa2.parquet",
    },
    "uamcc": {
        "cohort": "value_sets_uamcc_value_set_cohort.parquet",
        "ccs_icd10_cm": "value_sets_uamcc_value_set_ccs_icd10_cm.parquet",
        "ccs_icd10_pcs": "value_sets_uamcc_value_set_ccs_icd10_pcs.parquet",
        "exclusions": "value_sets_uamcc_value_set_exclusions.parquet",
        "paa1": "value_sets_uamcc_value_set_paa1.parquet",
        "paa2": "value_sets_uamcc_value_set_paa2.parquet",
        "paa3": "value_sets_uamcc_value_set_paa3.parquet",
        "paa4": "value_sets_uamcc_value_set_paa4.parquet",
    },
    # DAH carries no value-sets; spec uses claim attributes directly.
    "dah": {},
}


def _load_value_sets(silver_path: Any, measure: str) -> dict[str, pl.LazyFrame]:
    """Load all silver value-set parquets for a measure into a {key: LazyFrame} dict.

    Missing files are silently skipped — the transform is responsible for
    handling absent value sets (e.g. ACR falls back to no readmit-CCS join).
    """
    from pathlib import Path

    silver_path = Path(silver_path)
    out: dict[str, pl.LazyFrame] = {}
    for key, fname in _VALUE_SET_FILES.get(measure, {}).items():
        f = silver_path / fname
        if f.exists():
            out[key] = pl.scan_parquet(f)
    return out


# ---------------------------------------------------------------------------
# Stage 1: scope discovery
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ScopeRow:
    measure: str
    py: int
    quarter: int
    source_filename: str
    ref_row_count: int
    ref_columns_hash: str
    transform_class: str | None
    status: str  # "ready" | "skip:no_transform" | "skip:no_ref_rows"
    skip_reason: str | None


def _hash_columns(cols: list[str]) -> str:
    import hashlib

    return hashlib.sha1("|".join(sorted(cols)).encode()).hexdigest()[:12]


def discover_scopes(silver_path: Any) -> list[ScopeRow]:
    """
    Greedy scan of silver/blqqr_*.parquet → all known (measure, py, q) scopes.

    Data-aware: PY/quarter come from the actual ``source_filename`` values
    present in each ref parquet, so first-available through latest-available
    is whatever's on disk today.
    """
    from pathlib import Path

    from .._transforms._quality_measure_base import MeasureFactory

    silver_path = Path(silver_path)
    rows: list[ScopeRow] = []
    registered = set(MeasureFactory.list_measures())

    for measure_short, measure_id in _MEASURE_REGISTRY.items():
        ref_file = silver_path / f"blqqr_{measure_short}.parquet"
        if not ref_file.exists():
            continue

        ref = pl.scan_parquet(ref_file)
        cols = ref.collect_schema().names()
        col_hash = _hash_columns(cols)
        filenames = (
            ref.select("source_filename").unique().collect().to_series().to_list()
        )

        # Group filenames by (py, quarter); count ref rows per group.
        py_q_to_files: dict[tuple[int, int], list[str]] = {}
        for fn in filenames:
            m = _PY_QUARTER_RE.search(fn or "")
            if not m:
                continue
            q, py = int(m.group(1)), int(m.group(2))
            py_q_to_files.setdefault((py, q), []).append(fn)

        for (py, q), fns in sorted(py_q_to_files.items()):
            n_rows = (
                ref.filter(pl.col("source_filename").is_in(fns))
                .select(pl.len())
                .collect()
                .item()
            )
            transform_class = measure_id if measure_id in registered else None
            if transform_class is None:
                status, skip = "skip:no_transform", f"{measure_id} not in MeasureFactory"
            elif n_rows == 0:
                status, skip = "skip:no_ref_rows", "BLQQR ref empty for scope"
            else:
                status, skip = "ready", None
            rows.append(
                ScopeRow(
                    measure=measure_short,
                    py=py,
                    quarter=q,
                    source_filename=fns[0],
                    ref_row_count=n_rows,
                    ref_columns_hash=col_hash,
                    transform_class=transform_class,
                    status=status,
                    skip_reason=skip,
                )
            )
    return rows


def _scope_rows_to_frame(rows: list[ScopeRow]) -> pl.DataFrame:
    return pl.DataFrame(
        {
            "measure": [r.measure for r in rows],
            "py": [r.py for r in rows],
            "quarter": [r.quarter for r in rows],
            "source_filename": [r.source_filename for r in rows],
            "ref_row_count": [r.ref_row_count for r in rows],
            "ref_columns_hash": [r.ref_columns_hash for r in rows],
            "transform_class": [r.transform_class for r in rows],
            "status": [r.status for r in rows],
            "skip_reason": [r.skip_reason for r in rows],
        }
    )


# ---------------------------------------------------------------------------
# Stage 2: per-scope compute
# ---------------------------------------------------------------------------


def _scope_output_name(measure: str, py: int, quarter: int) -> str:
    return f"mx_validate_{measure}_PY{py}_Q{quarter}.parquet"


def compute_scope(
    scope: ScopeRow,
    claims: pl.LazyFrame,
    eligibility: pl.LazyFrame,
    hcc_scores: pl.LazyFrame | None = None,
    silver_path: Any = None,
) -> pl.LazyFrame:
    """
    Run the registered measure for one scope and return the per-bene result.

    Output columns: person_id, denom_flag, num_flag, plus the measure's
    tieout column under its computed name (see _COMPUTED_TIEOUT_COLUMN).

    Per-measure value-set wiring:
      - DAH (REACH_DAH) gets value_sets['hcc_scores'] pre-filtered to
        ``performance_year == scope.py - 1`` (CMS PY2025 QMMR §3.3.2 p15
        criterion 4). DAH carries no codeset value-sets.
      - ACR (NQF1789) and UAMCC (NQF2888) need codeset value-sets loaded
        from silver/value_sets_*.parquet. The set of files per measure
        is defined in _VALUE_SET_FILES; ``silver_path`` must be passed
        so we can locate them.
    """
    from .._transforms._quality_measure_base import MeasureFactory

    # Different measure classes consult different config keys for the year:
    # ACR/DAH read 'performance_year', UAMCC reads 'measurement_year'.
    # Pass both so the matrix is invariant to the convention.
    measure = MeasureFactory.create(
        scope.transform_class,
        config={
            "performance_year": scope.py,
            "measurement_year": scope.py,
            "quarter": scope.quarter,
        },
    )

    # value_sets is the measure-class extension point.
    value_sets: dict[str, pl.LazyFrame] = {"eligibility": eligibility}
    if hcc_scores is not None:
        value_sets["hcc_scores"] = hcc_scores.filter(
            pl.col("performance_year") == (scope.py - 1)
        )
    if silver_path is not None:
        value_sets.update(_load_value_sets(silver_path, scope.measure))

    denom = measure.calculate_denominator(claims, eligibility, value_sets)
    num = measure.calculate_numerator(denom, claims, value_sets)

    out_col = _COMPUTED_TIEOUT_COLUMN[scope.measure]
    if out_col == "numerator_flag":
        # ACR: numerator_flag is already on `num`
        result = (
            denom.join(num, on="person_id", how="left")
            .with_columns(pl.col("numerator_flag").fill_null(False))
            .select(
                [
                    "person_id",
                    pl.col("denominator_flag").alias("denom_flag"),
                    pl.col("numerator_flag").alias("num_flag"),
                    pl.col("numerator_flag").cast(pl.Int64).alias(out_col),
                ]
            )
        )
    else:
        # UAMCC / DAH carry their tieout column directly on `num`.
        result = (
            denom.join(num, on="person_id", how="left")
            .with_columns(
                [
                    pl.col(out_col).fill_null(0),
                ]
            )
            .select(
                [
                    "person_id",
                    pl.col("denominator_flag").alias("denom_flag"),
                    pl.lit(True).alias("num_flag"),
                    out_col,
                ]
            )
        )
    return result


# ---------------------------------------------------------------------------
# Stage 3: tieout
# ---------------------------------------------------------------------------


def _ref_value_for_scope(
    silver_path: Any, scope: ScopeRow
) -> pl.LazyFrame:
    """
    Load the BLQQR reference values for one scope, with the tieout column
    cast to a comparable Int64.
    """
    from pathlib import Path

    silver_path = Path(silver_path)
    ref_col = _MEASURE_TIEOUT_COLUMN[scope.measure]
    ref = (
        pl.scan_parquet(silver_path / f"blqqr_{scope.measure}.parquet")
        .filter(pl.col("source_filename") == scope.source_filename)
    )
    # ref join key is (aco_id, bene_id); computed key is person_id, which
    # in this codebase equals bene_id from CCLF. Surface bene_id as person_id.
    ref = ref.select(
        [
            pl.col("bene_id").alias("person_id"),
            pl.col(ref_col).cast(pl.Int64, strict=False).alias("ref_value"),
        ]
    )
    return ref


def tieout_scope(
    scope: ScopeRow,
    computed: pl.LazyFrame,
    ref: pl.LazyFrame,
) -> dict[str, Any]:
    """
    Per-scope agreement metrics + worst-mismatch sample.
    """
    out_col = _COMPUTED_TIEOUT_COLUMN[scope.measure]
    joined = (
        ref.join(
            computed.select(["person_id", pl.col(out_col).cast(pl.Int64).alias("computed_value")]),
            on="person_id",
            how="left",
        )
        .with_columns(pl.col("computed_value").fill_null(0))
        .with_columns(
            (pl.col("computed_value") - pl.col("ref_value")).abs().alias("abs_diff")
        )
        .collect()
    )

    n_ref = joined.height
    n_matched = joined.filter(pl.col("abs_diff") == 0).height
    agreement_pct = (n_matched / n_ref) if n_ref else 0.0
    mean_abs_diff = joined["abs_diff"].mean() if n_ref else 0.0

    worst = (
        joined.filter(pl.col("abs_diff") > 0)
        .sort("abs_diff", descending=True)
        .head(10)
        .to_dicts()
    )
    return {
        "measure": scope.measure,
        "py": scope.py,
        "quarter": scope.quarter,
        "source_filename": scope.source_filename,
        "bene_count_ref": n_ref,
        "bene_count_computed": int(
            computed.select(pl.len()).collect().item()
        ),
        "bene_count_matched": n_matched,
        "agreement_pct": agreement_pct,
        "mean_abs_diff": float(mean_abs_diff or 0.0),
        "worst_mismatches_json": json.dumps(worst, default=str),
    }


# ---------------------------------------------------------------------------
# Pipeline entry
# ---------------------------------------------------------------------------


@register_pipeline(
    name="mx_validate",
    description=(
        "Greedy measure-validation matrix: per (measure × PY × quarter) compute "
        "+ tieout against BLQQR reference. Reads silver, writes silver per-scope "
        "compute tables and one consolidated gold tieout table."
    ),
)
def apply_mx_validate_pipeline(
    executor: Any, logger: Any, force: bool = False
) -> dict[str, Any]:
    """Run the three mx_validate stages end-to-end."""
    from pathlib import Path

    from ..medallion import MedallionLayer

    bronze_path = Path(executor.storage_config.get_path(MedallionLayer.BRONZE))
    silver_path = Path(executor.storage_config.get_path(MedallionLayer.SILVER))
    gold_path = Path(executor.storage_config.get_path(MedallionLayer.GOLD))

    started = datetime.now()
    logger.info("=" * 80)
    logger.info("mx_validate pipeline starting")
    logger.info("=" * 80)

    # --- Stage 1: scope discovery -----------------------------------------
    scopes = discover_scopes(silver_path)
    scope_frame = _scope_rows_to_frame(scopes)
    bronze_path.mkdir(parents=True, exist_ok=True)
    scope_out = bronze_path / "mx_validate_scope.parquet"
    scope_frame.write_parquet(scope_out)
    logger.info(
        f"[scope] {len(scopes)} scopes "
        f"({sum(s.status == 'ready' for s in scopes)} ready, "
        f"{sum(s.status != 'ready' for s in scopes)} skipped) → {scope_out.name}"
    )
    for s in scopes:
        if s.status != "ready":
            logger.info(f"  skip {s.measure}/PY{s.py}/Q{s.quarter}: {s.skip_reason}")

    ready = [s for s in scopes if s.status == "ready"]

    # --- Stage 2: per-scope compute ---------------------------------------
    silver_path.mkdir(parents=True, exist_ok=True)
    claims = pl.scan_parquet(gold_path / "medical_claim.parquet")
    eligibility = pl.scan_parquet(gold_path / "eligibility.parquet")
    # HCC scores are required for DAH denominator (CMS PY2025 QMMR §3.3.2 p15:
    # avg HCC composite risk score ≥ 2.0 in the year before the PY). Optional
    # for measures that don't need it; if the file is absent we pass None and
    # the DAH transform logs a warning.
    hcc_scores_path = gold_path / "hcc_risk_scores.parquet"
    hcc_scores = pl.scan_parquet(hcc_scores_path) if hcc_scores_path.exists() else None
    written: list[str] = []
    for scope in ready:
        out_file = silver_path / _scope_output_name(scope.measure, scope.py, scope.quarter)
        if out_file.exists() and not force:
            logger.info(f"[compute] skip (exists) {out_file.name}")
            written.append(out_file.name)
            continue
        try:
            frame = compute_scope(
                scope, claims, eligibility,
                hcc_scores=hcc_scores, silver_path=silver_path,
            )
            frame.collect(streaming=True).write_parquet(out_file)
            written.append(out_file.name)
            logger.info(f"[compute] {out_file.name}")
        except Exception as e:  # ALLOWED: continue matrix on per-scope failure
            logger.error(f"[compute] FAILED {out_file.name}: {e}")

    # --- Stage 3: consolidated tieout -------------------------------------
    tieout_rows: list[dict[str, Any]] = []
    for scope in ready:
        out_file = silver_path / _scope_output_name(scope.measure, scope.py, scope.quarter)
        if not out_file.exists():
            continue
        computed = pl.scan_parquet(out_file)
        ref = _ref_value_for_scope(silver_path, scope)
        try:
            tieout_rows.append(tieout_scope(scope, computed, ref))
        except Exception as e:  # ALLOWED: continue matrix on per-scope failure
            logger.error(f"[tieout] FAILED {scope.measure}/PY{scope.py}/Q{scope.quarter}: {e}")

    gold_path.mkdir(parents=True, exist_ok=True)
    tieout_out = gold_path / "mx_validate_tieout.parquet"
    pl.DataFrame(tieout_rows).write_parquet(tieout_out) if tieout_rows else None
    logger.info(f"[tieout] {len(tieout_rows)} scopes → {tieout_out.name}")

    elapsed = (datetime.now() - started).total_seconds()
    logger.info("=" * 80)
    logger.info(f"mx_validate complete in {elapsed:.1f}s")
    logger.info("=" * 80)

    return {
        "scope": scope_out,
        "compute": [silver_path / n for n in written],
        "tieout": tieout_out if tieout_rows else None,
    }
