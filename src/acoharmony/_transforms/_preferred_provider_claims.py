# © 2025 HarmonyCares
# All rights reserved.

"""
Preferred-provider claims attribution transform.

Joins the participant roster (silver/participant_list) against the
medical-claims fact (gold/medical_claim) to answer the question
"which beneficiaries received care at each ``(tin, npi)`` for a given
``(provider_category, provider_type)`` facet?"

Two grain outputs are produced:

* **Per-facility rollup** — one row per ``(tin, npi)``:
  ``unique_bene_count``, ``claim_count``, DOS range, paid total,
  claim-type set. Default output:
  ``gold/preferred_provider_facility_rollup.parquet``.

* **Per-bene-at-facility detail** — one row per
  ``(tin, npi, member_id)``: claim count, DOS range, paid total,
  claim-type set. Default output:
  ``gold/preferred_provider_facility_benes.parquet``.

The facet is parameterized — default is
``("Preferred Provider", "Facility and Institutional Provider")`` per
the original ask, but the same machinery works for any other facet
(e.g. organizational preferred providers, individual participants).
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from .._expressions._facility_claims import (
    DEFAULT_PROVIDER_CATEGORY,
    DEFAULT_PROVIDER_TYPE,
    FacilityClaimsExpression,
)

# Default output filenames (in the gold tier).
DEFAULT_ROLLUP_FILENAME: str = "preferred_provider_facility_rollup.parquet"
DEFAULT_BENES_FILENAME: str = "preferred_provider_facility_benes.parquet"


def _scan_participant_list(silver_path: Path) -> pl.LazyFrame:
    """Lazy-scan the silver participant_list parquet."""
    return pl.scan_parquet(silver_path / "participant_list.parquet")


def _scan_medical_claim(gold_path: Path) -> pl.LazyFrame:
    """Lazy-scan the gold medical_claim parquet."""
    return pl.scan_parquet(gold_path / "medical_claim.parquet")


def build_rollup_lazy(
    silver_path: Path,
    gold_path: Path,
    provider_category: str = DEFAULT_PROVIDER_CATEGORY,
    provider_type: str = DEFAULT_PROVIDER_TYPE,
) -> pl.LazyFrame:
    """
    Build the per-``(tin, npi)`` facility rollup as a ``LazyFrame``.

    Pure data-construction — no I/O beyond the lazy scans. Suitable for
    sinking via the pipeline stage runner (which calls ``sink_parquet``
    with streaming).
    """
    return FacilityClaimsExpression.build_facility_rollup(
        _scan_participant_list(silver_path),
        _scan_medical_claim(gold_path),
        provider_category,
        provider_type,
    )


def build_bene_detail_lazy(
    silver_path: Path,
    gold_path: Path,
    provider_category: str = DEFAULT_PROVIDER_CATEGORY,
    provider_type: str = DEFAULT_PROVIDER_TYPE,
) -> pl.LazyFrame:
    """
    Build the per-``(tin, npi, member_id)`` detail as a ``LazyFrame``.

    Pure data-construction — no I/O beyond the lazy scans.
    """
    return FacilityClaimsExpression.build_facility_bene_detail(
        _scan_participant_list(silver_path),
        _scan_medical_claim(gold_path),
        provider_category,
        provider_type,
    )


def execute_to_gold(
    silver_path: Path,
    gold_path: Path,
    provider_category: str = DEFAULT_PROVIDER_CATEGORY,
    provider_type: str = DEFAULT_PROVIDER_TYPE,
    rollup_filename: str = DEFAULT_ROLLUP_FILENAME,
    benes_filename: str = DEFAULT_BENES_FILENAME,
    logger: Any | None = None,
) -> tuple[Path, Path]:
    """
    Run the rollup + bene-detail joins and write both parquets to ``gold/``.

    Notebook-friendly direct entrypoint. The :mod:`preferred_providers` pipe
    bypasses this and calls the pure builders via the stage runner so the
    write path goes through ``sink_parquet`` streaming.

    Returns
    -------
    (rollup_path, benes_path)
        Absolute paths of the two gold parquets just written.
    """
    gold_path.mkdir(parents=True, exist_ok=True)

    rollup = build_rollup_lazy(
        silver_path, gold_path, provider_category, provider_type
    ).collect()
    rollup_out = gold_path / rollup_filename
    rollup.write_parquet(rollup_out, compression="zstd")
    if logger is not None:
        logger.info(
            f"Wrote {rollup.height:,} rows × {len(rollup.columns)} cols "
            f"to gold/{rollup_out.name}"
        )

    bene_detail = build_bene_detail_lazy(
        silver_path, gold_path, provider_category, provider_type
    ).collect()
    benes_out = gold_path / benes_filename
    bene_detail.write_parquet(benes_out, compression="zstd")
    if logger is not None:
        logger.info(
            f"Wrote {bene_detail.height:,} rows × {len(bene_detail.columns)} cols "
            f"to gold/{benes_out.name}"
        )

    return rollup_out, benes_out


@transform(
    name="preferred_provider_claims",
    tier=["gold"],
    sql_enabled=False,
)
@transform_method(enable_composition=False, threshold=5.0)
def apply_transform(
    df: pl.LazyFrame,
    schema: dict,
    catalog: Any,
    logger: Any,
    force: bool = False,
    provider_category: str = DEFAULT_PROVIDER_CATEGORY,
    provider_type: str = DEFAULT_PROVIDER_TYPE,
) -> pl.LazyFrame:
    """
    Runner-compatible entry point for the preferred-provider claims join.

    Reads silver/participant_list and gold/medical_claim from the storage
    backend, writes the two gold output parquets, and returns the rollup
    LazyFrame so the calling runner can also persist a copy to its
    default output location if it chooses to.

    Notes
    -----
    The runner contract passes ``df`` as the registered schema's bronze→
    silver LazyFrame, but this transform sources its own inputs from the
    storage backend (we need *both* the participant roster *and* the
    claims fact, so we ignore ``df``). The notebook entry point bypasses
    the runner entirely and calls :func:`execute_to_gold` directly.
    """
    logger.info(
        f"Starting transform: preferred_provider_claims "
        f"(facet: {provider_category} × {provider_type})"
    )

    silver_path = catalog.storage_config.get_path("silver")
    gold_path = catalog.storage_config.get_path("gold")

    rollup_path, _benes_path = execute_to_gold(
        Path(silver_path),
        Path(gold_path),
        provider_category=provider_category,
        provider_type=provider_type,
        logger=logger,
    )

    logger.info("Completed transform: preferred_provider_claims")
    return pl.scan_parquet(rollup_path)
