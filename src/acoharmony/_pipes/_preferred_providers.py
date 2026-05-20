# © 2025 HarmonyCares
# All rights reserved.

"""Preferred-provider claims pipeline.

Two-stage gold pipeline that joins silver/participant_list against
gold/medical_claim to attribute beneficiaries to each preferred provider
``(tin, npi)``. Default facet is
``("Preferred Provider", "Facility and Institutional Provider")``.

Prerequisites
-------------
- ``silver/participant_list.parquet``  (participant_list transform)
- ``gold/medical_claim.parquet``       (cclf_gold pipeline)

Stages
------
1. ``preferred_provider_facility_rollup``  — one row per ``(tin, npi)``
2. ``preferred_provider_facility_benes``   — one row per ``(tin, npi, member_id)``
"""

from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import transform_method
from ._registry import register_pipeline
from ._stage import PipelineStage, execute_stage


@register_pipeline(name="preferred_providers")
@transform_method(
    enable_composition=False,
    threshold=5.0,
)
def apply_preferred_providers_pipeline(
    executor: Any, logger: Any, force: bool = False
) -> dict[str, pl.LazyFrame]:
    """
    Apply preferred-provider claims attribution gold pipeline.

    Builds two gold outputs from silver/participant_list × gold/medical_claim:

    - ``preferred_provider_facility_rollup``  — per ``(tin, npi)`` facility
    - ``preferred_provider_facility_benes``   — per ``(tin, npi, member_id)``

    Args:
        executor: TransformRunner instance with ``storage_config`` access
        logger: Logger instance for recording operations
        force: Force reprocessing regardless of tracking state

    Returns:
        dict[str, Path]: Mapping of stage name → output parquet path

    Raises:
        FileNotFoundError: If either prerequisite parquet is missing.
    """
    from .._transforms import (
        preferred_provider_facility_benes,
        preferred_provider_facility_rollup,
    )
    from ..medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = Path(storage.get_path(MedallionLayer.SILVER))
    gold_path = Path(storage.get_path(MedallionLayer.GOLD))

    # Prerequisite checks — fail fast with an actionable message rather than
    # surfacing a polars ColumnNotFoundError mid-stream.
    prerequisite_silver = silver_path / "participant_list.parquet"
    if not prerequisite_silver.exists():
        msg = (
            f"Prerequisite file not found: {prerequisite_silver}\n"
            "Run the participant_list transform first: "
            "uv run aco transform participant_list"
        )
        logger.error(msg)
        raise FileNotFoundError(msg)

    prerequisite_gold = gold_path / "medical_claim.parquet"
    if not prerequisite_gold.exists():
        msg = (
            f"Prerequisite file not found: {prerequisite_gold}\n"
            "Run the cclf_gold pipeline first: uv run aco pipeline cclf_gold"
        )
        logger.error(msg)
        raise FileNotFoundError(msg)

    stages = [
        PipelineStage(
            name="preferred_provider_facility_rollup",
            module=preferred_provider_facility_rollup,
            group="gold",
            order=1,
            depends_on=["participant_list", "medical_claim"],
        ),
        PipelineStage(
            name="preferred_provider_facility_benes",
            module=preferred_provider_facility_benes,
            group="gold",
            order=2,
            depends_on=["participant_list", "medical_claim"],
        ),
    ]

    from ._checkpoint import PipelineCheckpoint

    pipeline_start = datetime.now()
    checkpoint = PipelineCheckpoint("preferred_providers", force=force)

    logger.info(f"Starting Preferred Providers Pipeline: {len(stages)} tables")
    logger.info("=" * 80)
    checkpoint.log_resume_info(logger, len(stages))

    for stage in sorted(stages, key=lambda s: s.order):
        output_file = gold_path / f"{stage.name}.parquet"
        should_skip, _ = checkpoint.should_skip_stage(stage.name, output_file, logger)

        if should_skip:
            logger.info(f"[{stage.group.upper()}] Stage {stage.order}: {stage.name}")
            checkpoint.completed_stages.append(stage.name)
            continue

        try:
            execute_stage(stage, executor, logger, gold_path)
            checkpoint.mark_stage_complete(stage.name)
        except Exception as e:
            logger.error(f"[ERROR] {stage.name} failed: {e}")
            logger.info(f"\n{'=' * 80}")
            logger.info(
                f"Pipeline STOPPED at stage {stage.order}/{len(stages)}: {stage.name}"
            )
            logger.info(f"Completed stages saved to: {checkpoint.get_tracking_file_path()}")
            logger.info(
                "To resume from this stage, run again (completed stages will be skipped)"
            )
            logger.info("To force re-run all stages, use --force flag")
            logger.info(f"{'=' * 80}\n")
            raise

    logger.info("=" * 80)
    logger.info("Counting final row counts...")
    total_rows = 0
    for stage_name in checkpoint.completed_stages:
        file_path = gold_path / f"{stage_name}.parquet"
        row_count = pl.scan_parquet(file_path).select(pl.len()).collect().item()
        total_rows += row_count
        logger.info(f"  {stage_name}: {row_count:,} rows")

    pipeline_elapsed = (datetime.now() - pipeline_start).total_seconds()
    logger.info("=" * 80)
    logger.info(
        f"[OK] Preferred Providers Pipeline Complete: "
        f"{len(checkpoint.completed_stages)} tables generated"
    )
    logger.info(f"  Total rows: {total_rows:,}")
    if pipeline_elapsed > 0:
        logger.info(
            f"  Pipeline time: {pipeline_elapsed:.2f}s "
            f"({total_rows / pipeline_elapsed:,.0f} rows/sec)"
        )
    logger.info("=" * 80)

    checkpoint.mark_pipeline_complete(total_rows, pipeline_elapsed)

    return {name: gold_path / f"{name}.parquet" for name in checkpoint.completed_stages}
