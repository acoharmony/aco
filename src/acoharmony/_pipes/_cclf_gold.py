# © 2025 HarmonyCares
# All rights reserved.

"""CCLF gold layer pipeline."""

from datetime import datetime
from typing import Any

import polars as pl

from .._decor8 import transform_method
from ._registry import register_pipeline
from ._stage import PipelineStage, execute_stage


@register_pipeline(name="cclf_gold")
@transform_method(
    enable_composition=False,
    threshold=5.0,
)
def apply_cclf_gold_pipeline(
    executor: Any, logger: Any, force: bool = False
) -> dict[str, pl.LazyFrame]:
    """
    Apply CCLF gold transformations to produce final normalized outputs.

    Pipeline Order:
        Stage 1: medical_claim - Union all claim types into single table
        Stage 2: pharmacy_claim - Part D claims normalized
        Stage 3: eligibility - Demographics + enrollment for member spans

    Args:
        executor: TransformRunner instance with storage_config and catalog access
        logger: Logger instance for recording operations
        force: Force reprocessing regardless of tracking state

    Returns:
        dict[str, pl.LazyFrame]: Dictionary mapping output names to LazyFrames
    """
    from .._transforms import eligibility, medical_claim, pharmacy_claim
    from ..medallion import MedallionLayer

    gold_path = executor.storage_config.get_path(MedallionLayer.GOLD)

    stages = [
        PipelineStage(
            name="medical_claim",
            module=medical_claim,
            group="gold",
            order=1,
            depends_on=[
                "int_dme_claim_deduped",
                "int_physician_claim_deduped",
                "int_institutional_claim_deduped",
            ],
        ),
        PipelineStage(
            name="pharmacy_claim",
            module=pharmacy_claim,
            group="gold",
            order=2,
            depends_on=["int_pharmacy_claim_deduped"],
        ),
        PipelineStage(
            name="eligibility",
            module=eligibility,
            group="gold",
            order=3,
            depends_on=["int_beneficiary_demographics_deduped", "int_enrollment"],
        ),
    ]

    from ._checkpoint import PipelineCheckpoint

    pipeline_start = datetime.now()
    checkpoint = PipelineCheckpoint("cclf_gold", force=force)

    logger.info(f"Starting CCLF Gold Pipeline: {len(stages)} tables")
    logger.info("=" * 80)
    checkpoint.log_resume_info(logger, len(stages))

    # Execute stages with checkpoint/resume
    for stage in sorted(stages, key=lambda s: s.order):
        output_file = gold_path / f"{stage.name}.parquet"

        # Check if we should skip this stage
        should_skip, row_count = checkpoint.should_skip_stage(stage.name, output_file, logger)

        if should_skip:
            # Stage already completed in previous run
            logger.info(f"[{stage.group.upper()}] Stage {stage.order}: {stage.name}")
            checkpoint.completed_stages.append(stage.name)
            continue

        try:
            # Execute the stage
            _, _ = execute_stage(stage, executor, logger, gold_path)
            checkpoint.mark_stage_complete(stage.name)

        except Exception as e:
            logger.error(f"[ERROR] {stage.name} failed: {e}")
            logger.info(f"\n{'=' * 80}")
            logger.info(f"Pipeline STOPPED at stage {stage.order}/{len(stages)}: {stage.name}")
            logger.info(f"Completed stages saved to: {checkpoint.get_tracking_file_path()}")
            logger.info("To resume from this stage, run again (completed stages will be skipped)")
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
    logger.info(f"[OK] CCLF Gold Pipeline Complete: {len(checkpoint.completed_stages)} tables generated")
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info(
        f"  Pipeline time: {pipeline_elapsed:.2f}s ({total_rows / pipeline_elapsed:,.0f} rows/sec)"
    )
    logger.info("=" * 80)

    # Mark pipeline as complete - next run will start fresh
    checkpoint.mark_pipeline_complete(total_rows, pipeline_elapsed)

    # Return dict mapping stage names to parquet paths for compatibility
    return {name: gold_path / f"{name}.parquet" for name in checkpoint.completed_stages}
