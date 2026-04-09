# © 2025 HarmonyCares
# All rights reserved.

"""
Wound care analysis pipeline.

Filters medical claims for wound care and skin substitute procedures.

Outputs:
- wound_care_claims.parquet: All wound care HCPCS codes (137 codes)
- skin_substitute_claims.parquet: Skin substitute codes (Q4xxx, 15xxx, C5xxx, A2xxx)
"""

from typing import Any

import polars as pl

from .._decor8 import transform_method
from .._log import LogWriter
from ._registry import register_pipeline
from ._stage import PipelineStage, execute_stage

logger = LogWriter("pipes.wound_care")


@register_pipeline(name="wound_care")
@transform_method(
    enable_composition=False,
    threshold=10.0,
)
def apply_wound_care_pipeline(
    executor: Any, logger: Any, force: bool = False
) -> dict[str, pl.LazyFrame]:
    """
    Apply wound care analysis pipeline.

    This pipeline:
    1. Filters medical claims for wound care HCPCS codes
    2. Filters medical claims for skin substitute HCPCS codes

    Pipeline Order:
        Stage 1: wound_care_claims - All wound care procedures (137 codes)
        Stage 2: skin_substitute_claims - Skin substitutes only (Q4xxx, etc.)

    Args:
        executor: TransformRunner instance with storage_config and catalog access
        logger: Logger instance for recording operations
        force: Force reprocessing regardless of tracking state

    Returns:
        dict[str, pl.LazyFrame]: Dictionary mapping output names to LazyFrames
    """
    from ..medallion import MedallionLayer
    from ._checkpoint import PipelineCheckpoint

    gold_path = executor.storage_config.get_path(MedallionLayer.GOLD)

    # Import transforms
    from .._transforms import skin_substitute_claims, wound_care_claims

    # Stage 1: Wound care claims
    wound_care_stage = PipelineStage(
        name="wound_care_claims",
        module=wound_care_claims,
        group="wound_care",
        order=1,
        depends_on=["medical_claim"],
    )

    # Stage 2: Skin substitute claims
    skin_substitute_stage = PipelineStage(
        name="skin_substitute_claims",
        module=skin_substitute_claims,
        group="wound_care",
        order=2,
        depends_on=["medical_claim"],
    )

    stages = [wound_care_stage, skin_substitute_stage]

    checkpoint = PipelineCheckpoint("wound_care", force=force)

    logger.info(f"Starting Wound Care Pipeline: {len(stages)} stages")
    logger.info("=" * 80)
    checkpoint.log_resume_info(logger, len(stages))

    # Execute stages with checkpoint/resume
    for stage in sorted(stages, key=lambda s: s.order):
        output_file = gold_path / f"{stage.name}.parquet"

        # Check if we should skip this stage
        should_skip, row_count = checkpoint.should_skip_stage(stage.name, output_file, logger)

        if should_skip:
            # Stage already completed in previous run
            logger.info(f"[{stage.group.upper()}] Stage {stage.order}: {stage.name} (skipped)")
            checkpoint.completed_stages.append(stage.name)
            continue

        try:
            # Execute transform module
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

    # Count rows from written parquet files
    logger.info("=" * 80)
    logger.info("Counting final row counts...")
    total_rows = 0
    for stage_name in checkpoint.completed_stages:
        file_path = gold_path / f"{stage_name}.parquet"
        if file_path.exists():
            row_count = pl.scan_parquet(file_path).select(pl.len()).collect().item()
            total_rows += row_count
            logger.info(f"  {stage_name}: {row_count:,} rows")

    logger.info("=" * 80)
    logger.info(
        f"[OK] Wound Care Pipeline Complete: {len(checkpoint.completed_stages)} tables generated"
    )
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info("=" * 80)

    # Mark pipeline as complete
    checkpoint.mark_pipeline_complete(total_rows, 0)

    # Return dict mapping stage names to parquet paths
    return {name: gold_path / f"{name}.parquet" for name in checkpoint.completed_stages}
