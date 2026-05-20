# © 2025 HarmonyCares
# All rights reserved.

"""Home visit claims gold layer pipeline."""

from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import transform_method
from ._registry import register_pipeline
from ._stage import PipelineStage, execute_stage


@register_pipeline(name="home_visit_gold")
@transform_method(
    enable_composition=False,
    threshold=5.0,
)
def apply_home_visit_gold_pipeline(
    executor: Any, logger: Any, force: bool = False
) -> dict[str, pl.LazyFrame]:
    """
    Apply home visit claims gold transformation.

    Filters physician claims to home visit HCPCS codes and produces
    TIN/NPI combinations for home visit services.

    Pipeline Order:
        Stage 1: home_visit_claims - Filter physician claims to home visit codes

    Prerequisites:
        - int_physician_claim_deduped must exist in silver layer

    Args:
        executor: TransformRunner instance with storage_config and catalog access
        logger: Logger instance for recording operations
        force: Force reprocessing regardless of tracking state

    Returns:
        dict[str, pl.LazyFrame]: Dictionary mapping output names to LazyFrames

    Raises:
        FileNotFoundError: If prerequisite int_physician_claim_deduped does not exist
    """
    from .._transforms import home_visit_claims
    from ..medallion import MedallionLayer

    # Verify prerequisites exist
    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)
    gold_path_check = storage.get_path(MedallionLayer.GOLD)

    # Check silver layer prerequisite
    prerequisite_silver = silver_path / "int_physician_claim_deduped.parquet"
    if not Path(prerequisite_silver).exists():
        error_msg = (
            f"Prerequisite file not found: {prerequisite_silver}\n"
            "Run the CCLF silver pipeline first: uv run aco pipeline cclf_silver"
        )
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    # Check gold layer prerequisite
    prerequisite_gold = gold_path_check / "medical_claim.parquet"
    if not Path(prerequisite_gold).exists():
        error_msg = (
            f"Prerequisite file not found: {prerequisite_gold}\n"
            "Run the CCLF gold pipeline first: uv run aco pipeline cclf_gold"
        )
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    gold_path = storage.get_path(MedallionLayer.GOLD)

    stages = [
        PipelineStage(
            name="home_visit_claims",
            module=home_visit_claims,
            group="gold",
            order=1,
            depends_on=["int_physician_claim_deduped"],
        ),
    ]

    from ._checkpoint import PipelineCheckpoint

    pipeline_start = datetime.now()
    checkpoint = PipelineCheckpoint("home_visit_gold", force=force)

    logger.info(f"Starting Home Visit Gold Pipeline: {len(stages)} table")
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
    logger.info(f"[OK] Home Visit Gold Pipeline Complete: {len(checkpoint.completed_stages)} table generated")
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info(
        f"  Pipeline time: {pipeline_elapsed:.2f}s ({total_rows / pipeline_elapsed:,.0f} rows/sec)"
    )
    logger.info("=" * 80)

    # Mark pipeline as complete - next run will start fresh
    checkpoint.mark_pipeline_complete(total_rows, pipeline_elapsed)

    # Return dict mapping stage names to parquet paths for compatibility
    return {name: gold_path / f"{name}.parquet" for name in checkpoint.completed_stages}
