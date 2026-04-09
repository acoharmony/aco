# © 2025 HarmonyCares
# All rights reserved.

"""Analytics gold layer pipeline - post-processes Tuva outputs and runs our own analytics transforms."""

from datetime import datetime
from typing import Any

import polars as pl

from .._decor8 import transform_method
from ._registry import register_pipeline
from ._stage import PipelineStage, execute_stage


@register_pipeline(name="analytics_gold")
@transform_method(
    enable_composition=False,
    threshold=10.0,
)
def apply_analytics_gold_pipeline(
    executor: Any, logger: Any, force: bool = False
) -> dict[str, pl.LazyFrame]:
    """
    Apply analytics gold layer pipeline.

    This pipeline:
    1. Post-processes Tuva outputs (dedupe readmissions, fix PMPM)
    2. Runs our own analytics transforms
    3. Produces clean outputs for dashboards

    Pipeline Order:
        Stage 1: readmissions_summary_deduped - Dedupe Tuva readmissions output
        Stage 2: financial_total_cost - Our PMPM calculations
        Stage 3: readmissions_enhanced - Our enhanced readmissions analysis (optional)

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

    # Import all transforms
    from .._transforms import (
        beneficiary_metrics,
        financial_pmpm_by_category,
        readmissions_summary,
        readmissions_summary_deduped,
    )

    # Stage 1: Service Category Classification (uses HealthcareTransformBase temporarily)
    service_category_stage = PipelineStage(
        name="service_category",
        module=None,  # Custom logic below - TODO: refactor to composable pattern
        group="analytics",
        order=1,
        depends_on=["medical_claim", "pharmacy_claim"],
    )

    # Stage 2: Readmissions Summary (our implementation)
    readmissions_stage = PipelineStage(
        name="readmissions_summary",
        module=readmissions_summary,
        group="analytics",
        order=2,
        depends_on=["medical_claim"],
    )

    # Stage 3: Dedupe readmissions_summary
    readmissions_deduped_stage = PipelineStage(
        name="readmissions_summary_deduped",
        module=readmissions_summary_deduped,
        group="analytics",
        order=3,
        depends_on=["readmissions_summary"],
    )

    # Stage 4: Financial PMPM by Category (our implementation)
    financial_pmpm_stage = PipelineStage(
        name="financial_pmpm_by_category",
        module=financial_pmpm_by_category,
        group="analytics",
        order=4,
        depends_on=["service_category", "consolidated_alignment"],
    )

    # Stage 5: Beneficiary-Level Metrics (spend, utilization, clinical indicators)
    beneficiary_metrics_stage = PipelineStage(
        name="beneficiary_metrics",
        module=beneficiary_metrics,
        group="analytics",
        order=5,
        depends_on=["medical_claim", "pharmacy_claim"],
    )

    stages = [
        service_category_stage,
        readmissions_stage,
        readmissions_deduped_stage,
        financial_pmpm_stage,
        beneficiary_metrics_stage,
    ]

    pipeline_start = datetime.now()
    checkpoint = PipelineCheckpoint("analytics_gold", force=force)

    logger.info(f"Starting Analytics Gold Pipeline: {len(stages)} stages")
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
            # Custom logic for service category classification
            if stage.name == "service_category":
                logger.info(
                    f"[{stage.group.upper()}] Stage {stage.order}: {stage.name} - Classifying claims by service category"
                )

                # Check if medical_claim exists
                medical_claim_path = gold_path / "medical_claim.parquet"
                if not medical_claim_path.exists():
                    logger.warning(f"medical_claim.parquet not found at {medical_claim_path}")
                    logger.info("Skipping service_category - run 'aco pipeline cclf_gold' first")
                    continue

                # Import and instantiate the transform
                from .._store import StorageBackend
                from .._transforms._service_category import ServiceCategoryTransform

                storage = StorageBackend()
                transform = ServiceCategoryTransform(storage=storage)

                # Execute the transform
                logger.info("  Running ServiceCategoryTransform.execute()...")
                transform.execute()

                # Get row count
                row_count = pl.scan_parquet(output_file).select(pl.len()).collect().item()
                logger.info(f"  [OK] Categorized {row_count:,} claims → {output_file.name}")

                checkpoint.mark_stage_complete(stage.name)

            else:
                # Execute normal transform module
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

    pipeline_elapsed = (datetime.now() - pipeline_start).total_seconds()
    logger.info("=" * 80)
    logger.info(
        f"[OK] Analytics Gold Pipeline Complete: {len(checkpoint.completed_stages)} tables generated"
    )
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info(
        f"  Total time: {pipeline_elapsed:.2f}s ({total_rows / pipeline_elapsed if pipeline_elapsed > 0 else 0:,.0f} rows/sec)"
    )
    logger.info("=" * 80)

    # Mark pipeline as complete - next run will start fresh
    checkpoint.mark_pipeline_complete(total_rows, pipeline_elapsed)

    # Return dict mapping stage names to parquet paths for compatibility
    return {name: gold_path / f"{name}.parquet" for name in checkpoint.completed_stages}
