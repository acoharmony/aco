# © 2025 HarmonyCares
# All rights reserved.

"""CCLF silver layer pipeline."""

from typing import Any

import polars as pl

from .._decor8 import transform_method
from ._registry import register_pipeline
from ._stage import PipelineStage, execute_stage


@register_pipeline(name="cclf_silver")
@transform_method(
    enable_composition=False,
    threshold=10.0,
)
def apply_cclf_silver_pipeline(
    executor: Any, logger: Any, force: bool = False
) -> dict[str, pl.LazyFrame]:
    """
    Apply CCLF silver transformations in dependency order.

    Pipeline Order (dependency-aware):
        Stage 1-2:   Foundation (xref, demographics)
        Stage 3-6:   ADR (adjustment/cancellation netting for all claim types)
        Stage 7-9:   Simple Dedups (physician, dme, pharmacy)
        Stage 10-12: Supporting Tables (diagnosis, procedure, revenue center)
        Stage 13-14: Pivots (diagnosis_pivot, procedure_pivot)
        Stage 15:    Complex Dedup (institutional - requires supporting tables)
        Stage 16:    Enrollment (member-month spans)

    Args:
        executor: TransformRunner instance with storage_config and catalog access
        logger: Logger instance for recording operations
        force: Force reprocessing regardless of tracking state

    Returns:
        dict[str, pl.LazyFrame]: Dictionary mapping output names to LazyFrames
    """
    from .._transforms import (
        int_beneficiary_demographics_deduped,
        int_beneficiary_xref_deduped,
        int_diagnosis_deduped,
        int_diagnosis_pivot,
        int_dme_claim_adr,
        int_dme_claim_deduped,
        int_enrollment,
        int_institutional_claim_adr,
        int_institutional_claim_deduped,
        int_pharmacy_claim_adr,
        int_pharmacy_claim_deduped,
        int_physician_claim_adr,
        int_physician_claim_deduped,
        int_procedure_deduped,
        int_procedure_pivot,
        int_revenue_center_deduped,
    )
    from ..medallion import MedallionLayer

    silver_path = executor.storage_config.get_path(MedallionLayer.SILVER)

    stages = [
        # Foundation
        PipelineStage(
            name="int_beneficiary_xref_deduped",
            module=int_beneficiary_xref_deduped,
            group="foundation",
            order=1,
        ),
        PipelineStage(
            name="int_beneficiary_demographics_deduped",
            module=int_beneficiary_demographics_deduped,
            group="foundation",
            order=2,
            depends_on=["int_beneficiary_xref_deduped"],
        ),
        # All ADR stages (adjustment/cancellation netting)
        PipelineStage(
            name="int_physician_claim_adr",
            module=int_physician_claim_adr,
            group="adr",
            order=3,
            depends_on=["int_beneficiary_xref_deduped"],
        ),
        PipelineStage(
            name="int_dme_claim_adr",
            module=int_dme_claim_adr,
            group="adr",
            order=4,
            depends_on=["int_beneficiary_xref_deduped"],
        ),
        PipelineStage(
            name="int_institutional_claim_adr",
            module=int_institutional_claim_adr,
            group="adr",
            order=5,
            depends_on=["int_beneficiary_xref_deduped"],
        ),
        PipelineStage(
            name="int_pharmacy_claim_adr",
            module=int_pharmacy_claim_adr,
            group="adr",
            order=6,
            depends_on=["int_beneficiary_xref_deduped"],
        ),
        # Simple claim dedups (no supporting tables needed)
        PipelineStage(
            name="int_physician_claim_deduped",
            module=int_physician_claim_deduped,
            group="dedup_simple",
            order=7,
            depends_on=["int_physician_claim_adr"],
        ),
        PipelineStage(
            name="int_dme_claim_deduped",
            module=int_dme_claim_deduped,
            group="dedup_simple",
            order=8,
            depends_on=["int_dme_claim_adr"],
        ),
        PipelineStage(
            name="int_pharmacy_claim_deduped",
            module=int_pharmacy_claim_deduped,
            group="dedup_simple",
            order=9,
            depends_on=["int_pharmacy_claim_adr"],
        ),
        # Supporting table dedups (diagnosis, procedure, revenue center)
        PipelineStage(
            name="int_diagnosis_deduped",
            module=int_diagnosis_deduped,
            group="supporting",
            order=10,
            depends_on=["int_beneficiary_xref_deduped"],
        ),
        PipelineStage(
            name="int_procedure_deduped",
            module=int_procedure_deduped,
            group="supporting",
            order=11,
            depends_on=["int_beneficiary_xref_deduped"],
        ),
        PipelineStage(
            name="int_revenue_center_deduped",
            module=int_revenue_center_deduped,
            group="supporting",
            order=12,
            depends_on=["int_beneficiary_xref_deduped"],
        ),
        # Pivots (transform supporting tables to wide format)
        PipelineStage(
            name="int_diagnosis_pivot",
            module=int_diagnosis_pivot,
            group="pivot",
            order=13,
            depends_on=["int_diagnosis_deduped"],
        ),
        PipelineStage(
            name="int_procedure_pivot",
            module=int_procedure_pivot,
            group="pivot",
            order=14,
            depends_on=["int_procedure_deduped"],
        ),
        # Complex dedup (requires supporting tables + pivots)
        PipelineStage(
            name="int_institutional_claim_deduped",
            module=int_institutional_claim_deduped,
            group="dedup_complex",
            order=15,
            depends_on=[
                "int_institutional_claim_adr",
                "int_revenue_center_deduped",
                "int_diagnosis_pivot",
                "int_procedure_pivot",
            ],
        ),
        # Enrollment
        PipelineStage(
            name="int_enrollment",
            module=int_enrollment,
            group="enrollment",
            order=16,
            depends_on=["int_beneficiary_xref_deduped"],
        ),
    ]

    from datetime import datetime

    from ._checkpoint import PipelineCheckpoint

    pipeline_start = datetime.now()
    checkpoint = PipelineCheckpoint("cclf_silver", force=force)

    logger.info(f"Starting CCLF Silver Pipeline: {len(stages)} stages")
    logger.info("=" * 80)
    checkpoint.log_resume_info(logger, len(stages))

    # Execute stages with checkpoint/resume
    for stage in sorted(stages, key=lambda s: s.order):
        output_file = silver_path / f"{stage.name}.parquet"

        # Check if we should skip this stage
        should_skip, row_count = checkpoint.should_skip_stage(stage.name, output_file, logger)

        if should_skip:
            # Stage already completed in previous run
            logger.info(f"[{stage.group.upper()}] Stage {stage.order}: {stage.name}")
            checkpoint.completed_stages.append(stage.name)
            continue

        try:
            # Execute the stage
            _, _ = execute_stage(stage, executor, logger, silver_path)
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

    # Count rows from written parquet files (fast, no memory pressure)
    logger.info("=" * 80)
    logger.info("Counting final row counts...")
    total_rows = 0
    for stage_name in checkpoint.completed_stages:
        file_path = silver_path / f"{stage_name}.parquet"
        row_count = pl.scan_parquet(file_path).select(pl.len()).collect().item()
        total_rows += row_count
        logger.info(f"  {stage_name}: {row_count:,} rows")

    pipeline_elapsed = (datetime.now() - pipeline_start).total_seconds()
    logger.info("=" * 80)
    logger.info(f"[OK] CCLF Silver Pipeline Complete: {len(checkpoint.completed_stages)} tables generated")
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info(
        f"  Total time: {pipeline_elapsed:.2f}s ({total_rows / pipeline_elapsed:,.0f} rows/sec)"
    )
    logger.info("=" * 80)

    # Mark pipeline as complete - next run will start fresh
    checkpoint.mark_pipeline_complete(total_rows, pipeline_elapsed)

    # Return dict mapping stage names to parquet paths for compatibility
    return {name: silver_path / f"{name}.parquet" for name in checkpoint.completed_stages}
