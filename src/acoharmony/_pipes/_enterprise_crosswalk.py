# © 2025 HarmonyCares
# All rights reserved.

"""
Enterprise Crosswalk Pipeline

Builds enterprise-wide MBI crosswalk with HCMPI integration.
"""

from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import transform_method
from ._registry import register_pipeline
from ._stage import PipelineStage, execute_stage


@register_pipeline(name="enterprise_crosswalk")
@transform_method(
    enable_composition=False,
    threshold=10.0,
)
def apply_enterprise_crosswalk_pipeline(
    executor: Any,
    logger: Any,
    force: bool = False,
) -> dict[str, pl.LazyFrame]:
    """
    Build enterprise crosswalk from source data.

    This pipeline generates a silver-tier crosswalk table by calling
    the registered enterprise_xwalk transform.

    Pipeline Order:
        Stage 1: int_beneficiary_xref_deduped - Deduplicate beneficiary xref
        Stage 2: int_beneficiary_demographics_deduped - Deduplicate beneficiary demographics
        Stage 3: enterprise_crosswalk - Build MBI crosswalk with HCMPI

    Prerequisites:
        - hcmpi_master must exist
        - cclf9 (beneficiary_xref) must exist
        - cclf8 (beneficiary_demographics) must exist

    Args:
        executor: TransformRunner instance
        logger: Logger instance
        force: Force recalculation

    Returns:
        dict[str, pl.LazyFrame]: Output data
    """
    from .._transforms import (
        int_beneficiary_demographics_deduped,
        int_beneficiary_xref_deduped,
    )
    from .._transforms._enterprise_xwalk import apply_transform
    from ..medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    # Create pipeline stage
    class EnterpriseCrosswalkModule:
        """Pseudo-module for enterprise crosswalk transform."""

        @staticmethod
        def execute(executor):
            return apply_transform(None, {}, executor.catalog, executor.logger, force=force)

    stages = [
        PipelineStage(
            name="int_beneficiary_xref_deduped",
            module=int_beneficiary_xref_deduped,
            group="foundation",
            order=1,
            depends_on=[],
        ),
        PipelineStage(
            name="int_beneficiary_demographics_deduped",
            module=int_beneficiary_demographics_deduped,
            group="foundation",
            order=2,
            depends_on=["int_beneficiary_xref_deduped"],
        ),
        PipelineStage(
            name="enterprise_crosswalk",
            module=EnterpriseCrosswalkModule,
            group="silver",
            order=3,
            depends_on=["int_beneficiary_xref_deduped", "int_beneficiary_demographics_deduped"],
        ),
    ]

    from ._checkpoint import PipelineCheckpoint

    pipeline_start = datetime.now()
    checkpoint = PipelineCheckpoint("enterprise_crosswalk", force=force)

    logger.info(f"Starting Enterprise Crosswalk Pipeline: {len(stages)} stages")
    logger.info("=" * 80)
    checkpoint.log_resume_info(logger, len(stages))

    # Execute stages
    for stage in sorted(stages, key=lambda s: s.order):
        output_file = silver_path / f"{stage.name}.parquet"

        should_skip, row_count = checkpoint.should_skip_stage(stage.name, output_file, logger)

        if should_skip:
            logger.info(f"[{stage.group.upper()}] Stage {stage.order}: {stage.name}")
            checkpoint.completed_stages.append(stage.name)
            continue

        try:
            _, _ = execute_stage(stage, executor, logger, silver_path)
            checkpoint.mark_stage_complete(stage.name)

        except Exception as e:
            logger.error(f"[ERROR] {stage.name} failed: {e}")
            logger.info(f"\n{'=' * 80}")
            logger.info(f"Pipeline STOPPED at stage {stage.order}/{len(stages)}: {stage.name}")
            logger.info(f"Completed stages saved to: {checkpoint.get_tracking_file_path()}")
            logger.info("To resume from this stage, run again")
            logger.info(f"{'=' * 80}\n")
            raise

    logger.info("=" * 80)
    logger.info("Counting final row counts...")
    total_rows = 0
    for stage_name in checkpoint.completed_stages:
        file_path = silver_path / f"{stage_name}.parquet"
        if Path(file_path).exists():
            row_count = pl.scan_parquet(file_path).select(pl.len()).collect().item()
            total_rows += row_count
            logger.info(f"  {stage_name}: {row_count:,} rows")

    pipeline_elapsed = (datetime.now() - pipeline_start).total_seconds()
    logger.info("=" * 80)
    logger.info(f"[OK] Enterprise Crosswalk Pipeline Complete: {len(checkpoint.completed_stages)} stages completed")
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info(f"  Elapsed time: {pipeline_elapsed:.2f} seconds")
    logger.info(f"  Primary output: {silver_path}/enterprise_crosswalk.parquet")

    checkpoint.mark_pipeline_complete(total_rows, pipeline_elapsed)

    return {name: silver_path / f"{name}.parquet" for name in checkpoint.completed_stages}
