# © 2025 HarmonyCares
# All rights reserved.

"""
Unified alignment pipeline.

Consolidates voluntary alignment (SVA/PBVAR/emails/mailings) and
ACO alignment (temporal matrix, demographics, office matching,
provider attribution, metrics, metadata, transitions) into a
single end-to-end pipeline.

Output: gold/consolidated_alignment.parquet
"""

from datetime import datetime
from typing import Any

import polars as pl

from .._decor8 import transform_method
from ._builder import PipelineStage
from ._registry import register_pipeline


def _execute_stage(
    stage: PipelineStage, df: pl.LazyFrame | None, catalog: Any, logger: Any, force: bool
) -> pl.LazyFrame:
    """Execute a single alignment pipeline stage."""
    logger.info(f"[{stage.group.upper()}] Stage {stage.order}: {stage.name}")
    result = stage.module.apply_transform(df, {}, catalog, logger, force=force)
    logger.info(f"  [OK] {stage.name} complete")
    return result


@register_pipeline(name="alignment")
@transform_method(
    enable_composition=False,
    threshold=10.0,
)
def apply_alignment_pipeline(
    executor: Any, logger: Any, force: bool = False
) -> dict[str, pl.LazyFrame]:
    """
    Build the complete alignment view from source data.

    This pipeline runs all alignment stages end-to-end:

    Stage 0: voluntary_alignment — Consolidate SVA/PBVAR/email/mail touchpoints
    Stage 1: temporal_matrix     — Build year-month enrollment tracking
    Stage 2: join_voluntary      — Join voluntary alignment into temporal matrix
    Stage 3: demographics        — Join beneficiary demographics
    Stage 4: office_matching     — Match office locations (direct + fuzzy)
    Stage 5: provider_attribution — Join provider attribution
    Stage 6: consolidated_metrics — Calculate alignment metrics
    Stage 7: metadata_and_actions — Add metadata and action flags
    Stage 8: year_over_year_transitions — Analyze YoY transitions

    All stages are idempotent and can be safely re-run.

    Args:
        executor: TransformRunner instance
        logger: Logger instance
        force: Force reprocessing of all stages

    Returns:
        dict with 'consolidated_alignment' LazyFrame
    """
    from .._transforms import (
        _aco_alignment_demographics,
        _aco_alignment_metadata,
        _aco_alignment_metrics,
        _aco_alignment_office,
        _aco_alignment_provider,
        _aco_alignment_temporal,
        _aco_alignment_transitions,
        _aco_alignment_voluntary,
        _voluntary_alignment,
    )
    from ..medallion import MedallionLayer

    # --- Stage 0: Build voluntary alignment (silver) ---
    logger.info("[VOLUNTARY] Stage 0: voluntary_alignment")
    logger.info("  Consolidating SVA, PBVAR, email, and mail touchpoints")
    _voluntary_alignment.apply_transform(None, {}, executor.catalog, logger, force=force)

    silver_path = executor.storage_config.get_path(MedallionLayer.SILVER)
    output_path = silver_path / "voluntary_alignment.parquet"
    if output_path.exists():
        row_count = pl.scan_parquet(output_path).select(pl.len()).collect().item()
        logger.info(f"  [OK] voluntary_alignment → {row_count:,} rows")
    else:
        logger.info("  [OK] voluntary_alignment complete")

    # --- Stages 1-8: ACO alignment chain ---
    stages = [
        PipelineStage(
            name="temporal_matrix",
            module=_aco_alignment_temporal,
            group="foundation",
            order=1,
        ),
        PipelineStage(
            name="join_voluntary",
            module=_aco_alignment_voluntary,
            group="alignment",
            order=2,
            depends_on=["temporal_matrix"],
        ),
        PipelineStage(
            name="demographics",
            module=_aco_alignment_demographics,
            group="alignment",
            order=3,
            depends_on=["join_voluntary"],
        ),
        PipelineStage(
            name="office_matching",
            module=_aco_alignment_office,
            group="alignment",
            order=4,
            depends_on=["demographics"],
        ),
        PipelineStage(
            name="provider_attribution",
            module=_aco_alignment_provider,
            group="alignment",
            order=5,
            depends_on=["office_matching"],
        ),
        PipelineStage(
            name="consolidated_metrics",
            module=_aco_alignment_metrics,
            group="metrics",
            order=6,
            depends_on=["provider_attribution"],
        ),
        PipelineStage(
            name="metadata_and_actions",
            module=_aco_alignment_metadata,
            group="metadata",
            order=7,
            depends_on=["consolidated_metrics"],
        ),
        PipelineStage(
            name="year_over_year_transitions",
            module=_aco_alignment_transitions,
            group="analytics",
            order=8,
            depends_on=["metadata_and_actions"],
        ),
    ]

    pipeline_start = datetime.now()
    logger.info(f"Starting Alignment Pipeline: {len(stages) + 1} stages")
    logger.info("=" * 80)

    result_df = None
    for stage in sorted(stages, key=lambda s: s.order):
        try:
            result_df = _execute_stage(
                stage, result_df, executor.catalog, logger, force
            )
        except Exception as e:
            logger.error(f"[ERROR] {stage.name} failed: {e}")
            raise

    # Write consolidated output
    gold_path = executor.storage_config.get_path(MedallionLayer.GOLD)
    consolidated_path = gold_path / "consolidated_alignment.parquet"

    from ..config import get_config

    config = get_config()

    try:
        result_collected = result_df.collect()
        result_collected.write_parquet(
            str(consolidated_path),
            compression=config.transform.compression,
            row_group_size=config.transform.row_group_size,
        )
    except Exception as e:
        logger.error(f"Error writing parquet: {e}")
        logger.error(f"Schema: {result_df.collect_schema()}")
        raise

    pipeline_elapsed = (datetime.now() - pipeline_start).total_seconds()
    row_count = pl.scan_parquet(consolidated_path).select(pl.len()).collect().item()

    logger.info("=" * 80)
    logger.info("[OK] Alignment Pipeline Complete")
    logger.info(f"  Output: consolidated_alignment ({row_count:,} rows)")
    logger.info(f"  Time: {pipeline_elapsed:.2f}s")
    logger.info("=" * 80)

    return {"consolidated_alignment": result_df}
