# © 2025 HarmonyCares
# All rights reserved.

"""
Identity Timeline Pipeline

Builds the unified identifier-history pipeline:
    silver/identity_timeline.parquet         — append-only MBI observations
    gold/identity_timeline.parquet           — joins bnex opt-outs as observations
    gold/identity_timeline_metrics.parquet   — per-file_date churn / quality metrics

Consolidates the identifier-history concerns previously scattered across
enterprise_crosswalk, int_beneficiary_xref_deduped, beneficiary_xref, and
bnex. The legacy tables remain untouched — downstream consumers keep working
while new work migrates to the timeline.
"""

from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import transform_method
from ._registry import register_pipeline
from ._stage import PipelineStage, execute_stage


@register_pipeline(name="identity_timeline")
@transform_method(
    enable_composition=False,
    threshold=10.0,
)
def apply_identity_timeline_pipeline(
    executor: Any,
    logger: Any,
    force: bool = False,
) -> dict[str, pl.LazyFrame]:
    """
    Build the identity timeline pipeline end to end.

    Pipeline Order:
        Stage 1: identity_timeline             (silver) — CCLF9 + CCLF8 → timeline
        Stage 2: identity_timeline_gold        (gold)   — + bnex opt-outs
        Stage 3: identity_timeline_metrics     (gold)   — per-file_date metrics

    Prerequisites:
        - silver/cclf9.parquet
        - silver/cclf8.parquet
        - silver/bnex.parquet    (gold stage only)
        - silver/hcmpi_master.parquet (optional; identity enrichment)

    Args:
        executor: TransformRunner instance
        logger: Logger instance
        force: Force recalculation

    Returns:
        dict[str, Path]: stage_name -> output parquet path
    """
    from .._transforms import (
        _identity_timeline,
        _identity_timeline_gold,
        _identity_timeline_metrics,
    )
    from ..medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)
    gold_path = storage.get_path(MedallionLayer.GOLD)

    # Stage 1 lives in silver; stages 2 and 3 write to gold. execute_stage
    # derives the output file from `{base_path}/{stage.name}.parquet`, so we
    # wrap each module to advertise the right target.
    stages = [
        PipelineStage(
            name="identity_timeline",
            module=_identity_timeline,
            group="silver",
            order=1,
            depends_on=[],
        ),
        PipelineStage(
            name="identity_timeline_gold",
            module=_identity_timeline_gold,
            group="gold",
            order=2,
            depends_on=["identity_timeline"],
        ),
        PipelineStage(
            name="identity_timeline_metrics",
            module=_identity_timeline_metrics,
            group="gold",
            order=3,
            depends_on=["identity_timeline"],
        ),
    ]

    from ._checkpoint import PipelineCheckpoint

    pipeline_start = datetime.now()
    checkpoint = PipelineCheckpoint("identity_timeline", force=force)

    logger.info(f"Starting Identity Timeline Pipeline: {len(stages)} stages")
    logger.info("=" * 80)
    checkpoint.log_resume_info(logger, len(stages))

    # The gold stages' outputs under gold/ share a name with no silver
    # collision, so write-names are stable. We just need to route the
    # base_path per stage.
    stage_base = {
        "identity_timeline": silver_path,
        "identity_timeline_gold": gold_path,
        "identity_timeline_metrics": gold_path,
    }

    # Gold stage emits to `identity_timeline.parquet` in gold — disambiguate
    # from the silver file by giving the gold file a distinct name.
    rename_output = {
        "identity_timeline_gold": "identity_timeline.parquet",
        "identity_timeline_metrics": "identity_timeline_metrics.parquet",
    }

    for stage in sorted(stages, key=lambda s: s.order):
        base = stage_base[stage.name]
        out_name = rename_output.get(stage.name, f"{stage.name}.parquet")
        output_file = base / out_name

        should_skip, row_count = checkpoint.should_skip_stage(stage.name, output_file, logger)

        if should_skip:
            logger.info(f"[{stage.group.upper()}] Stage {stage.order}: {stage.name} (skipped, {row_count:,} rows)")
            checkpoint.completed_stages.append(stage.name)
            continue

        try:
            lf = stage.module.execute(executor)
            df = lf.collect()
            base.mkdir(parents=True, exist_ok=True)
            df.write_parquet(output_file)
            logger.info(
                f"[{stage.group.upper()}] Stage {stage.order}: {stage.name} "
                f"wrote {df.height:,} rows to {output_file.name}"
            )
            checkpoint.mark_stage_complete(stage.name)
        except Exception as e:
            logger.error(f"[ERROR] {stage.name} failed: {e}")
            logger.info(f"Completed stages saved to: {checkpoint.get_tracking_file_path()}")
            raise

    logger.info("=" * 80)
    total_rows = 0
    for stage in stages:
        if stage.name in checkpoint.completed_stages:
            base = stage_base[stage.name]
            out_name = rename_output.get(stage.name, f"{stage.name}.parquet")
            file_path = base / out_name
            if Path(file_path).exists():
                row_count = pl.scan_parquet(file_path).select(pl.len()).collect().item()
                total_rows += row_count
                logger.info(f"  {stage.name}: {row_count:,} rows")

    pipeline_elapsed = (datetime.now() - pipeline_start).total_seconds()
    logger.info("=" * 80)
    logger.info(f"[OK] Identity Timeline Pipeline Complete: {len(checkpoint.completed_stages)} stages")
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info(f"  Elapsed time: {pipeline_elapsed:.2f} seconds")

    checkpoint.mark_pipeline_complete(total_rows, pipeline_elapsed)

    return {
        stage.name: stage_base[stage.name] / rename_output.get(stage.name, f"{stage.name}.parquet")
        for stage in stages
        if stage.name in checkpoint.completed_stages
    }
