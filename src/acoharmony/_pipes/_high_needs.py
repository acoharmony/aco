# © 2025 HarmonyCares
# All rights reserved.

"""
High-Needs eligibility pipeline.

Wires the three High-Needs transforms end-to-end:

    Stage 1 (gold): hcc_risk_scores              — CMS-HCC + CMS-HCC
                                                   ESRD + CMMI-HCC
                                                   Concurrent per-MBI
                                                   scores
    Stage 2 (gold): high_needs_eligibility       — per-(mbi, check_date)
                                                   eligibility decision
    Stage 3 (gold): high_needs_reconciliation    — our decision vs BAR

Stages 2 and 3 depend on Stage 1; Stage 3 depends on Stage 2. Each
stage's output parquet lands directly in the gold tier.

Runs incrementally: each stage streams to parquet via ``execute_stage``
(no full in-memory collect), and a ``PipelineCheckpoint`` lets a
resumed run skip stages whose parquet already landed. Pass ``force=True``
to re-run everything from scratch.

Prerequisites (bronze/silver must be materialised):
    silver/cclf1.parquet
    silver/cclf6.parquet   (optional; empty d-criterion if absent)
    silver/bar.parquet
    silver/reach_appendix_tables_mobility_impairment_icd10.parquet
    silver/reach_appendix_tables_frailty_hcpcs.parquet
    gold/eligibility.parquet
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import transform_method
from ._checkpoint import PipelineCheckpoint
from ._registry import register_pipeline
from ._stage import PipelineStage, execute_stage


@register_pipeline(name="high_needs")
@transform_method(
    enable_composition=False,
    threshold=10.0,
)
def apply_high_needs_pipeline(
    executor: Any,
    logger: Any,
    force: bool = False,
) -> dict[str, Path]:
    """
    Run the High-Needs pipeline end to end.

    Writes:
        gold/hcc_risk_scores.parquet
        gold/high_needs_eligibility.parquet
        gold/high_needs_reconciliation.parquet

    Args:
        executor: TransformRunner with storage_config and catalog access.
        logger: Logger for recording operations.
        force: Re-run every stage even if its parquet is already present.

    Returns:
        dict mapping stage name to its gold parquet path.
    """
    from .._transforms import (
        hcc_risk_scores,
        high_needs_eligibility,
        high_needs_reconciliation,
    )
    from ..medallion import MedallionLayer

    storage = executor.storage_config
    gold_path = Path(storage.get_path(MedallionLayer.GOLD))
    gold_path.mkdir(parents=True, exist_ok=True)

    stages = [
        PipelineStage(
            name="hcc_risk_scores",
            module=hcc_risk_scores,
            group="gold",
            order=1,
            depends_on=[],
        ),
        PipelineStage(
            name="high_needs_eligibility",
            module=high_needs_eligibility,
            group="gold",
            order=2,
            depends_on=["hcc_risk_scores"],
        ),
        PipelineStage(
            name="high_needs_reconciliation",
            module=high_needs_reconciliation,
            group="gold",
            order=3,
            depends_on=["high_needs_eligibility"],
        ),
    ]

    pipeline_start = datetime.now()
    checkpoint = PipelineCheckpoint("high_needs", force=force)

    logger.info(f"Starting High-Needs Pipeline: {len(stages)} stages")
    logger.info("=" * 80)
    checkpoint.log_resume_info(logger, len(stages))

    for stage in sorted(stages, key=lambda s: s.order):
        output_file = gold_path / f"{stage.name}.parquet"

        should_skip, _ = checkpoint.should_skip_stage(
            stage.name, output_file, logger
        )
        if should_skip:
            logger.info(
                f"[{stage.group.upper()}] Stage {stage.order}: {stage.name}"
            )
            checkpoint.completed_stages.append(stage.name)
            continue

        try:
            execute_stage(stage, executor, logger, gold_path)
            checkpoint.mark_stage_complete(stage.name)
        except Exception as e:
            logger.error(f"[ERROR] {stage.name} failed: {e}")
            logger.info(f"\n{'=' * 80}")
            logger.info(
                f"Pipeline STOPPED at stage {stage.order}/{len(stages)}: "
                f"{stage.name}"
            )
            logger.info(
                f"Completed stages saved to: "
                f"{checkpoint.get_tracking_file_path()}"
            )
            logger.info(
                "To resume from this stage, run again "
                "(completed stages will be skipped)"
            )
            logger.info("To force re-run all stages, pass force=True")
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

    elapsed = (datetime.now() - pipeline_start).total_seconds()
    logger.info("=" * 80)
    logger.info(
        f"[OK] High-Needs Pipeline Complete: "
        f"{len(checkpoint.completed_stages)} stages"
    )
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info(f"  Pipeline time: {elapsed:.2f}s")
    logger.info("=" * 80)

    checkpoint.mark_pipeline_complete(total_rows, elapsed)

    return {name: gold_path / f"{name}.parquet" for name in checkpoint.completed_stages}
