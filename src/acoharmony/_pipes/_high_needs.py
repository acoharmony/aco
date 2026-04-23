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
from ._registry import register_pipeline
from ._stage import PipelineStage


@register_pipeline(name="high_needs")
@transform_method(
    enable_composition=False,
    threshold=10.0,
)
def apply_high_needs_pipeline(
    executor: Any,
    logger: Any,
    force: bool = False,
) -> dict[str, pl.LazyFrame]:
    """
    Run the High-Needs pipeline end to end.

    Writes:
        gold/hcc_risk_scores.parquet
        gold/high_needs_eligibility.parquet
        gold/high_needs_reconciliation.parquet
    """
    from .._transforms import (
        hcc_risk_scores,
        high_needs_eligibility,
        high_needs_reconciliation,
    )
    from ..medallion import MedallionLayer

    storage = executor.storage_config
    gold_path = Path(storage.get_path(MedallionLayer.GOLD))

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
    logger.info(f"Starting High-Needs Pipeline: {len(stages)} stages")
    logger.info("=" * 80)

    results: dict[str, pl.LazyFrame] = {}
    for stage in sorted(stages, key=lambda s: s.order):
        logger.info(f"[{stage.group.upper()}] Stage {stage.order}: {stage.name}")
        start = datetime.now()
        lf = stage.module.execute(executor)
        df = lf.collect()
        gold_path.mkdir(parents=True, exist_ok=True)
        output = gold_path / f"{stage.name}.parquet"
        df.write_parquet(output)
        elapsed = (datetime.now() - start).total_seconds()
        logger.info(
            f"  [OK] {stage.name} → {output.name} ({df.height:,} rows, {elapsed:.1f}s)"
        )
        results[stage.name] = df.lazy()

    elapsed = (datetime.now() - pipeline_start).total_seconds()
    logger.info("=" * 80)
    logger.info(f"High-Needs Pipeline complete in {elapsed:.1f}s")
    return results
