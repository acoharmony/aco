# © 2025 HarmonyCares
# All rights reserved.

"""CCLF bronze layer pipeline."""

import gc
from typing import Any

from .._decor8 import transform_method
from ..result import ResultStatus, TransformResult
from ._builder import BronzeStage
from ._registry import register_pipeline


def execute_bronze_stage(
    stage: BronzeStage, runner: Any, logger: Any, force: bool = False
) -> tuple[str, TransformResult]:
    """Execute a single bronze parsing stage with logging."""
    logger.info(f"[{stage.group.upper()}] Stage {stage.order}: {stage.name}")
    if stage.description:
        logger.info(f"  {stage.description}")

    try:
        result = runner.transform_schema(stage.name, force=force)
        status_icon = "[OK]" if result.success else "⚠"
        logger.info(f"  {status_icon} {stage.name} → {result.message}")
        gc.collect()

        return (stage.name, result)
    except Exception as e:
        if stage.optional:
            logger.warning(f"  ⚠ {stage.name} skipped (optional): {e}")
            result = TransformResult(
                status=ResultStatus.SKIPPED,
                message=f"Optional stage skipped: {e}",
            )
            gc.collect()
            return (stage.name, result)
        else:
            logger.error(f"  [ERROR] {stage.name} failed: {e}")
            gc.collect()
            raise


@register_pipeline(name="cclf_bronze")
@transform_method(
    enable_composition=False,
    threshold=30.0,
)
def apply_cclf_bronze_pipeline(
    executor: Any, logger: Any, force: bool = False
) -> dict[str, TransformResult]:
    """
    Apply CCLF bronze transformations to parse raw files into parquet.

    Pipeline Order:
        Stage 1-2:   Core Metadata (CCLF0 summary, Management report)
        Stage 3-10:  Claims Data (CCLF1-7: Institutional, Physician, DME, Pharmacy)
        Stage 11-14: Demographics & Enrollment (CCLF8-9, CCLFA-B)

    Args:
        executor: Executor instance (TransformRunner) with storage and catalog access
        logger: Logger instance for recording operations
        force: Force reprocessing regardless of tracking state

    Returns:
        dict[str, TransformResult]: Dictionary mapping schema names to TransformResults
    """
    results = {}
    stages = [
        BronzeStage(
            name="cclf0",
            group="metadata",
            order=1,
            description="CCLF0 Summary Statistics - Record counts for all CCLF files",
        ),
        BronzeStage(
            name="cclf_management_report",
            group="metadata",
            order=2,
            description="CCLF Management Report - File delivery and processing status",
            optional=True,
        ),
        BronzeStage(
            name="cclf1",
            group="claims_institutional",
            order=3,
            description="CCLF1 Part A Claims Header - Institutional inpatient claims",
        ),
        BronzeStage(
            name="cclf2",
            group="claims_institutional",
            order=4,
            description="CCLF2 Part A Claims Revenue Center Detail - Line-level revenue codes",
        ),
        BronzeStage(
            name="cclf3",
            group="claims_institutional",
            order=5,
            description="CCLF3 Part A Procedure Code - ICD procedure codes for institutional",
        ),
        BronzeStage(
            name="cclf5",
            group="claims_physician",
            order=6,
            description="CCLF5 Part B Physicians - Carrier claim header and line",
        ),
        BronzeStage(
            name="cclf6",
            group="claims_physician",
            order=7,
            description="CCLF6 Part B DME - Durable medical equipment claims",
        ),
        BronzeStage(
            name="cclf7",
            group="claims_pharmacy",
            order=8,
            description="CCLF7 Part D - Prescription drug events (PDE)",
        ),
        BronzeStage(
            name="cclf4",
            group="diagnosis",
            order=9,
            description="CCLF4 Part A Diagnosis Code - ICD diagnosis for institutional",
        ),
        BronzeStage(
            name="cclf8",
            group="demographics",
            order=12,
            description="CCLF8 Beneficiary Demographics - Member demographic data",
        ),
        BronzeStage(
            name="cclf9",
            group="enrollment",
            order=13,
            description="CCLF9 Beneficiary XREF - MBI crosswalk for beneficiary ID changes",
        ),
        BronzeStage(
            name="cclfa",
            group="enrollment",
            order=14,
            description="CCLFA Beneficiary Enrollment - Monthly enrollment periods",
        ),
        BronzeStage(
            name="cclfb",
            group="enrollment",
            order=15,
            description="CCLFB Beneficiary Exclusions - Data sharing opt-outs",
            optional=True,
        ),
    ]

    logger.info(f"Starting CCLF Bronze Pipeline: {len(stages)} parsing stages")
    if force:
        logger.info("⚠ FORCE MODE: Reprocessing all files regardless of tracking state")
    logger.info("=" * 80)

    for stage in sorted(stages, key=lambda s: s.order):
        stage_name, stage_result = execute_bronze_stage(stage, executor, logger, force=force)
        results[stage_name] = stage_result

    logger.info("=" * 80)

    successful = sum(1 for r in results.values() if r.success)
    skipped = sum(1 for r in results.values() if r.status.value == "skipped")
    failed = sum(1 for r in results.values() if not r.success and r.status.value != "skipped")

    logger.info(
        f"[OK] CCLF Bronze Pipeline Complete: {successful} parsed, {skipped} skipped, {failed} failed"
    )

    return results
