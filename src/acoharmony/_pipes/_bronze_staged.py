# © 2025 HarmonyCares
# All rights reserved.

"""Staged bronze layer pipeline for non-CCLF data sources."""

import gc
from typing import Any

from .._decor8 import transform_method
from ..result import TransformResult
from ._builder import BronzeStage
from ._cclf_bronze import execute_bronze_stage
from ._registry import register_pipeline


@register_pipeline(name="bronze_staged")
@transform_method(
    enable_composition=False,
    threshold=60.0,
)
def apply_bronze_staged_pipeline(
    executor: Any, logger: Any, force: bool = False
) -> dict[str, TransformResult]:
    """
    Apply staged bronze transformations for non-CCLF data sources.

    This pipeline processes all data sources EXCEPT CCLF files, which are
    handled separately by the cclf_bronze pipeline. Includes memory management
    with cache clearing and garbage collection between stages.

    Pipeline Order:
        Stage 1-5:   REACH Model Files (REACH ACO variant)
        Stage 6-32:  CMS Reports (quality, financial, benchmarks, alignment, specialized)
        Stage 33-34: Reference Data (zip_to_county)

    Args:
        executor: Executor instance (TransformRunner) with storage and catalog access
        logger: Logger instance for recording operations
        force: Force reprocessing regardless of tracking state

    Returns:
        dict[str, TransformResult]: Dictionary mapping schema names to TransformResults
    """
    results = {}
    stages = [
        # REACH Model Files (Stage 1-5)
        BronzeStage("reach_bnmr", "reach", 1, "REACH Beneficiary MBI Record", optional=True),
        BronzeStage("hdai_reach", "reach", 2, "REACH HDAI Data", optional=True),
        BronzeStage("shadow_bundle_reach", "reach", 3, "REACH Shadow Bundle", optional=True),
        BronzeStage("reach_calendar", "reach", 4, "ACO REACH Calendar", optional=True),

        # CMS Quality Reports (Stage 6-10)
        BronzeStage(
            "annual_quality_report",
            "reports_quality",
            6,
            "Annual Quality Report (AQR)",
            optional=True,
        ),
        BronzeStage(
            "quarterly_quality_report",
            "reports_quality",
            7,
            "Quarterly Quality Report (QQR)",
            optional=True,
        ),
        BronzeStage(
            "annual_beneficiary_level_quality_report",
            "reports_quality",
            8,
            "Annual Beneficiary-Level Quality",
            optional=True,
        ),
        BronzeStage(
            "quarterly_beneficiary_level_quality_report",
            "reports_quality",
            9,
            "Quarterly Beneficiary-Level Quality",
            optional=True,
        ),
        BronzeStage(
            "sbqr", "reports_quality", 10, "Shared Beneficiary Quality Report", optional=True
        ),

        # CMS Financial Reports (Stage 11-17)
        BronzeStage(
            "preliminary_benchmark_report_for_dc",
            "reports_financial",
            11,
            "Preliminary Benchmark Report (DC)",
            optional=True,
        ),
        BronzeStage(
            "preliminary_benchmark_report_unredacted",
            "reports_financial",
            12,
            "Preliminary Benchmark Report (Unredacted)",
            optional=True,
        ),
        BronzeStage(
            "alternative_payment_arrangement_report",
            "reports_financial",
            13,
            "Alternative Payment Arrangement Report",
            optional=True,
        ),
        BronzeStage(
            "preliminary_alternative_payment_arrangement_report_156",
            "reports_financial",
            14,
            "Preliminary APA Report 156",
            optional=True,
        ),
        BronzeStage(
            "mexpr",
            "reports_financial",
            15,
            "Monthly Expenditure Report",
            optional=True,
        ),
        BronzeStage(
            "prospective_plus_opportunity_report",
            "reports_financial",
            16,
            "Prospective Plus Opportunity",
            optional=True,
        ),
        BronzeStage(
            "estimated_cisep_change_threshold_report",
            "reports_financial",
            17,
            "Estimated CISEP Change Threshold",
            optional=True,
        ),
        BronzeStage(
            "aco_financial_guarantee_amount",
            "reports_financial",
            18,
            "ACO Financial Guarantee Amount",
            optional=True,
        ),

        # CMS Alignment Reports (Stage 18-21)
        BronzeStage(
            "preliminary_alignment_estimate",
            "reports_alignment",
            19,
            "Preliminary Alignment Estimate",
            optional=True,
        ),
        BronzeStage("alr", "reports_alignment", 20, "Assignment List Report (ALR)", optional=True),
        BronzeStage(
            "bar", "reports_alignment", 21, "Benchmark Assignment Report (BAR)", optional=True
        ),
        BronzeStage("pbvar", "reports_alignment", 22, "PBP Variance Report", optional=True),

        # CMS Specialized Reports (Stage 22-32)
        BronzeStage(
            "risk_adjustment_data",
            "reports_specialized",
            23,
            "Risk Adjustment Data (RAD)",
            optional=True,
        ),
        BronzeStage(
            "rap", "reports_specialized", 24, "Risk Adjustment Processing (RAP)", optional=True
        ),
        BronzeStage(
            "pecos_terminations_monthly_report",
            "reports_specialized",
            25,
            "PECOS Terminations Monthly",
            optional=True,
        ),
        BronzeStage(
            "plaru",
            "reports_specialized",
            26,
            "Provider-Level Attribution Report Upload",
            optional=True,
        ),
        BronzeStage(
            "palmr",
            "reports_specialized",
            27,
            "Provider Attribution List Monthly Report",
            optional=True,
        ),
        BronzeStage(
            "sbnabp", "reports_specialized", 28, "Shared Beneficiary NAB Period", optional=True
        ),
        BronzeStage(
            "pyred", "reports_specialized", 29, "Prior Year Reconciliation Detail", optional=True
        ),
        BronzeStage(
            "tparc",
            "reports_specialized",
            30,
            "Third Party Administrator Reconciliation",
            optional=True,
        ),
        BronzeStage(
            "beneficiary_hedr_transparency_files",
            "reports_specialized",
            31,
            "Beneficiary HEDR Transparency",
            optional=True,
        ),
        BronzeStage(
            "beneficiary_data_sharing_exclusion_file",
            "reports_specialized",
            32,
            "Beneficiary Data Sharing Exclusion",
            optional=True,
        ),

        # Shared Beneficiary Monthly Reports (Stage 33-40)
        BronzeStage("sbmdm", "reports_sbm", 33, "SBM DME (Durable Medical Equipment)", optional=True),
        BronzeStage("sbmepi", "reports_sbm", 34, "SBM Episode (Bundled Episodes)", optional=True),
        BronzeStage("sbmhh", "reports_sbm", 35, "SBM Home Health", optional=True),
        BronzeStage("sbmhs", "reports_sbm", 36, "SBM Hospice", optional=True),
        BronzeStage("sbmip", "reports_sbm", 37, "SBM Inpatient", optional=True),
        BronzeStage("sbmopl", "reports_sbm", 38, "SBM Outpatient", optional=True),
        BronzeStage("sbmpb", "reports_sbm", 39, "SBM Part B Professional", optional=True),
        BronzeStage("sbmsn", "reports_sbm", 40, "SBM SNF (Skilled Nursing Facility)", optional=True),

        # Reference Data (Stage 41-42)
        BronzeStage("zip_to_county", "reference", 41, "ZIP to County Crosswalk", optional=True),
    ]

    logger.info(f"Starting Bronze Staged Pipeline (Non-CCLF): {len(stages)} parsing stages")
    if force:
        logger.info("⚠ FORCE MODE: Reprocessing all files regardless of tracking state")
    logger.info("=" * 80)

    prev_stage_name = None
    for stage in sorted(stages, key=lambda s: s.order):
        # Clear previous stage's result from memory before starting new stage
        if prev_stage_name and prev_stage_name in results:
            results[prev_stage_name] = None  # Release TransformResult reference
            gc.collect()

        stage_name, stage_result = execute_bronze_stage(stage, executor, logger, force=force)
        results[stage_name] = stage_result
        prev_stage_name = stage_name

    # Final cleanup after all stages complete
    for name in results:
        if results[name] is not None:
            results[name] = None
    gc.collect()

    logger.info("=" * 80)
    successful = sum(1 for r in results.values() if r and r.success)
    skipped = sum(1 for r in results.values() if r and r.status.value == "skipped")
    failed = sum(1 for r in results.values() if r and not r.success and r.status.value != "skipped")

    logger.info(
        f"[OK] Bronze Staged Pipeline Complete: {successful} parsed, {skipped} skipped, {failed} failed"
    )

    return results
