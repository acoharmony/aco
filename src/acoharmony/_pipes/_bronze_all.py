# © 2025 HarmonyCares
# All rights reserved.

"""Comprehensive bronze layer pipeline for all data sources."""

from typing import Any

from .._decor8 import transform_method
from ..result import TransformResult
from ._builder import BronzeStage
from ._cclf_bronze import execute_bronze_stage
from ._registry import register_pipeline


@register_pipeline(name="bronze_all")
@transform_method(
    enable_composition=False,
    threshold=60.0,
)
def apply_bronze_all_pipeline(
    executor: Any, logger: Any, force: bool = False
) -> dict[str, TransformResult]:
    """
    Apply comprehensive bronze transformations for ALL data sources.

    Pipeline Order:
        Stage 1-15:  CCLF Files (core ACO claims and demographics)
        Stage 16-20: REACH Model Files (REACH ACO variant)
        Stage 21-40: CMS Reports (quality, financial, benchmarks)
        Stage 41-50: Reference Data (crosswalks, lookups, metadata)

    Args:
        executor: Executor instance (TransformRunner) with storage and catalog access
        logger: Logger instance for recording operations
        force: Force reprocessing regardless of tracking state

    Returns:
        dict[str, TransformResult]: Dictionary mapping schema names to TransformResults
    """
    results = {}
    stages = [
        BronzeStage("cclf0", "cclf_metadata", 1, "CCLF0 Summary Statistics"),
        BronzeStage(
            "cclf_management_report", "cclf_metadata", 2, "CCLF Management Report", optional=True
        ),
        BronzeStage("cclf1", "cclf_institutional", 3, "CCLF1 Part A Claims Header"),
        BronzeStage("cclf2", "cclf_institutional", 4, "CCLF2 Part A Revenue Center Detail"),
        BronzeStage("cclf3", "cclf_institutional", 5, "CCLF3 Part A Procedure Code"),
        BronzeStage("cclf4", "cclf_diagnosis", 6, "CCLF4 Part A Diagnosis Code"),
        BronzeStage("cclf5", "cclf_physician", 7, "CCLF5 Part B Physicians"),
        BronzeStage("cclf6", "cclf_dme", 8, "CCLF6 Part B DME"),
        BronzeStage("cclf7", "cclf_pharmacy", 9, "CCLF7 Part D Pharmacy"),
        BronzeStage("cclf8", "cclf_demographics", 10, "CCLF8 Beneficiary Demographics"),
        BronzeStage("cclf9", "cclf_enrollment", 11, "CCLF9 Beneficiary XREF"),
        BronzeStage("cclfa", "cclf_enrollment", 12, "CCLFA Beneficiary Enrollment"),
        BronzeStage("cclfb", "cclf_enrollment", 13, "CCLFB Beneficiary Exclusions", optional=True),
        BronzeStage("reach_bnmr", "reach", 16, "REACH Beneficiary MBI Record", optional=True),
        BronzeStage("hdai_reach", "reach", 17, "REACH HDAI Data", optional=True),
        BronzeStage("shadow_bundle_reach", "reach", 18, "REACH Shadow Bundle", optional=True),
        BronzeStage(
            "annual_quality_report",
            "reports_quality",
            21,
            "Annual Quality Report (AQR)",
            optional=True,
        ),
        BronzeStage(
            "quarterly_quality_report",
            "reports_quality",
            22,
            "Quarterly Quality Report (QQR)",
            optional=True,
        ),
        BronzeStage(
            "annual_beneficiary_level_quality_report",
            "reports_quality",
            23,
            "Annual Beneficiary-Level Quality",
            optional=True,
        ),
        BronzeStage(
            "quarterly_beneficiary_level_quality_report",
            "reports_quality",
            24,
            "Quarterly Beneficiary-Level Quality",
            optional=True,
        ),
        BronzeStage(
            "sbqr", "reports_quality", 25, "Shared Beneficiary Quality Report", optional=True
        ),
        BronzeStage(
            "preliminary_benchmark_report_for_dc",
            "reports_financial",
            29,
            "Preliminary Benchmark Report (DC)",
            optional=True,
        ),
        BronzeStage(
            "preliminary_benchmark_report_unredacted",
            "reports_financial",
            30,
            "Preliminary Benchmark Report (Unredacted)",
            optional=True,
        ),
        BronzeStage(
            "alternative_payment_arrangement_report",
            "reports_financial",
            31,
            "Alternative Payment Arrangement Report",
            optional=True,
        ),
        BronzeStage(
            "preliminary_alternative_payment_arrangement_report_156",
            "reports_financial",
            32,
            "Preliminary APA Report 156",
            optional=True,
        ),
        BronzeStage(
            "mexpr",
            "reports_financial",
            33,
            "Monthly Expenditure Report",
            optional=True,
        ),
        BronzeStage(
            "prospective_plus_opportunity_report",
            "reports_financial",
            34,
            "Prospective Plus Opportunity",
            optional=True,
        ),
        BronzeStage(
            "estimated_cisep_change_threshold_report",
            "reports_financial",
            35,
            "Estimated CISEP Change Threshold",
            optional=True,
        ),
        BronzeStage(
            "aco_financial_guarantee_amount",
            "reports_financial",
            36,
            "ACO Financial Guarantee Amount",
            optional=True,
        ),
        BronzeStage(
            "preliminary_alignment_estimate",
            "reports_alignment",
            37,
            "Preliminary Alignment Estimate",
            optional=True,
        ),
        BronzeStage("alr", "reports_alignment", 38, "Assignment List Report (ALR)", optional=True),
        BronzeStage(
            "bar", "reports_alignment", 39, "Benchmark Assignment Report (BAR)", optional=True
        ),
        BronzeStage("pbvar", "reports_alignment", 40, "PBP Variance Report", optional=True),
        BronzeStage(
            "risk_adjustment_data",
            "reports_specialized",
            43,
            "Risk Adjustment Data (RAD)",
            optional=True,
        ),
        BronzeStage(
            "rap", "reports_specialized", 44, "Risk Adjustment Processing (RAP)", optional=True
        ),
        BronzeStage(
            "pecos_terminations_monthly_report",
            "reports_specialized",
            45,
            "PECOS Terminations Monthly",
            optional=True,
        ),
        BronzeStage(
            "plaru",
            "reports_specialized",
            46,
            "Provider-Level Attribution Report Upload",
            optional=True,
        ),
        BronzeStage(
            "palmr",
            "reports_specialized",
            47,
            "Provider Attribution List Monthly Report",
            optional=True,
        ),
        BronzeStage(
            "sbnabp", "reports_specialized", 48, "Shared Beneficiary NAB Period", optional=True
        ),
        BronzeStage(
            "pyred", "reports_specialized", 49, "Prior Year Reconciliation Detail", optional=True
        ),
        BronzeStage(
            "tparc",
            "reports_specialized",
            50,
            "Third Party Administrator Reconciliation",
            optional=True,
        ),
        BronzeStage(
            "beneficiary_hedr_transparency_files",
            "reports_specialized",
            51,
            "Beneficiary HEDR Transparency",
            optional=True,
        ),
        BronzeStage(
            "beneficiary_data_sharing_exclusion_file",
            "reports_specialized",
            52,
            "Beneficiary Data Sharing Exclusion",
            optional=True,
        ),
        BronzeStage("census", "reference", 53, "Census Data", optional=True),
        BronzeStage("zip_to_county", "reference", 54, "ZIP to County Crosswalk", optional=True),
        BronzeStage("office_zip", "reference", 55, "Office ZIP Codes", optional=True),
    ]

    logger.info(f"Starting Full Bronze Pipeline: {len(stages)} parsing stages")
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
        f"[OK] Full Bronze Pipeline Complete: {successful} parsed, {skipped} skipped, {failed} failed"
    )

    return results
