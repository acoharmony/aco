# © 2025 HarmonyCares
# All rights reserved.

"""Reference data transformation pipeline for Tuva seeds."""

from typing import Any

from .._decor8 import transform_method
from ..result import ResultStatus, TransformResult
from ._registry import register_pipeline


@register_pipeline(name="reference_data")
@transform_method(
    enable_composition=False,
    threshold=60.0,
)
def apply_reference_data_pipeline(
    executor: Any, logger: Any, force: bool = False, overwrite: bool = False
) -> dict[str, TransformResult]:
    """
    Apply reference data transformation pipeline for all Tuva seeds.

    This pipeline downloads and converts ALL Tuva reference data (seeds) into
    the silver layer as parquet files. Reference data includes:

    - Terminology: ICD-10-CM, ICD-9-CM, CPT, HCPCS, NDC, etc.
    - Value Sets: CCSR, clinical groupings, quality measures
    - Risk Adjustment: CMS-HCC mappings
    - Data Quality: Validation rules and checks
    - Clinical: ED classification, clinical systems
    - Quality Measures: AHRQ quality measures
    - Reference: General lookup tables

    Pipeline Order:
        1. Parse dbt_project.yml to discover all seeds
        2. Download CSVs from S3 to bronze/tuva_seeds
        3. Convert CSVs to parquet in silver with flat naming

    Args:
        executor: Executor instance (TransformRunner) with storage and catalog access
        logger: Logger instance for recording operations
        force: Force reprocessing regardless of tracking state
        overwrite: Overwrite existing reference files

    Returns:
        dict[str, TransformResult]: Dictionary mapping flat_name to TransformResult
    """
    from .._transforms._reference import transform_all_reference_data

    logger.info("=" * 80)
    logger.info("REFERENCE DATA PIPELINE: Tuva Seeds → Silver")
    logger.info("=" * 80)
    logger.info("")
    logger.info("This pipeline will:")
    logger.info("  1. Parse Tuva dbt_project.yml to discover all seeds")
    logger.info("  2. Download CSVs from S3 (tuva-public-resources)")
    logger.info("  3. Convert to parquet in silver with flat naming")
    logger.info("")

    if force:
        overwrite = True
        logger.info("⚠ FORCE MODE: Redownloading and reconverting all reference data")
        logger.info("")

    results = transform_all_reference_data(executor, logger, overwrite=overwrite)

    logger.info("\n" + "=" * 80)
    logger.info("Results by category")
    logger.info("=" * 80)

    # Group results by category (extracted from flat_name prefix)
    by_category = {}
    for flat_name, result in results.items():
        category = flat_name.split("_")[0]
        if category not in by_category:
            by_category[category] = {"success": 0, "skipped": 0, "failed": 0}
        if result.status == ResultStatus.SUCCESS:
            by_category[category]["success"] += 1
        elif result.status == ResultStatus.SKIPPED:
            by_category[category]["skipped"] += 1
        else:
            by_category[category]["failed"] += 1

    for category in sorted(by_category.keys()):
        stats = by_category[category]
        total = stats["success"] + stats["skipped"] + stats["failed"]
        logger.info(
            f"  {category:20s}: {stats['success']:3d} success, "
            f"{stats['skipped']:3d} skipped, {stats['failed']:3d} failed (total: {total})"
        )

    logger.info("=" * 80)

    return results
