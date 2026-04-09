# © 2025 HarmonyCares
# All rights reserved.

"""
Medicare Physician Fee Schedule (PFS) Payment Rates Pipeline

Pipeline Overview
=================

This pipeline calculates Medicare payment rates for physician services across all
HarmonyCares office locations using the Resource-Based Relative Value Scale (RBRVS)
methodology. The output provides expected Medicare reimbursement amounts for specified
HCPCS codes at each office, accounting for geographic cost variations.

Purpose and Applications
========================

Medicare pays for physician services using a formula that adjusts national relative
values for local cost differences. This pipeline automates that calculation to support:

1. **Financial Planning**: Predict Medicare revenue for specific services
2. **Budget Analysis**: Calculate expected payments based on utilization patterns
3. **Geographic Analysis**: Compare payment rates across office locations
4. **Trend Analysis**: Track year-over-year payment changes
5. **Contract Evaluation**: Compare Medicare rates to other payer rates

The pipeline produces a comprehensive rates table suitable for integration with claims
data, utilization forecasts, and financial modeling systems.

Medicare Payment Methodology
=============================

Background: Resource-Based Relative Value Scale (RBRVS)
--------------------------------------------------------

Since 1992, Medicare has paid for physician services using RBRVS, which assigns
relative values to each CPT/HCPCS code based on resources required. The system
recognizes three resource types:

1. **Physician Work**: Time, skill, training, intensity, and risk
2. **Practice Expense**: Overhead costs like staff, rent, equipment, supplies
3. **Malpractice**: Professional liability insurance premiums

Each resource type has:
- A national relative value (RVU) that is the same everywhere
- A geographic adjustment factor (GPCI) that varies by locality

Payment Formula
---------------

The Medicare payment for any service is calculated as:

    Payment = [(Work_RVU × Work_GPCI) + (PE_RVU × PE_GPCI) + (MP_RVU × MP_GPCI)] × CF

Where:
- RVUs measure national resource intensity (dimensionless relative values)
- GPCIs adjust for local cost variations (typically range 0.70 to 1.50)
- CF (Conversion Factor) translates adjusted RVUs into dollars (e.g., $34.61 for 2026)

Example Calculation
-------------------

Consider HCPCS 99347 (home visit, established patient, 25 min) in Manhattan, NY:

**Step 1: National RVUs** (same everywhere)
- Work RVU = 1.92
- Non-Facility PE RVU = 1.31
- Malpractice RVU = 0.15

**Step 2: Manhattan GPCIs** (locality-specific, carrier 15999, locality 00)
- Work GPCI = 1.088 (wages 8.8% above national average)
- PE GPCI = 1.459 (office costs 45.9% above average)
- MP GPCI = 1.494 (insurance 49.4% above average)

**Step 3: Geographic Adjustment**
- Work component = 1.92 × 1.088 = 2.089
- PE component = 1.31 × 1.459 = 1.911
- MP component = 0.15 × 1.494 = 0.224
- Total adjusted RVU = 4.224

**Step 4: Dollar Conversion**
- Payment = 4.224 × $34.6062 = $146.17

**Comparison: Rural Alabama** (carrier 00510, locality 01)
- GPCIs: Work=0.966, PE=0.871, MP=0.525
- Total adjusted RVU = 3.072
- Payment = 3.072 × $34.6062 = $106.31
- **Geographic difference: $39.86 (37.5% higher in Manhattan)**

This demonstrates how geographic adjustments create substantial payment variations
for identical services performed in different locations.

Pipeline Data Flow
==================

The pipeline orchestrates a multi-stage data integration and calculation process:

Stage 1: Data Prerequisites Validation
---------------------------------------

Before calculation begins, the pipeline verifies that all required reference tables
exist in the bronze data layer:

- **office_zip.parquet**: Maps office locations to ZIP codes
- **cms_geo_zips.parquet**: Maps ZIP codes to Medicare localities
- **gpci_inputs.parquet**: Contains GPCI values for each locality
- **pprvu_inputs.parquet**: Contains RVU values for each HCPCS code

Missing prerequisites trigger a clear error message identifying which tables are
needed and how to generate them.

Stage 2: Configuration Building
--------------------------------

The pipeline translates user parameters into a validated configuration object:

- **HCPCS Code Selection**: Either user-provided list or predefined home visit codes
- **Year Selection**: Target payment year (defaults to most recent available)
- **Comparison Year**: Prior year for trend analysis (defaults to target year - 1)
- **Facility Setting**: Non-facility (office) or facility (hospital) rates
- **Conversion Factor**: CMS-published or user-override value

Configuration validation ensures required parameters are provided and values are
within acceptable ranges.

Stage 3: Geographic Mapping
----------------------------

The transform execution begins by establishing geographic relationships:

**Office → ZIP → Locality → GPCI**

Each office location is mapped through its ZIP code to a Medicare locality, which
determines the applicable GPCI values. This mapping is year-specific because:
- Locality boundaries can change
- ZIP codes can be reassigned
- New localities can be created

Offices without locality mappings default to national average GPCIs (1.00) to
prevent calculation failures while flagging data quality issues.

Stage 4: Service Definition
----------------------------

HCPCS codes are matched to their RVU values from the Physician Fee Schedule:

**HCPCS → RVUs (Work, PE, MP)**

Each code retrieves three RVU components plus metadata (description, status flags).
Practice Expense RVUs vary by setting:
- Non-Facility: Higher values reflecting full office overhead
- Facility: Lower values since hospital provides overhead

Missing codes are logged and omitted from output, typically indicating:
- Codes not separately payable under Medicare
- Codes not yet effective for the selected year
- Bundled services paid as part of other codes

Stage 5: Cartesian Product
---------------------------

The pipeline creates all combinations of offices and HCPCS codes:

**N offices × M codes = N×M calculations**

This ensures complete coverage - every specified code is evaluated at every office
location. For a typical deployment with 50 offices and 12 home visit codes, this
produces 600 unique payment scenarios.

Stage 6: Payment Calculation
-----------------------------

For each office × code combination, the Medicare payment formula is applied:

1. Multiply each RVU by its corresponding GPCI (geographic adjustment)
2. Sum the three adjusted components (work + PE + MP)
3. Multiply by the conversion factor (translate to dollars)

This produces both component-level detail (work payment, PE payment, MP payment)
and the final total payment rate.

Stage 7: Year-Over-Year Comparison (Future Enhancement)
--------------------------------------------------------

When a prior year is specified, the pipeline will repeat the calculation using
prior year data and compute changes:

**Dollar Change** = Current Payment - Prior Payment
**Percent Change** = (Change / Prior Payment) × 100

This reveals payment trends driven by:
- CMS revaluation of specific services (RVU changes)
- Geographic cost data updates (GPCI changes)
- Budget neutrality adjustments (CF changes)
- Policy changes (setting reclassifications)

Stage 8: Output Generation
---------------------------

The final gold-tier table contains one row per:
    (HCPCS code, Office location, Year, Facility type)

This structure supports:
- **Lookup queries**: "What does Medicare pay for 99347 at the Boston office?"
- **Geographic analysis**: "How do home visit rates vary across our markets?"
- **Trend analysis**: "How did 99347 payments change from 2025 to 2026?"
- **Revenue projections**: "Expected Medicare revenue given utilization forecasts"

Pipeline Execution
==================

Command-Line Interface
----------------------

The pipeline is invoked via the ACO CLI:

    # Calculate home visit codes for most recent year
    aco pipeline pfs_rates --use-home-visits

    # Calculate specific codes for 2026 with 2025 comparison
    aco pipeline pfs_rates \\
        --hcpcs 99341,99342,99347 \\
        --year 2026 \\
        --compare-year 2025

    # Calculate both facility and non-facility rates
    aco pipeline pfs_rates \\
        --use-home-visits \\
        --facility-type both

    # Override conversion factor
    aco pipeline pfs_rates \\
        --use-home-visits \\
        --conversion-factor 34.6062

Programmatic Interface
----------------------

The pipeline can also be invoked programmatically:

    executor.run_pipeline(
        "pfs_rates",
        use_home_visits=True,
        year=2026,
        compare_year=2025
    )

Pipeline Output
===============

Summary Statistics
------------------

Upon completion, the pipeline logs:
- Total rows generated (office × code combinations)
- Unique HCPCS codes calculated
- Unique office locations included
- Payment rate statistics (mean, min, max)

Example output:
    Total rows: 600 (50 offices × 12 codes)
    Unique HCPCS codes: 12
    Unique offices: 50
    Avg payment rate: $127.45
    Min payment rate: $89.23
    Max payment rate: $168.91

Output File Location
--------------------

Results are written to the gold data layer:
    /opt/s3/data/workspace/gold/pfs_rates.parquet

The file is partitioned by year for efficient querying of specific time periods.

Data Quality Considerations
============================

Missing Data Handling
---------------------

**Unmapped Localities**: Offices whose ZIP codes don't map to Medicare localities
receive national average GPCIs (1.00), allowing calculations to proceed while
flagging the data quality issue for investigation.

**Missing GPCI Values**: Null or zero GPCI values are defaulted to 1.00, preventing
calculation failures. These are logged for data steward review.

**Missing HCPCS Codes**: Codes not found in the RVU file are omitted from output
and logged. This is expected for codes that are bundled, non-payable, or not yet
effective.

Validation Checks
-----------------

The pipeline performs several validation checks:
- RVU values must be >= 0
- GPCI values should be in reasonable range (0.7 to 1.5)
- Conversion factor should be reasonable ($30-$50 for recent years)
- Calculated payment must equal formula result (numerical precision check)

Year-Specific Data
------------------

All reference data (RVUs, GPCIs, ZIP mappings) is year-specific. The pipeline
automatically selects data matching the specified year. Using data from the wrong
year will produce incorrect payment calculations.

References and Regulations
===========================

Legal Framework
---------------
- **42 CFR 414.22**: Relative value units (RVUs)
- **42 CFR 414.26**: Geographic adjustment factors (GPCIs)
- **Social Security Act §1848**: Payment for physicians' services

CMS Publications
----------------
- **Physician Fee Schedule Final Rules**: Published annually, typically November
- **Addenda Files**: Contain RVU and GPCI values
- **Federal Register**: Contains regulatory text and preambles

Data Sources
------------
- **PPRVU File**: RVU values for all HCPCS codes
- **GPCI File**: Geographic adjustment factors by locality
- **ZIP File**: ZIP code to locality crosswalk

Available at: https://www.cms.gov/medicare/payment/fee-schedules/physician
"""

from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import transform_method
from .._expressions._pfs_rate_calc import PFSRateCalcConfig
from ._registry import register_pipeline
from ._stage import PipelineStage, execute_stage


@register_pipeline(name="pfs_rates")
@transform_method(
    enable_composition=False,
    threshold=30.0,
)
def apply_pfs_rates_pipeline(
    executor: Any,
    logger: Any,
    force: bool = False,
    hcpcs_codes: list[str] | None = None,
    use_home_visits: bool = False,
    year: int | None = None,
    compare_year: int | None = None,
    facility_type: str = "non_facility",
    conversion_factor: float | None = None,
) -> dict[str, pl.LazyFrame]:
    """
    Calculate Medicare PFS payment rates for HCPCS codes across office locations.

    This pipeline generates a gold-tier rates table by:
    1. Loading office locations and mapping to Medicare localities
    2. Loading GPCI (geographic adjustment) data
    3. Loading RVU (relative value unit) data for HCPCS codes
    4. Calculating geographically adjusted payment rates
    5. Optionally comparing to prior year rates

    Pipeline Order:
        Stage 1: pfs_rates - Calculate payment rates for all office/HCPCS combinations

    Prerequisites:
        - office_zip must exist in bronze layer
        - cms_geo_zips must exist in bronze layer
        - gpci_inputs must exist in bronze layer
        - pprvu_inputs must exist in bronze layer

    Args:
        executor: TransformRunner instance with storage_config and catalog access
        logger: Logger instance for recording operations
        force: Force recalculation even if data exists
        hcpcs_codes: List of HCPCS codes to calculate (e.g., ["99341", "99342"])
        use_home_visits: Use predefined home visit code list
        year: Target payment year (None = most recent available)
        compare_year: Prior year for comparison (None = year - 1)
        facility_type: Setting type ("non_facility", "facility", "both")
        conversion_factor: Override conversion factor (None = use from data)

    Returns:
        dict[str, pl.LazyFrame]: Dictionary mapping output names to LazyFrames

    Raises:
        FileNotFoundError: If prerequisite tables do not exist
        ValueError: If neither hcpcs_codes nor use_home_visits is specified

    """
    from .._transforms._pfs_rates import calculate_pfs_rates
    from ..medallion import MedallionLayer

    # Verify prerequisites exist
    storage = executor.storage_config
    bronze_path = storage.get_path(MedallionLayer.BRONZE)
    gold_path = storage.get_path(MedallionLayer.GOLD)

    # Check bronze layer prerequisites
    prerequisites = [
        ("office_zip.parquet", "office location data"),
        ("cms_geo_zips.parquet", "CMS geographic ZIP codes"),
        ("gpci_inputs.parquet", "Geographic Practice Cost Indices"),
        ("pprvu_inputs.parquet", "Physician Fee Schedule RVUs"),
    ]

    missing = []
    for filename, description in prerequisites:
        file_path = bronze_path / filename
        if not Path(file_path).exists():
            missing.append(f"{description} ({filename})")

    if missing:
        error_msg = (
            "Missing prerequisite files:\n"
            + "\n".join(f"  - {item}" for item in missing)
            + "\n\nRun the appropriate transforms to generate these files."
        )
        logger.error(error_msg)
        raise FileNotFoundError(error_msg)

    # Build configuration
    config = PFSRateCalcConfig(
        hcpcs_codes=hcpcs_codes or [],
        use_home_visit_codes=use_home_visits,
        year=year,
        prior_year=compare_year,
        facility_type=facility_type,
        conversion_factor=conversion_factor,
        include_comparison=(compare_year is not None or year is not None),
    )

    # Validate configuration
    if not config.hcpcs_codes and not config.use_home_visit_codes:
        error_msg = (
            "Must specify either hcpcs_codes or use_home_visits=True\n\n"
            "Examples:\n"
            "  aco pipeline pfs_rates --use-home-visits\n"
            "  aco pipeline pfs_rates --hcpcs 99341,99342,99347"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)

    logger.info("PFS Rates Pipeline Configuration:")
    logger.info(
        f"  HCPCS codes: {len(config.hcpcs_codes) if not use_home_visits else 'Home Visit Codes'}"
    )
    logger.info(f"  Target year: {config.year or 'Most Recent'}")
    logger.info(f"  Compare year: {config.prior_year or 'Previous Year'}")
    logger.info(f"  Facility type: {config.facility_type}")
    if config.conversion_factor:
        logger.info(f"  Conversion factor: ${config.conversion_factor:.2f}")

    # Create pipeline stage
    # Note: We're creating a pseudo-module object to work with the stage pattern
    class PFSRatesModule:
        """Pseudo-module for PFS rates transform."""

        @staticmethod
        def apply_transform(df, schema, catalog, logger, force=False):
            return calculate_pfs_rates(df, schema, catalog, logger, config=config, force=force)

    stages = [
        PipelineStage(
            name="pfs_rates",
            module=PFSRatesModule,
            group="gold",
            order=1,
            depends_on=[],  # Generative transform - no dependencies
        ),
    ]

    from ._checkpoint import PipelineCheckpoint

    pipeline_start = datetime.now()
    checkpoint = PipelineCheckpoint("pfs_rates", force=force)

    logger.info(f"Starting PFS Rates Pipeline: {len(stages)} table")
    logger.info("=" * 80)
    checkpoint.log_resume_info(logger, len(stages))

    # Execute stages with checkpoint/resume
    for stage in sorted(stages, key=lambda s: s.order):
        output_file = gold_path / f"{stage.name}.parquet"

        # Check if we should skip this stage
        should_skip, row_count = checkpoint.should_skip_stage(stage.name, output_file, logger)

        if should_skip:
            # Stage already completed in previous run
            logger.info(f"[{stage.group.upper()}] Stage {stage.order}: {stage.name}")
            checkpoint.completed_stages.append(stage.name)
            continue

        try:
            # Execute the stage
            _, _ = execute_stage(stage, executor, logger, gold_path)
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

    logger.info("=" * 80)
    logger.info("Counting final row counts...")
    total_rows = 0
    for stage_name in checkpoint.completed_stages:
        file_path = gold_path / f"{stage_name}.parquet"
        if Path(file_path).exists():
            row_count = pl.scan_parquet(file_path).select(pl.len()).collect().item()
            total_rows += row_count
            logger.info(f"  {stage_name}: {row_count:,} rows")

            # Show summary statistics
            stats = (
                pl.scan_parquet(file_path)
                .select(
                    [
                        pl.col("hcpcs_code").n_unique().alias("unique_hcpcs"),
                        pl.col("office_name").n_unique().alias("unique_offices"),
                        pl.col("payment_rate").mean().alias("avg_rate"),
                        pl.col("payment_rate").min().alias("min_rate"),
                        pl.col("payment_rate").max().alias("max_rate"),
                    ]
                )
                .collect()
            )

            logger.info(f"    Unique HCPCS codes: {stats['unique_hcpcs'][0]}")
            logger.info(f"    Unique offices: {stats['unique_offices'][0]}")
            logger.info(f"    Avg payment rate: ${stats['avg_rate'][0]:.2f}")
            logger.info(f"    Min payment rate: ${stats['min_rate'][0]:.2f}")
            logger.info(f"    Max payment rate: ${stats['max_rate'][0]:.2f}")

    pipeline_elapsed = (datetime.now() - pipeline_start).total_seconds()
    logger.info("=" * 80)
    logger.info(f"[OK] PFS Rates Pipeline Complete: {len(checkpoint.completed_stages)} table generated")
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info(f"  Elapsed time: {pipeline_elapsed:.2f} seconds")
    logger.info(f"  Output: {gold_path}/pfs_rates.parquet")

    # Mark pipeline as complete - next run will start fresh
    checkpoint.mark_pipeline_complete(total_rows, pipeline_elapsed)

    # Return dict mapping stage names to parquet paths for compatibility
    return {name: gold_path / f"{name}.parquet" for name in checkpoint.completed_stages}
