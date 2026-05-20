# © 2025 HarmonyCares
# All rights reserved.

"""
Medicare Physician Fee Schedule (PFS) Payment Rate Calculation Transform

Mathematical Methodology
========================

This transform calculates Medicare payment rates for physician services by applying the
Resource-Based Relative Value Scale (RBRVS) methodology with geographic cost adjustments.
The result is a table showing expected Medicare reimbursement for each HCPCS code at each
office location.

Core Methodology
----------------

Medicare payment for physician services follows a multi-step calculation that combines:
1. National relative values (RVUs) for each service
2. Local cost adjustments (GPCIs) for each geographic area
3. A national conversion factor to translate relative values into dollars

The fundamental equation applied to each HCPCS code at each office location is:

    Payment = [(Work_RVU × Work_GPCI) + (PE_RVU × PE_GPCI) + (MP_RVU × MP_GPCI)] × CF

This transform implements this calculation by systematically:
- Determining the Medicare locality for each office location
- Retrieving the appropriate GPCI values for that locality
- Retrieving RVU values for each HCPCS code
- Computing the geographically adjusted payment for all combinations

Geographic Mapping Process
---------------------------

Step 1: Office Location Identification
Each office is identified by its ZIP code, which serves as the starting point for
geographic mapping. Office locations include metadata like state, market, and region
for downstream analysis.

Step 2: Medicare Locality Assignment
Medicare divides the United States into payment localities, each identified by a
carrier number and locality code. ZIP codes map to localities through the CMS
geographic ZIP file (cms_geo_zips). This mapping is year-specific because locality
boundaries can change.

Example: A Manhattan office (ZIP 10001) maps to carrier 15999, locality 00 (New York
County), while a rural Alabama office (ZIP 36003) maps to carrier 00510, locality 01.

Step 3: Geographic Practice Cost Index (GPCI) Retrieval
Each Medicare locality has three GPCI values reflecting local cost variations:
- Work GPCI: Adjusts for local physician compensation levels
- Practice Expense GPCI: Adjusts for local costs of rent, staff, equipment, supplies
- Malpractice GPCI: Adjusts for local professional liability insurance premiums

GPCIs are normalized to 1.00 at the national average. Higher-cost areas have GPCIs
above 1.00, lower-cost areas have GPCIs below 1.00.

For unmapped offices or missing GPCI data, the transform applies a default GPCI of
1.00 (national average), ensuring calculations proceed without data gaps.

Relative Value Unit (RVU) Assignment
-------------------------------------

Step 4: HCPCS Code RVU Retrieval
Each HCPCS code has three national RVU components representing resource intensity:

- Work RVU: Reflects physician time, effort, skill, and intensity required
- Practice Expense RVU: Reflects overhead costs (varies by facility setting)
- Malpractice RVU: Reflects professional liability risk

Practice Expense has two values:
- Non-Facility (NF_PE_RVU): Used when service is performed in physician office
- Facility (F_PE_RVU): Used when service is performed in hospital or facility

This transform defaults to non-facility settings but supports facility calculations
through configuration.

RVUs are the same nationwide - a home visit (99347) requires the same physician work
in Manhattan and Alabama. Geographic variation enters through GPCIs, not RVUs.

Payment Calculation
-------------------

Step 5: Cartesian Product Construction
The transform creates all combinations of offices and HCPCS codes through a cross join.
This ensures every specified HCPCS code is evaluated at every office location.

For N offices and M HCPCS codes, this produces N × M rows, each representing a unique
payment scenario.

Step 6: Component Payment Calculation
For each office × HCPCS combination, three payment components are calculated:

    Work Payment = Work_RVU × Work_GPCI
    PE Payment = PE_RVU × PE_GPCI
    MP Payment = MP_RVU × MP_GPCI

These three components are summed to produce the total geographically adjusted RVU:

    Total_Adjusted_RVU = Work_Payment + PE_Payment + MP_Payment

Step 7: Dollar Conversion
The total adjusted RVU is converted to a dollar payment rate by multiplying by the
annual conversion factor:

    Payment_Rate = Total_Adjusted_RVU × Conversion_Factor

The conversion factor is set annually by CMS through rulemaking and represents the
dollar value of one relative value unit. Recent values:
- 2024: $32.7476
- 2025: $33.2875
- 2026: $34.6062

Conversion factors can be provided via configuration or retrieved from RVU data files.

Year-Over-Year Comparison
--------------------------

Step 8: Prior Year Calculation (Future Enhancement)
To track payment changes over time, the transform supports calculating rates for a
prior year using the same methodology with prior year data:
- Prior year RVUs (which may differ if CMS revalued services)
- Prior year GPCIs (which change annually based on cost data)
- Prior year conversion factor

Step 9: Change Calculation
Dollar and percentage changes are computed:

    Dollar_Change = Current_Payment - Prior_Payment
    Percent_Change = (Dollar_Change / Prior_Payment) × 100

This reveals how payment rates evolved due to:
- RVU changes (service revaluation)
- GPCI changes (cost data updates)
- Conversion factor changes (Medicare budget adjustments)
- Budget neutrality adjustments (increases offset by decreases)

Output Structure
----------------

The transform produces a gold-tier table with one row per:
    (HCPCS code, Office location, Year, Facility type)

Each row contains:
- Service identifiers (HCPCS code, description)
- Location identifiers (office name, ZIP, state, carrier, locality)
- Input components (RVUs, GPCIs, conversion factor)
- Calculated payments (work, PE, MP components and total)
- Comparison data (prior year values and changes)
- Metadata (facility type, calculation date, market, region)

This structure supports multiple analytical use cases:
1. Payment rate lookup for specific procedures at specific offices
2. Geographic payment variation analysis across localities
3. Year-over-year payment trend analysis
4. Budget impact analysis for service utilization

Data Quality Considerations
----------------------------

Missing Locality Mappings: Some ZIP codes may not map to Medicare localities,
particularly newer ZIP codes not yet in CMS geographic files. These offices receive
national average GPCIs (1.00) for all components.

GPCI Validation: GPCI values are validated and defaulted to 1.00 if null or zero,
preventing calculation failures while flagging data quality issues.

HCPCS Code Availability: If requested HCPCS codes are not found in RVU files, they
are omitted from output. This typically indicates codes that are not separately
payable under Medicare or codes not yet effective.

Transform Characteristics
-------------------------

- **Type**: Generative (does not require input DataFrame)
- **Tier**: Gold (analytical output for business intelligence)
- **Idempotency**: Produces identical output for identical inputs
- **Composition**: Enabled (can be part of larger pipelines)
- **SQL**: Not enabled (uses Polars lazy evaluation)

The transform is deterministic except for the calculation_date timestamp, which
records when the calculation was performed.
"""

from datetime import datetime
from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from .._expressions._hcpcs_filter import HCPCSFilterExpression
from .._expressions._pfs_rate_calc import PFSRateCalcConfig, PFSRateCalcExpression


@transform(name="pfs_rates", tier=["gold"], sql_enabled=False)
@transform_method(enable_composition=True, threshold=30.0)
def calculate_pfs_rates(
    df: pl.LazyFrame | None,
    schema: dict,
    catalog: Any,
    logger: Any,
    config: PFSRateCalcConfig | None = None,
    force: bool = False,
) -> pl.LazyFrame:
    """
    Calculate Medicare PFS payment rates for HCPCS codes across office locations.

    This is a generative transform that doesn't require input data - it creates
    the rates table from scratch using reference data.

    Args:
        df: Ignored - this is a generative transform
        schema: PFS rates schema configuration
        catalog: Data catalog for accessing source tables
        logger: Logger instance
        config: PFS rate calculation configuration
        force: Force recalculation even if data exists

    Returns:
        LazyFrame with calculated payment rates by HCPCS code and office
    """
    logger.info("Starting PFS rate calculation transform")

    # Use default config if none provided
    if config is None:
        config = PFSRateCalcConfig()

    # Determine HCPCS codes to calculate
    if config.use_home_visit_codes:
        hcpcs_codes = HCPCSFilterExpression.home_visit_codes
        logger.info(f"Using home visit codes: {len(hcpcs_codes)} codes")
    elif config.hcpcs_codes:
        hcpcs_codes = config.hcpcs_codes
        logger.info(f"Using provided HCPCS codes: {len(hcpcs_codes)} codes")
    else:
        logger.error("No HCPCS codes specified - set hcpcs_codes or use_home_visit_codes=True")
        raise ValueError("Must specify HCPCS codes to calculate rates for")

    # Step 1: Load office locations
    logger.info("Loading office location data")
    office_zip_df = catalog.scan_table("office_zip")
    if office_zip_df is None:
        raise ValueError("office_zip table not found in catalog")

    # Select unique offices with their ZIP codes
    offices = (
        office_zip_df.filter(pl.col("office_name").is_not_null())
        .select(
            [
                pl.col("zip_code").alias("office_zip"),
                pl.col("office_name"),
                pl.col("state"),
                pl.col("market"),
                pl.col("region_name"),
            ]
        )
        .unique()
    )

    office_count = offices.select(pl.len()).collect().item()
    logger.info(f"Found {office_count} unique office locations")

    # Step 2: Map offices to Medicare localities
    logger.info("Mapping offices to Medicare localities")
    cms_geo_zips_df = catalog.scan_table("cms_geo_zips")
    if cms_geo_zips_df is None:
        raise ValueError("cms_geo_zips table not found in catalog")

    # Determine year to use
    if config.year is None:
        # Get most recent year from cms_geo_zips
        max_year_quarter = (
            cms_geo_zips_df.select(pl.col("year_quarter").str.slice(0, 4).cast(pl.Int32).max())
            .collect()
            .item()
        )
        config.year = max_year_quarter
        logger.info(f"Using most recent year: {config.year}")

    # Filter for target year and join to offices
    cms_geo_current = cms_geo_zips_df.filter(
        pl.col("year_quarter").str.starts_with(str(config.year))
    ).select(
        [
            pl.col("geo_zip_5"),
            pl.col("geo_state_cd"),
            pl.col("carrier"),
            pl.col("locality"),
        ]
    )

    offices_with_locality = offices.join(
        cms_geo_current,
        left_on="office_zip",
        right_on="geo_zip_5",
        how="left",
    )

    # Check for unmapped offices
    unmapped = offices_with_locality.filter(pl.col("locality").is_null())
    unmapped_count = unmapped.select(pl.len()).collect().item()
    if unmapped_count > 0:
        logger.warning(f"{unmapped_count} offices could not be mapped to Medicare localities")
        unmapped_offices = unmapped.select("office_name").collect()["office_name"].to_list()
        logger.warning(f"Unmapped offices: {unmapped_offices[:5]}")

    # Step 3: Load GPCI data for current year
    logger.info(f"Loading GPCI data for year {config.year}")
    gpci_df = catalog.scan_table("gpci_inputs")
    if gpci_df is None:
        raise ValueError("gpci_inputs table not found in catalog")

    # Join offices with GPCI data
    offices_with_gpci = offices_with_locality.join(
        gpci_df.select(
            [
                pl.col("geo_locality_state_cd"),
                pl.col("geo_locality_num"),
                pl.col("geo_locality_name"),
                pl.col("pw_gpci"),
                pl.col("pe_gpci"),
                pl.col("pe_mp_gpci").alias("mp_gpci"),
            ]
        ),
        left_on=["state", "locality"],
        right_on=["geo_locality_state_cd", "geo_locality_num"],
        how="left",
    )

    # Validate GPCI values (handle nulls with national average = 1.0)
    offices_with_gpci = offices_with_gpci.with_columns(
        [
            PFSRateCalcExpression.validate_gpci("pw_gpci", 1.0).alias("pw_gpci"),
            PFSRateCalcExpression.validate_gpci("pe_gpci", 1.0).alias("pe_gpci"),
            PFSRateCalcExpression.validate_gpci("mp_gpci", 1.0).alias("mp_gpci"),
        ]
    )

    # Step 4: Load RVU data for HCPCS codes
    logger.info(f"Loading RVU data for {len(hcpcs_codes)} HCPCS codes")
    pprvu_df = catalog.scan_table("pprvu_inputs")
    if pprvu_df is None:
        raise ValueError("pprvu_inputs table not found in catalog")

    # Select PE RVU column based on facility type
    pe_rvu_col = PFSRateCalcExpression.select_pe_rvu_column(config.facility_type)

    # Filter for HCPCS codes
    rvu_data = pprvu_df.filter(pl.col("hcpcs").is_in(hcpcs_codes)).select(
        [
            pl.col("hcpcs").alias("hcpcs_code"),
            pl.col("description").alias("hcpcs_description"),
            pl.col("work_rvu"),
            pl.col(pe_rvu_col).alias("nf_pe_rvu"),
            pl.col("mp_rvu"),
            pl.col("conversion_factor"),
        ]
    )

    rvu_count = rvu_data.select(pl.len()).collect().item()
    logger.info(f"Found RVU data for {rvu_count} HCPCS codes")

    # Override conversion factor if provided in config
    if config.conversion_factor is not None:
        logger.info(f"Using config conversion factor: ${config.conversion_factor:.2f}")
        rvu_data = rvu_data.with_columns(
            [pl.lit(config.conversion_factor).alias("conversion_factor")]
        )

    # Step 5: Cross join offices and HCPCS codes to create all combinations
    logger.info("Creating office × HCPCS combinations")
    rates_base = offices_with_gpci.join(rvu_data, how="cross")

    combo_count = rates_base.select(pl.len()).collect().item()
    logger.info(f"Created {combo_count:,} office × HCPCS combinations")

    # Step 6: Calculate current year payment rates
    logger.info(f"Calculating payment rates for year {config.year}")

    # Build payment calculations using expression
    payment_exprs = PFSRateCalcExpression.build_payment_calculation(
        work_rvu="work_rvu",
        pe_rvu="nf_pe_rvu",
        mp_rvu="mp_rvu",
        pw_gpci="pw_gpci",
        pe_gpci="pe_gpci",
        mp_gpci="mp_gpci",
        conversion_factor="conversion_factor",
    )

    current_year_rates = rates_base.with_columns(**payment_exprs).with_columns(
        [
            pl.lit(config.year).alias("year"),
            pl.lit(config.facility_type).alias("facility_type"),
            pl.lit(datetime.now()).alias("calculation_date"),
        ]
    )

    # Step 7: Calculate prior year rates if comparison is enabled
    if config.include_comparison:
        prior_year = config.prior_year if config.prior_year else config.year - 1
        logger.info(f"Calculating prior year rates for comparison (year {prior_year})")

        # TODO: Implement prior year calculation
        # This requires loading prior year GPCI and RVU data
        # For now, add placeholder null columns
        current_year_rates = current_year_rates.with_columns(
            [
                pl.lit(prior_year).alias("prior_year"),
                pl.lit(None, dtype=pl.Float64).alias("prior_work_rvu"),
                pl.lit(None, dtype=pl.Float64).alias("prior_nf_pe_rvu"),
                pl.lit(None, dtype=pl.Float64).alias("prior_mp_rvu"),
                pl.lit(None, dtype=pl.Float64).alias("prior_conversion_factor"),
                pl.lit(None, dtype=pl.Float64).alias("prior_payment_rate"),
                pl.lit(None, dtype=pl.Float64).alias("rate_change_dollars"),
                pl.lit(None, dtype=pl.Float64).alias("rate_change_percent"),
            ]
        )

    # Step 8: Final column selection to match schema
    final_columns = [
        "hcpcs_code",
        "hcpcs_description",
        "office_name",
        "office_zip",
        "state",
        "carrier",
        "locality",
        "geo_locality_name",
        "year",
        "work_rvu",
        "nf_pe_rvu",
        "mp_rvu",
        "pw_gpci",
        "pe_gpci",
        "mp_gpci",
        "conversion_factor",
        "work_payment",
        "pe_payment",
        "mp_payment",
        "total_rvu_adjusted",
        "payment_rate",
        "facility_type",
        "calculation_date",
        "market",
        "region_name",
    ]

    if config.include_comparison:
        final_columns.extend(
            [
                "prior_year",
                "prior_work_rvu",
                "prior_nf_pe_rvu",
                "prior_mp_rvu",
                "prior_conversion_factor",
                "prior_payment_rate",
                "rate_change_dollars",
                "rate_change_percent",
            ]
        )

    result = current_year_rates.select(final_columns)

    # Log summary statistics
    summary = result.select(
        [
            pl.len().alias("total_rows"),
            pl.col("hcpcs_code").n_unique().alias("unique_hcpcs"),
            pl.col("office_name").n_unique().alias("unique_offices"),
            pl.col("payment_rate").mean().alias("avg_payment_rate"),
            pl.col("payment_rate").min().alias("min_payment_rate"),
            pl.col("payment_rate").max().alias("max_payment_rate"),
        ]
    ).collect()

    logger.info("PFS rates calculation summary:")
    logger.info(f"  Total rows: {summary['total_rows'][0]:,}")
    logger.info(f"  Unique HCPCS codes: {summary['unique_hcpcs'][0]}")
    logger.info(f"  Unique offices: {summary['unique_offices'][0]}")
    logger.info(f"  Avg payment rate: ${summary['avg_payment_rate'][0]:.2f}")
    logger.info(f"  Min payment rate: ${summary['min_payment_rate'][0]:.2f}")
    logger.info(f"  Max payment rate: ${summary['max_payment_rate'][0]:.2f}")

    logger.info("PFS rate calculation transform complete")
    return result
