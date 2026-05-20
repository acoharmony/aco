# © 2025 HarmonyCares
# All rights reserved.

"""
Standardization transformation implementations.

 transformations for normalizing and standardizing healthcare data
across different sources and formats. Standardization is a critical mid-to-late pipeline
stage that ensures consistent column names, data types, computed fields, and business
logic across disparate data sources (CMS CCLF files, custom extracts, provider feeds).

What are Standardization Transformations?

Standardization transformations convert raw or semi-processed data into a consistent,
normalized format that downstream analytics, reporting, and business intelligence tools
expect. This includes:

1. **Column Renaming**: Map source-specific column names to standard business names
   - CMS naming (cur_clm_uniq_id) → business naming (claim_id)
   - Multiple source systems with different conventions → single standard

2. **Computed Columns**: Add derived fields needed for analytics
   - Date components (year, month, quarter) from service dates
   - Business flags (is_deceased, is_high_cost) from data conditions
   - Enrollment periods and coverage spans from eligibility data

3. **Data Type Consistency**: Ensure types are correct for downstream consumption
   - Date columns are proper date types (not strings)
   - Numeric columns are float64 or int64 (not mixed types)
   - Boolean flags are consistent (not 0/1 mixed with true/false)

4. **Conditional Logic**: Apply business rules for data enrichment
   - Enrollment end dates with death date truncation
   - Coverage periods with plan transitions
   - Provider attribution logic

Key Concepts

- **Configuration-Driven Design**: Standardization is controlled by schema configuration
  (YAML/dict) rather than hardcoded logic, enabling flexibility across projects and
  client-specific requirements without code changes

- **Idempotency**: Safe to run multiple times; only applies transformations not already
  present (checks if columns already renamed/computed before applying)

- **Graceful Degradation**: Missing optional columns don't cause failures; standardization
  applies what it can and skips unavailable transformations

- **Pipeline Awareness**: Understands upstream stages (XREF creates current_bene_mbi_id,
  dedup creates _dedup_processed flag) and uses their outputs

- **Extensibility**: Supports registered expression functions from _expressions module
  for complex computations (e.g., enrollment_end_date_with_death_truncation)

Common Use Cases

1. **Claim Standardization**: Professional and institutional claims with consistent naming
   - Rename: cur_clm_uniq_id → claim_id, bene_mbi_id → beneficiary_id
   - Compute: service_year, service_month from service_from_date
   - Add: claim_type (inpatient/outpatient/professional), source_file metadata

2. **Enrollment Standardization**: Member eligibility and coverage periods
   - Rename: bene_mbi_id → beneficiary_id, enrollment_effective_date → enrollment_start_date
   - Compute: enrollment_end_date with death date truncation, coverage_months, age
   - Add: enrollment_year_month format for aggregation, is_deceased flag

3. **Provider Standardization**: Provider rosters and attribution tables
   - Rename: rndrg_prvdr_npi → provider_npi, prvdr_oscar_num → provider_id
   - Compute: provider_specialty_group from taxonomy codes, is_primary_care flag
   - Add: aco_id, tin (Tax Identification Number) from external sources

4. **Multi-Source Integration**: Combining data from different sources with different schemas
   - Source A: claim_number → claim_id
   - Source B: unique_claim_identifier → claim_id
   - Source C: clm_id → claim_id
   - Result: All sources have consistent claim_id column

5. **Business Intelligence Preparation**: Format data for BI tools (Tableau, Power BI)
   - Add year/month/quarter columns for time-based slicing
   - Compute cost categories (low/medium/high) for filtering
   - Add aggregation keys (beneficiary_year_month) for joins

How It Works

Standardization operates through two main functions:

1. **standardize()**: Simple, hardcoded standard mappings for common CMS columns
   - Applies predefined rename map (cur_clm_uniq_id → claim_id, etc.)
   - Adds year/month from service_from_date if available
   - Fast and lightweight for typical CMS CCLF data

2. **apply_standard_transform()**: Configuration-driven, flexible standardization
   - Reads rename_columns, add_computed, add_columns, conditional_columns from config
   - Supports literal values ("ACO123"), column references (copy one column to another),
     date computations (year/month/quarter extraction), conditional logic (when/then/else)
   - Integrates with registered expression functions for complex business logic
   - Logs all transformations for audit trail

Pipeline Position

Standardization typically runs mid-to-late in the data pipeline, after structural
transformations but before final aggregation and analytics:

Bronze (Raw) → Parsing → XREF (Crosswalk) → Deduplication → ADR (Adjustments) →
Union (Multi-Source Merge) → Pivot (Code Reshaping) → [STANDARDIZATION] →
Silver (Processed) → Analytics → Gold (Business Reports)

Why this position?
- **After XREF**: Uses current_bene_mbi_id created by crosswalk mapping
- **After Dedup**: Operates on clean, deduplicated records
- **After ADR**: Works with adjusted amounts and reconciled claims
- **After Pivot**: Has access to diagnosis_1..25, procedure_1..25 columns if needed
- **Before Analytics**: Ensures analytics queries use consistent column names
- **Before Gold**: Business reports rely on standardized schema

Configuration

Standardization is controlled by schema configuration:

```yaml
standardization:
  rename_columns:
    cur_clm_uniq_id: claim_id
    bene_mbi_id: beneficiary_id
    clm_from_dt: service_from_date
    clm_thru_dt: service_thru_date

  add_computed:
    service_year: year_from_service_date
    service_month: month_from_service_date
    service_quarter: quarter_from_service_date
    enrollment_end_date: enrollment_end_date_with_death_truncation

  add_columns:
    - name: aco_id
      value: "ACO123"  # Literal value
    - name: source_system
      value: cclf_source  # Column reference
    - name: is_processed
      value: "true"

  conditional_columns:
    - name: is_deceased
      condition: "bene_death_dt.is_not_null()"
      value: "true"
      else: "false"
    - name: coverage_end_date
      condition: "bene_death_dt.is_not_null()"
      value: "bene_death_dt.dt.end_of_month()"
      else: "null"
```

Performance Considerations

- **Lazy Evaluation**: Uses Polars LazyFrames to optimize query plans across renames,
  computed columns, and conditional logic (entire standardization may become single
  optimized operation)

- **Schema Checks**: Checks if columns exist before renaming/computing to avoid errors;
  slight overhead but necessary for robustness across data sources

- **Conditional Logic Parsing**: String-based condition parsing (is_not_null(),
  end_of_month()) has small overhead; consider using registered expressions for
  complex conditions

- **Column Addition Order**: add_columns processes in order; later columns can reference
  earlier renamed columns (schema_names refreshed between operations)

- **Memory Impact**: Minimal; renaming is zero-copy in Polars, computed columns add
  new fields but don't duplicate data

See Also

- `_expressions._standardize`: Registered expression functions for complex computations
- `_transforms._pipeline`: Orchestrates standardization with other transform stages
- `_transforms._core`: Transform class that wraps standardization logic
"""

from typing import Any

import polars as pl

from .._decor8 import transform, transform_method


@transform_method(
    enable_composition=True,
    threshold=5.0,
)
@transform(
    name="standardize",
    tier=["bronze", "silver"],
    description="Standardize column names and data types using CMS mappings",
    sql_enabled=True,
)
def standardize(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Standardize column names and data types using predefined CMS mappings.

        This function applies a fixed set of column renamings commonly used for CMS CCLF data
        and adds computed date columns (year, month) from service dates. It's designed for
        quick standardization of typical Medicare claims and enrollment data without requiring
        external configuration.

        The function is idempotent: if a target column already exists, it won't rename the
        source column, preventing duplicate column errors when run multiple times.

        Parameters

        df : pl.LazyFrame
            Input LazyFrame to standardize, typically containing raw CMS CCLF columns

        Returns

        pl.LazyFrame
            Standardized LazyFrame with renamed columns and computed date fields

        Standard Mappings

        - cur_clm_uniq_id → claim_id (CMS unique claim identifier)
        - bene_mbi_id → beneficiary_id (beneficiary Medicare Beneficiary Identifier)
        - current_bene_mbi_id → beneficiary_id (after XREF crosswalk)
        - clm_from_dt → service_from_date (claim from date)
        - clm_thru_dt → service_thru_date (claim through date)
        - prvdr_oscar_num → provider_id (provider OSCAR number)
        - rndrg_prvdr_npi → provider_npi (rendering provider NPI)

        Computed Columns (if service_from_date exists)

        - service_year: 4-digit year (e.g., 2024)
        - service_month: Month number 1-12 (e.g., 3 for March)

        Notes

        - Only renames columns that exist in the input DataFrame
        - Only adds computed columns if service_from_date is present
        - Safe to run multiple times (idempotent)
        - For more flexible, configuration-driven standardization, use apply_standard_transform()
    """
    standard_map = {
        "cur_clm_uniq_id": "claim_id",
        "bene_mbi_id": "beneficiary_id",
        "current_bene_mbi_id": "beneficiary_id",
        "clm_from_dt": "service_from_date",
        "clm_thru_dt": "service_thru_date",
        "prvdr_oscar_num": "provider_id",
        "rndrg_prvdr_npi": "provider_npi",
    }

    rename_map = {}
    for old, new in standard_map.items():
        if old in df.columns and new not in df.columns:
            rename_map[old] = new

    if rename_map:
        df = df.rename(rename_map)

    if "service_from_date" in df.columns:
        df = df.with_columns(
            [
                pl.col("service_from_date").dt.year().alias("service_year"),
                pl.col("service_from_date").dt.month().alias("service_month"),
            ]
        )

    return df


@transform_method(enable_composition=True, threshold=5.0)
@transform(name="apply_standard_transform", tier=["bronze", "silver"], sql_enabled=True)
def apply_standard_transform(df: pl.LazyFrame, std_config: dict, logger: Any) -> pl.LazyFrame:
    """
    Apply configuration-driven standardization transformation (idempotent).

         flexible, schema-controlled standardization supporting column
        renaming, computed date fields, literal value columns, column references, and conditional
        business logic. It's the primary standardization function for project-specific and
        client-specific requirements that go beyond the fixed mappings in standardize().

        The function is idempotent: only applies transformations that haven't been applied yet,
        making it safe to run multiple times in complex pipelines.

        Parameters

        df : pl.LazyFrame
            Input LazyFrame to standardize
        std_config : dict
            Configuration dictionary with standardization instructions:

            - **rename_columns** (dict): Map source column names to target names
              Example: {"cur_clm_uniq_id": "claim_id", "bene_mbi_id": "beneficiary_id"}

            - **add_computed** (dict): Define computed columns from date extractions or
              registered expression functions
              Example: {"service_year": "year_from_service_date",
                        "enrollment_end_date": "enrollment_end_date_with_death_truncation"}

            - **add_columns** (list[dict]): Add columns with literal values or references
              Each item: {"name": "column_name", "value": "literal_or_column_reference"}
              Example: [{"name": "aco_id", "value": "ACO123"},
                        {"name": "source", "value": "cclf_source"}]

            - **conditional_columns** (list[dict]): Add columns with conditional logic
              Each item: {"name": "column_name", "condition": "expr", "value": "true_expr",
                          "else": "false_expr"}
              Example: [{"name": "is_deceased", "condition": "bene_death_dt.is_not_null()",
                         "value": "true", "else": "false"}]

        logger : Any
            Logger instance for recording transformations and debugging

        Returns

        pl.LazyFrame
            Standardized LazyFrame with all configured transformations applied

        Supported Computed Column Types

        - **year_from_service_date**: Extract year from service_from_date
        - **month_from_service_date**: Extract month from service_from_date
        - **quarter_from_service_date**: Extract quarter from service_from_date
        - **format_year_month_from_enrollment_start**: Format as YYYYMM string
        - **enrollment_end_date_with_death_truncation**: Registered expression for enrollment

        Supported Conditional Patterns

        - **is_not_null()**: Check if column has non-null value
        - **end_of_month()**: Calculate last day of month from date column

        Notes

        - Only renames columns that exist and aren't already named the target name
        - Skips computed columns if required source columns are missing
        - Refreshes schema between add_columns operations for column reference resolution
        - Logs all transformations for audit trail
        - Gracefully handles errors in conditional column parsing

    """
    rename_map = std_config.get("rename_columns", {})
    computed_cols = std_config.get("add_computed", {})
    add_cols = std_config.get("add_columns", [])
    conditional_cols = std_config.get("conditional_columns", [])

    logger.info(
        f"Applying standardization with {len(rename_map)} renames, {len(computed_cols)} computed columns, {len(add_cols)} additional columns, and {len(conditional_cols)} conditional columns"
    )

    if rename_map:
        schema_names = df.collect_schema().names()
        actual_renames = {}
        for old_name, new_name in rename_map.items():
            if old_name in schema_names and new_name not in schema_names:
                actual_renames[old_name] = new_name

        if actual_renames:
            df = df.rename(actual_renames)
            logger.info(f"Renamed {len(actual_renames)} columns")

    for col_name, computation in computed_cols.items():
        if col_name not in df.collect_schema().names():
            if computation == "enrollment_end_date_with_death_truncation":
                # Inline expression: enrollment end date with death date truncation
                # If beneficiary died, enrollment ends on last day of month they died
                death_date_col = "bene_death_dt"
                enrollment_end_expr = (
                    pl.when(pl.col(death_date_col).is_not_null())
                    .then(
                        pl.col(death_date_col)
                        + pl.duration(
                            days=pl.col(death_date_col).dt.days_in_month()
                            - pl.col(death_date_col).dt.day()
                        )
                    )
                    .otherwise(None)
                    .alias("enrollment_end_date")
                )
                df = df.with_columns(enrollment_end_expr)
                logger.debug(f"Applied registered expression: {computation}")
                continue
            if computation == "year_from_service_date":
                if "service_from_date" in df.collect_schema().names():
                    df = df.with_columns(pl.col("service_from_date").dt.year().alias(col_name))
            elif computation == "month_from_service_date":
                if "service_from_date" in df.collect_schema().names():
                    df = df.with_columns(pl.col("service_from_date").dt.month().alias(col_name))
            elif computation == "quarter_from_service_date":
                if "service_from_date" in df.collect_schema().names():
                    df = df.with_columns(pl.col("service_from_date").dt.quarter().alias(col_name))
            elif computation == "format_year_month_from_enrollment_start":
                if "enrollment_start_date" in df.collect_schema().names():
                    df = df.with_columns(
                        pl.col("enrollment_start_date").dt.strftime("%Y%m").alias(col_name)
                    )
    for col_def in add_cols:
        col_name = col_def.get("name")
        col_value = col_def.get("value")
        if not col_name:
            continue
        schema_names = df.collect_schema().names()
        if col_name in schema_names:
            continue
        if col_value in schema_names:
            df = df.with_columns(pl.col(col_value).alias(col_name))
            logger.debug(f"Added column {col_name} referencing {col_value}")
        elif col_value == "null":
            df = df.with_columns(pl.lit(None).alias(col_name))
            logger.debug(f"Added column {col_name} with null value")
        else:
            df = df.with_columns(pl.lit(col_value).alias(col_name))
            logger.debug(f"Added column {col_name} with literal value {col_value}")

    for col_def in conditional_cols:
        col_name = col_def.get("name")
        condition_str = col_def.get("condition")
        true_value_str = col_def.get("value")
        else_value_str = col_def.get("else")

        if not col_name or not condition_str:
            continue

        schema_names = df.collect_schema().names()
        if col_name in schema_names:
            continue

        try:
            if "is_not_null()" in condition_str:
                col_ref = condition_str.replace(".is_not_null()", "").strip()
                if col_ref in schema_names:
                    condition_expr = pl.col(col_ref).is_not_null()
                else:
                    logger.warning(f"Column {col_ref} not found for condition")
                    continue
            else:
                logger.warning(f"Unsupported condition: {condition_str}")
                continue

            if "end_of_month()" in true_value_str:
                col_ref = true_value_str.replace(".dt.end_of_month()", "").strip()
                if col_ref in schema_names:
                    true_expr = pl.col(col_ref) + pl.duration(
                        days=pl.col(col_ref).dt.days_in_month() - pl.col(col_ref).dt.day()
                    )
                else:
                    logger.warning(f"Column {col_ref} not found for true value")
                    continue
            else:
                true_expr = pl.lit(true_value_str) if true_value_str != "null" else pl.lit(None)

            else_expr = pl.lit(else_value_str) if else_value_str != "null" else pl.lit(None)

            df = df.with_columns(
                pl.when(condition_expr).then(true_expr).otherwise(else_expr).alias(col_name)
            )
            logger.debug(f"Added conditional column {col_name}")

        except (
            Exception
        ) as e:  # ALLOWED: Logs error and returns, caller handles the error condition
            logger.warning(f"Failed to add conditional column {col_name}: {e}")

    return df
