# © 2025 HarmonyCares
# All rights reserved.

"""
Source tracking utilities for data lineage and audit trails.

 functions to enrich dataframes with standardized metadata
columns that track the origin, processing, and lineage of healthcare data
throughout the data pipeline. Source tracking is essential for debugging,
compliance, auditing, and understanding data currency in healthcare analytics.

What is Source Tracking?

Source tracking adds metadata columns to dataframes that capture the provenance
of each record, enabling complete data lineage from raw source files through
all transformations to final analytics. In healthcare data processing, where
data accuracy and traceability are critical for compliance and quality reporting,
source tracking provides:

**Data Lineage and Audit Trails**
    Track which source file each record came from, when it was processed, and
    what transformations were applied. Essential for regulatory compliance
    (HIPAA audit requirements, CMS program audits), data quality investigations
    (tracing erroneous claims back to source files), and version control
    (identifying which file version was used for reporting).

**Debugging and Issue Resolution**
    When data quality issues arise (unexpected claim amounts, missing beneficiary
    records, duplicate enrollments), source tracking enables rapid root cause
    analysis by identifying the exact source file and processing timestamp.
    Critical for production support and data quality monitoring.

**Data Currency and Freshness**
    Track when data was processed and what time period it represents (file_date).
    Essential for ensuring analytics use current data, detecting stale data,
    and managing data refresh cycles in dashboards and reports.

**Medallion Architecture Support**
    Track which medallion layer (bronze/silver/gold) each record belongs to,
    enabling clear separation of raw data (bronze), cleansed data (silver),
    and aggregated analytics (gold). Supports multi-layer data architectures
    and helps prevent cross-layer contamination.

Key Concepts

**Standardized Metadata Columns**
    The module adds five standard tracking columns to every dataframe:

    - **processed_at**: ISO 8601 timestamp of when the data was processed
      (e.g., "2024-05-08T14:32:15.123456"). Captured at parse time, not
      file generation time. Used for tracking processing latency, identifying
      stale data, and debugging processing issues.

    - **source_file**: Schema name identifying the data structure/source type
      (e.g., 'cclf1', 'bar', 'alr', 'voluntary_alignment'). Maps to schema
      definitions and determines parsing logic. Used for filtering by data
      source and routing data through appropriate transformations.

    - **source_filename**: Raw filename without path (e.g., 'CCLF1.ZC2Y24.D240508.txt',
      'BAR.ALGR23.RP.D240424.csv'). Preserves exact source file name for
      traceability. Used for identifying specific file versions and tracking
      reprocessing.

    - **file_date**: Extracted date from filename representing the reporting
      period or data through date (e.g., '2024-05-08', '2023-12-31'). May be
      None if date extraction fails. Used for time-based filtering, data
      freshness validation, and chronological ordering.

    - **medallion_layer**: Medallion architecture layer ('bronze', 'silver',
      'gold') indicating data maturity. Bronze = raw/unchanged, Silver = cleansed/
      standardized, Gold = aggregated/analytics. May be None if not specified.

**Literal Column Values**
    All tracking columns use literal values (constants) applied to every row
    in the dataframe. This is efficient because Polars stores literals as
    single values, not repeated per row, minimizing memory overhead.

**ISO 8601 Timestamps**
    All timestamps use ISO 8601 format (YYYY-MM-DDTHH:MM:SS.ffffff) for
    consistent parsing across systems, timezone awareness, and sortability.

**Filename Extraction**
    The module automatically extracts just the filename from full paths,
    handling both Windows (\\) and Unix (/) path separators. This ensures
    source_filename is portable and doesn't leak directory structure.

Common Use Cases

**Raw File Ingestion (Bronze Layer)**
    Add source tracking when parsing raw healthcare files (CCLF claims, BAR
    beneficiary assignments, ALR reconciliation files, provider rosters, quality
    measure reports). Bronze layer preserves raw data with full lineage for
    regulatory compliance and reprocessing.

    Example: Parse CCLF1 professional claims file, add bronze layer tracking,
    write to bronze table with complete audit trail.

**Cleansed Data (Silver Layer)**
    Add source tracking after applying data quality transformations (deduplication,
    XREF identifier resolution, ADR adjustment/denial processing, standardization).
    Silver layer tracks transformations applied while maintaining lineage to
    original source files.

    Example: Load bronze claims, deduplicate, resolve MBIs via XREF, add silver
    layer tracking, write to silver table.

**Aggregated Analytics (Gold Layer)**
    Add source tracking to final aggregated datasets (beneficiary annual summaries,
    ACO performance metrics, quality measure calculations, cost reports). Gold
    layer enables tracing aggregated results back to source data.

    Example: Aggregate silver claims to beneficiary annual spend, add gold layer
    tracking, write to gold table for dashboards.

**Multi-Source Data Integration**
    When combining data from multiple source files (e.g., claims from multiple
    months, beneficiary data from CCLF8 and BAR files), source tracking preserves
    which records came from which files. Essential for reconciliation and
    debugging cross-source issues.

    Example: Union 12 months of CCLF1 files, each with source tracking, to
    create annual claims dataset. Filter by source_filename to analyze specific
    months or reprocess individual files.

**Data Quality Monitoring**
    Use source tracking columns to monitor data quality over time (track claims
    counts per source file, detect missing files by date gaps, identify outlier
    files with unusual record counts). Automated quality checks query processed_at
    and file_date to detect stale data or processing delays.

    Example: Daily dashboard queries processed_at to alert if yesterday's CCLF
    files haven't been processed within 24 hours.

**Regulatory Compliance and Auditing**
    Maintain complete audit trails for regulatory compliance (CMS program audits,
    HIPAA audit trails, SOC 2 compliance). Source tracking enables answering
    questions like "Which version of the beneficiary file was used for Q2 quality
    reporting?" or "When was claim CLM123456 processed and from which source file?"

**Reprocessing and Data Recovery**
    When reprocessing is needed (file corrections from CMS, schema changes,
    logic updates), source tracking enables targeted reprocessing (filter by
    source_filename to reprocess specific files, use processed_at to identify
    records processed with old logic). Prevents full historical reprocessing.

How It Works

The `add_source_tracking()` function performs the following steps:

1. **Extract Filename**: Use pathlib.Path to extract just the filename from
   the full source_file path, handling both Windows and Unix paths.

2. **Convert Medallion Layer**: If a MedallionLayer enum is provided, extract
   its string value ('bronze', 'silver', 'gold'). If None, layer_value is None.

3. **Create Literal Columns**: Build a list of literal column expressions:
   - processed_at: Current timestamp in ISO format
   - source_file: Schema name (provided parameter)
   - source_filename: Extracted filename only
   - file_date: Pre-extracted date (provided parameter, may be None)
   - medallion_layer: Layer value string (may be None)

4. **Add Columns to DataFrame**: Use Polars `.with_columns()` to add all
   literal columns in a single operation. Returns a new LazyFrame with
   tracking columns appended.

5. **Return LazyFrame**: Original data unchanged, new columns added. Lazy
   evaluation means no data is materialized until `.collect()` is called.

Pipeline Position

Source tracking is applied at multiple stages in the data pipeline, depending
on the medallion layer:

**Early Pipeline - Bronze Layer**
    Immediately after file parsing, before any transformations:

    Raw File → [PARSE] → Add Bronze Source Tracking → Write Bronze Table

    Bronze layer gets first source tracking application, capturing raw lineage.

**Mid Pipeline - Silver Layer**
    After cleansing and standardization transformations:

    Bronze Table → [DEDUP] → [XREF] → [ADR] → Add Silver Source Tracking → Write Silver Table

    Silver layer replaces or updates medallion_layer to 'silver', preserving
    original source_file and source_filename from bronze.

**Late Pipeline - Gold Layer**
    After aggregation and analytics:

    Silver Table → [AGGREGATE] → [ENRICH] → Add Gold Source Tracking → Write Gold Table

    Gold layer updates medallion_layer to 'gold'. Source tracking may be at
    aggregated grain (e.g., beneficiary-year level, not claim level).

**Before Writing to Tables**
    Source tracking should always be applied before writing to Delta/Parquet
    tables to ensure all persisted data has complete lineage metadata.

**After Transformation Chains**
    When chaining multiple transformations (dedup → XREF → ADR → standardization),
    apply source tracking once at the end of the chain, not after each step,
    to minimize overhead.

Performance Considerations

**Memory Efficiency**
    Literal columns (pl.lit()) are stored efficiently by Polars as single
    constant values, not repeated per row. Adding five tracking columns adds
    minimal memory overhead regardless of dataframe size.

**Lazy Evaluation**
    Function returns LazyFrame, so tracking columns are added to the query
    plan but not materialized until collect(). Allows Polars query optimizer
    to eliminate unused columns if tracking metadata isn't needed downstream.

**Single Operation**
    All tracking columns added in one `.with_columns()` call for efficiency.
    Avoids multiple DataFrame copies.

**String Literals**
    Schema names and filenames are string literals. For very large dataframes
    (millions of rows), the string overhead is negligible due to Polars'
    efficient literal storage.

**Timestamp Overhead**
    Capturing current timestamp (datetime.now()) is negligible overhead.
    Timestamp is computed once per function call, not per row.

**Path Extraction**
    Using pathlib.Path for filename extraction is fast and handles cross-platform
    path formats correctly.

**No Data Materialization**
    Function doesn't call collect(), so no data is loaded into memory. Suitable
    for large files that rely on streaming.
"""

from datetime import datetime
from typing import TYPE_CHECKING, Optional

import polars as pl

if TYPE_CHECKING:
    from ..medallion import MedallionLayer


def add_source_tracking(
    df: pl.LazyFrame,
    source_file: str,
    schema_name: str,
    file_date: str | None = None,
    medallion_layer: Optional["MedallionLayer"] = None,
) -> pl.LazyFrame:
    """
    Add standardized source tracking columns for data lineage.

        This function enriches dataframes with metadata columns that track the
        origin and processing of data. These columns are essential for:
        - Data lineage and audit trails
        - Debugging data issues
        - Understanding data currency
        - Compliance and regulatory requirements
        - Medallion architecture layer tracking

        Added Columns (UNIFORM across ALL raw files):
            - processed_at: ISO timestamp of when the data was processed
            - source_file: Schema name (e.g., 'cclf1', 'bar', 'alr')
            - source_filename: Raw filename without path (e.g., 'CCLF1.ZC2Y24.D240508.txt')
            - file_date: Extracted date from filename (if applicable)
            - medallion_layer: Medallion architecture layer (bronze/silver/gold)

        Args:
            df: LazyFrame to enrich with tracking columns
            source_file: Name/path of the source file being processed
            schema_name: Name of the schema used for parsing
            file_date: Pre-extracted date from filename (optional)
            medallion_layer: Medallion layer (bronze/silver/gold) for this data

        Returns:
            pl.LazyFrame: Original dataframe with added tracking columns

        Note:
            - The file_date column may be None if date extraction fails
            - All tracking columns are added as literal values (constant across rows)
            - Timestamp uses ISO format for consistency and sortability
            - source_filename is extracted from the path (basename only)
            - medallion_layer is stored as string value (bronze/silver/gold) or None
            - Function is suitable for large files - no data materialization occurs
            - Literal column storage is highly memory-efficient in Polars
    """
    from pathlib import Path

    # Extract just the filename without path
    filename_only = Path(source_file).name if source_file else None

    # Get medallion layer as string value
    layer_value = medallion_layer.value if medallion_layer else None

    columns = [
        pl.lit(datetime.now().isoformat()).alias("processed_at"),
        pl.lit(schema_name).alias("source_file"),  # Schema name
        pl.lit(filename_only).alias("source_filename"),  # Raw filename without path
        pl.lit(file_date).alias("file_date"),
        pl.lit(layer_value).alias("medallion_layer"),  # bronze/silver/gold
    ]

    return df.with_columns(columns)
