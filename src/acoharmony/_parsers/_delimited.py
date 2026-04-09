# © 2025 HarmonyCares
# All rights reserved.

"""
Delimited file parser for healthcare data with flexible delimiter support.

 schema-driven parsing for various delimited text file formats
commonly used in healthcare data exchange, including pipe-delimited (|),
tab-delimited (\t), and custom delimiter formats. Unlike CSV parsers that assume
comma separation, this parser handles multiple delimiter types and provides
automatic delimiter detection and configuration-based delimiter specification.

What is Delimited File Parsing?

Delimited file parsing reads text files where columns are separated by specific
delimiter characters (|, \t, semicolon, multi-character sequences). This parsing
is critical in healthcare because:

- **Format Flexibility**: Different healthcare systems use different delimiters
  (HL7 uses |, laboratory systems use \t, legacy systems use custom delimiters)
- **Schema Enforcement**: Ensures consistent column structure across varying
  source file formats
- **Quote and Escape Handling**: Properly handles quoted fields and escape
  sequences common in healthcare text data
- **Header Support**: Can parse files with or without header rows

Key Concepts

Delimiter Types:
    Healthcare data exchange uses several delimiter formats:

    - Pipe (|): Most common in healthcare, used in HL7 messages, EDI transactions,
      BAR files, CMS REACH reporting files
    - Tab (\t): Common in laboratory results, clinical data exports, research
      datasets, quality measure reports
    - Semicolon (;): European healthcare systems, international data exchanges
    - Custom Multi-Character: Legacy systems may use || or ~|~ or other sequences

Automatic Delimiter Detection:
    When delimiter is not explicitly specified, the parser uses priority order:

    1. Parameter: delimiter argument takes highest priority
    2. Schema File Format: schema.file_format.delimiter configuration
    3. Default Fallback: defaults to pipe (|) delimiter

    Detection logic:
    - Check delimiter parameter first
    - Fall back to schema.file_format.delimiter
    - Default to pipe | if neither specified

Header Handling:
    Files may or may not have header rows:

    - has_header=True: First row contains column names (most common)
    - has_header=False: No header row, schema column names applied directly
    - Column Naming: When no header, uses schema column names in order

Quote Character Handling:
    Delimited files often use quotes for fields containing delimiters:

    - "Smith, John"|M|45 - comma within quoted field
    - "Hospital|Clinic"|2024-01-15 - pipe within quoted field
    - Escape sequences: \" for quotes within quoted fields

Schema Column Selection:
    Parser can select specific columns from schema:

    - Read only columns defined in schema
    - Ignore extra columns in file
    - Order columns according to schema definition
    - Useful for large files with many unused columns

Common Use Cases

HL7 Message Files:
    Parse pipe-delimited HL7 message files containing patient demographics,
    admissions, discharges, transfers (ADT messages), laboratory results (ORU
    messages), billing information (DFT messages).

EDI Transaction Files:
    Parse pipe-delimited EDI 837 (claim submission) and 835 (remittance advice)
    files for billing and payment processing.

BAR Files (Beneficiary Assignment and Reconciliation):
    Parse pipe-delimited CMS BAR files for REACH and MSSP programs containing
    beneficiary assignments, voluntary alignments, claims runout data.

Laboratory Results:
    Parse tab-delimited laboratory result files with test codes, values,
    reference ranges, flags (high/low/critical).

    Example (tab-separated):
    - PATIENT_ID<tab>TEST_CODE<tab>RESULT_VALUE<tab>REFERENCE_RANGE<tab>FLAG
    - 123456789<tab>GLU<tab>95<tab>70-100<tab>NORMAL

Quality Measure Reports:
    Parse semicolon-delimited quality measure calculation results for HEDIS,
    CMS Stars, MSSP quality reporting.

Provider Roster Files:
    Parse delimited provider roster files with NPIs, names, specialties,
    addresses from various source systems.

Custom Legacy System Exports:
    Parse files from legacy healthcare systems using custom multi-character
    delimiters (e.g., || or ~|~) for field separation.

How It Works

parse_delimited() Function:
    Main parser function handles all delimiter types:

    1. Determine Delimiter:
       - Use delimiter parameter if provided
       - Else use schema.file_format.delimiter if available
       - Else default to pipe |

    2. Get Column Names:
       - If has_header=False, extract column names from schema
       - Build list of column names in schema order

    3. TPARC File Detection:
       - Check if filename contains "TPARC" (case-insensitive)
       - Route to specialized TPARC parser if detected
       - TPARC parser handles specific TPARC file format rules

    4. Build Scan Options:
       - source: file path to read
       - separator: delimiter character(s)
       - has_header: whether first row is header
       - encoding: UTF-8 for international characters
       - try_parse_dates: attempt date parsing
       - n_rows: row limit if specified
       - new_columns: schema column names if no header

    5. Scan File with Polars:
       - Use pl.scan_csv() with delimiter as separator
       - Lazy evaluation - no data loaded until collect()
       - Streaming capable for large files

    6. Apply Schema Column Selection:
       - If schema defines specific columns, select only those
       - Filter out extra columns not in schema
       - Maintain schema column order

    7. Return LazyFrame:
       - Return LazyFrame for further transformations
       - Actual file reading happens on collect()

parse_pipe_delimited() Function:
    Convenience wrapper for pipe-delimited files:
    - Calls parse_delimited() with delimiter="|"
    - Used for HL7, EDI, BAR, and CMS reporting files
    - Most common delimiter in healthcare

parse_tab_delimited() Function:
    Convenience wrapper for tab-delimited files:
    - Calls parse_delimited() with delimiter="\t"
    - Used for laboratory results and clinical data exports
    - Common in research datasets

Pipeline Position

Delimited file parsing is typically the first stage in data ingestion:

    Raw Delimited File → [DELIMITED PARSING] → Bronze LazyFrame → Transforms → Silver

    Before parsing:
    - File discovery and routing
    - Schema lookup based on file type
    - ACO ID and date extraction from filename

    After parsing:
    - Date parsing for date columns
    - XREF crosswalk mapping
    - Deduplication
    - Standardization transformations

    Parser registry:
    - Registered with @register_parser("delimited")
    - Also registered as "pipe_delimited" and "tab_delimited"
    - Dynamically invoked by file processors based on:
      * File extension (.txt, .dat, .psv, .tsv)
      * Schema configuration file_type="delimited"

"""

from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import parser_method
from ._registry import register_parser


@register_parser("delimited", metadata={"delimiters": ["|", "\t", ",", ";"]})
@parser_method(
    threshold=2.0,
    validate_path="file_path",
)
def parse_delimited(
    file_path: Path,
    schema: Any,
    limit: int | None = None,
    delimiter: str | None = None,
    has_header: bool = True,
) -> pl.LazyFrame:
    """
    Parse delimited text files with configurable separators.

        This parser handles various delimited file formats including pipe-delimited,
        tab-delimited, and custom delimiter files common in healthcare data exchange.

        Args:
            file_path: Path to the delimited file
            schema: TableMetadata object containing column definitions
            limit: Optional number of rows to read
            delimiter: Character(s) used to separate fields (auto-detect if None)
            has_header: Whether first row contains column names

        Returns:
            pl.LazyFrame: Lazily loaded delimited data with applied schema

        Features:
            - Automatic delimiter detection
            - Custom delimiter support
            - Header row handling
            - Quote character handling
            - Escape sequence support
    """
    # Get delimiter from schema or parameter
    if delimiter is None:
        if isinstance(schema, dict):
            delimiter = schema.get("delimiter", "|")
        elif hasattr(schema, "file_format"):
            delimiter = schema.file_format.get("delimiter", "|") if schema.file_format else "|"
        else:
            delimiter = "|"

    # Get column names from schema
    column_names = None
    columns = (
        schema.columns
        if hasattr(schema, "columns")
        else schema.get("columns", [])
        if isinstance(schema, dict)
        else []
    )
    if not has_header and columns:
        column_names = [col["name"] if isinstance(col, dict) else col.name for col in columns]

    # Check for TPARC file
    if "TPARC" in str(file_path).upper():
        # Use TPARC-specific parser
        from ._tparc import parse_tparc

        return parse_tparc(file_path, schema, limit)

    # Parse delimited file
    scan_options = {
        "source": file_path,
        "separator": delimiter,
        "has_header": has_header,
        "encoding": "utf8",
        "try_parse_dates": True,
        "ignore_errors": False,
    }

    if limit:
        scan_options["n_rows"] = limit

    if column_names and not has_header:
        scan_options["new_columns"] = column_names

    lf = pl.scan_csv(**scan_options)

    # Apply schema column selection if specified
    if hasattr(schema, "columns"):
        schema_cols = [col["name"] for col in schema.columns]
        existing_cols = lf.collect_schema().names()
        cols_to_select = [col for col in schema_cols if col in existing_cols]

        if cols_to_select:
            lf = lf.select(cols_to_select)

    return lf


@register_parser("pipe_delimited")
def parse_pipe_delimited(file_path: Path, schema: Any, limit: int | None = None) -> pl.LazyFrame:
    """
    Parse pipe-delimited files (common in healthcare).

        Pipe-delimited format is widely used in healthcare data exchange,
        particularly for HL7 messages and EDI transactions.

        Args:
            file_path: Path to the pipe-delimited file
            schema: TableMetadata object
            limit: Optional row limit

        Returns:
            pl.LazyFrame: Parsed pipe-delimited data
            10000
            String

        HL7 ADT message file:
            ['ADT^A01', 'ADT^A03', 'ADT^A08', 'ADT^A11']

        BAR file with beneficiary data:
            11.0

        EDI 837 claim submission file:
            2500000.75

        Parse with row limit:
            500

        Real-world voluntary alignment file:
            8500

        Empty pipe-delimited file:
            0

        Lazy evaluation with filtering:
            True
    """
    return parse_delimited(file_path, schema, limit, delimiter="|")


@register_parser("tab_delimited")
def parse_tab_delimited(file_path: Path, schema: Any, limit: int | None = None) -> pl.LazyFrame:
    """
    Parse tab-delimited files (TSV format).

        Tab-delimited files are common for laboratory results and
        clinical data exports.

        Args:
            file_path: Path to the tab-delimited file
            schema: TableMetadata object
            limit: Optional row limit

        Returns:
            pl.LazyFrame: Parsed tab-delimited data
            5000
            120

        Clinical data export:
            250

        Quality measure data:
            15

        Research dataset with many columns:
            5

        Parse with row limit for sampling:
            1000

        Laboratory results with flags:
            425

        Empty tab-delimited file:
            0

        Real-world genomic data:
            1200

        Lazy evaluation with complex filtering:
            True
    """
    return parse_delimited(file_path, schema, limit, delimiter="\t")
