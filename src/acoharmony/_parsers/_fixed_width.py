# © 2025 HarmonyCares
# All rights reserved.

"""
Fixed-width file parser for healthcare data with position-based field extraction.

 schema-driven parsing for fixed-width format text files commonly
used in legacy healthcare systems and CMS data exchange. Fixed-width files define
column boundaries using character positions rather than delimiters, with fields padded
to consistent widths. This format is prevalent in CMS CCLF (Claims and Claims Line Feed)
files, BAR (Beneficiary Assignment and Reconciliation) files, and various legacy
healthcare system exports.

Unlike delimited parsers that rely on separator characters (commas, pipes, tabs),
fixed-width parsers use precise character positions to extract each field, making them
essential for reading CMS data files and legacy system outputs where column alignment
is enforced through padding rather than delimiters.

What Is Fixed-Width File Parsing?
==================================

Fixed-width file parsing reads text files where each field occupies a fixed number
of character positions, with shorter values padded (usually with spaces) to maintain
alignment. This format is common in:

- **CMS CCLF Files**: Medicare claims data with hundreds of precisely positioned fields
- **Legacy System Exports**: Mainframe and COBOL system outputs
- **EDI Transactions**: Some EDI formats use fixed-width segments
- **Administrative Data**: Historical healthcare data exchange formats

**Why Fixed-Width Format?**

- **Data Integrity**: No escaping issues with delimiters appearing in field values
- **Legacy Compatibility**: Mainframe and COBOL systems traditionally use fixed-width
- **Precise Layouts**: Medical coding standards often define exact field positions
- **Performance**: Simple substring extraction is very fast
- **Validation**: Field width constraints enforce data quality

**Critical Parsing Challenges:**

- **Position Accuracy**: Off-by-one errors corrupt all subsequent fields
- **Padding Variations**: Spaces, zeros, or other characters used for padding
- **Encoding Issues**: CMS files may use latin-1 encoding not UTF-8
- **Line Terminators**: CRLF vs LF differences between Windows and Unix
- **Schema Precision**: Must match exact CMS specifications for CCLF files

Key Concepts
============

**Position-Based Field Extraction**

Fields are defined by their starting position and width in characters:

- **Start Position**: 1-based character position where field begins
  - Position 1 is first character of line
  - CMS schemas use 1-based indexing
  - Parser converts to 0-based for internal processing

- **Field Width/Length**: Number of characters allocated to field
  - Fixed for all records in file
  - Includes padding characters
  - May vary between CCLF file types (CCLF1 vs CCLF8 different layouts)

- **End Position**: Last character position of field (start + width - 1)
  - Some schemas specify end position instead of width
  - Parser handles both formats

**Example Field Layout:**

```
Position:  1         11    16        26
           |---------|-----|---------|
Field:     Member ID  ZIP   Last Name
Width:     11         5     10
```

For line: `12345678901 12345Smith     `
- Member ID = `12345678901` (positions 1-11)
- ZIP = `12345` (positions 12-16)
- Last Name = `Smith` (positions 17-26, padded)

**Schema Column Specifications**

Fixed-width schemas define column positions and metadata:

- **start_pos** or **start**: 1-based starting character position
- **end_pos**: Last character position (if not using width)
- **length** or **width**: Field width in characters
- **name**: Internal column name
- **output_name**: Final column name in output (defaults to name)
- **data_type**: Type for casting (string/int/float/date)
- **keep**: Boolean to include/exclude column (false to skip)

**Encoding Considerations**

CMS and healthcare files use different character encodings:

- **UTF-8**: Modern standard, supports international characters
  - Most new healthcare systems use UTF-8
  - Supports accents, umlauts, non-Latin scripts
  - Variable-width encoding (1-4 bytes per character)

- **Latin-1 (ISO-8859-1)**: Legacy CMS encoding
  - Fixed single-byte encoding
  - Common in older CCLF and BAR files
  - Supports Western European characters
  - Position calculations simpler (1 byte = 1 character)

- **Encoding Detection**: Parser must be told encoding
  - Wrong encoding causes garbled characters
  - Position calculations can break with multi-byte characters
  - Always check file documentation or CMS specs

**Padding and Trimming**

Fixed-width fields are padded to maintain alignment:

- **Space Padding**: Most common for text fields
  - Right-padded: `"Smith     "` for 10-character field
  - Left-padded: `"     Smith"` rare but possible
  - Parser automatically strips trailing/leading spaces

- **Zero Padding**: Common for numeric fields
  - Left-padded: `0000012345` for amounts
  - Must preserve leading zeros for IDs: `0123456789` is valid NPI
  - Parser keeps as string, type casting happens later

- **Mixed Padding**: Some fields use specific characters
  - Asterisks, underscores, or other fill characters
  - Parser may need custom stripping logic

**Chunked Processing for Large Files**

Fixed-width files can be very large (millions of claims):

- **Lazy Evaluation**: Parser returns LazyFrame for streaming
- **Row Limiting**: `limit` parameter reads only first N rows
- **Offset Support**: `offset` parameter skips header or earlier rows
- **Memory Mapping**: Polars uses efficient memory-mapped file access
- **Streaming Mode**: Process chunks without loading entire file

Common Use Cases
================

**CCLF Claims File Processing**

Parse Medicare claims data from CMS CCLF files:

- **CCLF1-CCLF7**: Part A (institutional) claims and line items
- **CCLF8**: Beneficiary demographics and crosswalk (MBI mapping)
- **CCLF9**: Historical beneficiary crosswalk for identifier changes

Files contain hundreds of fields with precise positions defined in CMS documentation.
Parser must handle exact specifications to avoid data corruption.

**BAR File Processing**

Parse Beneficiary Assignment and Reconciliation files:

- **ALGR**: Annual reconciliation files with final assignments
- **ALGC**: Current/quarterly assignment updates
- **Voluntary Alignment**: Beneficiary opt-in records

BAR files have different layouts than CCLF files, requiring specific schemas.

**Provider Roster Files**

Parse provider list files from legacy systems:

- **NPI Lists**: National Provider Identifier directories
- **Specialty Rosters**: Provider specialty and demographic files
- **Network Directories**: In-network provider listings

Often exported from mainframe systems in fixed-width format.

**Laboratory Result Files**

Parse lab result exports in fixed-width format:

- **Test Codes**: LOINC or proprietary test identifiers
- **Result Values**: Numeric and text results
- **Reference Ranges**: Normal ranges for interpretation
- **Flags**: High/Low/Critical indicators

Legacy lab systems frequently use fixed-width formats.

**Financial Reconciliation Files**

Parse payment and reconciliation reports:

- **Transaction Records**: Payment details and adjustments
- **Remittance Advice**: 835 equivalent data in fixed-width
- **Settlement Reports**: Monthly financial reconciliation

Banking and financial systems often use fixed-width formats for compatibility.

How It Works
============

**parse_fixed_width() Function**

Main parser function handles all fixed-width file types:

1. **Build Column Specifications**
   - Extract column definitions from schema
   - Support both start+width and start+end formats
   - Filter out columns marked keep=false
   - Convert 1-based positions to 0-based for Polars

2. **Read File Lazily**
   - Use pl.scan_csv() with newline separator
   - Treat each line as single field "_raw_line"
   - Apply encoding (utf8 or latin-1)
   - Skip rows if offset specified
   - Limit rows if limit specified
   - Enable low_memory streaming mode

3. **Extract Fixed-Width Columns**
   - Build string slice expression for each column
   - Use pl.col("_raw_line").str.slice(start, length)
   - Strip padding characters with .str.strip_chars()
   - Alias to final column name

4. **Apply All Extractions**
   - Execute all slice expressions in single select()
   - Efficient columnar processing
   - Return LazyFrame for downstream transforms

5. **Return LazyFrame**
   - No data materialized until collect()
   - Allows query optimization and predicate pushdown
   - Supports streaming for large files

**parse_cclf() Function**

Convenience wrapper for CCLF-specific parsing:

- Extracts encoding from schema file_format
- Defaults to utf-8 if not specified
- Delegates to parse_fixed_width()
- Registered as "cclf" parser type

Pipeline Position
=================

Fixed-width file parsing is typically the first stage in data ingestion for files
in fixed-width format:

```
Raw Fixed-Width File → [FIXED-WIDTH PARSING] → Bronze LazyFrame → Transforms → Silver
```

**Parser Registry:**

This parser is registered with `@register_parser("fixed_width")` and `@register_parser("cclf")`
so it can be dynamically invoked by file processors based on:
- File extension: `.dat`, `.txt`, `.cclf`
- Schema configuration: `file_format.type = "fixed_width"` or `"cclf"`
- File type detection: CCLF file naming patterns
"""

from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import parser_method
from ._registry import register_parser


@register_parser("fixed_width", metadata={"formats": ["cclf", "bar", "reach"]})
@parser_method(
    threshold=5.0,
    validate_path="file_path",
)
def parse_fixed_width(
    file_path: Path,
    schema: Any,
    limit: int | None = None,
    encoding: str = "utf-8",
    offset: int | None = None,
) -> pl.LazyFrame:
    """
    Parse fixed-width format files common in healthcare data.

        Fixed-width files use character positions to define field boundaries,
        commonly used in legacy healthcare systems and CMS data files like CCLF.
        Each field has a defined start and end position, with padding for
        shorter values.

        Args:
            file_path: Path to the fixed-width file
            schema: TableMetadata object with column positions and definitions
            limit: Optional number of rows to read
            encoding: File encoding (default: 'utf-8', sometimes 'latin-1' needed)

        Returns:
            pl.LazyFrame: Parsed data with columns extracted by position

        Schema Requirements:
            The schema must define columns with 'start' and 'width' attributes:
            - start: 1-based starting position
            - width: Field width in characters
            - name: Column name
            - dtype: Data type for conversion

        Features:
            - Position-based field extraction
            - Automatic trimming of padded values
            - Support for various character encodings
            - Efficient chunked reading for large files
        3
        String
        '1A2B3C4D5E6'

        Parse with row limit for testing or sampling:
        100
        'CLM000000000000'

        Parse BAR file with latin-1 encoding:
        'José'
        'García'

        Schema with end_pos instead of width:
        '12345'
        'NY'

        Filter columns with keep=false in schema:
        2
        {'claim_id', 'amount'}

        Preserve leading zeros in identifier fields:
        '0123456789'
        '001234567'

        Handle padding with automatic trimming:
        'PRV001'
        'Smith Medical Group'
        'San Francisco'

        Parse with offset to skip header rows:
        2
        'REC001'

        Real-world CCLF8 beneficiary crosswalk file:
        '0Z9Y8X7W6V5'

        Real-world provider roster with specialty codes:
        3
        '01'

        Large file with streaming:
        10000

        Lazy evaluation with filtering before collect:
        2
        'CLM000000000001'

        Empty file handling:
        0
        {'field1', 'field2'}

        Real-world laboratory results file:
        1

        Real-world financial reconciliation file:
        'TXN-2024-000001'
        '12345.67'

        Schema Column Definition Examples:
            Using start and width:
            {
                'name': 'member_id',
                'start': 1,
                'width': 11,
                'dtype': 'str'
            }

            Using start and end_pos:
            {
                'name': 'member_id',
                'start': 1,
                'end_pos': 11,
                'dtype': 'str'
            }

            With output_name for renaming:
            {
                'name': 'MEMBER_ID',
                'output_name': 'member_id',
                'start': 1,
                'width': 11
            }

            Exclude column from output:
            {
                'name': 'filler_field',
                'start': 100,
                'width': 50,
                'keep': false
            }
    """
    # Build column specifications from schema
    col_specs = []

    columns = schema.columns if hasattr(schema, "columns") else []
    schema.file_format if hasattr(schema, "file_format") else {}

    for col_def in columns:
        # Skip columns marked as keep: false (but include columns where keep is not specified)
        if col_def.get("keep") is False:
            continue

        # Support multiple position formats
        start_pos = col_def.get("start_pos") or col_def.get("start")
        end_pos = col_def.get("end_pos")
        length = col_def.get("length") or col_def.get("width")

        if start_pos and end_pos:
            # Convert from 1-based to 0-based indexing
            col_specs.append(
                {
                    "name": col_def.get("output_name", col_def.get("name")),
                    "start": start_pos - 1,
                    "end": end_pos,  # end_pos is already inclusive in the schema
                }
            )
        elif start_pos and length:
            col_specs.append(
                {
                    "name": col_def.get("output_name", col_def.get("name")),
                    "start": start_pos - 1,
                    "end": start_pos - 1 + length,
                }
            )

    if not col_specs:
        raise ValueError("No valid column specifications found for fixed-width file")

    # Use native Polars lazy reading with memory mapping for performance
    # Polars expects 'utf8' not 'utf-8'
    polars_encoding = "utf8" if encoding == "utf-8" else encoding

    # Read the file lazily as CSV with no separator (treating each line as one field)
    # Then extract fixed-width columns using string slicing
    df = pl.scan_csv(
        file_path,
        has_header=False,
        separator="\n",  # Read entire line as one column
        encoding=polars_encoding,
        skip_rows=offset if offset else 0,  # Skip rows for chunked processing
        n_rows=limit,
        low_memory=True,  # Use streaming mode
        rechunk=False,  # Don't rechunk to save memory
        new_columns=["_raw_line"],
    )

    # Extract columns using efficient string slicing
    expressions = []
    for spec in col_specs:
        # Use Polars string slicing which is very fast
        expr = (
            pl.col("_raw_line")
            .str.slice(spec["start"], spec["end"] - spec["start"])
            .str.strip_chars()
            .alias(spec["name"])
        )
        expressions.append(expr)

    # Apply all column extractions at once
    df = df.select(expressions)

    return df


@register_parser("cclf")
def parse_cclf(file_path: Path, schema: Any, limit: int | None = None) -> pl.LazyFrame:
    """
    Specialized parser for CCLF (Claims and Claims Line Feed) files.

        CCLF files are fixed-width format files provided by CMS containing
        Medicare claims data. This parser handles the specific requirements
        of CCLF file formats including automatic encoding detection from schema
        and proper handling of CMS-specific field layouts.

        CCLF files come in multiple types (CCLF1-CCLF9) with different layouts:
        - CCLF1-CCLF7: Part A/B claims and line items
        - CCLF8: Beneficiary demographics and current crosswalk
        - CCLF9: Historical beneficiary crosswalk for identifier changes

        This parser automatically extracts the encoding specification from the
        schema's file_format configuration and delegates to parse_fixed_width()
        with the appropriate encoding.

        Args:
            file_path: Path to the CCLF file
            schema: CCLF schema with column positions and file_format config
            limit: Optional row limit for sampling or testing

        Returns:
            pl.LazyFrame: Parsed CCLF data with all columns extracted by position
        2
        '240801234567890'

        CCLF8 beneficiary crosswalk with latin-1 encoding:
        '0Z9Y8X7W6V5'

        CCLF9 historical crosswalk for identifier changes:
        '1A2B3C4D5E6'

        CCLF file with row limit for sampling:
        1000

        Schema without explicit encoding defaults to UTF-8:
        2

        Real-world CCLF1 professional claim with diagnosis codes:
        'E11.9'
        'Z23'

        Real-world CCLF5 DME claims:
        'E0601'

        Dict schema format (legacy support):
        'CLM000000000001'
    """
    # Get encoding from schema
    if isinstance(schema, dict):
        encoding = schema.get("encoding", "utf-8")
    elif hasattr(schema, "file_format") and schema.file_format:
        encoding = schema.file_format.get("encoding", "utf-8")
    else:
        encoding = "utf-8"
    return parse_fixed_width(file_path, schema, limit, encoding)
