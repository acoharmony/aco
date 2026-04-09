# © 2025 HarmonyCares
# All rights reserved.

"""
Date extraction utilities for healthcare data filenames.

 sophisticated pattern matching to extract dates from
various healthcare file naming conventions used in CMS programs (REACH, MSSP),
BAR files, CCLF files, and other reporting formats. It implements a hierarchical
pattern matching system that handles multiple date encoding formats.

What is Date Extraction from Filenames?

Date extraction identifies the reporting period or data date from standardized
filename patterns used in healthcare data files. This metadata is critical for:

- **Data Timeliness**: Determining reporting period, performance year, data freshness
- **File Organization**: Sorting files by date, identifying latest version
- **Data Validation**: Ensuring data aligns with expected reporting periods
- **Pipeline Orchestration**: Loading files in chronological order, detecting gaps
- **Audit and Compliance**: Tracking when data was generated, version control

Key Concepts

**Hierarchical Pattern Matching**:
    Patterns are tested in order from most specific to most general:

    1. **Annual Reconciliation**: Y[YYYY].D[YY]MMDD (final year-end data)
    2. **Schema-Defined Patterns**: Custom regex from configuration
    3. **Quarterly Data**: [YYYY]Q[Q] format (Q1-Q4)
    4. **ISO Timestamp**: YYYY-MM-DDTHH:MM:SS format
    5. **Standard Date**: YYYY-MM-DD or YYYYMMDD format
    6. **CCLF Format**: ZC[X]Y[YY].D[YYMMDD] CMS CCLF file format
    7. **BAR/REACH Format**: ALGR/ALGC with date codes
    8. **Beginning of Year**: D[YY]9999 format (January 1st marker)

**Date Semantics**:
    Different patterns represent different date meanings:

    - **Performance Year**: Year of service delivery (e.g., ALGR23 = 2023 performance)
    - **Reporting Date**: Date file was generated/delivered
    - **Data Through Date**: Latest date of data included
    - **Quarter End**: Last day of reporting quarter (Q1=03-31, Q2=06-30, etc.)

**Special File Types**:
    Certain file types have special date interpretation rules:

    - **ALGR (Reconciliation)**: Always 12/31 of performance year
    - **ALGC (Current)**: Actual date from filename
    - **9999 Date Code**: Interpreted as beginning of year (01-01)
    - **CCLF Weekly/Runout**: Uses date portion (D[YYMMDD])

**Graceful Degradation**:
    Returns None for unrecognized patterns or extraction errors, allowing
    pipelines to handle non-standard filenames without failing.

Common Use Cases

1. **CCLF File Processing**:
   Extract data through date from CCLF file naming (e.g.,
   P.A2671.ACO.ZC2Y24.D240508.T0902530 → 2024-05-08) to track data freshness
   and identify latest files.

2. **BAR File Organization**:
   Extract performance year from BAR reconciliation files (e.g.,
   BAR.ALGR23.RP.D240424 → 2023-12-31) to group data by performance year
   regardless of delivery date.

3. **Quarterly Reporting**:
   Extract quarter end dates from quarterly reports (e.g., 2024Q1_claims.csv
   → 2024-03-31) for standardized time period grouping.

4. **Annual Reconciliation**:
   Identify final year-end data files (e.g., Y2022.D259999 → 2022-12-31)
   for reconciliation and final reporting.

5. **File Sorting and Selection**:
   Sort files chronologically, select files within date range, identify most
   recent file for each file type.

How It Works

**extract_file_date() Function**:
    1. Try most specific pattern: Y[YYYY].D[YY]MMDD annual reconciliation
    2. Try schema-defined custom patterns from configuration
    3. Try quarterly pattern: [YYYY]Q[Q]
    4. Try ISO timestamp: YYYY-MM-DDTHH:MM:SS
    5. Try standard date: YYYY-MM-DD or YYYYMMDD
    6. Try CCLF pattern: ZC[X]Y[YY].D[YYMMDD]
    7. Try BAR/REACH pattern: ALGR/ALGC with date codes
    8. Try beginning of year: D[YY]9999
    9. Return None if no pattern matches

**Date Normalization**:
    All extracted dates are returned in ISO format (YYYY-MM-DD) for consistency
    and easy parsing/comparison.

**Quarter End Logic**:
    Quarterly patterns return the last day of the quarter:
    - Q1 → March 31
    - Q2 → June 30
    - Q3 → September 30
    - Q4 → December 31

Pipeline Position

Date extraction typically happens at the **earliest stage of file discovery**:

**Pipeline Flow**::

    File Discovery → [DATE EXTRACTION] → [ACO ID EXTRACTION] → File Routing → Parsing

- **Before File Routing**: Extract date to select correct files for processing
- **File Organization**: Sort files by date, group by reporting period
- **Metadata Addition**: Add ``file_date`` or ``report_date`` columns during parsing
- **Validation**: Flag files outside expected date range

Performance Considerations

- **Regex Efficiency**: Pattern matching is fast, sequential testing stops at first match
- **No File I/O**: Operates only on filename string, no disk access
- **Exception Handling**: Catches all errors, returns None (never raises)
- **Memoization Opportunity**: Cache results for repeated calls on same filename
- **Pattern Ordering**: Most common patterns tested first for efficiency
"""

import re
from typing import Any


def extract_file_date(filename: str, schema: Any) -> str | None:
    """
    Extract date information from healthcare data filenames.

        This function implements a sophisticated pattern matching system to extract
        dates from various healthcare file naming conventions. It uses a hierarchy
        of patterns, starting with the most specific and falling back to more
        general patterns.

        Pattern Hierarchy:
            1. Annual reconciliation: Y[YYYY].D[YY]MMDD format
            2. Schema-defined patterns from configuration
            3. Quarterly data: [YYYY]Q[Q] format
            4. Standard date: YYYYMMDD format
            5. CCLF format: ZC[X]Y[YY].D[YYMMDD]
            6. BAR/REACH format: ALGR/ALGC with date codes
            7. Beginning of year: D[YY]9999 format

        Args:
            filename: Name of the file to extract date from
            schema: TableMetadata object containing optional file_patterns configuration
                    with report_year_extraction patterns

        Returns:
            Optional[str]: Date in ISO format (YYYY-MM-DD) if extractable, None otherwise
    """
    try:
        # Check most specific patterns first before schema patterns

        # Pattern: Y[YYYY].D[YY]MMDD - Annual reconciliation with final data (e.g., Y2022.D259999)
        # This represents final data for the year YYYY (through 12/31/YYYY)
        match = re.search(r"Y(20\d{2})\.D(\d{2})(\d{2})(\d{2})", filename)
        if match:
            year = match.group(1)  # Use the Y year, not the D year
            # This is final reconciliation data for the specified year
            return f"{year}-12-31"

        # Then try schema-defined patterns
        if schema and hasattr(schema, "storage"):
            file_patterns = schema.storage.get("file_patterns", {})
            year_extraction = file_patterns.get("report_year_extraction", {})
        else:
            year_extraction = {}

        for pattern_type, regex_pattern in year_extraction.items():
            match = re.search(regex_pattern, filename)
            if match:
                if pattern_type == "annual":
                    # Extract year: Y2024 -> 2024-01-01 (beginning of year)
                    year = match.group(1)
                    return f"{year}-01-01"
                elif pattern_type == "quarterly":
                    # Extract year and quarter, use end of quarter date
                    year = match.group(1)
                    quarter = match.group(2)
                    quarter_end_dates = {
                        "1": f"{year}-03-31",  # Q1 ends March 31
                        "2": f"{year}-06-30",  # Q2 ends June 30
                        "3": f"{year}-09-30",  # Q3 ends September 30
                        "4": f"{year}-12-31",  # Q4 ends December 31
                    }
                    return quarter_end_dates.get(quarter, f"{year}-03-31")

        # Fallback patterns for common date formats in healthcare data files
        # Check in order of specificity

        # Pattern: [YYYY]Q[Q] - Quarterly data (e.g., 2024Q1)
        match = re.search(r"(20\d{2})Q([1-4])", filename)
        if match:
            year = match.group(1)
            quarter = match.group(2)
            # Use last day of quarter for quarterly data
            quarter_end_dates = {
                "1": f"{year}-03-31",  # Q1 ends March 31
                "2": f"{year}-06-30",  # Q2 ends June 30
                "3": f"{year}-09-30",  # Q3 ends September 30
                "4": f"{year}-12-31",  # Q4 ends December 31
            }
            return quarter_end_dates.get(quarter, f"{year}-03-31")

        # Pattern: M-DD-YYYY or MM-DD-YYYY format (e.g., D0259 Provider List - 1-30-2026 15.27.44.xlsx)
        # Used in provider list exports with timestamp
        match = re.search(r"(\d{1,2})-(\d{1,2})-(20\d{2})\s+\d{2}\.\d{2}\.\d{2}", filename)
        if match:
            month = match.group(1).zfill(2)  # Zero-pad month if needed
            day = match.group(2).zfill(2)    # Zero-pad day if needed
            year = match.group(3)
            return f"{year}-{month}-{day}"

        # Pattern: M-D-YY or MM-DD-YY format (e.g., ACO REACH Participant List PY2025 - 8-5-25 13.19.51.xlsx)
        # Used in REACH participant list exports with 2-digit year
        match = re.search(r"(\d{1,2})-(\d{1,2})-(\d{2})\s+\d{2}\.\d{2}\.\d{2}", filename)
        if match:
            month = match.group(1).zfill(2)  # Zero-pad month if needed
            day = match.group(2).zfill(2)    # Zero-pad day if needed
            yy = match.group(3)
            year = f"20{yy}"  # Convert 2-digit year to 4-digit
            return f"{year}-{month}-{day}"

        # Pattern: ISO timestamp format (e.g., 2025-09-23T08_20_29)
        # Used in email and mailed exports
        match = re.search(r"(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])T", filename)
        if match:
            return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

        # Pattern: YYYY-MM-DD format (e.g., HC REACH Report 2025-08-25.xlsx)
        # Common in manually named reports
        match = re.search(r"(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])", filename)
        if match:
            return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

        # Pattern: YYYYMMDD in filename (e.g., 20240831)
        match = re.search(r"(20\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])", filename)
        if match:
            return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

        # Pattern: CCLF files - P.A*.ACO.ZC[X]Y[YY].D[YYMMDD].T[HHMMSS]
        # Example: P.A2671.ACO.ZC2Y24.D240508.T0902530
        # Also handles: ZCAY (yearly), ZCAR (runout), ZCAWY (weekly), ZCBWY, etc.
        match = re.search(
            r"\.ZC[A-Z0-9]*[YR](\d{2})\.D(\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\.", filename
        )
        if match:
            match.group(1)  # YY from ZC2Y24
            date_yy = match.group(2)  # YY from D240508
            mm = match.group(3)
            dd = match.group(4)
            # Use the date from the D portion
            return f"20{date_yy}-{mm}-{dd}"

        # Pattern: D[YY]MMDD for BAR/REACH files
        # ALGR = Reconciliation files: always use 12/31 of PRIOR year
        # ALGC = Current files: use the actual date
        match = re.search(
            r"\.(ALGR|ALGC)(\d{2})\.RP\.D(\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\.", filename
        )
        if match:
            file_type = match.group(1)  # ALGR or ALGC
            file_year = match.group(2)  # YY from ALGR23 or ALGC24
            delivery_yy = match.group(3)  # YY from D240424
            mm = match.group(4)
            dd = match.group(5)

            if file_type == "ALGR":
                # Reconciliation file: always end of performance year (12/31)
                # The file year (e.g., ALGR23) indicates the performance year
                return f"20{file_year}-12-31"
            else:  # ALGC
                # Current file: use the actual date from filename
                year = f"20{delivery_yy}"
                return f"{year}-{mm}-{dd}"

        # Pattern: D[YY]MMDD without Y prefix - Beginning of year data (e.g., D249999, D259999)
        # These represent beginning of year snapshots (January 1st)
        match = re.search(r"^[^Y]*\.D(\d{2})(\d{4})", filename)
        if match:
            yy = match.group(1)
            mmdd = match.group(2)

            # Convert 2-digit year to 4-digit
            year = f"20{yy}"

            # D249999 or D259999 without Y prefix = beginning of year data
            if mmdd == "9999":
                return f"{year}-01-01"

            # Otherwise parse the actual month/day
            mm = mmdd[:2]
            dd = mmdd[2:]
            if 1 <= int(mm) <= 12:
                return f"{year}-{mm}-{dd}"

        return None

    except Exception:  # ALLOWED: Returns None to indicate error
        return None
