# © 2025 HarmonyCares
# All rights reserved.

"""
ACO ID extraction from source filenames.

 functions to extract ACO identifiers and program types from
various filename patterns used in ACO (Accountable Care Organization) data files
from CMS programs like REACH (Realizing Equity, Access, and Community Health) and
MSSP (Medicare Shared Savings Program).

What is ACO ID Extraction?

ACO ID extraction identifies the specific ACO organization and program type from
standardized filename conventions used in CMS data files. This metadata is critical
for:

- **Data Source Tracking**: Linking claims/enrollment data to specific ACOs
- **Multi-ACO Processing**: Processing data from multiple ACOs in batch pipelines
- **Program-Specific Logic**: Applying REACH vs MSSP specific business rules
- **File Organization**: Routing files to correct processing pipelines
- **Audit and Lineage**: Tracking data provenance back to source ACO and program

Key Concepts

**ACO ID Patterns**:
    Standard CMS formats for ACO identification:

    - **REACH ACOs**: D prefix + 4+ digits (e.g., D0259, D1234)
    - **MSSP ACOs**: A prefix + 4+ digits (e.g., A2671, A5432)
    - **Generic**: ACO-#### or ACO#### format (e.g., ACO-1234, ACO5678)
    - **Legacy**: P.D#### for REACH, V.## for MSSP (older format)

**Program Types**:
    CMS Value-Based Care programs with different rules:

    - **REACH**: Risk-adjusted capitation model for underserved populations
    - **MSSP**: Shared savings track 1/2/3 with upside/downside risk
    - Program type determines file formats, data elements, payment logic


Common Use Cases

1. **Automated File Routing**:
   Parse incoming files, extract ACO ID, route to correct processing pipeline
   based on program type and ACO-specific configurations.

2. **Multi-ACO Batch Processing**:
   Process directory of files from multiple ACOs, group by ACO ID, apply
   ACO-specific transformations and validation rules.

3. **Data Source Metadata**:
   Add ``aco_id`` and ``program`` columns to parsed data for tracking, filtering,
   and reporting by ACO organization.

4. **Program-Specific Logic**:
   Branch processing based on REACH vs MSSP (different schemas, payment models,
   quality measures, risk adjustment methodologies).

5. **File Validation**:
   Verify files contain expected ACO ID, flag mismatches between filename and
   file contents, detect misrouted files.

How It Works

**extract_aco_id()**:
    Uses regex patterns to search filename for ACO ID in priority order:

    1. Letter + digits pattern (D0259, A2671) - most common
    2. ACO-#### or ACO#### format - generic
    3. P.D#### or V.## format - legacy

    Returns first match found or None.

**extract_program_from_aco_id()**:
    Maps ACO ID prefix to program type:

    - D#### → REACH
    - A#### → MSSP
    - P.D#### → REACH (legacy)
    - V.## → MSSP (legacy)

**extract_program_from_filename()**:
    Fallback extraction when ACO ID doesn't indicate program:

    1. Check for "REACH" or "MSSP" in filename
    2. Extract ACO ID and map to program
    3. Return None if no program identified

Pipeline Position

ACO ID extraction typically happens at the **earliest stage of file ingestion**:

**Pipeline Flow**::

    File Discovery → [ACO ID EXTRACTION] → File Routing → Parsing → Transforms

- **Before Parsing**: Extract ACO ID from filename before file is opened
- **File Routing**: Use ACO ID to select correct parser and schema
- **Metadata Addition**: Add ``aco_id`` and ``program`` columns during parsing
- **Transform Configuration**: Load ACO-specific configs for downstream transforms

Performance Considerations

- **Lightweight Regex**: Simple pattern matching, negligible overhead
- **No File I/O**: Operates only on filename string, no disk access
- **Memoization Opportunity**: Cache results for repeated calls on same filename
- **Batch Efficiency**: Extract once per file, reuse for all rows in file
"""

import re


def extract_aco_id(filename: str) -> str | None:
    """
    Extract ACO ID from source filename.

        ACO IDs follow patterns like:
        - D0259 (REACH - D prefix)
        - A2671 (MSSP - A prefix)
        - ACO-1234 (Generic format)

        Parameters

        filename : str
            Source filename to extract ACO ID from

        Returns

        str or None
            ACO ID if found, None otherwise. Result is always uppercased.
    """
    if not filename:
        return None

    # Pattern 1: Letter followed by digits (most common)
    # Examples: D0259 (REACH), A2671 (MSSP)
    match = re.search(r"([A-Z]\d{4,})", filename, re.IGNORECASE)
    if match:
        return match.group(1).upper()

    # Pattern 2: ACO-#### or ACO#### format
    # Examples: ACO-1234, ACO1234
    match = re.search(r"(ACO-?\d+)", filename, re.IGNORECASE)
    if match:
        return match.group(1).upper()

    # Pattern 3: P.D#### or V.## format (older format)
    # Examples: P.D0259, V.36
    match = re.search(r"([PV]\.[A-Z0-9]+)", filename, re.IGNORECASE)
    if match:
        return match.group(1).upper()

    return None


def extract_program_from_aco_id(aco_id: str) -> str | None:
    """
    Determine program type from ACO ID.

        Parameters

        aco_id : str
            ACO identifier (e.g., 'D0259', 'A2671')

        Returns

        str or None
            Program name ('REACH', 'MSSP') or None if program cannot be determined
    """
    if not aco_id:
        return None

    aco_upper = aco_id.upper()

    # D prefix indicates REACH (e.g., D0259)
    if aco_upper.startswith("D") and aco_upper[1:].isdigit():
        return "REACH"

    # A prefix indicates MSSP (e.g., A2671)
    if aco_upper.startswith("A") and len(aco_upper) > 1 and aco_upper[1:].isdigit():
        return "MSSP"

    # Legacy formats
    # P.D prefix indicates REACH
    if aco_upper.startswith("P.D"):
        return "REACH"

    # V. prefix indicates MSSP
    if aco_upper.startswith("V."):
        return "MSSP"

    return None


def extract_program_from_filename(filename: str) -> str | None:
    """
    Extract program type from filename patterns.

        This is a fallback when ACO ID is not available or doesn't
        clearly indicate the program. Uses multiple strategies:
        1. Check for "REACH" or "MSSP" in filename
        2. Extract ACO ID and map to program

        Parameters

        filename : str
            Source filename

        Returns

        str or None
            Program name ('REACH', 'MSSP') or None if program cannot be determined
    """
    if not filename:
        return None

    filename_upper = filename.upper()

    # Check for REACH indicators
    if "REACH" in filename_upper:
        return "REACH"

    # Check for MSSP indicators
    if "MSSP" in filename_upper:
        return "MSSP"

    # Check based on ACO ID if present
    aco_id = extract_aco_id(filename)
    if aco_id:
        return extract_program_from_aco_id(aco_id)

    return None
