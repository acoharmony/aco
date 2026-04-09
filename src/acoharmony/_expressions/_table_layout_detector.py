# © 2025 HarmonyCares
# All rights reserved.

"""
Table layout detector for identifying Excel sheet structure types.

WHY THIS EXISTS
===============
Excel reports use different layouts: metadata sheets are key-value pairs (2 columns),
data tables are wide with many columns, payment tables have multi-level headers.
Parser logic must adapt based on layout type - key-value needs pivoting, tables need
multi-level header extraction, etc.

Without auto-detection, schemas must hardcode layout types and fail when reports change.
This expression automatically classifies sheet structure to route to correct handlers.

PROBLEM SOLVED
==============
Sheet "REPORT_PARAMETERS": 2 columns, 20 rows → Long Key-Value layout → Use pivot
Sheet "PAYMENT_HISTORY": 4 columns, 14 rows → Wide Tabular → Use standard parser
Sheet "BASE_PCC_PMT_DETAILED": 15 columns, 78 rows, multi-row headers → Multi-Level → Use multi-level header extractor

 functionality for analyzing Excel sheet structures
and classifying them into layout types for appropriate handler routing.

Core Concepts

**Layout Types**: Different table structure patterns
  - Long Key-Value: 2 columns with key-value pairs
  - Wide Tabular: Standard multi-column table
  - Multi-Level Header: Headers spanning multiple rows
  - Mixed: Multiple sub-tables or complex layouts

**Automatic Detection**: Analyze structure to determine type
  - Column count analysis
  - Header row patterns
  - Cell density patterns
  - Data distribution analysis

Use Cases

**Parser Routing**: Route to appropriate handler based on layout
**Validation**: Verify expected structure matches actual
**Dynamic Processing**: Adapt processing based on detected layout
**Error Detection**: Identify unexpected or malformed layouts

Key Features

- Multiple layout type detection
- Confidence scoring for detection
- Configurable detection rules
- Extensible for new layout types
True
True
"""

from enum import StrEnum

import polars as pl
from pydantic import BaseModel, Field

from .._trace import TracerWrapper
from ._registry import register_expression

tracer = TracerWrapper("expression.table_layout_detector")


class TableLayout(StrEnum):
    """
    Enumeration of detected table layout types.

        Attributes

        LONG_KEY_VALUE : str
            Two-column format with key-value pairs.
            Example: First column is labels, second column is values

        WIDE_TABULAR : str
            Standard multi-column table format.
            Example: Header row with multiple data columns

        MULTI_LEVEL_HEADER : str
            Headers spanning multiple rows.
            Example: Hierarchical column structure

        MIXED : str
            Complex layout with multiple sub-tables or irregular structure.
            Example: Multiple distinct data regions

        UNKNOWN : str
            Unable to determine layout type.
            Example: Empty or malformed data
    """

    LONG_KEY_VALUE = "long_key_value"
    WIDE_TABULAR = "wide_tabular"
    MULTI_LEVEL_HEADER = "multi_level_header"
    MIXED = "mixed"
    UNKNOWN = "unknown"


class LayoutDetectionResult(BaseModel):
    """
    Result of layout detection analysis.

        Attributes

        layout : TableLayout
            Detected layout type

        confidence : float
            Confidence score (0.0 to 1.0)

        reasons : list[str]
            List of reasons supporting this detection

        column_count : int
            Number of columns in the data

        row_count : int
            Number of rows in the data
    """

    layout: TableLayout = Field(description="Detected layout type")
    confidence: float = Field(ge=0.0, le=1.0, description="Confidence score")
    reasons: list[str] = Field(default_factory=list, description="Detection reasons")
    column_count: int = Field(description="Number of columns")
    row_count: int = Field(description="Number of rows")


@register_expression(
    "table_layout_detector",
    schemas=["bronze"],
    description="Detect and classify Excel sheet layout types",
)
class TableLayoutDetector:
    """
    Detect table layout types from DataFrame structure.

        TableLayoutDetector analyzes DataFrame structure and patterns to
        determine what type of layout it represents. This enables routing
        to appropriate handlers for different sheet structures.

        The class handles:
        - Layout type classification
        - Confidence scoring
        - Pattern recognition
        - Structure validation

    """

    @staticmethod
    def is_long_key_value(df: pl.DataFrame) -> tuple[bool, float, list[str]]:
        """
        Check if DataFrame matches long key-value pattern.

                Parameters

                df : pl.DataFrame
                    Input DataFrame

                Returns

                tuple[bool, float, list[str]]
                    (is_match, confidence, reasons)
        """
        reasons = []
        confidence = 0.0

        # Must have exactly 2 columns
        if len(df.columns) != 2:
            return False, 0.0, ["Not 2 columns"]

        reasons.append("Has 2 columns")
        confidence += 0.4

        # Should have at least a few rows
        if df.height >= 3:
            reasons.append(f"Has {df.height} rows (sufficient for key-value)")
            confidence += 0.3

        # First column should have high text density (keys are typically text)
        first_col = df[df.columns[0]]
        non_null_first = first_col.drop_nulls().len()

        if non_null_first >= df.height * 0.8:
            reasons.append("First column has high non-null density (keys)")
            confidence += 0.3

        # Check if values in first column look like labels
        # (contain spaces, longer strings)
        if non_null_first > 0:
            first_col_values = first_col.drop_nulls()
            avg_length = first_col_values.str.len_chars().mean()

            if avg_length and avg_length > 5:
                reasons.append("First column has label-like strings")
                confidence += 0.2

        is_match = confidence >= 0.7
        return is_match, min(confidence, 1.0), reasons

    @staticmethod
    def is_wide_tabular(df: pl.DataFrame) -> tuple[bool, float, list[str]]:
        """
        Check if DataFrame matches wide tabular pattern.

                Parameters

                df : pl.DataFrame
                    Input DataFrame

                Returns

                tuple[bool, float, list[str]]
                    (is_match, confidence, reasons)
        """
        reasons = []
        confidence = 0.0

        # Should have more than 2 columns
        if len(df.columns) > 2:
            reasons.append(f"Has {len(df.columns)} columns")
            confidence += 0.4
        else:
            return False, 0.0, ["Not enough columns"]

        # Should have at least 2 data rows
        if df.height >= 2:
            reasons.append(f"Has {df.height} data rows")
            confidence += 0.3

        # Check if data is relatively uniform across columns
        # (not sparse like key-value)
        densities = []
        for col in df.columns:
            non_null = df[col].drop_nulls().len()
            density = non_null / df.height if df.height > 0 else 0
            densities.append(density)

        avg_density = sum(densities) / len(densities) if densities else 0

        if avg_density >= 0.5:
            reasons.append(f"Average column density: {avg_density:.2f}")
            confidence += 0.3

        is_match = confidence >= 0.7
        return is_match, min(confidence, 1.0), reasons

    @staticmethod
    def is_multi_level_header(df: pl.DataFrame) -> tuple[bool, float, list[str]]:
        """
        Check if DataFrame has multi-level header pattern.

                This is harder to detect from a processed DataFrame, but we can
                look for patterns like:
                - First few rows have high similarity (header rows)
                - Column names are generic (col_0, col_1, etc.)

                Parameters

                df : pl.DataFrame
                    Input DataFrame

                Returns

                tuple[bool, float, list[str]]
                    (is_match, confidence, reasons)
        """
        reasons = []
        confidence = 0.0

        # Check if column names are generic (col_0, col_1, etc.)
        generic_pattern = all(
            col.startswith("col_") or col.startswith("column_") for col in df.columns
        )

        if generic_pattern:
            reasons.append("Has generic column names (likely raw Excel columns)")
            confidence += 0.5

        # Check if first 2-3 rows have similar patterns
        # (might be header rows)
        if df.height >= 3:
            # Look at data types - header rows often all strings
            first_rows_all_strings = True
            for row_idx in range(min(3, df.height)):
                row_data = df.row(row_idx)
                for val in row_data:
                    if val is not None and not isinstance(val, str):
                        first_rows_all_strings = False
                        break

            if first_rows_all_strings:
                reasons.append("First rows are all strings (potential headers)")
                confidence += 0.3

        is_match = confidence >= 0.5
        return is_match, min(confidence, 1.0), reasons

    @staticmethod
    def detect(df_raw: pl.DataFrame) -> TableLayout:
        """
        Detect the most likely table layout type.

                Parameters

                df_raw : pl.DataFrame
                    Raw DataFrame to analyze

                Returns

                TableLayout
                    Detected layout type
                <TableLayout.LONG_KEY_VALUE: 'long_key_value'>

                Wide tabular detection:
                <TableLayout.WIDE_TABULAR: 'wide_tabular'>

                Multi-level header detection:
                <TableLayout.MULTI_LEVEL_HEADER: 'multi_level_header'>
        """
        if df_raw.height == 0 or len(df_raw.columns) == 0:
            return TableLayout.UNKNOWN

        # Check each layout type
        is_kv, kv_confidence, kv_reasons = TableLayoutDetector.is_long_key_value(df_raw)
        is_wide, wide_confidence, wide_reasons = TableLayoutDetector.is_wide_tabular(df_raw)
        is_multi, multi_confidence, multi_reasons = TableLayoutDetector.is_multi_level_header(
            df_raw
        )

        # Select layout with highest confidence
        results = [
            (TableLayout.LONG_KEY_VALUE, kv_confidence),
            (TableLayout.WIDE_TABULAR, wide_confidence),
            (TableLayout.MULTI_LEVEL_HEADER, multi_confidence),
        ]

        # Sort by confidence
        results.sort(key=lambda x: x[1], reverse=True)

        # Return highest confidence layout
        best_layout, best_confidence = results[0]

        if best_confidence >= 0.5:
            return best_layout
        else:
            return TableLayout.UNKNOWN

    @staticmethod
    def detect_with_details(df_raw: pl.DataFrame) -> LayoutDetectionResult:
        """
        Detect layout with detailed results.

                Parameters

                df_raw : pl.DataFrame
                    Raw DataFrame to analyze

                Returns

                LayoutDetectionResult
                    Detailed detection result with confidence and reasons
        """
        if df_raw.height == 0 or len(df_raw.columns) == 0:
            return LayoutDetectionResult(
                layout=TableLayout.UNKNOWN,
                confidence=0.0,
                reasons=["Empty DataFrame"],
                column_count=len(df_raw.columns),
                row_count=df_raw.height,
            )

        # Check each layout type
        is_kv, kv_confidence, kv_reasons = TableLayoutDetector.is_long_key_value(df_raw)
        is_wide, wide_confidence, wide_reasons = TableLayoutDetector.is_wide_tabular(df_raw)
        is_multi, multi_confidence, multi_reasons = TableLayoutDetector.is_multi_level_header(
            df_raw
        )

        # Select layout with highest confidence
        results = [
            (TableLayout.LONG_KEY_VALUE, kv_confidence, kv_reasons),
            (TableLayout.WIDE_TABULAR, wide_confidence, wide_reasons),
            (TableLayout.MULTI_LEVEL_HEADER, multi_confidence, multi_reasons),
        ]

        # Sort by confidence
        results.sort(key=lambda x: x[1], reverse=True)

        # Return highest confidence layout
        best_layout, best_confidence, best_reasons = results[0]

        if best_confidence < 0.5:
            best_layout = TableLayout.UNKNOWN
            best_reasons = ["No layout matched with sufficient confidence"]

        return LayoutDetectionResult(
            layout=best_layout,
            confidence=best_confidence,
            reasons=best_reasons,
            column_count=len(df_raw.columns),
            row_count=df_raw.height,
        )
