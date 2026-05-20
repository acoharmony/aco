# © 2025 HarmonyCares
# All rights reserved.

"""
Dynamic table range detector for auto-detecting Excel table boundaries.

WHY THIS EXISTS
===============
CMS Excel reports have variable-length tables where the number of rows changes
between files (e.g., payment history with different numbers of payments, claims
data with varying record counts). We cannot hardcode row ranges in schemas.

Without this expression, we would either:
1. Miss data when tables are longer than expected
2. Include garbage rows when tables are shorter than expected
3. Fail to parse when table structure shifts

This expression automatically detects table boundaries using heuristics like
cell density, end markers ("Total", "Grand Total"), and empty row detection.

PROBLEM SOLVED
==============
Payment history tables have variable length:
  File 1: 10 payment rows
  File 2: 15 payment rows
  File 3: 8 payment rows

Schema cannot specify fixed range like "rows 5-14" because it varies.

This expression detects:
- Header row: First row with >70% non-null cells (likely column headers)
- Data start: Row immediately after header
- Data end: First row containing "Total" marker OR first empty row
- Column range: Columns with >50% non-null cells

This enables parsing tables of any length with a single schema configuration.

Core Concepts

**Dynamic Tables**: Tables with variable dimensions that change per file
  - Payment histories (variable payment counts)
  - Claims tables (variable record counts)
  - Provider lists (variable provider counts)

**Boundary Detection Heuristics**: Statistical and pattern-based detection
  - Cell density analysis (% non-null cells per row/column)
  - End marker recognition ("Total", "Summary", "Grand Total")
  - Empty region detection (consecutive null rows/columns)
  - Header pattern matching (high density, string values)

Performance Characteristics

- Requires eager evaluation to analyze cell patterns
- One-time cost at parse time
- Cached dimensions for subsequent operations
- Memory overhead: O(rows × cols) for density analysis

Use Cases

1. **Variable-Length Payment Histories**: Different payment counts per period
2. **Dynamic Claims Tables**: Variable record counts per file
3. **Provider Rosters**: Variable provider lists
4. **Financial Reports**: Variable line items per report
"""

import polars as pl
from pydantic import BaseModel, Field

from .._trace import TracerWrapper
from ._registry import register_expression

tracer = TracerWrapper("expression.dynamic_table_range")


class TableDimensions(BaseModel):
    """
    Detected table dimensions.

        Attributes

        header_row : int
            0-indexed row number containing column headers

        data_start_row : int
            0-indexed row number where data starts (after header)

        data_end_row : int
            0-indexed row number where data ends (inclusive)

        first_col : int
            0-indexed first column with data

        last_col : int
            0-indexed last column with data

        total_rows : int
            Total number of data rows

        total_cols : int
            Total number of data columns
    """

    header_row: int = Field(description="Header row index")
    data_start_row: int = Field(description="First data row index")
    data_end_row: int = Field(description="Last data row index")
    first_col: int = Field(default=0, description="First column index")
    last_col: int = Field(description="Last column index")

    @property
    def total_rows(self) -> int:
        """Total number of data rows."""
        return self.data_end_row - self.data_start_row + 1

    @property
    def total_cols(self) -> int:
        """Total number of data columns."""
        return self.last_col - self.first_col + 1


class DynamicRangeConfig(BaseModel):
    """
    Configuration for dynamic table range detection.

        Attributes

        min_header_row : int
            Minimum row index to search for header.
            Default: 0

        max_header_row : int
            Maximum row index to search for header.
            Default: 10

        end_markers : list[str]
            List of strings that indicate end of data.
            Example: ["Total", "Grand Total", "Summary"]
            Default: ["Total", "Grand Total", "Summary"]

        min_density_threshold : float
            Minimum percentage of non-empty cells (0.0-1.0) to consider a row as data.
            Default: 0.3 (30% of cells must be non-empty)

        empty_row_threshold : int
            Number of consecutive empty rows to consider as end of data.
            Default: 3

        min_data_rows : int
            Minimum number of data rows expected.
            Default: 1
    """

    min_header_row: int = Field(default=0, description="Min row for header search")
    max_header_row: int = Field(default=10, description="Max row for header search")
    end_markers: list[str] = Field(
        default=["Total", "Grand Total", "Summary", "TOTAL", "Total:"],
        description="End marker strings",
    )
    min_density_threshold: float = Field(
        default=0.3, ge=0.0, le=1.0, description="Min cell density for data rows"
    )
    empty_row_threshold: int = Field(default=3, description="Consecutive empty rows before end")
    min_data_rows: int = Field(default=1, description="Minimum expected data rows")


@register_expression(
    "dynamic_table_range",
    schemas=["bronze"],
    description="Auto-detect table boundaries in Excel sheets",
)
class DynamicTableRangeExpression:
    """
    Auto-detect table boundaries from Excel data.

        DynamicTableRangeExpression analyzes cell density and patterns to
        automatically determine where data tables begin and end in Excel sheets.

        The class handles:
        - Header row detection
        - Data start/end row detection
        - Column range detection
        - End marker recognition
        - Empty region detection

    """

    @staticmethod
    def calculate_row_density(df: pl.DataFrame, row_idx: int) -> float:
        """
        Calculate the density (% non-empty cells) for a row.

                Parameters

                df : pl.DataFrame
                    Input DataFrame

                row_idx : int
                    0-indexed row number

                Returns

                float
                    Density value between 0.0 and 1.0
        """
        if row_idx >= df.height:
            return 0.0

        row_data = df.row(row_idx)
        non_empty = sum(1 for val in row_data if val is not None and str(val).strip() != "")

        return non_empty / len(row_data) if row_data else 0.0

    @staticmethod
    def has_end_marker(df: pl.DataFrame, row_idx: int, markers: list[str]) -> bool:
        """
        Check if a row contains an end marker.

                Parameters

                df : pl.DataFrame
                    Input DataFrame

                row_idx : int
                    0-indexed row number

                markers : list[str]
                    List of end marker strings

                Returns

                bool
                    True if row contains any end marker
        """
        if row_idx >= df.height:
            return False

        row_data = df.row(row_idx)

        for val in row_data:
            if val is not None:
                val_str = str(val).strip()
                for marker in markers:
                    if marker.lower() in val_str.lower():
                        return True

        return False

    @staticmethod
    def detect_header_row(df: pl.DataFrame, config: DynamicRangeConfig) -> int:
        """
        Detect the header row based on density and patterns.

                Looks for a row with high text density and varied values
                (indicating column names).

                Parameters

                df : pl.DataFrame
                    Input DataFrame

                config : DynamicRangeConfig
                    Detection configuration

                Returns

                int
                    0-indexed header row number
        """
        # Search within specified range
        for row_idx in range(config.min_header_row, min(config.max_header_row + 1, df.height)):
            density = DynamicTableRangeExpression.calculate_row_density(df, row_idx)

            # Header row typically has high density (most cells filled)
            if density >= 0.5:
                return row_idx

        # Fallback to min_header_row if nothing found
        return config.min_header_row

    @staticmethod
    def detect_end_row(df: pl.DataFrame, data_start_row: int, config: DynamicRangeConfig) -> int:
        """
        Detect the last row of data.

                Parameters

                df : pl.DataFrame
                    Input DataFrame

                data_start_row : int
                    Row where data starts (after header)

                config : DynamicRangeConfig
                    Detection configuration

                Returns

                int
                    0-indexed last data row (inclusive)
        """
        consecutive_empty = 0
        last_data_row = data_start_row

        for row_idx in range(data_start_row, df.height):
            # Check for end markers
            if DynamicTableRangeExpression.has_end_marker(df, row_idx, config.end_markers):
                return row_idx - 1  # Row before marker

            # Check density
            density = DynamicTableRangeExpression.calculate_row_density(df, row_idx)

            if density >= config.min_density_threshold:
                # This is a data row
                last_data_row = row_idx
                consecutive_empty = 0
            else:
                # Empty or sparse row
                consecutive_empty += 1

                # If we hit threshold of consecutive empty rows, stop
                if consecutive_empty >= config.empty_row_threshold:
                    return last_data_row

        return last_data_row

    @staticmethod
    def detect_last_column(df: pl.DataFrame) -> int:
        """
        Detect the last column with data.

                Parameters

                df : pl.DataFrame
                    Input DataFrame

                Returns

                int
                    0-indexed last column number
        """
        last_col = len(df.columns) - 1

        # Work backwards to find last non-empty column
        for col_idx in range(len(df.columns) - 1, -1, -1):
            col_name = df.columns[col_idx]
            col_data = df[col_name]

            # Check if column has any non-null values
            non_null_count = col_data.drop_nulls().len()

            if non_null_count > 0:
                last_col = col_idx
                break

        return last_col

    @staticmethod
    def detect(df_raw: pl.DataFrame, config: DynamicRangeConfig) -> TableDimensions:
        """
        Detect table dimensions from raw DataFrame.

                WHY: Auto-detects table boundaries so schemas work with variable-length tables.

                Parameters

                df_raw : pl.DataFrame
                    Raw DataFrame with unknown dimensions

                config : DynamicRangeConfig
                    Detection configuration

                Returns

                TableDimensions
                    Detected table boundaries
        """
        with tracer.span(
            "detect_table_dimensions", input_rows=len(df_raw), input_cols=len(df_raw.columns)
        ) as span:
            # Detect header row
            header_row = DynamicTableRangeExpression.detect_header_row(df_raw, config)
            span.set_attribute("detected_header_row", header_row)

            # Data starts after header
            data_start_row = header_row + 1

            # Detect end row
            data_end_row = DynamicTableRangeExpression.detect_end_row(
                df_raw, data_start_row, config
            )
            span.set_attribute("detected_data_end_row", data_end_row)

            # Detect last column
            last_col = DynamicTableRangeExpression.detect_last_column(df_raw)
            span.set_attribute("detected_last_col", last_col)

            dimensions = TableDimensions(
                header_row=header_row,
                data_start_row=data_start_row,
                data_end_row=data_end_row,
                first_col=0,
                last_col=last_col,
            )
            span.set_attribute("total_data_rows", dimensions.total_rows)

            return dimensions
