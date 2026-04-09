# © 2025 HarmonyCares
# All rights reserved.

"""
Dynamic meta-detection for multi-level headers with dynamic information.

WHY THIS EXISTS
===============
CMS payment reports have time-series columns (Jan, Feb, Mar...) that change per report period.
Cannot hardcode column names like "jan_amount" in schema because next year it's different months.
This expression detects the pattern (Month + Amount) and dynamically generates column names
based on actual header values, enabling schemas to work across different time periods.

PROBLEM SOLVED
==============
Payment report has columns: ["Jan", "Feb", "Mar"] × ["Amount", "Count"]
Next period: ["Apr", "May", "Jun"] × ["Amount", "Count"]

Schema needs ONE config that works for both, not hardcoded column lists.
This expression detects the repeating pattern and generates: jan_amount, jan_count, feb_amount, ...

 functionality for detecting and extracting metadata patterns
from multi-level headers where column information varies dynamically (months,
years, quarters, etc.).

Core Concepts

**Dynamic Headers**: Headers where values change but pattern is consistent
  Example 1 - Monthly data:
    Row 1: ["Jan", "Feb", "Mar", "Apr", ...]
    Row 2: ["Amount", "Amount", "Amount", "Amount", ...]
    Result: jan_amount, feb_amount, mar_amount, apr_amount

  Example 2 - Quarterly data with years:
    Row 1: ["2024", "2024", "2024", "2024", "2025", ...]
    Row 2: ["Q1", "Q2", "Q3", "Q4", "Q1", ...]
    Row 3: ["Sales", "Sales", "Sales", "Sales", "Sales", ...]
    Result: 2024_q1_sales, 2024_q2_sales, ..., 2025_q1_sales

**Meta Detection**: Automatically identify patterns
  - Month names (Jan, January, 01, etc.)
  - Years (2024, 24, etc.)
  - Quarters (Q1, Quarter 1, etc.)
  - Custom repeating patterns

**Value Propagation**: Forward-fill sparse headers
Use Cases

**Financial Reports**: Monthly/quarterly columns with repeating structure
**Time Series Data**: Date-based columns with metric names
**Multi-Period Analysis**: Year-over-year comparisons
**Hierarchical Metrics**: Category + Subcategory + Value patterns

Key Features

- Pattern recognition for common date/time formats
- Forward-fill for sparse parent headers
- Custom pattern detection
- Metadata extraction (month numbers, year values)
- Flexible column naming strategies
"""

from enum import StrEnum
from typing import Any

import polars as pl
from pydantic import BaseModel, Field

from .._decor8 import explain, timeit, traced
from ._registry import register_expression


class HeaderPattern(StrEnum):
    """Enumeration of recognized header patterns."""

    MONTH_ABBR = "month_abbr"  # Jan, Feb, Mar
    MONTH_FULL = "month_full"  # January, February
    MONTH_NUM = "month_num"  # 01, 02, 03
    QUARTER = "quarter"  # Q1, Q2, Q3, Q4
    YEAR_FULL = "year_full"  # 2024, 2025
    YEAR_SHORT = "year_short"  # 24, 25
    CUSTOM = "custom"  # User-defined pattern
    REPEATING = "repeating"  # Same value repeated
    UNKNOWN = "unknown"


class DynamicMetaConfig(BaseModel):
    """
    Configuration for dynamic meta-detection.

        Attributes

        header_rows : list[int]
            List of 0-indexed row numbers containing headers.

        separator : str
            String used to join header parts.
            Default: "_"

        forward_fill_sparse : bool
            Whether to forward-fill empty cells in parent rows.
            Default: True

        sanitize_names : bool
            Whether to sanitize final column names.
            Default: True

        detect_patterns : bool
            Whether to detect common patterns (months, quarters, years).
            Default: True

        custom_patterns : Optional[dict[str, list[str]]]
            Custom pattern definitions.
            Example: {"region": ["North", "South", "East", "West"]}
            Default: None
    """

    header_rows: list[int] = Field(description="0-indexed row numbers containing headers")
    separator: str = Field(default="_", description="Separator for joining header parts")
    forward_fill_sparse: bool = Field(
        default=True, description="Forward-fill empty cells in parent rows"
    )
    sanitize_names: bool = Field(default=True, description="Sanitize final column names")
    detect_patterns: bool = Field(
        default=True, description="Detect common patterns (months, quarters, years)"
    )
    custom_patterns: dict[str, list[str]] | None = Field(
        default=None, description="Custom pattern definitions"
    )


class ColumnMetadata(BaseModel):
    """
    Metadata extracted from a column header.

        Attributes

        column_name : str
            Generated column name

        patterns : dict[str, str]
            Detected patterns for each header level
            Example: {"level_0": "year_full", "level_1": "quarter"}

        values : dict[str, str]
            Actual values for each header level
            Example: {"level_0": "2024", "level_1": "Q1"}

        metadata : dict[str, Any]
            Extracted metadata (month numbers, year values, etc.)
            Example: {"year": 2024, "quarter": 1}
    """

    column_name: str
    patterns: dict[str, str] = Field(default_factory=dict)
    values: dict[str, str] = Field(default_factory=dict)
    metadata: dict[str, Any] = Field(default_factory=dict)


@register_expression(
    "dynamic_meta_detect",
    schemas=["bronze", "silver"],
    description="Detect and extract metadata from dynamic multi-level headers",
)
class DynamicMetaDetectExpression:
    """
    Dynamic meta-detection for multi-level headers with varying values.

        This expression identifies patterns in multi-level headers and extracts
        metadata to create meaningful column names with associated metadata.
    """

    MONTHS_ABBR = [
        "Jan",
        "Feb",
        "Mar",
        "Apr",
        "May",
        "Jun",
        "Jul",
        "Aug",
        "Sep",
        "Oct",
        "Nov",
        "Dec",
    ]
    MONTHS_FULL = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]

    QUARTERS = ["Q1", "Q2", "Q3", "Q4", "Quarter 1", "Quarter 2", "Quarter 3", "Quarter 4"]

    @staticmethod
    def detect_pattern(
        value: str, custom_patterns: dict[str, list[str]] | None = None
    ) -> HeaderPattern:
        """
        Detect the pattern type of a header value.

                Parameters

                value : str
                    Header value to analyze

                custom_patterns : Optional[dict[str, list[str]]]
                    Custom pattern definitions

                Returns

                HeaderPattern
                    Detected pattern type
        """
        value_clean = str(value).strip()

        # Check custom patterns first
        if custom_patterns:
            for _pattern_name, pattern_values in custom_patterns.items():
                if value_clean in pattern_values:
                    return HeaderPattern.CUSTOM

        # Check month abbreviations
        if value_clean in DynamicMetaDetectExpression.MONTHS_ABBR:
            return HeaderPattern.MONTH_ABBR

        # Check full month names
        if value_clean in DynamicMetaDetectExpression.MONTHS_FULL:
            return HeaderPattern.MONTH_FULL

        # Check month numbers (01-12)
        if value_clean.isdigit() and 1 <= int(value_clean) <= 12 and len(value_clean) <= 2:
            return HeaderPattern.MONTH_NUM

        # Check quarters
        if value_clean in DynamicMetaDetectExpression.QUARTERS or value_clean.upper() in [
            "Q1",
            "Q2",
            "Q3",
            "Q4",
        ]:
            return HeaderPattern.QUARTER

        # Check year (full)
        if value_clean.isdigit() and len(value_clean) == 4 and 1900 <= int(value_clean) <= 2100:
            return HeaderPattern.YEAR_FULL

        # Check year (short)
        if value_clean.isdigit() and len(value_clean) == 2:
            return HeaderPattern.YEAR_SHORT

        return HeaderPattern.UNKNOWN

    @staticmethod
    def extract_metadata(value: str, pattern: HeaderPattern) -> dict[str, Any]:
        """
        Extract metadata from a header value based on its pattern.

                Parameters

                value : str
                    Header value

                pattern : HeaderPattern
                    Detected pattern type

                Returns

                dict[str, Any]
                    Extracted metadata
        """
        metadata = {}
        value_clean = str(value).strip()

        if pattern == HeaderPattern.MONTH_ABBR:
            month_idx = DynamicMetaDetectExpression.MONTHS_ABBR.index(value_clean)
            metadata["month"] = month_idx + 1
            metadata["month_name"] = DynamicMetaDetectExpression.MONTHS_FULL[month_idx]
            metadata["month_abbr"] = value_clean

        elif pattern == HeaderPattern.MONTH_FULL:
            month_idx = DynamicMetaDetectExpression.MONTHS_FULL.index(value_clean)
            metadata["month"] = month_idx + 1
            metadata["month_name"] = value_clean
            metadata["month_abbr"] = DynamicMetaDetectExpression.MONTHS_ABBR[month_idx]

        elif pattern == HeaderPattern.MONTH_NUM:
            month_num = int(value_clean)
            metadata["month"] = month_num
            metadata["month_name"] = DynamicMetaDetectExpression.MONTHS_FULL[month_num - 1]
            metadata["month_abbr"] = DynamicMetaDetectExpression.MONTHS_ABBR[month_num - 1]

        elif pattern == HeaderPattern.QUARTER:
            if value_clean.upper() in ["Q1", "Q2", "Q3", "Q4"]:
                quarter_num = int(value_clean[1])
            else:
                # "Quarter 1" format
                quarter_num = int(value_clean.split()[-1])
            metadata["quarter"] = quarter_num

        elif pattern == HeaderPattern.YEAR_FULL:
            metadata["year"] = int(value_clean)

        elif pattern == HeaderPattern.YEAR_SHORT:
            # Assume 20XX for 00-50, 19XX for 51-99
            year_short = int(value_clean)
            if year_short <= 50:
                metadata["year"] = 2000 + year_short
            else:
                metadata["year"] = 1900 + year_short

        return metadata

    @staticmethod
    def forward_fill_row(row_values: list) -> list:
        """
        Forward-fill empty values in a row.

                Parameters

                row_values : list
                    Row values to fill

                Returns

                list
                    Forward-filled row values
        """
        filled = []
        last_value = None

        for value in row_values:
            if value is not None and str(value).strip() != "":
                last_value = value
                filled.append(value)
            else:
                filled.append(last_value if last_value is not None else "")

        return filled

    @staticmethod
    def extract_dynamic_headers(
        df_raw: pl.DataFrame, config: DynamicMetaConfig
    ) -> dict[str, ColumnMetadata]:
        """
        Extract dynamic headers with metadata.

                Parameters

                df_raw : pl.DataFrame
                    Raw DataFrame with headers in data rows

                config : DynamicMetaConfig
                    Configuration for extraction

                Returns

                dict[str, ColumnMetadata]
                    Mapping from original column names to metadata
        """
        column_metadata = {}
        original_columns = df_raw.columns

        header_data = []
        for row_idx in config.header_rows:
            row_values = df_raw.row(row_idx)

            if config.forward_fill_sparse:
                row_values = DynamicMetaDetectExpression.forward_fill_row(list(row_values))

            header_data.append(row_values)

        for col_idx, col_name in enumerate(original_columns):
            col_header_values = [
                header_data[row_idx][col_idx] for row_idx in range(len(config.header_rows))
            ]

            metadata = ColumnMetadata(column_name="", patterns={}, values={}, metadata={})

            header_parts = []
            combined_metadata = {}

            for level_idx, value in enumerate(col_header_values):
                value_str = str(value).strip() if value is not None else ""

                if value_str:
                    if config.detect_patterns:
                        pattern = DynamicMetaDetectExpression.detect_pattern(
                            value_str, config.custom_patterns
                        )
                    else:
                        pattern = HeaderPattern.UNKNOWN

                    metadata.patterns[f"level_{level_idx}"] = pattern.value
                    metadata.values[f"level_{level_idx}"] = value_str

                    level_metadata = DynamicMetaDetectExpression.extract_metadata(
                        value_str, pattern
                    )
                    combined_metadata.update(level_metadata)

                    header_parts.append(value_str)

            if header_parts:
                combined_name = config.separator.join(header_parts)
            else:
                combined_name = col_name  # Fallback

            if config.sanitize_names:
                from ._multi_level_header import MultiLevelHeaderExpression

                combined_name = MultiLevelHeaderExpression.sanitize_column_name(combined_name)

            metadata.column_name = combined_name
            metadata.metadata = combined_metadata

            column_metadata[col_name] = metadata

        return column_metadata

    @traced()
    @explain(
        why="Apply failed",
        how="Check configuration and input data are valid",
        causes=["Invalid config", "Missing required fields", "Data processing error"],
    )
    @timeit(log_level="debug")
    @staticmethod
    def apply(
        df: pl.LazyFrame, config: DynamicMetaConfig, data_start_row: int | None = None
    ) -> tuple[pl.LazyFrame, dict[str, ColumnMetadata]]:
        """
        Apply dynamic meta-detection to a LazyFrame.

                Parameters

                df : pl.LazyFrame
                    Input LazyFrame

                config : DynamicMetaConfig
                    Configuration for detection

                data_start_row : Optional[int]
                    Row index where data starts. If None, uses max(header_rows) + 1

                Returns

                tuple[pl.LazyFrame, dict[str, ColumnMetadata]]
                    Renamed LazyFrame and column metadata
        """
        df_collected = df.collect()

        column_metadata = DynamicMetaDetectExpression.extract_dynamic_headers(df_collected, config)

        column_mapping = {orig_col: meta.column_name for orig_col, meta in column_metadata.items()}

        if data_start_row is None:
            data_start_row = max(config.header_rows) + 1

        df_data = df_collected.slice(data_start_row)
        df_renamed = df_data.rename(column_mapping)

        return df_renamed.lazy(), column_metadata
