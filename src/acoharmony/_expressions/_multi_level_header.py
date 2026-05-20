# © 2025 HarmonyCares
# All rights reserved.

"""
Multi-level header expression for combining multi-row headers in Excel reports.

WHY THIS EXISTS
===============
Healthcare and financial Excel reports from CMS (like PLARU, PALMR, BNMR) use
multi-level headers where column meanings span multiple rows. These headers
create hierarchical relationships that standard Excel parsing cannot capture.

Without this expression, we get generic column names (col_0, col_1) that lose
all semantic meaning. This expression preserves the hierarchical structure by
combining header rows into meaningful column names.

PROBLEM SOLVED
==============
Excel reports like CMS REACH payment reports use multi-level headers:
  Row 4: ["", "", "Base PCC", "Enhanced PCC", "APO"]
  Row 5: ["Payment Date", "Total", "Amount", "Amount", "Amount"]

Standard parsing creates: column_0, column_1, column_2, column_3, column_4

This expression creates: payment_date, total, base_pcc_amount, enhanced_pcc_amount, apo_amount

This preserves the parent-child relationships and makes data queryable.

Core Concepts

**Multi-Level Headers**: Headers spanning multiple rows with parent-child relationships
  - Parent row provides category (e.g., "Base PCC")
  - Child row provides specific field (e.g., "Amount")
  - Combined name captures both levels (e.g., "base_pcc_amount")

**Hierarchical Column Structure**: Common in CMS reports where columns group related data
  - Payment calculations by type
  - Financial metrics by category
  - Time series data by period

Performance Characteristics

- Requires collecting LazyFrame to read header rows (temporary eager evaluation)
- Returns LazyFrame to maintain pipeline laziness
- Header extraction is one-time cost at parse time
- No runtime overhead after column renaming

Use Cases

1. **CMS Payment Reports**: PLARU, PALMR with grouped payment columns
2. **Financial Reports**: Multi-level categorization of amounts
3. **Time Series Data**: Period headers with metric sub-headers
4. **Hierarchical Metrics**: Category/subcategory column structures

Configuration Schema

```yaml
multi_level_header:
  header_rows: [3, 4]      # 0-indexed rows containing headers
  separator: "_"           # Join character for combining levels
  skip_empty_parts: true   # Skip null cells when combining
  sanitize_names: true     # Clean special chars, lowercase
```
"""

import polars as pl
from pydantic import BaseModel, Field

from .._decor8 import explain, timeit, traced
from .._trace import TracerWrapper
from ._registry import register_expression

tracer = TracerWrapper("expression.multi_level_header")


class MultiLevelHeaderConfig(BaseModel):
    """
    Configuration for multi-level header extraction.

        Attributes

        header_rows : list[int]
            List of 0-indexed row numbers containing header information.
            Example: [3, 4] means rows 3 and 4 contain headers.

        separator : str
            String used to join header parts.
            Default: "_"

        skip_empty_parts : bool
            Whether to skip empty/null parts when combining headers.
            Default: True

        sanitize_names : bool
            Whether to sanitize final column names (lowercase, clean special chars).
            Default: True

        forward_fill : bool
            Whether to forward-fill None/empty values in each header row.
            This handles spanning headers where a label applies to multiple columns.
            Example: "Projected Experience" spans Jan/Feb/Mar columns.
            Default: False
    """

    header_rows: list[int] = Field(description="0-indexed row numbers containing headers")
    separator: str = Field(default="_", description="Separator for joining header parts")
    skip_empty_parts: bool = Field(default=True, description="Skip empty parts when combining")
    sanitize_names: bool = Field(default=True, description="Sanitize final column names")
    forward_fill: bool = Field(
        default=False,
        description="Forward-fill None/empty values in each header row for spanning headers",
    )


@register_expression(
    "multi_level_header",
    schemas=["bronze", "silver"],
    description="Extract and combine multi-level headers from Excel sheets",
)
class MultiLevelHeaderExpression:
    """
    Build multi-level header extraction from schema configuration.

        MultiLevelHeaderExpression extracts header information from multiple
        rows and combines them into single column names. This is particularly
        useful for Excel reports with hierarchical column structures.

        The class handles:
        - Multi-row header extraction
        - Header part combination with separators
        - Empty cell handling
        - Column name sanitization
        - Mapping generation for DataFrame.rename()

    """

    @staticmethod
    def sanitize_column_name(name: str) -> str:
        """
        Sanitize a column name to be valid and clean.

                Parameters

                name : str
                    Original column name

                Returns

                str
                    Sanitized column name (lowercase, underscores, alphanumeric)
        """
        import re

        # Convert to lowercase
        name = name.lower()

        # Replace spaces and hyphens with underscores
        name = name.replace(" ", "_").replace("-", "_")

        # Remove special characters except underscores
        name = re.sub(r"[^a-z0-9_]", "", name)

        # Replace multiple underscores with single
        name = re.sub(r"_+", "_", name)

        # Remove leading/trailing underscores
        name = name.strip("_")

        # Ensure it doesn't start with a number
        if name and name[0].isdigit():
            name = f"col_{name}"

        return name

    @staticmethod
    def extract_headers(df_raw: pl.DataFrame, config: MultiLevelHeaderConfig) -> dict[str, str]:
        """
        Extract and combine multi-level headers into column name mapping.

                WHY: Combines hierarchical Excel headers into meaningful column names.

                Reads specified header rows from the DataFrame and combines them
                into a mapping of original column names to new combined names.

                Parameters

                df_raw : pl.DataFrame
                    Raw DataFrame with headers in data rows (must be collected)

                config : MultiLevelHeaderConfig
                    Configuration for header extraction

                Returns

                dict[str, str]
                    Mapping from original column names to new combined names
                    Example: {"col_0": "Year_2024", "col_1": "Q1_Sales"}

                Notes

                - Input must be a collected DataFrame
                - Header rows should be 0-indexed
                - Empty cells are handled based on skip_empty_parts config
                - Original column names (col_0, col_1, etc.) are keys
                - When forward_fill is True, None/empty values are filled with the previous non-None value
        """
        with tracer.span(
            "extract_multi_level_headers",
            num_header_rows=len(config.header_rows),
            num_columns=len(df_raw.columns),
        ):
            column_mapping = {}

            original_columns = df_raw.columns

            header_row_data = {}
            if config.forward_fill:
                for row_idx in config.header_rows:
                    row_values = []
                    for col_name in original_columns:
                        value = df_raw[col_name][row_idx]
                        if value is not None:
                            value_str = str(value).strip()
                        else:
                            value_str = None
                        row_values.append(value_str)

                    filled_values = []
                    current_fill = None
                    for value in row_values:
                        if value is not None and value != "":
                            current_fill = value
                            filled_values.append(value)
                        else:
                            filled_values.append(current_fill if current_fill else "")

                    header_row_data[row_idx] = filled_values

            for col_idx, col_name in enumerate(original_columns):
                header_parts = []

                for row_idx in config.header_rows:
                    if config.forward_fill:
                        value_str = header_row_data[row_idx][col_idx]
                    else:
                        value = df_raw[col_name][row_idx]
                        if value is not None:
                            value_str = str(value).strip()
                        else:
                            value_str = ""

                    if value_str or not config.skip_empty_parts:
                        header_parts.append(value_str)

                if header_parts:
                    combined_name = config.separator.join(part for part in header_parts if part)
                else:
                    combined_name = col_name  # Fallback to original

                if config.sanitize_names:
                    combined_name = MultiLevelHeaderExpression.sanitize_column_name(combined_name)

                column_mapping[col_name] = combined_name

            name_counts = {}
            final_mapping = {}

            for col_name, combined_name in column_mapping.items():
                if combined_name not in name_counts:
                    name_counts[combined_name] = 0
                    final_mapping[col_name] = combined_name
                else:
                    name_counts[combined_name] += 1
                    final_mapping[col_name] = f"{combined_name}_{name_counts[combined_name]}"

            return final_mapping

    @traced()
    @explain(
        why="Apply failed",
        how="Check configuration and input data are valid",
        causes=["Invalid config", "Missing required fields", "Data processing error"],
    )
    @timeit(log_level="debug")
    @staticmethod
    def apply(
        df: pl.LazyFrame, config: MultiLevelHeaderConfig, data_start_row: int | None = None
    ) -> pl.LazyFrame:
        """
        Apply multi-level header extraction to a LazyFrame.

                WHY: Transforms Excel sheets with multi-row headers into queryable DataFrames
                with meaningful column names, enabling downstream analysis and transformations.

                This is a helper method that:
                1. Collects the LazyFrame to extract headers
                2. Extracts and combines header information
                3. Skips header rows to get data rows
                4. Renames columns with combined headers
                5. Returns as LazyFrame

                Parameters

                df : pl.LazyFrame
                    Input LazyFrame (will be temporarily collected)

                config : MultiLevelHeaderConfig
                    Configuration for header extraction

                data_start_row : int | None
                    Row index where data starts (0-indexed).
                    If None, uses max(header_rows) + 1

                Returns

                pl.LazyFrame
                    Renamed LazyFrame with combined headers (stays lazy!)

                Notes

                - Temporarily collects DataFrame for header extraction
                - Returns LazyFrame for downstream processing
                - Automatically skips header rows based on header_rows config
                ['year_2024', 'q1_sales']
                (2, 2)
        """
        with tracer.span(
            "apply_multi_level_header", num_header_rows=len(config.header_rows)
        ) as span:
            df_collected = df.collect()
            span.set_attribute("input_rows", len(df_collected))
            span.set_attribute("input_cols", len(df_collected.columns))

            column_mapping = MultiLevelHeaderExpression.extract_headers(df_collected, config)
            span.set_attribute("num_mapped_columns", len(column_mapping))

            if data_start_row is None:
                data_start_row = max(config.header_rows) + 1

            df_data = df_collected.slice(data_start_row)
            df_renamed = df_data.rename(column_mapping)
            span.set_attribute("output_rows", len(df_renamed))

            return df_renamed.lazy()
