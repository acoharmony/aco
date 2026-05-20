# © 2025 HarmonyCares
# All rights reserved.

"""
Key-value pivot expression for transforming long-format metadata to wide format.

WHY THIS EXISTS
===============
Excel report metadata sheets (like CMS REACH report parameters) store configuration
as vertical key-value pairs. These need to be pivoted into wide format (single row
with keys as columns) for:
1. Joining with other tables as foreign keys
2. Filtering data by report parameters
3. Tracking lineage and provenance
4. Creating normalized dimensional tables

Without this expression, metadata would remain in unusable vertical format and could
not be joined to fact tables or used for filtering.

PROBLEM SOLVED
==============
Report metadata sheets have this structure:
  Row 1: "REACH ACO Identifier" | "D0259"
  Row 2: "REACH ACO Name"       | "HarmonyCares ACO LLC"
  Row 3: "Performance Year"     | "2024"

We need a single row for joining:
  reach_aco_identifier="D0259", reach_aco_name="HarmonyCares ACO LLC", performance_year="2024"

This enables queries like:
  SELECT * FROM payment_history p
  JOIN report_meta m ON p.source_file = m.source_file
  WHERE m.performance_year = '2024'

Core Concepts

**Long Format (Input)**: Vertical key-value pairs in report metadata sheets
  - Each row represents one metadata field
  - Keys become column names after sanitization
  - Values become the single row's data

**Wide Format (Output)**: Single-row DataFrame with metadata as columns
  - Enables joining to fact tables
  - Normalized structure for dimensional modeling
  - Queryable metadata attributes

Performance Characteristics

- Efficient Polars pivot operation
- Single row output (minimal memory)
- Column name sanitization is one-time cost
- Lazy evaluation maintained after pivot

Use Cases

1. **CMS Report Parameters**: PLARU, PALMR, BNMR metadata sheets
2. **Configuration Extraction**: Report settings and parameters
3. **Dimensional Attributes**: Creating dimension tables from metadata
4. **Data Lineage**: Tracking source file parameters and versions
"""

import polars as pl
from pydantic import BaseModel, Field

from .._decor8 import explain, timeit, traced
from .._expressions._registry import register_expression
from .._trace import TracerWrapper

# Initialize tracer for this expression
tracer = TracerWrapper("expression.key_value_pivot")


class KeyValuePivotConfig(BaseModel):
    """
    Configuration for key-value pivot transformation.

        Attributes

        key_column : str
            Column containing keys that will become column names.
            Default: "column_1"

        value_column : str
            Column containing values that will populate the new columns.
            Default: "column_2"

        skip_empty_values : bool
            Whether to skip rows with null or empty values.
            Default: True

        sanitize_keys : bool
            Whether to convert keys to valid column names.
            (lowercase, spaces to underscores, special chars removed)
            Default: True

        key_mapping : Optional[dict[str, str]]
            Optional dictionary to rename specific keys before pivoting.
            Example: {"Old Key": "new_key"}
            Default: None
    """

    key_column: str = Field(default="column_1", description="Column containing keys")
    value_column: str = Field(default="column_2", description="Column containing values")
    skip_empty_values: bool = Field(default=True, description="Skip null/empty values")
    sanitize_keys: bool = Field(default=True, description="Sanitize key names")
    key_mapping: dict[str, str] | None = Field(default=None, description="Custom key mappings")


@register_expression(
    "key_value_pivot",
    schemas=["bronze", "silver"],
    description="Transform long-format key-value pairs to wide-format single row",
)
class KeyValuePivotExpression:
    """
    Build key-value pivot transformation from schema configuration.

        KeyValuePivotExpression transforms long-format key-value pairs into
        wide-format single-row DataFrames. This is particularly useful for
        report metadata sheets where parameters are stored as key-value pairs.

        The class handles:
        - Column name sanitization
        - Custom key mapping
        - Empty value filtering
        - Single-row output generation

    """

    @staticmethod
    def sanitize_column_name(name: str) -> str:
        """
        Sanitize a key name to be a valid Polars column name.

                Parameters

                name : str
                    Original key name

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
            name = f"_{name}"

        return name

    @traced()
    @explain(
        why="Build failed",
        how="Check configuration and input data are valid",
        causes=["Invalid config", "Missing required fields", "Data processing error"],
    )
    @timeit(log_level="debug")
    @staticmethod
    def build(df: pl.DataFrame, config: KeyValuePivotConfig) -> pl.LazyFrame:
        """
        Build key-value pivot transformation.

                WHY: Converts vertical metadata into horizontal format for joining and querying.

                Transforms a long-format DataFrame with key-value pairs into a
                wide-format single-row LazyFrame. Keys become column names, values
                become cell values.

                Parameters

                df : pl.DataFrame
                    Input DataFrame with key-value structure (must be collected)

                config : KeyValuePivotConfig
                    Configuration for the pivot transformation

                Returns

                pl.LazyFrame
                    Single-row LazyFrame with pivoted columns

                Notes

                - Input must be a collected DataFrame (not LazyFrame) for pivot operation
                - Returns LazyFrame for downstream lazy evaluation
                - Duplicate keys will use the last value encountered
                - Column ordering is not guaranteed
        """
        with tracer.span(
            "key_value_pivot",
            input_rows=len(df),
            key_column=config.key_column,
            value_column=config.value_column,
        ) as span:
            # Filter out empty values if configured
            if config.skip_empty_values:
                df = df.filter(
                    pl.col(config.value_column).is_not_null() & (pl.col(config.value_column) != "")
                )
                span.set_attribute("filtered_rows", len(df))

            # Extract keys and values
            keys = df[config.key_column].to_list()
            values = df[config.value_column].to_list()

            # Build column mapping
            pivoted_data = {}

            for key, value in zip(keys, values, strict=False):
                # Apply custom key mapping if provided
                if config.key_mapping and key in config.key_mapping:
                    col_name = config.key_mapping[key]
                else:
                    col_name = key

                # Sanitize column name if configured
                if config.sanitize_keys:
                    col_name = KeyValuePivotExpression.sanitize_column_name(col_name)

                # Store value (last value wins for duplicate keys)
                pivoted_data[col_name] = value

            span.set_attribute("output_columns", len(pivoted_data))

            # Create single-row DataFrame with pivoted columns
            result_df = pl.DataFrame([pivoted_data])

            # Return as LazyFrame for downstream processing
            return result_df.lazy()
