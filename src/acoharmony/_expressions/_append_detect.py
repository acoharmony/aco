# © 2025 HarmonyCares
# All rights reserved.

"""
Append-detect for combining multi-row/column values to determine variable names.

 functionality for combining values from multiple rows or
columns to create meaningful variable names for dynamic cell ranges.

Core Concepts

**Append Detection**: Combine values from multiple locations to name variables
  Example 1 - Row append:
    Cell A1: "Payment"
    Cell A2: "Date"
    Result: payment_date

  Example 2 - Column append:
    Cell A1: "Total"
    Cell B1: "Amount"
    Result: total_amount

  Example 3 - Cross append (row + column):
    Cell A1: "2024"  (column header)
    Cell B3: "Sales" (row header)
    Cell B1: <data>
    Result: 2024_sales

**Range-Based Naming**: Name cells based on their position relative to headers
  Example - Pivot table structure:
    Row headers define one dimension
    Column headers define another dimension
    Data cells are named from both

**Context Propagation**: Use surrounding context to name cells
  Example - Sparse headers:
    Some cells have direct headers, others inherit from nearest

Use Cases

**Pivot Tables**: Row and column headers define cell names
**Cross-Tabulations**: Multi-dimensional naming
**Sparse Layouts**: Irregular header structures
**Financial Statements**: Line items × periods

Key Features

- Multi-directional value combination (row, column, diagonal)
- Context-aware naming based on position
- Offset-based lookups for headers
- Custom combination strategies
- Metadata preservation from combined values
"""

from enum import StrEnum

import polars as pl
from pydantic import BaseModel, Field

from .._decor8 import explain, timeit, traced
from ._registry import register_expression


class AppendDirection(StrEnum):
    """Direction for appending values."""

    ROW = "row"  # Append values from same row
    COLUMN = "column"  # Append values from same column
    CROSS = "cross"  # Append row header + column header
    DIAGONAL = "diagonal"  # Append diagonal values


class AppendStrategy(StrEnum):
    """Strategy for combining appended values."""

    CONCATENATE = "concatenate"  # Simple concatenation with separator
    FIRST_NON_EMPTY = "first_non_empty"  # Use first non-empty value
    LAST_NON_EMPTY = "last_non_empty"  # Use last non-empty value
    HIERARCHICAL = "hierarchical"  # Parent → child hierarchy


class CellReference(BaseModel):
    """
    Reference to a cell location.

        Attributes

        row_offset : int
            Row offset from current cell (negative = above, positive = below)

        col_offset : int
            Column offset from current cell (negative = left, positive = right)
    """

    row_offset: int = Field(description="Row offset from current cell")
    col_offset: int = Field(description="Column offset from current cell")


class AppendDetectConfig(BaseModel):
    """
    Configuration for append-detect.

        Attributes

        references : list[CellReference]
            List of cell references to append

        direction : AppendDirection
            Direction for appending values

        strategy : AppendStrategy
            Strategy for combining values

        separator : str
            Separator for concatenating values
            Default: "_"

        sanitize_names : bool
            Whether to sanitize final names
            Default: True

        skip_empty : bool
            Whether to skip empty values when appending
            Default: True

        data_start_row : int
            Row where data starts (0-indexed)

        data_start_col : int
            Column where data starts (0-indexed)

        row_header_cols : Optional[list[int]]
            Column indices for row headers (for CROSS direction)
            Default: None

        col_header_rows : Optional[list[int]]
            Row indices for column headers (for CROSS direction)
            Default: None
    """

    references: list[CellReference] = Field(
        default_factory=list, description="Cell references to append"
    )
    direction: AppendDirection = Field(
        default=AppendDirection.CROSS, description="Direction for appending"
    )
    strategy: AppendStrategy = Field(
        default=AppendStrategy.CONCATENATE, description="Strategy for combining values"
    )
    separator: str = Field(default="_", description="Separator for concatenation")
    sanitize_names: bool = Field(default=True, description="Sanitize final names")
    skip_empty: bool = Field(default=True, description="Skip empty values")
    data_start_row: int = Field(default=0, description="Data start row")
    data_start_col: int = Field(default=0, description="Data start column")
    row_header_cols: list[int] | None = Field(
        default=None, description="Column indices for row headers"
    )
    col_header_rows: list[int] | None = Field(
        default=None, description="Row indices for column headers"
    )


class CellNameMetadata(BaseModel):
    """
    Metadata for a named cell.

        Attributes

        cell_name : str
            Generated name for the cell

        row : int
            Row index

        col : int
            Column index

        components : list[str]
            Component values that formed the name

        offsets : list[tuple[int, int]]
            Offsets used to find component values
    """

    cell_name: str
    row: int
    col: int
    components: list[str] = Field(default_factory=list)
    offsets: list[tuple[int, int]] = Field(default_factory=list)


@register_expression(
    "append_detect",
    schemas=["bronze", "silver"],
    description="Combine multi-row/column values to determine variable names",
)
class AppendDetectExpression:
    """
    Append-detect for combining values from multiple locations.

        This expression reads values from specified cell offsets and combines them
        to create meaningful variable names for data cells.
    """

    @staticmethod
    def get_cell_value(df: pl.DataFrame, row: int, col: int) -> str | None:
        """
        Safely get a cell value.

                Parameters

                df : pl.DataFrame
                    DataFrame

                row : int
                    Row index

                col : int
                    Column index

                Returns

                Optional[str]
                    Cell value or None if out of bounds
        """
        if row < 0 or row >= df.height:
            return None
        if col < 0 or col >= len(df.columns):
            return None

        col_name = df.columns[col]
        value = df[col_name][row]

        if value is None:
            return None

        value_str = str(value).strip()
        return value_str if value_str else None

    @staticmethod
    def combine_values(values: list[str], strategy: AppendStrategy, separator: str) -> str:
        """
        Combine values using specified strategy.

                Parameters

                values : list[str]
                    Values to combine

                strategy : AppendStrategy
                    Combination strategy

                separator : str
                    Separator for concatenation

                Returns

                str
                    Combined value
        """
        if strategy == AppendStrategy.CONCATENATE:
            return separator.join(values)

        elif strategy == AppendStrategy.FIRST_NON_EMPTY:
            for val in values:
                if val:
                    return val
            return ""

        elif strategy == AppendStrategy.LAST_NON_EMPTY:
            for val in reversed(values):
                if val:
                    return val
            return ""

        elif strategy == AppendStrategy.HIERARCHICAL:
            # Similar to concatenate but implies parent-child relationship
            return separator.join(values)

        return separator.join(values)

    @staticmethod
    def generate_cell_names_cross(
        df: pl.DataFrame, config: AppendDetectConfig
    ) -> dict[tuple[int, int], CellNameMetadata]:
        """
        Generate cell names using cross (row × column) strategy.

                Parameters

                df : pl.DataFrame
                    Input DataFrame

                config : AppendDetectConfig
                    Configuration

                Returns

                dict[tuple[int, int], CellNameMetadata]
                    Mapping from (row, col) to cell metadata
        """
        cell_names = {}

        if not config.row_header_cols or not config.col_header_rows:
            return cell_names

        # Iterate over data cells
        for row in range(config.data_start_row, df.height):
            for col in range(config.data_start_col, len(df.columns)):
                components = []
                offsets = []

                # Get column header values
                for header_row in config.col_header_rows:
                    value = AppendDetectExpression.get_cell_value(df, header_row, col)
                    if value and (not config.skip_empty or value.strip()):
                        components.append(value)
                        offsets.append((header_row, col))

                # Get row header values
                for header_col in config.row_header_cols:
                    value = AppendDetectExpression.get_cell_value(df, row, header_col)
                    if value and (not config.skip_empty or value.strip()):
                        components.append(value)
                        offsets.append((row, header_col))

                # Combine components
                if components:
                    combined_name = AppendDetectExpression.combine_values(
                        components, config.strategy, config.separator
                    )

                    # Sanitize if configured
                    if config.sanitize_names:
                        from ._multi_level_header import MultiLevelHeaderExpression

                        combined_name = MultiLevelHeaderExpression.sanitize_column_name(
                            combined_name
                        )

                    cell_names[(row, col)] = CellNameMetadata(
                        cell_name=combined_name,
                        row=row,
                        col=col,
                        components=components,
                        offsets=offsets,
                    )

        return cell_names

    @staticmethod
    def generate_cell_names_references(
        df: pl.DataFrame, config: AppendDetectConfig
    ) -> dict[tuple[int, int], CellNameMetadata]:
        """
        Generate cell names using explicit cell references.

                Parameters

                df : pl.DataFrame
                    Input DataFrame

                config : AppendDetectConfig
                    Configuration with cell references

                Returns

                dict[tuple[int, int], CellNameMetadata]
                    Mapping from (row, col) to cell metadata
        """
        cell_names = {}

        # Iterate over data cells
        for row in range(config.data_start_row, df.height):
            for col in range(config.data_start_col, len(df.columns)):
                components = []
                offsets = []

                # Get values from each reference
                for ref in config.references:
                    ref_row = row + ref.row_offset
                    ref_col = col + ref.col_offset

                    value = AppendDetectExpression.get_cell_value(df, ref_row, ref_col)
                    if value and (not config.skip_empty or value.strip()):
                        components.append(value)
                        offsets.append((ref_row, ref_col))

                # Combine components
                if components:
                    combined_name = AppendDetectExpression.combine_values(
                        components, config.strategy, config.separator
                    )

                    # Sanitize if configured
                    if config.sanitize_names:
                        from ._multi_level_header import MultiLevelHeaderExpression

                        combined_name = MultiLevelHeaderExpression.sanitize_column_name(
                            combined_name
                        )

                    cell_names[(row, col)] = CellNameMetadata(
                        cell_name=combined_name,
                        row=row,
                        col=col,
                        components=components,
                        offsets=offsets,
                    )

        return cell_names

    @traced()
    @explain(
        why="Apply failed",
        how="Check configuration and input data are valid",
        causes=["Invalid config", "Missing required fields", "Data processing error"],
    )
    @timeit(log_level="debug")
    @staticmethod
    def apply(
        df: pl.DataFrame, config: AppendDetectConfig
    ) -> dict[tuple[int, int], CellNameMetadata]:
        """
        Apply append-detect to generate cell names.

                Parameters

                df : pl.DataFrame
                    Input DataFrame (must be collected, not lazy)

                config : AppendDetectConfig
                    Configuration

                Returns

                dict[tuple[int, int], CellNameMetadata]
                    Cell name metadata for each data cell
        """
        if config.direction == AppendDirection.CROSS:
            return AppendDetectExpression.generate_cell_names_cross(df, config)
        else:
            # Use explicit references
            if not config.references:
                # Auto-generate references based on direction
                if config.direction == AppendDirection.ROW:
                    # Use cell to the left
                    config.references = [CellReference(row_offset=0, col_offset=-1)]
                elif config.direction == AppendDirection.COLUMN:
                    # Use cell above
                    config.references = [CellReference(row_offset=-1, col_offset=0)]
                elif config.direction == AppendDirection.DIAGONAL:
                    # Use cell diagonally up-left
                    config.references = [CellReference(row_offset=-1, col_offset=-1)]

            return AppendDetectExpression.generate_cell_names_references(df, config)
