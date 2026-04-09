# © 2025 HarmonyCares
# All rights reserved.

"""
File version filtering expressions for selecting most recent data.

Provides reusable expressions to filter datasets to only the most recent file version
based on tracking metadata (source_file, source_filename, processed_at).

These expressions work across ALL datasets since all processed data has tracking columns.
"""

import polars as pl

from .._decor8 import expression
from ._registry import register_expression


@register_expression(
    "file_version",
    schemas=["bronze", "silver", "gold"],
    dataset_types=["all"],
    callable=True,
    description="Filter datasets to most recent file version using tracking metadata",
)
class FileVersionExpression:
    """
    Expressions for filtering to most recent file versions.

    All datasets in the system have tracking columns added during processing:
    - source_file: Full path to source file
    - source_filename: Just the filename
    - processed_at: Timestamp when file was processed

    These expressions use those columns to filter to the most recent version.
    """

    @staticmethod
    @expression(
        name="most_recent_by_filename",
        tier=["bronze", "silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def filter_most_recent_by_filename() -> pl.Expr:
        """
        Filter to rows from the most recent file based on source_filename.

        Uses lexicographic ordering of filenames (works when filenames include dates).
        For files like "Report 2025-10-31.xlsx" and "Report 2025-11-15.xlsx",
        the later date will sort higher.

        Returns:
            Expression that filters to rows from the most recent filename
        """
        return pl.col("source_filename") == pl.col("source_filename").max()

    @staticmethod
    @expression(
        name="most_recent_by_processed_at",
        tier=["bronze", "silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def filter_most_recent_by_processed_at() -> pl.Expr:
        """
        Filter to rows from the most recent file based on processed_at timestamp.

        Uses the actual processing timestamp, which is more reliable than filename sorting.

        Returns:
            Expression that filters to rows from the most recently processed file
        """
        return pl.col("processed_at") == pl.col("processed_at").max()

    @staticmethod
    @expression(
        name="most_recent_source_file",
        tier=["bronze", "silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def filter_most_recent_source_file() -> pl.Expr:
        """
        Filter to rows from the most recent file based on source_file path.

        Uses lexicographic ordering of full file paths.

        Returns:
            Expression that filters to rows from the most recent source file
        """
        return pl.col("source_file") == pl.col("source_file").max()

    @staticmethod
    @expression(
        name="get_most_recent_filename",
        tier=["bronze", "silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def get_most_recent_filename() -> pl.Expr:
        """
        Get the most recent filename (for display/logging).

        Returns:
            Expression that returns the most recent filename
        """
        return pl.col("source_filename").max()

    @staticmethod
    @expression(
        name="file_version_rank",
        tier=["bronze", "silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def add_file_version_rank() -> pl.Expr:
        """
        Add a rank column showing file version recency.

        Rank 1 = most recent file, 2 = second most recent, etc.
        Useful for debugging or keeping N most recent versions.

        Returns:
            Expression that ranks files by recency (1 = most recent)
        """
        return (
            pl.col("source_filename")
            .rank(method="dense", descending=True)
            .alias("file_version_rank")
        )

    @staticmethod
    @expression(
        name="most_recent_by_date_in_filename",
        tier=["bronze", "silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def filter_most_recent_by_date_in_filename() -> pl.Expr:
        """
        Filter to rows from file with the most recent date extracted from filename.

        Extracts date pattern from filename (MM-DD-YY HH.MM.SS format common in our files).
        More robust than lexicographic sorting for files with dates.

        For files like:
        - "Report PY2025 - 8-5-25 13.19.51.xlsx"
        - "Report PY2025 - 10-7-25 16.16.36.xlsx"

        Returns:
            Expression that filters to rows from file with most recent date
        """
        # Extract date components from filename pattern: MM-DD-YY HH.MM.SS
        # Convert to sortable format for comparison
        date_str = pl.col("source_filename").str.extract(
            r"(\d{1,2})-(\d{1,2})-(\d{2})\s+(\d{2})\.(\d{2})\.(\d{2})", 0
        )

        return (
            pl.col("source_filename")
            == pl.col("source_filename").sort_by(date_str, descending=True).first()
        )

    @staticmethod
    @expression(
        name="keep_only_most_recent",
        tier=["bronze", "silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def keep_only_most_recent_file() -> pl.Expr:
        """
        Universal filter for most recent file - extracts date from filename.

        For files with date patterns like "Report - MM-DD-YY HH.MM.SS.xlsx",
        extracts and compares the actual dates to find the most recent.

        Falls back to lexicographic sorting if no date pattern found.

        Returns:
            Expression that filters to rows from the most recent file
        """
        # Extract date components: MM-DD-YY HH.MM.SS
        # Build sortable date string: YYMMDD-HHMMSS
        date_sortable = pl.concat_str(
            [
                # Year (position 3): DD-YY -> YY
                pl.col("source_filename").str.extract(r"(\d{1,2})-(\d{1,2})-(\d{2})", 3),
                # Month (position 1): MM-DD-YY -> MM (zero-padded)
                pl.col("source_filename")
                .str.extract(r"(\d{1,2})-(\d{1,2})-(\d{2})", 1)
                .str.zfill(2),
                # Day (position 2): MM-DD-YY -> DD (zero-padded)
                pl.col("source_filename")
                .str.extract(r"(\d{1,2})-(\d{1,2})-(\d{2})", 2)
                .str.zfill(2),
                pl.lit("-"),
                # Time HH.MM.SS
                pl.col("source_filename").str.extract(r"(\d{2})\.(\d{2})\.(\d{2})", 1).str.zfill(2),
                pl.col("source_filename").str.extract(r"(\d{2})\.(\d{2})\.(\d{2})", 2).str.zfill(2),
                pl.col("source_filename").str.extract(r"(\d{2})\.(\d{2})\.(\d{2})", 3).str.zfill(2),
            ]
        )

        # Filter to rows where date_sortable equals the max date_sortable
        max_date = date_sortable.max()
        return date_sortable == max_date
