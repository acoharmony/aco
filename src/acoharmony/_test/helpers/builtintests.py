"""
Built-in test implementations for common data quality checks.
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import Any

import polars as pl


class BuiltinTests:
    """Collection of built-in data quality tests."""

    def test_unique(self, df: pl.LazyFrame, column_name: str) -> dict[str, Any]:
        """Test that values in a column are unique."""
        if not column_name:
            raise ValueError("Column name is required for unique test")

        # Count total rows and unique values
        result = df.select(
            [pl.len().alias("total_rows"), pl.col(column_name).n_unique().alias("unique_values")]
        ).collect()

        total_rows = result["total_rows"][0]
        unique_values = result["unique_values"][0]
        duplicates = total_rows - unique_values

        return {
            "passed": duplicates == 0,
            "rows_tested": total_rows,
            "rows_failed": duplicates,
            "error_message": None if duplicates == 0 else f"{duplicates} duplicate values found",
        }

    def test_not_null(self, df: pl.LazyFrame, column_name: str) -> dict[str, Any]:
        """Test that a column contains no null values."""
        if not column_name:
            raise ValueError("Column name is required for not_null test")

        # Count nulls
        result = df.select(
            [pl.len().alias("total_rows"), pl.col(column_name).null_count().alias("null_count")]
        ).collect()

        total_rows = result["total_rows"][0]
        null_count = result["null_count"][0]

        return {
            "passed": null_count == 0,
            "rows_tested": total_rows,
            "rows_failed": null_count,
            "error_message": None if null_count == 0 else f"{null_count} null values found",
        }

    def test_accepted_values(
        self, df: pl.LazyFrame, column_name: str, values: list[Any]
    ) -> dict[str, Any]:
        """Test that column values are in the accepted list."""
        if not column_name:
            raise ValueError("Column name is required for accepted_values test")
        if not values:
            raise ValueError("Values list is required for accepted_values test")

        # Count rows with values not in accepted list
        result = df.select(
            [
                pl.len().alias("total_rows"),
                (~pl.col(column_name).is_in(values)).sum().alias("invalid_count"),
            ]
        ).collect()

        total_rows = result["total_rows"][0]
        invalid_count = result["invalid_count"][0]

        return {
            "passed": invalid_count == 0,
            "rows_tested": total_rows,
            "rows_failed": invalid_count,
            "error_message": None
            if invalid_count == 0
            else f"{invalid_count} values not in accepted list",
        }

    def test_relationships(
        self, df: pl.LazyFrame, column_name: str, to_table: str, field: str
    ) -> dict[str, Any]:
        """Test referential integrity between tables."""
        if not all([column_name, to_table, field]):
            raise ValueError("Column name, to_table, and field are required for relationships test")

        # This is a simplified version - in practice would need to load the referenced table
        # For now, just check for nulls as a basic referential check
        result = df.select(
            [pl.len().alias("total_rows"), pl.col(column_name).null_count().alias("null_count")]
        ).collect()

        total_rows = result["total_rows"][0]
        null_count = result["null_count"][0]

        return {
            "passed": null_count == 0,
            "rows_tested": total_rows,
            "rows_failed": null_count,
            "error_message": None if null_count == 0 else f"{null_count} null foreign key values",
        }

    # Healthcare-specific tests

    def test_valid_mbi(self, df: pl.LazyFrame, column_name: str) -> dict[str, Any]:
        """Test that MBI values are properly formatted (11 characters, alphanumeric)."""
        if not column_name:
            raise ValueError("Column name is required for valid_mbi test")

        # MBI should be 11 characters, alphanumeric
        result = df.select(
            [
                pl.len().alias("total_rows"),
                (
                    (pl.col(column_name).str.len_chars() != 11)
                    | (~pl.col(column_name).str.contains(r"^[A-Z0-9]{11}$"))
                )
                .sum()
                .alias("invalid_count"),
            ]
        ).collect()

        total_rows = result["total_rows"][0]
        invalid_count = result["invalid_count"][0]

        return {
            "passed": invalid_count == 0,
            "rows_tested": total_rows,
            "rows_failed": invalid_count,
            "error_message": None if invalid_count == 0 else f"{invalid_count} invalid MBI formats",
        }

    def test_valid_npi(self, df: pl.LazyFrame, column_name: str) -> dict[str, Any]:
        """Test that NPI values are properly formatted (10 digits)."""
        if not column_name:
            raise ValueError("Column name is required for valid_npi test")

        result = df.select(
            [
                pl.len().alias("total_rows"),
                (
                    (pl.col(column_name).str.len_chars() != 10)
                    | (~pl.col(column_name).str.contains(r"^[0-9]{10}$"))
                )
                .sum()
                .alias("invalid_count"),
            ]
        ).collect()

        total_rows = result["total_rows"][0]
        invalid_count = result["invalid_count"][0]

        return {
            "passed": invalid_count == 0,
            "rows_tested": total_rows,
            "rows_failed": invalid_count,
            "error_message": None if invalid_count == 0 else f"{invalid_count} invalid NPI formats",
        }

    def test_valid_tin(self, df: pl.LazyFrame, column_name: str) -> dict[str, Any]:
        """Test that TIN values are properly formatted (9 digits)."""
        if not column_name:
            raise ValueError("Column name is required for valid_tin test")

        result = df.select(
            [
                pl.len().alias("total_rows"),
                (
                    (pl.col(column_name).str.len_chars() != 9)
                    | (~pl.col(column_name).str.contains(r"^[0-9]{9}$"))
                )
                .sum()
                .alias("invalid_count"),
            ]
        ).collect()

        total_rows = result["total_rows"][0]
        invalid_count = result["invalid_count"][0]

        return {
            "passed": invalid_count == 0,
            "rows_tested": total_rows,
            "rows_failed": invalid_count,
            "error_message": None if invalid_count == 0 else f"{invalid_count} invalid TIN formats",
        }

    def test_date_sequence(
        self, df: pl.LazyFrame, start_column: str, end_column: str
    ) -> dict[str, Any]:
        """Test that start date is before or equal to end date."""
        if not all([start_column, end_column]):
            raise ValueError("Both start_column and end_column are required for date_sequence test")

        result = df.select(
            [
                pl.len().alias("total_rows"),
                (pl.col(end_column).is_not_null() & (pl.col(start_column) > pl.col(end_column)))
                .sum()
                .alias("invalid_count"),
            ]
        ).collect()

        total_rows = result["total_rows"][0]
        invalid_count = result["invalid_count"][0]

        return {
            "passed": invalid_count == 0,
            "rows_tested": total_rows,
            "rows_failed": invalid_count,
            "error_message": None
            if invalid_count == 0
            else f"{invalid_count} invalid date sequences",
        }

    def test_enrollment_logic(
        self, df: pl.LazyFrame, test_config: dict[str, Any]
    ) -> dict[str, Any]:
        """Test enrollment business logic."""
        # This would be customized based on specific enrollment rules
        # For now, implement basic enrollment consistency checks

        result = df.select(
            [
                pl.len().alias("total_rows"),
                # Example: Check that currently enrolled beneficiaries don't have end dates
                (
                    (pl.col("is_currently_enrolled"))
                    & (pl.col("enrollment_end").is_not_null())
                )
                .sum()
                .alias("logic_errors"),
            ]
        ).collect()

        total_rows = result["total_rows"][0]
        logic_errors = result["logic_errors"][0]

        return {
            "passed": logic_errors == 0,
            "rows_tested": total_rows,
            "rows_failed": logic_errors,
            "error_message": None
            if logic_errors == 0
            else f"{logic_errors} enrollment logic violations",
        }

    def test_claim_integrity(self, df: pl.LazyFrame, test_config: dict[str, Any]) -> dict[str, Any]:
        """Test claim data integrity."""
        # Example claim integrity checks
        result = df.select(
            [
                pl.len().alias("total_rows"),
                # Check for negative payment amounts
                (pl.col("claim_payment_amount") < 0).sum().alias("negative_payments"),
                # Check for payments greater than charges (suspicious)
                (
                    (pl.col("claim_payment_amount") > pl.col("total_charge_amount"))
                    & (pl.col("total_charge_amount") > 0)
                )
                .sum()
                .alias("payment_exceeds_charges"),
            ]
        ).collect()

        total_rows = result["total_rows"][0]
        negative_payments = result["negative_payments"][0]
        payment_exceeds_charges = result["payment_exceeds_charges"][0]

        total_failures = negative_payments + payment_exceeds_charges

        error_details = []
        if negative_payments > 0:
            error_details.append(f"{negative_payments} negative payments")
        if payment_exceeds_charges > 0:
            error_details.append(f"{payment_exceeds_charges} payments exceed charges")

        return {
            "passed": total_failures == 0,
            "rows_tested": total_rows,
            "rows_failed": total_failures,
            "error_message": None if total_failures == 0 else "; ".join(error_details),
        }
