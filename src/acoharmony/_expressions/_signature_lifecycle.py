# © 2025 HarmonyCares
# All rights reserved.

"""
Signature Lifecycle expression for SVA expiry tracking and outreach prioritization.

Implements CMS REACH Voluntary Alignment signature validity rules:
- Signatures are valid for 3 years from signature date
- Expiry is January 1 of the year that is 3 years after signature year
- Performance Year (PY) validity: Signature must be within 2-year lookback
- Example: Signature in 2024 → Valid for PY 2024, 2025, 2026 → Expires Jan 1, 2027

This expression calculates:
- Signature expiry dates
- Days until expiry
- Performance Year coverage
- Outreach priority based on expiry urgency
"""

from datetime import datetime

import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression


@register_expression(
    "signature_lifecycle",
    schemas=["silver", "gold"],
    dataset_types=["alignment", "voluntary"],
    callable=True,  # Can be called as function to add columns
    description="Calculate SVA signature expiry, PY validity, and outreach priority",
)
class SignatureLifecycleExpression:
    """
    Expression for calculating signature lifecycle metrics.

        Reusable across any table with signature dates.
        Can be applied to:
        - voluntary_alignment intermediate
        - consolidated_alignment final output
        - SVA campaign targeting
    """

    @staticmethod
    @expression(name="signature_lifecycle", tier=["silver"], idempotent=True, sql_enabled=True)
    def calculate_signature_lifecycle(
        signature_date_col: str = "last_valid_signature_date",
        current_py: int | None = None,
        reference_date: datetime | None = None,
    ) -> list[pl.Expr]:
        """
        Calculate signature lifecycle metrics.

                Args:
                    signature_date_col: Column name containing signature date
                    current_py: Current Performance Year (defaults to current year)
                    reference_date: Reference date for calculations (defaults to today)
                                  Using explicit date ensures idempotency

                Returns:
                    List of Polars expressions for signature lifecycle columns:
                    - signature_expiry_date: Jan 1 of signature_year + 3
                    - days_until_signature_expiry: Days from reference_date to expiry
                    - signature_valid_for_current_py: Boolean - valid for current PY
                    - signature_valid_for_pys: String like "2024-2026"
                    - sva_outreach_priority: expired/urgent/upcoming/active/no_signature

        """
        # Default to current year if not specified
        if current_py is None:
            current_py = datetime.now().year

        # Default to today if not specified
        if reference_date is None:
            reference_date = datetime.now().date()
        elif isinstance(reference_date, datetime):
            reference_date = reference_date.date()

        return [
            # signature_expiry_date: January 1 of (signature_year + 3)
            # Example: Signed in 2024 → Expires Jan 1, 2027
            pl.when(pl.col(signature_date_col).is_not_null())
            .then(
                pl.date(
                    pl.col(signature_date_col).dt.year() + 3,  # 3 years after signature
                    1,  # January
                    1,  # 1st day
                )
            )
            .otherwise(None)
            .alias("signature_expiry_date"),
            # last_signature_expiry_date: Alias for compatibility
            pl.when(pl.col(signature_date_col).is_not_null())
            .then(
                pl.date(
                    pl.col(signature_date_col).dt.year() + 3,
                    1,
                    1,
                )
            )
            .otherwise(None)
            .alias("last_signature_expiry_date"),
            # days_until_signature_expiry: Days from reference_date to expiry
            # Negative values mean expired
            pl.when(pl.col(signature_date_col).is_not_null())
            .then(
                (
                    pl.date(
                        pl.col(signature_date_col).dt.year() + 3,
                        1,
                        1,
                    )
                    - pl.lit(reference_date)
                ).dt.total_days()
            )
            .otherwise(None)
            .alias("days_until_signature_expiry"),
            # signature_valid_for_current_py: Valid if signature year >= (current_py - 2)
            # Example: PY 2025 accepts signatures from 2023, 2024, 2025
            pl.when(pl.col(signature_date_col).is_not_null())
            .then(pl.col(signature_date_col).dt.year() >= (current_py - 2))
            .otherwise(False)
            .alias("signature_valid_for_current_py"),
            # signature_valid_for_pys: String like "2024-2026"
            # Signature in year Y is valid for PY Y, Y+1, Y+2
            pl.when(pl.col(signature_date_col).is_not_null())
            .then(
                pl.concat_str(
                    [
                        pl.col(signature_date_col).dt.year().cast(pl.Utf8),
                        pl.lit("-"),
                        (pl.col(signature_date_col).dt.year() + 2).cast(pl.Utf8),
                    ]
                )
            )
            .otherwise(None)
            .alias("signature_valid_for_pys"),
            # sva_outreach_priority: Categorize based on days until expiry
            # MUST check for null FIRST before any operations on the date column
            # We calculate this AFTER days_until_signature_expiry, so we can reference it
            # But Polars doesn't allow referencing just-calculated columns, so we need to recalculate
            pl.when(pl.col(signature_date_col).is_null())
            .then(pl.lit("no_signature"))
            .when(
                # Expired: days_until_expiry < 0
                pl.col(signature_date_col).is_not_null()
                & (
                    (
                        pl.date(
                            pl.col(signature_date_col).dt.year() + 3,
                            1,
                            1,
                        )
                        - pl.lit(reference_date)
                    ).dt.total_days()
                    < 0
                )
            )
            .then(pl.lit("expired"))
            .when(
                # Urgent: < 90 days until expiry
                pl.col(signature_date_col).is_not_null()
                & (
                    (
                        pl.date(
                            pl.col(signature_date_col).dt.year() + 3,
                            1,
                            1,
                        )
                        - pl.lit(reference_date)
                    ).dt.total_days()
                    < 90
                )
            )
            .then(pl.lit("urgent"))
            .when(
                # Upcoming: < 180 days until expiry
                pl.col(signature_date_col).is_not_null()
                & (
                    (
                        pl.date(
                            pl.col(signature_date_col).dt.year() + 3,
                            1,
                            1,
                        )
                        - pl.lit(reference_date)
                    ).dt.total_days()
                    < 180
                )
            )
            .then(pl.lit("upcoming"))
            .otherwise(pl.lit("active"))
            .alias("sva_outreach_priority"),
        ]

