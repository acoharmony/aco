"""
CCLF claim filtering and validation expressions.

This module provides registered expressions for processing CMS Claim and Claim Line Feed
(CCLF) data according to the official CCLF Implementation Guide specifications.

References:
- CCLF Implementation Guide v40.0 (02/06/2025)
- Section 3.5: Part A Header Expenditures vs Part A Revenue Center Expenditures
- Section 3.6: Date Fields
- Section 5.3: Calculating Beneficiary-Level Expenditures

Key Features:
- ADR (Adjustment/Deletion/Replacement) filtering per Section 5.3
- Latest claim version selection based on row_num ranking
- Invalid date detection per Section 3.6
- Revenue center payment validation per Section 3.5

Schema Requirements:
- clm_adjsmt_type_cd: Claim adjustment type code ('0'=Original, '1'=Cancel, '2'=Adjust)
- row_num: Claim version ranking (1=latest, >1=historical)
- Date fields following CCLF format (1000-01-01 and 9999-12-31 represent null)

Use Cases:
1. Filter to latest, non-canceled claims across all claim types
2. Detect and handle invalid/sentinel date values
3. Validate institutional revenue center payments match header
4. Apply consistent ADR logic across Part A and Part B claims
"""

import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression


@register_expression(
    "cclf_claim_filters",
    schemas=["silver"],
    dataset_types=["claims", "medical_claim"],
    callable=False,
    description="CCLF claim filtering expressions per Implementation Guide Section 5.3",
)
class CclfClaimFilterExpression:
    """
    Build expressions for CCLF claim filtering and validation.

    Per CCLF Implementation Guide v40.0 Section 5.3.1:
    "To correctly 'sign' the payment amounts as a payment to the provider, or a
    recovery from the provider, follow the steps below to identify beneficiary-level
    expenditures for Part A and Part B services."

    This expression builder creates idempotent expressions for:
    - Filtering to latest claim versions (row_num = 1)
    - Excluding canceled claims (clm_adjsmt_type_cd = '1')
    - Validating payment totals for institutional claims
    - Detecting invalid/sentinel dates
    """

    @staticmethod
    @expression(
        name="cclf_latest_non_canceled",
        tier=["silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def latest_non_canceled_filter() -> pl.Expr:
        """
        Create filter expression for latest, non-canceled claims.

        Per CCLF Guide Section 5.3.1:
        - Step 1: "Identify the canceled claims in the Part A Header file.
          These claims are identified by CLM_ADJSMT_TYPE_CD=1"
        - Similar logic for Part B claims in Steps 2-3

        The row_num field (created during ADR processing) ranks claim versions
        by effective date, with row_num=1 representing the most recent version.

        Returns:
            pl.Expr: Boolean expression that is True for claims that are:
                - Latest version (row_num == 1)
                - Not canceled (clm_adjsmt_type_cd != '1')

        Notes:
            - Canceled claims (clm_adjsmt_type_cd='1') should have amounts negated
              during ADR processing before applying this filter
            - This filter is applied AFTER ADR netting to exclude fully canceled claims
            - Adjustment type '0' = Original, '1' = Cancellation, '2' = Adjustment
        """
        return (pl.col("row_num") == 1) & (pl.col("clm_adjsmt_type_cd") != "1")

    @staticmethod
    @expression(
        name="cclf_invalid_dates",
        tier=["silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def invalid_date_detection() -> list[pl.Expr]:
        """
        Create expressions to detect and nullify invalid CCLF dates.

        Per CCLF Guide Section 3.6:
        "There are various date-related fields in the CCLF data files. In some
        instances, the date field is not required or is not available in the
        source. In these cases, that date field is commonly filled in with
        '1000-01-01' or '9999-12-31.' These dates should be treated as 'missing'
        or 'null' values."

        Returns:
            list[pl.Expr]: List of expressions checking if dates are invalid:
                - is_sentinel_date_1000: True if date is 1000-01-01 (historical null)
                - is_sentinel_date_9999: True if date is 9999-12-31 (future null)
                - is_invalid_date: True if date is either sentinel value

        Notes:
            - These sentinel dates appear in date fields when data is unavailable
            - Both values should be treated identically as null/missing
            - Applies to all date fields: from_dt, thru_dt, efctv_dt, etc.
        """
        sentinel_dates = ["1000-01-01", "9999-12-31"]

        def is_sentinel(col_name: str) -> pl.Expr:
            return pl.col(col_name).cast(pl.Utf8).is_in(sentinel_dates)

        return [
            is_sentinel("clm_from_dt").alias("is_sentinel_from_dt"),
            is_sentinel("clm_thru_dt").alias("is_sentinel_thru_dt"),
            is_sentinel("clm_efctv_dt").alias("is_sentinel_efctv_dt"),
        ]

    @staticmethod
    @expression(
        name="cclf_positive_amounts",
        tier=["silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def positive_amounts_filter(columns: list[str] | None = None) -> pl.Expr:
        """
        Create filter for claims with positive payment amounts after ADR netting.

        Per CCLF Guide Section 5.3.1:
        After negating canceled claim amounts and summing, fully canceled claims
        will have sum <= 0 and should be excluded from final expenditure calculations.

        Parameters:
            columns: List of column names in the DataFrame (for column detection).
                If None, defaults to checking sum_clm_pmt_amt.

        Returns:
            pl.Expr: Boolean expression checking if payment amounts are positive.
        """
        cols = set(columns or [])
        # Check for various payment amount column patterns
        if "sum_clm_pmt_amt" in cols:
            return pl.col("sum_clm_pmt_amt") > 0
        elif "sum_clm_line_cvrd_pd_amt" in cols:
            return pl.col("sum_clm_line_cvrd_pd_amt") > 0
        elif "clm_pmt_amt" in cols:
            return pl.col("clm_pmt_amt") > 0
        elif "clm_line_cvrd_pd_amt" in cols:
            return pl.col("clm_line_cvrd_pd_amt") > 0
        else:
            # Fallback: no filter if no payment column found
            return pl.lit(True)


@register_expression(
    "cclf_revenue_center_validation",
    schemas=["silver"],
    dataset_types=["claims", "institutional"],
    callable=False,
    description="Revenue center payment validation per CCLF Guide Section 3.5",
)
class CclfRevenueCenterValidationExpression:
    """
    Build expressions for validating Part A revenue center payments.

    Per CCLF Implementation Guide v40.0 Section 3.5:
    "Both the Part A Header file (CCLF1) and the Part A Revenue Center file (CCLF2)
    contain a payment related field, entitled CLM_PMT_AMT and CLM_LINE_CVRD_PD_AMT,
    respectively. The revenue center payment amounts should only be relied on if they
    sum to the header level payment amount. If the revenue center level payment amounts
    do not sum to the header level payment amount, then the revenue center level payment
    amounts should be ignored."

    Reasons for discrepancy (per Section 3.5):
    - Some claims do not have revenue center level payments (e.g., inpatient DRG-based)
    - Some claims are not required to report at revenue center level
    - For some claims the revenue center amounts were not those used for payment
    """

    @staticmethod
    @expression(
        name="cclf_revenue_center_matches_header",
        tier=["silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def revenue_center_matches_header(tolerance: float = 0.01) -> pl.Expr:
        """
        Check if revenue center payments sum to header amount.

        Per CCLF Guide Section 3.5, revenue center line payments should only be
        used if they sum to the header payment amount. This expression validates
        that relationship.

        Args:
            tolerance: Acceptable percentage difference (default 0.01 = 1%).
                Allows for minor rounding differences.

        Returns:
            pl.Expr: Boolean expression that is True when:
                - Line payment sum is within tolerance% of header payment
                - Example: If header=$1000, line sum $990-$1010 is valid

        Notes:
            - Tolerance accounts for floating point precision and rounding
            - Invalid line payments (sum != header) should use header amount only
            - For institutional claims, typically 84% have valid line payments
            - For DRG-based inpatient claims, line payments are often zero/invalid
        """
        return (pl.col("revenue_center_pmt_sum") / pl.col("sum_clm_pmt_amt")).is_between(
            1.0 - tolerance, 1.0 + tolerance
        )

    @staticmethod
    @expression(
        name="cclf_allocate_header_payment",
        tier=["silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def allocate_header_payment_to_first_line() -> pl.Expr:
        """
        Allocate header payment to first revenue center line only.

        Per CCLF Guide Section 3.5, when revenue center payments don't sum to
        header (invalid), the header payment should be used instead. To avoid
        double-counting across multiple revenue center lines, allocate the full
        header amount to the first line only.

        Returns:
            pl.Expr: Expression that:
                - Uses clm_line_cvrd_pd_amt if valid (use_line_payments=True)
                - Uses sum_clm_pmt_amt on first line if invalid (row_num=1)
                - Uses 0 for other lines when invalid

        Notes:
            - Prevents double-counting when same header amount would appear on all lines
            - First line determined by ranking clm_line_num within cur_clm_uniq_id
            - For claims with valid line payments, all lines get their actual amounts
            - For claims without valid line payments, only first line gets header amount
        """
        return (
            pl.when(pl.col("use_line_payments") & pl.col("clm_line_cvrd_pd_amt").is_not_null())
            .then(pl.col("clm_line_cvrd_pd_amt"))
            .when(pl.col("clm_line_num").rank("dense").over("cur_clm_uniq_id") == 1)
            .then(pl.col("sum_clm_pmt_amt"))
            .otherwise(0)
        )
