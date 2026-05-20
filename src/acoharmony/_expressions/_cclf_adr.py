"""
CCLF Adjustment/Deletion/Replacement (ADR) expressions.

This module provides registered expressions for applying CMS CCLF ADR (Adjustment,
Deletion, Replacement) logic to claim payment amounts according to the official
CCLF Implementation Guide.

References:
- CCLF Implementation Guide v40.0 (02/06/2025)
- Section 5.3.1: Calculating Total Part A and B Expenditures
- Table 3: Claim Effective Date and the Claim Adjustment Type Code

Key Concepts (Per Section 5.3.1):
"Calculating total expenditures for a beneficiary using debit/credit data is a
conceptually simple process of adding up all the debit and credit amounts associated
with the claims incurred by a beneficiary during a specific time period.

However, it is slightly more complicated for two reasons: First, the payment amounts
on each record are not 'signed' to indicate whether the payment amount is a payment
to the provider or a recovery from the provider. Therefore, it is necessary to use
the CLM_ADJSMT_TYPE_CD to determine whether to 'add' or 'subtract' the payment
amount from the running total."

Adjustment Type Codes:
- '0' = Original claim (positive payment to provider)
- '1' = Cancellation (negative payment, recovery from provider)
- '2' = Adjustment (positive payment, replaces previous)

ADR Process:
1. Negate amounts for cancellations (clm_adjsmt_type_cd='1')
2. Sum amounts by natural keys (account for multiple versions)
3. Filter to latest version (row_num=1) after netting
4. Exclude fully canceled claims (sum <= 0 after netting)

Use Cases:
1. Apply correct payment signs before aggregation
2. Net cancellations against original claims
3. Handle claim adjustments and replacements
4. Calculate accurate beneficiary-level expenditures
"""

import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression


@register_expression(
    "cclf_adr",
    schemas=["silver"],
    dataset_types=["claims", "medical_claim"],
    callable=False,
    description="CCLF ADR expressions per Implementation Guide Section 5.3.1",
)
class CclfAdrExpression:
    """
    Build expressions for CCLF Adjustment/Deletion/Replacement logic.

    Per CCLF Implementation Guide v40.0 Section 5.3.1:
    "Identify the canceled claims... These claims are identified by
    CLM_ADJSMT_TYPE_CD=1. Change the 'sign' of the variable CLM_PMT_AMT
    for each of these cancellation claims (i.e., multiply the CLM_PMT_AMT by -1)."

    This applies to:
    - Part A Header payments (CLM_PMT_AMT in CCLF1)
    - Part B Physician line payments (CLM_LINE_CVRD_PD_AMT in CCLF5)
    - Part B DME line payments (CLM_LINE_CVRD_PD_AMT in CCLF6)
    """

    @staticmethod
    @expression(
        name="cclf_negate_cancellations_header",
        tier=["silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def negate_cancellations_header() -> list[pl.Expr]:
        """
        Negate payment amounts for canceled Part A claims.

        Per CCLF Guide Section 5.3.1 Step 1:
        "Identify the canceled claims in the Part A Header file. These claims
        are identified by CLM_ADJSMT_TYPE_CD=1. Change the 'sign' of the variable
        CLM_PMT_AMT for each of these cancellation claims (i.e., multiply the
        CLM_PMT_AMT by -1). For example, if on a given cancellation claim the
        value for CLM_PMT_AMT=$30.18, then change this to equal -$30.18."

        Returns:
            list[pl.Expr]: Expressions that negate amounts for canceled claims:
                - clm_pmt_amt: Negated if clm_adjsmt_type_cd='1', else unchanged
                - clm_mdcr_instnl_tot_chrg_amt: Negated for consistency

        Notes:
            - Cancellations (type '1') reverse original payments (type '0')
            - Adjustments (type '2') replace previous amounts with new values
            - After netting, claims with sum=0 are fully canceled
            - Cast to Decimal(scale=2) for precision in financial calculations
        """
        return [
            pl.when(pl.col("clm_adjsmt_type_cd") == "1")
            .then(-pl.col("clm_pmt_amt").cast(pl.Decimal(scale=2)))
            .otherwise(pl.col("clm_pmt_amt").cast(pl.Decimal(scale=2)))
            .alias("clm_pmt_amt"),
            pl.when(pl.col("clm_adjsmt_type_cd") == "1")
            .then(-pl.col("clm_mdcr_instnl_tot_chrg_amt").cast(pl.Decimal(scale=2)))
            .otherwise(pl.col("clm_mdcr_instnl_tot_chrg_amt").cast(pl.Decimal(scale=2)))
            .alias("clm_mdcr_instnl_tot_chrg_amt"),
        ]

    @staticmethod
    @expression(
        name="cclf_negate_cancellations_line",
        tier=["silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def negate_cancellations_line() -> list[pl.Expr]:
        """
        Negate payment amounts for canceled Part B line items.

        Per CCLF Guide Section 5.3.1 Steps 2-3:
        "Identify all the canceled records (line items) in the Part B Physician file.
        The canceled line items are identified by CLM_ADJSMT_TYPE_CD=1. Change the
        'sign' of the variable CLM_LINE_CVRD_PD_AMT for each of these canceled
        line items."

        Same logic applies to Part B DME file (CCLF6).

        Returns:
            list[pl.Expr]: Expressions that negate amounts for canceled line items:
                - clm_line_cvrd_pd_amt: Negated if clm_adjsmt_type_cd='1'
                - clm_line_alowd_chrg_amt: Negated for consistency

        Notes:
            - Applies to both CCLF5 (Physician) and CCLF6 (DME)
            - Line-level cancellations work same as header-level
            - After netting, zero-sum line items are fully canceled
            - Allowed charge amount also negated for consistency
        """
        return [
            pl.when(pl.col("clm_adjsmt_type_cd") == "1")
            .then(-pl.col("clm_line_cvrd_pd_amt").cast(pl.Decimal(scale=2)))
            .otherwise(pl.col("clm_line_cvrd_pd_amt").cast(pl.Decimal(scale=2)))
            .alias("clm_line_cvrd_pd_amt"),
            pl.when(pl.col("clm_adjsmt_type_cd") == "1")
            .then(-pl.col("clm_line_alowd_chrg_amt").cast(pl.Decimal(scale=2)))
            .otherwise(pl.col("clm_line_alowd_chrg_amt").cast(pl.Decimal(scale=2)))
            .alias("clm_line_alowd_chrg_amt"),
        ]

    @staticmethod
    @expression(
        name="cclf_rank_by_effective_date",
        tier=["silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def rank_by_effective_date(partition_keys: list[str]) -> pl.Expr:
        """
        Rank claim versions by effective date within natural key groups.

        Per CCLF Guide Section 5.1:
        "Claims with the same natural key represent different versions of the
        same claim event. The CLM_EFCTV_DT (effective date) indicates when
        each version became active."

        The latest version (most recent effective date) is the final action
        and should be used for utilization and expenditure calculations.

        Args:
            partition_keys: List of natural key columns to group by.
                For institutional: ["clm_blg_prvdr_oscar_num", "clm_from_dt",
                                   "clm_thru_dt", "current_bene_mbi_id"]
                For physician/DME: ["clm_cntl_num", "clm_line_num",
                                    "current_bene_mbi_id"]

        Returns:
            pl.Expr: Expression creating row_num column where:
                - 1 = latest version (most recent effective date)
                - 2+ = historical versions (older effective dates)

        Notes:
            - Sorts by clm_efctv_dt descending (most recent first)
            - Ties broken by cur_clm_uniq_id descending
            - Row number resets for each natural key group
            - After ADR netting, row_num=1 represents final claim state
        """
        return (
            pl.int_range(pl.len(), dtype=pl.Int64)
            .over(partition_keys)
            .add(1)
            .alias("row_num")
        )

    @staticmethod
    @expression(
        name="cclf_sum_by_natural_key",
        tier=["silver"],
        idempotent=True,
        sql_enabled=True,
    )
    def sum_by_natural_key(amount_col: str, partition_keys: list[str]) -> pl.Expr:
        """
        Sum payment amounts by natural key to net ADR adjustments.

        Per CCLF Guide Section 5.3.1:
        After negating cancellations, sum amounts by natural key to calculate
        net payment. This handles scenarios like:
        - Original claim: +$200
        - Cancellation: -$200
        - Replacement: +$210
        - Net result: +$210

        Args:
            amount_col: Name of the payment amount column to sum.
                Examples: "clm_pmt_amt", "clm_line_cvrd_pd_amt"
            partition_keys: Natural key columns that identify same claim event.

        Returns:
            pl.Expr: Expression creating sum column with name "sum_{amount_col}"
                containing the net payment after all adjustments.

        Notes:
            - Sum is calculated AFTER negating cancellations
            - Negative sums indicate errors or over-cancellations
            - Zero sums represent fully canceled claims
            - Window function computes sum without grouping
        """
        return (
            pl.col(amount_col)
            .sum()
            .over(partition_keys)
            .alias(f"sum_{amount_col}")
        )
