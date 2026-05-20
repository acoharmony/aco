# © 2025 HarmonyCares
# All rights reserved.

"""
Response Code Parser expression for SVA/PBVAR response code analysis.

Provides flexible, reusable logic to parse CMS response codes into structured
error categories, eligibility issues, and precedence flags.

Response Code Categories (per CMS documentation):
- A0/A1: Accepted alignments
- A2: Accepted but ineligible for performance year
- V0-V2: Validation errors (invalid signature, missing data, etc.)
- P0-P2: Precedence issues (duplicate, superseded, already in another model)
- E0-E5: Eligibility issues (deceased, not enrolled, MA, outside service area, etc.)
"""

import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression

# Response code mapping (per CMS REACH/MSSP documentation)
RESPONSE_CODE_MAP = {
    # Acceptance codes
    "A0": ("acceptance", "Accepted - Voluntary Alignment"),
    "A1": ("acceptance", "Accepted - Claims-Based Alignment"),
    "A2": ("acceptance_ineligible", "Accepted but Ineligible for Performance Year"),
    # Validation errors
    "V0": ("validation_errors", "Invalid Signature"),
    "V1": ("validation_errors", "Missing Required Data"),
    "V2": ("validation_errors", "Signature Date Invalid"),
    # Precedence issues
    "P0": ("precedence_issues", "Duplicate Submission"),
    "P1": ("precedence_issues", "Superseded by Later Submission"),
    "P2": ("precedence_issues", "Already in Another ACO Model"),
    # Eligibility issues
    "E0": ("eligibility_issues", "Beneficiary Deceased"),
    "E1": ("eligibility_issues", "Not Enrolled in Medicare Part A/B"),
    "E2": ("eligibility_issues", "Enrolled in Medicare Advantage"),
    "E3": ("eligibility_issues", "Outside ACO Service Area"),
    "E4": ("eligibility_issues", "ESRD Status"),
    "E5": ("eligibility_issues", "Hospice Care"),
}


@register_expression(
    "response_code_parser",
    schemas=["silver", "gold"],
    dataset_types=["alignment", "voluntary"],
    callable=True,  # Can be called as a function
    description="Parse SVA/PBVAR response codes into structured error categories",
)
class ResponseCodeParserExpression:
    """
    Expression for parsing CMS response codes into actionable categories.

        This is a REUSABLE, FLEXIBLE expression that can be applied to:
        - SVA submission response files
        - PBVAR alignment reports
        - Historical response code tracking
        - Any CMS response code field

        Returns structured data suitable for business logic and operational workflows.
    """

    @staticmethod
    @expression(name="parse_response_codes", tier=["bronze"], idempotent=True, sql_enabled=True)
    def parse_response_codes(response_code_col: str = "response_codes") -> list[pl.Expr]:
        """
        Parse response codes into structured categories.

                This returns a list of Polars expressions that can be added to any DataFrame
                containing a response code column.

                Args:
                    response_code_col: Name of the column containing response codes
                                      (comma-separated string like "A0,V1,E2")

                Returns:
                    List of Polars expressions for:
                    - response_code_list: Unique sorted response codes
                    - latest_response_code: First/primary response code
                    - error_category: Primary error category
                    - eligibility_issues: Comma-separated eligibility codes
                    - precedence_issues: Comma-separated precedence codes
                    - validation_errors: Comma-separated validation codes
                    - has_ineligible_alignment: Boolean for A2 code
                    - has_acceptance: Boolean for A0/A1 codes
                    - has_validation_error: Boolean for V* codes
                    - has_precedence_issue: Boolean for P* codes
                    - has_eligibility_issue: Boolean for E* codes

        """
        return [
            # response_code_list: Keep original comma-separated list, sorted for consistency
            pl.when(pl.col(response_code_col).is_not_null())
            .then(pl.col(response_code_col).str.split(",").list.unique().list.sort().list.join(","))
            .otherwise(None)
            .alias("response_code_list"),
            # latest_response_code: First code in list (most recent/primary)
            pl.when(pl.col(response_code_col).is_not_null())
            .then(pl.col(response_code_col).str.split(",").list.first())
            .otherwise(None)
            .alias("latest_response_code"),
            # Acceptance flags
            pl.when(pl.col(response_code_col).is_not_null())
            .then(
                pl.col(response_code_col).str.contains(r"A[01]")  # A0 or A1
            )
            .otherwise(False)
            .alias("has_acceptance"),
            pl.when(pl.col(response_code_col).is_not_null())
            .then(
                pl.col(response_code_col).str.contains("A2")  # Accepted but ineligible
            )
            .otherwise(False)
            .alias("has_ineligible_alignment"),
            # Extract validation errors (V0, V1, V2)
            pl.when(pl.col(response_code_col).is_not_null())
            .then(pl.col(response_code_col).str.extract_all(r"V[0-2]").list.join(","))
            .otherwise(None)
            .alias("validation_errors"),
            pl.when(pl.col(response_code_col).is_not_null())
            .then(pl.col(response_code_col).str.contains(r"V[0-2]"))
            .otherwise(False)
            .alias("has_validation_error"),
            # Extract precedence issues (P0, P1, P2)
            pl.when(pl.col(response_code_col).is_not_null())
            .then(pl.col(response_code_col).str.extract_all(r"P[0-2]").list.join(","))
            .otherwise(None)
            .alias("precedence_issues"),
            pl.when(pl.col(response_code_col).is_not_null())
            .then(pl.col(response_code_col).str.contains(r"P[0-2]"))
            .otherwise(False)
            .alias("has_precedence_issue"),
            # Extract eligibility issues (E0-E5)
            pl.when(pl.col(response_code_col).is_not_null())
            .then(pl.col(response_code_col).str.extract_all(r"E[0-5]").list.join(","))
            .otherwise(None)
            .alias("eligibility_issues"),
            pl.when(pl.col(response_code_col).is_not_null())
            .then(pl.col(response_code_col).str.contains(r"E[0-5]"))
            .otherwise(False)
            .alias("has_eligibility_issue"),
            # error_category: Primary category (prioritize in order: eligibility > precedence > validation > acceptance)
            pl.when(pl.col(response_code_col).is_not_null())
            .then(
                pl.when(pl.col(response_code_col).str.contains(r"E[0-5]"))
                .then(pl.lit("eligibility_issues"))
                .when(pl.col(response_code_col).str.contains(r"P[0-2]"))
                .then(pl.lit("precedence_issues"))
                .when(pl.col(response_code_col).str.contains(r"V[0-2]"))
                .then(pl.lit("validation_errors"))
                .when(pl.col(response_code_col).str.contains("A2"))
                .then(pl.lit("accepted_ineligible"))
                .when(pl.col(response_code_col).str.contains(r"A[01]"))
                .then(pl.lit("accepted"))
                .otherwise(pl.lit("unknown"))
            )
            .otherwise(None)
            .alias("error_category"),
        ]

