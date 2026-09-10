# © 2025 HarmonyCares
# All rights reserved.

"""
Response Code Parser expression for SVA/PBVAR response code analysis.

Provides flexible, reusable logic to parse CMS response codes into structured
error categories, eligibility issues, and precedence flags.

Response Code Categories (per CMS documentation):
- A0/A1: Accepted alignments
- A2: Accepted but previously lost eligibility for the performance year
- V0-V2: Validation errors (MBI, signature date, TIN/NPI)
- P0-P2: Precedence issues (duplicate, superseded, another model/ACO)
- E0-E5: Eligibility issues (deceased, High Needs, Medicare coverage, MA, service area)
"""

import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression

# Response code mapping (per CMS REACH/MSSP documentation)
RESPONSE_CODE_MAP = {
    # Acceptance codes
    "A0": ("acceptance", "Accepted attestation; newly aligned beneficiary."),
    "A1": (
        "acceptance",
        "Accepted attestation; beneficiary was already aligned to the same ACO during the current performance year.",
    ),
    "A2": (
        "acceptance_ineligible",
        "Accepted attestation; beneficiary was already aligned to the same ACO during the current performance year but previously lost eligibility, as such this beneficiary remains ineligible to be aligned for the remainder of this performance year.",
    ),
    # Validation errors
    "V0": (
        "validation_errors",
        "Rejected because of a missing or invalid Medicare Beneficiary Identifier (MBI) that could not be matched to CMS data.",
    ),
    "V1": (
        "validation_errors",
        "Rejected because of a missing or invalid signature date (e.g., signature date before the ACO had access to SVA; signature date after submission of SVA file, signature date outside the provider start/end date window listed on the ACO's participant list).",
    ),
    "V2": (
        "validation_errors",
        "Rejected because the Tax Identification Number (TIN) or National Provider Identifier (NPI) is missing or not listed on the ACO's Participant Provider list.",
    ),
    # Precedence issues
    "P0": (
        "precedence_issues",
        "Rejected because a duplicate attestation was submitted by the same ACO with the same or more recent signature date.",
    ),
    "P1": (
        "precedence_issues",
        "Rejected because a more recent attestation for the beneficiary took precedence outside the ACO.",
    ),
    "P2": (
        "precedence_issues",
        "Rejected because the beneficiary is participating in another model or ACO.",
    ),
    # Eligibility issues
    "E0": (
        "eligibility_issues",
        "Rejected because the beneficiary is reported as deceased in CMS data.",
    ),
    "E1": (
        "eligibility_issues",
        "High Needs ACOs only: Rejected because the beneficiary does not meet the High Needs criteria.",
    ),
    "E2": (
        "eligibility_issues",
        "Rejected because the beneficiary is not enrolled in Medicare Part A or Part B in CMS data.",
    ),
    "E3": (
        "eligibility_issues",
        "Rejected because the beneficiary is not eligible for the model because of enrollment in Medicare Advantage in CMS data.",
    ),
    "E4": (
        "eligibility_issues",
        "Rejected because the beneficiary does not live within the ACOs Extended Service Area (but lives within the United States).",
    ),
    "E5": (
        "eligibility_issues",
        "Rejected because the beneficiary does not live within the United States.",
    ),
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
