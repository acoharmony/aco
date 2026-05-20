# © 2025 HarmonyCares
# All rights reserved.

"""
ACO alignment consolidated metrics expression builders.

Pure expression builders for calculating business metrics from combined
ACO and voluntary alignment data. All expressions are reusable and composable.
"""

import polars as pl


def build_consolidated_program_expr() -> pl.Expr:
    """
    Build expression for consolidated program status.

    Logic: BOTH if enrolled in both REACH and MSSP, else individual program, else NONE

    Returns:
        pl.Expr: Consolidated program classification (BOTH, REACH, MSSP, NONE)
    """
    return (
        pl.when(pl.col("ever_reach") & pl.col("ever_mssp"))
        .then(pl.lit("BOTH"))
        .when(pl.col("ever_reach"))
        .then(pl.lit("REACH"))
        .when(pl.col("ever_mssp"))
        .then(pl.lit("MSSP"))
        .otherwise(pl.lit("NONE"))
        .alias("consolidated_program")
    )


def build_total_aligned_months_expr() -> pl.Expr:
    """
    Build expression for total aligned months across all programs.

    Returns:
        pl.Expr: Sum of months_in_reach + months_in_mssp
    """
    return (pl.col("months_in_reach") + pl.col("months_in_mssp")).alias("total_aligned_months")


def build_primary_alignment_source_expr() -> pl.Expr:
    """
    Build expression for primary alignment source.

    Priority: VOLUNTARY (SVA/PBVAR) > CLAIMS-based > NONE

    Returns:
        pl.Expr: Primary alignment source classification
    """
    return (
        pl.when(pl.col("has_valid_voluntary_alignment"))
        .then(pl.lit("VOLUNTARY"))
        .when(pl.col("ever_reach") | pl.col("ever_mssp"))
        .then(pl.lit("CLAIMS"))
        .otherwise(pl.lit("NONE"))
        .alias("primary_alignment_source")
    )


def build_is_currently_aligned_expr() -> pl.Expr:
    """
    Build expression for current alignment status.

    Aligned if: in a program OR has valid voluntary alignment

    Returns:
        pl.Expr: Boolean for current alignment status
    """
    return (
        pl.when((pl.col("current_program") != "None") | pl.col("has_valid_voluntary_alignment"))
        .then(True)
        .otherwise(False)
        .alias("is_currently_aligned")
    )


def build_has_voluntary_alignment_filled_expr() -> pl.Expr:
    """
    Build expression to fill null has_voluntary_alignment values.

    Returns:
        pl.Expr: Boolean with nulls filled to False
    """
    return pl.col("has_voluntary_alignment").fill_null(False)


def build_has_valid_historical_sva_expr() -> pl.Expr:
    """
    Build expression for historical valid SVA status.

    SVA is valid if: ever in REACH AND has voluntary alignment

    Returns:
        pl.Expr: Boolean for historical SVA validity
    """
    return (pl.col("ever_reach") & pl.col("has_voluntary_alignment")).alias(
        "has_valid_historical_sva"
    )


def build_has_program_transition_expr() -> pl.Expr:
    """
    Build expression to detect program transitions.

    Transition if: enrolled in both REACH and MSSP at different times

    Returns:
        pl.Expr: Boolean for program transition
    """
    return (
        pl.when((pl.col("ever_reach")) & (pl.col("ever_mssp")))
        .then(True)
        .otherwise(False)
        .alias("has_program_transition")
    )


def build_has_continuous_enrollment_expr() -> pl.Expr:
    """
    Build expression for continuous enrollment status.

    Continuous if: no enrollment gaps

    Returns:
        pl.Expr: Boolean for continuous enrollment
    """
    return (
        pl.when(pl.col("enrollment_gaps") == 0).then(True).otherwise(False).alias(
            "has_continuous_enrollment"
        )
    )


def build_bene_death_date_expr() -> pl.Expr:
    """
    Build expression to alias death_date as bene_death_date.

    Returns:
        pl.Expr: Death date column alias
    """
    return pl.col("death_date").alias("bene_death_date")


def build_crosswalk_mapping_exprs() -> list[pl.Expr]:
    """
    Build expressions for crosswalk mapping metadata.

    Creates:
    - prvs_num: Previous MBI if crosswalked
    - mapping_type: 'xref' if crosswalked, 'direct' otherwise

    Returns:
        list[pl.Expr]: Crosswalk mapping expressions
    """
    return [
        pl.when(pl.col("current_mbi") != pl.col("bene_mbi"))
        .then(pl.col("bene_mbi"))
        .otherwise(None)
        .alias("prvs_num"),
        pl.when(pl.col("current_mbi") != pl.col("bene_mbi"))
        .then(pl.lit("xref"))
        .otherwise(pl.lit("direct"))
        .alias("mapping_type"),
    ]


def build_mssp_recruitment_exprs() -> list[pl.Expr]:
    """
    Build expressions for MSSP-to-REACH recruitment logic.

    Creates:
    - mssp_sva_recruitment_target: MSSP beneficiary without valid SVA
    - mssp_to_reach_status: Recruitment readiness (ready_for_reach, needs_renewal, needs_initial_sva)

    Returns:
        list[pl.Expr]: MSSP recruitment expressions
    """
    return [
        ((pl.col("current_program") == "MSSP") & ~pl.col("has_valid_voluntary_alignment")).alias(
            "mssp_sva_recruitment_target"
        ),
        pl.when(pl.col("current_program") == "MSSP")
        .then(
            pl.when(pl.col("has_valid_voluntary_alignment"))
            .then(pl.lit("ready_for_reach"))
            .when(pl.col("has_voluntary_alignment"))
            .then(pl.lit("needs_renewal"))
            .otherwise(pl.lit("needs_initial_sva"))
        )
        .otherwise(None)
        .alias("mssp_to_reach_status"),
    ]


def build_pbvar_integration_exprs() -> list[pl.Expr]:
    """
    Build expressions for PBVAR integration logic.

    Creates:
    - sva_submitted_after_pbvar: SVA more recent than PBVAR report
    - needs_sva_refresh_from_pbvar: PBVAR shows response but no recent SVA

    Returns:
        list[pl.Expr]: PBVAR integration expressions
    """
    return [
        pl.when(
            pl.col("last_sva_submission_date").is_not_null()
            & pl.col("pbvar_report_date").is_not_null()
        )
        .then(pl.col("last_sva_submission_date") > pl.col("pbvar_report_date"))
        .otherwise(None)
        .alias("sva_submitted_after_pbvar"),
        pl.when(
            pl.col("latest_response_codes").is_not_null()
            & (
                pl.col("last_sva_submission_date").is_null()
                | (pl.col("last_sva_submission_date") <= pl.col("pbvar_report_date"))
            )
        )
        .then(True)
        .otherwise(False)
        .alias("needs_sva_refresh_from_pbvar"),
    ]


def build_provider_validation_exprs() -> list[pl.Expr]:
    """
    Build expressions for provider validation logic.

    Creates:
    - sva_tin_match: SVA TIN matches aligned TIN
    - sva_npi_match: SVA NPI matches aligned NPI

    Returns:
        list[pl.Expr]: Provider validation expressions
    """
    return [
        pl.when(
            pl.col("voluntary_provider_tin").is_not_null()
            & pl.col("aligned_provider_tin").is_not_null()
        )
        .then(pl.col("voluntary_provider_tin") == pl.col("aligned_provider_tin"))
        .otherwise(None)
        .alias("sva_tin_match"),
        pl.when(
            pl.col("voluntary_provider_npi").is_not_null()
            & pl.col("aligned_provider_npi").is_not_null()
        )
        .then(pl.col("voluntary_provider_npi") == pl.col("aligned_provider_npi"))
        .otherwise(None)
        .alias("sva_npi_match"),
    ]


def build_previous_program_expr() -> pl.Expr:
    """
    Build expression for previous program.

    Derives previous program from has_program_transition:
    - If currently REACH and transitioned, previous was MSSP
    - If currently MSSP and transitioned, previous was REACH

    Returns:
        pl.Expr: Previous program classification
    """
    return (
        pl.when(pl.col("has_program_transition"))
        .then(
            pl.when(pl.col("current_program") == "REACH")
            .then(pl.lit("MSSP"))
            .when(pl.col("current_program") == "MSSP")
            .then(pl.lit("REACH"))
            .otherwise(None)
        )
        .otherwise(None)
        .alias("previous_program")
    )


def build_program_transitions_expr(fallback_value: int = 0) -> pl.Expr:
    """
    Build expression for program transitions count.

    Tries to use program_switches column, falls back to default if not available.

    Args:
        fallback_value: Default value if program_switches doesn't exist

    Returns:
        pl.Expr: Program transitions count
    """
    return pl.coalesce([pl.col("program_switches"), pl.lit(fallback_value)]).cast(pl.UInt32).alias(
        "program_transitions"
    )
