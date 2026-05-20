# © 2025 HarmonyCares
# All rights reserved.

"""
Voluntary alignment statistics calculations for notebook.

Provides transforms for calculating SVA (Standard Voluntary Alignment) metrics,
including current status, renewal needs, and outreach statistics.
"""

import polars as pl

from .._expressions._enrollment_status import (
    build_active_enrollment_expr,
    build_living_beneficiary_expr,
)
from .._expressions._voluntary_alignment_notebook import (
    build_email_engaged_expr,
    build_has_outreach_expr,
    build_has_valid_sva_expr,
    build_needs_outreach_expr,
    build_needs_sva_renewal_expr,
)


def calculate_voluntary_alignment_stats(df_enriched: pl.LazyFrame, yearmo: str) -> dict[str, int]:
    """
    Calculate voluntary alignment statistics for a specific year-month.

    Calculates comprehensive SVA metrics including:
    - Current REACH beneficiaries with valid/expired SVA
    - MSSP beneficiaries eligible for SVA
    - Outreach contact statistics
    - Renewal needs analysis

    Args:
        df_enriched: LazyFrame with enriched alignment data (including outreach columns)
        yearmo: Year-month string (e.g., "202401")

    Returns:
        dict[str, int]: SVA statistics with 16 metrics:
            - currently_voluntary: REACH beneficiaries with valid SVA
            - currently_claims: REACH beneficiaries without valid SVA
            - reach_needs_renewal: REACH with expired SVA
            - reach_contacted: REACH who received outreach
            - reach_email_engaged: REACH who opened emails
            - reach_needs_outreach: REACH needing outreach
            - mssp_sva_eligible: MSSP eligible for SVA
            - mssp_expired_sva: MSSP with expired SVA
            - mssp_contacted: MSSP who received outreach
            - mssp_needs_outreach: MSSP needing outreach
            - ever_voluntary: Total ever having SVA
            - has_valid_signature: Total with valid SVA
            - total_contacted: Total who received outreach
            - total_outreach_attempts: Total outreach attempts
            - total_email_engaged: Total who engaged with emails
            - total_email_clicked: Total who clicked email links
    """
    schema = df_enriched.collect_schema().names()

    # Build filter expressions
    build_living_beneficiary_expr(schema)
    reach_active_expr = build_active_enrollment_expr(yearmo, "reach", schema)
    mssp_active_expr = build_active_enrollment_expr(yearmo, "mssp", schema)

    # Build SVA status expressions
    has_valid_sva = build_has_valid_sva_expr(schema)
    needs_renewal = build_needs_sva_renewal_expr(schema)
    has_outreach = build_has_outreach_expr(schema)
    email_engaged = build_email_engaged_expr(schema)
    needs_outreach = build_needs_outreach_expr(schema)

    # Calculate REACH statistics
    reach_stats = (
        df_enriched.filter(reach_active_expr)
        .select(
            [
                has_valid_sva.sum().alias("currently_voluntary"),
                (~has_valid_sva).sum().alias("currently_claims"),
                needs_renewal.sum().alias("reach_needs_renewal"),
                has_outreach.sum().alias("reach_contacted"),
                email_engaged.sum().alias("reach_email_engaged"),
                needs_outreach.sum().alias("reach_needs_outreach"),
            ]
        )
        .collect()
    )

    # Calculate MSSP statistics
    mssp_stats = (
        df_enriched.filter(mssp_active_expr)
        .select(
            [
                (~pl.col("has_voluntary_alignment") if "has_voluntary_alignment" in schema else pl.lit(True))
                .sum()
                .alias("mssp_sva_eligible"),
                (needs_renewal).sum().alias("mssp_expired_sva"),
                has_outreach.sum().alias("mssp_contacted"),
                ((~pl.col("has_voluntary_alignment") if "has_voluntary_alignment" in schema else pl.lit(False)) & ~has_outreach)
                .sum()
                .alias("mssp_needs_outreach"),
            ]
        )
        .collect()
    )

    # Calculate overall statistics
    overall_stats = (
        df_enriched.select(
            [
                (pl.col("has_voluntary_alignment") if "has_voluntary_alignment" in schema else pl.lit(False))
                .sum()
                .alias("ever_voluntary"),
                has_valid_sva.sum().alias("has_valid_signature"),
                has_outreach.sum().alias("total_contacted"),
                (pl.col("voluntary_outreach_attempts") if "voluntary_outreach_attempts" in schema else pl.lit(0))
                .sum()
                .alias("total_outreach_attempts"),
                email_engaged.sum().alias("total_email_engaged"),
                ((pl.col("voluntary_emails_clicked") > 0) if "voluntary_emails_clicked" in schema else pl.lit(False))
                .sum()
                .alias("total_email_clicked"),
            ]
        )
        .collect()
    )

    # Combine results
    return {
        "currently_voluntary": int(reach_stats["currently_voluntary"][0]),
        "currently_claims": int(reach_stats["currently_claims"][0]),
        "reach_needs_renewal": int(reach_stats["reach_needs_renewal"][0]),
        "reach_contacted": int(reach_stats["reach_contacted"][0]),
        "reach_email_engaged": int(reach_stats["reach_email_engaged"][0]),
        "reach_needs_outreach": int(reach_stats["reach_needs_outreach"][0]),
        "mssp_sva_eligible": int(mssp_stats["mssp_sva_eligible"][0]),
        "mssp_expired_sva": int(mssp_stats["mssp_expired_sva"][0]),
        "mssp_contacted": int(mssp_stats["mssp_contacted"][0]),
        "mssp_needs_outreach": int(mssp_stats["mssp_needs_outreach"][0]),
        "ever_voluntary": int(overall_stats["ever_voluntary"][0]),
        "has_valid_signature": int(overall_stats["has_valid_signature"][0]),
        "total_contacted": int(overall_stats["total_contacted"][0]),
        "total_outreach_attempts": int(overall_stats["total_outreach_attempts"][0]),
        "total_email_engaged": int(overall_stats["total_email_engaged"][0]),
        "total_email_clicked": int(overall_stats["total_email_clicked"][0]),
    }
