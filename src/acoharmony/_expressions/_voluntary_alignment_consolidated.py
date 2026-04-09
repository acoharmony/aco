# © 2025 HarmonyCares
# All rights reserved.

"""
Consolidated voluntary alignment expression builders.

Pure expression builders for calculating derived metrics across all touchpoint sources
(emails, mailings, SVA, PBVAR) in the voluntary_alignment consolidation.
"""

import polars as pl


def build_outreach_consolidated_exprs() -> list[pl.Expr]:
    """
    Build expressions for consolidated outreach metrics across email and mail.

    Calculates:
    - First outreach date (min of first_email_date and first_mailed_date)
    - Last outreach date (max of last_email_date and last_mailed_date)
    - Total touchpoints (sum of email and mail campaigns)

    Returns:
        list[pl.Expr]: List of consolidated outreach expressions
    """
    return [
        pl.min_horizontal([
            pl.col("first_email_date").fill_null(pl.lit(None).cast(pl.Date)),
            pl.col("first_mailed_date").fill_null(pl.lit(None).cast(pl.Date))
        ]).alias("first_outreach_date"),
        pl.max_horizontal([
            pl.col("last_email_date").fill_null(pl.lit(None).cast(pl.Date)),
            pl.col("last_mailed_date").fill_null(pl.lit(None).cast(pl.Date))
        ]).alias("last_outreach_date"),
        (pl.col("email_campaigns_sent").fill_null(0) + pl.col("mailed_campaigns_sent").fill_null(0)).alias("total_touchpoints"),
    ]


def build_days_in_funnel_expr() -> pl.Expr:
    """
    Build expression for days in funnel (calculated after first/last outreach).

    Returns:
        pl.Expr: Days between first and last outreach
    """
    return (pl.col("last_outreach_date") - pl.col("first_outreach_date")).dt.total_days().alias("days_in_funnel")


def build_alignment_journey_status_expr() -> pl.Expr:
    """
    Build expression for alignment journey status classification.

    Classification logic:
    - Never Contacted: No email or mail touchpoints
    - Contacted No Response: Has touchpoints but no engagement
    - Engaged: Has opened emails or clicked
    - Signed: Has SVA signature
    - Aligned: Currently in PBVAR

    Returns:
        pl.Expr: Alignment journey status string
    """
    return (
        pl.when(pl.col("pbvar_aligned").fill_null(False))
        .then(pl.lit("Aligned"))
        .when(pl.col("sva_signature_count") > 0)
        .then(pl.lit("Signed"))
        .when((pl.col("emails_opened") > 0) | (pl.col("emails_clicked") > 0))
        .then(pl.lit("Engaged"))
        .when(pl.col("total_touchpoints") > 0)
        .then(pl.lit("Contacted No Response"))
        .otherwise(pl.lit("Never Contacted"))
        .alias("alignment_journey_status")
    )


def build_signature_status_expr() -> pl.Expr:
    """
    Build expression for signature recency/status classification.

    Classification logic:
    - Never Signed: No SVA signatures
    - Invalid Provider: Has signature but provider not valid
    - Current Year: Signature in current calendar year
    - Recent: Signature within last 365 days
    - Aging: Signature 365-730 days old
    - Old: Signature > 730 days old

    Returns:
        pl.Expr: Signature status string
    """
    from datetime import date

    current_year = pl.lit(date.today().year)

    return (
        pl.when(pl.col("sva_signature_count") == 0)
        .then(pl.lit("Never Signed"))
        .when(~pl.col("sva_provider_valid").fill_null(False))
        .then(pl.lit("Invalid Provider"))
        .when(pl.col("most_recent_sva_date").dt.year() == current_year)
        .then(pl.lit("Current Year"))
        .when(pl.col("days_since_last_sva") <= 365)
        .then(pl.lit("Recent"))
        .when(pl.col("days_since_last_sva") <= 730)
        .then(pl.lit("Aging"))
        .otherwise(pl.lit("Old"))
        .alias("signature_status")
    )


def build_outreach_response_status_expr() -> pl.Expr:
    """
    Build expression for outreach response status classification.

    Classification logic:
    - Complained: Has email complaint
    - Unsubscribed: Has email unsubscribe
    - Email Engaged: Opened or clicked emails
    - No Response: Has touchpoints but no engagement

    Returns:
        pl.Expr: Outreach response status string
    """
    return (
        pl.when(pl.col("email_complained").fill_null(False))
        .then(pl.lit("Complained"))
        .when(pl.col("email_unsubscribed").fill_null(False))
        .then(pl.lit("Unsubscribed"))
        .when((pl.col("emails_opened") > 0) | (pl.col("emails_clicked") > 0))
        .then(pl.lit("Email Engaged"))
        .when(pl.col("total_touchpoints") > 0)
        .then(pl.lit("No Response"))
        .otherwise(pl.lit("Never Contacted"))
        .alias("outreach_response_status")
    )


def build_chase_list_eligibility_exprs() -> list[pl.Expr]:
    """
    Build expressions for chase list eligibility and reason.

    Beneficiaries are chase list eligible if:
    - Has FFS service (need signature for alignment)
    - Never signed OR signature is aging/old
    - Not complained or unsubscribed
    - Not currently aligned via PBVAR

    Returns:
        list[pl.Expr]: Chase eligibility and reason expressions
    """
    is_eligible = (
        pl.col("has_ffs_service").fill_null(False)
        & ~pl.col("pbvar_aligned").fill_null(False)
        & ~pl.col("email_complained").fill_null(False)
        & ~pl.col("email_unsubscribed").fill_null(False)
        & (
            (pl.col("signature_status") == "Never Signed")
            | (pl.col("signature_status") == "Aging")
            | (pl.col("signature_status") == "Old")
            | (pl.col("signature_status") == "Invalid Provider")
        )
    )

    chase_reason = (
        pl.when(~pl.col("has_ffs_service").fill_null(False))
        .then(pl.lit(None).cast(pl.String))
        .when(pl.col("pbvar_aligned").fill_null(False))
        .then(pl.lit(None).cast(pl.String))
        .when(pl.col("email_complained").fill_null(False))
        .then(pl.lit(None).cast(pl.String))
        .when(pl.col("email_unsubscribed").fill_null(False))
        .then(pl.lit(None).cast(pl.String))
        .when(pl.col("signature_status") == "Never Signed")
        .then(pl.lit("Never signed, has FFS service"))
        .when(pl.col("signature_status") == "Aging")
        .then(pl.lit("Signature aging (1-2 years old)"))
        .when(pl.col("signature_status") == "Old")
        .then(pl.lit("Signature old (>2 years)"))
        .when(pl.col("signature_status") == "Invalid Provider")
        .then(pl.lit("Invalid provider TIN/NPI"))
        .otherwise(pl.lit(None).cast(pl.String))
    )

    return [
        is_eligible.alias("chase_list_eligible"),
        chase_reason.alias("chase_reason"),
    ]


def build_data_quality_exprs() -> list[pl.Expr]:
    """
    Build expressions for data quality checks (invalid outreach after death/termination).

    Note: These require death_date and termination_date to be joined from other sources.
    If those columns don't exist, will default to False.

    Returns:
        list[pl.Expr]: Data quality flag expressions
    """
    return [
        # Will be calculated after death_date is joined
        pl.lit(False).alias("invalid_email_after_death"),
        pl.lit(False).alias("invalid_mail_after_death"),
        pl.lit(False).alias("invalid_outreach_after_termination"),
    ]


def build_ffs_status_exprs() -> list[pl.Expr]:
    """
    Build expressions for FFS service status flags.

    These provide default values that can be updated when FFS data is joined.

    Returns:
        list[pl.Expr]: FFS status expressions
    """
    return [
        pl.lit(False).alias("has_ffs_service"),
        pl.lit(None).cast(pl.Date).alias("ffs_first_date"),
        pl.lit(0).cast(pl.UInt32).alias("ffs_claim_count"),
        pl.lit(None).cast(pl.Int32).alias("days_since_first_ffs"),
        pl.lit(False).alias("ffs_before_alignment"),
    ]
