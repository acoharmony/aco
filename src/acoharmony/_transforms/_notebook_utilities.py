# © 2025 HarmonyCares
# All rights reserved.

"""
Utility functions for notebook calculations.

Provides helper transforms for common operations like extracting year-months,
calculating basic stats, preparing outreach data, and enriching datasets.
"""

from datetime import date

import polars as pl
from dateutil.relativedelta import relativedelta

_BENEFICIARY_ID_COLUMNS = ("current_mbi", "bene_mbi", "mbi")
_DEATH_DATE_COLUMNS = ("death_date", "bene_death_date", "bene_date_of_death")


def _beneficiary_id_column(schema: list[str]) -> str | None:
    return next((col for col in _BENEFICIARY_ID_COLUMNS if col in schema), None)


def _bool_expr(schema: list[str], column: str) -> pl.Expr:
    if column in schema:
        return pl.col(column).fill_null(False)
    return pl.lit(False)


def _living_expr(schema: list[str]) -> pl.Expr:
    expr = pl.lit(True)
    for column in _DEATH_DATE_COLUMNS:
        if column in schema:
            expr = expr & pl.col(column).is_null()
    return expr


def _month_bounds(year_month: str) -> tuple[date, date]:
    year = int(year_month[:4])
    month = int(year_month[4:6])
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    return date(year, month, 1), next_month - relativedelta(days=1)


def _recent_ffs_expr(schema: list[str], year_month: str, reach: pl.Expr, mssp: pl.Expr) -> pl.Expr:
    if "last_ffs_date" in schema:
        _, month_end = _month_bounds(year_month)
        lookback_start = month_end - relativedelta(months=24)
        return (
            pl.col("last_ffs_date")
            .cast(pl.Date, strict=False)
            .is_between(lookback_start, month_end, closed="both")
            .fill_null(False)
            & ~reach
            & ~mssp
        )
    return _bool_expr(schema, f"ym_{year_month}_ffs") & ~reach & ~mssp


def _beneficiary_count(df: pl.LazyFrame, schema: list[str]) -> int:
    id_column = _beneficiary_id_column(schema)
    expr = pl.col(id_column).n_unique() if id_column else pl.len()
    return df.select(expr).collect().item()


def calculate_basic_stats(df: pl.LazyFrame) -> dict[str, int | str | None]:
    """
    Calculate basic dataset statistics.

    Args:
        df: LazyFrame with consolidated alignment data

    Returns:
        dict with:
            - total_records: Total number of rows
            - total_columns: Total number of columns
            - unique_beneficiaries: Unique beneficiary count when an ID column exists
            - duplicate_beneficiary_records: Rows above the unique beneficiary grain
            - living_beneficiaries: Beneficiaries without a death date
            - deceased_beneficiaries: Beneficiaries with a death date
            - most_recent_ym: Most recent year-month in the wide monthly columns
            - current_aligned_beneficiaries: Living beneficiaries currently in REACH or MSSP
    """
    schema = df.collect_schema().names()
    total_records = df.select(pl.len()).collect().item()
    total_columns = len(schema)
    unique_beneficiaries = _beneficiary_count(df, schema)
    living_df = df.filter(_living_expr(schema))
    living_beneficiaries = _beneficiary_count(living_df, schema)
    deceased_beneficiaries = max(unique_beneficiaries - living_beneficiaries, 0)

    year_months = sorted({col.split("_")[1] for col in schema if col.startswith("ym_")})
    most_recent_ym = year_months[-1] if year_months else None
    current_aligned_beneficiaries = 0
    current_ffs_beneficiaries = 0
    current_view_beneficiaries = 0
    if most_recent_ym:
        reach_col = f"ym_{most_recent_ym}_reach"
        mssp_col = f"ym_{most_recent_ym}_mssp"
        reach = _bool_expr(schema, reach_col)
        mssp = _bool_expr(schema, mssp_col)
        ffs = _recent_ffs_expr(schema, most_recent_ym, reach, mssp)
        current_counts = living_df.select(
            [
                (reach | mssp).sum().alias("current_aligned_beneficiaries"),
                ffs.sum().alias("current_ffs_beneficiaries"),
                (reach | mssp | ffs).sum().alias("current_view_beneficiaries"),
            ]
        ).collect()
        current_aligned_beneficiaries = current_counts["current_aligned_beneficiaries"][0]
        current_ffs_beneficiaries = current_counts["current_ffs_beneficiaries"][0]
        current_view_beneficiaries = current_counts["current_view_beneficiaries"][0]

    return {
        "total_records": total_records,
        "total_columns": total_columns,
        "unique_beneficiaries": unique_beneficiaries,
        "duplicate_beneficiary_records": total_records - unique_beneficiaries,
        "living_beneficiaries": living_beneficiaries,
        "deceased_beneficiaries": deceased_beneficiaries,
        "most_recent_ym": most_recent_ym,
        "current_aligned_beneficiaries": current_aligned_beneficiaries,
        "current_aco_aligned_beneficiaries": current_aligned_beneficiaries,
        "current_ffs_beneficiaries": current_ffs_beneficiaries,
        "current_view_beneficiaries": current_view_beneficiaries,
    }


def extract_year_months(df: pl.LazyFrame) -> tuple[str | None, list[str]]:
    """
    Extract available year-months from ym_* columns.

    Scans column names for ym_YYYYMM_* pattern and extracts unique
    year-month values, returning them sorted.

    Args:
        df: LazyFrame with consolidated alignment data

    Returns:
        Tuple of (most_recent_ym, year_months):
            - most_recent_ym: Most recent year-month string (e.g., "202401")
            - year_months: Sorted list of all year-month strings
    """
    # Get all year-month columns
    ym_columns = [col for col in df.collect_schema().names() if col.startswith("ym_")]

    if ym_columns:
        # Extract unique year-months and find the most recent
        year_months = sorted({col.split("_")[1] for col in ym_columns})
        most_recent_ym = year_months[-1] if year_months else None
    else:
        most_recent_ym = None
        year_months = []

    return most_recent_ym, year_months


def calculate_historical_program_distribution(df: pl.LazyFrame) -> pl.DataFrame:
    """
    Calculate HISTORICAL program distribution (ever aligned).

    Calculates how many beneficiaries were EVER in REACH, MSSP, both, or neither
    over the entire observable history.

    Args:
        df: LazyFrame with consolidated alignment data

    Returns:
        DataFrame with historical alignment counts:
            - total_beneficiaries: Unique beneficiary count
            - ever_reach_only_count: Ever enrolled in REACH but never MSSP
            - ever_mssp_only_count: Ever enrolled in MSSP but never REACH
            - ever_both_count: Ever in both programs
            - never_aligned_count: Never enrolled in either
            - ever_reach_count: Backward-compatible any-REACH count
            - ever_mssp_count: Backward-compatible any-MSSP count
            - ever_reach_any_count: Any REACH count
            - ever_mssp_any_count: Any MSSP count
    """
    schema = df.collect_schema().names()
    id_column = _beneficiary_id_column(schema)

    normalized = df.with_columns(
        [
            _bool_expr(schema, "ever_reach").alias("_ever_reach"),
            _bool_expr(schema, "ever_mssp").alias("_ever_mssp"),
        ]
    )
    if id_column:
        beneficiary_df = normalized.group_by(id_column).agg(
            [
                pl.col("_ever_reach").any().alias("ever_reach"),
                pl.col("_ever_mssp").any().alias("ever_mssp"),
            ]
        )
    else:
        beneficiary_df = normalized.select(
            [
                pl.col("_ever_reach").alias("ever_reach"),
                pl.col("_ever_mssp").alias("ever_mssp"),
            ]
        )

    reach = pl.col("ever_reach")
    mssp = pl.col("ever_mssp")
    return beneficiary_df.select(
        [
            pl.len().alias("total_beneficiaries"),
            (reach & ~mssp).sum().alias("ever_reach_only_count"),
            (~reach & mssp).sum().alias("ever_mssp_only_count"),
            (reach & mssp).sum().alias("ever_both_count"),
            (~reach & ~mssp).sum().alias("never_aligned_count"),
            reach.sum().alias("ever_reach_count"),
            mssp.sum().alias("ever_mssp_count"),
            reach.sum().alias("ever_reach_any_count"),
            mssp.sum().alias("ever_mssp_any_count"),
        ]
    ).collect()


def calculate_current_program_distribution(
    df: pl.LazyFrame, most_recent_ym: str | None
) -> pl.DataFrame:
    """
    Calculate CURRENT program distribution based on most recent month.

    Calculates current alignment status (REACH/MSSP/both/neither) for the
    most recent available month.

    Args:
        df: LazyFrame with consolidated alignment data
        most_recent_ym: Most recent year-month string (e.g., "202401")

    Returns:
        DataFrame with current alignment counts:
            - living_beneficiaries: Living denominator
            - currently_reach_only: Currently in REACH only
            - currently_mssp_only: Currently in MSSP only
            - currently_both: Currently in both programs
            - currently_ffs: Currently FFS
            - currently_unassigned: Living beneficiaries not in REACH/MSSP/FFS
            - currently_reach/currently_mssp: Backward-compatible any counts
            - currently_neither: Backward-compatible not-REACH-or-MSSP count
    """
    if most_recent_ym:
        # Use the most recent month columns for current status
        current_reach_col = f"ym_{most_recent_ym}_reach"
        current_mssp_col = f"ym_{most_recent_ym}_mssp"

        # Check if columns exist
        schema_names = df.collect_schema().names()

        if current_reach_col in schema_names or current_mssp_col in schema_names:
            id_column = _beneficiary_id_column(schema_names)
            reach_input = _bool_expr(schema_names, current_reach_col)
            mssp_input = _bool_expr(schema_names, current_mssp_col)
            ffs_input = _recent_ffs_expr(schema_names, most_recent_ym, reach_input, mssp_input)
            normalized = df.filter(_living_expr(schema_names)).with_columns(
                [
                    reach_input.alias("_current_reach"),
                    mssp_input.alias("_current_mssp"),
                    ffs_input.alias("_current_ffs"),
                ]
            )
            if id_column:
                beneficiary_df = normalized.group_by(id_column).agg(
                    [
                        pl.col("_current_reach").any().alias("current_reach"),
                        pl.col("_current_mssp").any().alias("current_mssp"),
                        pl.col("_current_ffs").any().alias("current_ffs"),
                    ]
                )
            else:
                beneficiary_df = normalized.select(
                    [
                        pl.col("_current_reach").alias("current_reach"),
                        pl.col("_current_mssp").alias("current_mssp"),
                        pl.col("_current_ffs").alias("current_ffs"),
                    ]
                )

            reach = pl.col("current_reach")
            mssp = pl.col("current_mssp")
            ffs = pl.col("current_ffs") & ~reach & ~mssp
            current_alignment_stats = beneficiary_df.select(
                [
                    pl.len().alias("living_beneficiaries"),
                    (reach & ~mssp).sum().alias("currently_reach_only"),
                    (~reach & mssp).sum().alias("currently_mssp_only"),
                    (reach & mssp).sum().alias("currently_both"),
                    ffs.sum().alias("currently_ffs"),
                    (~reach & ~mssp & ~ffs).sum().alias("currently_unassigned"),
                    reach.sum().alias("currently_reach"),
                    mssp.sum().alias("currently_mssp"),
                    (reach | mssp).sum().alias("currently_aligned_any"),
                    (reach | mssp | ffs).sum().alias("currently_active_any"),
                    (~reach & ~mssp).sum().alias("currently_neither"),
                ]
            ).collect()
        else:
            # Fallback if columns don't exist
            current_alignment_stats = pl.DataFrame(
                {
                    "living_beneficiaries": [0],
                    "currently_reach_only": [0],
                    "currently_mssp_only": [0],
                    "currently_both": [0],
                    "currently_ffs": [0],
                    "currently_unassigned": [0],
                    "currently_reach": [0],
                    "currently_mssp": [0],
                    "currently_aligned_any": [0],
                    "currently_active_any": [0],
                    "currently_neither": [0],
                }
            )
    else:
        # No temporal data available
        current_alignment_stats = pl.DataFrame(
            {
                "living_beneficiaries": [0],
                "currently_reach_only": [0],
                "currently_mssp_only": [0],
                "currently_both": [0],
                "currently_ffs": [0],
                "currently_unassigned": [0],
                "currently_reach": [0],
                "currently_mssp": [0],
                "currently_aligned_any": [0],
                "currently_active_any": [0],
                "currently_neither": [0],
            }
        )

    return current_alignment_stats


def analyze_sva_action_categories(df_enriched: pl.LazyFrame) -> pl.DataFrame:
    """
    Analyze SVA action needed categories across beneficiary population.

    Groups beneficiaries by their `sva_action_needed` status and counts each category.
    This helps identify how many beneficiaries need SVA renewal, new signatures, etc.

    Args:
        df_enriched: LazyFrame with enriched alignment data (must have sva_action_needed column)

    Returns:
        DataFrame with SVA action categories and counts, sorted by frequency descending
    """
    schema = df_enriched.collect_schema().names()

    if "sva_action_needed" not in schema:
        # Return empty DataFrame if column doesn't exist
        return pl.DataFrame({"sva_action_needed": [], "count": []})

    action_stats = (
        df_enriched.group_by("sva_action_needed")
        .agg(pl.len().alias("count"))
        .collect()
        .sort("count", descending=True)
    )

    return action_stats


def calculate_current_and_historical_sources(
    df_enriched: pl.LazyFrame, selected_ym: str | None
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """
    Calculate BOTH current and historical alignment source distributions.

    Args:
        df_enriched: LazyFrame with enriched alignment data
        selected_ym: Selected year-month string (optional)

    Returns:
        Tuple of (current_source_stats, historical_source_stats):
            - current_source_stats: Current alignment sources (SVA Active/Expired, PBVAR, Claims)
            - historical_source_stats: Historical primary alignment sources
    """
    from .._expressions._enrollment_status import build_living_beneficiary_expr

    schema = df_enriched.collect_schema().names()

    # CURRENT alignment source (based on slider-selected month)
    if selected_ym:
        reach_col = f"ym_{selected_ym}_reach"
        mssp_col = f"ym_{selected_ym}_mssp"

        if reach_col in schema and mssp_col in schema:
            # Filter to living and currently aligned
            living_expr = build_living_beneficiary_expr(schema)
            currently_aligned = df_enriched.filter(
                (pl.col(reach_col) | pl.col(mssp_col)) & living_expr
            )

            if currently_aligned.select(pl.len()).collect().item() > 0:
                # Build current source expression
                current_source_expr = (
                    pl.when(
                        (pl.col("primary_alignment_source") == "sva")
                        if "primary_alignment_source" in schema
                        else pl.lit(False)
                    )
                    .then(
                        pl.when(
                            pl.col("has_valid_voluntary_alignment")
                            if "has_valid_voluntary_alignment" in schema
                            else pl.lit(False)
                        )
                        .then(pl.lit("SVA (Active)"))
                        .otherwise(pl.lit("SVA (Expired)"))
                    )
                    .when(
                        (pl.col("primary_alignment_source") == "pbvar")
                        if "primary_alignment_source" in schema
                        else pl.lit(False)
                    )
                    .then(
                        pl.when(
                            pl.col("has_valid_voluntary_alignment")
                            if "has_valid_voluntary_alignment" in schema
                            else pl.lit(False)
                        )
                        .then(pl.lit("PBVAR (Active)"))
                        .otherwise(pl.lit("PBVAR (Expired)"))
                    )
                    .when(
                        (pl.col("primary_alignment_source") == "claims")
                        if "primary_alignment_source" in schema
                        else pl.lit(False)
                    )
                    .then(pl.lit("Claims-Based"))
                    .otherwise(pl.lit("Unknown"))
                    .alias("current_alignment_source")
                )

                current_source_stats = (
                    currently_aligned.select([current_source_expr])
                    .group_by("current_alignment_source")
                    .agg(pl.len().alias("count"))
                    .sort("count", descending=True)
                    .collect()
                )
            else:
                current_source_stats = pl.DataFrame(
                    {"current_alignment_source": ["NO DATA"], "count": [0]}
                )
        else:
            current_source_stats = pl.DataFrame(
                {"current_alignment_source": ["NO DATA"], "count": [0]}
            )
    else:
        current_source_stats = pl.DataFrame({"current_alignment_source": ["NO DATA"], "count": [0]})

    # HISTORICAL alignment source (primary_alignment_source shows historical)
    if "primary_alignment_source" in schema:
        historical_source_stats = (
            df_enriched.group_by("primary_alignment_source")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .collect()
        )
    else:
        historical_source_stats = pl.DataFrame(
            {"primary_alignment_source": ["Unknown"], "count": [0]}
        )

    return current_source_stats, historical_source_stats


def prepare_voluntary_outreach_data(
    emails_df: pl.LazyFrame, mailed_df: pl.LazyFrame
) -> tuple[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]:
    """
    Prepare VOLUNTARY ALIGNMENT outreach data for joining with alignment data.

    Filters email and mailed campaigns for "ACO Voluntary Alignment" and creates:
    - Aggregated email/letter counts per MBI
    - Campaign-specific engagement tracking
    - Campaign period extraction (e.g., "2024_Q2")

    Args:
        emails_df: LazyFrame with email campaign data
        mailed_df: LazyFrame with mailed letters data

    Returns:
        Tuple of (email_by_campaign, email_mbis, mailed_by_campaign, mailed_mbis):
            - email_by_campaign: Email campaigns grouped by period and MBI
            - email_mbis: Aggregated email metrics per MBI
            - mailed_by_campaign: Mailed campaigns grouped by period and MBI
            - mailed_mbis: Aggregated mailed metrics per MBI
    """
    email_schema = emails_df.collect_schema().names()
    mail_schema = mailed_df.collect_schema().names()

    # Filter for voluntary alignment campaigns only and extract quarter
    voluntary_emails = (
        emails_df.filter(
            pl.col("campaign").str.contains("ACO Voluntary Alignment")
            if "campaign" in email_schema
            else pl.lit(False)
        )
        .with_columns(
            [
                # Extract year and quarter from campaign name (e.g., "2024 Q2 ACO Voluntary Alignment")
                pl.col("campaign").str.extract(r"(\d{4})\s+Q(\d)", 1).alias("campaign_year")
                if "campaign" in email_schema
                else pl.lit(None),
                pl.col("campaign").str.extract(r"(\d{4})\s+Q(\d)", 2).alias("campaign_quarter")
                if "campaign" in email_schema
                else pl.lit(None),
            ]
        )
        .with_columns(
            (pl.col("campaign_year") + "_Q" + pl.col("campaign_quarter")).alias("campaign_period")
        )
    )

    # Get unique MBIs that have received voluntary alignment emails
    email_mbis = (
        voluntary_emails.filter(
            pl.col("mbi").is_not_null() if "mbi" in email_schema else pl.lit(False)
        )
        .group_by("mbi")
        .agg(
            [
                pl.len().alias("voluntary_email_count"),
                pl.col("campaign").n_unique().alias("voluntary_email_campaigns")
                if "campaign" in email_schema
                else pl.lit(0),
                pl.col("campaign_period").str.join(", ").alias("email_campaign_periods"),
                (
                    (pl.col("has_been_opened") == "true").sum()
                    if "has_been_opened" in email_schema
                    else pl.lit(0)
                ).alias("voluntary_emails_opened"),
                (
                    (pl.col("has_been_clicked") == "true").sum()
                    if "has_been_clicked" in email_schema
                    else pl.lit(0)
                ).alias("voluntary_emails_clicked"),
                pl.col("send_datetime").max().alias("last_voluntary_email_date")
                if "send_datetime" in email_schema
                else pl.lit(None),
            ]
        )
    )

    # Filter for voluntary alignment mailed campaigns
    voluntary_mailed = (
        mailed_df.filter(
            pl.col("campaign_name").str.contains("ACO Voluntary Alignment")
            if "campaign_name" in mail_schema
            else pl.lit(False)
        )
        .with_columns(
            [
                # Extract year and quarter from campaign name
                pl.col("campaign_name").str.extract(r"(\d{4})\s+Q(\d)", 1).alias("campaign_year")
                if "campaign_name" in mail_schema
                else pl.lit(None),
                pl.col("campaign_name").str.extract(r"(\d{4})\s+Q(\d)", 2).alias("campaign_quarter")
                if "campaign_name" in mail_schema
                else pl.lit(None),
            ]
        )
        .with_columns(
            (pl.col("campaign_year") + "_Q" + pl.col("campaign_quarter")).alias("campaign_period")
        )
    )

    # Get unique MBIs that have received voluntary alignment letters
    mailed_mbis = (
        voluntary_mailed.filter(
            pl.col("mbi").is_not_null() if "mbi" in mail_schema else pl.lit(False)
        )
        .group_by("mbi")
        .agg(
            [
                pl.len().alias("voluntary_letter_count"),
                pl.col("campaign_name").n_unique().alias("voluntary_letter_campaigns")
                if "campaign_name" in mail_schema
                else pl.lit(0),
                pl.col("campaign_period").str.join(", ").alias("letter_campaign_periods"),
                pl.col("send_datetime").max().alias("last_voluntary_letter_date")
                if "send_datetime" in mail_schema
                else pl.lit(None),
            ]
        )
    )

    # Also create campaign-specific aggregations for detailed analysis
    email_by_campaign = voluntary_emails.group_by(["campaign_period", "mbi"]).agg(
        [
            pl.len().alias("emails_sent"),
            (
                (pl.col("has_been_opened") == "true").any()
                if "has_been_opened" in email_schema
                else pl.lit(False)
            ).alias("opened"),
            (
                (pl.col("has_been_clicked") == "true").any()
                if "has_been_clicked" in email_schema
                else pl.lit(False)
            ).alias("clicked"),
        ]
    )

    mailed_by_campaign = voluntary_mailed.group_by(["campaign_period", "mbi"]).agg(
        [
            pl.len().alias("letters_sent"),
            pl.col("status").first().alias("letter_status")
            if "status" in mail_schema
            else pl.lit(None),
        ]
    )

    return email_by_campaign, email_mbis, mailed_by_campaign, mailed_mbis


def enrich_with_outreach_data(
    df: pl.LazyFrame, email_mbis: pl.LazyFrame, mailed_mbis: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Enrich alignment data with VOLUNTARY ALIGNMENT outreach information.

    Joins outreach data (emails/letters) to beneficiary alignment data and creates
    summary columns for outreach attempts, types, and engagement levels.

    Args:
        df: LazyFrame with consolidated alignment data
        email_mbis: LazyFrame with aggregated email outreach per MBI
        mailed_mbis: LazyFrame with aggregated mailed letter outreach per MBI

    Returns:
        LazyFrame with enriched alignment data including outreach columns:
            - voluntary_outreach_attempts: Total outreach attempts
            - has_voluntary_outreach: Boolean if contacted
            - voluntary_outreach_type: Type of outreach (Email, Letter, Both)
            - voluntary_engagement_level: Engagement level (Clicked, Opened, Contacted, Not Contacted)
            - campaign_periods_contacted: Comma-separated list of campaign periods
    """
    # Join email outreach data
    df_with_emails = df.join(email_mbis, left_on="current_mbi", right_on="mbi", how="left")

    # Join mailed letter outreach data
    df_enriched = df_with_emails.join(
        mailed_mbis, left_on="current_mbi", right_on="mbi", how="left"
    )

    # Create outreach summary columns for VOLUNTARY ALIGNMENT campaigns
    df_enriched = df_enriched.with_columns(
        [
            # Total voluntary alignment outreach attempts
            (
                pl.col("voluntary_email_count").fill_null(0)
                + pl.col("voluntary_letter_count").fill_null(0)
            ).alias("voluntary_outreach_attempts"),
            # Has been contacted for voluntary alignment
            ((pl.col("voluntary_email_count") > 0) | (pl.col("voluntary_letter_count") > 0)).alias(
                "has_voluntary_outreach"
            ),
            # Voluntary outreach type
            pl.when((pl.col("voluntary_email_count") > 0) & (pl.col("voluntary_letter_count") > 0))
            .then(pl.lit("Email & Letter"))
            .when(pl.col("voluntary_email_count") > 0)
            .then(pl.lit("Email Only"))
            .when(pl.col("voluntary_letter_count") > 0)
            .then(pl.lit("Letter Only"))
            .otherwise(pl.lit("No Voluntary Outreach"))
            .alias("voluntary_outreach_type"),
            # Voluntary alignment engagement level
            pl.when(pl.col("voluntary_emails_clicked") > 0)
            .then(pl.lit("Clicked"))
            .when(pl.col("voluntary_emails_opened") > 0)
            .then(pl.lit("Opened"))
            .when((pl.col("voluntary_email_count") > 0) | (pl.col("voluntary_letter_count") > 0))
            .then(pl.lit("Contacted"))
            .otherwise(pl.lit("Not Contacted"))
            .alias("voluntary_engagement_level"),
            # Campaign periods contacted (for tracking which quarters)
            pl.when(
                pl.col("email_campaign_periods").is_not_null()
                & pl.col("letter_campaign_periods").is_not_null()
            )
            .then(pl.col("email_campaign_periods") + ", " + pl.col("letter_campaign_periods"))
            .when(pl.col("email_campaign_periods").is_not_null())
            .then(pl.col("email_campaign_periods"))
            .when(pl.col("letter_campaign_periods").is_not_null())
            .then(pl.col("letter_campaign_periods"))
            .otherwise(pl.lit(""))
            .alias("campaign_periods_contacted"),
        ]
    )

    return df_enriched
