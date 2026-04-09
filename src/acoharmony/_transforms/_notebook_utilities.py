# © 2025 HarmonyCares
# All rights reserved.

"""
Utility functions for notebook calculations.

Provides helper transforms for common operations like extracting year-months,
calculating basic stats, preparing outreach data, and enriching datasets.
"""

import polars as pl


def calculate_basic_stats(df: pl.LazyFrame) -> dict[str, int]:
    """
    Calculate basic dataset statistics.

    Args:
        df: LazyFrame with consolidated alignment data

    Returns:
        dict with:
            - total_records: Total number of rows
            - total_columns: Total number of columns
    """
    total_records = df.select(pl.len()).collect().item()
    total_columns = len(df.collect_schema())

    return {"total_records": total_records, "total_columns": total_columns}


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
            - ever_reach_count: Ever enrolled in REACH
            - ever_mssp_count: Ever enrolled in MSSP
            - ever_both_count: Ever in both programs
            - never_aligned_count: Never enrolled in either
    """
    schema = df.collect_schema().names()

    # Build aggregations
    agg_exprs = []

    if "ever_reach" in schema:
        agg_exprs.append(pl.col("ever_reach").sum().alias("ever_reach_count"))
    else:
        agg_exprs.append(pl.lit(0).alias("ever_reach_count"))

    if "ever_mssp" in schema:
        agg_exprs.append(pl.col("ever_mssp").sum().alias("ever_mssp_count"))
    else:
        agg_exprs.append(pl.lit(0).alias("ever_mssp_count"))

    if "ever_reach" in schema and "ever_mssp" in schema:
        agg_exprs.append((pl.col("ever_reach") & pl.col("ever_mssp")).sum().alias("ever_both_count"))
        agg_exprs.append((~pl.col("ever_reach") & ~pl.col("ever_mssp")).sum().alias("never_aligned_count"))
    else:
        agg_exprs.append(pl.lit(0).alias("ever_both_count"))
        agg_exprs.append(pl.len().alias("never_aligned_count"))

    historical_stats = df.select(agg_exprs).collect()

    return historical_stats


def calculate_current_program_distribution(df: pl.LazyFrame, most_recent_ym: str | None) -> pl.DataFrame:
    """
    Calculate CURRENT program distribution based on most recent month.

    Calculates current alignment status (REACH/MSSP/both/neither) for the
    most recent available month.

    Args:
        df: LazyFrame with consolidated alignment data
        most_recent_ym: Most recent year-month string (e.g., "202401")

    Returns:
        DataFrame with current alignment counts:
            - currently_reach: Currently in REACH only
            - currently_mssp: Currently in MSSP only
            - currently_both: Currently in both programs
            - currently_neither: Not currently in either program
    """
    if most_recent_ym:
        # Use the most recent month columns for current status
        current_reach_col = f"ym_{most_recent_ym}_reach"
        current_mssp_col = f"ym_{most_recent_ym}_mssp"

        # Check if columns exist
        schema_names = df.collect_schema().names()

        if current_reach_col in schema_names and current_mssp_col in schema_names:
            current_alignment_stats = df.select(
                [
                    pl.col(current_reach_col).sum().alias("currently_reach"),
                    pl.col(current_mssp_col).sum().alias("currently_mssp"),
                    (pl.col(current_reach_col) & pl.col(current_mssp_col)).sum().alias("currently_both"),
                    (~pl.col(current_reach_col) & ~pl.col(current_mssp_col)).sum().alias("currently_neither"),
                ]
            ).collect()
        else:
            # Fallback if columns don't exist
            current_alignment_stats = pl.DataFrame(
                {
                    "currently_reach": [0],
                    "currently_mssp": [0],
                    "currently_both": [0],
                    "currently_neither": [0],
                }
            )
    else:
        # No temporal data available
        current_alignment_stats = pl.DataFrame(
            {
                "currently_reach": [0],
                "currently_mssp": [0],
                "currently_both": [0],
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
        df_enriched.group_by("sva_action_needed").agg(pl.len().alias("count")).collect().sort("count", descending=True)
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
            currently_aligned = df_enriched.filter((pl.col(reach_col) | pl.col(mssp_col)) & living_expr)

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
                current_source_stats = pl.DataFrame({"current_alignment_source": ["NO DATA"], "count": [0]})
        else:
            current_source_stats = pl.DataFrame({"current_alignment_source": ["NO DATA"], "count": [0]})
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
        historical_source_stats = pl.DataFrame({"primary_alignment_source": ["Unknown"], "count": [0]})

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
            pl.col("campaign").str.contains("ACO Voluntary Alignment") if "campaign" in email_schema else pl.lit(False)
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
        .with_columns((pl.col("campaign_year") + "_Q" + pl.col("campaign_quarter")).alias("campaign_period"))
    )

    # Get unique MBIs that have received voluntary alignment emails
    email_mbis = (
        voluntary_emails.filter(pl.col("mbi").is_not_null() if "mbi" in email_schema else pl.lit(False))
        .group_by("mbi")
        .agg(
            [
                pl.len().alias("voluntary_email_count"),
                pl.col("campaign").n_unique().alias("voluntary_email_campaigns")
                if "campaign" in email_schema
                else pl.lit(0),
                pl.col("campaign_period").str.join(", ").alias("email_campaign_periods"),
                ((pl.col("has_been_opened") == "true").sum() if "has_been_opened" in email_schema else pl.lit(0)).alias(
                    "voluntary_emails_opened"
                ),
                (
                    (pl.col("has_been_clicked") == "true").sum() if "has_been_clicked" in email_schema else pl.lit(0)
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
        .with_columns((pl.col("campaign_year") + "_Q" + pl.col("campaign_quarter")).alias("campaign_period"))
    )

    # Get unique MBIs that have received voluntary alignment letters
    mailed_mbis = (
        voluntary_mailed.filter(pl.col("mbi").is_not_null() if "mbi" in mail_schema else pl.lit(False))
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
            ((pl.col("has_been_opened") == "true").any() if "has_been_opened" in email_schema else pl.lit(False)).alias(
                "opened"
            ),
            (
                (pl.col("has_been_clicked") == "true").any() if "has_been_clicked" in email_schema else pl.lit(False)
            ).alias("clicked"),
        ]
    )

    mailed_by_campaign = voluntary_mailed.group_by(["campaign_period", "mbi"]).agg(
        [
            pl.len().alias("letters_sent"),
            pl.col("status").first().alias("letter_status") if "status" in mail_schema else pl.lit(None),
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
    df_enriched = df_with_emails.join(mailed_mbis, left_on="current_mbi", right_on="mbi", how="left")

    # Create outreach summary columns for VOLUNTARY ALIGNMENT campaigns
    df_enriched = df_enriched.with_columns(
        [
            # Total voluntary alignment outreach attempts
            (pl.col("voluntary_email_count").fill_null(0) + pl.col("voluntary_letter_count").fill_null(0)).alias(
                "voluntary_outreach_attempts"
            ),
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
            pl.when(pl.col("email_campaign_periods").is_not_null() & pl.col("letter_campaign_periods").is_not_null())
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
