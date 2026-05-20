# © 2025 HarmonyCares
# All rights reserved.

"""
Outreach effectiveness statistics calculations for notebook.

Provides transforms for calculating voluntary alignment campaign effectiveness,
including conversion rates, engagement metrics, and office-level performance.
"""

import polars as pl

from .._expressions._enrollment_status import build_living_beneficiary_expr


def analyze_outreach_effectiveness(
    df_enriched: pl.LazyFrame, most_recent_ym: str | None = None, selected_ym: str | None = None
) -> dict[str, int | float | dict | None]:
    """
    Analyze outreach effectiveness for voluntary alignment campaigns.

    Calculates comprehensive conversion metrics showing the effectiveness
    of voluntary alignment outreach by tracking beneficiaries from contact
    through SVA signature to REACH enrollment.

    Args:
        df_enriched: LazyFrame with enriched alignment and outreach data
        most_recent_ym: Most recent year-month for REACH context (optional)
        selected_ym: Selected year-month for current metrics (optional)

    Returns:
        dict with outreach effectiveness metrics:
            - total_population: Total beneficiaries
            - total_contacted: Total contacted for voluntary alignment
            - total_emailed: Total who received emails
            - total_mailed: Total who received letters
            - total_with_valid_sva: Total with valid SVA
            - contacted_to_sva_rate: % of contacted who signed SVA
            - email_opened_to_sva_rate: % of email openers who signed SVA
            - email_clicked_to_sva_rate: % of email clickers who signed SVA
            - not_contacted_sva_rate: Baseline SVA rate (no outreach)
            - contacted_with_sva: Count contacted with SVA
            - opened_with_sva: Count who opened emails and have SVA
            - clicked_with_sva: Count who clicked emails and have SVA
            - not_contacted_with_sva: Count not contacted with SVA
            - contacted_sva_in_reach: Count contacted+SVA currently in REACH
            - current_metrics: Current month metrics dict (if selected_ym provided)
    """
    schema = df_enriched.collect_schema().names()

    # Calculate conversion metrics for ENTIRE POPULATION
    overall_stats = df_enriched.select(
        [
            pl.len().alias("total_population"),
            (pl.col("has_voluntary_outreach") if "has_voluntary_outreach" in schema else pl.lit(False))
            .sum()
            .alias("total_contacted"),
            ((pl.col("voluntary_email_count") > 0) if "voluntary_email_count" in schema else pl.lit(False))
            .sum()
            .alias("total_emailed"),
            ((pl.col("voluntary_letter_count") > 0) if "voluntary_letter_count" in schema else pl.lit(False))
            .sum()
            .alias("total_mailed"),
            ((pl.col("voluntary_emails_opened") > 0) if "voluntary_emails_opened" in schema else pl.lit(False))
            .sum()
            .alias("total_email_opened"),
            ((pl.col("voluntary_emails_clicked") > 0) if "voluntary_emails_clicked" in schema else pl.lit(False))
            .sum()
            .alias("total_email_clicked"),
            (pl.col("has_valid_voluntary_alignment") if "has_valid_voluntary_alignment" in schema else pl.lit(False))
            .sum()
            .alias("total_with_valid_sva"),
        ]
    ).collect()

    # Calculate TRUE conversion rates from outreach to SVA signature
    contacted_expr = (
        pl.col("has_voluntary_outreach") if "has_voluntary_outreach" in schema else pl.lit(False)
    )
    sva_expr = (
        pl.col("has_valid_voluntary_alignment") if "has_valid_voluntary_alignment" in schema else pl.lit(False)
    )

    contacted_to_sva = (
        df_enriched.filter(contacted_expr)
        .select(
            [
                pl.len().alias("contacted_count"),
                sva_expr.sum().alias("contacted_with_sva"),
            ]
        )
        .collect()
    )

    email_opened_to_sva = (
        df_enriched.filter(
            (pl.col("voluntary_emails_opened") > 0) if "voluntary_emails_opened" in schema else pl.lit(False)
        )
        .select(
            [
                pl.len().alias("opened_count"),
                sva_expr.sum().alias("opened_with_sva"),
            ]
        )
        .collect()
    )

    email_clicked_to_sva = (
        df_enriched.filter(
            (pl.col("voluntary_emails_clicked") > 0) if "voluntary_emails_clicked" in schema else pl.lit(False)
        )
        .select(
            [
                pl.len().alias("clicked_count"),
                sva_expr.sum().alias("clicked_with_sva"),
            ]
        )
        .collect()
    )

    not_contacted_sva = (
        df_enriched.filter(~contacted_expr)
        .select(
            [
                pl.len().alias("not_contacted_count"),
                sva_expr.sum().alias("not_contacted_with_sva"),
            ]
        )
        .collect()
    )

    # Check current REACH status for context
    contacted_sva_in_reach = 0
    if most_recent_ym:
        reach_col = f"ym_{most_recent_ym}_reach"
        if reach_col in schema:
            contacted_sva_to_reach = (
                df_enriched.filter(contacted_expr & sva_expr)
                .select(
                    [
                        pl.len().alias("contacted_sva_count"),
                        pl.col(reach_col).sum().alias("contacted_sva_in_reach"),
                    ]
                )
                .collect()
            )
            if len(contacted_sva_to_reach) > 0:
                contacted_sva_in_reach = int(contacted_sva_to_reach["contacted_sva_in_reach"][0])

    outreach_metrics = {
        "total_population": int(overall_stats["total_population"][0]),
        "total_contacted": int(overall_stats["total_contacted"][0]),
        "total_emailed": int(overall_stats["total_emailed"][0]),
        "total_mailed": int(overall_stats["total_mailed"][0]),
        "total_with_valid_sva": int(overall_stats["total_with_valid_sva"][0]),
        # TRUE conversion rates
        "contacted_to_sva_rate": (
            float(contacted_to_sva["contacted_with_sva"][0]) / contacted_to_sva["contacted_count"][0] * 100
        )
        if contacted_to_sva["contacted_count"][0] > 0
        else 0.0,
        "email_opened_to_sva_rate": (
            float(email_opened_to_sva["opened_with_sva"][0]) / email_opened_to_sva["opened_count"][0] * 100
        )
        if email_opened_to_sva["opened_count"][0] > 0
        else 0.0,
        "email_clicked_to_sva_rate": (
            float(email_clicked_to_sva["clicked_with_sva"][0])
            / email_clicked_to_sva["clicked_count"][0]
            * 100
        )
        if email_clicked_to_sva["clicked_count"][0] > 0
        else 0.0,
        "not_contacted_sva_rate": (
            float(not_contacted_sva["not_contacted_with_sva"][0])
            / not_contacted_sva["not_contacted_count"][0]
            * 100
        )
        if not_contacted_sva["not_contacted_count"][0] > 0
        else 0.0,
        # Raw numbers for transparency
        "contacted_with_sva": int(contacted_to_sva["contacted_with_sva"][0]),
        "opened_with_sva": int(email_opened_to_sva["opened_with_sva"][0])
        if len(email_opened_to_sva) > 0
        else 0,
        "clicked_with_sva": int(email_clicked_to_sva["clicked_with_sva"][0])
        if len(email_clicked_to_sva) > 0
        else 0,
        "not_contacted_with_sva": int(not_contacted_sva["not_contacted_with_sva"][0])
        if len(not_contacted_sva) > 0
        else 0,
        # REACH conversion (for context)
        "contacted_sva_in_reach": contacted_sva_in_reach,
        # Current metrics placeholder
        "current_metrics": None,
    }

    # Calculate CURRENT metrics if a month is selected
    if selected_ym:
        reach_col = f"ym_{selected_ym}_reach"
        mssp_col = f"ym_{selected_ym}_mssp"

        # Check if columns exist
        has_reach = reach_col in schema
        has_mssp = mssp_col in schema

        if has_reach or has_mssp:
            # Filter for actively aligned beneficiaries (alive only)
            living_expr = build_living_beneficiary_expr(schema)

            if has_reach and has_mssp:
                aligned_expr = pl.col(reach_col) | pl.col(mssp_col)
            elif has_reach:
                aligned_expr = pl.col(reach_col)
            else:
                aligned_expr = pl.col(mssp_col)

            currently_aligned_outreach = df_enriched.filter(aligned_expr & living_expr)

            current_outreach_stats = currently_aligned_outreach.select(
                [
                    pl.len().alias("current_population"),
                    contacted_expr.sum().alias("current_contacted"),
                    ((pl.col("voluntary_email_count") > 0) if "voluntary_email_count" in schema else pl.lit(False))
                    .sum()
                    .alias("current_emailed"),
                    ((pl.col("voluntary_letter_count") > 0) if "voluntary_letter_count" in schema else pl.lit(False))
                    .sum()
                    .alias("current_mailed"),
                    sva_expr.sum().alias("current_with_sva"),
                ]
            ).collect()

            curr_contacted_to_sva = (
                currently_aligned_outreach.filter(contacted_expr)
                .select(
                    [
                        pl.len().alias("contacted_count"),
                        sva_expr.sum().alias("contacted_with_sva"),
                    ]
                )
                .collect()
            )

            curr_email_opened = (
                currently_aligned_outreach.filter(
                    (pl.col("voluntary_emails_opened") > 0)
                    if "voluntary_emails_opened" in schema
                    else pl.lit(False)
                )
                .select(
                    [
                        pl.len().alias("opened_count"),
                        sva_expr.sum().alias("opened_with_sva"),
                    ]
                )
                .collect()
            )

            curr_email_clicked = (
                currently_aligned_outreach.filter(
                    (pl.col("voluntary_emails_clicked") > 0)
                    if "voluntary_emails_clicked" in schema
                    else pl.lit(False)
                )
                .select(
                    [
                        pl.len().alias("clicked_count"),
                        sva_expr.sum().alias("clicked_with_sva"),
                    ]
                )
                .collect()
            )

            curr_not_contacted = (
                currently_aligned_outreach.filter(~contacted_expr)
                .select(
                    [
                        pl.len().alias("not_contacted_count"),
                        sva_expr.sum().alias("not_contacted_with_sva"),
                    ]
                )
                .collect()
            )

            outreach_metrics["current_metrics"] = {
                "total": int(current_outreach_stats["current_population"][0]),
                "contacted": int(current_outreach_stats["current_contacted"][0]),
                "emailed": int(current_outreach_stats["current_emailed"][0]),
                "mailed": int(current_outreach_stats["current_mailed"][0]),
                "with_sva": int(current_outreach_stats["current_with_sva"][0]),
                "contacted_to_sva_rate": (
                    float(curr_contacted_to_sva["contacted_with_sva"][0])
                    / curr_contacted_to_sva["contacted_count"][0]
                    * 100
                )
                if curr_contacted_to_sva["contacted_count"][0] > 0
                else 0.0,
                "email_opened_to_sva_rate": (
                    float(curr_email_opened["opened_with_sva"][0]) / curr_email_opened["opened_count"][0] * 100
                )
                if curr_email_opened["opened_count"][0] > 0
                else 0.0,
                "email_clicked_to_sva_rate": (
                    float(curr_email_clicked["clicked_with_sva"][0])
                    / curr_email_clicked["clicked_count"][0]
                    * 100
                )
                if curr_email_clicked["clicked_count"][0] > 0
                else 0.0,
                "not_contacted_sva_rate": (
                    float(curr_not_contacted["not_contacted_with_sva"][0])
                    / curr_not_contacted["not_contacted_count"][0]
                    * 100
                )
                if curr_not_contacted["not_contacted_count"][0] > 0
                else 0.0,
                "contacted_with_sva": int(curr_contacted_to_sva["contacted_with_sva"][0])
                if len(curr_contacted_to_sva) > 0
                else 0,
                "opened_with_sva": int(curr_email_opened["opened_with_sva"][0])
                if len(curr_email_opened) > 0
                else 0,
                "clicked_with_sva": int(curr_email_clicked["clicked_with_sva"][0])
                if len(curr_email_clicked) > 0
                else 0,
                "not_contacted_with_sva": int(curr_not_contacted["not_contacted_with_sva"][0])
                if len(curr_not_contacted) > 0
                else 0,
            }

    return outreach_metrics


def calculate_quarterly_campaign_effectiveness(
    df_enriched: pl.LazyFrame, email_by_campaign: pl.LazyFrame, mailed_by_campaign: pl.LazyFrame
) -> pl.DataFrame:
    """
    Calculate quarterly campaign effectiveness analysis.

    Analyzes voluntary alignment campaign performance by quarter, including:
    - Outreach volume (emails, letters, both)
    - Engagement metrics (opens, clicks)
    - Conversion rates to valid SVA signature
    - Channel-specific effectiveness

    Args:
        df_enriched: LazyFrame with enriched alignment data
        email_by_campaign: LazyFrame with email campaigns by period and MBI
        mailed_by_campaign: LazyFrame with mailed campaigns by period and MBI

    Returns:
        DataFrame with campaign metrics by quarter including conversion rates
    """
    # Combine email and mailed campaign data
    campaign_outreach = email_by_campaign.join(
        mailed_by_campaign, on=["campaign_period", "mbi"], how="full", suffix="_mail"
    ).with_columns(
        [
            pl.col("emails_sent").fill_null(0),
            pl.col("letters_sent").fill_null(0),
            pl.col("opened").fill_null(False),
            pl.col("clicked").fill_null(False),
            (pl.col("emails_sent") > 0).alias("got_email"),
            (pl.col("letters_sent") > 0).alias("got_letter"),
        ]
    )

    # Join with alignment data to get SVA status
    campaign_effectiveness = campaign_outreach.join(
        df_enriched.select(["current_mbi", "has_valid_voluntary_alignment"]),
        left_on="mbi",
        right_on="current_mbi",
        how="left",
    ).with_columns(pl.col("has_valid_voluntary_alignment").fill_null(False))

    # Calculate effectiveness by campaign period
    campaign_metrics = (
        campaign_effectiveness.group_by("campaign_period")
        .agg(
            [
                # Outreach volume
                pl.len().alias("total_contacted"),
                pl.col("got_email").sum().alias("emailed"),
                pl.col("got_letter").sum().alias("mailed"),
                (pl.col("got_email") & pl.col("got_letter")).sum().alias("both_email_and_letter"),
                # Engagement
                pl.col("opened").sum().alias("emails_opened"),
                pl.col("clicked").sum().alias("emails_clicked"),
                # Conversion to SVA
                pl.col("has_valid_voluntary_alignment").sum().alias("signed_sva"),
                # Conversion rates
                (pl.col("has_valid_voluntary_alignment").sum() / pl.len() * 100).alias(
                    "overall_conversion_rate"
                ),
                # Email-specific conversions
                (
                    (pl.col("has_valid_voluntary_alignment") & pl.col("got_email")).sum()
                    / pl.col("got_email").sum()
                    * 100
                ).alias("email_to_sva_rate"),
                (
                    (pl.col("has_valid_voluntary_alignment") & pl.col("opened")).sum()
                    / pl.col("opened").sum()
                    * 100
                ).alias("opened_to_sva_rate"),
                (
                    (pl.col("has_valid_voluntary_alignment") & pl.col("clicked")).sum()
                    / pl.col("clicked").sum()
                    * 100
                ).alias("clicked_to_sva_rate"),
                # Letter-specific conversion
                (
                    (pl.col("has_valid_voluntary_alignment") & pl.col("got_letter")).sum()
                    / pl.col("got_letter").sum()
                    * 100
                ).alias("letter_to_sva_rate"),
            ]
        )
        .sort("campaign_period")
        .collect()
    )

    return campaign_metrics


def calculate_office_campaign_effectiveness(
    df_enriched: pl.LazyFrame, email_by_campaign: pl.LazyFrame, mailed_by_campaign: pl.LazyFrame
) -> pl.DataFrame:
    """
    Calculate campaign effectiveness by office_name.

    Analyzes voluntary alignment campaign performance by office, including:
    - Outreach volume per office (emails, letters)
    - Engagement metrics by office
    - Conversion rates to valid SVA by office
    - Office-specific campaign performance

    Args:
        df_enriched: LazyFrame with enriched alignment data (must include office_name, office_location)
        email_by_campaign: LazyFrame with email campaigns by period and MBI
        mailed_by_campaign: LazyFrame with mailed campaigns by period and MBI

    Returns:
        DataFrame with campaign metrics by office_name including conversion rates
    """
    schema = df_enriched.collect_schema().names()

    # Check if office columns exist
    if "office_name" not in schema:
        # Return empty DataFrame if office columns don't exist
        return pl.DataFrame()

    # Combine email and mailed campaign data
    campaign_outreach = email_by_campaign.join(
        mailed_by_campaign, on=["campaign_period", "mbi"], how="full", suffix="_mail"
    ).with_columns(
        [
            pl.col("emails_sent").fill_null(0),
            pl.col("letters_sent").fill_null(0),
            pl.col("opened").fill_null(False),
            pl.col("clicked").fill_null(False),
            (pl.col("emails_sent") > 0).alias("got_email"),
            (pl.col("letters_sent") > 0).alias("got_letter"),
        ]
    )

    # Select columns for join
    select_cols = ["current_mbi", "has_valid_voluntary_alignment", "office_name"]
    if "office_location" in schema:
        select_cols.append("office_location")

    # Join with alignment data to get SVA status AND office assignment
    campaign_with_office = campaign_outreach.join(
        df_enriched.select(select_cols),
        left_on="mbi",
        right_on="current_mbi",
        how="left",
    ).with_columns(pl.col("has_valid_voluntary_alignment").fill_null(False))

    # Group by columns
    group_cols = ["office_name"]
    if "office_location" in schema:
        group_cols.append("office_location")

    # Calculate effectiveness by office
    office_campaign_metrics = (
        campaign_with_office.filter(pl.col("office_name").is_not_null())
        .group_by(group_cols)
        .agg(
            [
                # Outreach volume
                pl.len().alias("total_contacted"),
                pl.col("got_email").sum().alias("emailed"),
                pl.col("got_letter").sum().alias("mailed"),
                (pl.col("got_email") & pl.col("got_letter")).sum().alias("both_email_and_letter"),
                # Engagement
                pl.col("opened").sum().alias("emails_opened"),
                pl.col("clicked").sum().alias("emails_clicked"),
                # Conversion to SVA
                pl.col("has_valid_voluntary_alignment").sum().alias("signed_sva"),
                # Conversion rates
                (pl.col("has_valid_voluntary_alignment").sum() / pl.len() * 100).alias(
                    "overall_conversion_rate"
                ),
                # Email-specific conversions
                (
                    (pl.col("has_valid_voluntary_alignment") & pl.col("got_email")).sum()
                    / pl.col("got_email").sum()
                    * 100
                ).alias("email_to_sva_rate"),
                (
                    (pl.col("has_valid_voluntary_alignment") & pl.col("opened")).sum()
                    / pl.col("opened").sum()
                    * 100
                ).alias("opened_to_sva_rate"),
                (
                    (pl.col("has_valid_voluntary_alignment") & pl.col("clicked")).sum()
                    / pl.col("clicked").sum()
                    * 100
                ).alias("clicked_to_sva_rate"),
                # Letter-specific conversion
                (
                    (pl.col("has_valid_voluntary_alignment") & pl.col("got_letter")).sum()
                    / pl.col("got_letter").sum()
                    * 100
                ).alias("letter_to_sva_rate"),
            ]
        )
        .sort("signed_sva", descending=True)
        .collect()
    )

    return office_campaign_metrics


def calculate_enhanced_campaign_performance(emails_df: pl.LazyFrame, mailed_df: pl.LazyFrame) -> dict[str, dict]:
    """
    Calculate comprehensive campaign performance metrics for voluntary alignment outreach.

    Calculates detailed performance metrics for both email and mail campaigns, including:
    - Delivery rates (excluding bounced, dropped, failed)
    - Email engagement rates (opens, clicks)
    - Unique recipient counts
    - Cost estimates

    Args:
        emails_df: LazyFrame with email campaign data
        mailed_df: LazyFrame with mail campaign data

    Returns:
        dict with campaign performance metrics:
            - email: Dict with email statistics (total_sent, delivered, opened, clicked,
                     unique_recipients, delivery_rate, open_rate, click_rate)
            - mail: Dict with mail statistics (total_sent, delivered, unique_recipients,
                    delivery_rate, estimated_cost)
    """
    email_schema = emails_df.collect_schema().names()
    mail_schema = mailed_df.collect_schema().names()

    # Calculate overall email campaign performance
    email_filter = (
        pl.col("campaign").str.contains("ACO Voluntary Alignment")
        if "campaign" in email_schema
        else pl.lit(False)
    )

    voluntary_email_stats = (
        emails_df.filter(email_filter)
        .select(
            [
                pl.len().alias("total_emails_sent"),
                # Delivered = NOT bounced/dropped/deferred (more accurate than checking == "Delivered")
                (
                    ~pl.col("status").is_in(["Bounced", "Dropped", "Deferred"])
                    if "status" in email_schema
                    else pl.lit(True)
                )
                .sum()
                .alias("emails_delivered"),
                (
                    (pl.col("has_been_opened") == "true") if "has_been_opened" in email_schema else pl.lit(False)
                )
                .sum()
                .alias("emails_opened"),
                (
                    (pl.col("has_been_clicked") == "true") if "has_been_clicked" in email_schema else pl.lit(False)
                )
                .sum()
                .alias("emails_clicked"),
                pl.col("mbi").n_unique().alias("unique_recipients") if "mbi" in email_schema else pl.lit(0),
            ]
        )
        .collect()
    )

    # Calculate overall mail campaign performance
    mail_filter = (
        pl.col("campaign_name").str.contains("ACO Voluntary Alignment")
        if "campaign_name" in mail_schema
        else pl.lit(False)
    )

    voluntary_mail_stats = (
        mailed_df.filter(mail_filter)
        .select(
            [
                pl.len().alias("total_letters_sent"),
                # Delivered = NOT failed/returned (more accurate)
                (
                    ~pl.col("status").is_in(["Failed", "Returned", "Cancelled"])
                    if "status" in mail_schema
                    else pl.lit(True)
                )
                .sum()
                .alias("letters_delivered"),
                pl.col("mbi").n_unique().alias("unique_recipients") if "mbi" in mail_schema else pl.lit(0),
            ]
        )
        .collect()
    )

    # Calculate email engagement rates
    if voluntary_email_stats.height > 0:
        total_emails = int(voluntary_email_stats["total_emails_sent"][0])
        delivered = int(voluntary_email_stats["emails_delivered"][0])
        opened = int(voluntary_email_stats["emails_opened"][0])
        clicked = int(voluntary_email_stats["emails_clicked"][0])
        unique_email = int(voluntary_email_stats["unique_recipients"][0])

        delivery_rate = (delivered / total_emails * 100) if total_emails > 0 else 0.0
        open_rate = (opened / delivered * 100) if delivered > 0 else 0.0
        click_rate = (clicked / opened * 100) if opened > 0 else 0.0
    else:
        total_emails = delivered = opened = clicked = unique_email = 0
        delivery_rate = open_rate = click_rate = 0.0

    # Get mail stats
    if voluntary_mail_stats.height > 0:
        total_letters = int(voluntary_mail_stats["total_letters_sent"][0])
        letters_delivered = int(voluntary_mail_stats["letters_delivered"][0])
        unique_mail = int(voluntary_mail_stats["unique_recipients"][0])
        mail_delivery_rate = (letters_delivered / total_letters * 100) if total_letters > 0 else 0.0
    else:
        total_letters = letters_delivered = unique_mail = 0
        mail_delivery_rate = 0.0

    return {
        "email": {
            "total_sent": total_emails,
            "delivered": delivered,
            "opened": opened,
            "clicked": clicked,
            "unique_recipients": unique_email,
            "delivery_rate": delivery_rate,
            "open_rate": open_rate,
            "click_rate": click_rate,
        },
        "mail": {
            "total_sent": total_letters,
            "delivered": letters_delivered,
            "unique_recipients": unique_mail,
            "delivery_rate": mail_delivery_rate,
            "estimated_cost": total_letters * 0.65,
        },
    }
