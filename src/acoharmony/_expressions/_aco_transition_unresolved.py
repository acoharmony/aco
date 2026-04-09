# © 2025 HarmonyCares
# All rights reserved.

"""
Year-over-year transition expression: Unresolved status.

Identifies beneficiaries who lost alignment but the reason is unclear
or unresolved - they don't fit into any other specific loss category.
"""

import polars as pl


def build_unresolved_expr(current_year: int, previous_year: int, current_year_months: list[int] | None = None) -> pl.Expr:
    """
    Calculate beneficiaries with unresolved status.

    Unresolved means:
    - Lost alignment from previous to current year
    - Does NOT match any other specific loss reason:
      * Not termed on BAR
      * Not expired SVA
      * Not lost provider
      * Not moved to MA
      * Not inactive (has some recent activity)
      * Not deceased

    This is a catch-all for unexplained losses that need investigation.

    Args:
        current_year: Current year to compare (e.g., 2026)
        previous_year: Previous year to compare against (e.g., 2025)
        current_year_months: List of months available in current year (e.g., [1, 2, 3]). If None, assumes all 12 months.

    Returns:
        pl.Expr: Boolean expression indicating if beneficiary has unresolved status
    """
    prev_year_prefix = f"ym_{previous_year}"
    curr_year_prefix = f"ym_{current_year}"

    # Determine which months to check for current year
    if current_year_months is None:
        current_year_months = list(range(1, 13))

    # Was in REACH in December of previous year
    was_reach_dec_prev = pl.col(f"{prev_year_prefix}12_reach").fill_null(False)

    # Not in REACH in current year
    not_reach_curr = ~pl.any_horizontal([
        pl.col(f"{curr_year_prefix}{month:02d}_reach").fill_null(False)
        for month in current_year_months
    ])

    # Exclude all other known loss reasons
    not_termed = ~pl.col(f"termed_bar_{previous_year}").fill_null(False)
    not_expired_sva = ~pl.col(f"expired_sva_{previous_year}").fill_null(False)
    not_lost_provider = ~pl.col(f"lost_provider_{previous_year}").fill_null(False)
    not_moved_ma = ~pl.col(f"moved_ma_{previous_year}").fill_null(False)
    not_inactive = ~pl.col(f"inactive_{previous_year}").fill_null(False)

    # Also not deceased
    not_deceased = (
        pl.col("death_date").is_null() |
        (pl.col("death_date").dt.year() > previous_year)
    )

    return (
        was_reach_dec_prev &
        not_reach_curr &
        not_termed &
        not_expired_sva &
        not_lost_provider &
        not_moved_ma &
        not_inactive &
        not_deceased
    ).alias(f"unresolved_{previous_year}")


def build_potential_unresolved_reasons_expr() -> pl.Expr:
    """
    Generate potential reasons for unresolved status based on available data.

    Returns:
        pl.Expr: String expression with potential reasons
    """
    # Check for various data quality or edge case indicators using fields that exist
    return (
        pl.when(pl.col("latest_response_codes").is_null())
        .then(pl.lit("Missing response codes"))
        .when(pl.col("sva_provider_valid").is_null())
        .then(pl.lit("Provider validation unknown"))
        .when(pl.col("has_voluntary_alignment").is_null())
        .then(pl.lit("Voluntary alignment status unknown"))
        .otherwise(pl.lit("Requires investigation"))
    ).alias("unresolved_reason")


def get_unresolved_losses(
    alignment_df: pl.LazyFrame,
    previous_year: int,
    current_year: int
) -> pl.LazyFrame:
    """
    Get beneficiaries with unresolved loss status for detailed investigation.

    Returns beneficiaries who lost alignment but don't have a clear reason,
    along with all available context for manual review.

    Args:
        alignment_df: Main alignment dataframe with all transition flags
        previous_year: Previous year
        current_year: Current year

    Returns:
        pl.LazyFrame: DataFrame with current_mbi and investigation details
    """
    unresolved = (
        alignment_df
        .filter(pl.col(f"unresolved_{previous_year}"))
        .select([
            pl.col("current_mbi"),
            pl.col("bene_first_name"),
            pl.col("bene_last_name"),
            pl.col("office_location"),
            pl.col("last_reach_date"),
            pl.col("last_mssp_date"),
            pl.col("last_outreach_date"),
            pl.col("has_voluntary_alignment"),
            pl.col("provider_valid"),
            pl.col("latest_response_codes"),
            pl.col("death_date"),
            build_potential_unresolved_reasons_expr(),
            pl.lit("Needs investigation").alias("action_required")
        ])
        .unique(subset=["current_mbi"])
    )

    return unresolved


def build_transition_category_expr(previous_year: int, current_year: int) -> pl.Expr:
    """
    Categorize each beneficiary's transition reason into a single primary category.

    Uses a priority order to assign one category when multiple may apply:
    1. Deceased (highest priority)
    2. Moved to MA
    3. Termed on BAR
    4. Expired SVA
    5. Lost Provider
    6. Inactive
    7. Unresolved (catch-all)
    8. Lost (Other) - lost but no specific reason
    9. Retained (still aligned)

    Args:
        previous_year: Previous year for column naming
        current_year: Current year for column naming

    Returns:
        pl.Expr: String expression with transition category
    """
    return (
        pl.when(pl.col("death_date").is_not_null() & (pl.col("death_date").dt.year() == previous_year))
        .then(pl.lit("Deceased"))
        .when(pl.col(f"moved_ma_{previous_year}").fill_null(False))
        .then(pl.lit("Moved to MA"))
        .when(pl.col(f"termed_bar_{previous_year}").fill_null(False))
        .then(pl.lit("Termed on BAR"))
        .when(pl.col(f"expired_sva_{previous_year}").fill_null(False))
        .then(pl.lit("Expired SVA"))
        .when(pl.col(f"lost_provider_{previous_year}").fill_null(False))
        .then(pl.lit("Lost Provider"))
        .when(pl.col(f"inactive_{previous_year}").fill_null(False))
        .then(pl.lit("Inactive"))
        .when(pl.col(f"unresolved_{previous_year}").fill_null(False))
        .then(pl.lit("Unresolved"))
        .when(pl.col(f"lost_{previous_year}_to_{current_year}").fill_null(False))
        .then(pl.lit("Lost (Other)"))
        .otherwise(pl.lit("Retained"))
    ).alias(f"transition_category_{previous_year}")
