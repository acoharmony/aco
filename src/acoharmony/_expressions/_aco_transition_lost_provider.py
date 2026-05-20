# © 2025 HarmonyCares
# All rights reserved.

"""
Year-over-year transition expression: Lost provider.

Identifies beneficiaries who lost alignment because their provider is
no longer on the PVAR (voluntary alignment) or participant list (ACO roster).
"""


import polars as pl


def build_lost_provider_expr(current_year: int, previous_year: int, current_year_months: list[int] | None = None) -> pl.Expr:
    """
    Calculate beneficiaries lost due to provider no longer being on roster.

    A beneficiary loses alignment when:
    - Their provider (NPI/TIN) from previous year is not on current year roster
    - They were voluntarily aligned (SVA/PBVAR) or claims-based attributed
    - Provider left the ACO or REACH DCE

    Logic checks:
    - Had alignment in previous year
    - Provider NPI/TIN from previous year attribution
    - No alignment in current year
    - Provider validation flag indicates provider not found

    Args:
        current_year: Current year to compare (e.g., 2026)
        previous_year: Previous year to compare against (e.g., 2025)
        current_year_months: List of months available in current year (e.g., [1, 2, 3]). If None, assumes all 12 months.

    Returns:
        pl.Expr: Boolean expression indicating if beneficiary lost alignment due to provider loss
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

    # Lost provider means their PBVAR/SVA provider is no longer on the participant list
    # sva_provider_valid comes from validating voluntary_provider_npi/tin against current participant list
    # When sva_provider_valid = False, their provider left the ACO/REACH DCE
    provider_no_longer_valid = ~pl.col("sva_provider_valid").fill_null(True)

    # They must have had voluntary alignment for this to apply
    had_voluntary = pl.col("has_voluntary_alignment").fill_null(False)

    return (
        was_reach_dec_prev &
        not_reach_curr &
        had_voluntary &
        provider_no_longer_valid
    ).alias(f"lost_provider_{previous_year}")


def get_lost_providers(
    alignment_df: pl.LazyFrame,
    participant_list: pl.LazyFrame,
    pvar_df: pl.LazyFrame,
    previous_year: int,
    current_year: int
) -> pl.LazyFrame:
    """
    Identify beneficiaries who lost alignment due to provider roster changes.

    Cross-references previous year providers with current year rosters
    to find providers who left the ACO/DCE.

    Args:
        alignment_df: Main alignment dataframe with provider attributions
        participant_list: Current participant list (TIN/NPI roster)
        pvar_df: Provider Voluntary Alignment Roster
        previous_year: Previous year
        current_year: Current year

    Returns:
        pl.LazyFrame: DataFrame with current_mbi and lost provider details
    """
    # Get beneficiaries with provider from previous year
    prev_attributed = (
        alignment_df
        .filter(
            (pl.col("alignment_year") == previous_year) &
            (pl.col("voluntary_provider_npi").is_not_null() | pl.col("voluntary_provider_tin").is_not_null())
        )
        .select([
            pl.col("current_mbi"),
            pl.col("voluntary_provider_npi").alias("prev_year_npi"),
            pl.col("voluntary_provider_tin").alias("prev_year_tin"),
            pl.col("voluntary_provider_name").alias("prev_year_provider_name")
        ])
        .unique(subset=["current_mbi"])
    )

    # Get current year participant list (valid providers)
    current_participants = (
        participant_list
        .filter(pl.col("performance_year") == current_year)
        .select([
            pl.col("npi").alias("current_npi"),
            pl.col("tin").alias("current_tin")
        ])
        .unique()
    )

    # Anti-join: find prev year providers NOT in current year roster
    lost_providers = (
        prev_attributed
        .join(
            current_participants,
            left_on="prev_year_npi",
            right_on="current_npi",
            how="anti"  # Keep only rows from left that don't match right
        )
        .select([
            pl.col("current_mbi"),
            pl.col("prev_year_npi"),
            pl.col("prev_year_tin"),
            pl.col("prev_year_provider_name"),
            pl.lit("Provider no longer on roster").alias("loss_reason"),
            pl.lit(True).alias(f"lost_provider_{previous_year}")
        ])
    )

    return lost_providers
