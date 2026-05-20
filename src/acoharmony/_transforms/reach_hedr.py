# © 2025 HarmonyCares
# All rights reserved.

"""
REACH HEDR (Health Equity Data Reporting) Eligibility Transform.

Calculates HEDR eligibility for REACH beneficiaries based on CMS requirements:

Numerator:
    Number of beneficiaries with at least 6 months of alignment to the REACH ACO
    during the PY as of October 1 for which the REACH ACO successfully reports all
    required data elements.

Denominator:
    Number of beneficiaries with at least 6 months of alignment to the REACH ACO
    accumulated BY October 1 of the PY, based on April 1 of PY+1 final
    eligibility runout file. Beneficiaries included regardless of when enrollment
    ended after October 1.

This transform uses BAR (Beneficiary Alignment Report) data and temporal alignment
data to determine eligibility accurately.
"""

from datetime import date, datetime

import polars as pl

from acoharmony._expressions._reach_hedr_eligible import (
    build_reach_hedr_denominator_expr,
    build_reach_hedr_numerator_expr,
)


def execute(executor) -> pl.LazyFrame:
    """
    Execute REACH HEDR eligibility analysis.

    This transform:
    1. Loads BAR data (most recent runout for current PY)
    2. Loads temporal alignment data (aco_alignment)
    3. Calculates months of alignment during performance year
    4. Determines denominator eligibility (≥6 months alignment BY Oct 1)
    5. Determines numerator eligibility (denominator + complete HEDR data)
    6. Returns analysis-ready LazyFrame with eligibility flags

    Args:
        executor: Transform executor with access to catalog and storage

    Returns:
        LazyFrame with HEDR eligibility analysis
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)
    gold_path = storage.get_path(MedallionLayer.GOLD)

    # Performance year and key dates
    # Hard-coded to 2025 per requirements
    performance_year = 2025
    october_first = date(2025, 10, 1)
    april_first_final = date(2026, 4, 1)

    executor.logger.info(f"Calculating HEDR eligibility for PY {performance_year}")
    executor.logger.info(f"Alignment date: {october_first}")
    executor.logger.info(f"Final eligibility check date: {april_first_final}")

    # Load BAR data (contains beneficiary demographics and alignment dates)
    bar = pl.scan_parquet(silver_path / "bar.parquet")

    # Get BAR files from the performance year (2025)
    # For HEDR, we need alignment status as of October 1, 2025
    bar_py = bar.filter(pl.col("file_date").str.starts_with(str(performance_year)))

    # Prioritize runout files (contain ".RP." in filename)
    # Extract date from filename pattern: P.D0259.ALGR25.RP.D{YYMMDD}.T{time}.xlsx
    bar_with_file_date = bar_py.with_columns(
        [
            pl.col("source_filename").str.extract(r"\.D(\d{6})\.", 1).alias("file_yymmdd"),
            pl.col("source_filename").str.contains(r"\.RP\.").alias("is_runout"),
        ]
    )

    # Get the most recent runout file if available, otherwise most recent regular BAR
    file_info = (
        bar_with_file_date.select(["source_filename", "file_date", "file_yymmdd", "is_runout"])
        .unique()
        .collect()
    )

    runout_files = file_info.filter(pl.col("is_runout"))

    if len(runout_files) > 0:
        # Use most recent runout file based on filename date
        most_recent_file = runout_files.sort("file_yymmdd", descending=True).row(0, named=True)
        most_recent_bar_date = most_recent_file["file_date"]
        executor.logger.info(f"Using runout BAR file: {most_recent_file['source_filename']}")
        bar_latest = bar_with_file_date.filter(
            pl.col("source_filename") == most_recent_file["source_filename"]
        )
    else:
        # Fall back to most recent regular BAR file
        bar_file_dates = file_info.select("file_date").unique().sort("file_date")
        most_recent_bar_date = bar_file_dates["file_date"].max()
        executor.logger.info(
            f"Using BAR file dated: {most_recent_bar_date} for PY {performance_year}"
        )
        bar_latest = bar_with_file_date.filter(pl.col("file_date") == most_recent_bar_date)

    # Drop helper columns
    bar_latest = bar_latest.drop(["file_yymmdd", "is_runout"])

    # Load SDOH assessment data if available (from Bronze template)
    sdoh_path = silver_path / "reach_sdoh.parquet"
    if sdoh_path.exists():
        sdoh = pl.scan_parquet(sdoh_path)
        has_sdoh = True
        executor.logger.info("Loaded REACH SDOH assessment data")
    else:
        executor.logger.warning(f"REACH SDOH data not available at {sdoh_path}")
        sdoh = None
        has_sdoh = False

    # Load temporal alignment data if available
    alignment_path = gold_path / "aco_alignment.parquet"
    if alignment_path.exists():
        alignment = pl.scan_parquet(alignment_path)
        has_alignment = True
        executor.logger.info("Loaded temporal alignment data")
    else:
        executor.logger.warning(f"Temporal alignment data not available at {alignment_path}")
        executor.logger.info("Will calculate months of alignment from BAR dates only")
        alignment = None
        has_alignment = False

    # Prepare BAR data with key fields
    bar_prepared = bar_latest.with_columns(
        [
            pl.col("bene_mbi").alias("mbi"),
            pl.col("start_date").alias("reach_start_date"),
            pl.col("end_date").alias("reach_end_date"),
            pl.col("bene_date_of_death").alias("death_date"),
            pl.col("bene_date_of_birth").alias("date_of_birth"),
            # Living beneficiary check
            pl.col("bene_date_of_death").is_null().alias("is_alive"),
        ]
    )

    if has_alignment:
        # Join with temporal alignment to get month-by-month tracking
        hedr_data = bar_prepared.join(
            alignment.select(
                [
                    "current_mbi",
                    "months_in_reach",
                    "first_reach_date",
                    "last_reach_date",
                    "observable_start",
                    "observable_end",
                ]
                + [
                    col
                    for col in alignment.collect_schema().names()
                    if col.startswith(f"ym_{performance_year}") and col.endswith("_reach")
                ]
            ),
            left_on="mbi",
            right_on="current_mbi",
            how="left",
        )

        # Count months in REACH for this specific performance year
        reach_month_cols = [
            col
            for col in alignment.collect_schema().names()
            if col.startswith(f"ym_{performance_year}") and col.endswith("_reach")
        ]

        if reach_month_cols:
            # Sum up the months where beneficiary was in REACH
            hedr_data = hedr_data.with_columns(
                [
                    sum(
                        pl.col(col).fill_null(False).cast(pl.Int8) for col in reach_month_cols
                    ).alias(f"reach_months_{performance_year}")
                ]
            )
        else:
            # Fallback: use months_in_reach if available
            hedr_data = hedr_data.with_columns(
                [pl.col("months_in_reach").alias(f"reach_months_{performance_year}")]
            )

    else:
        # Calculate months from BAR dates only
        # Need to calculate overlap between alignment period and PY 2025
        py_start = date(performance_year, 1, 1)
        py_end = date(performance_year, 12, 31)

        hedr_data = bar_prepared.with_columns(
            [
                # Calculate months of overlap between alignment period and performance year
                pl.when(pl.col("reach_start_date").is_not_null())
                .then(
                    # Determine effective end date: use end_date if present, otherwise use PY end
                    pl.when(pl.col("reach_end_date").is_not_null())
                    .then(
                        # Both start and end dates present
                        # Calculate overlap: max(start, py_start) to min(end, py_end)
                        (
                            pl.min_horizontal(pl.col("reach_end_date"), pl.lit(py_end)).dt.year()
                            * 12
                            + pl.min_horizontal(pl.col("reach_end_date"), pl.lit(py_end)).dt.month()
                            - pl.max_horizontal(
                                pl.col("reach_start_date"), pl.lit(py_start)
                            ).dt.year()
                            * 12
                            - pl.max_horizontal(
                                pl.col("reach_start_date"), pl.lit(py_start)
                            ).dt.month()
                            + 1
                        ).clip(0, 12)
                    )
                    .otherwise(
                        # No end date (still aligned) - calculate from start to end of PY
                        (
                            pl.lit(py_end).dt.year() * 12
                            + pl.lit(py_end).dt.month()
                            - pl.max_horizontal(
                                pl.col("reach_start_date"), pl.lit(py_start)
                            ).dt.year()
                            * 12
                            - pl.max_horizontal(
                                pl.col("reach_start_date"), pl.lit(py_start)
                            ).dt.month()
                            + 1
                        ).clip(0, 12)
                    )
                )
                .otherwise(0)
                .alias(f"reach_months_{performance_year}"),
                pl.col("reach_start_date").alias("first_reach_date"),
                pl.col("reach_end_date").alias("last_reach_date"),
            ]
        )

    # Calculate HEDR denominator using the expression builder
    # This ensures we use the correct logic: ≥6 months BY October 1
    # (not necessarily still enrolled ON October 1)
    denominator_expr = build_reach_hedr_denominator_expr(
        performance_year=performance_year,
        october_first_date=october_first,
        april_first_final_date=april_first_final,
        df_schema=hedr_data.collect_schema().names(),
    )

    hedr_data = hedr_data.with_columns([denominator_expr.alias("hedr_denominator")])

    # Join SDOH data if available
    if has_sdoh:
        hedr_data = hedr_data.join(
            sdoh.select(
                [
                    "mbi",
                    "date_assessment_complete",
                    "assessment_declined",
                ]
            ),
            left_on="mbi",
            right_on="mbi",
            how="left",
        )

    # Calculate HEDR numerator using the expression builder
    # Define required data columns for HEDR reporting
    required_hedr_columns = [
        "bene_first_name",
        "bene_last_name",
        "bene_address_line_1",
        "bene_city",
        "bene_state",
        "bene_zip_5",
        "bene_race_ethnicity",
    ]

    # Add SDOH assessment requirement if data is available
    if has_sdoh:
        required_hedr_columns.extend(["date_assessment_complete"])

    numerator_expr = build_reach_hedr_numerator_expr(
        performance_year=performance_year,
        october_first_date=october_first,
        april_first_final_date=april_first_final,
        required_data_columns=required_hedr_columns,
        df_schema=hedr_data.collect_schema().names(),
    )

    # For SDOH, also check that assessment was not declined
    if has_sdoh:
        numerator_expr = numerator_expr & (pl.col("assessment_declined") != "Yes")

    hedr_data = hedr_data.with_columns([numerator_expr.alias("hedr_numerator")])

    # Add calculated fields for analysis
    hedr_data = hedr_data.with_columns(
        [
            # Eligibility status
            pl.when(pl.col("hedr_numerator"))
            .then(pl.lit("Complete"))
            .when(pl.col("hedr_denominator"))
            .then(pl.lit("Incomplete"))
            .otherwise(pl.lit("Ineligible"))
            .alias("hedr_status"),
            # Missing data indicators
            pl.when(pl.col("hedr_denominator") & ~pl.col("hedr_numerator"))
            .then(
                pl.concat_str(
                    [
                        pl.when(
                            pl.col("bene_first_name").is_null()
                            | (pl.col("bene_first_name").str.strip_chars() == "")
                        )
                        .then(pl.lit("first_name,"))
                        .otherwise(pl.lit("")),
                        pl.when(
                            pl.col("bene_last_name").is_null()
                            | (pl.col("bene_last_name").str.strip_chars() == "")
                        )
                        .then(pl.lit("last_name,"))
                        .otherwise(pl.lit("")),
                        pl.when(
                            pl.col("bene_address_line_1").is_null()
                            | (pl.col("bene_address_line_1").str.strip_chars() == "")
                        )
                        .then(pl.lit("address,"))
                        .otherwise(pl.lit("")),
                        pl.when(
                            pl.col("bene_city").is_null()
                            | (pl.col("bene_city").str.strip_chars() == "")
                        )
                        .then(pl.lit("city,"))
                        .otherwise(pl.lit("")),
                        pl.when(
                            pl.col("bene_state").is_null()
                            | (pl.col("bene_state").str.strip_chars() == "")
                        )
                        .then(pl.lit("state,"))
                        .otherwise(pl.lit("")),
                        pl.when(
                            pl.col("bene_zip_5").is_null()
                            | (pl.col("bene_zip_5").str.strip_chars() == "")
                        )
                        .then(pl.lit("zip,"))
                        .otherwise(pl.lit("")),
                        pl.when(
                            pl.col("bene_race_ethnicity").is_null()
                            | (pl.col("bene_race_ethnicity").str.strip_chars() == "")
                        )
                        .then(pl.lit("race_ethnicity,"))
                        .otherwise(pl.lit("")),
                    ]
                    + (
                        [
                            pl.when(
                                pl.col("date_assessment_complete").is_null()
                                | (pl.col("assessment_declined") == "Yes")
                            )
                            .then(pl.lit("sdoh_assessment,"))
                            .otherwise(pl.lit(""))
                        ]
                        if has_sdoh
                        else []
                    )
                ).str.strip_chars_end(",")
            )
            .otherwise(None)
            .alias("missing_data_fields"),
            # Add metadata
            pl.lit(performance_year).alias("performance_year"),
            pl.lit(october_first).alias("alignment_date"),
            pl.lit(most_recent_bar_date).alias("bar_file_date"),
            pl.lit(datetime.now()).alias("processed_at"),
        ]
    )

    executor.logger.info("HEDR eligibility calculation complete")

    return hedr_data
