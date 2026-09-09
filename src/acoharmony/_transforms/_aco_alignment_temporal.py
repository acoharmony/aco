# © 2025 HarmonyCares
# All rights reserved.

"""
Temporal matrix transform for ACO alignment pipeline.

Builds point-in-time year-month boolean columns tracking beneficiary enrollment
status across REACH, MSSP, and FFS programs. This is the foundation stage that
creates the temporal tracking matrix used by all subsequent alignment stages.

The temporal matrix uses only data available AS OF each month to prevent data
leakage, making it suitable for historical analysis and forecasting.
"""

from datetime import date, datetime
from typing import Any

import polars as pl
from dateutil.relativedelta import relativedelta

from .._decor8 import transform, transform_method
from .._expressions._aco_temporal_alr import (
    build_alr_preparation_exprs,
    build_alr_select_expr,
)
from .._expressions._aco_temporal_bar import (
    build_bar_preparation_exprs,
    build_bar_select_expr,
)
from .._expressions._aco_temporal_demographics import (
    build_demographics_mbi_expr,
    build_demographics_select_expr,
)
from .._expressions._aco_temporal_ffs import build_ffs_mbi_crosswalk_expr, build_ffs_select_expr
from .._expressions._aco_temporal_summary import build_summary_statistics_exprs


@transform(name="aco_alignment_temporal", tier=["gold"], sql_enabled=False)
@transform_method(enable_composition=False, threshold=10.0)
def apply_transform(
    df: pl.LazyFrame | None, schema: dict, catalog: Any, logger: Any, force: bool = False
) -> pl.LazyFrame:
    """
    Build temporal matrix with point-in-time enrollment tracking.

    This transform is the foundation of ACO alignment tracking. It creates
    year-month boolean columns (ym_YYYYMM_reach, ym_YYYYMM_mssp, ym_YYYYMM_ffs)
    for each month in the observable window.

    Key characteristics:
    - Point-in-time tracking (no data leakage)
    - REACH takes precedence over MSSP (mutually exclusive)
    - FFS status requires first claim date AND no ACO enrollment
    - Idempotent via catalog check

    Args:
        df: Not used (None) - this transform builds from catalog sources directly
        schema: Schema config
        catalog: Catalog for accessing source data
        logger: Logger instance
        force: Force rebuild of temporal matrix

    Returns:
        pl.LazyFrame: Temporal matrix with year-month columns and demographics

    Raises:
        ValueError: If required sources are missing
    """
    # Note: df parameter is None for this foundation transform

    # Collect required sources first (needed for both check and build)
    sources = _collect_required_sources(catalog, logger)
    observable_start, observable_end = _determine_observable_range(sources, logger)

    # Check if temporal matrix needs rebuild by comparing source dates and modification times
    if not force and catalog.get_table_metadata("aco_alignment") is not None:
        try:
            existing_matrix = catalog.scan_table("aco_alignment")
            matrix_data = (
                existing_matrix.select(["observable_end", "processed_at"]).first().collect()
            )
            existing_end_raw = matrix_data["observable_end"].item()
            matrix_processed_at = matrix_data["processed_at"].item()

            # Convert existing_end to date for comparison
            if isinstance(existing_end_raw, str):
                existing_end = datetime.strptime(existing_end_raw[:10], "%Y-%m-%d").date()
            elif isinstance(existing_end_raw, datetime):
                existing_end = existing_end_raw.date()
            else:
                existing_end = existing_end_raw

            # Check 1: observable date range changed
            if observable_end > existing_end:
                logger.info(
                    f"Source data updated ({existing_end} → {observable_end}), rebuilding temporal matrix"
                )
            else:
                # Convert to datetime for comparison
                def to_datetime(val):
                    if val is None:
                        return datetime.min
                    if isinstance(val, str):
                        return datetime.fromisoformat(val.replace("Z", "+00:00"))
                    return val

                def source_freshness(source: pl.LazyFrame | None) -> datetime:
                    if source is None:
                        return datetime.min
                    source_schema = source.collect_schema().names()
                    freshness_col = next(
                        (
                            col
                            for col in (
                                "processed_at",
                                "extracted_at",
                                "lineage_processed_at",
                            )
                            if col in source_schema
                        ),
                        None,
                    )
                    if freshness_col is None:
                        return datetime.min
                    value = source.select(pl.col(freshness_col).max()).collect().item()
                    return to_datetime(value)

                source_max_processed = max(
                    source_freshness(sources.get(name))
                    for name in (
                        "bar",
                        "alr",
                        "ffs_first_dates",
                        "last_ffs_service",
                    )
                )
                matrix_processed_dt = to_datetime(matrix_processed_at)

                if source_max_processed > matrix_processed_dt:
                    logger.info(
                        f"Source data modified after matrix build ({matrix_processed_at} < {source_max_processed}), rebuilding"
                    )
                else:
                    logger.info(
                        f"Temporal matrix is current (observable_end: {existing_end}), loading from catalog"
                    )
                    return existing_matrix
        except Exception as e:
            # Table metadata exists but file not found or other error, rebuild
            logger.info(f"Temporal matrix metadata found but needs rebuild: {e}")

    logger.info("Building temporal matrix from source data")
    logger.info(f"Observable range: {observable_start} to {observable_end}")

    # STEP 2: Build MBI crosswalk map
    mbi_map = _build_mbi_map(sources["enterprise_crosswalk"], logger)
    logger.info(f"Built MBI crosswalk with {len(mbi_map)} mappings")

    # STEP 3: Prepare source data using expression builders
    bar_data = _prepare_bar_data(sources["bar"], mbi_map, logger)
    alr_data = _prepare_alr_data(sources["alr"], mbi_map, logger)
    ffs_data = _prepare_ffs_data(
        sources["ffs_first_dates"], sources.get("last_ffs_service"), mbi_map, logger
    )
    demographics = _prepare_demographics(sources["beneficiary_demographics"], mbi_map, logger)

    # STEP 4: Build temporal matrix (stateful loop logic)
    # Pass demographics to get death dates for filtering
    alignment_matrix = _build_temporal_matrix_vectorized(
        bar_data, alr_data, observable_start, observable_end, ffs_data, demographics, logger
    )

    # Note: Demographics death dates were used inside _build_temporal_matrix_vectorized
    # but we still need to join full demographics for other columns

    # STEP 5: Join full demographics for all demographic columns (not just death_date)
    alignment_matrix = alignment_matrix.join(demographics, on="current_mbi", how="left")

    # STEP 6: Calculate summary statistics using expression builders
    alignment_matrix = alignment_matrix.with_columns(build_summary_statistics_exprs())

    # STEP 7: Add metadata
    alignment_matrix = alignment_matrix.with_columns(
        [
            pl.lit(observable_start).alias("observable_start"),
            pl.lit(observable_end).alias("observable_end"),
            pl.lit(datetime.now()).alias("processed_at"),
        ]
    )

    # STEP 8: Deduplicate
    alignment_matrix = alignment_matrix.unique(subset=["current_mbi"], keep="last")

    logger.info("Temporal matrix build complete")

    # Save to silver for caching
    from ..config import get_config

    config = get_config()
    silver_path = config.storage.base_path / config.storage.silver_dir
    output_path = silver_path / "aco_alignment.parquet"

    alignment_matrix.sink_parquet(
        str(output_path),
        compression=config.transform.compression,
        row_group_size=config.transform.row_group_size,
    )
    logger.info(f"Saved temporal matrix to {output_path}")

    # Return as LazyFrame for pipeline
    return pl.scan_parquet(output_path)


def _collect_required_sources(catalog: Any, logger: Any) -> dict[str, pl.LazyFrame]:
    """
    Collect all required source data from catalog.

    Args:
        catalog: Catalog instance
        logger: Logger instance

    Returns:
        dict[str, pl.LazyFrame]: Dictionary of source LazyFrames

    Raises:
        ValueError: If required sources are missing
    """
    from ..config import get_config

    required_sources_from_catalog = [
        "bar",
        "alr",
        "ffs_first_dates",
        "beneficiary_demographics",
    ]

    sources = {}
    for source_name in required_sources_from_catalog:
        source = catalog.scan_table(source_name)
        if source is None:
            raise ValueError(f"{source_name} source not found - required for temporal matrix")
        sources[source_name] = source

    # Load MBI crosswalk from identity_timeline (replaces legacy enterprise_crosswalk).
    # Dict key is kept as "enterprise_crosswalk" for internal stability; consumers
    # only read (prvs_num, crnt_num) which the helper provides.
    from ._identity_timeline import current_mbi_lookup_lazy

    config = get_config()
    silver_path = config.storage.base_path / config.storage.silver_dir
    timeline_path = silver_path / "identity_timeline.parquet"

    if not timeline_path.exists():
        raise ValueError(
            "identity_timeline not found - required for temporal matrix. Run 'aco pipeline identity_timeline' first."
        )

    try:
        sources["last_ffs_service"] = catalog.scan_table("last_ffs_service")
    except Exception as exc:
        logger.warning(
            "last_ffs_service source not found; FFS month flags will fall back to "
            f"ffs_first_dates and may undercount recent returning beneficiaries: {exc}"
        )

    sources["enterprise_crosswalk"] = current_mbi_lookup_lazy(silver_path)

    logger.info(f"Collected {len(sources)} required sources (MBI crosswalk from identity_timeline)")
    return sources


def _determine_observable_range(sources: dict[str, pl.LazyFrame], logger: Any) -> tuple[date, date]:
    """
    Determine the observable date range from BAR and ALR files.

    Scans all source file dates to establish the temporal window. This ensures
    point-in-time columns only reflect information available at each month.

    Args:
        sources: Dictionary of source LazyFrames
        logger: Logger instance

    Returns:
        tuple[date, date]: (earliest_date, latest_date) from sources
    """
    dates = []

    for source_name in ["bar", "alr"]:
        file_dates = sources[source_name].select("file_date").unique().collect()
        for d in file_dates["file_date"].to_list():
            dates.append(datetime.strptime(d[:10], "%Y-%m-%d").date())

    return min(dates), max(dates)


def _build_mbi_map(crosswalk_df: pl.LazyFrame, logger: Any) -> dict[str, str]:
    """
    Build MBI crosswalk lookup dictionary.

    Creates a mapping from historical MBIs to current MBIs for beneficiary
    tracking across MBI changes.

    Args:
        crosswalk_df: Enterprise crosswalk LazyFrame
        logger: Logger instance

    Returns:
        dict[str, str]: Dictionary mapping previous_mbi -> current_mbi
    """
    mbi_map = {}
    xwalk = crosswalk_df.select(["prvs_num", "crnt_num"]).collect()
    for row in xwalk.iter_rows():
        if row[0] and row[1] and row[0] != row[1]:
            mbi_map[row[0]] = row[1]
    return mbi_map


def _prepare_bar_data(bar_df: pl.LazyFrame, mbi_map: dict, logger: Any) -> pl.LazyFrame:
    """
    Prepare BAR (Beneficiary Alignment Report) data using expression builders.

    Args:
        bar_df: Raw BAR data LazyFrame
        mbi_map: MBI crosswalk dictionary
        logger: Logger instance

    Returns:
        pl.LazyFrame: Prepared BAR data
    """
    return bar_df.with_columns(build_bar_preparation_exprs(mbi_map)).select(build_bar_select_expr())


def _prepare_alr_data(alr_df: pl.LazyFrame, mbi_map: dict, logger: Any) -> pl.LazyFrame:
    """
    Prepare ALR (Alignment Report) data using expression builders.

    Args:
        alr_df: Raw ALR data LazyFrame
        mbi_map: MBI crosswalk dictionary
        logger: Logger instance

    Returns:
        pl.LazyFrame: Prepared ALR data
    """
    return alr_df.with_columns(build_alr_preparation_exprs(mbi_map)).select(build_alr_select_expr())


def _prepare_ffs_data(
    ffs_first_df: pl.LazyFrame,
    ffs_last_df: pl.LazyFrame | None,
    mbi_map: dict,
    logger: Any,
) -> pl.LazyFrame:
    """
    Prepare FFS service-window data using expression builders.

    Args:
        ffs_first_df: Raw FFS first dates LazyFrame
        ffs_last_df: Raw last FFS service LazyFrame, when available
        mbi_map: MBI crosswalk dictionary
        logger: Logger instance

    Returns:
        pl.LazyFrame: Prepared FFS data with first and last practice service dates
    """
    first_prepared = ffs_first_df.with_columns([build_ffs_mbi_crosswalk_expr(mbi_map)]).select(
        build_ffs_select_expr()
    )
    if ffs_last_df is None:
        logger.warning(
            "last_ffs_service unavailable; using ffs_first_date as last_ffs_date for "
            "24-month FFS eligibility"
        )
        return first_prepared.with_columns(
            [
                pl.col("ffs_first_date").alias("last_ffs_date"),
                pl.lit(None).cast(pl.String).alias("last_ffs_tin"),
                pl.lit(None).cast(pl.String).alias("last_ffs_npi"),
            ]
        )

    last_prepared = ffs_last_df.with_columns([build_ffs_mbi_crosswalk_expr(mbi_map)]).select(
        [
            pl.col("current_mbi"),
            pl.col("last_ffs_date"),
            pl.col("last_ffs_tin"),
            pl.col("last_ffs_npi"),
            pl.col("claim_count").alias("last_ffs_claim_count"),
        ]
    )
    return (
        last_prepared.join(first_prepared, on="current_mbi", how="left")
        .with_columns(
            [
                pl.col("has_ffs_service").fill_null(True),
                pl.col("ffs_claim_count").fill_null(pl.col("last_ffs_claim_count")),
            ]
        )
        .drop("last_ffs_claim_count")
    )


def _prepare_demographics(demo_df: pl.LazyFrame, mbi_map: dict, logger: Any) -> pl.LazyFrame:
    """
    Prepare demographics data using expression builders.

    Args:
        demo_df: Raw demographics LazyFrame
        mbi_map: MBI crosswalk dictionary (not used but kept for consistency)
        logger: Logger instance

    Returns:
        pl.LazyFrame: Prepared demographics data
    """
    return demo_df.with_columns([build_demographics_mbi_expr()]).select(
        build_demographics_select_expr()
    )


def _calculate_first_program_date(year_months: list[str], program_suffix: str) -> pl.Expr:
    """
    Calculate the first date a beneficiary had a specific program status.

    Iterates through year_months to find the first TRUE value, converts YYYYMM to date.

    Args:
        year_months: List of year-month strings (YYYYMM format)
        program_suffix: Program suffix ('reach' or 'mssp')

    Returns:
        pl.Expr: Expression that returns the first date or None
    """
    expr = pl.lit(None).cast(pl.Date)

    # Iterate in reverse to build nested when-then chain (last month has priority for "first")
    for ym in reversed(year_months):
        year = int(ym[:4])
        month = int(ym[4:6])
        month_date = date(year, month, 1)

        expr = pl.when(pl.col(f"ym_{ym}_{program_suffix}")).then(pl.lit(month_date)).otherwise(expr)

    return expr


def _calculate_last_program_date(year_months: list[str], program_suffix: str) -> pl.Expr:
    """
    Calculate the last date a beneficiary had a specific program status.

    Iterates through year_months to find the last TRUE value, converts YYYYMM to date.

    Args:
        year_months: List of year-month strings (YYYYMM format)
        program_suffix: Program suffix ('reach' or 'mssp')

    Returns:
        pl.Expr: Expression that returns the last date or None
    """
    expr = pl.lit(None).cast(pl.Date)

    # Iterate forward to build nested when-then chain (last month overwrites earlier ones)
    for ym in year_months:
        year = int(ym[:4])
        month = int(ym[4:6])
        month_date = date(year, month, 1)

        expr = pl.when(pl.col(f"ym_{ym}_{program_suffix}")).then(pl.lit(month_date)).otherwise(expr)

    return expr


def _build_temporal_matrix_vectorized(
    bar_data: pl.LazyFrame,
    alr_data: pl.LazyFrame,
    start_date: date,
    end_date: date,
    ffs_data: pl.LazyFrame | None,
    demographics: pl.LazyFrame,
    logger: Any,
) -> pl.LazyFrame:
    """
    Build point-in-time temporal matrix with year-month columns.

    This is the core stateful logic that creates boolean columns for each
    month/program combination. The loop iterates through months and builds
    columns based only on data available AS OF each month.

    Key logic:
    - REACH (BAR) and MSSP (ALR) are mutually exclusive
    - REACH takes precedence over MSSP
    - FFS status requires first claim date AND no ACO enrollment
    - Uses vectorized Polars operations within the loop
    - Filters by death date from demographics (beneficiaries who died before a month are excluded)

    Args:
        bar_data: Prepared BAR data
        alr_data: Prepared ALR data
        start_date: Observable start date
        end_date: Observable end date
        ffs_data: Optional prepared FFS data
        demographics: Prepared demographics data with death dates
        logger: Logger instance

    Returns:
        pl.LazyFrame: Temporal matrix with year-month boolean columns
    """
    # Combine BAR and ALR data
    combined = pl.concat([bar_data.collect(), alr_data.collect()], how="vertical")

    # Join demographics to get death dates for ALL beneficiaries (BAR and ALR)
    # Use left join to preserve all alignment records
    demographics_collected = demographics.collect()
    demographics_deaths = demographics_collected.select(
        [pl.col("current_mbi"), pl.col("death_date").alias("death_date_from_demo")]
    )
    combined = combined.join(demographics_deaths, on="current_mbi", how="left")

    # Create unified death_date column: prefer BAR's bene_date_of_death, fallback to demographics
    combined = combined.with_columns(
        pl.coalesce([pl.col("bene_date_of_death"), pl.col("death_date_from_demo")]).alias(
            "unified_death_date"
        )
    )

    combined = combined.with_columns(
        [pl.col("file_date_parsed").dt.strftime("%Y%m").alias("file_year_month")]
    )

    # Sort and deduplicate - keep most recent per MBI/date
    combined = combined.sort(["current_mbi", "file_date_parsed", "program"]).unique(
        subset=["current_mbi", "file_date_parsed"], keep="last"
    )

    # Get all unique MBIs
    all_mbis = combined.select("current_mbi").unique()

    # Build FFS lookup dictionaries. FFS is a current practice-touch universe:
    # living beneficiaries with a practice-TIN claim inside the trailing
    # 24-month window, excluding anyone aligned to REACH/MSSP for the month.
    ffs_dict = {}
    ffs_last_dict = {}
    if ffs_data is not None:
        ffs_collected = ffs_data.collect()
        for row in ffs_collected.iter_rows(named=True):
            if row["ffs_first_date"]:
                ffs_dict[row["current_mbi"]] = row["ffs_first_date"]
            if "last_ffs_date" in ffs_collected.columns and row["last_ffs_date"]:
                ffs_last_dict[row["current_mbi"]] = row["last_ffs_date"]

        ffs_mbis = ffs_collected.select("current_mbi").unique()
        all_mbis = pl.concat([all_mbis, ffs_mbis]).unique()

    death_dict = {}
    for row in demographics_collected.select(["current_mbi", "death_date"]).iter_rows(named=True):
        death_dict[row["current_mbi"]] = row["death_date"]

    combined = combined.sort(["current_mbi", "file_date_parsed"])

    # Generate list of year-months
    year_months = []
    current_date = start_date.replace(day=1)
    end_month = end_date.replace(day=1)

    while current_date <= end_month:
        year_months.append(current_date.strftime("%Y%m"))
        current_date += relativedelta(months=1)

    if end_date.day > 1 and end_date.strftime("%Y%m") not in year_months:
        year_months.append(end_date.strftime("%Y%m"))

    logger.info(f"Building temporal matrix for {len(year_months)} months")

    # Start with all MBIs
    result = all_mbis

    # STATEFUL LOOP: Build columns for each month
    for ym in year_months:
        year = int(ym[:4])
        month = int(ym[4:6])
        if month == 12:
            next_year = year + 1
            next_month = 1
        else:
            next_year = year
            next_month = month + 1
        month_end = date(next_year, next_month, 1) - relativedelta(days=1)
        ffs_lookback_start = month_end - relativedelta(months=24)

        # Filter to data available AS OF this month (point-in-time)
        data_as_of_month = combined.filter(pl.col("file_date_parsed") <= month_end)

        # Calculate month start date for death date filtering
        month_date = date(year, month, 1)

        if data_as_of_month.height == 0:
            # No data yet - all false
            result = result.with_columns(
                [
                    pl.lit(False).alias(f"ym_{ym}_reach"),
                    pl.lit(False).alias(f"ym_{ym}_mssp"),
                    pl.lit(False).alias(f"ym_{ym}_ffs"),
                    pl.lit(False).alias(f"ym_{ym}_first_claim"),
                ]
            )
            continue

        # Get most recent REACH status from BAR files
        reach_data = data_as_of_month.filter(pl.col("program") == "REACH")
        reach_mbis = set()
        if reach_data.height > 0:
            max_reach_date = reach_data.select(pl.col("file_date_parsed").max()).item()
            reach_status = (
                reach_data.filter(pl.col("file_date_parsed") == max_reach_date)
                # CRITICAL: Exclude beneficiaries who died before this month started
                .filter(
                    pl.col("unified_death_date").is_null()
                    | (pl.col("unified_death_date") >= month_date)
                )
                .select(["current_mbi"])
                .unique()
            )
            reach_mbis = set(reach_status["current_mbi"].to_list())
            reach_status = reach_status.with_columns(pl.lit(True).alias(f"ym_{ym}_reach"))
        else:
            reach_status = pl.DataFrame(
                {
                    "current_mbi": pl.Series([], dtype=pl.String),
                    f"ym_{ym}_reach": pl.Series([], dtype=pl.Boolean),
                }
            )

        # Get most recent MSSP status from ALR files (exclude REACH MBIs)
        mssp_data = data_as_of_month.filter(pl.col("program") == "MSSP")
        if mssp_data.height > 0:
            max_mssp_date = mssp_data.select(pl.col("file_date_parsed").max()).item()
            mssp_candidates = (
                mssp_data.filter(pl.col("file_date_parsed") == max_mssp_date)
                # CRITICAL: Exclude beneficiaries who died before this month started
                .filter(
                    pl.col("unified_death_date").is_null()
                    | (pl.col("unified_death_date") >= month_date)
                )
                .select(["current_mbi"])
                .unique()
            )

            if reach_mbis:
                mssp_status = mssp_candidates.filter(~pl.col("current_mbi").is_in(list(reach_mbis)))
            else:
                mssp_status = mssp_candidates

            mssp_status = mssp_status.with_columns(pl.lit(True).alias(f"ym_{ym}_mssp"))
        else:
            mssp_status = pl.DataFrame(
                {
                    "current_mbi": pl.Series([], dtype=pl.String),
                    f"ym_{ym}_mssp": pl.Series([], dtype=pl.Boolean),
                }
            )

        # Join program status
        result = result.join(reach_status, on="current_mbi", how="left")
        result = result.join(mssp_status, on="current_mbi", how="left")

        # Fill nulls with False
        result = result.with_columns(
            [
                pl.col(f"ym_{ym}_reach").fill_null(False),
                pl.col(f"ym_{ym}_mssp").fill_null(False),
            ]
        )

        # Calculate FFS status: has first claim AND not in ACO
        ffs_conditions = []
        first_claim_conditions = []
        for mbi in result["current_mbi"].to_list():
            is_in_ffs = False
            had_first_claim = False
            if mbi in ffs_dict:
                had_first_claim = ffs_dict[mbi] <= month_end
            last_ffs_date = ffs_last_dict.get(mbi, ffs_dict.get(mbi))
            death_date = death_dict.get(mbi)
            is_living_for_month = death_date is None or death_date >= month_date
            if last_ffs_date is not None and is_living_for_month:
                is_in_ffs = ffs_lookback_start <= last_ffs_date <= month_end
            ffs_conditions.append(is_in_ffs)
            first_claim_conditions.append(had_first_claim)

        result = result.with_columns([pl.Series(f"ym_{ym}_ffs_eligible", ffs_conditions)])
        result = result.with_columns(
            [pl.Series(f"ym_{ym}_first_claim_seen", first_claim_conditions)]
        )

        result = result.with_columns(
            [
                # FFS = living, not ACO, and touched a practice TIN in the trailing 24 months.
                (
                    ~pl.col(f"ym_{ym}_reach")
                    & ~pl.col(f"ym_{ym}_mssp")
                    & pl.col(f"ym_{ym}_ffs_eligible")
                ).alias(f"ym_{ym}_ffs"),
                pl.col(f"ym_{ym}_first_claim_seen").alias(f"ym_{ym}_first_claim"),
            ]
        ).drop([f"ym_{ym}_ffs_eligible", f"ym_{ym}_first_claim_seen"])

    # Deduplicate
    result = result.unique(subset=["current_mbi"], keep="last")

    # Calculate summary columns
    result = result.with_columns(
        [
            pl.sum_horizontal([pl.col(f"ym_{ym}_reach") for ym in year_months]).alias(
                "months_in_reach"
            ),
            pl.sum_horizontal([pl.col(f"ym_{ym}_mssp") for ym in year_months]).alias(
                "months_in_mssp"
            ),
            pl.sum_horizontal([pl.col(f"ym_{ym}_ffs") for ym in year_months]).alias(
                "months_in_ffs"
            ),
            pl.any_horizontal([pl.col(f"ym_{ym}_reach") for ym in year_months]).alias("ever_reach"),
            pl.any_horizontal([pl.col(f"ym_{ym}_mssp") for ym in year_months]).alias("ever_mssp"),
            pl.any_horizontal([pl.col(f"ym_{ym}_ffs") for ym in year_months]).alias("ever_ffs"),
            pl.col("current_mbi").alias("bene_mbi"),
            pl.when(pl.col(f"ym_{year_months[-1]}_reach") if year_months else False)
            .then(pl.lit("REACH"))
            .when(pl.col(f"ym_{year_months[-1]}_mssp") if year_months else False)
            .then(pl.lit("MSSP"))
            .when(pl.col(f"ym_{year_months[-1]}_ffs") if year_months else False)
            .then(pl.lit("FFS"))
            .otherwise(pl.lit("None"))
            .alias("current_program"),
            pl.lit(None).cast(pl.String).alias("current_aco_id"),
            pl.sum_horizontal(
                [
                    pl.col(f"ym_{ym}_reach") | pl.col(f"ym_{ym}_mssp") | pl.col(f"ym_{ym}_ffs")
                    for ym in year_months
                ]
            )
            .eq(len(year_months))
            .alias("continuous_enrollment"),
            pl.lit(0).alias("program_switches"),
            (
                len(year_months)
                - pl.sum_horizontal(
                    [
                        pl.col(f"ym_{ym}_reach") | pl.col(f"ym_{ym}_mssp") | pl.col(f"ym_{ym}_ffs")
                        for ym in year_months
                    ]
                )
            ).alias("enrollment_gaps"),
            pl.lit(0).alias("previous_mbi_count"),
            _calculate_first_program_date(year_months, "reach").alias("first_reach_date"),
            _calculate_last_program_date(year_months, "reach").alias("last_reach_date"),
            _calculate_first_program_date(year_months, "mssp").alias("first_mssp_date"),
            _calculate_last_program_date(year_months, "mssp").alias("last_mssp_date"),
        ]
    )

    logger.info(f"Temporal matrix complete with {len(year_months)} month columns")
    return result.lazy()
