# © 2025 HarmonyCares
# All rights reserved.

"""
BAR and HDAI Report Reconciliation Transform.

Compares the latest Beneficiary Alignment Report (BAR) with the latest HDAI REACH report
to identify:
- Patients who are alive (no death date or death date is null)
- Patients on the most recent BAR
- Patients missing from each report
- Alignment status discrepancies
"""

import polars as pl

from .._decor8 import composable, measure_dataframe_size, timeit, traced, transform


@composable
@transform(name="bar_hdai_reconciliation", tier=["silver"], sql_enabled=False)
@traced()
@timeit(log_level="debug")
@measure_dataframe_size
def execute(executor) -> pl.LazyFrame:
    """
    Execute BAR and HDAI reconciliation analysis.

        Args:
            executor: TuvaSQLExecutor instance with access to staging models

        Returns:
            LazyFrame with reconciliation results
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    bar = pl.scan_parquet(silver_path / "bar.parquet")

    hdai = pl.scan_parquet(silver_path / "hdai_reach.parquet")

    bar_file_dates = bar.select(pl.col("file_date")).collect()
    most_recent_bar_date = bar_file_dates["file_date"].max()

    bar_latest = bar.filter(pl.col("file_date") == most_recent_bar_date)

    hdai_file_dates = hdai.select(pl.col("file_date")).collect()
    most_recent_hdai_date = hdai_file_dates["file_date"].max()

    hdai_latest = hdai.filter(pl.col("file_date") == most_recent_hdai_date)

    bar_with_status = bar_latest.with_columns(
        [
            pl.col("bene_date_of_death").is_null().alias("is_alive_bar"),
            pl.lit(True).alias("in_bar"),
            pl.col("start_date").alias("bar_start_date"),
            pl.col("end_date").alias("bar_end_date"),
            pl.col("bene_mbi").alias("mbi_bar"),
        ]
    )

    hdai_with_status = hdai_latest.with_columns(
        [
            pl.col("patient_dod").is_null().alias("is_alive_hdai"),
            pl.lit(True).alias("in_hdai"),
            pl.col("enrollment_status").alias("hdai_enrollment_status"),
            pl.col("mbi").alias("mbi_hdai"),
            pl.col("most_recent_awv_date").alias("hdai_awv_date"),
            pl.col("last_em_visit").alias("hdai_last_em"),
        ]
    )

    reconciliation = bar_with_status.join(
        hdai_with_status, left_on="bene_mbi", right_on="mbi", how="outer", coalesce=True
    ).with_columns(
        [
            pl.col("in_bar").fill_null(False).alias("in_bar"),
            pl.col("in_hdai").fill_null(False).alias("in_hdai"),
            pl.when(pl.col("in_bar").fill_null(False) & pl.col("in_hdai").fill_null(False))
            .then(pl.lit("In Both"))
            .when(pl.col("in_bar").fill_null(False) & ~pl.col("in_hdai").fill_null(False))
            .then(pl.lit("BAR Only"))
            .when(~pl.col("in_bar").fill_null(False) & pl.col("in_hdai").fill_null(False))
            .then(pl.lit("HDAI Only"))
            .otherwise(pl.lit("Unknown"))
            .alias("reconciliation_status"),
            pl.coalesce(["is_alive_bar", "is_alive_hdai"]).alias("is_alive"),
            pl.coalesce(["bene_mbi", "mbi_hdai"]).alias("mbi"),
            pl.coalesce(["bene_date_of_death", "patient_dod"]).alias("death_date"),
            pl.coalesce(["bene_address_line_1", "patient_address"]).alias("address_line_1"),
            pl.col("bene_city").alias("bar_city"),
            pl.col("patient_city").alias("hdai_city"),
            pl.coalesce(["bene_city", "patient_city"]).alias("city"),
            pl.col("bene_state").alias("bar_state"),
            pl.col("patient_state").alias("hdai_state"),
            pl.coalesce(["bene_state", "patient_state"]).alias("state"),
            pl.col("bene_zip_5").alias("bar_zip"),
            pl.col("patient_zip").alias("hdai_zip"),
            pl.coalesce(["bene_zip_5", "patient_zip"]).alias("zip_code"),
            pl.coalesce(["bene_first_name", "patient_first_name"]).alias("first_name"),
            pl.coalesce(["bene_last_name", "patient_last_name"]).alias("last_name"),
            pl.coalesce(["bene_date_of_birth", "patient_dob"]).alias("date_of_birth"),
            pl.lit(most_recent_bar_date).alias("bar_report_date"),
            pl.lit(most_recent_hdai_date).alias("hdai_report_date"),
        ]
    )

    result = reconciliation.select(
        [
            pl.col("mbi"),
            pl.col("first_name"),
            pl.col("last_name"),
            pl.col("date_of_birth"),
            pl.col("death_date"),
            pl.col("is_alive"),
            pl.col("reconciliation_status"),
            pl.col("in_bar"),
            pl.col("in_hdai"),
            pl.col("bar_start_date"),
            pl.col("bar_end_date"),
            pl.col("hdai_enrollment_status"),
            pl.col("hdai_awv_date"),
            pl.col("hdai_last_em"),
            pl.col("address_line_1"),
            pl.col("city"),
            pl.col("state"),
            pl.col("zip_code"),
            pl.col("bar_city"),
            pl.col("hdai_city"),
            pl.col("bar_state"),
            pl.col("hdai_state"),
            pl.col("bar_zip"),
            pl.col("hdai_zip"),
            pl.col("bar_report_date"),
            pl.col("hdai_report_date"),
        ]
    )

    return result
