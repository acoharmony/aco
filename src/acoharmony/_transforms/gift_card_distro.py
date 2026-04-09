# © 2025 HarmonyCares
# All rights reserved.

"""
Gift Card Distribution Analysis Transform.

Compares the latest BAR, HDAI REACH report, and gift card management (GCM) data to:
- Verify patients are alive (no death date)
- Confirm patients are on the most recent BAR
- Compare against HDAI report enrollment status
- Identify who is missing from each source
- Validate AWV completion status across sources
"""

import polars as pl

from .._decor8 import measure_dataframe_size, transform, transform_method


@transform(name="gift_card_distro", tier=["silver"], sql_enabled=False)
@transform_method(enable_composition=True, threshold=5.0)
@measure_dataframe_size
def execute(executor) -> pl.LazyFrame:
    """
    Execute gift card distribution analysis across BAR, HDAI, and GCM.

        Args:
            executor: TuvaSQLExecutor instance with access to staging models

        Returns:
            LazyFrame with gift card distribution analysis results
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    gcm = pl.scan_parquet(silver_path / "gcm.parquet")

    bar = pl.scan_parquet(silver_path / "bar.parquet")

    hdai = pl.scan_parquet(silver_path / "hdai_reach.parquet")

    bar_file_dates = bar.select(pl.col("file_date")).collect()
    most_recent_bar_date = bar_file_dates["file_date"].max()

    bar_latest = bar.filter(pl.col("file_date") == most_recent_bar_date).with_columns(
        [
            pl.col("bene_mbi").alias("mbi_bar"),
            pl.col("bene_date_of_death").alias("bar_death_date"),
            pl.col("bene_date_of_death").is_null().alias("is_alive_bar"),
            pl.lit(True).alias("in_bar"),
            pl.col("start_date").alias("bar_start_date"),
            pl.col("end_date").alias("bar_end_date"),
        ]
    )

    hdai_file_dates = hdai.select(pl.col("file_date")).collect()
    most_recent_hdai_date = hdai_file_dates["file_date"].max()

    hdai_latest = hdai.filter(pl.col("file_date") == most_recent_hdai_date).with_columns(
        [
            pl.col("mbi").alias("mbi_hdai"),
            pl.col("patient_dod").alias("hdai_death_date"),
            pl.col("patient_dod").is_null().alias("is_alive_hdai"),
            pl.lit(True).alias("in_hdai"),
            pl.col("enrollment_status").alias("hdai_enrollment_status"),
            pl.col("most_recent_awv_date").alias("hdai_awv_date"),
            pl.col("last_em_visit").alias("hdai_last_em"),
        ]
    )

    gcm_prep = gcm.with_columns(
        [
            pl.col("mbi").alias("mbi_gcm"),
            pl.lit(True).alias("in_gcm"),
            pl.col("awv_date").alias("gcm_awv_date"),
            pl.col("awv_status").alias("gcm_awv_status"),
            pl.col("gift_card_status"),
            pl.col("lc_status_current").alias("gcm_lifecycle_status"),
            pl.col("roll12_awv_enc").alias("gcm_roll12_awv"),
            pl.col("roll12_em").alias("gcm_roll12_em"),
        ]
    )

    gcm_bar = gcm_prep.join(
        bar_latest.select(
            [
                "bene_mbi",
                "bar_death_date",
                "is_alive_bar",
                "in_bar",
                "bar_start_date",
                "bar_end_date",
                "bene_first_name",
                "bene_last_name",
                "bene_address_line_1",
                "bene_city",
                "bene_state",
                "bene_zip_5",
            ]
        ),
        left_on="mbi",
        right_on="bene_mbi",
        how="left",
    )

    gcm_bar_hdai = gcm_bar.join(
        hdai_latest.select(
            [
                "mbi",
                "hdai_death_date",
                "is_alive_hdai",
                "in_hdai",
                "hdai_enrollment_status",
                "hdai_awv_date",
                "hdai_last_em",
                "patient_first_name",
                "patient_last_name",
                "patient_address",
                "patient_city",
                "patient_state",
                "patient_zip",
            ]
        ),
        left_on="mbi",
        right_on="mbi",
        how="left",
        suffix="_hdai",
    )

    result = gcm_bar_hdai.with_columns(
        [
            pl.col("in_bar").fill_null(False).alias("in_bar"),
            pl.col("in_hdai").fill_null(False).alias("in_hdai"),
            pl.when(pl.col("bar_death_date").is_not_null())
            .then(pl.lit(False))
            .when(pl.col("hdai_death_date").is_not_null())
            .then(pl.lit(False))
            .otherwise(pl.lit(True))
            .alias("is_alive"),
            pl.coalesce(["bar_death_date", "hdai_death_date"]).alias("death_date"),
            pl.when(pl.col("in_bar").fill_null(False) & pl.col("in_hdai").fill_null(False))
            .then(pl.lit("GCM+BAR+HDAI"))
            .when(pl.col("in_bar").fill_null(False))
            .then(pl.lit("GCM+BAR"))
            .when(pl.col("in_hdai").fill_null(False))
            .then(pl.lit("GCM+HDAI"))
            .otherwise(pl.lit("GCM Only"))
            .alias("data_source_status"),
            (pl.col("gcm_awv_date") == pl.col("hdai_awv_date")).alias("awv_date_matches_hdai"),
            (~pl.col("in_bar").fill_null(False)).alias("alert_not_in_bar"),
            (~pl.col("in_hdai").fill_null(False)).alias("alert_not_in_hdai"),
            (~pl.col("is_alive_bar").fill_null(True)).alias("alert_deceased_in_bar"),
            (~pl.col("is_alive_hdai").fill_null(True)).alias("alert_deceased_in_hdai"),
            (pl.col("patientaddress").is_null() | (pl.col("patientaddress") == "null")).alias(
                "missing_address_in_gcm"
            ),
            pl.coalesce(["bene_first_name", "patient_first_name"]).alias("first_name"),
            pl.coalesce(["bene_last_name", "patient_last_name"]).alias("last_name"),
            pl.coalesce(["bene_address_line_1", "patient_address", "patientaddress"]).alias(
                "address"
            ),
            pl.coalesce(["bene_city", "patient_city", "patientcity"]).alias("city"),
            pl.coalesce(["bene_state", "patient_state", "patientstate"]).alias("state"),
            pl.coalesce(["bene_zip_5", "patient_zip", "patientzip"]).alias("zip"),
            pl.lit(most_recent_bar_date).alias("bar_report_date"),
            pl.lit(most_recent_hdai_date).alias("hdai_report_date"),
        ]
    )

    final = result.select(
        [
            pl.col("hcmpi"),
            pl.col("mbi"),
            pl.col("first_name"),
            pl.col("last_name"),
            pl.col("is_alive"),
            pl.col("death_date"),
            pl.col("in_gcm"),
            pl.col("in_bar"),
            pl.col("in_hdai"),
            pl.col("data_source_status"),
            pl.col("gift_card_status"),
            pl.col("gcm_lifecycle_status"),
            pl.col("gcm_awv_date"),
            pl.col("hdai_awv_date"),
            pl.col("awv_date_matches_hdai"),
            pl.col("gcm_awv_status"),
            pl.col("gcm_roll12_awv"),
            pl.col("gcm_roll12_em"),
            pl.col("hdai_last_em"),
            pl.col("bar_start_date"),
            pl.col("bar_end_date"),
            pl.col("hdai_enrollment_status"),
            pl.col("address"),
            pl.col("city"),
            pl.col("state"),
            pl.col("zip"),
            pl.col("missing_address_in_gcm"),
            pl.col("patientaddress").alias("gcm_address"),
            pl.col("patientcity").alias("gcm_city"),
            pl.col("patientstate").alias("gcm_state"),
            pl.col("patientzip").alias("gcm_zip"),
            pl.col("alert_not_in_bar"),
            pl.col("alert_not_in_hdai"),
            pl.col("alert_deceased_in_bar"),
            pl.col("alert_deceased_in_hdai"),
            pl.col("bar_report_date"),
            pl.col("hdai_report_date"),
            pl.col("payer"),
            pl.col("payer_current"),
            pl.col("total_count"),
        ]
    )

    return final
