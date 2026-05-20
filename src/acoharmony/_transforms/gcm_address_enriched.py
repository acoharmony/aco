# © 2025 HarmonyCares
# All rights reserved.

"""
Gift Card Management Address Enrichment Transform.

Populates empty or null addresses in the GCM data using deduplicated beneficiary
demographics from CCLF8 (int_beneficiary_demographics_deduped).
"""

import polars as pl

from .._decor8 import composable, measure_dataframe_size, timeit, traced, transform


@composable
@transform(name="gcm_address_enriched", tier=["silver"], sql_enabled=False)
@traced()
@timeit(log_level="debug")
@measure_dataframe_size
def execute(executor) -> pl.LazyFrame:
    """
    Enrich GCM data with addresses from beneficiary demographics.

        Args:
            executor: TuvaSQLExecutor instance with access to staging models

        Returns:
            LazyFrame with enriched GCM data including populated addresses
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)
    gcm = pl.scan_parquet(silver_path / "gcm.parquet")
    bene_demo = pl.scan_parquet(silver_path / "cclf8.parquet")
    bene_demo_latest = (
        bene_demo.with_columns(
            pl.col("file_date")
            .rank(method="ordinal", descending=True)
            .over("bene_mbi_id")
            .alias("row_num")
        )
        .filter(pl.col("row_num") == 1)
        .select(
            [
                pl.col("bene_mbi_id"),
                pl.col("bene_line_1_adr").alias("demo_address_line_1"),
                pl.col("bene_line_2_adr").alias("demo_address_line_2"),
                pl.col("bene_city").alias("demo_city"),
                pl.col("bene_state").alias("demo_state"),
                pl.col("bene_zip").alias("demo_zip"),
            ]
        )
    )

    gcm_enriched = gcm.join(bene_demo_latest, left_on="mbi", right_on="bene_mbi_id", how="left")

    result = gcm_enriched.with_columns(
        [
            (
                pl.col("patientaddress").is_null()
                | (pl.col("patientaddress") == "null")
                | (pl.col("patientaddress").str.strip_chars() == "")
            ).alias("address_was_missing"),
            pl.when(
                pl.col("patientaddress").is_null()
                | (pl.col("patientaddress") == "null")
                | (pl.col("patientaddress").str.strip_chars() == "")
            )
            .then(pl.col("demo_address_line_1"))
            .otherwise(pl.col("patientaddress"))
            .alias("patientaddress_enriched"),
            pl.when(
                pl.col("patientaddress2").is_null()
                | (pl.col("patientaddress2") == "null")
                | (pl.col("patientaddress2").str.strip_chars() == "")
            )
            .then(pl.col("demo_address_line_2"))
            .otherwise(pl.col("patientaddress2"))
            .alias("patientaddress2_enriched"),
            pl.when(
                pl.col("patientcity").is_null()
                | (pl.col("patientcity") == "null")
                | (pl.col("patientcity").str.strip_chars() == "")
            )
            .then(pl.col("demo_city"))
            .otherwise(pl.col("patientcity"))
            .alias("patientcity_enriched"),
            pl.when(
                pl.col("patientstate").is_null()
                | (pl.col("patientstate") == "null")
                | (pl.col("patientstate").str.strip_chars() == "")
            )
            .then(pl.col("demo_state"))
            .otherwise(pl.col("patientstate"))
            .alias("patientstate_enriched"),
            pl.when(
                pl.col("patientzip").is_null()
                | (pl.col("patientzip") == "null")
                | (pl.col("patientzip").str.strip_chars() == "")
            )
            .then(pl.col("demo_zip"))
            .otherwise(pl.col("patientzip"))
            .alias("patientzip_enriched"),
        ]
    )

    final = result.select(
        [
            pl.col("total_count"),
            pl.col("hcmpi"),
            pl.col("payer_current"),
            pl.col("payer"),
            pl.col("roll12_awv_enc"),
            pl.col("awv_status"),
            pl.col("roll12_em"),
            pl.col("lc_status_current"),
            pl.col("awv_date"),
            pl.col("mbi"),
            pl.col("patientaddress").alias("patientaddress_original"),
            pl.col("patientaddress2").alias("patientaddress2_original"),
            pl.col("patientcity").alias("patientcity_original"),
            pl.col("patientstate").alias("patientstate_original"),
            pl.col("patientzip").alias("patientzip_original"),
            pl.col("patientaddress_enriched").alias("patientaddress"),
            pl.col("patientaddress2_enriched").alias("patientaddress2"),
            pl.col("patientcity_enriched").alias("patientcity"),
            pl.col("patientstate_enriched").alias("patientstate"),
            pl.col("patientzip_enriched").alias("patientzip"),
            pl.col("address_was_missing"),
            pl.col("gift_card_status"),
            pl.col("processed_at"),
            pl.col("source_file"),
            pl.col("source_filename"),
            pl.col("file_date"),
            pl.col("medallion_layer"),
        ]
    )

    return final
