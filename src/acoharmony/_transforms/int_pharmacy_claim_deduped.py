# © 2025 HarmonyCares
# All rights reserved.

"""
Pure Polars implementation of int_pharmacy_claim_deduped.

Filters pharmacy claims to latest version, removes canceled claims,
and handles duplicates. Maps to Tuva pharmacy_claim schema.
"""

import polars as pl

from .._decor8 import measure_dataframe_size, transform_method


@transform_method(enable_composition=False, threshold=5.0)
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Execute pharmacy claim deduplication logic.

        Algorithm:
        1. Filter to row_num = 1 (latest version)
        2. Exclude canceled claims (clm_adjsmt_type_cd = '1')
        3. Remove claim IDs that still have duplicates after adjustment
        4. Map to pharmacy_claim schema
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    claims = pl.scan_parquet(silver_path / "int_pharmacy_claim_adr.parquet")

    result = (
        claims
        .filter((pl.col("row_num") == 1) & (pl.col("clm_adjsmt_type_cd") != "1"))
        .with_columns(
            pl.col("cur_clm_uniq_id").count().over("cur_clm_uniq_id").alias("claim_count")
        )
        .filter(pl.col("claim_count") == 1)
        .with_columns(
            [
                pl.col("cur_clm_uniq_id").alias("claim_id"),
                pl.lit(1).alias("claim_line_number"),
                pl.col("current_bene_mbi_id").alias("person_id"),
                pl.col("current_bene_mbi_id").alias("member_id"),
                pl.lit("medicare").alias("payer"),
                pl.lit("medicare").alias("plan"),
                pl.when(pl.col("prvdr_prsbng_id_qlfyr_cd").is_in(["1", "01"]))
                .then(pl.col("clm_prsbng_prvdr_gnrc_id_num"))
                .otherwise(None)
                .alias("prescribing_provider_npi"),
                pl.when(pl.col("prvdr_srvc_id_qlfyr_cd").is_in(["1", "01"]))
                .then(pl.col("clm_srvc_prvdr_gnrc_id_num"))
                .otherwise(None)
                .alias("dispensing_provider_npi"),
                pl.col("clm_line_from_dt").alias("dispensing_date"),
                pl.col("clm_line_ndc_cd").alias("ndc_code"),
                pl.col("clm_line_srvc_unit_qty").alias("quantity"),
                pl.col("clm_line_days_suply_qty").alias("days_supply"),
                pl.col("clm_line_rx_fill_num").alias("refills"),
                pl.col("clm_line_from_dt").alias("paid_date"),
                pl.col("clm_line_bene_pmt_amt").alias("paid_amount"),
                pl.lit(None).cast(pl.String).alias("allowed_amount"),
                pl.lit(None).cast(pl.String).alias("charge_amount"),
                pl.lit(None).cast(pl.String).alias("coinsurance_amount"),
                pl.col("clm_line_bene_pmt_amt").alias("copayment_amount"),
                pl.lit(None).cast(pl.String).alias("deductible_amount"),
                pl.lit(1).alias("in_network_flag"),
                pl.lit("medicare cclf").alias("data_source"),
                pl.col("file_date").alias("ingest_datetime"),
            ]
        )
        .select(
            [
                "claim_id",
                "claim_line_number",
                "person_id",
                "member_id",
                "payer",
                "plan",
                "prescribing_provider_npi",
                "dispensing_provider_npi",
                "dispensing_date",
                "ndc_code",
                "quantity",
                "days_supply",
                "refills",
                "paid_date",
                "paid_amount",
                "allowed_amount",
                "charge_amount",
                "coinsurance_amount",
                "copayment_amount",
                "deductible_amount",
                "in_network_flag",
                "data_source",
                "source_filename",
                "ingest_datetime",
            ]
        )
    )

    return result
