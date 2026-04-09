# © 2025 HarmonyCares
# All rights reserved.

"""
Pure Polars implementation of pharmacy_claim final model.

Simply casts types to match Tuva schema requirements.
"""

import polars as pl

from .._decor8 import measure_dataframe_size


@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Execute pharmacy_claim type casting.

        This is a simple type conversion layer on top of int_pharmacy_claim_deduped.

        NOTE: Final models read from pre-processed int_* parquet files, not re-execute logic.
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    deduped = pl.scan_parquet(silver_path / "int_pharmacy_claim_deduped.parquet")

    result = deduped.with_columns(
        [
            pl.col("claim_id").cast(pl.String),
            pl.col("claim_line_number").cast(pl.Int64),
            pl.col("person_id").cast(pl.String),
            pl.col("member_id").cast(pl.String),
            pl.col("payer").cast(pl.String),
            pl.col("plan").cast(pl.String),
            pl.col("prescribing_provider_npi").cast(pl.String),
            pl.col("dispensing_provider_npi").cast(pl.String),
            pl.col("ndc_code").cast(pl.String),
            pl.col("quantity").cast(pl.Int64),
            pl.col("days_supply").cast(pl.Int64),
            pl.col("refills").cast(pl.Int64),
            pl.col("paid_amount").cast(pl.Decimal(scale=2)),
            pl.col("allowed_amount").cast(pl.Decimal(scale=2)),
            pl.col("charge_amount").cast(pl.Decimal(scale=2)),
            pl.col("coinsurance_amount").cast(pl.Decimal(scale=2)),
            pl.col("copayment_amount").cast(pl.Decimal(scale=2)),
            pl.col("deductible_amount").cast(pl.Decimal(scale=2)),
            pl.col("in_network_flag").cast(pl.Int64),
            pl.col("data_source").cast(pl.String),
            pl.col("source_filename").cast(pl.String),
            pl.col("ingest_datetime").str.to_date(),
        ]
    )

    return result
