# © 2025 HarmonyCares
# All rights reserved.

"""
Pure Polars implementation of int_pharmacy_claim_adr.

Applies adjustment logic to Part D pharmacy claims:
1. Deduplicates claims from multiple files
2. Applies MBI crosswalk
3. Sorts by adjustment type code (0=Original, 1=Cancellation, 2=Adjustment)
"""

import polars as pl

from .._decor8 import composable, measure_dataframe_size, timeit, traced


@composable
@traced()
@timeit(log_level="debug")
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Execute pharmacy claim adjustment logic using native Polars.

        Process:
        1. Load staging data and deduplicate by file_date
        2. Apply MBI crosswalk to get current beneficiary ID
        3. Apply adjustment logic:
           - Group by natural keys (date, provider IDs, rx reference, fill number)
           - Sort by clm_adjsmt_type_cd desc (2 > 1 > 0)
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    staged_data = pl.scan_parquet(silver_path / "cclf7.parquet")

    beneficiary_xref = pl.scan_parquet(silver_path / "int_beneficiary_xref_deduped.parquet")

    partition_cols = [
        "cur_clm_uniq_id",
        "bene_mbi_id",
        "bene_hic_num",
        "clm_line_ndc_cd",
        "clm_line_from_dt",
        "prvdr_srvc_id_qlfyr_cd",
        "clm_srvc_prvdr_gnrc_id_num",
        "clm_dspnsng_stus_cd",
        "clm_line_srvc_unit_qty",
        "clm_line_days_suply_qty",
        "prvdr_prsbng_id_qlfyr_cd",
        "clm_prsbng_prvdr_gnrc_id_num",
        "clm_line_bene_pmt_amt",
        "clm_adjsmt_type_cd",
        "clm_line_rx_srvc_rfrnc_num",
        "clm_line_rx_fill_num",
    ]

    deduped = (
        staged_data.with_columns(
            pl.col("file_date")
            .rank(method="ordinal", descending=True)
            .over(partition_cols)
            .alias("row_num")
        )
        .filter(pl.col("row_num") == 1)
        .select(
            [
                "cur_clm_uniq_id",
                "bene_mbi_id",
                "bene_hic_num",
                "clm_line_ndc_cd",
                "clm_line_from_dt",
                "prvdr_srvc_id_qlfyr_cd",
                "clm_srvc_prvdr_gnrc_id_num",
                "clm_dspnsng_stus_cd",
                "clm_line_srvc_unit_qty",
                "clm_line_days_suply_qty",
                "prvdr_prsbng_id_qlfyr_cd",
                "clm_prsbng_prvdr_gnrc_id_num",
                "clm_line_bene_pmt_amt",
                "clm_adjsmt_type_cd",
                "clm_line_rx_srvc_rfrnc_num",
                "clm_line_rx_fill_num",
                "source_filename",
                "file_date",
            ]
        )
    )

    with_mbi = deduped.join(
        beneficiary_xref.select(["prvs_num", "crnt_num"]),
        left_on="bene_mbi_id",
        right_on="prvs_num",
        how="left",
    ).select(
        [
            pl.exclude("prvs_num", "crnt_num"),
            pl.coalesce(["crnt_num", "bene_mbi_id"]).alias("current_bene_mbi_id"),
        ]
    )

    result = with_mbi.with_columns(
        pl.col("clm_adjsmt_type_cd")
        .rank(method="ordinal", descending=True)
        .over(
            [
                "clm_line_from_dt",
                "prvdr_srvc_id_qlfyr_cd",
                "clm_srvc_prvdr_gnrc_id_num",
                "clm_dspnsng_stus_cd",
                "clm_line_rx_srvc_rfrnc_num",
                "clm_line_rx_fill_num",
            ]
        )
        .alias("row_num")
    ).select(
        [
            "cur_clm_uniq_id",
            "current_bene_mbi_id",
            "bene_hic_num",
            "clm_line_ndc_cd",
            "clm_line_from_dt",
            "prvdr_srvc_id_qlfyr_cd",
            "clm_srvc_prvdr_gnrc_id_num",
            "clm_dspnsng_stus_cd",
            "clm_line_srvc_unit_qty",
            "clm_line_days_suply_qty",
            "prvdr_prsbng_id_qlfyr_cd",
            "clm_prsbng_prvdr_gnrc_id_num",
            "clm_line_bene_pmt_amt",
            "clm_adjsmt_type_cd",
            "clm_line_rx_srvc_rfrnc_num",
            "clm_line_rx_fill_num",
            "source_filename",
            "file_date",
            "row_num",
        ]
    )

    return result
