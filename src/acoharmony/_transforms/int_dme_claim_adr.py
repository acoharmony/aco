# © 2025 HarmonyCares
# All rights reserved.

"""
Pure Polars implementation of int_dme_claim_adr.

Applies adjustment logic to DME (Durable Medical Equipment) claims using
Polars-native operations for optimal performance and clarity.

Key optimizations:
- Uses .unique() instead of row_number() for deduplication
- Leverages sort_by() + group_by().head() for ranking
- Applies transformations only on needed columns
- No unnecessary CTEs or intermediate column selection

References:
    CCLF Implementation Guide v40.0 Section 5.3.1 Step 3
"""

import polars as pl

from .._decor8 import composable, measure_dataframe_size, timeit, traced
from .._expressions import CclfAdrExpression


@composable
@traced()
@timeit(log_level="debug")
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Execute DME claim adjustment logic using Polars best practices.

        Algorithm:
        1. Deduplicate: Keep unique rows, preferring newest file_date
        2. MBI Crosswalk: Map to current beneficiary IDs
        3. Adjustment Logic: Negate amounts for canceled claims, rank by effective date
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    claims = pl.scan_parquet(silver_path / "cclf6.parquet")
    xref = pl.scan_parquet(silver_path / "int_beneficiary_xref_deduped.parquet")

    result = (
        claims
        .sort("file_date", descending=True)
        .unique(
            subset=[
                "cur_clm_uniq_id",
                "clm_line_num",
                "bene_mbi_id",
                "clm_from_dt",
                "clm_thru_dt",
                "clm_line_hcpcs_cd",
                "clm_line_cvrd_pd_amt",
                "clm_adjsmt_type_cd",
                "clm_cntl_num",
            ],
            keep="first",
            maintain_order=True,
        )
        .join(
            xref.select(["prvs_num", "crnt_num"]),
            left_on="bene_mbi_id",
            right_on="prvs_num",
            how="left",
        )
        .with_columns(pl.coalesce(["crnt_num", "bene_mbi_id"]).alias("current_bene_mbi_id"))
        # Apply CCLF ADR logic: negate amounts for cancellations (Section 5.3.1 Step 3)
        .with_columns(CclfAdrExpression.negate_cancellations_line())
        # Rank by effective date to identify latest version
        .sort(["clm_efctv_dt", "cur_clm_uniq_id"], descending=True)
        .with_columns(
            CclfAdrExpression.rank_by_effective_date(
                ["clm_cntl_num", "clm_line_num", "current_bene_mbi_id"]
            )
        )
        .select(
            [
                "cur_clm_uniq_id",
                "clm_line_num",
                "current_bene_mbi_id",
                "clm_from_dt",
                "clm_thru_dt",
                "clm_pos_cd",
                "clm_line_from_dt",
                "clm_line_thru_dt",
                "clm_line_hcpcs_cd",
                "clm_line_cvrd_pd_amt",
                "payto_prvdr_npi_num",
                "ordrg_prvdr_npi_num",
                "clm_adjsmt_type_cd",
                "clm_efctv_dt",
                "clm_cntl_num",
                "clm_line_alowd_chrg_amt",
                "source_filename",
                "file_date",
                "row_num",
            ]
        )
    )

    return result
