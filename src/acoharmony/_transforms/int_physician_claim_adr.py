# © 2025 HarmonyCares
# All rights reserved.

"""
Pure Polars implementation of int_physician_claim_adr.

Applies adjustment logic to Physician (Part B) claims using
Polars-native operations for optimal performance and clarity.

This follows the same pattern as int_dme_claim_adr with more columns.

References:
    CCLF Implementation Guide v40.0 Section 5.3.1 Steps 2-3
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
    Execute Physician claim adjustment logic using Polars best practices.

        Algorithm:
        1. Deduplicate: Keep unique rows, preferring newest file_date
        2. MBI Crosswalk: Map to current beneficiary IDs
        3. Adjustment Logic: Negate amounts for canceled claims, rank by effective date
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    claims = pl.scan_parquet(silver_path / "cclf5.parquet")
    from ._identity_timeline import current_mbi_lookup_lazy
    xref = current_mbi_lookup_lazy(silver_path)

    dedup_columns = [
        "cur_clm_uniq_id",
        "clm_line_num",
        "bene_mbi_id",
        "bene_hic_num",
        "clm_type_cd",
        "clm_from_dt",
        "clm_thru_dt",
        "rndrg_prvdr_type_cd",
        "rndrg_prvdr_fips_st_cd",
        "clm_prvdr_spclty_cd",
        "clm_fed_type_srvc_cd",
        "clm_pos_cd",
        "clm_line_from_dt",
        "clm_line_thru_dt",
        "clm_line_hcpcs_cd",
        "clm_line_cvrd_pd_amt",
        "clm_line_prmry_pyr_cd",
        "clm_line_dgns_cd",
        "clm_rndrg_prvdr_tax_num",
        "rndrg_prvdr_npi_num",
        "clm_carr_pmt_dnl_cd",
        "clm_prcsg_ind_cd",
        "clm_adjsmt_type_cd",
        "clm_efctv_dt",
        "clm_idr_ld_dt",
        "clm_cntl_num",
        "bene_eqtbl_bic_hicn_num",
        "clm_line_alowd_chrg_amt",
        "clm_line_srvc_unit_qty",
        "hcpcs_1_mdfr_cd",
        "hcpcs_2_mdfr_cd",
        "hcpcs_3_mdfr_cd",
        "hcpcs_4_mdfr_cd",
        "hcpcs_5_mdfr_cd",
        "clm_disp_cd",
        "clm_dgns_1_cd",
        "clm_dgns_2_cd",
        "clm_dgns_3_cd",
        "clm_dgns_4_cd",
        "clm_dgns_5_cd",
        "clm_dgns_6_cd",
        "clm_dgns_7_cd",
        "clm_dgns_8_cd",
        "dgns_prcdr_icd_ind",
        "clm_dgns_9_cd",
        "clm_dgns_10_cd",
        "clm_dgns_11_cd",
        "clm_dgns_12_cd",
        "hcpcs_betos_cd",
    ]

    result = (
        claims
        .sort("file_date", descending=True)
        .unique(
            subset=dedup_columns,
            keep="first",  # Keeps newest file_date
            maintain_order=True,
        )
        .join(
            xref.select(["prvs_num", "crnt_num"]),
            left_on="bene_mbi_id",
            right_on="prvs_num",
            how="left",
        )
        .with_columns(pl.coalesce(["crnt_num", "bene_mbi_id"]).alias("current_bene_mbi_id"))
        # Apply CCLF ADR logic: negate amounts for cancellations (Section 5.3.1 Steps 2-3)
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
                "clm_rndrg_prvdr_tax_num",
                "rndrg_prvdr_npi_num",
                "clm_adjsmt_type_cd",
                "clm_efctv_dt",
                "clm_cntl_num",
                "clm_line_alowd_chrg_amt",
                "clm_line_srvc_unit_qty",
                "hcpcs_1_mdfr_cd",
                "hcpcs_2_mdfr_cd",
                "hcpcs_3_mdfr_cd",
                "hcpcs_4_mdfr_cd",
                "hcpcs_5_mdfr_cd",
                "clm_dgns_1_cd",
                "clm_dgns_2_cd",
                "clm_dgns_3_cd",
                "clm_dgns_4_cd",
                "clm_dgns_5_cd",
                "clm_dgns_6_cd",
                "clm_dgns_7_cd",
                "clm_dgns_8_cd",
                "dgns_prcdr_icd_ind",
                "clm_dgns_9_cd",
                "clm_dgns_10_cd",
                "clm_dgns_11_cd",
                "clm_dgns_12_cd",
                "source_filename",
                "file_date",
                "row_num",
            ]
        )
    )

    return result
