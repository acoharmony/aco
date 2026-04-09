# © 2025 HarmonyCares
# All rights reserved.

"""
Pure Polars implementation of int_institutional_claim_adr.

Applies adjustment logic to Institutional (Part A) claims using
Polars-native operations for optimal performance and clarity.

This follows the same pattern as int_dme_claim_adr and int_physician_claim_adr.

References:
    CCLF Implementation Guide v40.0 Section 5.3.1 Step 1
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
    Execute Institutional claim adjustment logic using Polars best practices.

        Algorithm:
        1. Deduplicate: Keep unique rows, preferring newest file_date
        2. MBI Crosswalk: Map to current beneficiary IDs
        3. Adjustment Logic: Negate amounts for canceled claims, rank by effective date

        Key difference: Groups by clm_blg_prvdr_oscar_num + dates + MBI (not clm_cntl_num)
    """
    from acoharmony.medallion import MedallionLayer
    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)
    claims = pl.scan_parquet(silver_path / "cclf1.parquet")
    xref = pl.scan_parquet(silver_path / "int_beneficiary_xref_deduped.parquet")
    claims = claims.with_columns(
        [
            pl.lit(None).cast(pl.Utf8).alias("bene_hic_num"),
            pl.lit(None).cast(pl.Utf8).alias("bene_eqtbl_bic_hicn_num"),
        ]
    )
    dedup_columns = [
        "cur_clm_uniq_id",
        "prvdr_oscar_num",
        "bene_mbi_id",
        "bene_hic_num",
        "clm_type_cd",
        "clm_from_dt",
        "clm_thru_dt",
        "clm_bill_fac_type_cd",
        "clm_bill_clsfctn_cd",
        "prncpl_dgns_cd",
        "admtg_dgns_cd",
        "clm_mdcr_npmt_rsn_cd",
        "clm_pmt_amt",
        "clm_nch_prmry_pyr_cd",
        "prvdr_fac_fips_st_cd",
        "bene_ptnt_stus_cd",
        "dgns_drg_cd",
        "clm_op_srvc_type_cd",
        "fac_prvdr_npi_num",
        "oprtg_prvdr_npi_num",
        "atndg_prvdr_npi_num",
        "othr_prvdr_npi_num",
        "clm_adjsmt_type_cd",
        "clm_efctv_dt",
        "clm_idr_ld_dt",
        "bene_eqtbl_bic_hicn_num",
        "clm_admsn_type_cd",
        "clm_admsn_src_cd",
        "clm_bill_freq_cd",
        "clm_query_cd",
        "dgns_prcdr_icd_ind",
        "clm_mdcr_instnl_tot_chrg_amt",
        "clm_mdcr_ip_pps_cptl_ime_amt",
        "clm_oprtnl_ime_amt",
        "clm_mdcr_ip_pps_dsprprtnt_amt",
        "clm_hipps_uncompd_care_amt",
        "clm_oprtnl_dsprprtnt_amt",
        "clm_blg_prvdr_oscar_num",
        "clm_blg_prvdr_npi_num",
        "clm_oprtg_prvdr_npi_num",
        "clm_atndg_prvdr_npi_num",
        "clm_othr_prvdr_npi_num",
        "clm_cntl_num",
        "clm_org_cntl_num",
        "clm_cntrctr_num",
    ]
    result = (
        claims
        .sort("file_date", descending=True)
        .unique(
            subset=dedup_columns,
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
        # Apply CCLF ADR logic: negate amounts for cancellations (Section 5.3.1 Step 1)
        .with_columns(CclfAdrExpression.negate_cancellations_header())
        # Rank by effective date to identify latest version
        .sort(["clm_efctv_dt", "cur_clm_uniq_id"], descending=True)
        .with_columns(
            CclfAdrExpression.rank_by_effective_date(
                ["clm_blg_prvdr_oscar_num", "clm_from_dt", "clm_thru_dt", "current_bene_mbi_id"]
            )
        )
        .select(
            [
                "cur_clm_uniq_id",
                "current_bene_mbi_id",
                "clm_from_dt",
                "clm_thru_dt",
                "clm_bill_fac_type_cd",
                "clm_bill_clsfctn_cd",
                "clm_pmt_amt",
                "bene_ptnt_stus_cd",
                "dgns_drg_cd",
                "fac_prvdr_npi_num",
                "atndg_prvdr_npi_num",
                "clm_adjsmt_type_cd",
                "clm_efctv_dt",
                "clm_admsn_type_cd",
                "clm_admsn_src_cd",
                "clm_bill_freq_cd",
                "dgns_prcdr_icd_ind",
                "clm_mdcr_instnl_tot_chrg_amt",
                "clm_blg_prvdr_oscar_num",
                "source_filename",
                "file_date",
                "row_num",
            ]
        )
    )

    return result
