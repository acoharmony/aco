# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for sbmepi schema.

Generated from: _schemas/sbmepi.yml
"""

from datetime import date

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_four_icli,
    with_parser,
    with_storage,
)


@register_schema(
    name="sbmepi",
    version=2,
    tier="bronze",
    description="Shadow Bundles Monthly Episode (bundled episodes)",
    file_patterns={"reach": ["D????.PY????.??.SBMEPI.D??????.T*.csv"]},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=True, embedded_transforms=False
)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["D????.PY????.??.SBMEPI.D??????.T*.csv"]},
    silver={"output_name": "sbmepi.parquet", "refresh_frequency": "monthly"},
)
@with_four_icli(
    category="Reports",
    file_type_code=243,
    file_pattern="D????.PY????.??.SBMEPI.D??????.T*.csv",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class Sbmepi:
    """
    Shadow Bundles Monthly Episode (bundled episodes)
    """

    episode_id: str | None = Field(default=None, description="Episode identifier")
    aco_org_id: str | None = Field(default=None, description="ACO organization identifier")
    aco_org_id_name: str | None = Field(default=None, description="ACO organization name")
    episode_group_name: str | None = Field(default=None, description="Episode group name")
    service_line_group: str | None = Field(default=None, description="Service line group")
    curhic_uneq: str | None = Field(default=None, description="Current HIC unique identifier")
    mbi_id: str | None = Field(default=None, description="Medicare Beneficiary Identifier")
    bene_gvn_name: str | None = Field(default=None, description="Beneficiary given name")
    bene_mdl_name: str | None = Field(default=None, description="Beneficiary middle name")
    bene_srnm_name: str | None = Field(default=None, description="Beneficiary surname")
    bene_age: int | None = Field(default=None, description="Beneficiary age")
    bene_gender: str | None = Field(default=None, description="Beneficiary gender")
    bene_birth_dt: date | None = Field(default=None, description="Beneficiary birth date")
    bene_death_dt: date | None = Field(default=None, description="Beneficiary death date")
    anchor_type: str | None = Field(default=None, description="Anchor type (IP or OP)")
    anchor_trigger_cd: str | None = Field(default=None, description="Anchor trigger code")
    anchor_apc: str | None = Field(default=None, description="Anchor APC code")
    anchor_apc_pmt_rate: str | None = Field(default=None, description="Anchor APC payment rate")
    anchor_c_apc_flag: int | None = Field(default=None, description="Anchor comprehensive APC flag")
    anchor_provider: str | None = Field(default=None, description="Anchor provider number")
    anchor_at_npi: str | None = Field(default=None, description="Anchor attending NPI")
    anchor_op_npi: str | None = Field(default=None, description="Anchor operating NPI")
    anchor_beg_dt: date | None = Field(default=None, description="Anchor begin date")
    anchor_end_dt: date | None = Field(default=None, description="Anchor end date")
    post_dsch_beg_dt: date | None = Field(default=None, description="Post-discharge begin date")
    post_dsch_end_dt: date | None = Field(default=None, description="Post-discharge end date")
    anchor_standard_allowed_amt: str | None = Field(
        default=None, description="Anchor standardized allowed amount"
    )
    anchor_allowed_amt: str | None = Field(default=None, description="Anchor allowed amount")
    transfer_stay: int | None = Field(default=None, description="Transfer stay indicator")
    drop_episode: int | None = Field(default=None, description="Drop episode indicator")
    epi_pre_overlap: int | None = Field(default=None, description="Episode pre-overlap indicator")
    epi_post_overlap: int | None = Field(default=None, description="Episode post-overlap indicator")
    dropflag_non_ach: int | None = Field(default=None, description="Drop flag for non-ACH")
    dropflag_excluded_state: int | None = Field(
        default=None, description="Drop flag for excluded state"
    )
    dropflag_not_cont_enr_ab_no_c: int | None = Field(
        default=None, description="Drop flag for not continuously enrolled in AB without C"
    )
    dropflag_esrd: int | None = Field(default=None, description="Drop flag for ESRD")
    dropflag_other_primary_payer: int | None = Field(
        default=None, description="Drop flag for other primary payer"
    )
    dropflag_no_bene_enr_info: int | None = Field(
        default=None, description="Drop flag for no beneficiary enrollment info"
    )
    dropflag_los_gt_59: int | None = Field(
        default=None, description="Drop flag for length of stay greater than 59"
    )
    dropflag_non_highest_j1: int | None = Field(
        default=None, description="Drop flag for non-highest J1"
    )
    dropflag_death_dur_anchor: int | None = Field(
        default=None, description="Drop flag for death during anchor"
    )
    dropflag_trans_w_cah_cancer: int | None = Field(
        default=None, description="Drop flag for transfer with CAH cancer"
    )
    dropflag_rch_demo: int | None = Field(default=None, description="Drop flag for REACH demo")
    dropflag_rural_pa: int | None = Field(default=None, description="Drop flag for rural PA")
    dropflag_cjr: int | None = Field(default=None, description="Drop flag for CJR")
    dropflag_prelim_cjr_overlap: int | None = Field(
        default=None, description="Drop flag for preliminary CJR overlap"
    )
    dropflag_natural_disaster: int | None = Field(
        default=None, description="Drop flag for natural disaster"
    )
    dropflag_prelim_overlap: int | None = Field(
        default=None, description="Drop flag for preliminary overlap"
    )
    dropflag_trans_w_exc_drg_mdc: int | None = Field(
        default=None, description="Drop flag for transfer with excluded DRG/MDC"
    )
    hem_stroke_flag: int | None = Field(default=None, description="Hemorrhagic stroke flag")
    ibd_fistula_flag: int | None = Field(default=None, description="IBD fistula flag")
    ibd_uc_flag: int | None = Field(default=None, description="IBD ulcerative colitis flag")
    prior_hosp_w_non_pac_ip_flag_180: int | None = Field(
        default=None, description="Prior hospitalization with non-PAC IP flag (180 days)"
    )
    prior_pac_flag: int | None = Field(default=None, description="Prior PAC flag")
    death_dur_postdschrg: int | None = Field(
        default=None, description="Death during post-discharge flag"
    )
    tot_std_allowed: str | None = Field(default=None, description="Total standardized allowed")
    tot_raw_allowed: str | None = Field(default=None, description="Total raw allowed")
    tot_std_allowed_opl: str | None = Field(
        default=None, description="Total standardized allowed outpatient"
    )
    tot_std_allowed_ip: str | None = Field(
        default=None, description="Total standardized allowed inpatient"
    )
    tot_std_allowed_dm: str | None = Field(
        default=None, description="Total standardized allowed DME"
    )
    tot_std_allowed_pb: str | None = Field(
        default=None, description="Total standardized allowed Part B"
    )
    tot_std_allowed_sn: str | None = Field(
        default=None, description="Total standardized allowed SNF"
    )
    tot_std_allowed_hs: str | None = Field(
        default=None, description="Total standardized allowed hospice"
    )
    tot_std_allowed_hh_nonrap: str | None = Field(
        default=None, description="Total standardized allowed home health non-RAP"
    )
    winsorize_epi_1_99: int | None = Field(
        default=None, description="Winsorize episode 1-99 percentile"
    )
    epi_std_pmt_fctr_win_1_99: str | None = Field(
        default=None, description="Episode standardized payment factor winsorized 1-99"
    )
    anchor_claimno: str | None = Field(default=None, description="Anchor claim number")
    anchor_lineitem: str | None = Field(default=None, description="Anchor line item")
    anchor_stay_id: str | None = Field(default=None, description="Anchor stay ID")
    trans_ip_stay_1: str | None = Field(default=None, description="Transfer inpatient stay 1")
    trans_ip_stay_2: str | None = Field(default=None, description="Transfer inpatient stay 2")
    trans_ip_stay_3: str | None = Field(default=None, description="Transfer inpatient stay 3")
    trans_ip_stay_4: str | None = Field(default=None, description="Transfer inpatient stay 4")
    trans_ip_stay_5: str | None = Field(default=None, description="Transfer inpatient stay 5")
    trans_ip_stay_6: str | None = Field(default=None, description="Transfer inpatient stay 6")
    origds: int | None = Field(default=None, description="Original disability status")
    lti: int | None = Field(default=None, description="Long-term institutional indicator")
    any_dual: int | None = Field(default=None, description="Any dual eligible status")
    hcc1: int | None = Field(default=None, description="HCC 1")
    hcc2: int | None = Field(default=None, description="HCC 2")
    hcc6: int | None = Field(default=None, description="HCC 6")
    hcc8: int | None = Field(default=None, description="HCC 8")
    hcc9: int | None = Field(default=None, description="HCC 9")
    hcc10: int | None = Field(default=None, description="HCC 10")
    hcc11: int | None = Field(default=None, description="HCC 11")
    hcc12: int | None = Field(default=None, description="HCC 12")
    hcc17: int | None = Field(default=None, description="HCC 17")
    hcc18: int | None = Field(default=None, description="HCC 18")
    hcc19: int | None = Field(default=None, description="HCC 19")
    hcc21: int | None = Field(default=None, description="HCC 21")
    hcc22: int | None = Field(default=None, description="HCC 22")
    hcc23: int | None = Field(default=None, description="HCC 23")
    hcc27: int | None = Field(default=None, description="HCC 27")
    hcc28: int | None = Field(default=None, description="HCC 28")
    hcc29: int | None = Field(default=None, description="HCC 29")
    hcc33: int | None = Field(default=None, description="HCC 33")
    hcc34: int | None = Field(default=None, description="HCC 34")
    hcc35: int | None = Field(default=None, description="HCC 35")
    hcc39: int | None = Field(default=None, description="HCC 39")
    hcc40: int | None = Field(default=None, description="HCC 40")
    hcc46: int | None = Field(default=None, description="HCC 46")
    hcc47: int | None = Field(default=None, description="HCC 47")
    hcc48: int | None = Field(default=None, description="HCC 48")
    hcc54: int | None = Field(default=None, description="HCC 54")
    hcc55: int | None = Field(default=None, description="HCC 55")
    hcc57: int | None = Field(default=None, description="HCC 57")
    hcc58: int | None = Field(default=None, description="HCC 58")
    hcc70: int | None = Field(default=None, description="HCC 70")
    hcc71: int | None = Field(default=None, description="HCC 71")
    hcc72: int | None = Field(default=None, description="HCC 72")
    hcc73: int | None = Field(default=None, description="HCC 73")
    hcc74: int | None = Field(default=None, description="HCC 74")
    hcc75: int | None = Field(default=None, description="HCC 75")
    hcc76: int | None = Field(default=None, description="HCC 76")
    hcc77: int | None = Field(default=None, description="HCC 77")
    hcc78: int | None = Field(default=None, description="HCC 78")
    hcc79: int | None = Field(default=None, description="HCC 79")
    hcc80: int | None = Field(default=None, description="HCC 80")
    hcc82: int | None = Field(default=None, description="HCC 82")
    hcc83: int | None = Field(default=None, description="HCC 83")
    hcc84: int | None = Field(default=None, description="HCC 84")
    hcc85: int | None = Field(default=None, description="HCC 85")
    hcc86: int | None = Field(default=None, description="HCC 86")
    hcc87: int | None = Field(default=None, description="HCC 87")
    hcc88: int | None = Field(default=None, description="HCC 88")
    hcc96: int | None = Field(default=None, description="HCC 96")
    hcc99: int | None = Field(default=None, description="HCC 99")
    hcc100: int | None = Field(default=None, description="HCC 100")
    hcc103: int | None = Field(default=None, description="HCC 103")
    hcc104: int | None = Field(default=None, description="HCC 104")
    hcc106: int | None = Field(default=None, description="HCC 106")
    hcc107: int | None = Field(default=None, description="HCC 107")
    hcc108: int | None = Field(default=None, description="HCC 108")
    hcc110: int | None = Field(default=None, description="HCC 110")
    hcc111: int | None = Field(default=None, description="HCC 111")
    hcc112: int | None = Field(default=None, description="HCC 112")
    hcc114: int | None = Field(default=None, description="HCC 114")
    hcc115: int | None = Field(default=None, description="HCC 115")
    hcc122: int | None = Field(default=None, description="HCC 122")
    hcc124: int | None = Field(default=None, description="HCC 124")
    hcc134: int | None = Field(default=None, description="HCC 134")
    hcc135: int | None = Field(default=None, description="HCC 135")
    hcc136: int | None = Field(default=None, description="HCC 136")
    hcc137: int | None = Field(default=None, description="HCC 137")
    hcc157: int | None = Field(default=None, description="HCC 157")
    hcc158: int | None = Field(default=None, description="HCC 158")
    hcc161: int | None = Field(default=None, description="HCC 161")
    hcc162: int | None = Field(default=None, description="HCC 162")
    hcc166: int | None = Field(default=None, description="HCC 166")
    hcc167: int | None = Field(default=None, description="HCC 167")
    hcc169: int | None = Field(default=None, description="HCC 169")
    hcc170: int | None = Field(default=None, description="HCC 170")
    hcc173: int | None = Field(default=None, description="HCC 173")
    hcc176: int | None = Field(default=None, description="HCC 176")
    hcc186: int | None = Field(default=None, description="HCC 186")
    hcc188: int | None = Field(default=None, description="HCC 188")
    hcc189: int | None = Field(default=None, description="HCC 189")
    sepsis_card_resp_fail: int | None = Field(
        default=None, description="Sepsis cardiorespiratory failure"
    )
    cancer_immune: int | None = Field(default=None, description="Cancer immune")
    diabetes_chf: int | None = Field(default=None, description="Diabetes CHF")
    chf_copd: int | None = Field(default=None, description="CHF COPD")
    chf_renal: int | None = Field(default=None, description="CHF renal")
    copd_card_resp_fail: int | None = Field(
        default=None, description="COPD cardiorespiratory failure"
    )
    partial_hip: int | None = Field(default=None, description="Partial hip")
    partial_ka: int | None = Field(default=None, description="Partial KA")
    tka: int | None = Field(default=None, description="Total knee arthroplasty")
    ankle_reattach_other: int | None = Field(default=None, description="Ankle reattachment other")
    tha_hip_resurf: int | None = Field(
        default=None, description="Total hip arthroplasty hip resurfacing"
    )
    disabled_hcc6: int | None = Field(default=None, description="Disabled HCC 6")
    disabled_hcc34: int | None = Field(default=None, description="Disabled HCC 34")
    disabled_hcc46: int | None = Field(default=None, description="Disabled HCC 46")
    disabled_hcc54: int | None = Field(default=None, description="Disabled HCC 54")
    disabled_hcc55: int | None = Field(default=None, description="Disabled HCC 55")
    disabled_hcc110: int | None = Field(default=None, description="Disabled HCC 110")
    disabled_hcc176: int | None = Field(default=None, description="Disabled HCC 176")
