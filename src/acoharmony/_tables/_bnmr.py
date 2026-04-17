# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic ORM models for the 17 BNMR benchmark silver tables.

Each class represents one silver-tier parquet output from the BNMR
multi-sheet parser. The parser reads a single BNMR Excel workbook
(``REACH.D*.BNMR.*.xlsx``) and splits it by ``sheet_type`` into
separate tables — these models define the typed schema for each.

Metadata columns (``performance_year``, ``aco_id``, ``aco_type``,
``risk_arrangement``, etc.) are stamped onto every row by the parser's
``matrix_fields`` and ``filename_fields`` mechanisms. They're defined
once in :class:`BnmrMetadataMixin` and inherited by every table.

Usage::

    from acoharmony._tables.bnmr_benchmark import BnmrClaims, BnmrUspcc

    # Type-checked row access
    row = BnmrClaims(perf_yr="2025", clndr_yr="2025", ...)

    # Schema introspection
    BnmrClaims.schema_metadata()
"""

from __future__ import annotations

from typing import Optional

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import register_schema, with_storage


# ── Report type vocabulary ────────────────────────────────────────────────
# Cell B1 of the REPORT_PARAMETERS sheet ("ACO Parameters" row).

BNMR_REPORT_TYPES: dict[str, str] = {
    "Q1": "Quarterly Benchmark — Q1",
    "Q2": "Quarterly Benchmark — Q2",
    "Q3": "Quarterly Benchmark — Q3",
    "Q4": "Quarterly Benchmark — Q4",
    "SP": "Provisional Settlement",
    "S1": "Settlement 1",
    "S2": "Settlement 2",
    "S3": "Settlement 3",
    "P1": "Preliminary Benchmark Report",
    "PSA1": "Preliminary Settlement Addendum 1",
}
"""Known BNMR report type codes.

Quarterly (Q1–Q4) have the least claims run-out. SP is the
provisional settlement. S1/S2/S3 are successive settlement
recalculations with progressively more run-out. P1 is the
preliminary benchmark report. PSA1 is a preliminary settlement
addendum.
"""


# ── Shared metadata mixin ────────────────────────────────────────────────


@dataclass
class BnmrMetadataMixin:
    """Columns common to all BNMR silver tables."""

    report_type: Optional[str] = Field(
        default=None,
        description=(
            "Benchmark report type code — Q1-Q4 quarterly, SP provisional "
            "settlement, S1/S2/S3 settlement, P1 preliminary, PSA1 addendum"
        ),
    )
    performance_year: Optional[str] = Field(default=None, description="Performance year (from filename)")
    aco_id: Optional[str] = Field(default=None, description="ACO identifier (from filename)")
    aco_type: Optional[str] = Field(default=None, description="ACO type (High Needs / Standard / New Entrant)")
    risk_arrangement: Optional[str] = Field(default=None, description="Risk arrangement (Global / Professional)")
    payment_mechanism: Optional[str] = Field(default=None, description="Payment mechanism (PCC / APO / TCC)")
    discount: Optional[float] = Field(default=None, description="CMS shared savings discount rate")
    shared_savings_rate: Optional[float] = Field(default=None, description="Shared savings rate (0-1)")
    advanced_payment_option: Optional[str] = Field(default=None, description="Advanced payment option elected (Yes/No)")
    stop_loss_elected: Optional[str] = Field(default=None, description="Stop-loss elected (Yes/No)")
    stop_loss_type: Optional[str] = Field(default=None, description="Stop-loss type (Standard/Alternate)")
    quality_withhold: Optional[float] = Field(default=None, description="Quality withhold percentage")
    quality_score: Optional[float] = Field(default=None, description="ACO quality score")
    voluntary_aligned_benchmark: Optional[str] = Field(default=None, description="Voluntary aligned benchmark type")
    blend_percentage: Optional[float] = Field(default=None, description="Historical blend percentage")
    blend_ceiling: Optional[float] = Field(default=None, description="Blend ceiling cap")
    blend_floor: Optional[float] = Field(default=None, description="Blend floor")
    ad_retrospective_trend: Optional[float] = Field(default=None, description="A&D retrospective trend factor")
    esrd_retrospective_trend: Optional[float] = Field(default=None, description="ESRD retrospective trend factor")
    ad_completion_factor: Optional[float] = Field(default=None, description="A&D completion-incurred factor")
    esrd_completion_factor: Optional[float] = Field(default=None, description="ESRD completion-incurred factor")
    stop_loss_payout_neutrality_factor: Optional[float] = Field(default=None, description="Stop-loss payout neutrality factor")

    # Provenance
    source_filename: Optional[str] = Field(default=None, description="Source BNMR workbook filename")
    source_file: Optional[str] = Field(default=None, description="Schema name used for parsing")
    processed_at: Optional[str] = Field(default=None, description="Processing timestamp")
    file_date: Optional[str] = Field(default=None, description="Delivery file date")
    medallion_layer: Optional[str] = Field(default=None, description="Medallion tier (bronze/silver/gold)")


# ═══════════════════════════════════════════════════════════════════════════
# Report Parameters
# ═══════════════════════════════════════════════════════════════════════════


@register_schema(name="bnmr_report_parameters", version=1, tier="silver",
                 description="BNMR Report Parameters — ACO model parameters, risk corridors, HEBA percentiles, trend factors")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrReportParameters(BnmrMetadataMixin):
    """REPORT_PARAMETERS sheet: key-value ACO configuration parameters."""

    parameter_name: Optional[str] = Field(default=None, description="Parameter name/label from column A")
    value_primary: Optional[str] = Field(default=None, description="Primary value (column B)")


# ═══════════════════════════════════════════════════════════════════════════
# Financial Settlement
# ═══════════════════════════════════════════════════════════════════════════


@register_schema(name="bnmr_financial_settlement", version=1, tier="silver",
                 description="BNMR Financial Settlement — line-by-line settlement waterfall (benchmark → savings → payment)")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrFinancialSettlement(BnmrMetadataMixin):
    """FINANCIAL_SETTLEMENT sheet: settlement calculation lines."""

    line_number: Optional[str] = Field(default=None, description="Settlement line number")
    line_description: Optional[str] = Field(default=None, description="Settlement line description")
    ad_value: Optional[float] = Field(default=None, description="A&D (Aged/Disabled) amount")
    esrd_value: Optional[float] = Field(default=None, description="ESRD amount")
    total_value: Optional[float] = Field(default=None, description="Total (AD + ESRD)")
    benchmark_before_discount_ad: Optional[str] = Field(default=None, description="AD benchmark before discount")
    benchmark_before_discount_esrd: Optional[str] = Field(default=None, description="ESRD benchmark before discount")
    benchmark_all_aligned_ad: Optional[str] = Field(default=None, description="AD benchmark all aligned")
    benchmark_all_aligned_esrd: Optional[str] = Field(default=None, description="ESRD benchmark all aligned")
    benchmark_all_aligned_total: Optional[str] = Field(default=None, description="Total benchmark all aligned")
    benchmark_after_heba_total: Optional[str] = Field(default=None, description="Benchmark after HEBA adjustment")
    total_cost_before_stoploss_total: Optional[str] = Field(default=None, description="Total cost before stop-loss")
    total_cost_after_stoploss_total: Optional[str] = Field(default=None, description="Total cost after stop-loss")
    total_cost_with_ibnr_total: Optional[str] = Field(default=None, description="Total cost with IBNR")
    gross_savings_losses_total: Optional[str] = Field(default=None, description="Gross savings/losses")
    total_monies_owed: Optional[str] = Field(default=None, description="Total monies owed to/from ACO")


# ═══════════════════════════════════════════════════════════════════════════
# Claims
# ═══════════════════════════════════════════════════════════════════════════


@register_schema(name="bnmr_claims", version=1, tier="silver",
                 description="BNMR Claims — aggregate claim payments by type, month, and benchmark segment")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrClaims(BnmrMetadataMixin):
    """DATA_CLAIMS sheet: claims aggregate at monthly × claim-type grain."""

    perf_yr: Optional[str] = Field(default=None, description="Performance year")
    clndr_yr: Optional[str] = Field(default=None, description="Calendar year")
    clndr_mnth: Optional[str] = Field(default=None, description="Calendar month")
    bnmrk: Optional[str] = Field(default=None, description="Benchmark segment (AD/ESRD)")
    align_type: Optional[str] = Field(default=None, description="Alignment type (C=Claims, V=Voluntary)")
    bnmrk_type: Optional[str] = Field(default=None, description="Benchmark type (RATEBOOK/BLEND)")
    clm_type_cd: Optional[str] = Field(default=None, description="Claim type code (10/20/30/40/50/60/71/72/81/82)")
    clm_pmt_amt_agg: Optional[float] = Field(default=None, description="Gross claim payment amount")
    sqstr_amt_agg: Optional[float] = Field(default=None, description="Sequestration reduction")
    apa_rdctn_amt_agg: Optional[float] = Field(default=None, description="APA reduction")
    ucc_amt_agg: Optional[float] = Field(default=None, description="Uncompensated care")
    op_dsh_amt_agg: Optional[float] = Field(default=None, description="Outpatient DSH")
    cp_dsh_amt_agg: Optional[float] = Field(default=None, description="Capital DSH")
    op_ime_amt_agg: Optional[float] = Field(default=None, description="Outpatient IME")
    cp_ime_amt_agg: Optional[float] = Field(default=None, description="Capital IME")
    aco_amt_agg_apa: Optional[float] = Field(default=None, description="ACO APA adjustment (additive)")
    srvc_month: Optional[str] = Field(default=None, description="Service month")
    efctv_month: Optional[str] = Field(default=None, description="Effective month")
    apa_cd: Optional[str] = Field(default=None, description="APA code")
    nonpbp_rdct_amt_agg: Optional[float] = Field(default=None, description="Non-PBP reduction")


# ═══════════════════════════════════════════════════════════════════════════
# Risk
# ═══════════════════════════════════════════════════════════════════════════


@register_schema(name="bnmr_risk", version=1, tier="silver",
                 description="BNMR Risk — beneficiary counts and risk scores at monthly grain")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrRisk(BnmrMetadataMixin):
    """DATA_RISK sheet: risk scores and bene counts by month × alignment."""

    perf_yr: Optional[str] = Field(default=None, description="Performance year")
    clndr_yr: Optional[str] = Field(default=None, description="Calendar year")
    clndr_mnth: Optional[str] = Field(default=None, description="Calendar month")
    bnmrk: Optional[str] = Field(default=None, description="Benchmark segment")
    align_type: Optional[str] = Field(default=None, description="Alignment type")
    bnmrk_type: Optional[str] = Field(default=None, description="Benchmark type")
    va_cat: Optional[str] = Field(default=None, description="Voluntary alignment category (N/C)")
    bene_dcnt: Optional[int] = Field(default=None, description="Monthly beneficiary count")
    elig_mnths: Optional[int] = Field(default=None, description="Eligible months")
    raw_risk_score: Optional[float] = Field(default=None, description="Raw (unnormalized) risk score")
    norm_risk_score: Optional[float] = Field(default=None, description="Normalized risk score")
    risk_denom: Optional[float] = Field(default=None, description="Risk score denominator")
    score_type: Optional[str] = Field(default=None, description="Score type identifier")
    bene_dcnt_annual: Optional[int] = Field(default=None, description="Annual distinct beneficiary count")


# ═══════════════════════════════════════════════════════════════════════════
# County
# ═══════════════════════════════════════════════════════════════════════════


@register_schema(name="bnmr_county", version=1, tier="silver",
                 description="BNMR County — per-county benchmark allocation rates")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrCounty(BnmrMetadataMixin):
    """DATA_COUNTY sheet: county-level per-capita benchmark rates."""

    perf_yr: Optional[str] = Field(default=None, description="Performance year")
    clndr_yr: Optional[str] = Field(default=None, description="Calendar year")
    bnmrk: Optional[str] = Field(default=None, description="Benchmark segment")
    align_type: Optional[str] = Field(default=None, description="Alignment type")
    bnmrk_type: Optional[str] = Field(default=None, description="Benchmark type")
    bene_dcnt: Optional[int] = Field(default=None, description="Beneficiary count in county")
    elig_mnths: Optional[int] = Field(default=None, description="Eligible beneficiary-months")
    cty_accrl_cd: Optional[str] = Field(default=None, description="5-digit CMS county accrual code (FIPS-aligned)")
    cty_rate: Optional[float] = Field(default=None, description="Per-capita benchmark rate ($/month)")
    adj_cty_pmt: Optional[float] = Field(default=None, description="Adjusted county payment (= cty_rate × elig_mnths)")
    gaf_trend: Optional[float] = Field(default=None, description="Geographic adjustment factor trend")
    adj_gaf_trend: Optional[float] = Field(default=None, description="Adjusted GAF trend (= gaf_trend × elig_mnths)")


# ═══════════════════════════════════════════════════════════════════════════
# USPCC
# ═══════════════════════════════════════════════════════════════════════════


@register_schema(name="bnmr_uspcc", version=1, tier="silver",
                 description="BNMR USPCC — US Per Capita Cost national benchmarks")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrUspcc(BnmrMetadataMixin):
    """DATA_USPCC sheet: CMS-published national per-capita costs."""

    perf_yr: Optional[str] = Field(default=None, description="Performance year")
    clndr_yr: Optional[str] = Field(default=None, description="Calendar year")
    bnmrk: Optional[str] = Field(default=None, description="Benchmark segment (AD/ESRD)")
    uspcc: Optional[float] = Field(default=None, description="US Per Capita Cost ($/month)")
    ucc_hosp_adj: Optional[float] = Field(default=None, description="Hospital UCC adjustment (can be negative)")
    adj_ffs_uspcc: Optional[float] = Field(default=None, description="Adjusted FFS USPCC (= uspcc + ucc_hosp_adj)")


# ═══════════════════════════════════════════════════════════════════════════
# HEBA
# ═══════════════════════════════════════════════════════════════════════════


@register_schema(name="bnmr_heba", version=1, tier="silver",
                 description="BNMR HEBA — Health Equity Benchmark Adjustment")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrHeba(BnmrMetadataMixin):
    """DATA_HEBA sheet: health equity benchmark adjustments."""

    perf_yr: Optional[str] = Field(default=None, description="Performance year")
    heba_up_mnths: Optional[int] = Field(default=None, description="Upward-adjusted eligible months")
    heba_down_mnths: Optional[int] = Field(default=None, description="Downward-adjusted eligible months")
    heba_up_amt: Optional[float] = Field(default=None, description="Upward HEBA dollar adjustment")
    heba_down_amt: Optional[float] = Field(default=None, description="Downward HEBA dollar adjustment")


# ═══════════════════════════════════════════════════════════════════════════
# Capitation
# ═══════════════════════════════════════════════════════════════════════════


@register_schema(name="bnmr_cap", version=1, tier="silver",
                 description="BNMR Cap — capitation payment totals by payment month")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrCap(BnmrMetadataMixin):
    """DATA_CAP sheet: capitation amounts (old-format or new-format)."""

    perf_yr: Optional[str] = Field(default=None, description="Performance year")
    bnmrk: Optional[str] = Field(default=None, description="Benchmark segment")
    align_type: Optional[str] = Field(default=None, description="Alignment type")
    pmt_mnth: Optional[str] = Field(default=None, description="Payment month (YYYY-MM)")

    # Old format (pre-April 2025)
    aco_bpcc_amt_total: Optional[float] = Field(default=None, description="Base PCC total (old format)")
    aco_epcc_amt_total_seq: Optional[float] = Field(default=None, description="Enhanced PCC total after sequestration (old)")

    # New format (April 2025+): three variants per metric
    aco_bpcc_amt_pre_seq_actual: Optional[float] = Field(default=None, description="Base PCC pre-sequestration actual (new)")
    aco_bpcc_amt_post_seq_actual: Optional[float] = Field(default=None, description="Base PCC post-sequestration actual (new)")
    aco_bpcc_amt_post_seq_paid: Optional[float] = Field(default=None, description="Base PCC post-sequestration paid (new)")
    aco_tcc_amt_post_seq_paid: Optional[float] = Field(default=None, description="Total capitation paid (new)")
    aco_epcc_amt_post_seq_paid: Optional[float] = Field(default=None, description="Enhanced PCC paid (new)")
    aco_apo_amt_post_seq_paid: Optional[float] = Field(default=None, description="APO paid (new)")


# ═══════════════════════════════════════════════════════════════════════════
# Risk Score Normalization (AD and ESRD)
# ═══════════════════════════════════════════════════════════════════════════


@register_schema(name="bnmr_riskscore_ad", version=1, tier="silver",
                 description="BNMR Riskscore AD — A&D risk score normalization and capping worksheet")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrRiskscoreAd(BnmrMetadataMixin):
    """RISKSCORE_AD sheet: normalization chain for Aged/Disabled population."""

    line_number: Optional[str] = Field(default=None, description="Calculation line number")
    line_description: Optional[str] = Field(default=None, description="Calculation step description")
    reference_year_value: Optional[float] = Field(default=None, description="Reference year value")
    py_value: Optional[float] = Field(default=None, description="Performance year value")

    # Named fields (extracted from specific cells)
    normalized_risk_score_claims_py: Optional[str] = Field(default=None, description="Normalized risk score — claims aligned")
    capped_risk_score_claims_py: Optional[str] = Field(default=None, description="Capped risk score — claims aligned")
    benchmark_risk_score_claims_py: Optional[str] = Field(default=None, description="Benchmark risk score — claims aligned")
    normalized_risk_score_vol_new_py: Optional[str] = Field(default=None, description="Normalized — newly voluntary")
    normalized_risk_score_vol_cont_py: Optional[str] = Field(default=None, description="Normalized — continuously voluntary")
    benchmark_risk_score_vol_cont_py: Optional[str] = Field(default=None, description="Benchmark — continuously voluntary")
    weighted_avg_vol_risk_score: Optional[str] = Field(default=None, description="Weighted average voluntary risk score")


@register_schema(name="bnmr_riskscore_esrd", version=1, tier="silver",
                 description="BNMR Riskscore ESRD — ESRD risk score normalization and capping worksheet")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrRiskscoreEsrd(BnmrMetadataMixin):
    """RISKSCORE_ESRD sheet: normalization chain for ESRD population."""

    line_number: Optional[str] = Field(default=None, description="Calculation line number")
    line_description: Optional[str] = Field(default=None, description="Calculation step description")
    reference_year_value: Optional[float] = Field(default=None, description="Reference year value")
    py_value: Optional[float] = Field(default=None, description="Performance year value")
    normalized_risk_score_claims_py: Optional[str] = Field(default=None, description="Normalized — claims aligned")
    capped_risk_score_claims_py: Optional[str] = Field(default=None, description="Capped — claims aligned")
    benchmark_risk_score_claims_py: Optional[str] = Field(default=None, description="Benchmark — claims aligned")
    normalized_risk_score_vol_new_py: Optional[str] = Field(default=None, description="Normalized — newly voluntary")
    normalized_risk_score_vol_cont_py: Optional[str] = Field(default=None, description="Normalized — continuously voluntary")
    benchmark_risk_score_vol_cont_py: Optional[str] = Field(default=None, description="Benchmark — continuously voluntary")
    weighted_avg_vol_risk_score: Optional[str] = Field(default=None, description="Weighted average voluntary risk score")


# ═══════════════════════════════════════════════════════════════════════════
# Benchmark Historical (AD and ESRD)
# ═══════════════════════════════════════════════════════════════════════════


@register_schema(name="bnmr_benchmark_historical_ad", version=1, tier="silver",
                 description="BNMR Benchmark Historical AD — blended historical benchmark for A&D population")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrBenchmarkHistoricalAd(BnmrMetadataMixin):
    """BENCHMARK_HISTORICAL_AD sheet: 3-year claims/vol history for blended AD benchmark.

    Base year columns (BY1, BY2, BY3) are dynamic — the parser's
    ``dynamic_columns`` mechanism renames them to ``year_YYYY`` based on
    the actual calendar years in the workbook header row. For example,
    a PY2025 delivery might have BY1=2021, BY2=2022, BY3=2023.
    """

    line_number: Optional[str] = Field(default=None, description="Line number")
    line_description: Optional[str] = Field(default=None, description="Line description")

    # Base year values (dynamic — renamed at parse time from positional columns)
    # BY1, BY2, BY3 are the 3-year lookback; additional years may appear
    # depending on PY and CMS delivery format.
    by1_value: Optional[str] = Field(default=None, description="Base year 1 value (earliest lookback year)")
    by2_value: Optional[str] = Field(default=None, description="Base year 2 value")
    by3_value: Optional[str] = Field(default=None, description="Base year 3 value (most recent lookback year)")

    # Benchmark calculation outputs
    claims_benchmark: Optional[float] = Field(default=None, description="Claims-aligned benchmark value")
    vol_benchmark: Optional[float] = Field(default=None, description="Voluntary-aligned benchmark value")
    pbpm_historical_rate_claims_benchmark: Optional[str] = Field(default=None, description="PBPM historical rate — claims benchmark")
    pbpm_historical_rate_vol_benchmark: Optional[str] = Field(default=None, description="PBPM historical rate — voluntary benchmark")
    aco_regional_rate_claims: Optional[str] = Field(default=None, description="ACO regional rate — claims")
    aco_regional_rate_vol: Optional[str] = Field(default=None, description="ACO regional rate — voluntary")
    blended_benchmark_claims: Optional[str] = Field(default=None, description="Blended benchmark — claims")
    blended_benchmark_vol: Optional[str] = Field(default=None, description="Blended benchmark — voluntary")


@register_schema(name="bnmr_benchmark_historical_esrd", version=1, tier="silver",
                 description="BNMR Benchmark Historical ESRD — blended historical benchmark for ESRD population")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrBenchmarkHistoricalEsrd(BnmrMetadataMixin):
    """BENCHMARK_HISTORICAL_ESRD sheet: 3-year claims/vol history for blended ESRD benchmark.

    Same BY1/BY2/BY3 dynamic-year pattern as the AD variant.
    """

    line_number: Optional[str] = Field(default=None, description="Line number")
    line_description: Optional[str] = Field(default=None, description="Line description")
    by1_value: Optional[str] = Field(default=None, description="Base year 1 value")
    by2_value: Optional[str] = Field(default=None, description="Base year 2 value")
    by3_value: Optional[str] = Field(default=None, description="Base year 3 value")
    claims_benchmark: Optional[float] = Field(default=None, description="Claims-aligned benchmark value")
    vol_benchmark: Optional[float] = Field(default=None, description="Voluntary-aligned benchmark value")
    pbpm_historical_rate_claims_benchmark: Optional[str] = Field(default=None, description="PBPM historical rate — claims")
    pbpm_historical_rate_vol_benchmark: Optional[str] = Field(default=None, description="PBPM historical rate — voluntary")
    aco_regional_rate_claims: Optional[str] = Field(default=None, description="ACO regional rate — claims")
    aco_regional_rate_vol: Optional[str] = Field(default=None, description="ACO regional rate — voluntary")
    blended_benchmark_claims: Optional[str] = Field(default=None, description="Blended benchmark — claims")
    blended_benchmark_vol: Optional[str] = Field(default=None, description="Blended benchmark — voluntary")


# ═══════════════════════════════════════════════════════════════════════════
# Stop-Loss
# ═══════════════════════════════════════════════════════════════════════════


@register_schema(name="bnmr_stop_loss_charge", version=1, tier="silver",
                 description="BNMR Stop-Loss Charge — ACO stop-loss charge calculation worksheet")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrStopLossCharge(BnmrMetadataMixin):
    """STOP_LOSS_CHARGE sheet: line-by-line stop-loss charge calculation."""

    line_number: Optional[str] = Field(default=None, description="Charge line number")
    line_description: Optional[str] = Field(default=None, description="Charge line description")
    value: Optional[float] = Field(default=None, description="Charge value")


@register_schema(name="bnmr_stop_loss_payout", version=1, tier="silver",
                 description="BNMR Stop-Loss Payout — ACO stop-loss payout calculation worksheet")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrStopLossPayout(BnmrMetadataMixin):
    """STOP_LOSS_PAYOUT sheet: payout summary with neutrality factor."""

    line_number: Optional[str] = Field(default=None, description="Payout line number")
    line_description: Optional[str] = Field(default=None, description="Payout line description")
    value: Optional[float] = Field(default=None, description="Payout value")
    total_beneficiaries: Optional[str] = Field(default=None, description="Total beneficiaries subject to stop-loss")
    total_expenditures: Optional[str] = Field(default=None, description="Total expenditures")
    total_stop_loss_payouts: Optional[str] = Field(default=None, description="Total stop-loss payouts")
    total_payout_rate: Optional[str] = Field(default=None, description="Total payout rate")
    stop_loss_neutrality_factor: Optional[str] = Field(default=None, description="Stop-loss neutrality factor")
    adjusted_aggregate_stoploss_payout: Optional[str] = Field(default=None, description="Adjusted aggregate stop-loss payout")


@register_schema(name="bnmr_stop_loss_county", version=1, tier="silver",
                 description="BNMR Stop-Loss County — county-level stop-loss parameters")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrStopLossCounty(BnmrMetadataMixin):
    """STOP_LOSS_COUNTY sheet: per-county stop-loss allocation rates."""

    perf_yr: Optional[str] = Field(default=None, description="Performance year")
    clndr_yr: Optional[str] = Field(default=None, description="Calendar year")
    bnmrk: Optional[str] = Field(default=None, description="Benchmark segment")
    bene_dcnt: Optional[int] = Field(default=None, description="Beneficiary count")
    elig_mnths: Optional[int] = Field(default=None, description="Eligible months")
    cty_accrl_cd: Optional[str] = Field(default=None, description="County accrual code")
    gaf_trend: Optional[float] = Field(default=None, description="GAF trend")
    adj_gaf_trend: Optional[float] = Field(default=None, description="Adjusted GAF trend (= gaf_trend × elig_mnths)")
    avg_payout_pct: Optional[float] = Field(default=None, description="Average stop-loss payout percentage")
    ad_ry_avg_pbpm: Optional[float] = Field(default=None, description="AD reference year average PBPM")
    esrd_ry_avg_pbpm: Optional[float] = Field(default=None, description="ESRD reference year average PBPM")
    adj_avg_payout_pct: Optional[float] = Field(default=None, description="Adjusted payout pct (= avg × elig_mnths)")
    adj_ad_ry_avg_pbpm: Optional[float] = Field(default=None, description="Adjusted AD PBPM (= base × elig_mnths)")
    adj_esrd_ry_avg_pbpm: Optional[float] = Field(default=None, description="Adjusted ESRD PBPM (= base × elig_mnths)")


@register_schema(name="bnmr_stop_loss_claims", version=1, tier="silver",
                 description="BNMR Stop-Loss Claims — claims-aligned stop-loss data")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrStopLossClaims(BnmrMetadataMixin):
    """DATA_STOP_LOSS_CLAIMS sheet."""

    perf_yr: Optional[str] = Field(default=None, description="Performance year")


@register_schema(name="bnmr_data_stop_loss_payout", version=1, tier="silver",
                 description="BNMR Data Stop-Loss Payout — bucketed stop-loss exposure and payouts")
@with_storage(tier="silver", medallion_layer="silver")
@dataclass
class BnmrDataStopLossPayout(BnmrMetadataMixin):
    """DATA_STOP_LOSS_PAYOUT sheet: binned exposure by threshold bracket."""

    perf_yr: Optional[str] = Field(default=None, description="Performance year")
    clndr_yr: Optional[str] = Field(default=None, description="Calendar year")
    algn_aco_amt_agg: Optional[float] = Field(default=None, description="Aligned ACO aggregate amount")
    aco_stoploss_exp: Optional[float] = Field(default=None, description="Total stop-loss exposure")
    aco_stoploss_exp_b0: Optional[float] = Field(default=None, description="Stop-loss exposure bucket 0")
    aco_stoploss_exp_b1: Optional[float] = Field(default=None, description="Stop-loss exposure bucket 1")
    aco_stoploss_exp_b2: Optional[float] = Field(default=None, description="Stop-loss exposure bucket 2")
    aco_stoploss_payout_b0: Optional[float] = Field(default=None, description="Stop-loss payout bucket 0")
    aco_stoploss_payout_b1: Optional[float] = Field(default=None, description="Stop-loss payout bucket 1")
    aco_stoploss_payout_b2: Optional[float] = Field(default=None, description="Stop-loss payout bucket 2")
    aco_stoploss_payout_total: Optional[float] = Field(default=None, description="Total stop-loss payout")
    bene_cnt_b0: Optional[int] = Field(default=None, description="Beneficiary count bucket 0")
    bene_cnt_b1: Optional[int] = Field(default=None, description="Beneficiary count bucket 1")
    bene_cnt_b2: Optional[int] = Field(default=None, description="Beneficiary count bucket 2")
