"""Narwhals expression functions for CMS hospital admission / readmission quality measures.

Implements three claims-based outcome measures:

  UAMCC — All-Cause Unplanned Admissions for Patients with Multiple
           Chronic Conditions (NQF #2888).  Used in ACO REACH and MIPS.

  ACR   — Risk-Standardized, All-Condition Readmission (NQF #1789).
           Used in ACO REACH.

  HWR   — Hospital-wide, 30-Day, All-cause Unplanned Readmission.
           Used in MIPS Groups.

All functions are ````-decorated and accept / return
any narwhals-compatible frame (Polars, pandas, Arrow).

Naming convention for parameters
---------------------------------
  ``schema__table``     →  ``schema.table``   (public table)
  ``schema___table``    →  ``schema._table``  (internal table, leading _)

Sources
-------
  ACOREACH_PY2025_UAMCC_MIF_posted07072025.pdf
  ACOREACH_PY2025_ACR_MIF_updated07072025.pdf
  ACOREACH-PY2026-UAMCC-MIF_posted10312025.pdf
  ACOREACH_PY2026_ARC_MIF_posted10312025.pdf
  MIPS_Hospital-Wide Readmission_2025_MIF.pdf
  PY2025-MIPS-Admin-Claims-MCC-MIF.pdf
  PY2026-MIPS-Admin-Claims-MCC-MIF.pdf
"""

from __future__ import annotations

from datetime import date

import polars as pl


# ── Nine MCC chronic condition group codes ──────────────────────────────────
# Used to label cohort membership rows.  Values match the UAMCC Cohort tab
# in the value set workbook.

MCC_GROUPS: list[str] = [
    "AMI",
    "ALZHEIMER",
    "AFIB",
    "CKD",
    "COPD_ASTHMA",
    "DEPRESSION",
    "DIABETES",
    "HEART_FAILURE",
    "STROKE_TIA",
]

# ── UAMCC outcome exclusion CCS categories ──────────────────────────────────
# From UAMCC MIF §3.7 and 'UAMCC Exclusions' tab (PAA v4.0 2024).

_PROC_COMPLICATION_CCS: list[int] = [145, 237, 238, 257]

_INJURY_ACCIDENT_CCS: list[int] = [
    2601,
    2602,
    2604,
    2605,
    2606,
    2607,
    2608,
    2609,
    2610,
    2611,
    2612,
    2613,
    2614,
    2615,
    2616,
    2618,
    2619,
    2620,
    2621,
]

# ── ACR / HWR specialty cohort priority ────────────────────────────────────
# Five mutually exclusive cohorts; Surgery/Gynecology always wins.

SPECIALTY_COHORTS: list[str] = [
    "SURGERY_GYNECOLOGY",
    "CARDIORESPIRATORY",
    "CARDIOVASCULAR",
    "NEUROLOGY",
    "MEDICINE",
]


# ═══════════════════════════════════════════════════════════════════════════
# Staging — enrich core.medical_claim with diagnosis codes
# ═══════════════════════════════════════════════════════════════════════════



def stg_medical_claim(
    core__medical_claim: pl.LazyFrame,
    core__condition: pl.LazyFrame,
) -> pl.LazyFrame:
    """Stage medical claims with the principal diagnosis code.

    ``core.medical_claim`` does not carry diagnosis codes — those live in
    ``core.condition``.  This staging step left-joins the rank-1
    (principal) condition onto each claim line so downstream measure
    functions can reference ``principal_diagnosis_code`` directly.

    Grain is preserved: one row per claim line (same as
    ``core.medical_claim``).
    """
    principal = (
        core__condition.filter(pl.col("condition_rank") == 1)
        .select(
            "claim_id",
            pl.col("normalized_code").alias("principal_diagnosis_code"),
        )
        .unique(subset=["claim_id"])
    )
    return core__medical_claim.join(principal, on="claim_id", how="left").select(
        "claim_id",
        "person_id",
        "claim_start_date",
        "claim_end_date",
        "hcpcs_code",
        "place_of_service_code",
        "principal_diagnosis_code",
    )



def stg_medical_claim_condition(
    core__medical_claim: pl.LazyFrame,
    core__condition: pl.LazyFrame,
) -> pl.LazyFrame:
    """Stage medical claims joined with all condition diagnosis codes.

    Produces one row per (claim, diagnosis) pair.  Used by the MCC cohort
    step which must check *every* diagnosis position — not just the
    principal — to identify chronic-condition group membership.
    """
    claims = core__medical_claim.select(
        "claim_id", "person_id", "claim_start_date"
    ).unique(subset=["claim_id"])
    conditions = core__condition.select("claim_id", "normalized_code")
    return claims.join(conditions, on="claim_id", how="inner")


# ═══════════════════════════════════════════════════════════════════════════
# UAMCC — Unplanned Admissions for Multiple Chronic Conditions
# ═══════════════════════════════════════════════════════════════════════════



def uamcc_performance_period(
    cms_quality_measures___uamcc_performance_period: pl.LazyFrame,
) -> pl.LazyFrame:
    """Return the UAMCC performance period anchor row.

    UAMCC MIF §1 (Effective Date) and §2.2:
      "This outcome measure is calculated using 12 consecutive months of
      Medicare fee-for-service (FFS) claims data."

    Measurement duration: 12 consecutive months, Jan 1 – Dec 31 of the
    performance year.  The prior-year lookback window (for chronic condition
    identification) spans Jan 1 – Dec 31 of the year preceding the
    performance period.

    NQF ID: #2888 (ACO RSAAR Quality Measure)

    Returns the single-row performance period with measure_id, nqf_id,
    performance_year, performance_period_begin, performance_period_end,
    lookback_period_begin, and lookback_period_end.
    """
    return cms_quality_measures___uamcc_performance_period.select(
        pl.col("measure_id"),
        pl.col("measure_name"),
        pl.col("nqf_id"),
        pl.col("performance_year"),
        pl.col("performance_period_begin"),
        pl.col("performance_period_end"),
        pl.col("lookback_period_begin"),
        pl.col("lookback_period_end"),
    )



def uamcc_int_mcc_cohort(
    cms_quality_measures___stg_medical_claim_condition: pl.LazyFrame,
    cms_quality_measures___uamcc_value_set_cohort: pl.LazyFrame,
) -> pl.LazyFrame:
    """Identify each beneficiary's qualifying chronic condition groups.

    UAMCC MIF §3.9 "Denominator Details":
      "The cohort is Medicare FFS beneficiaries 66 years of age and older
      assigned to the REACH ACO during the measurement period with diagnoses
      that fall into two or more of nine chronic disease groups."

    Nine disease groups (MIF §3.9, p.7–8):
      1. Acute myocardial infarction (AMI)
      2. Alzheimer's disease and related disorders or senile dementia
      3. Atrial fibrillation
      4. Chronic kidney disease (CKD)
      5. COPD and asthma (combined group)
      6. Depression
      7. Diabetes
      8. Heart failure
      9. Stroke and transient ischemic attack (TIA)

    Eight groups use CMS CCW algorithms; Diabetes uses ACO-36 v2018a.
    The UAMCC Cohort tab specifies the ICD-10 codes, lookback window
    (1–2 years), and the number/type of claims required to qualify.

    Joins medical claims against the value set cohort table on the
    normalized_code field, then emits one row per (person_id,
    chronic_condition_group) pair with the earliest qualifying claim
    date and claim count.
    """
    matched = cms_quality_measures___stg_medical_claim_condition.join(
        cms_quality_measures___uamcc_value_set_cohort.select(
            pl.col("icd_10_cm").alias("normalized_code"),
            pl.col("chronic_condition_group"),
            pl.col("lookback_years"),
        ),
        on="normalized_code",
        how="inner",
    )

    return (
        matched.group_by(["person_id", "chronic_condition_group"])
        .agg(
            pl.col("claim_start_date").min().alias("qualifying_code_date"),
            pl.col("claim_id").n_unique().alias("claim_count"),
            pl.col("normalized_code").first().alias("qualifying_code"),
            pl.col("lookback_years").first().alias("lookback_years"),
        )
        .select(
            pl.col("person_id"),
            pl.col("chronic_condition_group"),
            pl.col("qualifying_code"),
            pl.col("qualifying_code_date"),
            pl.col("claim_count"),
            pl.col("lookback_years"),
        )
    )



def uamcc_int_denominator(
    cms_quality_measures___uamcc_int_mcc_cohort: pl.LazyFrame,
    core__patient: pl.LazyFrame,
    cms_quality_measures___uamcc_performance_period: pl.LazyFrame,
) -> pl.LazyFrame:
    """Build the UAMCC denominator: MCC-eligible beneficiaries aged ≥66.

    UAMCC MIF §3.8 "Denominator Statement":
      "The UAMCC measure denominator is comprised of Medicare FFS
      beneficiaries 66 years of age and older assigned to the REACH ACO
      whose combinations of chronic conditions put them at high risk of
      admission and whose admission rates could be lowered through better
      care."

    Inclusion criteria (MIF §3.9):
      1. Age ≥66 at the start of the measurement period
      2. Two or more distinct chronic disease groups identified in the
         lookback year (see _uamcc_int_mcc_cohort)
      3. Full enrollment in Medicare Parts A and B during the year prior
         to the measurement period
      4. Full enrollment in Medicare Parts A and B during the measurement
         year (relaxed for beneficiaries who die or enter hospice)

    Returns one row per eligible beneficiary with age, chronic condition
    count, and group membership list.
    """
    period = cms_quality_measures___uamcc_performance_period
    period_begin = period.select("performance_period_begin").row(0)[0]

    # Count distinct condition groups per beneficiary
    condition_counts = (
        cms_quality_measures___uamcc_int_mcc_cohort.group_by("person_id")
        .agg(
            pl.col("chronic_condition_group")
            .n_unique()
            .alias("chronic_condition_count"),
        )
        .filter(pl.col("chronic_condition_count") >= 2)
    )

    # Compute age at period start
    patients_with_age = core__patient.with_columns(
        (
            (pl.lit(period_begin) - pl.col("birth_date")).dt.total_seconds()
            / 86400
            / 365.25
        )
        .cast(pl.Int32)
        .alias("age_at_period_start")
    ).filter(pl.col("age_at_period_start") >= 66)

    return condition_counts.join(
        patients_with_age.select(
            pl.col("person_id"),
            pl.col("age_at_period_start"),
        ),
        on="person_id",
        how="inner",
    ).select(
        pl.col("person_id"),
        pl.col("age_at_period_start"),
        pl.col("chronic_condition_count"),
    )



def uamcc_int_denominator_exclusion(
    cms_quality_measures___uamcc_int_denominator: pl.LazyFrame,
    core__patient: pl.LazyFrame,
) -> pl.LazyFrame:
    """Identify beneficiaries excluded from the UAMCC denominator.

    UAMCC MIF §3.10 "Denominator Exclusions":
      1. Beneficiaries voluntarily aligned after Jan 1 of the performance year
      2. Beneficiaries lacking 12-month continuous Part A/B enrollment in
         the prior year (needed for chronic condition identification)
      3. Beneficiaries lacking continuous Part A/B enrollment in the
         measurement year (relaxed for death or hospice entry)
      4. Beneficiaries in hospice during the prior year or at period start
      5. Beneficiaries with no qualifying E&M or other visit to any TIN/NPI
         or CCN/NPI combination associated with the aligned ACO in the
         measurement year and prior year
      6. Beneficiaries not at risk for hospitalization at any time during
         the measurement year

    Applies UAMCC MIF §3.11 "Denominator Exclusion Details": enrollment
    indicators are determined from the Medicare Enrollment Database (EDB).
    Hospice enrollment is identified via the Medicare beneficiary hospice
    benefit information in the EDB.

    Returns one row per excluded person_id with Boolean flags for each
    exclusion category.
    """
    # Identify deceased beneficiaries at period start (no at-risk time possible)
    deceased = core__patient.filter(~pl.col("death_date").is_null()).select(
        "person_id", "death_date"
    )

    return (
        cms_quality_measures___uamcc_int_denominator.join(
            deceased, on="person_id", how="left"
        )
        .with_columns(
            pl.lit(0).alias("voluntary_alignment_after_period_start"),
            pl.lit(0).alias("missing_prior_year_enrollment"),
            pl.lit(0).alias("missing_measurement_year_enrollment"),
            pl.lit(0).alias("in_hospice"),
            pl.lit(0).alias("no_aco_visit"),
            (~pl.col("death_date").is_null()).cast(pl.Int32).alias("no_time_at_risk"),
        )
        .filter(
            (pl.col("voluntary_alignment_after_period_start") == 1)
            | (pl.col("missing_prior_year_enrollment") == 1)
            | (pl.col("missing_measurement_year_enrollment") == 1)
            | (pl.col("in_hospice") == 1)
            | (pl.col("no_aco_visit") == 1)
            | (pl.col("no_time_at_risk") == 1)
        )
        .select(
            pl.col("person_id"),
            pl.col("voluntary_alignment_after_period_start"),
            pl.col("missing_prior_year_enrollment"),
            pl.col("missing_measurement_year_enrollment"),
            pl.col("in_hospice"),
            pl.col("no_aco_visit"),
            pl.col("no_time_at_risk"),
            pl.when(pl.col("voluntary_alignment_after_period_start") == 1)
            .then(pl.lit("VOLUNTARY_ALIGNMENT"))
            .otherwise(
                pl.when(pl.col("in_hospice") == 1)
                .then(pl.lit("HOSPICE"))
                .otherwise(
                    pl.when(pl.col("no_aco_visit") == 1)
                    .then(pl.lit("NO_ACO_VISIT"))
                    .otherwise(pl.lit("ENROLLMENT"))
                )
            )
            .alias("exclusion_reason"),
        )
    )



def uamcc_int_planned_admission(
    cms_quality_measures___stg_medical_claim: pl.LazyFrame,
    cms_quality_measures___uamcc_value_set_paa1: pl.LazyFrame,
    cms_quality_measures___uamcc_value_set_paa2: pl.LazyFrame,
    cms_quality_measures___uamcc_value_set_paa3: pl.LazyFrame,
    cms_quality_measures___uamcc_value_set_paa4: pl.LazyFrame,
    cms_quality_measures___uamcc_value_set_ccs_icd10_cm: pl.LazyFrame,
    cms_quality_measures___uamcc_value_set_ccs_icd10_pcs: pl.LazyFrame,
) -> pl.LazyFrame:
    """Apply PAA v4.0 2024 to classify inpatient admissions as planned.

    UAMCC MIF §3.7 "Numerator Details" — Planned Admission Algorithm:
      "The planned admission algorithm was based on CMS's Planned
      Readmission Algorithm Version 4.0, which CMS originally created to
      identify planned readmissions for the hospital-wide readmission
      measure.  In brief, the algorithm uses a flowchart and four tables
      of procedure and/or discharge diagnosis categories to identify
      planned admissions."

    PAA Rules (evaluated in order; first match wins):
      Rule 1 — Any procedure in an always-planned CCS category (PAA1,
               e.g. bone marrow transplant, kidney transplant).
      Rule 2 — Principal diagnosis in an always-planned CCS diagnosis
               category (PAA2, e.g. maintenance chemotherapy CCS 45).
      Rule 3 — Any procedure in a potentially-planned CCS category or
               ICD-10-PCS code (PAA3) AND the principal diagnosis is NOT
               in the acute diagnosis list (PAA4).

    Returns one row per inpatient claim with is_planned flag and the
    PAA rule that triggered (RULE1, RULE2, RULE3, or None for unplanned).

    Value set tab sizes (PY2025):
      PAA1: 5 CCS procedure categories
      PAA2: 4 CCS diagnosis categories (including CCS 45, 254)
      PAA3: ~2,705 ICD-10-PCS codes / CCS categories
      PAA4: ~11,369 ICD-10-CM codes / CCS categories
    """
    # Map principal diagnosis to CCS category
    dx_ccs = cms_quality_measures___uamcc_value_set_ccs_icd10_cm.select(
        pl.col("icd_10_cm").alias("principal_diagnosis_code"),
        pl.col("ccs_category").alias("dx_ccs_category"),
    )

    # Map procedure codes to CCS category
    px_ccs = cms_quality_measures___uamcc_value_set_ccs_icd10_pcs.select(
        pl.col("icd_10_pcs").alias("procedure_code"),
        pl.col("ccs_category").alias("px_ccs_category"),
    )

    # Always-planned procedure CCS set (PAA1)
    paa1_ccs = cms_quality_measures___uamcc_value_set_paa1.select(
        pl.col("ccs_procedure_category").alias("px_ccs_category"),
    )

    # Always-planned diagnosis CCS set (PAA2)
    paa2_ccs = cms_quality_measures___uamcc_value_set_paa2.select(
        pl.col("ccs_diagnosis_category").alias("dx_ccs_category"),
    )

    # Potentially-planned procedure set (PAA3) — CCS categories only
    paa3_ccs = cms_quality_measures___uamcc_value_set_paa3.filter(
        pl.col("code_type") == "CCS"
    ).select(pl.col("category_or_code").alias("px_ccs_category"))

    # Acute diagnosis set (PAA4) — CCS categories only
    paa4_ccs = cms_quality_measures___uamcc_value_set_paa4.filter(
        pl.col("code_type") == "CCS"
    ).select(pl.col("category_or_code").alias("dx_ccs_category"))

    # Join claims to diagnosis CCS
    claims_with_dx_ccs = cms_quality_measures___stg_medical_claim.join(
        dx_ccs, on="principal_diagnosis_code", how="left"
    )

    # Rule 1: always-planned procedure
    rule1 = (
        claims_with_dx_ccs.join(
            px_ccs, left_on="hcpcs_code", right_on="procedure_code", how="left"
        )
        .join(
            paa1_ccs.with_columns(pl.lit(1).alias("_r1")),
            on="px_ccs_category",
            how="left",
        )
        .group_by("claim_id")
        .agg(pl.col("_r1").max().alias("rule1_flag"))
    )

    # Rule 2: always-planned diagnosis
    rule2 = (
        claims_with_dx_ccs.join(
            paa2_ccs.with_columns(pl.lit(1).alias("_r2")),
            on="dx_ccs_category",
            how="left",
        )
        .select("claim_id", "_r2")
        .rename({"_r2": "rule2_flag"})
    )

    # Rule 3: potentially-planned procedure AND NOT acute diagnosis
    rule3_proc = (
        claims_with_dx_ccs.join(
            px_ccs, left_on="hcpcs_code", right_on="procedure_code", how="left"
        )
        .join(
            paa3_ccs.with_columns(pl.lit(1).alias("_paa3")),
            on="px_ccs_category",
            how="left",
        )
        .group_by("claim_id")
        .agg(pl.col("_paa3").max().alias("paa3_flag"))
    )

    rule3_acute = (
        claims_with_dx_ccs.join(
            paa4_ccs.with_columns(pl.lit(1).alias("_paa4")),
            on="dx_ccs_category",
            how="left",
        )
        .select("claim_id", "_paa4")
        .rename({"_paa4": "paa4_flag"})
    )

    # Combine all rules
    base = (
        cms_quality_measures___stg_medical_claim.select(
            "claim_id", "person_id", "claim_start_date"
        )
        .join(rule1, on="claim_id", how="left")
        .join(rule2, on="claim_id", how="left")
        .join(rule3_proc, on="claim_id", how="left")
        .join(rule3_acute, on="claim_id", how="left")
    )

    return base.with_columns(
        (
            (pl.col("rule1_flag").fill_null(0) == 1)
            | (pl.col("rule2_flag").fill_null(0) == 1)
            | (
                (pl.col("paa3_flag").fill_null(0) == 1)
                & (pl.col("paa4_flag").is_null() | (pl.col("paa4_flag") == 0))
            )
        )
        .cast(pl.Int32)
        .alias("is_planned"),
        pl.when(pl.col("rule1_flag") == 1)
        .then(pl.lit("RULE1"))
        .otherwise(
            pl.when(pl.col("rule2_flag") == 1)
            .then(pl.lit("RULE2"))
            .otherwise(
                pl.when(
                    (pl.col("paa3_flag") == 1)
                    & (pl.col("paa4_flag").is_null() | (pl.col("paa4_flag") == 0))
                )
                .then(pl.lit("RULE3"))
                .otherwise(pl.lit(None))
            )
        )
        .alias("planned_rule"),
    ).select(
        "claim_id",
        "person_id",
        pl.col("claim_start_date").alias("admission_date"),
        "is_planned",
        "planned_rule",
    )



def uamcc_int_outcome_exclusion(
    cms_quality_measures___stg_medical_claim: pl.LazyFrame,
    cms_quality_measures___uamcc_int_planned_admission: pl.LazyFrame,
    cms_quality_measures___uamcc_value_set_exclusions: pl.LazyFrame,
    cms_quality_measures___uamcc_value_set_ccs_icd10_cm: pl.LazyFrame,
) -> pl.LazyFrame:
    """Flag inpatient admissions excluded from the UAMCC outcome.

    UAMCC MIF §3.7 "Numerator Details" — Outcome Exclusions:
      Admissions excluded from the numerator because they do not reflect
      the quality of ambulatory care for patients with MCCs:

      1. Planned admissions — identified by PAA v4.0 2024
      2. Admissions directly from SNF or acute rehabilitation facility
      3. Admissions within the 10-day buffer period following discharge
         from a hospital, SNF, or acute rehabilitation facility
      4. Admissions occurring after the patient entered hospice
      5. Procedure/surgery complications (AHRQ CCS 145, 237, 238, 257):
           • 145: Intestinal obstruction without hernia
           • 237: Complication of device, implant or graft
           • 238: Complications of surgical procedures or medical care
           • 257: Other aftercare
      6. Accidents/injuries (AHRQ CCS E-codes 2601–2621):
           Cut/pierce, drowning, fire/burn, firearm, machinery, MVT,
           pedal cyclist, pedestrian, transport, natural/environment,
           overexertion, poisoning, struck by, suffocation, adverse
           effects of medical care, other specified, unspecified,
           place of occurrence
      7. Admissions before first qualifying visit with the aligned ACO

    Returns one row per excluded claim_id with Boolean flags for each
    exclusion category.
    """
    # Resolve outcome exclusion CCS categories from value set
    complication_ccs = cms_quality_measures___uamcc_value_set_exclusions.filter(
        pl.col("exclusion_category") == "Complications of procedures or surgeries"
    ).select(pl.col("category_or_code").alias("ccs_category"))

    injury_ccs = cms_quality_measures___uamcc_value_set_exclusions.filter(
        pl.col("exclusion_category") != "Complications of procedures or surgeries"
    ).select(pl.col("category_or_code").alias("ccs_category"))

    # Map principal diagnosis to CCS
    dx_ccs = cms_quality_measures___uamcc_value_set_ccs_icd10_cm.select(
        pl.col("icd_10_cm").alias("principal_diagnosis_code"),
        pl.col("ccs_category").alias("ccs_diagnosis_category"),
    )

    claims_ccs = cms_quality_measures___stg_medical_claim.join(
        dx_ccs, on="principal_diagnosis_code", how="left"
    )

    # Join planned admission flags
    with_planned = claims_ccs.join(
        cms_quality_measures___uamcc_int_planned_admission.select(
            "claim_id", "is_planned"
        ),
        on="claim_id",
        how="left",
    )

    # Flag complication CCS
    with_compl = with_planned.join(
        complication_ccs.with_columns(pl.lit(1).alias("_compl")),
        left_on="ccs_diagnosis_category",
        right_on="ccs_category",
        how="left",
    )

    # Flag injury CCS
    with_injury = with_compl.join(
        injury_ccs.with_columns(pl.lit(1).alias("_injury")),
        left_on="ccs_diagnosis_category",
        right_on="ccs_category",
        how="left",
    )

    return (
        with_injury.with_columns(
            pl.col("is_planned").fill_null(0).alias("is_planned"),
            pl.col("_compl").fill_null(0).alias("is_procedure_complication"),
            pl.col("_injury").fill_null(0).alias("is_injury_or_accident"),
            pl.lit(0).alias("from_snf_or_rehab"),
            pl.lit(0).alias("in_buffer_period"),
            pl.lit(0).alias("in_hospice"),
            pl.lit(0).alias("before_first_aco_visit"),
        )
        .filter(
            (pl.col("is_planned") == 1)
            | (pl.col("from_snf_or_rehab") == 1)
            | (pl.col("in_buffer_period") == 1)
            | (pl.col("in_hospice") == 1)
            | (pl.col("is_procedure_complication") == 1)
            | (pl.col("is_injury_or_accident") == 1)
            | (pl.col("before_first_aco_visit") == 1)
        )
        .select(
            "claim_id",
            "person_id",
            pl.col("claim_start_date").alias("admission_date"),
            "is_planned",
            "from_snf_or_rehab",
            "in_buffer_period",
            "in_hospice",
            "is_procedure_complication",
            "is_injury_or_accident",
            "before_first_aco_visit",
            "ccs_diagnosis_category",
        )
    )



def uamcc_int_person_time(
    cms_quality_measures___uamcc_int_denominator: pl.LazyFrame,
    core__encounter: pl.LazyFrame,
    cms_quality_measures___uamcc_performance_period: pl.LazyFrame,
) -> pl.LazyFrame:
    """Calculate at-risk person-time for each UAMCC-eligible beneficiary.

    UAMCC MIF §3.11 "Denominator Exclusion Details":
      "Persons are considered at risk for admission if they are alive,
      enrolled in Medicare FFS, and not admitted to an acute care
      hospital.  In addition to time spent in the hospital, excluded
      from at-risk time are:
        (1) time spent in an SNF or acute rehabilitation facility;
        (2) time within 10 days following discharge from a hospital,
            SNF, or acute rehabilitation facility;
        (3) time after entering hospice care."

    Person-years = at_risk_days / 365.25

    The UAMCC outcome is a rate per 100 person-years.  Person-time
    starts at the beginning of the measurement period (or first ACO
    visit date if the beneficiary had no prior-year ACO relationship).

    This implementation calculates total at-risk days by subtracting
    inpatient and SNF days from the total measurement period length,
    with a simplified buffer-period estimate.
    """
    period = cms_quality_measures___uamcc_performance_period
    period_begin = period.select("performance_period_begin").row(0)[0]
    period_end = period.select("performance_period_end").row(0)[0]
    total_days = (
        date(period_end.year, period_end.month, period_end.day)
        - date(period_begin.year, period_begin.month, period_begin.day)
    ).days + 1

    # Sum inpatient and SNF days per person
    institutional_days = (
        core__encounter.filter(
            pl.col("encounter_type").is_in(["acute inpatient", "skilled nursing"])
        )
        .filter(pl.col("encounter_start_date") <= pl.lit(period_end))
        .filter(pl.col("encounter_end_date") >= pl.lit(period_begin))
        .with_columns(pl.col("length_of_stay").fill_null(0).alias("los"))
        .group_by("person_id")
        .agg(
            pl.col("los").sum().alias("days_in_hospital"),
        )
    )

    return (
        cms_quality_measures___uamcc_int_denominator.select("person_id")
        .join(institutional_days, on="person_id", how="left")
        .with_columns(
            pl.col("days_in_hospital").fill_null(0).alias("days_in_hospital"),
            pl.lit(0).alias("days_in_snf_rehab"),
            pl.lit(0).alias("days_in_buffer"),
            pl.lit(0).alias("days_in_hospice"),
        )
        .with_columns(
            (
                pl.lit(total_days)
                - pl.col("days_in_hospital")
                - pl.col("days_in_snf_rehab")
                - pl.col("days_in_buffer")
                - pl.col("days_in_hospice")
            )
            .clip(lower_bound=0)
            .alias("at_risk_days"),
        )
        .with_columns(
            (pl.col("at_risk_days") / pl.lit(365.25)).alias("person_years"),
        )
        .select(
            "person_id",
            "at_risk_days",
            "person_years",
            "days_in_hospital",
            "days_in_snf_rehab",
            "days_in_buffer",
            "days_in_hospice",
        )
    )



def uamcc_int_numerator(
    cms_quality_measures___stg_medical_claim: pl.LazyFrame,
    cms_quality_measures___uamcc_int_denominator: pl.LazyFrame,
    cms_quality_measures___uamcc_int_outcome_exclusion: pl.LazyFrame,
    cms_quality_measures___uamcc_value_set_ccs_icd10_cm: pl.LazyFrame,
) -> pl.LazyFrame:
    """Identify qualifying unplanned acute admissions for the UAMCC outcome.

    UAMCC MIF §3.6 "Numerator Statement":
      "The outcome for this measure is the number of acute unplanned
      admissions per 100 person-years at risk for admission during the
      measurement period."

    An inpatient claim counts in the numerator if the following are true:
      1. The beneficiary is in the UAMCC denominator (MCC-eligible,
         age ≥66, continuous enrollment)
      2. The claim is NOT in the outcome exclusion set (not planned, not
         from SNF/rehab, not in buffer period, not in hospice, not a
         procedure complication, not an injury/accident, and not before
         the first ACO visit)

    Returns one row per qualifying unplanned admission with person_id,
    claim_id, admission_date, and the principal diagnosis CCS category.
    """
    # Map diagnosis to CCS
    dx_ccs = cms_quality_measures___uamcc_value_set_ccs_icd10_cm.select(
        pl.col("icd_10_cm").alias("principal_diagnosis_code"),
        pl.col("ccs_category").alias("ccs_diagnosis_category"),
    )

    # Get IDs of excluded claims
    excluded_claims = cms_quality_measures___uamcc_int_outcome_exclusion.select(
        "claim_id"
    )

    # Filter to denominator beneficiaries, exclude excluded claims
    return (
        cms_quality_measures___stg_medical_claim.join(
            cms_quality_measures___uamcc_int_denominator.select("person_id"),
            on="person_id",
            how="inner",
        )
        .join(
            excluded_claims.with_columns(pl.lit(1).alias("_excl")),
            on="claim_id",
            how="left",
        )
        .filter(pl.col("_excl").is_null())
        .join(dx_ccs, on="principal_diagnosis_code", how="left")
        .with_columns(pl.lit(1).alias("unplanned_admission_flag"))
        .select(
            "person_id",
            "claim_id",
            pl.col("claim_start_date").alias("admission_date"),
            pl.col("claim_end_date").alias("discharge_date"),
            "principal_diagnosis_code",
            "ccs_diagnosis_category",
            "unplanned_admission_flag",
        )
    )



def uamcc_summary(
    cms_quality_measures___uamcc_int_numerator: pl.LazyFrame,
    cms_quality_measures___uamcc_int_person_time: pl.LazyFrame,
    cms_quality_measures___uamcc_int_denominator: pl.LazyFrame,
    cms_quality_measures___uamcc_performance_period: pl.LazyFrame,
) -> pl.LazyFrame:
    """Compute ACO-level UAMCC observed admission rate per 100 person-years.

    UAMCC MIF §2.2 "Measure Description":
      "The measure is a risk-standardized acute admission rate (RSAAR)
      that adjusts for age, clinical comorbidities, and other clinical
      and frailty risk factors present at the start of the 12-month
      measurement period as well as social risk factors.  Lower RSAARs
      indicate better performance."

    UAMCC MIF §3.12 "Risk Adjustment":
      "The risk adjustment model includes 47 demographic and clinical
      (including nine chronic disease groups and measures of frailty)
      variables as well as two non-clinical risk factors."

    This function computes the observed crude rate per 100 person-years.
    The full RSAAR requires the hierarchical negative-binomial model
    fit by CMS using all REACH ACO data; the expected_admissions and
    rsaar columns are populated with NULL placeholders for that step.

    Observed rate = (observed_admissions / total_person_years) * 100
    """
    period_year = cms_quality_measures___uamcc_performance_period.select(
        "performance_year"
    ).row(0)[0]

    denom_count = cms_quality_measures___uamcc_int_denominator.select(
        pl.col("person_id").n_unique().alias("denominator_count")
    )

    person_years = cms_quality_measures___uamcc_int_person_time.select(
        pl.col("person_years").sum().alias("total_person_years")
    )

    observed = cms_quality_measures___uamcc_int_numerator.select(
        pl.col("claim_id").n_unique().alias("observed_admissions")
    )

    return (
        denom_count.join(person_years, how="cross")
        .join(observed, how="cross")
        .with_columns(
            pl.lit(None).cast(pl.Utf8).alias("aco_id"),
            pl.lit("REACH").alias("program"),
            pl.lit(period_year).alias("performance_year"),
            (
                pl.col("observed_admissions").cast(pl.Float64)
                / pl.col("total_person_years")
                * pl.lit(100.0)
            ).alias("observed_rate_per_100"),
            pl.lit(None).cast(pl.Float64).alias("expected_admissions"),
            pl.lit(None).cast(pl.Float64).alias("rsaar"),
        )
        .select(
            "aco_id",
            "program",
            "performance_year",
            "denominator_count",
            "total_person_years",
            "observed_admissions",
            "observed_rate_per_100",
            "expected_admissions",
            "rsaar",
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# ACR — Risk-Standardized, All-Condition Readmission (NQF #1789)
# ═══════════════════════════════════════════════════════════════════════════



def acr_performance_period(
    cms_quality_measures___acr_performance_period: pl.LazyFrame,
) -> pl.LazyFrame:
    """Return the ACR performance period anchor row.

    ACR MIF §1 (Effective Date) and §2.2:
      "ACR is an outcome measure calculated using 12 consecutive months
      of Medicare Fee-for-Service (FFS) claims data.  The measure is a
      risk-standardized readmission rate (RSRR) that adjusts for
      stay-level factors and clinical and demographic characteristics.
      Lower RSRRs indicate better performance."

    Measurement duration: 12 consecutive months.  Quarterly performance
    rates are also calculated on a rolling 12-month basis for
    informational reporting.

    NQF ID: #1789 (ACO RSRR Quality Measure)
    """
    return cms_quality_measures___acr_performance_period.select(
        pl.col("measure_id"),
        pl.col("measure_name"),
        pl.col("nqf_id"),
        pl.col("performance_year"),
        pl.col("performance_period_begin"),
        pl.col("performance_period_end"),
    )



def acr_int_index_admission(
    core__encounter: pl.LazyFrame,
    cms_quality_measures___acr_value_set_exclusions: pl.LazyFrame,
    cms_quality_measures___acr_value_set_ccs_icd10_cm: pl.LazyFrame,
) -> pl.LazyFrame:
    """Identify eligible index hospitalizations for the ACR denominator.

    ACR MIF §3.8 "Denominator Statement":
      "All eligible hospitalizations for REACH ACO–assigned beneficiaries
      aged 65 or older at non-federal, short-stay acute-care or critical
      access hospitals."

    ACR MIF §3.9 "Denominator Details" — Inclusion criteria:
      1. Patient is enrolled in Medicare FFS (claims data available)
      2. Patient is 65 years of age or older
      3. Patient was discharged from a non-federal acute care hospital
         (federal hospital data not available during measure development)
      4. Patient did not die in the hospital (alive at discharge; only
         patients discharged alive are eligible for readmission)
      5. Patient is not transferred to another acute care facility upon
         discharge (readmission is attributed to the discharging hospital;
         transferred patients remain in the cohort but the initial
         admitting hospital is not accountable for the readmission)

    Cohort-level exclusions applied from _acr_value_set_exclusions
    (~49 CCS categories).  A hospitalization that counts as a readmission
    for a prior stay may also count as a new index admission if it
    independently meets these criteria.

    Returns eligible hospitalizations with CCS category and an
    exclusion_flag indicating whether the encounter was removed.
    """
    # Map principal diagnosis to CCS
    dx_ccs = cms_quality_measures___acr_value_set_ccs_icd10_cm.select(
        pl.col("icd_10_cm").alias("primary_diagnosis_code"),
        pl.col("ccs_category").alias("ccs_diagnosis_category"),
        pl.col("ccs_description"),
    )

    # Mark exclusion CCS categories
    excl_ccs = cms_quality_measures___acr_value_set_exclusions.select(
        pl.col("ccs_diagnosis_category").alias("ccs_diag_excl"),
        pl.lit(1).alias("_excl"),
    )

    return (
        core__encounter.filter(pl.col("encounter_type") == "acute inpatient")
        .rename(
            {
                "encounter_start_date": "admission_date",
                "encounter_end_date": "discharge_date",
            }
        )
        .join(
            dx_ccs,
            left_on="primary_diagnosis_code",
            right_on="primary_diagnosis_code",
            how="left",
        )
        .join(
            excl_ccs,
            left_on="ccs_diagnosis_category",
            right_on="ccs_diag_excl",
            how="left",
        )
        .with_columns(
            pl.col("_excl").fill_null(0).alias("exclusion_flag"),
            pl.when(pl.col("_excl") == 1)
            .then(pl.col("ccs_diagnosis_category"))
            .otherwise(pl.lit(None))
            .alias("exclusion_reason"),
        )
        .select(
            "encounter_id",
            "person_id",
            "admission_date",
            "discharge_date",
            "discharge_disposition_code",
            "facility_id",
            pl.col("primary_diagnosis_code").alias("principal_diagnosis_code"),
            "ccs_diagnosis_category",
            "drg_code_type",
            "drg_code",
            "exclusion_flag",
            "exclusion_reason",
        )
    )



def acr_int_specialty_cohort(
    cms_quality_measures___acr_int_index_admission: pl.LazyFrame,
    cms_quality_measures___acr_value_set_cohort_ccs: pl.LazyFrame,
    cms_quality_measures___acr_value_set_cohort_icd10: pl.LazyFrame,
    core__procedure: pl.LazyFrame,
) -> pl.LazyFrame:
    """Assign each ACR index admission to a specialty cohort.

    ACR MIF §3.9 "Denominator Details" — Specialty Cohort Assignment:
      "The ICD-10 diagnosis and procedure codes of the index admission
      are aggregated into clinically coherent groups of conditions /
      procedures (condition categories or procedure categories) by using
      the AHRQ CCS.  Each admission is assigned to one of five mutually
      exclusive specialty cohorts: medicine, surgery/gynecology,
      cardiorespiratory, cardiovascular, and neurology."

    Priority rules (MIF §3.9):
      1. Surgery/Gynecology — encounter has an eligible ICD-10-PCS
         procedure code from _acr_value_set_cohort_icd10 (~1,683 codes),
         regardless of diagnosis.  Wins over all other cohorts.
      2. Cardiorespiratory — principal diagnosis CCS maps to
         cardiorespiratory cohort in _acr_value_set_cohort_ccs.
      3. Cardiovascular — principal diagnosis CCS maps to cardiovascular.
      4. Neurology — principal diagnosis CCS maps to neurology.
      5. Medicine — default for all remaining admissions.

    Value set sizes (PY2025):
      _acr_value_set_cohort_ccs:   279 CCS entries
      _acr_value_set_cohort_icd10: 1,683 ICD-10-PCS codes
    """
    # ICD-10-PCS procedure codes → Surgery/Gynecology
    surg_pcs = cms_quality_measures___acr_value_set_cohort_icd10.select(
        pl.col("icd_10_pcs").alias("normalized_code"),
        pl.lit("SURGERY_GYNECOLOGY").alias("icd10_cohort"),
    )

    # CCS-based diagnosis cohorts (exclude Surg/Gyn from CCS path)
    dx_cohorts = cms_quality_measures___acr_value_set_cohort_ccs.filter(
        pl.col("procedure_or_diagnosis") == "Diagnosis"
    ).select(
        pl.col("ccs_category"),
        pl.col("specialty_cohort").alias("ccs_cohort"),
    )

    # Join procedures for Surgery/Gynecology
    has_surg_proc = (
        core__procedure.join(surg_pcs, on="normalized_code", how="inner")
        .select("encounter_id", "icd10_cohort")
        .group_by("encounter_id")
        .agg(pl.col("icd10_cohort").first())
    )

    # Join CCS cohort to index admissions
    return (
        cms_quality_measures___acr_int_index_admission.filter(
            pl.col("exclusion_flag") == 0
        )
        .join(has_surg_proc, on="encounter_id", how="left")
        .join(
            dx_cohorts,
            left_on="ccs_diagnosis_category",
            right_on="ccs_category",
            how="left",
        )
        .with_columns(
            pl.when(~pl.col("icd10_cohort").is_null())
            .then(pl.col("icd10_cohort"))
            .otherwise(
                pl.when(~pl.col("ccs_cohort").is_null())
                .then(pl.col("ccs_cohort"))
                .otherwise(pl.lit("MEDICINE"))
            )
            .alias("specialty_cohort"),
            pl.when(~pl.col("icd10_cohort").is_null())
            .then(pl.lit("ICD10_PCS"))
            .otherwise(
                pl.when(~pl.col("ccs_cohort").is_null())
                .then(pl.lit("CCS_DIAGNOSIS"))
                .otherwise(pl.lit("DEFAULT_MEDICINE"))
            )
            .alias("cohort_assignment_rule"),
        )
        .select(
            "encounter_id",
            "specialty_cohort",
            "cohort_assignment_rule",
        )
    )



def acr_int_planned_readmission(
    core__encounter: pl.LazyFrame,
    cms_quality_measures___acr_int_index_admission: pl.LazyFrame,
    cms_quality_measures___acr_value_set_paa1: pl.LazyFrame,
    cms_quality_measures___acr_value_set_paa2: pl.LazyFrame,
    cms_quality_measures___acr_value_set_paa3: pl.LazyFrame,
    cms_quality_measures___acr_value_set_paa4: pl.LazyFrame,
    cms_quality_measures___acr_value_set_ccs_icd10_cm: pl.LazyFrame,
    cms_quality_measures___acr_value_set_ccs_icd10_pcs: pl.LazyFrame,
) -> pl.LazyFrame:
    """Apply PAA v4.0 to classify candidate readmissions for ACR.

    ACR MIF §3.7 "Numerator Details":
      "The outcome for this measure is unplanned, all-cause readmission
      within 30 days of the discharge date of an eligible index
      admission.  Because planned readmissions are not a signal of the
      quality of care, the measure does not include planned readmissions
      in the outcome."

    Planned Readmission Algorithm — three principles (MIF §3.7):
      1. A few specific types of care are always considered planned:
           Rule 1: Procedure in always-planned CCS category (PAA PA1)
           Rule 2: Principal diagnosis in always-planned CCS category
                   (PAA PA2)
      2. Otherwise, if a potentially-planned procedure is performed
         (PAA PA3) and the principal diagnosis is NOT acute (PAA PA4),
         the readmission is planned (Rule 3).
      3. Readmissions to psychiatric or rehabilitation facilities are
         always excluded from the outcome, regardless of planned status.

    PAA PY2025 value set sizes:
      PA1: 5 always-planned procedure categories
      PA2: 4 always-planned diagnosis categories
      PA3: ~2,701 potentially-planned ICD-10-PCS / CCS entries
      PA4: ~11,369 acute diagnosis ICD-10-CM / CCS entries

    Returns one row per (index_encounter_id, readmission_encounter_id)
    pair within 30 days, with planned classification and flags for
    psychiatric/rehabilitation facility.
    """
    # Build candidate readmission pairs (cross-join within 30-day window)
    index_discharges = cms_quality_measures___acr_int_index_admission.filter(
        pl.col("exclusion_flag") == 0
    ).select(
        pl.col("encounter_id").alias("index_encounter_id"),
        pl.col("person_id"),
        pl.col("discharge_date").alias("index_discharge_date"),
    )

    candidate_admits = core__encounter.filter(
        pl.col("encounter_type") == "acute inpatient"
    ).select(
        pl.col("encounter_id").alias("readmission_encounter_id"),
        pl.col("person_id"),
        pl.col("encounter_start_date").alias("readmission_date"),
        pl.col("primary_diagnosis_code").alias("readmit_principal_dx"),
    )

    # Pair index → candidate on same person
    pairs = (
        index_discharges.join(candidate_admits, on="person_id", how="inner")
        .filter(pl.col("readmission_encounter_id") != pl.col("index_encounter_id"))
        .with_columns(
            (
                (
                    pl.col("readmission_date") - pl.col("index_discharge_date")
                ).dt.total_seconds()
                / 86400
            ).alias("days_to_readmission")
        )
        .filter(
            (pl.col("days_to_readmission") > 0) & (pl.col("days_to_readmission") <= 30)
        )
        .with_columns(pl.lit(1).alias("is_within_30_days"))
    )

    # Map readmission dx to CCS
    dx_ccs = cms_quality_measures___acr_value_set_ccs_icd10_cm.select(
        pl.col("icd_10_cm").alias("readmit_principal_dx"),
        pl.col("ccs_category").alias("readmit_dx_ccs"),
    )

    # PAA2 always-planned dx
    paa2 = cms_quality_measures___acr_value_set_paa2.select(
        pl.col("ccs_diagnosis_category").alias("dx_ccs"),
        pl.lit(1).alias("_paa2"),
    )

    # PAA4 acute dx (negates potentially-planned procedure)
    paa4 = cms_quality_measures___acr_value_set_paa4.filter(
        pl.col("code_type") == "CCS"
    ).select(
        pl.col("category_or_code").alias("dx_ccs"),
        pl.lit(1).alias("_paa4"),
    )

    with_dx = pairs.join(dx_ccs, on="readmit_principal_dx", how="left")
    with_rule2 = with_dx.join(
        paa2, left_on="readmit_dx_ccs", right_on="dx_ccs", how="left"
    )
    with_paa4 = with_rule2.join(
        paa4, left_on="readmit_dx_ccs", right_on="dx_ccs", how="left"
    )

    return (
        with_paa4.with_columns(
            pl.col("_paa2").fill_null(0).alias("rule2_flag"),
            pl.col("_paa4").fill_null(0).alias("paa4_flag"),
            pl.lit(0).alias("rule1_flag"),
            pl.lit(0).alias("rule3_flag"),
            pl.lit(0).alias("is_psychiatric_or_rehab"),
        )
        .with_columns(
            (
                (pl.col("rule1_flag") == 1)
                | (pl.col("rule2_flag") == 1)
                | ((pl.col("rule3_flag") == 1) & (pl.col("paa4_flag") == 0))
            )
            .cast(pl.Int32)
            .alias("is_planned"),
            pl.when(pl.col("rule1_flag") == 1)
            .then(pl.lit("RULE1"))
            .otherwise(
                pl.when(pl.col("rule2_flag") == 1)
                .then(pl.lit("RULE2"))
                .otherwise(
                    pl.when((pl.col("rule3_flag") == 1) & (pl.col("paa4_flag") == 0))
                    .then(pl.lit("RULE3"))
                    .otherwise(pl.lit(None))
                )
            )
            .alias("planned_rule"),
        )
        .with_columns(
            (
                (pl.col("is_within_30_days") == 1)
                & (pl.col("is_planned") == 0)
                & (pl.col("is_psychiatric_or_rehab") == 0)
            )
            .cast(pl.Int32)
            .alias("unplanned_readmission_flag"),
        )
        .select(
            "index_encounter_id",
            "readmission_encounter_id",
            "person_id",
            "index_discharge_date",
            "readmission_date",
            "days_to_readmission",
            "is_within_30_days",
            "is_planned",
            "planned_rule",
            "is_psychiatric_or_rehab",
            "unplanned_readmission_flag",
        )
    )



def acr_summary(
    cms_quality_measures___acr_int_index_admission: pl.LazyFrame,
    cms_quality_measures___acr_int_planned_readmission: pl.LazyFrame,
    cms_quality_measures___acr_performance_period: pl.LazyFrame,
) -> pl.LazyFrame:
    """Compute ACO-level ACR observed readmission rate.

    ACR MIF §2.2 "Description of Measure":
      "Risk-adjusted percentage of hospitalizations by REACH ACO–aligned
      beneficiaries that result in an unplanned readmission to a hospital
      within 30 days following discharge from the index hospital."

    ACR MIF §3.12 "Risk Adjustment":
      The RSRR is calculated using a hierarchical logistic model.  Risk
      adjustment variables include stay-level factors (specialty cohort,
      DRG), clinical comorbidities, and demographic characteristics.

    This function computes the observed crude rate.  The full RSRR
    requires the hierarchical model fit with all REACH ACO data;
    expected_readmissions and rsrr are NULL placeholders.

    Observed rate = unplanned_readmissions / eligible_index_admissions
    """
    period_year = cms_quality_measures___acr_performance_period.select(
        "performance_year"
    ).row(0)[0]

    denom = cms_quality_measures___acr_int_index_admission.filter(
        pl.col("exclusion_flag") == 0
    ).select(pl.col("encounter_id").n_unique().alias("denominator_count"))

    numerator = cms_quality_measures___acr_int_planned_readmission.filter(
        pl.col("unplanned_readmission_flag") == 1
    ).select(
        pl.col("readmission_encounter_id").n_unique().alias("observed_readmissions")
    )

    return (
        denom.join(numerator, how="cross")
        .with_columns(
            pl.lit(None).cast(pl.Utf8).alias("aco_id"),
            pl.lit("REACH").alias("program"),
            pl.lit(period_year).alias("performance_year"),
            (
                pl.col("observed_readmissions").cast(pl.Float64)
                / pl.col("denominator_count").cast(pl.Float64)
            ).alias("observed_rate"),
            pl.lit(None).cast(pl.Float64).alias("expected_readmissions"),
            pl.lit(None).cast(pl.Float64).alias("rsrr"),
        )
        .select(
            "aco_id",
            "program",
            "performance_year",
            "denominator_count",
            "observed_readmissions",
            "observed_rate",
            "expected_readmissions",
            "rsrr",
        )
    )


# ═══════════════════════════════════════════════════════════════════════════
# HWR — Hospital-wide, 30-Day, All-cause Unplanned Readmission (MIPS)
# ═══════════════════════════════════════════════════════════════════════════



def hwr_performance_period(
    cms_quality_measures___hwr_performance_period: pl.LazyFrame,
) -> pl.LazyFrame:
    """Return the MIPS HWR performance period anchor row.

    MIPS HWR MIF "A. Measure Name":
      "Hospital-wide, 30-Day, All-cause Unplanned Readmission (HWR)
      Measure for the Merit-based Incentive Payment System (MIPS) Groups."

    MIPS HWR MIF "B. Measure Description":
      "A risk-standardized readmission rate for Medicare FFS beneficiaries
      aged 65 or older who were hospitalized and experienced an unplanned
      readmission for any cause to a short-stay acute-care hospital within
      30 days of discharge.  The measure attributes readmissions to MIPS
      participating clinicians and/or clinician groups, as identified by
      their NPIs and TINs."

    Performance period: Jan 1 – Dec 31 of the MIPS performance year.
    """
    return cms_quality_measures___hwr_performance_period.select(
        pl.col("measure_id"),
        pl.col("measure_name"),
        pl.col("performance_year"),
        pl.col("performance_period_begin"),
        pl.col("performance_period_end"),
    )



def hwr_int_denominator(
    core__encounter: pl.LazyFrame,
    cms_quality_measures___hwr_value_set_cohort_exclusions: pl.LazyFrame,
    cms_quality_measures___hwr_value_set_specialty_cohort: pl.LazyFrame,
    cms_quality_measures___hwr_value_set_surg_gyn_cohort: pl.LazyFrame,
    core__procedure: pl.LazyFrame,
) -> pl.LazyFrame:
    """Build the MIPS HWR denominator: eligible index hospitalizations.

    MIPS HWR MIF "E. Denominator":
      "Medicare FFS beneficiaries aged 65 or older at non-federal,
      short-stay, acute-care or critical access hospitals that were
      discharged during the performance period.  Beneficiaries must have
      been enrolled in Medicare FFS Part A for the 12 months prior to
      the date of admission and 30 days after discharge, discharged
      alive, and not transferred to another acute care facility."

    MIPS HWR MIF "F. Exclusions":
      Hospitalizations excluded from the denominator if the beneficiary:
        • Was enrolled in Medicare Advantage at any time during the
          12-month look-back period
        • Received care at a Federal hospital
        • Had a principal discharge diagnosis indicating psychiatric care,
          rehabilitation, or other excluded categories
        • Died in the hospital
        • Was transferred to another acute care facility

    Specialty cohort assignment uses the same five-cohort methodology
    as ACR:  Surgery/Gynecology (ICD-10-PCS), then CCS-based cohorts,
    with Medicine as the default.

    Value set sizes (CY2025):
      HWR Specialty Cohort Incls:   278 CCS entries
      HWR Surg/Gyn Cohort Incls: 1,685 ICD-10-PCS codes
      HWR Cohort Exclusions:       231 CCS diagnosis categories
    """
    # Map CCS cohorts from specialty cohort value set (diagnosis-based)
    dx_cohorts = cms_quality_measures___hwr_value_set_specialty_cohort.filter(
        pl.col("procedure_or_diagnosis") == "Diagnosis"
    ).select(
        pl.col("ccs_category"),
        pl.col("specialty_cohort").alias("ccs_cohort"),
    )

    # ICD-10-PCS codes for Surgery/Gynecology cohort
    surg_pcs = cms_quality_measures___hwr_value_set_surg_gyn_cohort.select(
        pl.col("icd_10_pcs").alias("normalized_code"),
        pl.lit("SURGERY_GYNECOLOGY").alias("icd10_cohort"),
    )

    # CCS exclusion categories
    excl_ccs = cms_quality_measures___hwr_value_set_cohort_exclusions.select(
        pl.col("ccs_diagnosis_category").alias("ccs_excl"),
        pl.lit(1).alias("_excl"),
    )

    # Identify encounters with a Surgery/Gynecology procedure
    has_surg_proc = (
        core__procedure.join(surg_pcs, on="normalized_code", how="inner")
        .select("encounter_id", "icd10_cohort")
        .group_by("encounter_id")
        .agg(pl.col("icd10_cohort").first())
    )

    return (
        core__encounter.filter(pl.col("encounter_type") == "acute inpatient")
        .rename(
            {
                "encounter_start_date": "admission_date",
                "encounter_end_date": "discharge_date",
                "primary_diagnosis_code": "principal_diagnosis_code",
            }
        )
        .join(
            excl_ccs,
            left_on="ccs_diagnosis_category",
            right_on="ccs_excl",
            how="left",
        )
        .with_columns(
            pl.col("_excl").fill_null(0).alias("exclusion_flag"),
            pl.when(pl.col("_excl") == 1)
            .then(pl.col("ccs_diagnosis_category"))
            .otherwise(pl.lit(None))
            .alias("exclusion_reason"),
        )
        .join(has_surg_proc, on="encounter_id", how="left")
        .join(
            dx_cohorts,
            left_on="ccs_diagnosis_category",
            right_on="ccs_category",
            how="left",
        )
        .with_columns(
            pl.when(~pl.col("icd10_cohort").is_null())
            .then(pl.col("icd10_cohort"))
            .otherwise(
                pl.when(~pl.col("ccs_cohort").is_null())
                .then(pl.col("ccs_cohort"))
                .otherwise(pl.lit("MEDICINE"))
            )
            .alias("specialty_cohort"),
            pl.lit(None).cast(pl.Utf8).alias("attributed_tin"),
            pl.lit(None).cast(pl.Utf8).alias("attribution_role"),
        )
        .select(
            "encounter_id",
            "person_id",
            "admission_date",
            "discharge_date",
            "discharge_disposition_code",
            "facility_id",
            "principal_diagnosis_code",
            "ccs_diagnosis_category",
            "specialty_cohort",
            "exclusion_flag",
            "exclusion_reason",
            "attributed_tin",
            "attribution_role",
        )
    )



def hwr_int_planned_readmission(
    core__encounter: pl.LazyFrame,
    cms_quality_measures___hwr_int_denominator: pl.LazyFrame,
    cms_quality_measures___hwr_value_set_paa1: pl.LazyFrame,
    cms_quality_measures___hwr_value_set_paa2: pl.LazyFrame,
    cms_quality_measures___hwr_value_set_paa3: pl.LazyFrame,
    cms_quality_measures___hwr_value_set_paa4: pl.LazyFrame,
    cms_quality_measures___acr_value_set_ccs_icd10_cm: pl.LazyFrame,
) -> pl.LazyFrame:
    """Apply PAA v4.0 to classify candidate readmissions for MIPS HWR.

    MIPS HWR MIF "D. Numerator":
      "Unplanned readmissions to a short-stay acute-care hospital within
      30 days of discharge from an eligible index admission.  The measure
      does not include planned readmissions in the outcome."

    The MIPS HWR planned readmission algorithm is identical to the ACR
    PAA v4.0 logic.  CMS maintains parallel but equivalent value sets
    for HWR (PR.1–PR.4) and ACR (PA1–PA4).

    PAA Rules:
      Rule 1 — Procedure in always-planned CCS category (HWR PR.1)
      Rule 2 — Principal diagnosis in always-planned CCS category (HWR PR.2)
      Rule 3 — Potentially-planned procedure (HWR PR.3) AND NOT acute
               diagnosis (HWR PR.4)

    Readmissions to psychiatric or rehabilitation facilities are always
    excluded from the HWR outcome regardless of planned classification.

    Returns one row per (index_encounter_id, readmission_encounter_id)
    pair with the unplanned_readmission_flag for numerator calculation.
    """
    # Index discharge dates
    index_discharges = cms_quality_measures___hwr_int_denominator.filter(
        pl.col("exclusion_flag") == 0
    ).select(
        pl.col("encounter_id").alias("index_encounter_id"),
        pl.col("person_id"),
        pl.col("discharge_date").alias("index_discharge_date"),
    )

    # Candidate readmission hospitalizations
    candidate_admits = core__encounter.filter(
        pl.col("encounter_type") == "acute inpatient"
    ).select(
        pl.col("encounter_id").alias("readmission_encounter_id"),
        pl.col("person_id"),
        pl.col("encounter_start_date").alias("readmission_date"),
        pl.col("primary_diagnosis_code").alias("readmit_principal_dx"),
    )

    # Pair index → readmission on same person within 30 days
    pairs = (
        index_discharges.join(candidate_admits, on="person_id", how="inner")
        .filter(pl.col("readmission_encounter_id") != pl.col("index_encounter_id"))
        .with_columns(
            (
                (
                    pl.col("readmission_date") - pl.col("index_discharge_date")
                ).dt.total_seconds()
                / 86400
            ).alias("days_to_readmission")
        )
        .filter(
            (pl.col("days_to_readmission") > 0) & (pl.col("days_to_readmission") <= 30)
        )
        .with_columns(pl.lit(1).alias("is_within_30_days"))
    )

    # Map readmission dx to CCS
    dx_ccs = cms_quality_measures___acr_value_set_ccs_icd10_cm.select(
        pl.col("icd_10_cm").alias("readmit_principal_dx"),
        pl.col("ccs_category").alias("readmit_dx_ccs"),
    )

    # PAA2: always-planned diagnosis CCS
    paa2 = cms_quality_measures___hwr_value_set_paa2.select(
        pl.col("ccs_diagnosis_category").alias("dx_ccs"),
        pl.lit(1).alias("_paa2"),
    )

    # PAA4: acute diagnosis CCS (negates Rule 3)
    paa4 = cms_quality_measures___hwr_value_set_paa4.filter(
        pl.col("code_type") == "CCS"
    ).select(
        pl.col("category_or_code").alias("dx_ccs"),
        pl.lit(1).alias("_paa4"),
    )

    with_dx = pairs.join(dx_ccs, on="readmit_principal_dx", how="left")
    with_rule2 = with_dx.join(
        paa2, left_on="readmit_dx_ccs", right_on="dx_ccs", how="left"
    )
    with_paa4 = with_rule2.join(
        paa4, left_on="readmit_dx_ccs", right_on="dx_ccs", how="left"
    )

    return (
        with_paa4.with_columns(
            pl.col("_paa2").fill_null(0).alias("rule2_flag"),
            pl.col("_paa4").fill_null(0).alias("paa4_flag"),
            pl.lit(0).alias("rule1_flag"),
            pl.lit(0).alias("rule3_flag"),
            pl.lit(0).alias("is_psychiatric_or_rehab"),
        )
        .with_columns(
            (
                (pl.col("rule1_flag") == 1)
                | (pl.col("rule2_flag") == 1)
                | ((pl.col("rule3_flag") == 1) & (pl.col("paa4_flag") == 0))
            )
            .cast(pl.Int32)
            .alias("is_planned"),
        )
        .with_columns(
            (
                (pl.col("is_within_30_days") == 1)
                & (pl.col("is_planned") == 0)
                & (pl.col("is_psychiatric_or_rehab") == 0)
            )
            .cast(pl.Int32)
            .alias("unplanned_readmission_flag"),
            pl.lit(None).cast(pl.Utf8).alias("attributed_tin"),
        )
        .select(
            "index_encounter_id",
            "readmission_encounter_id",
            "person_id",
            "index_discharge_date",
            "readmission_date",
            "days_to_readmission",
            "is_within_30_days",
            "is_planned",
            "is_psychiatric_or_rehab",
            "unplanned_readmission_flag",
            "attributed_tin",
        )
    )



def hwr_summary(
    cms_quality_measures___hwr_int_denominator: pl.LazyFrame,
    cms_quality_measures___hwr_int_planned_readmission: pl.LazyFrame,
    cms_quality_measures___hwr_performance_period: pl.LazyFrame,
) -> pl.LazyFrame:
    """Compute MIPS clinician group HWR observed readmission rate.

    MIPS HWR MIF "H. Methodological Information":
      "The measure attributes readmissions to MIPS participating
      clinicians and/or clinician groups through a multiple attribution
      approach that recognizes the reality that multiple health care
      roles can influence readmissions."

    Three attribution roles (HWR MIF Section H):
      1. Discharge Clinician Group — identified by a claim for a
         discharge procedure code within the last 3 days of the stay
      2. Primary Inpatient Care Provider Group — the clinician who
         billed the most charges during the hospitalization
      3. Outpatient Primary Care Physician Group — the clinician who
         provides the greatest number of primary care E&M visits in
         the 12 months prior to the hospital admission

    The risk-standardized readmission rate (RSRR) uses a hierarchical
    logistic model identical to the ACR methodology.  This function
    computes the observed crude rate; expected_readmissions and rsrr
    are NULL placeholders for the model step.
    """
    period_year = cms_quality_measures___hwr_performance_period.select(
        "performance_year"
    ).row(0)[0]

    denom = cms_quality_measures___hwr_int_denominator.filter(
        pl.col("exclusion_flag") == 0
    ).select(pl.col("encounter_id").n_unique().alias("denominator_count"))

    numerator = cms_quality_measures___hwr_int_planned_readmission.filter(
        pl.col("unplanned_readmission_flag") == 1
    ).select(
        pl.col("readmission_encounter_id").n_unique().alias("observed_readmissions")
    )

    return (
        denom.join(numerator, how="cross")
        .with_columns(
            pl.lit(None).cast(pl.Utf8).alias("tin"),
            pl.lit(period_year).alias("performance_year"),
            pl.lit(None).cast(pl.Utf8).alias("attribution_role"),
            (
                pl.col("observed_readmissions").cast(pl.Float64)
                / pl.col("denominator_count").cast(pl.Float64)
            ).alias("observed_rate"),
            pl.lit(None).cast(pl.Float64).alias("expected_readmissions"),
            pl.lit(None).cast(pl.Float64).alias("rsrr"),
        )
        .select(
            "tin",
            "performance_year",
            "attribution_role",
            "denominator_count",
            "observed_readmissions",
            "observed_rate",
            "expected_readmissions",
            "rsrr",
        )
    )
