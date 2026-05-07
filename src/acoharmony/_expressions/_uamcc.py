# © 2025 HarmonyCares
# All rights reserved.

"""
UAMCC (NQF #2888) All-Cause Unplanned Admissions for Patients with
Multiple Chronic Conditions expression builder.

Expressions for calculating the ACO REACH UAMCC measure:
1. Identify MCC cohort (9 chronic condition groups from lookback year claims)
2. Build denominator (age >= 66, 2+ MCC groups)
3. Apply Planned Admission Algorithm v4.0 (PAA Rules 1-3)
4. Apply outcome exclusions (planned, complications, injuries)
5. Calculate person-time at risk
6. Count unplanned admissions (numerator)
7. Compute observed rate per 100 person-years

References:
- ACOREACH_PY2025_UAMCC_MIF_posted07072025.pdf
- NQF #2888
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import explain, profile_memory, timeit, traced
from ._registry import register_expression

# Nine MCC chronic condition groups
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


@register_expression(
    "uamcc",
    schemas=["silver", "gold"],
    dataset_types=["claims", "eligibility"],
    description="UAMCC NQF #2888 All-Cause Unplanned Admissions for Patients with MCCs",
)
class UamccExpression:
    """
    Generate expressions for UAMCC measure calculation.

        Configuration Structure:
            ```yaml
            uamcc:
              performance_year: 2025
              min_age: 66
              min_mcc_groups: 2
            ```
    """

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def load_aligned_population(
        silver_path: Path,
        program: str,
        aco_id: str,
        performance_year: int,
    ) -> pl.LazyFrame:
        """Aligned beneficiary MBIs for ``program`` / ``aco_id`` / PY.

        REACH: reads ``bar.parquet`` (the BAR alignment ingest), filters
        ``source_filename`` to ``ALG[CR]{yy}`` matching the PY for the
        given ``aco_id``, and returns the *union* of bene_mbi across
        every report for that PY/ACO. Union (not "latest only") because
        a bene who was aligned mid-year and dropped at runout still
        counts for the measure year — taking only the latest ALGR would
        drop them and shrink recall vs CMS BLQQR.

        MSSP / FFS: not yet implemented — alignment data not ingested.
        """
        program = program.upper()
        if program != "REACH":
            raise NotImplementedError(
                f"alignment gating for program={program!r} not implemented; "
                "only REACH is supported until MSSP/FFS alignment data is ingested"
            )

        bar_path = silver_path / "bar.parquet"
        if not bar_path.exists():
            return pl.LazyFrame(schema={"person_id": pl.Utf8})

        py_yy = f"{performance_year % 100:02d}"
        token_re = rf"P\.{aco_id}\.ALG[CR]{py_yy}\.RP\."

        return (
            pl.scan_parquet(bar_path)
            .filter(pl.col("source_filename").str.contains(token_re))
            .select(pl.col("bene_mbi").alias("person_id"))
            .unique()
        )

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def load_uamcc_value_sets(silver_path: Path) -> dict[str, pl.LazyFrame]:
        """Load all UAMCC value sets from silver layer."""
        value_sets: dict[str, pl.LazyFrame] = {}
        file_mappings = {
            "cohort": "value_sets_uamcc_value_set_cohort.parquet",
            "ccs_icd10_cm": "value_sets_uamcc_value_set_ccs_icd10_cm.parquet",
            "ccs_icd10_pcs": "value_sets_uamcc_value_set_ccs_icd10_pcs.parquet",
            "exclusions": "value_sets_uamcc_value_set_exclusions.parquet",
            "paa1": "value_sets_uamcc_value_set_paa1.parquet",
            "paa2": "value_sets_uamcc_value_set_paa2.parquet",
            "paa3": "value_sets_uamcc_value_set_paa3.parquet",
            "paa4": "value_sets_uamcc_value_set_paa4.parquet",
        }

        for key, filename in file_mappings.items():
            file_path = silver_path / filename
            if file_path.exists():
                value_sets[key] = pl.scan_parquet(file_path)
            else:
                value_sets[key] = pl.DataFrame().lazy()

        return value_sets

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    @profile_memory(log_result=True)
    def identify_mcc_cohort(
        claims: pl.LazyFrame,
        cohort_vs: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """Identify beneficiaries with qualifying chronic condition groups.

        Unpivots all diagnosis columns from claims and matches against the
        cohort value set to find which of the 9 MCC groups each beneficiary
        qualifies for. Uses the lookback year (PY-1).
        """
        performance_year = config.get("performance_year", 2025)
        lookback_begin = f"{performance_year - 1}-01-01"
        lookback_end = f"{performance_year - 1}-12-31"

        # Build long-form diagnosis table from all dx columns
        schema_names = claims.collect_schema().names()
        dx_cols = [c for c in schema_names if c.startswith("diagnosis_code_")]

        # Use claim_start_date for cohort identification: it's populated
        # on every claim type (100% non-null in gold), whereas
        # admission_date is only set on inpatient/SNF facility claims
        # (~9% of rows). CMS computes MCC from any claim line bearing a
        # qualifying dx during the lookback — using admission_date
        # silently restricts the search to inpatient claims and drops
        # virtually every CMS-aligned bene from our cohort.
        frames = []
        for col in dx_cols:
            frames.append(
                claims.select(
                    [
                        pl.col("claim_id"),
                        pl.col("person_id"),
                        pl.col("claim_start_date").alias("service_date"),
                        pl.col(col).alias("normalized_code"),
                    ]
                ).filter(pl.col("normalized_code").is_not_null())
            )

        if not frames:
            return pl.DataFrame(
                schema={
                    "person_id": pl.Utf8,
                    "chronic_condition_group": pl.Utf8,
                    "qualifying_code": pl.Utf8,
                    "qualifying_code_date": pl.Date,
                    "claim_count": pl.UInt32,
                }
            ).lazy()

        all_dx = pl.concat(frames)

        lookback_dx = all_dx.filter(
            (pl.col("service_date") >= pl.lit(lookback_begin).str.to_date("%Y-%m-%d"))
            & (pl.col("service_date") <= pl.lit(lookback_end).str.to_date("%Y-%m-%d"))
        )

        # Join with cohort value set
        matched = lookback_dx.join(
            cohort_vs.select(
                [
                    pl.col("icd_10_cm").alias("normalized_code"),
                    pl.col("chronic_condition_group"),
                ]
            ),
            on="normalized_code",
            how="inner",
        )

        return matched.group_by(["person_id", "chronic_condition_group"]).agg(
            [
                pl.col("service_date").min().alias("qualifying_code_date"),
                pl.col("claim_id").n_unique().alias("claim_count"),
                pl.col("normalized_code").first().alias("qualifying_code"),
            ]
        )

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def build_denominator(
        mcc_cohort: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """Build UAMCC denominator: age >= 66, 2+ MCC groups.

        When ``config`` includes ``program``, ``aco_id``, and ``silver_path``,
        also restricts to that program/ACO's PY-aligned population (today
        ``program="REACH"`` only — see ``load_aligned_population``).
        """
        performance_year = config.get("performance_year", 2025)
        min_age = config.get("min_age", 66)
        min_mcc_groups = config.get("min_mcc_groups", 2)
        program = config.get("program")
        aco_id = config.get("aco_id")
        silver_path = config.get("silver_path")
        period_begin = pl.date(performance_year, 1, 1)

        condition_counts = (
            mcc_cohort.group_by("person_id")
            .agg(
                [
                    pl.col("chronic_condition_group")
                    .n_unique()
                    .alias("chronic_condition_count"),
                ]
            )
            .filter(pl.col("chronic_condition_count") >= min_mcc_groups)
        )

        patients_with_age = (
            eligibility.select(
                [pl.col("person_id"), pl.col("birth_date").cast(pl.Date)]
            )
            .unique(subset=["person_id"], keep="first")
            .with_columns(
                [
                    (
                        (period_begin - pl.col("birth_date")).dt.total_days()
                        / 365.25
                    )
                    .cast(pl.Int32)
                    .alias("age_at_period_start")
                ]
            )
            .filter(pl.col("age_at_period_start") >= min_age)
        )

        denom = condition_counts.join(
            patients_with_age.select(["person_id", "age_at_period_start"]),
            on="person_id",
            how="inner",
        )

        if program and aco_id and silver_path is not None:
            aligned = UamccExpression.load_aligned_population(
                Path(silver_path), program, aco_id, performance_year
            )
            denom = denom.join(aligned, on="person_id", how="inner")

        return denom

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def classify_planned_admissions(
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """Apply CMS Planned Admission Algorithm v4.0.

        PAA Rules (first match wins):
          Rule 1 — always-planned procedure CCS (PAA1)
          Rule 2 — always-planned diagnosis CCS (PAA2)
          Rule 3 — potentially-planned procedure (PAA3) AND NOT acute dx (PAA4)
        """
        performance_year = config.get("performance_year", 2025)

        # Deduplicate claims to claim-level, cast dates
        base_claims = (
            claims.with_columns(
                [
                    pl.col("admission_date")
                    .str.to_date("%Y-%m-%d", strict=False)
                    .alias("admission_date"),
                    pl.col("discharge_date")
                    .str.to_date("%Y-%m-%d", strict=False)
                    .alias("discharge_date"),
                ]
            )
            .filter(
                pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("11")
                & pl.col("admission_date").is_not_null()
                & (pl.col("admission_date") >= pl.date(performance_year, 1, 1))
                & (pl.col("admission_date") <= pl.date(performance_year, 12, 31))
            )
            .unique(subset=["claim_id"], keep="first")
        )

        # Map principal diagnosis to CCS
        dx_ccs = value_sets["ccs_icd10_cm"].select(
            [
                pl.col("icd_10_cm").alias("diagnosis_code_1"),
                pl.col("ccs_category").alias("dx_ccs_category"),
            ]
        )

        # PAA1: always-planned procedure CCS categories
        paa1_set = (
            value_sets["paa1"]
            .select(pl.col("ccs_procedure_category"))
            .unique()
            .collect()["ccs_procedure_category"]
            .to_list()
        )

        # PAA2: always-planned diagnosis CCS categories
        paa2_set = (
            value_sets["paa2"]
            .select(pl.col("ccs_diagnosis_category"))
            .unique()
            .collect()["ccs_diagnosis_category"]
            .to_list()
        )

        # PAA3: potentially-planned procedure CCS categories
        paa3_set = (
            value_sets["paa3"]
            .filter(pl.col("code_type") == "CCS")
            .select(pl.col("category_or_code"))
            .unique()
            .collect()["category_or_code"]
            .to_list()
        )

        # PAA4: acute diagnosis CCS categories (negate rule 3)
        paa4_set = (
            value_sets["paa4"]
            .filter(pl.col("code_type") == "CCS")
            .select(pl.col("category_or_code"))
            .unique()
            .collect()["category_or_code"]
            .to_list()
        )

        # Map ICD-10-PCS → CCS for procedure flags. Inpatient claims
        # carry institutional procedures in ``procedure_code_1..25``
        # (not ``hcpcs_code``, which is empty on inpatient and only
        # populated on outpatient/Part B). Unpivot and aggregate so a
        # claim is flagged if *any* of its procedures hit PAA1/PAA3.
        proc_cols = [c for c in claims.collect_schema().names() if c.startswith("procedure_code_")]
        px_ccs_map = value_sets["ccs_icd10_pcs"].select(
            [
                pl.col("icd_10_pcs").alias("normalized_pcs"),
                pl.col("ccs_category").alias("px_ccs_category"),
            ]
        )
        if proc_cols:
            unpivoted = pl.concat(
                [
                    base_claims.select(
                        [
                            pl.col("claim_id"),
                            pl.col(c).alias("normalized_pcs"),
                        ]
                    ).filter(
                        pl.col("normalized_pcs").is_not_null()
                        & (pl.col("normalized_pcs") != "")
                    )
                    for c in proc_cols
                ]
            )
            claim_px_flags = (
                unpivoted.join(px_ccs_map, on="normalized_pcs", how="inner")
                .group_by("claim_id")
                .agg(
                    [
                        pl.col("px_ccs_category").is_in(paa1_set).any().alias("rule1_flag"),
                        pl.col("px_ccs_category").is_in(paa3_set).any().alias("paa3_flag"),
                    ]
                )
            )
        else:
            claim_px_flags = base_claims.select("claim_id").with_columns(
                [
                    pl.lit(False).alias("rule1_flag"),
                    pl.lit(False).alias("paa3_flag"),
                ]
            )

        claims_with_ccs = base_claims.join(
            dx_ccs, on="diagnosis_code_1", how="left"
        ).join(claim_px_flags, on="claim_id", how="left")

        # Coerce nulls (claims whose dx isn't in the CCS map, or which
        # had no procedure-CCS hit) to False so downstream OR/AND chains
        # produce concrete booleans. Without this ``is_planned``
        # propagates null and ``~is_excluded`` filters every
        # otherwise-eligible admission out of the numerator.
        return claims_with_ccs.with_columns(
            [
                pl.col("rule1_flag").fill_null(False),
                pl.col("paa3_flag").fill_null(False),
                pl.col("dx_ccs_category").is_in(paa2_set).fill_null(False).alias("rule2_flag"),
                pl.col("dx_ccs_category").is_in(paa4_set).fill_null(False).alias("paa4_flag"),
            ]
        ).with_columns(
            [
                (
                    pl.col("rule1_flag")
                    | pl.col("rule2_flag")
                    | (pl.col("paa3_flag") & ~pl.col("paa4_flag"))
                ).alias("is_planned"),
                pl.when(pl.col("rule1_flag"))
                .then(pl.lit("RULE1"))
                .when(pl.col("rule2_flag"))
                .then(pl.lit("RULE2"))
                .when(pl.col("paa3_flag") & ~pl.col("paa4_flag"))
                .then(pl.lit("RULE3"))
                .otherwise(pl.lit(None))
                .alias("planned_rule"),
            ]
        ).select(
            [
                "claim_id",
                "person_id",
                "admission_date",
                "discharge_date",
                "discharge_disposition_code",
                "admit_source_code",
                "diagnosis_code_1",
                "dx_ccs_category",
                "is_planned",
                "planned_rule",
            ]
        )

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def apply_outcome_exclusions(
        planned_admissions: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """Flag admissions excluded from numerator.

        Three exclusion categories (per UAMCC MIF §3.7):
            1. Procedure complications — matched against CCS diagnosis category
            2. Accidents or injuries — matched against CCS diagnosis category
            3. COVID-19 diagnosis — matched against ICD-10-CM principal dx

        Filters the value set to the current performance_year and to non-null
        codes; categories #1 and #2 are scoped to ``code_type == "CCS"`` so the
        ICD-10 COVID code doesn't leak into the CCS comparison.
        """
        performance_year = int(config["performance_year"])
        exclusion_vs = value_sets["exclusions"].filter(
            (pl.col("performance_year") == performance_year)
            & pl.col("category_or_code").is_not_null()
        )

        complication_ccs = (
            exclusion_vs.filter(
                (pl.col("exclusion_category") == "Complications of procedures or surgeries")
                & (pl.col("code_type") == "CCS")
            )
            .select("category_or_code")
            .unique()
            .collect()["category_or_code"]
            .to_list()
        )

        injury_ccs = (
            exclusion_vs.filter(
                (pl.col("exclusion_category") == "Accidents or injuries")
                & (pl.col("code_type") == "CCS")
            )
            .select("category_or_code")
            .unique()
            .collect()["category_or_code"]
            .to_list()
        )

        covid_icd10 = (
            exclusion_vs.filter(pl.col("exclusion_category") == "COVID-19 diagnosis")
            .select("category_or_code")
            .unique()
            .collect()["category_or_code"]
            .to_list()
        )

        return planned_admissions.with_columns(
            [
                pl.col("dx_ccs_category")
                .is_in(complication_ccs)
                .fill_null(False)
                .alias("is_procedure_complication"),
                pl.col("dx_ccs_category")
                .is_in(injury_ccs)
                .fill_null(False)
                .alias("is_injury_or_accident"),
                pl.col("diagnosis_code_1")
                .is_in(covid_icd10)
                .fill_null(False)
                .alias("is_covid_19"),
            ]
        ).with_columns(
            [
                (
                    pl.col("is_planned")
                    | pl.col("is_procedure_complication")
                    | pl.col("is_injury_or_accident")
                    | pl.col("is_covid_19")
                ).alias("is_excluded")
            ]
        )

    # Discharge-disposition codes that signal the bene was sent to
    # *another acute care* facility — the next admission, however soon,
    # is part of the same continuous stay. Per UFS data dictionary +
    # UAMCC MIF v4.0 §3.5:
    #   02 — short-term general hospital
    #   05 — designated cancer center / children's hospital
    #   65 — psych hospital / psych unit
    #   66 — critical access hospital
    #   82, 83, 86, 87, 88, 89, 90, 91, 93 — discharge/transfer-with-planned-readmission
    #         variants of the above (CMS 2020 NUBC update)
    _TRANSFER_DISCHARGE_CODES = (
        "02",
        "05",
        "65",
        "66",
        "82",
        "83",
        "86",
        "87",
        "88",
        "89",
        "90",
        "91",
        "93",
    )

    # Admit-source codes that signal arrival from another acute care
    # hospital. Per UB-04 SRC of admission codes:
    #   4 — transfer from a hospital (different facility)
    #   6 — transfer from another health-care facility
    _TRANSFER_ADMIT_SOURCE_CODES = ("4", "6")

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def link_admission_spells(
        per_claim: pl.LazyFrame,
    ) -> pl.LazyFrame:
        """Collapse contiguous inpatient claims into one continuous admission.

        Per UAMCC MIF v4.0 §3.5: if a beneficiary is transferred between
        acute facilities, or admitted to one within a day of being
        discharged from another, those stays are a single "linked" or
        "spell" admission. Counting them separately inflates the
        numerator — which is exactly what was happening pre-link
        (PY2024 over-count of +57%).

        Linking rule: two adjacent stays (ordered by ``admission_date``,
        then ``claim_id`` as tiebreak) are linked when ANY of these are
        true:
          1. the prior discharge disposition is a transfer-to-acute
             code (see ``_TRANSFER_DISCHARGE_CODES``);
          2. the next stay's admit_source signals "from-another-hospital"
             (see ``_TRANSFER_ADMIT_SOURCE_CODES``);
          3. the next admission_date is on or before the prior
             discharge_date (overlap or same-day).

        The linked spell inherits:
          * ``admission_date`` from the first stay,
          * ``discharge_date`` from the last,
          * ``discharge_disposition_code`` from the last,
          * ``is_planned`` = ``any(is_planned)`` across the chain
            (planned-anywhere wins; conservative — a chain that includes
            any planned stay is treated as a planned spell).
          * ``is_excluded`` = ``any(is_excluded)`` for the same reason,
          * a stable ``spell_id`` of ``"{first_claim_id}"``.
        """
        sentinel_disp = pl.lit(list(UamccExpression._TRANSFER_DISCHARGE_CODES))
        sentinel_src = pl.lit(list(UamccExpression._TRANSFER_ADMIT_SOURCE_CODES))

        ordered = per_claim.sort(["person_id", "admission_date", "claim_id"])

        with_lags = ordered.with_columns(
            [
                pl.col("discharge_date")
                .shift(1)
                .over("person_id")
                .alias("_prior_discharge"),
                pl.col("discharge_disposition_code")
                .shift(1)
                .over("person_id")
                .alias("_prior_disposition"),
            ]
        )

        with_link = with_lags.with_columns(
            [
                (
                    pl.col("_prior_disposition").is_in(sentinel_disp).fill_null(False)
                    | pl.col("admit_source_code").is_in(sentinel_src).fill_null(False)
                    | (
                        pl.col("_prior_discharge").is_not_null()
                        & (pl.col("admission_date") <= pl.col("_prior_discharge"))
                    )
                ).alias("_linked_to_prior")
            ]
        ).with_columns(
            [
                # is_chain_start = NOT linked to prior. The cumulative sum
                # of chain starts gives a dense per-person spell index.
                (~pl.col("_linked_to_prior")).cast(pl.Int32).alias("_chain_start"),
            ]
        ).with_columns(
            [
                pl.col("_chain_start").cum_sum().over("person_id").alias("_spell_idx"),
            ]
        )

        # Aggregate per (person_id, spell_idx). Sort by admission_date
        # so .first()/.last() pick true chronological endpoints.
        return (
            with_link.sort(["person_id", "_spell_idx", "admission_date", "claim_id"])
            .group_by(["person_id", "_spell_idx"], maintain_order=True)
            .agg(
                [
                    pl.col("claim_id").first().alias("spell_id"),
                    pl.col("admission_date").first().alias("admission_date"),
                    pl.col("discharge_date").last().alias("discharge_date"),
                    pl.col("discharge_disposition_code")
                    .last()
                    .alias("discharge_disposition_code"),
                    pl.col("is_planned").any().alias("is_planned"),
                    pl.col("is_excluded").any().alias("is_excluded"),
                    pl.col("claim_id").n_unique().alias("linked_claim_count"),
                ]
            )
            .drop("_spell_idx")
        )

    @staticmethod
    @traced()
    @timeit(log_level="debug")
    def calculate_person_time(
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        config: dict[str, Any],
    ) -> pl.LazyFrame:
        """Calculate at-risk person-time per beneficiary.

        at_risk_days = total_period_days - institutional_days
        person_years = at_risk_days / 365.25
        """
        performance_year = config.get("performance_year", 2025)
        # 365 days for non-leap, 366 for leap
        total_days = 366 if performance_year % 4 == 0 else 365

        # Institutional days from inpatient claims
        institutional = (
            claims.with_columns(
                [
                    pl.col("admission_date")
                    .str.to_date("%Y-%m-%d", strict=False)
                    .alias("admission_date"),
                    pl.col("discharge_date")
                    .str.to_date("%Y-%m-%d", strict=False)
                    .alias("discharge_date"),
                ]
            )
            .filter(
                pl.col("bill_type_code").cast(pl.Utf8).str.starts_with("11")
                & pl.col("admission_date").is_not_null()
            )
            .unique(subset=["claim_id"], keep="first")
            .with_columns(
                [
                    pl.when(pl.col("discharge_date").is_not_null())
                    .then(
                        (pl.col("discharge_date") - pl.col("admission_date"))
                        .dt.total_days()
                        .clip(lower_bound=1)
                    )
                    .otherwise(pl.lit(1))
                    .alias("los")
                ]
            )
            .group_by("person_id")
            .agg(pl.col("los").sum().alias("days_in_hospital"))
        )

        return (
            denominator.select("person_id")
            .join(institutional, on="person_id", how="left")
            .with_columns(
                [pl.col("days_in_hospital").fill_null(0).alias("days_in_hospital")]
            )
            .with_columns(
                [
                    (pl.lit(total_days) - pl.col("days_in_hospital"))
                    .clip(lower_bound=0)
                    .alias("at_risk_days")
                ]
            )
            .with_columns(
                [(pl.col("at_risk_days") / 365.25).alias("person_years")]
            )
            .filter(pl.col("at_risk_days") > 0)
        )

    @staticmethod
    @traced()
    @timeit(log_level="info")
    @profile_memory(log_result=True)
    @explain(
        why="UAMCC measure calculation failed",
        how="Check medical_claim and eligibility data, and UAMCC value sets in silver",
        causes=[
            "Missing input data",
            "Missing UAMCC value sets",
            "MCC cohort identification error",
        ],
    )
    def calculate_uamcc_measure(
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
        config: dict[str, Any],
    ) -> tuple[
        pl.LazyFrame,
        pl.LazyFrame,
        pl.LazyFrame,
        pl.LazyFrame,
        pl.LazyFrame,
        pl.LazyFrame,
    ]:
        """Run complete UAMCC measure pipeline.

        Returns:
            (mcc_cohort, denominator, planned_admissions,
             outcome_exclusions, person_time, summary)
        """
        # Step 1: MCC cohort from lookback year
        mcc_cohort = UamccExpression.identify_mcc_cohort(
            claims, value_sets["cohort"], config
        )

        # Step 2: Denominator
        denominator = UamccExpression.build_denominator(
            mcc_cohort, eligibility, config
        )

        # Step 3: PAA classification
        planned_admissions = UamccExpression.classify_planned_admissions(
            claims, value_sets, config
        )

        # Step 4: Outcome exclusions
        outcome_exclusions = UamccExpression.apply_outcome_exclusions(
            planned_admissions, value_sets, config
        )

        # Step 5: Person-time
        person_time = UamccExpression.calculate_person_time(
            denominator, claims, config
        )

        # Step 6: Numerator (unplanned admissions for denominator members)
        excluded_claims = outcome_exclusions.filter(
            pl.col("is_excluded")
        ).select("claim_id")

        numerator = (
            planned_admissions.join(
                denominator.select("person_id"),
                on="person_id",
                how="inner",
            )
            .join(
                excluded_claims.with_columns(
                    [pl.lit(True).alias("_excl")]
                ),
                on="claim_id",
                how="left",
            )
            .filter(pl.col("_excl").is_null())
            .drop("_excl")
            .with_columns([pl.lit(True).alias("unplanned_admission_flag")])
        )

        # Step 7: Summary
        performance_year = config.get("performance_year", 2025)

        denom_count = denominator.select(
            pl.col("person_id").n_unique().alias("denominator_count")
        )
        total_py = person_time.select(
            pl.col("person_years").sum().alias("total_person_years")
        )
        obs_admits = numerator.select(
            pl.col("claim_id").n_unique().alias("observed_admissions")
        )

        summary = (
            denom_count.join(total_py, how="cross")
            .join(obs_admits, how="cross")
            .with_columns(
                [
                    pl.lit("REACH").alias("program"),
                    pl.lit("UAMCC").alias("measure_id"),
                    pl.lit("2888").alias("nqf_id"),
                    pl.lit(performance_year).alias("performance_year"),
                    (
                        pl.col("observed_admissions").cast(pl.Float64)
                        / pl.col("total_person_years")
                        * 100.0
                    ).alias("observed_rate_per_100"),
                ]
            )
        )

        return (
            mcc_cohort,
            denominator,
            planned_admissions,
            outcome_exclusions,
            person_time,
            summary,
        )
