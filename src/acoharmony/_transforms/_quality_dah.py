# © 2025 HarmonyCares
# All rights reserved.

"""
DAH (Days at Home) ACO REACH quality measure.

Per beneficiary in the measurement period:
    survival_days  = min(365, days from period_start to dod or period_end)
    observed_dic   = institutional days inside the measurement window
                     (acute IP, SNF, IRF, LTCH spans + nursing-home transition residency)
    observed_dah   = max(survival_days - observed_dic, 0)

This is the ACO REACH operational definition. It is intentionally a
ceiling/floor on the more elaborate CMS spec — enough surface to tie out
to BLQQR DAH at the per-bene level for `mx_validate`. Refinements can
land later without changing the QualityMeasureBase contract.
"""

from __future__ import annotations

from datetime import date

import polars as pl

from .._decor8 import timeit, traced
from .._log import LogWriter
from ._quality_measure_base import MeasureFactory, MeasureMetadata, QualityMeasureBase

logger = LogWriter("transforms.quality_dah")

# Bill-type prefix → institutional facility type (first two chars of UB-04 type-of-bill)
# 11x = inpatient hospital, 21x = SNF inpatient, 18x = swing-bed/IRF, 41x = LTCH
_INSTITUTIONAL_BILL_PREFIXES = ("11", "18", "21", "41")


class DaysAtHome(QualityMeasureBase):
    """ACO REACH Days at Home measure (per-beneficiary)."""

    def get_metadata(self) -> MeasureMetadata:
        return MeasureMetadata(
            measure_id="REACH_DAH",
            measure_name="Days at Home",
            measure_steward="CMS",
            measure_version="2025",
            description=(
                "Per-beneficiary count of days spent at home (not in an "
                "institutional setting and not deceased) during the measurement period. "
                "Higher is better."
            ),
            numerator_description="Days at home = survival_days - observed institutional days",
            denominator_description="All beneficiaries with any enrollment in the measurement period",
            exclusions_description=None,
        )

    def _period_bounds(self) -> tuple[date, date]:
        py: int = int(self.config.get("performance_year", 2025))
        return date(py, 1, 1), date(py, 12, 31)

    @traced()
    @timeit(log_level="debug")
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """All persons with enrollment overlapping the measurement period."""
        period_start, period_end = self._period_bounds()
        return (
            eligibility.filter(
                (pl.col("enrollment_start_date") <= pl.lit(period_end))
                & (
                    pl.col("enrollment_end_date").is_null()
                    | (pl.col("enrollment_end_date") >= pl.lit(period_start))
                )
            )
            .select("person_id")
            .unique()
            .with_columns(pl.lit(True).alias("denominator_flag"))
        )

    @traced()
    @timeit(log_level="debug")
    def calculate_numerator(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """
        Per-bene survival_days, observed_dic, observed_dah.

        QualityMeasureBase signature only gives us claims+denominator (no
        eligibility). We work around that by looking up dob/dod from
        claims via a left join on the denominator's value_sets — but for
        the mx_validate pipeline the caller passes eligibility through
        the value_sets dict under key 'eligibility' (a LazyFrame).
        """
        period_start, period_end = self._period_bounds()
        # ACO REACH spec uses a 365-day measurement window even in leap years.
        period_days = 365

        eligibility = value_sets.get("eligibility")
        if eligibility is None:
            raise ValueError(
                "DaysAtHome.calculate_numerator requires value_sets['eligibility']; "
                "the mx_validate pipeline injects it."
            )

        bene_dates = (
            denominator.select("person_id")
            .join(
                eligibility.select(
                    [
                        "person_id",
                        pl.col("birth_date").cast(pl.Date, strict=False).alias("dob"),
                        pl.col("death_date").cast(pl.Date, strict=False).alias("dod"),
                        pl.col("enrollment_start_date").cast(pl.Date, strict=False),
                        pl.col("enrollment_end_date").cast(pl.Date, strict=False),
                    ]
                ),
                on="person_id",
                how="left",
            )
            .group_by("person_id")
            .agg(
                [
                    pl.col("dod").min().alias("dod"),
                    pl.col("enrollment_start_date").min().alias("enroll_start"),
                    pl.col("enrollment_end_date").max().alias("enroll_end"),
                ]
            )
        )

        # Survival days inside the window: from max(period_start, enroll_start)
        # to min(period_end, dod, enroll_end). Clamp to [0, period_days].
        # We keep the simple ACO REACH form: survival_days = min(period_end, dod) - period_start + 1.
        survival = bene_dates.with_columns(
            [
                pl.when(pl.col("dod").is_not_null() & (pl.col("dod") <= pl.lit(period_end)))
                .then(
                    (pl.col("dod") - pl.lit(period_start)).dt.total_days() + 1
                )
                .otherwise(period_days)
                .clip(0, period_days)
                .cast(pl.Int64)
                .alias("survival_days"),
            ]
        )

        # Institutional days: sum of (discharge - admission + 1) for
        # qualifying bill types overlapping the measurement period.
        # Cap each stay at the window and at 1 day minimum when admit==discharge.
        inst_claims = (
            claims.filter(
                pl.col("bill_type_code")
                .cast(pl.Utf8)
                .str.slice(0, 2)
                .is_in(list(_INSTITUTIONAL_BILL_PREFIXES))
            )
            .filter(
                pl.col("admission_date").is_not_null()
                & pl.col("discharge_date").is_not_null()
            )
            .select(
                [
                    "person_id",
                    pl.col("admission_date").cast(pl.Date, strict=False).alias("admit"),
                    pl.col("discharge_date").cast(pl.Date, strict=False).alias("disch"),
                ]
            )
            .filter(
                (pl.col("admit") <= pl.lit(period_end))
                & (pl.col("disch") >= pl.lit(period_start))
            )
            .with_columns(
                [
                    pl.max_horizontal([pl.col("admit"), pl.lit(period_start)]).alias(
                        "admit_clip"
                    ),
                    pl.min_horizontal([pl.col("disch"), pl.lit(period_end)]).alias(
                        "disch_clip"
                    ),
                ]
            )
            .with_columns(
                ((pl.col("disch_clip") - pl.col("admit_clip")).dt.total_days() + 1)
                .clip(0, period_days)
                .cast(pl.Int64)
                .alias("stay_days")
            )
            .group_by("person_id")
            .agg(pl.col("stay_days").sum().alias("observed_dic"))
        )

        result = (
            denominator.select("person_id")
            .join(survival, on="person_id", how="left")
            .join(inst_claims, on="person_id", how="left")
            .with_columns(
                [
                    pl.col("survival_days").fill_null(period_days),
                    pl.col("observed_dic").fill_null(0).cast(pl.Int64),
                ]
            )
            .with_columns(
                (pl.col("survival_days") - pl.col("observed_dic"))
                .clip(0, period_days)
                .cast(pl.Int64)
                .alias("observed_dah")
            )
            .select(
                [
                    "person_id",
                    "survival_days",
                    "observed_dic",
                    "observed_dah",
                    pl.lit(True).alias("numerator_flag"),
                ]
            )
        )
        return result

    def calculate_exclusions(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """DAH has no formal exclusions at this layer."""
        return denominator.select("person_id").with_columns(
            pl.lit(False).alias("exclusion_flag")
        )


MeasureFactory.register("REACH_DAH", DaysAtHome)
logger.debug("Registered DAH (REACH) quality measure and transform")
