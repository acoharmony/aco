# © 2025 HarmonyCares
# All rights reserved.

"""
Per Member Per Month (PMPM) financial analytics.

Backs ``notebooks/analytics.py``: deduplicates member-months,
aggregates spend by program / category / both, computes year-over-year
PMPM and category-spend pivots, and produces the per-program/per-year
detail used by the interactive explorer.

The gold-tier source is ``financial_pmpm_by_category.parquet``, which
has one row per ``(program, category, year_month)``. Member-months
appear on every row, so deduplication is required before summing.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from ._base import PluginRegistry

PROGRAM_NAMES = {
    "ffs": "Fee-for-Service",
    "mssp": "Medicare Shared Savings Program",
    "reach": "REACH ACO",
}


class PmpmPlugins(PluginRegistry):
    """PMPM rollups, year-over-year comparisons, and program/year drilldown."""

    def deduplicate_member_months(self, pmpm_df: pl.DataFrame) -> pl.DataFrame:
        """Distinct ``(year_month, program)`` rows with their member_months."""
        return pmpm_df.select("year_month", "program", "member_months").unique(
            subset=["year_month", "program"]
        )

    # ---- overall summary ----------------------------------------------

    def overall_summary(self, pmpm_df: pl.DataFrame) -> dict[str, Any]:
        """Top-level totals + counts for the dashboard summary cards."""
        total_spend = float(pmpm_df["total_spend"].sum())
        deduped = self.deduplicate_member_months(pmpm_df)
        total_mm = int(deduped["member_months"].sum())
        return {
            "total_spend": total_spend,
            "total_member_months": total_mm,
            "overall_pmpm": total_spend / total_mm if total_mm > 0 else 0,
            "unique_programs": pmpm_df["program"].n_unique(),
            "unique_categories": pmpm_df["category"].n_unique(),
        }

    # ---- per-axis rollups ---------------------------------------------

    def by_program(self, pmpm_df: pl.DataFrame) -> pl.DataFrame:
        """Spend / member-months / PMPM per program."""
        spend = (
            pmpm_df.group_by("program")
            .agg(pl.col("total_spend").sum().alias("total_spend"))
        )
        deduped = self.deduplicate_member_months(pmpm_df)
        mm = (
            deduped.group_by("program")
            .agg(pl.col("member_months").sum().alias("member_months"))
        )
        return (
            spend.join(mm, on="program", how="left")
            .with_columns((pl.col("total_spend") / pl.col("member_months")).alias("pmpm"))
            .sort("pmpm", descending=True)
        )

    def by_category(self, pmpm_df: pl.DataFrame, n: int = 20) -> pl.DataFrame:
        """Top-N service categories by spend with PMPM against total member-months."""
        spend = (
            pmpm_df.group_by("category")
            .agg(pl.col("total_spend").sum().alias("total_spend"))
        )
        total_mm = self.deduplicate_member_months(pmpm_df)["member_months"].sum()
        return (
            spend.with_columns(pl.lit(total_mm).alias("member_months"))
            .with_columns((pl.col("total_spend") / pl.col("member_months")).alias("pmpm"))
            .sort("total_spend", descending=True)
            .head(n)
        )

    def by_program_category(self, pmpm_df: pl.DataFrame, n: int = 30) -> pl.DataFrame:
        """Top combos by spend with PMPM scoped to each program's member-months."""
        spend = (
            pmpm_df.group_by("program", "category")
            .agg(pl.col("total_spend").sum().alias("total_spend"))
        )
        deduped = self.deduplicate_member_months(pmpm_df)
        mm_by_prog = (
            deduped.group_by("program")
            .agg(pl.col("member_months").sum().alias("member_months"))
        )
        return (
            spend.join(mm_by_prog, on="program", how="left")
            .with_columns((pl.col("total_spend") / pl.col("member_months")).alias("pmpm"))
            .sort("total_spend", descending=True)
            .head(n)
        )

    # ---- year-over-year ------------------------------------------------

    def yoy_pmpm_by_program(self, pmpm_df: pl.DataFrame) -> pl.DataFrame:
        """Wide pivot: rows are year, columns are programs, values are PMPM."""
        with_year = pmpm_df.with_columns((pl.col("year_month") // 100).alias("year"))
        spend = (
            with_year.group_by("year", "program")
            .agg(pl.col("total_spend").sum().alias("total_spend"))
        )
        deduped = self.deduplicate_member_months(with_year).with_columns(
            (pl.col("year_month") // 100).alias("year")
        )
        mm = (
            deduped.group_by("year", "program")
            .agg(pl.col("member_months").sum().alias("member_months"))
        )
        long = (
            spend.join(mm, on=["year", "program"], how="left")
            .with_columns((pl.col("total_spend") / pl.col("member_months")).alias("pmpm"))
            .sort("year", "program", descending=[True, False])
        )
        return long.pivot(index="year", on="program", values="pmpm").sort(
            "year", descending=True
        )

    def yoy_spend_by_category(
        self,
        pmpm_df: pl.DataFrame,
        top_n: int = 15,
        sort_year: str = "2025",
    ) -> pl.DataFrame:
        """Wide pivot: rows are top-N categories, columns are years, values are spend."""
        with_year = pmpm_df.with_columns((pl.col("year_month") // 100).alias("year"))
        top_categories = (
            with_year.group_by("category")
            .agg(pl.col("total_spend").sum().alias("total"))
            .sort("total", descending=True)
            .head(top_n)["category"]
            .to_list()
        )
        long = (
            with_year.filter(pl.col("category").is_in(top_categories))
            .group_by("year", "category")
            .agg(pl.col("total_spend").sum().alias("total_spend"))
            .sort("year", "category", descending=[True, False])
        )
        pivoted = long.pivot(index="category", on="year", values="total_spend")
        if sort_year in pivoted.columns:
            return pivoted.sort(pl.col(sort_year), descending=True, nulls_last=True)
        return pivoted

    # ---- per-program/year drilldown -----------------------------------

    def program_year_metrics(
        self,
        pmpm_df: pl.DataFrame,
        year: str | int,
        program: str,
    ) -> dict[str, Any] | None:
        """Detailed PMPM + category breakdown + monthly trend for one (program, year)."""
        year_int = int(year)
        filtered = pmpm_df.filter(
            (pl.col("year_month") >= year_int * 100 + 1)
            & (pl.col("year_month") <= year_int * 100 + 12)
            & (pl.col("program") == program)
        )
        if filtered.height == 0:
            return None

        total_spend = float(filtered["total_spend"].sum())
        unique_months = filtered.select("year_month", "member_months").unique(
            subset=["year_month"]
        )
        total_mm = int(unique_months["member_months"].sum())
        overall_pmpm = total_spend / total_mm if total_mm > 0 else 0

        by_category = (
            filtered.group_by("category")
            .agg(pl.col("total_spend").sum().alias("total_spend"))
            .with_columns(
                pl.lit(total_mm).alias("member_months"),
                (pl.col("total_spend") / pl.lit(total_mm)).alias("pmpm"),
            )
            .sort("total_spend", descending=True)
        )
        monthly_trend = (
            filtered.group_by("year_month")
            .agg(
                pl.col("total_spend").sum().alias("total_spend"),
                pl.col("member_months").first().alias("member_months"),
            )
            .with_columns((pl.col("total_spend") / pl.col("member_months")).alias("pmpm"))
            .sort("year_month")
        )
        return {
            "total_spend": total_spend,
            "total_member_months": total_mm,
            "overall_pmpm": overall_pmpm,
            "by_category": by_category,
            "monthly_trend": monthly_trend,
            "num_categories": by_category.height,
        }

    # ---- readmissions / service categories ----------------------------

    def readmissions_summary(self, readmissions_df: pl.DataFrame) -> dict[str, Any]:
        return {
            "total": readmissions_df.height,
            "unique_patients": readmissions_df["patient_id"].n_unique(),
            "avg_days": float(readmissions_df["days_to_readmission"].mean() or 0),
            "median_days": float(readmissions_df["days_to_readmission"].median() or 0),
        }

    def top_readmitted_patients(
        self,
        readmissions_df: pl.DataFrame,
        n: int = 10,
    ) -> pl.DataFrame:
        return (
            readmissions_df.group_by("patient_id")
            .agg(
                pl.len().alias("readmission_count"),
                pl.col("days_to_readmission").mean().alias("avg_days_to_readmission"),
            )
            .sort("readmission_count", descending=True)
            .head(n)
        )

    def service_category_summary(
        self,
        service_category_lf: pl.LazyFrame,
        n: int = 20,
    ) -> pl.DataFrame:
        return (
            service_category_lf.group_by("service_category_2")
            .agg(
                pl.len().alias("claim_count"),
                pl.col("paid").sum().alias("total_paid"),
                pl.col("person_id").n_unique().alias("unique_patients"),
            )
            .sort("total_paid", descending=True)
            .head(n)
            .collect()
        )
