# © 2025 HarmonyCares
# All rights reserved.

"""
ACO REACH analytics shared across the reach_* notebooks.

Year-over-year attribution analysis: pull BAR snapshots for two
windows (2025 and Jan 2026), diff the beneficiary sets, classify
term reasons against CRR death dates, and roll up temporal /
percentage breakdowns.

The schemas come from the silver tables ``bar.parquet`` and
``crr.parquet``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry


class ReachPlugins(PluginRegistry):
    """REACH attribution analytics for the y2y / cohort-loss dashboards."""

    # ---- BAR loading ---------------------------------------------------

    def load_bar(self, silver_path: Path) -> pl.LazyFrame:
        """Load ``bar.parquet`` and stamp a ``year_month`` column."""
        bar_path = Path(silver_path) / "bar.parquet"
        if not bar_path.exists():
            raise FileNotFoundError(f"BAR file not found: {bar_path}")
        return pl.scan_parquet(str(bar_path)).with_columns(
            (pl.col("start_date").dt.year() * 100 + pl.col("start_date").dt.month())
            .alias("year_month")
        )

    def benes_for_window(
        self,
        bar_lf: pl.LazyFrame,
        ym_min: int,
        ym_max: int | None = None,
    ) -> pl.DataFrame:
        """Distinct MBIs aligned within ``[ym_min, ym_max]`` (inclusive)."""
        upper = ym_max if ym_max is not None else ym_min
        return (
            bar_lf.filter(pl.col("year_month").is_between(ym_min, upper))
            .select(
                pl.col("bene_mbi").alias("mbi"),
                pl.col("year_month"),
                pl.col("start_date"),
            )
            .unique(subset=["mbi"])
            .collect()
        )

    def benes_for_month(self, bar_lf: pl.LazyFrame, year_month: int) -> pl.DataFrame:
        """Distinct MBIs aligned during a specific ``YYYYMM``."""
        return (
            bar_lf.filter(pl.col("year_month") == year_month)
            .select(
                pl.col("bene_mbi").alias("mbi"),
                pl.col("start_date"),
            )
            .collect()
        )

    # ---- y2y attribution diff ------------------------------------------

    def attribution_loss(
        self,
        benes_prev: pl.DataFrame,
        benes_next: pl.DataFrame,
    ) -> dict[str, Any]:
        """
        Set difference between two BAR snapshots.

        Returns ``{lost_mbis, lost_benes, total_lost, total_prev, total_next}``.
        ``lost_benes`` is the rows in ``benes_prev`` whose MBI no longer
        appears in ``benes_next``.
        """
        prev = set(benes_prev["mbi"].to_list())
        nxt = set(benes_next["mbi"].to_list())
        lost = prev - nxt
        return {
            "lost_mbis": lost,
            "lost_benes": benes_prev.filter(pl.col("mbi").is_in(list(lost))),
            "total_lost": len(lost),
            "total_prev": benes_prev.height,
            "total_next": benes_next.height,
        }

    # ---- term reason analysis -----------------------------------------

    def load_crr_for_lost(
        self,
        silver_path: Path,
        lost_mbis,
    ) -> pl.DataFrame | None:
        """CRR rows (mbi + bene_death_dt) for the lost cohort, or ``None``."""
        crr_path = Path(silver_path) / "crr.parquet"
        if not crr_path.exists():
            return None
        return (
            pl.scan_parquet(str(crr_path))
            .filter(pl.col("bene_mbi").is_in(list(lost_mbis)))
            .select(
                pl.col("bene_mbi").alias("mbi"),
                pl.col("bene_death_dt"),
            )
            .collect()
        )

    def lost_bar_records(
        self,
        bar_lf: pl.LazyFrame,
        lost_mbis,
    ) -> pl.DataFrame:
        """Latest BAR row per lost MBI: last_alignment_month, end_date, death_date, voluntary_type."""
        return (
            bar_lf.filter(pl.col("bene_mbi").is_in(list(lost_mbis)))
            .sort("year_month", descending=True)
            .group_by("bene_mbi")
            .agg(
                pl.col("year_month").first().alias("last_alignment_month"),
                pl.col("end_date").first().alias("end_date"),
                pl.col("bene_date_of_death").first().alias("death_date"),
                pl.col("voluntary_alignment_type").first().alias("voluntary_type"),
            )
            .collect()
            .rename({"bene_mbi": "mbi"})
        )

    def categorize_term_reasons(
        self,
        lost_bar_records: pl.DataFrame,
        lost_crr: pl.DataFrame | None,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Classify each lost MBI as Expired / Lost Provider / Other.

        Returns ``(per_mbi_categorized, term_summary_counts)``.
        """
        if lost_crr is not None:
            joined = lost_bar_records.join(lost_crr, on="mbi", how="left", suffix="_crr")
        else:
            joined = lost_bar_records.with_columns(pl.lit(None).alias("bene_death_dt"))

        categorized = joined.with_columns(
            pl.when(
                pl.col("death_date").is_not_null()
                | pl.col("bene_death_dt").is_not_null()
            )
            .then(pl.lit("Expired"))
            .when(pl.col("voluntary_type").is_not_null())
            .then(pl.lit("Lost Provider"))
            .otherwise(pl.lit("Other/Unknown"))
            .alias("term_category")
        )
        summary = (
            categorized.group_by("term_category")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
        )
        return categorized, summary

    def breakdown_stats(
        self,
        term_summary: pl.DataFrame,
        total_lost: int,
        has_end_date: int,
    ) -> dict[str, int]:
        """
        Flatten the term-summary into the dashboard's named buckets.

        ``Moved to MA`` / ``Moved to Hospice`` are zeros today — the BAR
        schema doesn't carry those flags. Kept in the shape so the
        dashboard layout doesn't have to special-case missing keys.
        """
        term_dict = {row["term_category"]: row["count"] for row in term_summary.to_dicts()}
        return {
            "Total Lost": total_lost,
            "With End Date": has_end_date,
            "Moved to MA": 0,
            "Moved to Hospice": 0,
            "Expired (SVA)": term_dict.get("Expired", 0),
            "Lost Provider": term_dict.get("Lost Provider", 0),
            "Other/Unknown Reason": term_dict.get("Other/Unknown", 0),
            "No End Date": total_lost - has_end_date,
        }

    # ---- temporal distribution -----------------------------------------

    def temporal_distribution(self, lost_bar_records: pl.DataFrame) -> pl.DataFrame:
        """Count of lost MBIs by their last_alignment_month (YYYYMM string)."""
        return (
            lost_bar_records.with_columns(
                pl.col("last_alignment_month").cast(pl.Utf8).alias("last_month_str")
            )
            .group_by("last_month_str")
            .agg(pl.len().alias("count"))
            .sort("last_month_str")
        )

    # ---- delivery provenance (calendar vs 4i inventory) ---------------

    @staticmethod
    def cadence_bucket(period: str | None) -> str:
        """Classify a period marker into a cadence bucket."""
        if period is None:
            return "unknown"
        if period.startswith("M"):
            return "monthly"
        if period.startswith("Q"):
            return "quarterly"
        if period.startswith("S"):
            return "semi_annual"
        if period == "A":
            return "annual"
        return "other"

    def delivery_status_pivot(
        self,
        df_prov: pl.DataFrame,
        statuses: tuple[str, ...] = ("on_time", "early", "late", "unscheduled"),
    ) -> pl.DataFrame:
        """One row per schema, columns are counts per status."""
        return (
            df_prov.filter(pl.col("schema_name").is_not_null())
            .group_by("schema_name")
            .agg(
                *[
                    (pl.col("delivery_status") == s).sum().alias(s)
                    for s in statuses
                ],
                pl.len().alias("total"),
            )
            .sort("total", descending=True)
        )

    def delivery_diff_stats(self, df_prov: pl.DataFrame) -> pl.DataFrame:
        """n / mean / median / quartiles / min / max of delivery_diff_days."""
        delivered = df_prov.filter(pl.col("delivery_diff_days").is_not_null())
        if delivered.height == 0:
            return pl.DataFrame()
        return delivered.select(
            pl.col("delivery_diff_days").count().alias("n"),
            pl.col("delivery_diff_days").mean().round(2).alias("mean_days"),
            pl.col("delivery_diff_days").median().alias("median_days"),
            pl.col("delivery_diff_days").quantile(0.25).alias("p25_days"),
            pl.col("delivery_diff_days").quantile(0.75).alias("p75_days"),
            pl.col("delivery_diff_days").min().alias("min_days"),
            pl.col("delivery_diff_days").max().alias("max_days"),
        )

    def delivery_outliers(
        self,
        df_prov: pl.DataFrame,
        kind: str,
        n: int = 20,
    ) -> pl.DataFrame:
        """Top-N late or early outlier rows."""
        descending = kind == "late"
        return (
            df_prov.filter(pl.col("delivery_status") == kind)
            .sort("delivery_diff_days", descending=descending)
            .select(
                "schema_name",
                "period",
                "py",
                "expected_date",
                "actual_delivery_date",
                "delivery_diff_days",
                "description",
            )
            .head(n)
        )

    def delivery_cadence(self, df_prov: pl.DataFrame) -> pl.DataFrame:
        """Dominant cadence + scheduled span per schema."""
        return (
            df_prov.filter(
                pl.col("schema_name").is_not_null()
                & pl.col("expected_date").is_not_null()
            )
            .with_columns(
                pl.col("period")
                .map_elements(self.cadence_bucket, return_dtype=pl.String)
                .alias("cadence_bucket")
            )
            .group_by("schema_name")
            .agg(
                pl.col("cadence_bucket").mode().first().alias("dominant_cadence"),
                pl.col("expected_date").min().alias("first_scheduled"),
                pl.col("expected_date").max().alias("last_scheduled"),
                pl.len().alias("scheduled_count"),
                pl.col("category").first().alias("category"),
            )
            .sort("scheduled_count", descending=True)
        )

    def delivery_trend(self, df_prov: pl.DataFrame) -> pl.DataFrame:
        """Mean lag days per (schema, year, quarter)."""
        delivered = df_prov.filter(pl.col("delivery_diff_days").is_not_null())
        if delivered.height == 0:
            return pl.DataFrame()
        return (
            delivered.with_columns(
                pl.col("expected_date").dt.year().alias("year"),
                ((pl.col("expected_date").dt.month() - 1) // 3 + 1).alias("quarter"),
            )
            .group_by("schema_name", "year", "quarter")
            .agg(
                pl.col("delivery_diff_days").mean().round(1).alias("mean_lag_days"),
                pl.len().alias("n_deliveries"),
            )
            .sort("schema_name", "year", "quarter")
        )

    def unexpected_deliveries(self, df_prov: pl.DataFrame) -> pl.DataFrame:
        """Rows tagged unscheduled — corrections / reissues / ad-hoc drops."""
        return (
            df_prov.filter(pl.col("delivery_status") == "unscheduled")
            .sort("actual_delivery_date", descending=True)
            .select(
                "schema_name",
                "period",
                "py",
                "actual_delivery_date",
                "actual_delivery_source",
                "delivered_file_count",
                "delivered_filenames",
            )
        )

    def schema_drilldown(self, df_prov: pl.DataFrame, schema: str) -> pl.DataFrame:
        """All scheduled and actual rows for a single schema, latest first."""
        return (
            df_prov.filter(pl.col("schema_name") == schema)
            .sort(
                pl.coalesce(["expected_date", "actual_delivery_date"]),
                descending=True,
                nulls_last=True,
            )
            .select(
                "period",
                "py",
                "expected_date",
                "actual_delivery_date",
                "delivery_diff_days",
                "delivery_status",
                "actual_delivery_source",
                "description",
                "delivered_file_count",
            )
        )
