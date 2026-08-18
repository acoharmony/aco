# © 2025 HarmonyCares
# All rights reserved.

"""MSSP → REACH transition analytics.

Backs ``notebooks/transitions.py``: describes beneficiaries whose consolidated
alignment semantics indicate prior MSSP attribution and current REACH
attribution after reconciliation.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry


class TransitionsPlugins(PluginRegistry):
    """Year-over-year MSSP → REACH transition analytics."""

    def load_consolidated(self, gold_path: Path) -> pl.LazyFrame:
        path = Path(gold_path) / "consolidated_alignment.parquet"
        return pl.scan_parquet(str(path)) if path.exists() else pl.LazyFrame()

    def year_windows(self, df: pl.LazyFrame) -> pl.DataFrame:
        """Return first/last available month per year with both MSSP and REACH flags."""
        months = sorted(
            {
                col.split("_")[1]
                for col in df.collect_schema().names()
                if col.startswith("ym_") and col.endswith("_mssp")
            }
        )
        rows: list[dict[str, Any]] = []
        for year in sorted({month[:4] for month in months}):
            year_months = [
                month
                for month in months
                if month.startswith(year) and f"ym_{month}_reach" in df.collect_schema().names()
            ]
            if len(year_months) < 2:
                continue
            rows.append(
                {
                    "year": int(year),
                    "initial_ym": year_months[0],
                    "final_ym": year_months[-1],
                    "initial_month": self._format_ym(year_months[0]),
                    "final_month": self._format_ym(year_months[-1]),
                }
            )
        return pl.DataFrame(rows)

    def transition_summary(self, df: pl.LazyFrame) -> pl.DataFrame:
        schema = df.collect_schema().names()
        if {"previous_program", "current_program"}.issubset(schema):
            transition_filter = (pl.col("previous_program") == "MSSP") & (
                pl.col("current_program") == "REACH"
            )
            current_year_expr = (
                pl.col("transition_analysis_current_year")
                if "transition_analysis_current_year" in schema
                else pl.lit(None)
            )
            previous_year_expr = (
                pl.col("transition_analysis_previous_year")
                if "transition_analysis_previous_year" in schema
                else pl.lit(None)
            )
            source_col = "newly_added_source_2025_to_2026"
            source_expr = (
                pl.col(source_col)
                if source_col in schema
                else pl.lit("Previous MSSP → Current REACH")
            )
            out = (
                df.with_columns(
                    current_year_expr.alias("year"),
                    previous_year_expr.alias("previous_year"),
                    source_expr.alias("transition_source"),
                    transition_filter.alias("mssp_to_reach"),
                    (pl.col("previous_program") == "MSSP").alias("prior_mssp_transition"),
                )
                .group_by(["previous_year", "year", "transition_source"])
                .agg(
                    pl.col("prior_mssp_transition").sum().alias("prior_mssp_patients"),
                    pl.col("mssp_to_reach").sum().alias("ended_reach_patients"),
                )
                .filter(pl.col("ended_reach_patients") > 0)
                .with_columns(
                    pl.lit("semantic transition fields").alias("basis"),
                    pl.col("previous_year").cast(pl.Utf8).alias("initial_month"),
                    pl.col("year").cast(pl.Utf8).alias("final_month"),
                    pl.col("prior_mssp_patients").alias("initial_mssp_patients"),
                    pl.lit(None, dtype=pl.UInt32).alias("retained_mssp_patients"),
                    pl.lit(None, dtype=pl.UInt32).alias("ended_ffs_patients"),
                    pl.lit(None, dtype=pl.UInt32).alias("other_or_unclassified"),
                    (pl.col("ended_reach_patients") / pl.col("prior_mssp_patients") * 100)
                    .round(2)
                    .alias("mssp_to_reach_rate"),
                )
                .select(
                    [
                        "previous_year",
                        "year",
                        "basis",
                        "transition_source",
                        "initial_month",
                        "final_month",
                        "initial_mssp_patients",
                        "ended_reach_patients",
                        "retained_mssp_patients",
                        "ended_ffs_patients",
                        "other_or_unclassified",
                        "mssp_to_reach_rate",
                    ]
                )
                .sort(["year", "ended_reach_patients"], descending=[False, True])
                .collect()
            )
            if not out.is_empty():
                return out

        rows: list[dict[str, Any]] = []
        for window in self.year_windows(df).iter_rows(named=True):
            initial_mssp = self._flag(f"ym_{window['initial_ym']}_mssp")
            final_mssp = self._flag(f"ym_{window['final_ym']}_mssp")
            final_reach = self._flag(f"ym_{window['final_ym']}_reach")
            final_ffs = self._flag(f"ym_{window['final_ym']}_ffs")

            base = df.filter(initial_mssp)
            initial_count = self._count(base)
            transitioned = self._count(base.filter(final_reach))
            retained_mssp = self._count(base.filter(final_mssp))
            moved_ffs = self._count(base.filter(final_ffs))
            other = max(initial_count - transitioned - retained_mssp - moved_ffs, 0)

            rows.append(
                {
                    **window,
                    "initial_mssp_patients": initial_count,
                    "ended_reach_patients": transitioned,
                    "retained_mssp_patients": retained_mssp,
                    "ended_ffs_patients": moved_ffs,
                    "other_or_unclassified": other,
                    "mssp_to_reach_rate": round(transitioned / initial_count * 100, 2)
                    if initial_count
                    else 0.0,
                }
            )
        return pl.DataFrame(rows).sort("year")

    def transition_detail(self, df: pl.LazyFrame, year: int | str) -> pl.DataFrame:
        schema = df.collect_schema().names()
        if {"previous_program", "current_program"}.issubset(schema):
            filters = (pl.col("previous_program") == "MSSP") & (
                pl.col("current_program") == "REACH"
            )
            if "transition_analysis_current_year" in schema:
                filters = filters & (pl.col("transition_analysis_current_year") == int(year))
            cols = self._detail_cols(schema)
            return (
                df.filter(filters)
                .select(cols)
                .with_columns(
                    pl.lit(int(year)).alias("transition_year"),
                    (
                        pl.col("transition_analysis_previous_year").cast(pl.Utf8)
                        if "transition_analysis_previous_year" in schema
                        else pl.lit(None)
                    ).alias("initial_mssp_month"),
                    (
                        pl.col("transition_analysis_current_year").cast(pl.Utf8)
                        if "transition_analysis_current_year" in schema
                        else pl.lit(None)
                    ).alias("final_reach_month"),
                )
                .sort(["mssp_provider_name", "reach_provider_name", "bene_mbi"])
                .collect()
            )

        window = self._window_for_year(df, year)
        if window is None:
            return pl.DataFrame()

        initial_mssp = self._flag(f"ym_{window['initial_ym']}_mssp")
        final_reach = self._flag(f"ym_{window['final_ym']}_reach")
        schema = df.collect_schema().names()
        cols = self._detail_cols(schema)

        return (
            df.filter(initial_mssp & final_reach)
            .select(cols)
            .with_columns(
                pl.lit(int(window["year"])).alias("transition_year"),
                pl.lit(window["initial_month"]).alias("initial_mssp_month"),
                pl.lit(window["final_month"]).alias("final_reach_month"),
            )
            .sort(["mssp_provider_name", "reach_provider_name", "bene_mbi"])
            .collect()
        )

    def provider_handoffs(self, detail_df: pl.DataFrame) -> pl.DataFrame:
        if detail_df.is_empty():
            return detail_df
        group_cols = [
            col
            for col in ["mssp_provider_name", "reach_provider_name", "reach_attribution_type"]
            if col in detail_df.columns
        ]
        return (
            detail_df.group_by(group_cols)
            .agg(pl.len().alias("patients"))
            .sort("patients", descending=True)
        )

    def attribution_type_breakdown(self, detail_df: pl.DataFrame) -> pl.DataFrame:
        if detail_df.is_empty() or "reach_attribution_type" not in detail_df.columns:
            return pl.DataFrame()
        return (
            detail_df.group_by("reach_attribution_type")
            .agg(pl.len().alias("patients"))
            .with_columns(
                (pl.col("patients") / pl.col("patients").sum() * 100).round(2).alias("pct")
            )
            .sort("patients", descending=True)
        )

    def summary_metrics(self, summary_df: pl.DataFrame) -> dict[str, Any]:
        if summary_df.is_empty():
            return {
                "years": 0,
                "total_initial_mssp": 0,
                "total_transitioned": 0,
                "best_year": "N/A",
                "best_rate": 0.0,
            }
        best = summary_df.sort("mssp_to_reach_rate", descending=True).row(0, named=True)
        return {
            "years": summary_df.height,
            "total_initial_mssp": int(summary_df["initial_mssp_patients"].sum()),
            "total_transitioned": int(summary_df["ended_reach_patients"].sum()),
            "best_year": int(best["year"]),
            "best_rate": float(best["mssp_to_reach_rate"]),
        }

    def _window_for_year(self, df: pl.LazyFrame, year: int | str) -> dict[str, Any] | None:
        windows = self.year_windows(df).filter(pl.col("year") == int(year))
        return None if windows.is_empty() else windows.row(0, named=True)

    def _detail_cols(self, schema: list[str]) -> list[str]:
        return [
            c
            for c in [
                "bene_mbi",
                "current_mbi",
                "current_program",
                "previous_program",
                "transition_analysis_previous_year",
                "transition_analysis_current_year",
                "months_in_mssp",
                "months_in_reach",
                "first_mssp_date",
                "last_mssp_date",
                "first_reach_date",
                "last_reach_date",
                "mssp_provider_name",
                "mssp_tin",
                "mssp_npi",
                "reach_provider_name",
                "reach_tin",
                "reach_npi",
                "reach_attribution_type",
                "has_valid_voluntary_alignment",
                "voluntary_alignment_date",
                "voluntary_alignment_type",
                "mssp_to_reach_status",
                "newly_added_source_2025_to_2026",
                "source_tables",
            ]
            if c in schema
        ]

    def _count(self, df: pl.LazyFrame) -> int:
        return int(df.select(pl.len()).collect().item())

    def _flag(self, col: str) -> pl.Expr:
        return pl.col(col).fill_null(False)

    def _format_ym(self, ym: str) -> str:
        return f"{ym[:4]}-{ym[4:]}"
