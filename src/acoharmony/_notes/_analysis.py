# © 2025 HarmonyCares
# All rights reserved.

"""Generic summary / top-N analysis helpers."""

from __future__ import annotations

from typing import Any

import polars as pl

from ._base import PluginRegistry


class AnalysisPlugins(PluginRegistry):
    """Lightweight summary stats and top-N rollups."""

    def compute_summary(
        self,
        df: pl.DataFrame,
        metrics: list[str] | None = None,
        group_by: str | None = None,
    ) -> dict[str, Any]:
        summary: dict[str, Any] = {
            "total_rows": df.height,
            "total_columns": df.width,
        }
        if metrics is None:
            metrics = [c for c in df.columns if df[c].dtype in (pl.Float64, pl.Int64)]
        for metric in metrics:
            if metric in df.columns:
                summary[f"{metric}_sum"] = df[metric].sum()
                summary[f"{metric}_mean"] = df[metric].mean()
                summary[f"{metric}_max"] = df[metric].max()
                summary[f"{metric}_min"] = df[metric].min()
        for date_col in [c for c in df.columns if "date" in c.lower()]:
            summary[f"{date_col}_min"] = df[date_col].min()
            summary[f"{date_col}_max"] = df[date_col].max()
        return summary

    def top_n_analysis(
        self,
        df: pl.DataFrame,
        group_col: str,
        metric_col: str,
        n: int = 10,
        agg_func: str = "sum",
    ) -> pl.DataFrame:
        agg_map = {
            "sum": pl.col(metric_col).sum(),
            "mean": pl.col(metric_col).mean(),
            "count": pl.len(),
        }
        if agg_func not in agg_map:
            raise ValueError(f"Unsupported aggregation: {agg_func}")
        return (
            df.group_by(group_col)
            .agg(agg_map[agg_func].alias(f"{metric_col}_{agg_func}"), pl.len().alias("count"))
            .sort(f"{metric_col}_{agg_func}", descending=True)
            .head(n)
        )
