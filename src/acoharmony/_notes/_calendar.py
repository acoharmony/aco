# © 2025 HarmonyCares
# All rights reserved.

"""
ACO REACH Calendar analytics for the reach_calendar dashboard.

Backs ``notebooks/reach_calendar.py``: per-axis rollups, multi-axis
filter, upcoming/recent slices, monthly timeline, and deadline-only
list. The silver source is ``reach_calendar.parquet`` (latest
``file_date`` snapshot).
"""

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import polars as pl

from ._base import PluginRegistry


class CalendarPlugins(PluginRegistry):
    """Reach calendar slicing + summary helpers."""

    def load_latest_snapshot(self, silver_path: Path) -> pl.DataFrame:
        """Read ``reach_calendar.parquet``, keep only the most recent file_date."""
        path = Path(silver_path) / "reach_calendar.parquet"
        if not path.exists():
            raise FileNotFoundError(
                f"Calendar silver parquet not found: {path}. "
                "Run the bronze pipeline to ingest ACO_REACH_Calendar_updated_*.xlsx."
            )
        lf = pl.scan_parquet(str(path))
        if "file_date" in lf.collect_schema().names():
            latest = lf.select(pl.col("file_date").max()).collect().item()
            lf = lf.filter(pl.col("file_date") == latest)
        return lf.collect().with_columns(
            pl.col("category").str.strip_chars().alias("category")
        )

    # ---- per-axis rollups ---------------------------------------------

    def by_performance_year(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.group_by("py")
            .agg(
                pl.len().alias("event_count"),
                pl.col("category").n_unique().alias("unique_categories"),
                pl.col("type").n_unique().alias("unique_types"),
            )
            .sort("py", nulls_last=True)
        )

    def by_category(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.group_by("category")
            .agg(
                pl.len().alias("event_count"),
                pl.col("type").n_unique().alias("unique_types"),
                pl.col("start_date").min().alias("earliest_date"),
                pl.col("start_date").max().alias("latest_date"),
            )
            .sort("event_count", descending=True)
        )

    def by_type(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.group_by("type")
            .agg(
                pl.len().alias("event_count"),
                pl.col("category").n_unique().alias("unique_categories"),
            )
            .sort("event_count", descending=True)
        )

    # ---- multi-axis filter --------------------------------------------

    def filter_events(
        self,
        df: pl.DataFrame,
        categories: list[str] | None = None,
        pys: list[str] | None = None,
        types: list[str] | None = None,
    ) -> pl.DataFrame:
        """Multi-axis filter — empty list / ``None`` means no filter on that axis."""
        out = df
        if categories:
            out = out.filter(pl.col("category").is_in(categories))
        if pys:
            py_values = [int(py) for py in pys]
            out = out.filter(pl.col("py").is_in(py_values))
        if types:
            out = out.filter(pl.col("type").is_in(types))
        return out

    # ---- date-window slices -------------------------------------------

    def upcoming(self, df: pl.DataFrame, today: date | None = None, n: int = 20) -> pl.DataFrame:
        ref = today or date.today()
        return (
            df.filter(pl.col("start_date") >= ref)
            .select("py", "category", "type", "start_date", "description")
            .sort("start_date")
            .head(n)
        )

    def recent(
        self,
        df: pl.DataFrame,
        today: date | None = None,
        lookback_days: int = 90,
    ) -> pl.DataFrame:
        ref = today or date.today()
        cutoff = ref - timedelta(days=lookback_days)
        return (
            df.filter(pl.col("start_date") >= cutoff)
            .filter(pl.col("start_date") <= ref)
            .select("py", "category", "type", "start_date", "description")
            .sort("start_date", descending=True)
        )

    def monthly_timeline(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.with_columns(
                pl.col("start_date").dt.year().alias("year"),
                pl.col("start_date").dt.month().alias("month"),
            )
            .group_by("year", "month")
            .agg(
                pl.len().alias("event_count"),
                pl.col("category").n_unique().alias("unique_categories"),
            )
            .sort("year", "month")
        )

    def deadlines(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.filter(pl.col("type") == "Deadline")
            .select("py", "category", "start_date", "description", "links")
            .sort("start_date")
        )

    # ---- data quality --------------------------------------------------

    def null_counts(self, df: pl.DataFrame) -> dict[str, int]:
        cols = ("start_date", "category", "type", "py", "description")
        return {c: df[c].null_count() for c in cols if c in df.columns}
