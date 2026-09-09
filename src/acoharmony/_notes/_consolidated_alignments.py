# © 2025 HarmonyCares
# All rights reserved.

"""
Consolidated alignment dashboard analytics.

Backs ``notebooks/consolidated_alignments.py``: thin facade over the
``acoharmony._transforms._notebook_*`` modules that already contain the
bulk of the analytic logic for program-distribution, enrollment trends,
transitions, voluntary-alignment outreach, office stats, vintage, and
cohort analysis.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
from dateutil.relativedelta import relativedelta

from ._base import PluginRegistry


def _month_bounds(year_month: str) -> tuple[date, date]:
    year = int(year_month[:4])
    month = int(year_month[4:6])
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    return date(year, month, 1), next_month - relativedelta(days=1)


class ConsolidatedAlignmentsPlugins(PluginRegistry):
    """Consolidated alignment dashboard analytics."""

    # ---- loading ----------------------------------------------------

    def load_consolidated(self, gold_path: Path, silver_path: Path | None = None) -> pl.LazyFrame:
        path = Path(gold_path) / "consolidated_alignment.parquet"
        if not path.exists():
            return pl.LazyFrame()
        df = pl.scan_parquet(str(path))
        if silver_path is None:
            return df

        last_ffs_path = Path(silver_path) / "last_ffs_service.parquet"
        schema = df.collect_schema().names()
        if not last_ffs_path.exists() or "last_ffs_date" in schema or "current_mbi" not in schema:
            return df

        last_ffs = (
            pl.scan_parquet(str(last_ffs_path))
            .select([pl.col("bene_mbi").alias("current_mbi"), "last_ffs_date"])
            .unique(subset=["current_mbi"], keep="first")
        )
        return df.join(last_ffs, on="current_mbi", how="left")

    def load_emails(self, silver_path: Path) -> pl.LazyFrame:
        path = Path(silver_path) / "emails.parquet"
        return pl.scan_parquet(str(path)) if path.exists() else pl.LazyFrame()

    def load_mailed(self, silver_path: Path) -> pl.LazyFrame:
        path = Path(silver_path) / "mailed.parquet"
        return pl.scan_parquet(str(path)) if path.exists() else pl.LazyFrame()

    # ---- filters / utilities -----------------------------------------

    def living_filter(self, df: pl.LazyFrame | None) -> pl.Expr:
        """Filter expression: living beneficiaries only (handles missing death cols)."""
        if df is None:
            return pl.lit(True)
        try:
            schema_names = df.collect_schema().names()
        except Exception:  # ALLOWED: empty/invalid LazyFrame
            return pl.lit(True)
        cond = pl.lit(True)
        if "death_date" in schema_names:
            cond = cond & pl.col("death_date").is_null()
        if "bene_death_date" in schema_names:
            cond = cond & pl.col("bene_death_date").is_null()
        if "bene_date_of_death" in schema_names:
            cond = cond & pl.col("bene_date_of_death").is_null()
        return cond

    def extract_year_months(self, df: pl.LazyFrame) -> tuple[str | None, list[str]]:
        ym_cols = [c for c in df.collect_schema().names() if c.startswith("ym_")]
        if not ym_cols:
            return None, []
        year_months = sorted({c.split("_")[1] for c in ym_cols})
        return year_months[-1], year_months

    def basic_stats(self, df: pl.LazyFrame) -> dict[str, Any]:
        from acoharmony._transforms._notebook_utilities import calculate_basic_stats

        return calculate_basic_stats(df)

    # ---- distributions ----------------------------------------------

    def historical_program_distribution(self, df: pl.LazyFrame) -> pl.DataFrame:
        from acoharmony._transforms._notebook_utilities import (
            calculate_historical_program_distribution,
        )

        return calculate_historical_program_distribution(df)

    def current_program_distribution(
        self, df: pl.LazyFrame, most_recent_ym: str | None
    ) -> pl.DataFrame:
        from acoharmony._transforms._notebook_utilities import (
            calculate_current_program_distribution,
        )

        return calculate_current_program_distribution(df, most_recent_ym)

    # ---- voluntary alignment outreach ---------------------------------

    def voluntary_outreach_data(
        self, emails_df: pl.LazyFrame, mailed_df: pl.LazyFrame
    ) -> dict[str, pl.LazyFrame]:
        """Return ``{email_by_campaign, email_mbis, mailed_by_campaign, mailed_mbis}``."""
        from acoharmony._transforms._notebook_utilities import (
            prepare_voluntary_outreach_data,
        )

        email_by_campaign, email_mbis, mailed_by_campaign, mailed_mbis = (
            prepare_voluntary_outreach_data(emails_df, mailed_df)
        )
        return {
            "email_by_campaign": email_by_campaign,
            "email_mbis": email_mbis,
            "mailed_by_campaign": mailed_by_campaign,
            "mailed_mbis": mailed_mbis,
        }

    def quarterly_campaign_effectiveness(
        self,
        df_enriched: pl.LazyFrame,
        email_by_campaign: pl.LazyFrame,
        mailed_by_campaign: pl.LazyFrame,
    ) -> pl.DataFrame:
        from acoharmony._transforms._notebook_outreach import (
            calculate_quarterly_campaign_effectiveness,
        )

        return calculate_quarterly_campaign_effectiveness(
            df_enriched, email_by_campaign, mailed_by_campaign
        )

    def office_campaign_effectiveness(
        self,
        df_enriched: pl.LazyFrame,
        email_by_campaign: pl.LazyFrame,
        mailed_by_campaign: pl.LazyFrame,
    ) -> pl.DataFrame:
        from acoharmony._transforms._notebook_outreach import (
            calculate_office_campaign_effectiveness,
        )

        return calculate_office_campaign_effectiveness(
            df_enriched, email_by_campaign, mailed_by_campaign
        )

    def enhanced_campaign_performance(
        self, emails_df: pl.LazyFrame, mailed_df: pl.LazyFrame
    ) -> dict[str, Any]:
        from acoharmony._transforms._notebook_outreach import (
            calculate_enhanced_campaign_performance,
        )

        return calculate_enhanced_campaign_performance(emails_df, mailed_df)

    # ---- enrollment + trends ------------------------------------------

    def selected_month_enrollment(
        self, df: pl.LazyFrame, selected_ym: str | None
    ) -> dict[str, int] | None:
        if not selected_ym:
            return None
        df_active = df.filter(self.living_filter(df))
        schema = df_active.collect_schema().names()

        def status_expr(col: str) -> pl.Expr:
            if col in schema:
                return pl.col(col).fill_null(False)
            return pl.lit(False)

        reach = status_expr(f"ym_{selected_ym}_reach")
        mssp = status_expr(f"ym_{selected_ym}_mssp")
        if "last_ffs_date" in schema:
            _, month_end = _month_bounds(selected_ym)
            lookback_start = month_end - relativedelta(months=24)
            ffs = (
                pl.col("last_ffs_date")
                .cast(pl.Date, strict=False)
                .is_between(lookback_start, month_end, closed="both")
                .fill_null(False)
                & ~reach
                & ~mssp
            )
        else:
            ffs = status_expr(f"ym_{selected_ym}_ffs") & ~reach & ~mssp
        normalized = df_active.with_columns(
            [
                reach.alias("_selected_reach"),
                mssp.alias("_selected_mssp"),
                ffs.alias("_selected_ffs"),
            ]
        )
        id_column = next(
            (col for col in ("current_mbi", "bene_mbi", "mbi") if col in schema),
            None,
        )
        if id_column:
            population = normalized.group_by(id_column).agg(
                [
                    pl.col("_selected_reach").any(),
                    pl.col("_selected_mssp").any(),
                    pl.col("_selected_ffs").any(),
                ]
            )
        else:
            population = normalized.select(["_selected_reach", "_selected_mssp", "_selected_ffs"])
        result = population.select(
            [
                pl.col("_selected_reach").sum().alias("REACH"),
                pl.col("_selected_mssp").sum().alias("MSSP"),
                (pl.col("_selected_ffs") & ~pl.col("_selected_reach") & ~pl.col("_selected_mssp"))
                .sum()
                .alias("FFS"),
                (~(pl.col("_selected_reach") | pl.col("_selected_mssp") | pl.col("_selected_ffs")))
                .sum()
                .alias("Not Enrolled"),
                pl.len().alias("Living Beneficiaries"),
            ]
        ).collect()
        return {key: int(value or 0) for key, value in result.row(0, named=True).items()}

    def alignment_trends(self, df: pl.LazyFrame, year_months: list[str]) -> pl.DataFrame | None:
        from acoharmony._transforms._notebook_trends import (
            calculate_alignment_trends_over_time,
        )

        return calculate_alignment_trends_over_time(df, year_months)

    def transitions(self, df: pl.LazyFrame, prev_ym: str, curr_ym: str) -> pl.DataFrame:
        from acoharmony._transforms._notebook_transitions import (
            calculate_alignment_transitions,
        )

        result = calculate_alignment_transitions(df, curr_ym, [prev_ym, curr_ym])
        if isinstance(result, tuple):
            transitions, _, _ = result
        else:
            transitions = result
        return transitions if transitions is not None else pl.DataFrame()

    # ---- office breakdowns -------------------------------------------

    def office_enrollment(self, df: pl.LazyFrame, selected_ym: str) -> pl.DataFrame | None:
        from acoharmony._transforms._notebook_office_stats import (
            calculate_office_enrollment_stats,
        )

        return calculate_office_enrollment_stats(df, selected_ym)

    def office_alignment_types(self, df: pl.LazyFrame, selected_ym: str) -> pl.DataFrame | None:
        from acoharmony._transforms._notebook_office_stats import (
            calculate_office_alignment_types,
        )

        return calculate_office_alignment_types(df, selected_ym)

    def office_program_distribution(
        self, df: pl.LazyFrame, selected_ym: str
    ) -> pl.DataFrame | None:
        from acoharmony._transforms._notebook_office_stats import (
            calculate_office_program_distribution,
        )

        return calculate_office_program_distribution(df, selected_ym)

    def office_transitions(self, df: pl.LazyFrame) -> pl.DataFrame | None:
        from acoharmony._transforms._notebook_office_stats import (
            calculate_office_transition_stats,
        )

        return calculate_office_transition_stats(df)

    # ---- SVA -----------------------------------------------------------

    def sva_action_categories(self, df_enriched: pl.LazyFrame) -> pl.DataFrame:
        return (
            df_enriched.group_by("sva_action_needed")
            .agg(pl.len().alias("count"))
            .collect()
            .sort("count", descending=True)
        )

    # ---- sample preview ---------------------------------------------

    def sample(self, df: pl.LazyFrame, sample_size: int = 100) -> pl.DataFrame:
        desired = (
            "current_mbi",
            "consolidated_program",
            "has_voluntary_alignment",
            "months_in_reach",
            "months_in_mssp",
            "office_location",
            "has_valid_voluntary_alignment",
            "has_voluntary_outreach",
            "voluntary_email_count",
            "voluntary_letter_count",
        )
        available = df.collect_schema().names()
        cols = [c for c in desired if c in available]
        if "current_mbi" not in cols and "current_mbi" in available:
            cols.insert(0, "current_mbi")
        if not cols:
            return df.head(sample_size).collect()
        return df.select(cols).head(sample_size).collect()
