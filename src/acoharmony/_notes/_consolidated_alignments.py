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

from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry


class ConsolidatedAlignmentsPlugins(PluginRegistry):
    """Consolidated alignment dashboard analytics."""

    # ---- loading ----------------------------------------------------

    def load_consolidated(self, gold_path: Path) -> pl.LazyFrame:
        path = Path(gold_path) / "consolidated_alignment.parquet"
        if not path.exists():
            return pl.LazyFrame()
        return pl.scan_parquet(str(path))

    def load_emails(self, silver_path: Path) -> pl.LazyFrame:
        path = Path(silver_path) / "emails.parquet"
        return pl.scan_parquet(str(path)) if path.exists() else pl.LazyFrame()

    def load_mailed(self, silver_path: Path) -> pl.LazyFrame:
        path = Path(silver_path) / "mailed.parquet"
        return pl.scan_parquet(str(path)) if path.exists() else pl.LazyFrame()

    # ---- filters / utilities -----------------------------------------

    def living_filter(self, df: pl.LazyFrame) -> pl.Expr:
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
        return cond

    def extract_year_months(
        self, df: pl.LazyFrame
    ) -> tuple[str | None, list[str]]:
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
        stats: dict[str, int] = {}
        for label, col in (
            ("REACH", f"ym_{selected_ym}_reach"),
            ("MSSP", f"ym_{selected_ym}_mssp"),
            ("FFS", f"ym_{selected_ym}_ffs"),
        ):
            stats[label] = (
                df_active.filter(pl.col(col)).select(pl.len()).collect().item()
                if col in schema
                else 0
            )
        total = df.select(pl.len()).collect().item()
        stats["Not Enrolled"] = (
            total - (stats["REACH"] + stats["MSSP"] + stats["FFS"])
        )
        return stats

    def alignment_trends(
        self, df: pl.LazyFrame, year_months: list[str]
    ) -> pl.DataFrame | None:
        from acoharmony._transforms._notebook_trends import (
            calculate_alignment_trends_over_time,
        )

        return calculate_alignment_trends_over_time(df, year_months)

    def transitions(
        self, df: pl.LazyFrame, prev_ym: str, curr_ym: str
    ) -> pl.DataFrame:
        from acoharmony._transforms._notebook_transitions import (
            calculate_alignment_transitions,
        )

        return calculate_alignment_transitions(df, prev_ym, curr_ym)

    # ---- office breakdowns -------------------------------------------

    def office_enrollment(
        self, df: pl.LazyFrame, selected_ym: str
    ) -> pl.DataFrame | None:
        from acoharmony._transforms._notebook_office_stats import (
            calculate_office_enrollment_stats,
        )

        return calculate_office_enrollment_stats(df, selected_ym)

    def office_alignment_types(
        self, df: pl.LazyFrame, selected_ym: str
    ) -> pl.DataFrame | None:
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

    def office_transitions(self, df: pl.LazyFrame) -> pl.DataFrame:
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
