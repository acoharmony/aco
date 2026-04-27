# © 2025 HarmonyCares
# All rights reserved.

"""
SVA (Signed Voluntary Alignment) analytics for the alignment notebooks.

Covers loaders + consolidators for SVA submissions, the most-recent
BAR snapshot, and PBVAR enrichment. The voluntary-alignment
beneficiaries notebook composes these into a single enriched list;
the SVA dashboards reuse the same loaders.
"""

from __future__ import annotations

from datetime import date as _date
from pathlib import Path

import polars as pl

from ._base import PluginRegistry


class SvaPlugins(PluginRegistry):
    """SVA / BAR / PBVAR loaders and the consolidated-list helpers."""

    # ---- silver loaders -----------------------------------------------

    def load_sva_last_n(
        self,
        silver_path: Path,
        n: int = 3,
    ) -> tuple[pl.DataFrame, pl.DataFrame, list[_date]]:
        """
        Load SVA rows for the most recent ``n`` distinct file_dates.

        Returns ``(rows, counts_by_date, distinct_dates)``.
        """
        sva_lf = pl.scan_parquet(str(Path(silver_path) / "sva.parquet"))
        unique_dates = (
            sva_lf.select(pl.col("file_date").str.to_date())
            .unique()
            .sort("file_date", descending=True)
            .head(n)
            .collect()
        )
        sva_dates = unique_dates["file_date"].to_list()
        rows = (
            sva_lf.filter(pl.col("file_date").str.to_date().is_in(sva_dates))
            .select(
                pl.col("bene_mbi").alias("mbi"),
                pl.col("file_date").str.to_date().alias("submission_date"),
            )
            .collect()
        )
        counts = (
            rows.group_by("submission_date")
            .agg(pl.len().alias("count"))
            .sort("submission_date", descending=True)
        )
        return rows, counts, sva_dates

    def load_bar_latest(
        self,
        silver_path: Path,
    ) -> tuple[pl.DataFrame, _date]:
        """Most-recent file_date BAR rows + the file_date itself."""
        bar_lf = pl.scan_parquet(str(Path(silver_path) / "bar.parquet"))
        bar_date = (
            bar_lf.select(pl.col("file_date").str.to_date().max()).collect().item()
        )
        rows = (
            bar_lf.filter(pl.col("file_date").str.to_date() == bar_date)
            .select(
                pl.col("bene_mbi").alias("mbi"),
                pl.col("file_date").str.to_date().alias("file_date"),
            )
            .collect()
        )
        return rows, bar_date

    def load_pbvar(self, silver_path: Path) -> pl.DataFrame:
        """One row per MBI with most-recent PBVAR response codes + signature date."""
        pbvar_lf = pl.scan_parquet(str(Path(silver_path) / "pbvar.parquet"))
        return (
            pbvar_lf.group_by("bene_mbi")
            .agg(
                pl.col("sva_response_code_list").last().alias("pbvar_response_codes"),
                pl.col("file_date").str.to_date().max().alias("pbvar_file_date"),
                pl.col("sva_signature_date").max().alias("most_recent_sva_date"),
            )
            .select(
                pl.col("bene_mbi").alias("mbi"),
                pl.col("pbvar_response_codes"),
                pl.col("pbvar_file_date"),
                pl.col("most_recent_sva_date"),
            )
            .collect()
        )

    # ---- consolidation -------------------------------------------------

    def consolidate(
        self,
        sva_rows: pl.DataFrame,
        bar_rows: pl.DataFrame,
    ) -> pl.DataFrame:
        """Union SVA + BAR MBIs with their source labels and latest date."""
        sva_part = sva_rows.select(
            pl.col("mbi"),
            pl.lit("SVA").alias("source"),
            pl.col("submission_date").alias("source_date"),
        )
        bar_part = bar_rows.select(
            pl.col("mbi"),
            pl.lit("BAR").alias("source"),
            pl.col("file_date").alias("source_date"),
        )
        combined = pl.concat([sva_part, bar_part], how="diagonal")
        return (
            combined.group_by("mbi")
            .agg(
                pl.col("source").unique().sort().str.join(", ").alias("sources"),
                pl.col("source_date").max().alias("latest_date"),
            )
            .sort("latest_date", descending=True)
        )

    def enrich(
        self,
        consolidated: pl.DataFrame,
        pbvar: pl.DataFrame,
        silver_path: Path,
    ) -> pl.DataFrame:
        """Add HCMPI from identity_timeline + PBVAR fields to a consolidated list."""
        from acoharmony._transforms._identity_timeline import (
            current_mbi_with_hcmpi_lookup_lazy,
        )

        xwalk = current_mbi_with_hcmpi_lookup_lazy(silver_path)
        with_hcmpi = (
            consolidated.lazy()
            .join(
                xwalk.select(["crnt_num", "hcmpi"]).unique(
                    subset=["crnt_num"], keep="first"
                ),
                left_on="mbi",
                right_on="crnt_num",
                how="left",
            )
            .collect()
        )
        return (
            with_hcmpi.lazy()
            .join(
                pbvar.lazy().select(
                    "mbi", "pbvar_response_codes", "pbvar_file_date"
                ),
                on="mbi",
                how="left",
            )
            .collect()
        )

    def source_breakdown(self, final_list: pl.DataFrame) -> list[dict]:
        """SVA-only / BAR-only / both / total counts as a list-of-dicts table."""
        sva = final_list.filter(pl.col("sources").str.contains("SVA")).height
        bar = final_list.filter(pl.col("sources").str.contains("BAR")).height
        both = final_list.filter(
            pl.col("sources").str.contains("SVA")
            & pl.col("sources").str.contains("BAR")
        ).height
        return [
            {"Source": "SVA Only", "Count": sva - both},
            {"Source": "BAR Only", "Count": bar - both},
            {"Source": "Both SVA and BAR", "Count": both},
            {"Source": "**Total**", "Count": final_list.height},
        ]
