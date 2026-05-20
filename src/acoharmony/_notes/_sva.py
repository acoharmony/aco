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
        pbvar: pl.DataFrame | None = None,
    ) -> pl.DataFrame:
        """Union SVA + BAR + PBVAR MBIs with their source labels and latest date.

        PBVAR is CMS's response feed for ALL voluntary alignment activity, so a
        bene with PBVAR responses but no recent SVA submission still has
        voluntary-alignment history — including PBVAR in the source labels
        prevents these benes from looking like "BAR only" downstream.
        """
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
        parts = [sva_part, bar_part]
        if pbvar is not None and not pbvar.is_empty():
            pbvar_part = pbvar.select(
                pl.col("mbi"),
                pl.lit("PBVAR").alias("source"),
                pl.col("pbvar_file_date").alias("source_date"),
            )
            parts.append(pbvar_part)
        combined = pl.concat(parts, how="diagonal")
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
        """Add HCMPI + PBVAR response columns to the consolidated list.

        ``consolidate`` already labels PBVAR-only benes via the ``sources``
        column; this step just attaches the response code + file_date for
        every row that has a PBVAR record (regardless of source label).
        """
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

    # ---- Mabel log dashboard ------------------------------------------

    def parse_mabel_log(self, log_path: Path):
        """Wrap ``acoharmony._parsers._mabel_log.parse_mabel_log``."""
        from acoharmony._parsers._mabel_log import parse_mabel_log

        return parse_mabel_log(log_path)

    def mabel_log_kpis(
        self,
        df_events: pl.DataFrame,
        df_sessions: pl.DataFrame,
        df_uploads: pl.DataFrame,
        df_daily: pl.DataFrame,
    ) -> dict:
        """Top-line counts + date range for the upload-monitor dashboard header."""
        total_uploads = df_uploads.height
        days_active = df_daily.height
        sessions_with_uploads = df_sessions.filter(pl.col("files_uploaded") > 0).height
        return {
            "total_sessions": df_sessions.height,
            "total_uploads": total_uploads,
            "total_patients": df_uploads.select(
                pl.col("patient_name").drop_nulls().n_unique()
            ).item(),
            "sva_forms": df_uploads.filter(pl.col("is_sva_form")).height,
            "non_sva": total_uploads - df_uploads.filter(pl.col("is_sva_form")).height,
            "days_active": days_active,
            "avg_per_day": round(total_uploads / max(days_active, 1), 1),
            "sessions_with_uploads": sessions_with_uploads,
            "idle_sessions": df_sessions.height - sessions_with_uploads,
            "first_date": (
                df_events["timestamp"].min().strftime("%b %d, %Y")
                if df_events.height > 0
                else "N/A"
            ),
            "last_date": (
                df_events["timestamp"].max().strftime("%b %d, %Y")
                if df_events.height > 0
                else "N/A"
            ),
        }

    def filter_sessions(self, df_sessions: pl.DataFrame, kind: str) -> pl.DataFrame:
        """``kind`` ∈ {all, uploads, idle}."""
        if kind == "uploads":
            return df_sessions.filter(pl.col("files_uploaded") > 0)
        if kind == "idle":
            return df_sessions.filter(pl.col("files_uploaded") == 0)
        return df_sessions

    def filter_uploads(
        self,
        df_uploads: pl.DataFrame,
        patient_search: str = "",
        sva_only: bool = False,
    ) -> pl.DataFrame:
        """Search by patient (case-insensitive substring) and SVA-form-only flag."""
        out = df_uploads
        if patient_search:
            needle = patient_search.lower()
            out = out.filter(
                pl.col("patient_name").is_not_null()
                & pl.col("patient_name").str.to_lowercase().str.contains(needle)
            )
        if sva_only:
            out = out.filter(pl.col("is_sva_form"))
        return out.filter(pl.col("submission_date").is_not_null())

    def patient_upload_summary(self, df_uploads: pl.DataFrame) -> pl.DataFrame:
        """Per-patient: total uploads, first/last upload, all-SVA flag."""
        return (
            df_uploads.filter(pl.col("patient_name").is_not_null())
            .group_by("patient_name")
            .agg(
                pl.len().alias("total_uploads"),
                pl.col("timestamp").min().dt.strftime("%Y-%m-%d").alias("first_upload"),
                pl.col("timestamp").max().dt.strftime("%Y-%m-%d").alias("last_upload"),
                pl.col("is_sva_form").all().alias("all_sva_format"),
            )
            .sort("total_uploads", descending=True)
        )

    def session_health(self, df_sessions: pl.DataFrame) -> dict:
        return {
            "auth_failures": df_sessions.filter(~pl.col("auth_succeeded")).height,
            "dirty_disconnects": df_sessions.filter(
                ~pl.col("disconnected_cleanly")
            ).height,
        }

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
