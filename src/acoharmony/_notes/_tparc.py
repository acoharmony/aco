# © 2025 HarmonyCares
# All rights reserved.

"""
TPARC (Total Payment-At-Risk Capitation) weekly-claims analytics.

Backs ``notebooks/tparc.py``: header (CLMH) and line (CLML) record
splits, financial summaries, top HCPCS codes, weekly-file rollups,
and the per-office monthly breakdown that drives the PCC reduction
review.
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from ._base import PluginRegistry


class TparcPlugins(PluginRegistry):
    """TPARC silver loader + per-record-type rollups."""

    def load(self, silver_path: Path) -> pl.LazyFrame:
        return pl.scan_parquet(str(Path(silver_path) / "tparc.parquet"))

    # ---- overall stats -------------------------------------------------

    def overall_stats(self, tparc_lf: pl.LazyFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
        """``(scalar_stats, record_type_counts)``."""
        stats = tparc_lf.select(
            pl.len().alias("total_records"),
            pl.col("record_type").n_unique().alias("record_types"),
            pl.col("source_filename").n_unique().alias("files_processed"),
        ).collect()
        record_types = (
            tparc_lf.group_by("record_type")
            .agg(pl.len().alias("count"))
            .sort("record_type")
            .collect()
        )
        return stats, record_types

    # ---- CLMH: claim headers -------------------------------------------

    def clmh_date_range(self, tparc_lf: pl.LazyFrame) -> pl.DataFrame:
        return (
            tparc_lf.filter(pl.col("record_type") == "CLMH")
            .select(
                pl.col("from_date").min().alias("earliest_date"),
                pl.col("thru_date").max().alias("latest_date"),
            )
            .collect()
        )

    # ---- CLML: claim lines --------------------------------------------

    def clml_records(self, tparc_lf: pl.LazyFrame) -> pl.LazyFrame:
        return tparc_lf.filter(pl.col("record_type") == "CLML")

    def clml_financial_summary(self, clml_lf: pl.LazyFrame) -> pl.DataFrame:
        return clml_lf.select(
            pl.col("total_charge_amt").sum().alias("total_charges"),
            pl.col("allowed_charge_amt").sum().alias("total_allowed"),
            pl.col("covered_paid_amt").sum().alias("total_paid"),
            pl.col("pcc_reduction_amt").sum().alias("total_reductions"),
            pl.col("sequestration_amt").sum().alias("total_sequestration"),
        ).collect()

    def top_hcpcs(self, clml_lf: pl.LazyFrame, n: int = 20) -> pl.DataFrame:
        return (
            clml_lf.filter(pl.col("hcpcs_code").is_not_null())
            .group_by("hcpcs_code")
            .agg(
                pl.len().alias("claim_lines"),
                pl.col("total_charge_amt").sum().alias("total_charges"),
            )
            .sort("claim_lines", descending=True)
            .limit(n)
            .collect()
        )

    def unique_patients(self, clml_lf: pl.LazyFrame) -> int:
        return (
            clml_lf.filter(pl.col("patient_control_num").is_not_null())
            .select(pl.col("patient_control_num").n_unique())
            .collect()
            .item()
        )

    # ---- weekly file rollup -------------------------------------------

    def file_stats(self, tparc_lf: pl.LazyFrame) -> pl.DataFrame:
        return (
            tparc_lf.group_by("source_filename")
            .agg(
                pl.len().alias("records"),
                pl.col("record_type").filter(pl.col("record_type") == "CLMH").len().alias("claims"),
                pl.col("record_type").filter(pl.col("record_type") == "CLML").len().alias("line_items"),
            )
            .sort("source_filename")
            .collect()
        )

    # ---- monthly office breakdown for current year --------------------

    def monthly_office_breakdown(
        self,
        tparc_lf: pl.LazyFrame,
        year: int,
    ) -> pl.DataFrame:
        """
        Group CLML rows by ``(rendering_provider_npi, year_month)`` for ``year``.

        ``from_date`` is encoded as YYYYMMDD integer in TPARC, so we
        bracket the year and pull year_month via integer division.
        """
        year_start = year * 10000 + 101
        year_end = year * 10000 + 1231
        monthly = (
            tparc_lf.filter(pl.col("record_type") == "CLML")
            .filter(pl.col("from_date").is_not_null())
            .filter(pl.col("from_date") >= year_start)
            .filter(pl.col("from_date") <= year_end)
            .with_columns(
                (pl.col("from_date") // 100).alias("year_month"),
                pl.col("rendering_provider_npi").alias("office_npi"),
            )
            .group_by("office_npi", "year_month")
            .agg(
                pl.len().alias("claim_lines"),
                pl.col("total_charge_amt").sum().alias("total_charges"),
                pl.col("allowed_charge_amt").sum().alias("total_allowed"),
                pl.col("covered_paid_amt").sum().alias("total_paid"),
                pl.col("pcc_reduction_amt").sum().alias("total_reductions"),
                pl.col("sequestration_amt").sum().alias("total_sequestration"),
            )
            .sort("year_month", "office_npi")
            .collect()
        )
        return monthly.with_columns(
            (
                pl.col("year_month").cast(pl.Utf8).str.slice(0, 4)
                + pl.lit("-")
                + pl.col("year_month").cast(pl.Utf8).str.slice(4, 2)
            ).alias("month"),
            pl.when(pl.col("total_allowed") > 0)
            .then(pl.col("total_reductions") / pl.col("total_allowed") * 100)
            .otherwise(0)
            .alias("reduction_rate_pct"),
        ).select(
            "month",
            "office_npi",
            "claim_lines",
            "total_charges",
            "total_allowed",
            "total_paid",
            "total_reductions",
            "total_sequestration",
            "reduction_rate_pct",
        )

    def top_offices_by_reductions(
        self,
        monthly_breakdown: pl.DataFrame,
        n: int = 15,
    ) -> pl.DataFrame:
        return (
            monthly_breakdown.group_by("office_npi")
            .agg(
                pl.col("claim_lines").sum().alias("total_claim_lines"),
                pl.col("total_reductions").sum().alias("total_reductions"),
                pl.col("total_allowed").sum().alias("total_allowed"),
            )
            .with_columns(
                pl.when(pl.col("total_allowed") > 0)
                .then(pl.col("total_reductions") / pl.col("total_allowed") * 100)
                .otherwise(0)
                .alias("avg_reduction_rate")
            )
            .sort("total_reductions", descending=True)
            .limit(n)
        )
