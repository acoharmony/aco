# © 2025 HarmonyCares
# All rights reserved.

"""
HEDR (Health Equity Data Reporting) eligibility analytics.

Backs ``notebooks/reach_hedr_eligibility.py``: HEDR denominator/
numerator metrics, status breakdown, months-in-REACH distribution,
missing-data-field rollup, SDOH template-vs-eligibility verification,
alignment-timing analysis, incomplete-beneficiary follow-up list.

The gold-tier source is ``reach_hedr.parquet`` (computed by
``acoharmony._transforms.reach_hedr``).
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry


class HedrPlugins(PluginRegistry):
    """HEDR eligibility rollups and SDOH template verification."""

    # ---- summary metrics -----------------------------------------------

    def summary_metrics(self, df_hedr: pl.DataFrame) -> dict[str, Any]:
        """Top-line HEDR counts: denominator / numerator / rate / incomplete / etc."""
        py = df_hedr["performance_year"][0]
        bar_date = df_hedr["bar_file_date"][0]
        total = df_hedr.height
        denom = int(df_hedr["hedr_denominator"].sum())
        num = int(df_hedr["hedr_numerator"].sum())
        rate = (num / denom * 100) if denom > 0 else 0.0
        return {
            "performance_year": py,
            "bar_date": bar_date,
            "total_beneficiaries": total,
            "denominator_count": denom,
            "numerator_count": num,
            "hedr_rate": rate,
            "incomplete_count": denom - num,
            "ineligible_count": total - denom,
        }

    # ---- per-axis rollups ---------------------------------------------

    def status_breakdown(self, df_hedr: pl.DataFrame) -> pl.DataFrame:
        return (
            df_hedr.group_by("hedr_status")
            .agg(pl.len().alias("count"))
            .with_columns(
                (pl.col("count") / pl.col("count").sum() * 100).alias("percentage")
            )
            .sort("count", descending=True)
        )

    def months_distribution(self, df_hedr: pl.DataFrame) -> pl.DataFrame:
        py = df_hedr["performance_year"][0]
        months_col = f"reach_months_{py}"
        return (
            df_hedr.group_by(months_col)
            .agg(pl.len().alias("count"))
            .sort(months_col)
            .with_columns(
                (pl.col("count") / pl.col("count").sum() * 100).alias("percentage")
            )
        )

    def missing_field_counts(self, df_hedr: pl.DataFrame) -> pl.DataFrame:
        """Per-field counts among beneficiaries who are denominator-but-not-numerator."""
        incomplete = df_hedr.filter(
            pl.col("hedr_denominator") & ~pl.col("hedr_numerator")
        )
        if incomplete.is_empty():
            return pl.DataFrame(
                schema={"missing_data_fields": pl.Utf8, "count": pl.Int64}
            )
        return (
            incomplete.select(pl.col("missing_data_fields").str.split(","))
            .explode("missing_data_fields")
            .group_by("missing_data_fields")
            .agg(pl.len().alias("count"))
            .filter(pl.col("missing_data_fields").is_not_null())
            .sort("count", descending=True)
        )

    def alignment_timing(
        self,
        df_hedr: pl.DataFrame,
    ) -> pl.DataFrame:
        """Counts by whether first_reach_date is before/after October 1 of the PY."""
        year = df_hedr["performance_year"][0]
        october_first = date(year, 10, 1)
        return (
            df_hedr.filter(pl.col("is_alive"))
            .with_columns(
                pl.when(pl.col("first_reach_date") <= pl.lit(october_first))
                .then(pl.lit("Started before Oct 1"))
                .when(pl.col("first_reach_date") > pl.lit(october_first))
                .then(pl.lit("Started after Oct 1"))
                .otherwise(pl.lit("Unknown"))
                .alias("alignment_timing")
            )
            .group_by("alignment_timing")
            .agg(
                pl.len().alias("count"),
                pl.col("hedr_denominator").sum().alias("denominator_eligible"),
                pl.col("hedr_numerator").sum().alias("numerator_complete"),
            )
            .sort("count", descending=True)
        )

    def incomplete_beneficiaries(self, df_hedr: pl.DataFrame) -> pl.DataFrame:
        """The follow-up list: denominator-eligible MBIs missing data."""
        py = df_hedr["performance_year"][0]
        return df_hedr.filter(
            pl.col("hedr_denominator") & ~pl.col("hedr_numerator")
        ).select(
            "mbi",
            "bene_first_name",
            "bene_last_name",
            "bene_city",
            "bene_state",
            f"reach_months_{py}",
            "missing_data_fields",
            "hedr_status",
        )

    # ---- SDOH template verification -----------------------------------

    def sdoh_template_check(
        self,
        silver_path: Path,
        df_hedr: pl.DataFrame,
    ) -> dict[str, Any] | None:
        """
        Verify SDOH template MBIs against HEDR denominator eligibility.

        Returns ``None`` when the template parquet is missing.
        """
        path = Path(silver_path) / "reach_sdoh.parquet"
        if not path.exists():
            return None
        template = pl.read_parquet(str(path))
        template_mbis = set(template["mbi"])
        hedr_mbis = set(df_hedr["mbi"])
        in_hedr = template_mbis & hedr_mbis
        not_in_hedr = template_mbis - hedr_mbis
        template_hedr = df_hedr.filter(pl.col("mbi").is_in(list(in_hedr)))
        denom_eligible = template_hedr.filter(pl.col("hedr_denominator"))
        denom_not_eligible = template_hedr.filter(~pl.col("hedr_denominator"))
        total = len(template_mbis)
        summary = pl.DataFrame(
            {
                "status": [
                    "In SDOH Template",
                    "✓ Denominator Eligible",
                    "✗ NOT Denominator Eligible",
                    "✗ Not in BAR at all",
                ],
                "count": [
                    total,
                    len(denom_eligible),
                    len(denom_not_eligible),
                    len(not_in_hedr),
                ],
                "percentage": [
                    100.0,
                    len(denom_eligible) / total * 100 if total else 0,
                    len(denom_not_eligible) / total * 100 if total else 0,
                    len(not_in_hedr) / total * 100 if total else 0,
                ],
            }
        )
        total_issues = len(denom_not_eligible) + len(not_in_hedr)
        issue_pct = (total_issues / total * 100) if total else 0
        return {
            "summary": summary,
            "total_issues": total_issues,
            "issue_pct": issue_pct,
            "denom_not_eligible": denom_not_eligible,
        }

    # ---- recommendation -----------------------------------------------

    @staticmethod
    def status_label(rate_pct: float) -> tuple[str, str]:
        """Return ``(severity, label)`` for the recommendation card."""
        if rate_pct >= 95:
            return ("ok", "Excellent HEDR compliance — continue monitoring data quality.")
        if rate_pct >= 85:
            return ("warn", "Good progress. Focus on completing data to reach 95%.")
        return ("critical", "Action needed to meet CMS requirements.")
