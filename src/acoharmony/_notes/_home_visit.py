# © 2025 HarmonyCares
# All rights reserved.

"""
Home-visit claims analytics for the home_visit_claims notebook.

Slices the gold ``home_visit_claims.parquet`` into provider-list /
outside-practice partitions, computes summary stats / HCPCS / top-
provider / monthly / place-of-service rollups, and projects 2026
Medicare rate increases on the 2024-2025 baseline.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry

HOME_VISIT_HCPCS_DESCRIPTIONS = {
    "99341": "Home/residence visit, new patient, 20 min",
    "99342": "Home/residence visit, new patient, 30 min",
    "99344": "Home/residence visit, new patient, 60 min",
    "99345": "Home/residence visit, new patient, 75 min",
    "99347": "Home/residence visit, established patient, 15 min",
    "99348": "Home/residence visit, established patient, 25 min",
    "99349": "Home/residence visit, established patient, 40 min",
    "99350": "Home/residence visit, established patient, 60 min",
    "G2211": "Visit complexity add-on (E/M)",
    "G0556": "Comprehensive assessment, first 60 min",
    "G0557": "Comprehensive assessment, each add'l 30 min",
    "G0558": "Comprehensive assessment, single session",
}

# 2026 MPFS rate increase percentages. ``pct_delta_w_addon`` assumes
# the G2211 complexity add-on is billed with the visit; ``pct_delta_wo_addon``
# is the base code increase only.
RATE_INCREASE_2026 = {
    "99341": {"pct_delta_w_addon": 0.37, "pct_delta_wo_addon": 0.01, "description": "Home/res vst new sf mdm 15"},
    "99342": {"pct_delta_w_addon": 0.25, "pct_delta_wo_addon": 0.02, "description": "Home/res vst new low mdm 30"},
    "99344": {"pct_delta_w_addon": 0.17, "pct_delta_wo_addon": 0.05, "description": "Home/res vst new mod mdm 60"},
    "99345": {"pct_delta_w_addon": 0.15, "pct_delta_wo_addon": 0.06, "description": "Home/res vst new high mdm 75"},
    "99347": {"pct_delta_w_addon": 0.42, "pct_delta_wo_addon": 0.03, "description": "Home/res vst est sf mdm 20"},
    "99348": {"pct_delta_w_addon": 0.27, "pct_delta_wo_addon": 0.04, "description": "Home/res vst est low mdm 30"},
    "99349": {"pct_delta_w_addon": 0.20, "pct_delta_wo_addon": 0.06, "description": "Home/res vst est mod mdm 40"},
    "99350": {"pct_delta_w_addon": 0.16, "pct_delta_wo_addon": 0.06, "description": "Home/res vst est high mdm 60"},
    "G0556": {"pct_delta_w_addon": 0.06, "pct_delta_wo_addon": 0.06, "description": "Adv prim care mgmt lvl 1"},
    "G0557": {"pct_delta_w_addon": 0.08, "pct_delta_wo_addon": 0.08, "description": "Adv prim care mgmt lvl 2"},
    "G0558": {"pct_delta_w_addon": 0.07, "pct_delta_wo_addon": 0.07, "description": "Adv prim care mgmt lvl 3"},
    "G2211": {"pct_delta_w_addon": 0.00, "pct_delta_wo_addon": 0.00, "description": "Complex e/m visit add on"},
}


class HomeVisitPlugins(PluginRegistry):
    """Home-visit claim slicing + analytics + 2026 projections."""

    # ---- provider-list TIN/NPI extraction -----------------------------

    def provider_tin_npi(self, silver_path: Path) -> pl.DataFrame:
        """Distinct TIN/NPI pairs (individual + organization) from the latest participant_list."""
        from acoharmony._expressions._file_version import FileVersionExpression

        path = Path(silver_path) / "participant_list.parquet"
        if not path.exists():
            return pl.DataFrame(schema={"tin": pl.Utf8, "npi": pl.Utf8})

        provider_list = (
            pl.scan_parquet(str(path))
            .filter(FileVersionExpression.keep_only_most_recent_file())
            .collect()
        )
        individual = provider_list.select(
            pl.col("base_provider_tin").alias("tin"),
            pl.col("individual_npi").alias("npi"),
        ).filter(pl.col("npi").is_not_null() & (pl.col("npi") != ""))
        organization = provider_list.select(
            pl.col("base_provider_tin").alias("tin"),
            pl.col("organization_npi").alias("npi"),
        ).filter(pl.col("npi").is_not_null() & (pl.col("npi") != ""))
        return pl.concat([individual, organization]).unique()

    # ---- partition all claims into provider_list vs outside ----------

    def partition_by_provider_list(
        self,
        df_all: pl.DataFrame,
        provider_tin_npi: pl.DataFrame,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Return ``(provider_list_only, outside_practice)``."""
        if provider_tin_npi.is_empty():
            return df_all, pl.DataFrame()
        provider = df_all.join(provider_tin_npi, on=["tin", "npi"], how="inner")
        outside = df_all.join(provider_tin_npi, on=["tin", "npi"], how="anti")
        return provider, outside

    # ---- rollups ------------------------------------------------------

    def summary_stats(self, df: pl.DataFrame) -> dict[str, Any]:
        if df.is_empty():
            return {
                "total_claims": 0,
                "total_patients": 0,
                "total_tin_npi": 0,
                "unique_tins": 0,
                "unique_npis": 0,
                "total_paid": 0.0,
                "total_allowed": 0.0,
                "avg_paid": 0.0,
                "min_date": None,
                "max_date": None,
            }
        date_range = df.select(
            pl.col("claim_start_date").min().alias("min_date"),
            pl.col("claim_start_date").max().alias("max_date"),
        ).to_dicts()[0]
        return {
            "total_claims": df.height,
            "total_patients": df.select(pl.col("person_id").n_unique()).item(),
            "total_tin_npi": df.select(pl.struct("tin", "npi").n_unique()).item(),
            "unique_tins": df.select(pl.col("tin").n_unique()).item(),
            "unique_npis": df.select(pl.col("npi").n_unique()).item(),
            "total_paid": float(df.select(pl.col("paid_amount").sum()).item() or 0),
            "total_allowed": float(df.select(pl.col("allowed_amount").sum()).item() or 0),
            "avg_paid": float(df.select(pl.col("paid_amount").mean()).item() or 0),
            "min_date": date_range["min_date"],
            "max_date": date_range["max_date"],
        }

    def hcpcs_distribution(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.group_by("hcpcs_code")
            .agg(
                pl.len().alias("claim_count"),
                pl.col("person_id").n_unique().alias("unique_patients"),
                pl.struct("tin", "npi").n_unique().alias("unique_providers"),
                pl.col("paid_amount").sum().alias("total_paid"),
                pl.col("paid_amount").mean().alias("avg_paid"),
                pl.col("allowed_amount").sum().alias("total_allowed"),
            )
            .sort("claim_count", descending=True)
        )

    def top_providers(self, df: pl.DataFrame, n: int = 50) -> pl.DataFrame:
        return (
            df.group_by("tin", "npi")
            .agg(
                pl.len().alias("claim_count"),
                pl.col("person_id").n_unique().alias("unique_patients"),
                pl.col("hcpcs_code").n_unique().alias("unique_hcpcs"),
                pl.col("paid_amount").sum().alias("total_paid"),
                pl.col("allowed_amount").sum().alias("total_allowed"),
            )
            .sort("claim_count", descending=True)
            .head(n)
        )

    def monthly_trends(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.with_columns(pl.col("claim_start_date").dt.strftime("%Y-%m").alias("month"))
            .group_by("month")
            .agg(
                pl.len().alias("claim_count"),
                pl.col("person_id").n_unique().alias("unique_patients"),
                pl.struct("tin", "npi").n_unique().alias("unique_providers"),
                pl.col("paid_amount").sum().alias("total_paid"),
            )
            .sort("month")
        )

    def place_of_service(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.group_by("place_of_service_code")
            .agg(
                pl.len().alias("claim_count"),
                pl.col("paid_amount").sum().alias("total_paid"),
            )
            .sort("claim_count", descending=True)
        )

    def filter_by_provider(self, df: pl.DataFrame, search: str) -> pl.DataFrame:
        if not search:
            return df
        term = search.strip()
        return df.filter(
            pl.col("tin").str.contains(term) | pl.col("npi").str.contains(term)
        )

    # ---- 2024-2025 baseline + 2026 projection -------------------------

    def year_comparison(self, df: pl.DataFrame) -> pl.DataFrame:
        """Per-year HCPCS aggregates for 2024 + 2025."""
        return (
            df.with_columns(pl.col("claim_start_date").dt.year().alias("year"))
            .filter(pl.col("year").is_in([2024, 2025]))
            .group_by("year", "hcpcs_code")
            .agg(
                pl.len().alias("claim_count"),
                pl.col("paid_amount").sum().alias("total_paid"),
                pl.col("paid_amount").mean().alias("avg_paid"),
                pl.col("allowed_amount").sum().alias("total_allowed"),
            )
            .sort("hcpcs_code", "year")
        )

    def project_2026(
        self,
        year_comparison: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Project 2026 increases off the 2024-2025 average baseline.

        Returns one row per HCPCS with claims_*, paid_*, avg_paid_2024_2025,
        pct_increase_w_addon_2026, projected_increase_2026_w_addon, and the
        same-suffixed ``wo_addon`` versions.
        """
        df_2024 = year_comparison.filter(pl.col("year") == 2024).select(
            "hcpcs_code",
            pl.col("claim_count").alias("claims_2024"),
            pl.col("total_paid").alias("paid_2024"),
            pl.col("avg_paid").alias("avg_paid_2024"),
        )
        df_2025 = year_comparison.filter(pl.col("year") == 2025).select(
            "hcpcs_code",
            pl.col("claim_count").alias("claims_2025"),
            pl.col("total_paid").alias("paid_2025"),
            pl.col("avg_paid").alias("avg_paid_2025"),
        )
        merged = df_2024.join(df_2025, on="hcpcs_code", how="full", coalesce=True).fill_null(0)
        return (
            merged.with_columns(
                ((pl.col("claims_2024") + pl.col("claims_2025")) / 2).alias(
                    "avg_claims_2024_2025"
                ),
                ((pl.col("paid_2024") + pl.col("paid_2025")) / 2).alias(
                    "avg_paid_2024_2025"
                ),
                pl.col("hcpcs_code")
                .map_elements(
                    lambda c: RATE_INCREASE_2026.get(c, {}).get("pct_delta_w_addon", 0.0),
                    return_dtype=pl.Float64,
                )
                .alias("pct_increase_w_addon_2026"),
                pl.col("hcpcs_code")
                .map_elements(
                    lambda c: RATE_INCREASE_2026.get(c, {}).get("pct_delta_wo_addon", 0.0),
                    return_dtype=pl.Float64,
                )
                .alias("pct_increase_wo_addon_2026"),
                pl.col("hcpcs_code")
                .map_elements(
                    lambda c: RATE_INCREASE_2026.get(c, {}).get("description", ""),
                    return_dtype=pl.Utf8,
                )
                .alias("description"),
            )
            .with_columns(
                (pl.col("avg_paid_2024_2025") * pl.col("pct_increase_w_addon_2026")).alias(
                    "projected_increase_2026_w_addon"
                ),
                (pl.col("avg_paid_2024_2025") * pl.col("pct_increase_wo_addon_2026")).alias(
                    "projected_increase_2026_wo_addon"
                ),
                (
                    pl.col("avg_paid_2024_2025") * (1 + pl.col("pct_increase_w_addon_2026"))
                ).alias("projected_paid_2026_w_addon"),
                (
                    pl.col("avg_paid_2024_2025") * (1 + pl.col("pct_increase_wo_addon_2026"))
                ).alias("projected_paid_2026_wo_addon"),
            )
            .sort("hcpcs_code")
        )

    def projection_totals(self, projections: pl.DataFrame) -> dict[str, float]:
        """Roll a projection table up to total $-impact + % increase."""
        if projections.is_empty():
            return {
                "total_2024": 0.0,
                "total_2025": 0.0,
                "total_avg_baseline": 0.0,
                "total_increase_w_addon": 0.0,
                "total_increase_wo_addon": 0.0,
                "total_projected_w_addon": 0.0,
                "total_projected_wo_addon": 0.0,
                "pct_increase_w": 0.0,
                "pct_increase_wo": 0.0,
            }
        totals = projections.select(
            pl.col("paid_2024").sum().cast(pl.Float64).alias("total_2024"),
            pl.col("paid_2025").sum().cast(pl.Float64).alias("total_2025"),
            pl.col("avg_paid_2024_2025").sum().cast(pl.Float64).alias("total_avg_baseline"),
            pl.col("projected_increase_2026_w_addon")
            .sum()
            .cast(pl.Float64)
            .alias("total_increase_w_addon"),
            pl.col("projected_increase_2026_wo_addon")
            .sum()
            .cast(pl.Float64)
            .alias("total_increase_wo_addon"),
            pl.col("projected_paid_2026_w_addon")
            .sum()
            .cast(pl.Float64)
            .alias("total_projected_w_addon"),
            pl.col("projected_paid_2026_wo_addon")
            .sum()
            .cast(pl.Float64)
            .alias("total_projected_wo_addon"),
        ).to_dicts()[0]
        baseline = totals["total_avg_baseline"] or 0
        return {
            **totals,
            "pct_increase_w": (
                (totals["total_increase_w_addon"] / baseline * 100) if baseline > 0 else 0
            ),
            "pct_increase_wo": (
                (totals["total_increase_wo_addon"] / baseline * 100) if baseline > 0 else 0
            ),
        }
