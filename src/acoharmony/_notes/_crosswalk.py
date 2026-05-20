# © 2025 HarmonyCares
# All rights reserved.

"""
CCLF crosswalk coverage analytics.

CCLF8 carries beneficiary demographics (population denominator); CCLF9
carries the prvs_num → crnt_num MBI mapping. The two helpers here roll
both up by year for the crosswalk coverage dashboard.
"""

from __future__ import annotations

import polars as pl

from ._base import PluginRegistry


def _with_year(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Slice a 'YYYYMMDD…' file_date column into an integer year column."""
    return lf.with_columns(
        pl.col("file_date").str.slice(0, 4).cast(pl.Int32).alias("year")
    )


class CrosswalkPlugins(PluginRegistry):
    """Year-over-year CCLF crosswalk coverage rollups."""

    def coverage_by_year(
        self,
        cclf8_lf: pl.LazyFrame,
        cclf9_lf: pl.LazyFrame,
    ) -> pl.DataFrame:
        """
        Per-year population vs. crosswalk MBI counts and coverage %.

        Returns a DataFrame with: year, total_unique_mbis,
        unique_mbis_with_xwalk, actual_crosswalks, crosswalk_percentage.
        """
        total_pop = (
            _with_year(cclf8_lf)
            .group_by("year")
            .agg(pl.col("bene_mbi_id").n_unique().alias("total_unique_mbis"))
            .collect()
            .sort("year")
        )
        xwalk = (
            _with_year(cclf9_lf)
            .group_by("year")
            .agg(
                pl.col("prvs_num").n_unique().alias("unique_mbis_with_xwalk"),
                (pl.col("prvs_num") != pl.col("crnt_num"))
                .sum()
                .alias("actual_crosswalks"),
            )
            .collect()
            .sort("year")
        )
        return total_pop.join(xwalk, on="year", how="left").with_columns(
            (
                (pl.col("unique_mbis_with_xwalk") / pl.col("total_unique_mbis")) * 100
            ).alias("crosswalk_percentage")
        )

    def mapping_detail_by_year(self, cclf9_lf: pl.LazyFrame) -> pl.DataFrame:
        """
        Per-year breakdown of unique prvs/crnt MBIs and self-vs-actual mappings.

        Returns: year, unique_prvs_mbis, unique_crnt_mbis,
        total_xref_records, actual_crosswalks, self_mappings.
        """
        return (
            _with_year(cclf9_lf)
            .group_by("year")
            .agg(
                pl.col("prvs_num").n_unique().alias("unique_prvs_mbis"),
                pl.col("crnt_num").n_unique().alias("unique_crnt_mbis"),
                pl.len().alias("total_xref_records"),
                (pl.col("prvs_num") != pl.col("crnt_num"))
                .sum()
                .alias("actual_crosswalks"),
                (pl.col("prvs_num") == pl.col("crnt_num"))
                .sum()
                .alias("self_mappings"),
            )
            .collect()
            .sort("year")
        )
