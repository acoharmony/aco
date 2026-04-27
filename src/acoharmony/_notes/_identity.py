# © 2025 HarmonyCares
# All rights reserved.

"""
identity_timeline analytics + as-of-date lookups.

The identity_timeline is the consolidated view of every observation of
an MBI across CCLF/BNEX deliveries, joined into chains by historical
remap. These helpers answer dashboard-shaped questions on top of it.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from ._base import PluginRegistry


class IdentityPlugins(PluginRegistry):
    """As-of resolution + identity_timeline metric rollups."""

    def resolve_as_of(
        self,
        gold_lf: pl.LazyFrame,
        mbi: str,
        as_of_date,
    ) -> dict[str, Any]:
        """
        Point-in-time canonical-MBI resolution.

        Uses only chain rows whose ``file_date <= as_of_date`` so the
        answer is stable regardless of when later deliveries arrive.
        Returns ``{input_mbi, chain_id, canonical_mbi, hcmpi, opted_out,
        opt_out_reasons, chain_members, last_observed}`` (or an
        ``error`` / ``note`` field on degenerate cases).
        """
        if gold_lf.collect_schema().names() == []:
            return {"error": "gold identity_timeline not loaded"}

        hits = gold_lf.filter(pl.col("mbi") == mbi).collect()
        if hits.height == 0:
            return {
                "input_mbi": mbi,
                "chain_id": None,
                "canonical_mbi": None,
                "hcmpi": None,
                "opted_out": False,
                "opt_out_reasons": [],
                "chain_members": [],
                "last_observed": None,
                "note": "MBI not found in identity_timeline",
            }

        chain_id = hits["chain_id"][0]
        chain_rows = gold_lf.filter(
            (pl.col("chain_id") == chain_id) & (pl.col("file_date") <= as_of_date)
        ).collect()
        if chain_rows.height == 0:
            return {
                "input_mbi": mbi,
                "chain_id": chain_id,
                "canonical_mbi": None,
                "hcmpi": None,
                "opted_out": False,
                "opt_out_reasons": [],
                "chain_members": [],
                "last_observed": None,
                "note": f"No observations <= {as_of_date}",
            }

        leaves = chain_rows.filter(pl.col("hop_index") == 0)
        if leaves.height > 0:
            canonical_mbi = leaves.sort("file_date", descending=True)["mbi"][0]
        else:
            canonical_mbi = chain_rows.sort("hop_index")["mbi"][0]

        hcmpi_rows = chain_rows.filter(pl.col("hcmpi").is_not_null())
        hcmpi = hcmpi_rows["hcmpi"][0] if hcmpi_rows.height > 0 else None

        optouts = chain_rows.filter(pl.col("observation_type") == "bnex_optout")
        reasons = (
            sorted(set(optouts["notes"].drop_nulls().to_list()))
            if optouts.height > 0
            else []
        )

        return {
            "input_mbi": mbi,
            "chain_id": chain_id,
            "canonical_mbi": canonical_mbi,
            "hcmpi": hcmpi,
            "opted_out": optouts.height > 0,
            "opt_out_reasons": reasons,
            "chain_members": sorted(chain_rows["mbi"].unique().to_list()),
            "last_observed": chain_rows["file_date"].max(),
        }

    def churn_by_file_date(self, metrics_lf: pl.LazyFrame) -> pl.DataFrame:
        """Wide pivot of ``remaps_total`` / ``remaps_new`` / ``chains_touched``."""
        return (
            metrics_lf.filter(
                pl.col("metric_name").is_in(
                    ["remaps_total", "remaps_new", "chains_touched"]
                )
            )
            .collect()
            .pivot(on="metric_name", index="file_date", values="value")
            .sort("file_date")
        )

    def chain_length_distribution(self, timeline_lf: pl.LazyFrame) -> pl.DataFrame:
        """Distribution of chain sizes for currently-active rows."""
        return (
            timeline_lf.filter(pl.col("is_current_as_of_file_date"))
            .group_by("chain_id")
            .agg(pl.col("mbi").n_unique().alias("chain_size"))
            .group_by("chain_size")
            .agg(pl.len().alias("n_chains"))
            .sort("chain_size")
            .collect()
        )

    def quality_metrics(
        self,
        metrics_lf: pl.LazyFrame,
        last_n_dates: int = 12,
    ) -> pl.DataFrame:
        """Last-N file_date snapshot of HCMPI coverage / circular refs / chains."""
        return (
            metrics_lf.filter(
                pl.col("metric_name").is_in(
                    [
                        "hcmpi_coverage_pct",
                        "circular_refs",
                        "multi_mbi_chains",
                        "singleton_chains",
                    ]
                )
            )
            .collect()
            .pivot(on="metric_name", index="file_date", values="value")
            .sort("file_date", descending=True)
            .head(last_n_dates)
        )
