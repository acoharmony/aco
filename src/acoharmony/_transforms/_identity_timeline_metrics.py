# © 2025 HarmonyCares
# All rights reserved.

"""
Gold transform: identity_timeline_metrics.

Per-run observability metrics for the identity_timeline pipeline. One row per
(metric_name, file_date) so you can see churn and crosswalk quality trend over
time. This is the "how accurate is our crosswalking, and how often does it
need to happen" dashboard backing.
"""

from datetime import datetime

import polars as pl

from .._decor8 import measure_dataframe_size, transform_method


@transform_method(enable_composition=False, threshold=5.0)
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Compute per-file_date metrics over the silver identity_timeline.

    Metrics emitted (long-format: metric_name, file_date, value):
        remaps_new         — unique (prvs_num, crnt_num) edges first seen in this file_date
        remaps_total       — total cclf9_remap observations in this file_date
        self_obs_total     — total cclf8_self observations in this file_date
        chains_touched     — distinct chain_ids with any row in this file_date
        multi_mbi_chains   — chains with >1 MBI active in this file_date
        singleton_chains   — chains with exactly 1 MBI active in this file_date
        hcmpi_coverage_pct — pct of MBIs in this file_date with an HCMPI
        chain_len_p50      — median chain length (distinct MBIs per chain)
        chain_len_max      — longest chain in this file_date
        circular_refs      — chain_ids where the edge set contains a cycle

    Args:
        executor: TransformRunner/executor with storage_config

    Returns:
        LazyFrame ready to be materialized to gold/identity_timeline_metrics.parquet
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    timeline = pl.scan_parquet(silver_path / "identity_timeline.parquet")

    # --- Per-file_date counts ----------------------------------------------
    remaps = timeline.filter(pl.col("observation_type") == "cclf9_remap")
    self_obs = timeline.filter(pl.col("observation_type") == "cclf8_self")

    remaps_total = remaps.group_by("file_date").agg(
        pl.len().cast(pl.Int64).alias("value")
    ).with_columns(pl.lit("remaps_total").alias("metric_name"))

    self_obs_total = self_obs.group_by("file_date").agg(
        pl.len().cast(pl.Int64).alias("value")
    ).with_columns(pl.lit("self_obs_total").alias("metric_name"))

    # "new" remaps: an edge (prvs_num, crnt_num) whose earliest file_date equals
    # the row's file_date.
    edge_first_seen = (
        remaps
        .group_by(["mbi", "maps_to_mbi"])
        .agg(pl.col("file_date").min().alias("first_seen"))
    )
    remaps_new = (
        remaps
        .join(edge_first_seen, on=["mbi", "maps_to_mbi"], how="left")
        .filter(pl.col("file_date") == pl.col("first_seen"))
        .group_by("file_date")
        .agg(pl.len().cast(pl.Int64).alias("value"))
        .with_columns(pl.lit("remaps_new").alias("metric_name"))
    )

    # --- Chain-level metrics per file_date ---------------------------------
    chains_per_file = (
        timeline
        .group_by(["file_date", "chain_id"])
        .agg(pl.col("mbi").n_unique().alias("chain_size"))
    )

    chains_touched = chains_per_file.group_by("file_date").agg(
        pl.len().cast(pl.Int64).alias("value")
    ).with_columns(pl.lit("chains_touched").alias("metric_name"))

    multi_mbi = (
        chains_per_file
        .filter(pl.col("chain_size") > 1)
        .group_by("file_date")
        .agg(pl.len().cast(pl.Int64).alias("value"))
        .with_columns(pl.lit("multi_mbi_chains").alias("metric_name"))
    )

    singletons = (
        chains_per_file
        .filter(pl.col("chain_size") == 1)
        .group_by("file_date")
        .agg(pl.len().cast(pl.Int64).alias("value"))
        .with_columns(pl.lit("singleton_chains").alias("metric_name"))
    )

    chain_len_p50 = chains_per_file.group_by("file_date").agg(
        pl.col("chain_size").median().cast(pl.Int64).alias("value")
    ).with_columns(pl.lit("chain_len_p50").alias("metric_name"))

    chain_len_max = chains_per_file.group_by("file_date").agg(
        pl.col("chain_size").max().cast(pl.Int64).alias("value")
    ).with_columns(pl.lit("chain_len_max").alias("metric_name"))

    # --- HCMPI coverage ----------------------------------------------------
    hcmpi_cov = (
        timeline
        .group_by("file_date")
        .agg([
            pl.col("mbi").n_unique().alias("mbi_total"),
            pl.col("mbi").filter(pl.col("hcmpi").is_not_null()).n_unique().alias("mbi_with_hcmpi"),
        ])
        .with_columns(
            (pl.col("mbi_with_hcmpi") * 100 / pl.col("mbi_total")).cast(pl.Int64).alias("value")
        )
        .select(["file_date", "value"])
        .with_columns(pl.lit("hcmpi_coverage_pct").alias("metric_name"))
    )

    # --- Circular refs (A->B and B->A both exist) --------------------------
    edges = remaps.select(["mbi", "maps_to_mbi", "file_date"]).unique()
    reversed_edges = edges.select([
        pl.col("maps_to_mbi").alias("mbi"),
        pl.col("mbi").alias("maps_to_mbi"),
        pl.col("file_date"),
    ])
    cycles = (
        edges.join(reversed_edges, on=["mbi", "maps_to_mbi", "file_date"], how="inner")
        .group_by("file_date")
        .agg(pl.len().cast(pl.Int64).alias("value"))
        .with_columns(pl.lit("circular_refs").alias("metric_name"))
    )

    union = pl.concat(
        [
            remaps_total.select(["metric_name", "file_date", "value"]),
            self_obs_total.select(["metric_name", "file_date", "value"]),
            remaps_new.select(["metric_name", "file_date", "value"]),
            chains_touched.select(["metric_name", "file_date", "value"]),
            multi_mbi.select(["metric_name", "file_date", "value"]),
            singletons.select(["metric_name", "file_date", "value"]),
            chain_len_p50.select(["metric_name", "file_date", "value"]),
            chain_len_max.select(["metric_name", "file_date", "value"]),
            hcmpi_cov.select(["metric_name", "file_date", "value"]),
            cycles.select(["metric_name", "file_date", "value"]),
        ],
        how="vertical",
    )

    computed_at = datetime.now().isoformat(timespec="seconds")
    return union.with_columns(pl.lit(computed_at).alias("computed_at")).sort(
        ["file_date", "metric_name"]
    )
