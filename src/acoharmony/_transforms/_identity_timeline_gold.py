# © 2025 HarmonyCares
# All rights reserved.

"""
Gold transform: identity_timeline.

Unified observation view over the silver identity_timeline plus beneficiary
opt-out events from bnex. Provides a single surface for answering
"which MBI applies on date D, and is the beneficiary opted out?"
"""

import polars as pl

from .._decor8 import measure_dataframe_size, transform_method


@transform_method(enable_composition=False, threshold=10.0)
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Join silver identity_timeline with bnex opt-out records.

    Args:
        executor: TransformRunner/executor with storage_config

    Returns:
        LazyFrame ready to be materialized to gold/identity_timeline.parquet

    Schema:
        observation_type in {cclf9_remap, cclf8_self, bnex_optout}. For
        bnex_optout rows, maps_to_mbi is null, chain_id is inherited from
        silver when the MBI is known to the timeline, otherwise a singleton
        hash. bnex rows bring their exclusion reason in the `notes` column.
    """
    import hashlib

    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    timeline = pl.scan_parquet(silver_path / "identity_timeline.parquet").with_columns(
        pl.lit(None, dtype=pl.String).alias("notes")
    )

    # Build an MBI -> chain_id map from the silver timeline so we can stitch
    # opt-out events into existing chains when possible.
    mbi_to_chain = (
        timeline.select(["mbi", "chain_id"])
        .unique()
        .rename({"chain_id": "_silver_chain_id"})
    )

    bnex = pl.scan_parquet(silver_path / "bnex.parquet").select([
        pl.col("MBI").alias("mbi"),
        pl.col("BeneExcReasons").alias("notes"),
        pl.col("file_date"),
        pl.col("source_filename").alias("source_file"),
    ]).filter(pl.col("mbi").is_not_null())

    # Most recent bnex file_date drives is_current_as_of_file_date for opt-outs.
    max_bnex_file_date = bnex.select(pl.col("file_date").max()).collect().item()

    bnex_enriched = (
        bnex
        .with_columns([
            pl.lit(None, dtype=pl.String).alias("maps_to_mbi"),
            pl.lit(None, dtype=pl.Date).alias("effective_date"),
            pl.lit(None, dtype=pl.Date).alias("obsolete_date"),
            pl.lit("bnex_optout").alias("observation_type"),
            (pl.col("file_date") == pl.lit(max_bnex_file_date)).alias("is_current_as_of_file_date"),
        ])
        .join(mbi_to_chain, on="mbi", how="left")
    )

    # For bnex MBIs absent from silver, compute a singleton chain_id client-side.
    bnex_with_chain = bnex_enriched.with_columns(
        pl.when(pl.col("_silver_chain_id").is_not_null())
        .then(pl.col("_silver_chain_id"))
        .otherwise(
            pl.col("mbi").map_elements(
                lambda m: hashlib.sha1(m.encode("utf-8")).hexdigest()[:16] if m else None,
                return_dtype=pl.String,
            )
        )
        .alias("chain_id")
    ).with_columns(pl.lit(None, dtype=pl.String).alias("hcmpi")).drop("_silver_chain_id")

    # Inherit the silver-assigned hop_index for the bnex MBI within its
    # chain. The earlier implementation hardcoded hop_index=0 for every
    # bnex row, which corrupts non-canonical MBIs in known chains: a bene
    # whose MBI rotates and *then* opts out lands at silver-hop=1 (their
    # rotated MBI) but bnex would re-stamp them at hop=0, producing two
    # distinct MBIs at hop_index=0 in the same chain. Downstream callers
    # treating hop_index=0 as canonical then pick non-deterministically
    # between them. New MBIs not yet in any silver chain (singleton case)
    # default to hop=0 because they are their own canonical.
    silver_hops = (
        timeline.select(["mbi", "chain_id", "hop_index"])
        .unique()
        .rename({"hop_index": "_silver_hop_index"})
    )
    bnex_with_chain = bnex_with_chain.join(
        silver_hops, on=["mbi", "chain_id"], how="left"
    ).with_columns(
        pl.col("_silver_hop_index").fill_null(0).cast(pl.Int64).alias("hop_index")
    ).drop("_silver_hop_index")

    # Backfill HCMPI for bnex rows from silver where possible.
    mbi_to_hcmpi = (
        timeline
        .filter(pl.col("hcmpi").is_not_null())
        .select(["mbi", "hcmpi"])
        .unique()
        .rename({"hcmpi": "_silver_hcmpi"})
    )
    bnex_with_hcmpi = bnex_with_chain.join(mbi_to_hcmpi, on="mbi", how="left").with_columns(
        pl.coalesce([pl.col("hcmpi"), pl.col("_silver_hcmpi")]).alias("hcmpi")
    ).drop("_silver_hcmpi")

    cols = [
        "mbi",
        "maps_to_mbi",
        "effective_date",
        "obsolete_date",
        "file_date",
        "observation_type",
        "source_file",
        "hcmpi",
        "chain_id",
        "hop_index",
        "is_current_as_of_file_date",
        "notes",
    ]

    return pl.concat(
        [timeline.select(cols), bnex_with_hcmpi.select(cols)],
        how="vertical",
    )
