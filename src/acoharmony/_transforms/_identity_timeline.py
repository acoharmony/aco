# © 2025 HarmonyCares
# All rights reserved.

"""
Silver transform: identity_timeline.

Builds an append-only, temporally-aware MBI identifier timeline from the full
history of CCLF9 (remap observations) and CCLF8 (self-observations). Preserves
the CMS-provided effective/obsolete dates that legacy crosswalk tables collapse
away, and computes a deterministic chain_id per connected component so every
MBI that ever referred to the same beneficiary shares one identifier.

Design notes:
    - chain_id is SHA1 of the sorted MBI set in the connected component.
      Stable unless the component grows — growth produces a new chain_id and
      the prior rows (with the old chain_id) remain as historical audit.
    - is_current_as_of_file_date is True when the (prvs_num) appears in the
      MAX(file_date) CCLF9 file (for remaps) or the MAX CCLF8 (for self-obs).
    - hop_index: 0 for leaves (MBIs that no remap edge leaves from in the
      current file_date slice), 1+ for interior nodes ordered by earliest
      first appearance.
"""

import hashlib
from pathlib import Path

import polars as pl

from .._decor8 import measure_dataframe_size, transform_method


def current_mbi_lookup_lazy(silver_path: Path) -> pl.LazyFrame:
    """
    Return a lazy (prvs_num, crnt_num) lookup derived from identity_timeline.

    Shape-compatible with the legacy `int_beneficiary_xref_deduped.parquet`
    lookup used by the Tuva `int_*` transforms. Every historical MBI in a
    chain resolves to its canonical (hop_index=0) current MBI.

    Rows are emitted for *every* MBI in the timeline — historical nodes get a
    real remap (prvs_num != crnt_num), canonical leaves get an identity row
    (prvs_num == crnt_num). Consumers doing a left join from bene_mbi_id on
    prvs_num therefore never lose rows.
    """
    tl = pl.scan_parquet(silver_path / "identity_timeline.parquet")
    leaves = (
        tl.filter(pl.col("hop_index") == 0)
        .select(["chain_id", pl.col("mbi").alias("crnt_num")])
        .unique()
    )
    return (
        tl.select(["chain_id", pl.col("mbi").alias("prvs_num")])
        .unique()
        .join(leaves, on="chain_id", how="left")
        .select(["prvs_num", "crnt_num"])
    )


def current_mbi_with_hcmpi_lookup_lazy(silver_path: Path) -> pl.LazyFrame:
    """
    Return a lazy (prvs_num, crnt_num, hcmpi) lookup derived from identity_timeline.

    Shape-compatible with the legacy `enterprise_crosswalk.parquet` columns
    consumed by `_voluntary_alignment` and `_aco_alignment_temporal`.
    """
    tl = pl.scan_parquet(silver_path / "identity_timeline.parquet")
    leaves = (
        tl.filter(pl.col("hop_index") == 0)
        .select(["chain_id", pl.col("mbi").alias("crnt_num")])
        .unique()
    )
    hcmpi_per_chain = (
        tl.filter(pl.col("hcmpi").is_not_null())
        .select(["chain_id", "hcmpi"])
        .unique(subset=["chain_id"], keep="first")
    )
    return (
        tl.select(["chain_id", pl.col("mbi").alias("prvs_num")])
        .unique()
        .join(leaves, on="chain_id", how="left")
        .join(hcmpi_per_chain, on="chain_id", how="left")
        .select(["prvs_num", "crnt_num", "hcmpi"])
    )


def _compute_chain_ids(edges: pl.DataFrame) -> pl.DataFrame:
    """
    Union-find over MBI edges. Returns a DataFrame with columns (mbi, chain_id).

    chain_id = sha1("|".join(sorted(component_mbis))), hex-encoded, truncated
    to 16 chars for readability (still 64 bits of entropy — collision risk
    is negligible at any realistic cohort size).
    """
    if edges.is_empty():
        return pl.DataFrame({"mbi": [], "chain_id": []}, schema={"mbi": pl.String, "chain_id": pl.String})

    # Materialize node set
    nodes = (
        pl.concat([
            edges.select(pl.col("prvs_num").alias("mbi")),
            edges.select(pl.col("crnt_num").alias("mbi")),
        ])
        .filter(pl.col("mbi").is_not_null())
        .unique()
        .to_series()
        .to_list()
    )

    parent: dict[str, str] = {n: n for n in nodes}

    def find(x: str) -> str:
        # Path compression
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: str, b: str) -> None:
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        # Union by lexicographic smallest root for determinism
        if ra < rb:
            parent[rb] = ra
        else:
            parent[ra] = rb

    for prvs, crnt in edges.select(["prvs_num", "crnt_num"]).iter_rows():
        if prvs is None or crnt is None:
            continue
        union(prvs, crnt)

    # Collect component membership
    components: dict[str, list[str]] = {}
    for n in nodes:
        root = find(n)
        components.setdefault(root, []).append(n)

    # Hash each component's sorted MBI list
    rows: list[tuple[str, str]] = []
    for members in components.values():
        members_sorted = sorted(members)
        digest = hashlib.sha1("|".join(members_sorted).encode("utf-8")).hexdigest()[:16]
        for m in members_sorted:
            rows.append((m, digest))

    return pl.DataFrame(rows, schema={"mbi": pl.String, "chain_id": pl.String}, orient="row")


@transform_method(enable_composition=False, threshold=10.0)
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Build the identity_timeline silver table.

    Args:
        executor: TransformRunner/executor with storage_config

    Returns:
        LazyFrame ready to be materialized to silver/identity_timeline.parquet
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    # ---- CCLF9: remap observations ----------------------------------------
    # Drop self-loop rows (prvs_num == crnt_num). CMS emits these as "no-change"
    # heartbeats in CCLF9; they carry no remap information, and the active-MBI
    # signal they provide is already captured by is_current_as_of_file_date
    # derived from CCLF8. Including them falsely inflates remaps_total, creates
    # fake circular_refs (every self-loop matches its own reverse), and demotes
    # canonical MBIs from leaf status in chain resolution.
    cclf9 = pl.scan_parquet(silver_path / "cclf9.parquet").select([
        pl.col("prvs_num"),
        pl.col("crnt_num"),
        pl.col("prvs_id_efctv_dt").alias("effective_date"),
        pl.col("prvs_id_obslt_dt").alias("obsolete_date"),
        pl.col("file_date").str.to_date(strict=False).alias("file_date"),
        pl.col("source_filename").alias("source_file"),
    ]).filter(
        pl.col("prvs_num").is_not_null()
        & pl.col("crnt_num").is_not_null()
        & (pl.col("prvs_num") != pl.col("crnt_num"))
    )

    # Most recent CCLF9 file_date — drives is_current_as_of_file_date
    max_cclf9_file_date = cclf9.select(pl.col("file_date").max()).collect().item()

    remap_rows = cclf9.with_columns([
        pl.lit("cclf9_remap").alias("observation_type"),
        (pl.col("file_date") == pl.lit(max_cclf9_file_date)).alias("is_current_as_of_file_date"),
    ]).rename({"prvs_num": "mbi", "crnt_num": "maps_to_mbi"})

    # ---- CCLF8: self-observations -----------------------------------------
    cclf8 = pl.scan_parquet(silver_path / "cclf8.parquet").select([
        pl.col("bene_mbi_id").alias("mbi"),
        pl.col("source_filename").alias("source_file"),
        pl.col("file_date"),
    ]).filter(pl.col("mbi").is_not_null())

    # cclf8.file_date is already Date; cclf9.file_date was str. Coerce.
    cclf8_schema = cclf8.collect_schema()
    if cclf8_schema["file_date"] == pl.String:
        cclf8 = cclf8.with_columns(pl.col("file_date").str.to_date(strict=False))

    # Dedup per (mbi, file_date) — CCLF8 has multiple rows per MBI otherwise
    cclf8_dedup = cclf8.unique(subset=["mbi", "file_date"], keep="first")
    max_cclf8_file_date = cclf8_dedup.select(pl.col("file_date").max()).collect().item()

    self_rows = cclf8_dedup.with_columns([
        pl.lit(None, dtype=pl.String).alias("maps_to_mbi"),
        pl.lit(None, dtype=pl.Date).alias("effective_date"),
        pl.lit(None, dtype=pl.Date).alias("obsolete_date"),
        pl.lit("cclf8_self").alias("observation_type"),
        (pl.col("file_date") == pl.lit(max_cclf8_file_date)).alias("is_current_as_of_file_date"),
    ])

    # ---- Chain computation -------------------------------------------------
    # Use only remap edges (self-edges add no topology). Materialize once.
    edges = cclf9.select(["prvs_num", "crnt_num"]).unique().collect()
    chain_map = _compute_chain_ids(edges).lazy()

    # Any MBI seen only in CCLF8 self-rows (never in a CCLF9 edge) is a
    # singleton chain — compute its chain_id from the MBI alone.
    def _singleton_hash(mbi_col: pl.Expr) -> pl.Expr:
        # Pure-polars sha1 isn't available; compute client-side per-row via map_elements.
        # Cheap — only fires for singletons (most of the cohort).
        return mbi_col.map_elements(
            lambda m: hashlib.sha1(m.encode("utf-8")).hexdigest()[:16] if m else None,
            return_dtype=pl.String,
        )

    # ---- Union, enrich, hop_index -----------------------------------------
    union_lf = pl.concat(
        [
            remap_rows.select([
                "mbi", "maps_to_mbi", "effective_date", "obsolete_date",
                "file_date", "observation_type", "source_file",
                "is_current_as_of_file_date",
            ]),
            self_rows.select([
                "mbi", "maps_to_mbi", "effective_date", "obsolete_date",
                "file_date", "observation_type", "source_file",
                "is_current_as_of_file_date",
            ]),
        ],
        how="vertical",
    )

    enriched = (
        union_lf
        .join(chain_map, on="mbi", how="left")
        .with_columns(
            pl.when(pl.col("chain_id").is_null())
            .then(_singleton_hash(pl.col("mbi")))
            .otherwise(pl.col("chain_id"))
            .alias("chain_id")
        )
    )

    # hop_index: one value per (chain_id, mbi). 0 goes to "leaf" MBIs —
    # those that never appear as prvs_num in any remap edge. Interior MBIs
    # are ranked by earliest file_date they appear, then by MBI string for
    # determinism. (Self-loops were already filtered from cclf9 above.)
    prvs_set = (
        cclf9.select(pl.col("prvs_num").alias("mbi"))
        .unique()
        .with_columns(pl.lit(True).alias("has_outgoing_remap"))
    )

    mbi_chain_enriched = (
        enriched
        .select(["chain_id", "mbi", "file_date"])
        .group_by(["chain_id", "mbi"])
        .agg(pl.col("file_date").min().alias("first_seen"))
        .join(prvs_set, on="mbi", how="left")
        .with_columns(pl.col("has_outgoing_remap").fill_null(False))
    )

    mbi_hops = mbi_chain_enriched.with_columns(
        pl.struct([
            pl.col("has_outgoing_remap").cast(pl.Int8),  # 0 = leaf -> ranked first
            pl.col("first_seen"),
            pl.col("mbi"),
        ]).rank(method="dense").over("chain_id").alias("_rank_within_chain")
    ).select([
        "chain_id",
        "mbi",
        (pl.col("_rank_within_chain") - 1).cast(pl.Int64).alias("hop_index"),
    ])

    with_hops = enriched.join(mbi_hops, on=["chain_id", "mbi"], how="left")

    # ---- HCMPI enrichment (optional) --------------------------------------
    # hcmpi_master stores identifiers row-per-(hcmpi, identifier_src_field).
    # MBIs live where identifier_src_field == 'member_mbi'. Keep one HCMPI
    # per MBI — if CMS has somehow assigned two, we take the first. The
    # presence check guards against the file genuinely being absent;
    # `pl.scan_parquet` is lazy, so a try/except around scan() never
    # fires and the FileNotFoundError used to leak out of collect().
    hcmpi_path = silver_path / "hcmpi_master.parquet"
    if hcmpi_path.exists():
        hcmpi = (
            pl.scan_parquet(hcmpi_path)
            .filter(pl.col("identifier_src_field") == "member_mbi")
            .select([
                pl.col("identifier").alias("mbi"),
                pl.col("hcmpi"),
            ])
            .unique(subset=["mbi"], keep="first")
        )
        final_lf = with_hops.join(hcmpi, on="mbi", how="left")
    else:
        final_lf = with_hops.with_columns(pl.lit(None, dtype=pl.String).alias("hcmpi"))

    # Final column order matches the pydantic schema
    return final_lf.select([
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
    ])
