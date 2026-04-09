# © 2025 HarmonyCares
# All rights reserved.

"""
Enterprise crosswalk transform with HCMPI integration and historical chain detection.

Builds enterprise-wide MBI crosswalk by:
1. Analyzing raw CCLF9 historical data to detect MBI changes over time
2. Extracting latest MBI mappings (CMS provides pre-collapsed chains)
3. Creating self-mappings for beneficiaries from int_beneficiary_demographics_deduped
4. Computing transitive closure for any remaining MBI chains
5. Enriching with HCMPI (enterprise patient identifiers)
6. Applying validation rules

Note: CMS provides pre-collapsed MBI chains in CCLF9, so transitive closure
typically finds zero chains. Historical analysis logs MBIs that changed multiple
times over the years for audit purposes.

Idempotent and schema-driven.
"""

import datetime
from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from .._expressions._ent_xwalk import EnterpriseCrosswalkExpression
from ._registry import register_crosswalk


@register_crosswalk(name="enterprise_xwalk", sql_enabled=False)
@transform(name="enterprise_xwalk", tier=["silver"], sql_enabled=False)
@transform_method(enable_composition=False, threshold=10.0)
def apply_transform(
    df: pl.LazyFrame | None, schema: dict, catalog: Any, logger: Any, force: bool = False
) -> pl.LazyFrame:
    """
    Build enterprise crosswalk from raw historical CCLF9 data.

    This transform analyzes all historical CCLF9 files to detect MBI changes
    over time, then creates a unified MBI crosswalk with HCMPI integration.

    Args:
        df: Not used (foundation transform builds from catalog sources)
        schema: Schema configuration
        catalog: Catalog for accessing source data
        logger: Logger instance
        force: Force reprocessing

    Returns:
        pl.LazyFrame: Enterprise crosswalk with columns:
            - prvs_num: Previous MBI
            - crnt_num: Current MBI
            - mapping_type: Type of mapping (self, xref, chain)
            - hcmpi: Enterprise patient identifier
            - mrn: Medical record number (if available)
            - created_at: Processing timestamp
            - created_by: Creator identifier
            - is_valid_mbi_format: Validation flag
            - has_circular_reference: Circular reference flag
            - chain_depth: Depth in MBI chain
            - source_system: Source system name
            - source_file: Source filename
            - load_date: Source file date
    """
    # Check if already exists
    if not force and catalog.get_table_metadata("enterprise_xwalk") is not None:
        try:
            logger.info("Enterprise crosswalk already exists, loading from catalog")
            return catalog.scan_table("enterprise_xwalk")
        except Exception:
            logger.info("Enterprise crosswalk metadata found but data missing, rebuilding")

    logger.info("Building enterprise crosswalk from raw CCLF9 data (with historical chain detection)")

    # Get silver path for scanning source data
    from ..medallion import MedallionLayer

    silver_path = catalog.storage_config.get_path(MedallionLayer.SILVER)

    # Scan source data - use raw CCLF9 for historical chain detection, deduplicated demographics for self-mappings
    try:
        bene_demo_df = pl.scan_parquet(silver_path / "int_beneficiary_demographics_deduped.parquet")
    except Exception:
        bene_demo_df = None

    try:
        # Use RAW CCLF9 data to preserve historical chain information
        bene_xref_df = pl.scan_parquet(silver_path / "cclf9.parquet")
        logger.info("Using raw CCLF9 data to detect historical MBI chains")
    except Exception:
        bene_xref_df = None

    hcmpi_df = catalog.scan_table("hcmpi_master")

    if bene_demo_df is None and bene_xref_df is None:
        raise ValueError(
            "At least one of int_beneficiary_demographics_deduped.parquet or cclf9.parquet required for enterprise crosswalk. "
            "Check that CCLF8/CCLF9 files have been processed."
        )

    dfs_to_union = []
    xref_mbis_set = set()

    # Step 1: Process CCLF9 xref mappings and detect chains from historical data
    if bene_xref_df is not None:
        logger.info("Processing CCLF9 xref mappings from raw historical data")

        # Collect all historical mappings first for analysis
        all_historical_mappings = (
            bene_xref_df
            .filter(pl.col("prvs_num") != pl.col("crnt_num"))
            .select(["prvs_num", "crnt_num", "file_date", "source_filename"])
            .collect()
        )

        logger.info(f"Loaded {all_historical_mappings.height:,} historical xref records")

        # Detect historical progressions for audit/tracking
        # Find MBIs that had multiple crnt_num values over time
        historical_progressions = (
            all_historical_mappings
            .group_by("prvs_num")
            .agg([
                pl.col("crnt_num").n_unique().alias("num_changes"),
                pl.col("crnt_num").sort_by("file_date").first().alias("first_crnt"),
                pl.col("crnt_num").sort_by("file_date").last().alias("latest_crnt"),
            ])
            .filter(pl.col("num_changes") > 1)
        )

        if historical_progressions.height > 0:
            logger.info(f"Found {historical_progressions.height:,} MBIs with historical changes (multiple crnt_num over time)")
            logger.info("Note: CMS provides pre-collapsed mappings, so chains are already resolved in latest data")

        # Deduplicate to get latest mapping per prvs_num
        # Use window function to rank by file_date descending
        xref_latest = (
            bene_xref_df
            .filter(pl.col("prvs_num") != pl.col("crnt_num"))  # Exclude self-references
            .with_columns(
                pl.col("file_date")
                .rank(method="ordinal", descending=True)
                .over("prvs_num")
                .alias("row_num")
            )
            .filter(pl.col("row_num") == 1)
            .drop("row_num")
        )

        xref_mappings = (
            xref_latest
            .select(
                [
                    "prvs_num",
                    "crnt_num",
                    pl.col("source_filename").alias("source_file"),
                    pl.col("file_date").alias("load_date"),
                ]
            )
            .with_columns(EnterpriseCrosswalkExpression.build_xref_mapping_metadata_expr())
            .select(
                [
                    "prvs_num",
                    "crnt_num",
                    "mapping_type",
                    "hcmpi",
                    "mrn",
                    "created_at",
                    "created_by",
                    "is_valid_mbi_format",
                    "has_circular_reference",
                    "chain_depth",
                    "source_system",
                    "source_file",
                    "load_date",
                ]
            )
        )
        dfs_to_union.append(xref_mappings)

        # Collect all MBIs from xref for self-mapping exclusion
        prvs_mbis = xref_mappings.select(pl.col("prvs_num").alias("mbi"))
        crnt_mbis = xref_mappings.select(pl.col("crnt_num").alias("mbi"))
        xref_collected = pl.concat([prvs_mbis, crnt_mbis]).unique().collect()
        xref_mbis_set = set(xref_collected.get_column("mbi").to_list())
        logger.info(f"Found {len(xref_mbis_set):,} unique MBIs in CCLF9 xref data")

    # Step 2: Process int_beneficiary_demographics_deduped self-mappings
    if bene_demo_df is not None:
        logger.info("Processing int_beneficiary_demographics_deduped self-mappings")

        # Get all demo MBIs
        all_demo_mbis = (
            bene_demo_df.select(
                [
                    pl.col("current_bene_mbi_id").alias("bene_mbi_id"),
                    pl.col("source_filename").alias("source_file"),
                    pl.col("file_date").alias("load_date"),
                ]
            )
            .unique(subset=["bene_mbi_id"])
            .collect()
        )
        demo_mbis_set = set(all_demo_mbis.get_column("bene_mbi_id").to_list())
        logger.info(f"Found {len(demo_mbis_set):,} unique MBIs in int_beneficiary_demographics_deduped")

        # Only create self-mappings for MBIs not already in xref
        mbis_needing_self_mapping = demo_mbis_set - xref_mbis_set
        logger.info(f"Creating self-mappings for {len(mbis_needing_self_mapping):,} MBIs")

        if mbis_needing_self_mapping:
            self_mapping_data = all_demo_mbis.filter(
                pl.col("bene_mbi_id").is_in(list(mbis_needing_self_mapping))
            )

            # Use expression builder for self-mapping metadata
            self_mappings = (
                self_mapping_data.with_columns(
                    EnterpriseCrosswalkExpression.build_self_mapping_metadata_expr()
                )
                .select(
                    [
                        "prvs_num",
                        "crnt_num",
                        "mapping_type",
                        "hcmpi",
                        "mrn",
                        "created_at",
                        "created_by",
                        "is_valid_mbi_format",
                        "has_circular_reference",
                        "chain_depth",
                        "source_system",
                        "source_file",
                        "load_date",
                    ]
                )
                .lazy()
            )
            dfs_to_union.append(self_mappings)

    # Step 3: Union all mappings
    if not dfs_to_union:
        raise ValueError("No source data available for enterprise crosswalk")

    logger.info("Unioning xref and self-mapping data")
    result = pl.concat(dfs_to_union, how="diagonal")

    # Step 4: Compute transitive closure for MBI chains
    if bene_xref_df is not None:
        logger.info("Computing transitive closure for MBI chains")

        # Collect xref mappings for chain computation
        xref_only = result.filter(pl.col("mapping_type") == "xref").collect()

        if len(xref_only) > 0:
            # Build mapping dictionary
            all_mappings = {}
            all_prvs = set()
            all_crnt = set()

            for row in xref_only.to_dicts():
                prvs = row.get("prvs_num")
                crnt = row.get("crnt_num")
                if prvs and crnt and prvs != crnt:
                    if prvs not in all_mappings:
                        all_mappings[prvs] = []
                    all_mappings[prvs].append(crnt)
                    all_prvs.add(prvs)
                    all_crnt.add(crnt)

            # Resolve chain conflicts (prefer intermediate hops)
            mapping_dict = {}
            for prvs, crnt_list in all_mappings.items():
                if len(crnt_list) == 1:
                    mapping_dict[prvs] = crnt_list[0]
                else:
                    intermediate_hops = [c for c in crnt_list if c in all_prvs]
                    if intermediate_hops:
                        mapping_dict[prvs] = intermediate_hops[0]
                    else:
                        mapping_dict[prvs] = crnt_list[0]

            # Resolve chains and compute depths
            def resolve_chain(mbi: str, visited: set = None) -> tuple[str, int]:
                """Resolve MBI chain to final MBI and depth."""
                if visited is None:
                    visited = set()
                if mbi in visited:  # Circular reference
                    return mbi, 0
                visited.add(mbi)
                if mbi not in mapping_dict:
                    return mbi, 0
                next_mbi = mapping_dict[mbi]
                if next_mbi == mbi:
                    return mbi, 0
                final_mbi, depth = resolve_chain(next_mbi, visited)
                return final_mbi, depth + 1

            # Build source info lookup
            source_info_dict = {}
            for row in xref_only.to_dicts():
                prvs = row.get("prvs_num")
                if prvs and prvs not in source_info_dict:
                    source_info_dict[prvs] = {
                        "source_file": row.get("source_file"),
                        "load_date": row.get("load_date"),
                    }

            # Create transitive chain mappings (depth > 1 only)
            chain_mappings = []
            chains_found = 0
            for start_mbi in mapping_dict.keys():
                final_mbi, depth = resolve_chain(start_mbi)
                if depth > 1:
                    chains_found += 1
                    source_info = source_info_dict.get(start_mbi, {})
                    chain_mappings.append(
                        {
                            "prvs_num": start_mbi,
                            "crnt_num": final_mbi,
                            "mapping_type": "chain",
                            "hcmpi": None,
                            "mrn": None,
                            "created_at": datetime.datetime.now(),
                            "created_by": "EnterpriseCrosswalkExpression",
                            "is_valid_mbi_format": len(start_mbi) == 11 and len(final_mbi) == 11,
                            "has_circular_reference": False,
                            "chain_depth": depth,
                            "source_system": "int_beneficiary_xref_deduped",
                            "source_file": source_info.get("source_file"),
                            "load_date": source_info.get("load_date"),
                        }
                    )

            logger.info(f"Found {chains_found:,} transitive chains")

            # Add chain mappings to result
            if chain_mappings:
                chain_df = (
                    pl.DataFrame(chain_mappings)
                    .with_columns(
                        [
                            pl.col("hcmpi").cast(pl.String),
                            pl.col("mrn").cast(pl.String),
                            pl.col("chain_depth").cast(pl.Int32),
                        ]
                    )
                    .lazy()
                )
                result = pl.concat([result, chain_df], how="diagonal")

            # Update chain_depth for direct xref mappings using expression builder
            result = result.with_columns([EnterpriseCrosswalkExpression.build_chain_depth_update_expr()])

    # Step 5: Enrich with HCMPI data
    if hcmpi_df is not None:
        logger.info("Enriching with HCMPI data")

        # Create HCMPI lookup: MBI -> HCMPI
        # Note: rcd_active is always null in current data, so use date-based filtering
        # Records with eff_end_dt = '9999-12-31' are current/active
        hcmpi_lookup = (
            hcmpi_df.filter(
                pl.col("identifier_src_field").cast(pl.String).str.contains("(?i)mbi")
            )
            .select(
                [
                    pl.col("identifier").alias("mbi"),
                    pl.col("hcmpi"),
                ]
            )
            .unique(subset=["mbi"], keep="first")
        )

        # Join HCMPI on prvs_num
        result = result.join(hcmpi_lookup, left_on="prvs_num", right_on="mbi", how="left", suffix="_prvs")

        # Join HCMPI on crnt_num
        result = result.join(hcmpi_lookup, left_on="crnt_num", right_on="mbi", how="left", suffix="_crnt")

        # Coalesce HCMPI using expression builder
        result = result.with_columns([EnterpriseCrosswalkExpression.build_hcmpi_coalesce_expr()]).drop(
            ["hcmpi_prvs", "hcmpi_crnt"]
        )

    # Step 6: Deduplicate on natural key
    logger.info("Deduplicating on natural key (prvs_num, crnt_num, mapping_type)")
    result = result.unique(subset=["prvs_num", "crnt_num", "mapping_type"], keep="last")

    logger.info("Enterprise crosswalk build complete")
    return result
