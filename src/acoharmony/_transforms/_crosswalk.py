# © 2025 HarmonyCares
# All rights reserved.

"""
Crosswalk transformation implementations.

This module contains transformations for mapping between different
identifier systems, particularly for beneficiary MBI resolution and
enterprise-wide patient matching.

What is Crosswalk?

Crosswalk transformations map between different identifier systems to resolve
identity changes over time. In healthcare, identifiers change due to:

- Medicare card replacements (MBI transitions)
- System migrations
- Data entry errors that get corrected
- Patient record merges

Without crosswalk, these changes break longitudinal patient analysis.

Key Concepts

- **MBI Mapping**: Map previous MBI (prvs_num) to current MBI (crnt_num)
- **Transitive Closure**: If A→B and B→C, then A→C (resolves chains)
- **Self-Mappings**: Current MBIs map to themselves for completeness
- **Enterprise Crosswalk**: Comprehensive mapping from all sources
- **CCLF9 Source**: CMS provides crosswalk data in CCLF9 files

Common Use Cases

- **MBI Resolution**: Resolve historical MBI to current MBI for claims matching
- **Identity Consolidation**: Link all MBI versions for a beneficiary
- **Historical Analysis**: Track beneficiary across identifier changes
- **Quality Measures**: Ensure continuity of care metrics span MBI changes
- **Attribution**: Correctly attribute members across identifier transitions

Transitive Closure

When MBI changes multiple times (A→B→C), we need all mappings:
- Direct: A→B, B→C (from CCLF9)
- Transitive: A→C (computed)
- Self: A→A, B→B, C→C (added)

This ensures any version of the MBI resolves to the current one.

CCLF File Sources

- **CCLF8**: Current beneficiary demographics (bene_mbi_id)
- **CCLF9**: MBI crosswalk mappings (prvs_num → crnt_num)

"""

from typing import Any

import polars as pl

from .._decor8 import transform, transform_method
from ._registry import register_crosswalk


@transform_method(
    enable_composition=True,
    threshold=5.0,
)
@transform(
    name="crosswalk",
    tier=["bronze", "silver"],
    description="Apply MBI crosswalk/mapping logic for identifier resolution",
    sql_enabled=True,
)
@register_crosswalk(name="standard")
def crosswalk(df: pl.LazyFrame) -> pl.LazyFrame:
    """
    Apply crosswalk/mapping logic for identifier resolution.

        Crosswalk transformations map between different identifier systems,
        most commonly for MBI (Medicare Beneficiary Identifier) resolution.
        This ensures that historical and current identifiers are properly
        linked.

        Args:
            df: Input LazyFrame containing identifiers to map

        Returns:
            pl.LazyFrame: LazyFrame with resolved identifiers

        Note:
            Prioritizes prvs_num when available, falls back to crnt_num.
    """
    if "prvs_num" in df.columns and "crnt_num" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("prvs_num").is_not_null())
            .then(pl.col("prvs_num"))
            .otherwise(pl.col("crnt_num"))
            .alias("resolved_mbi")
        )

    return df


@transform_method(enable_composition=True, threshold=5.0)
@transform(
    name="apply_xref_transform",
    tier=["silver"],
    description="Apply cross-reference mapping transformation with xref table join",
    sql_enabled=True,
)
@register_crosswalk(name="apply_xref_transform")
def apply_xref_transform(
    df: pl.LazyFrame, xref_config: dict, catalog: Any, logger: Any
) -> pl.LazyFrame:
    """
    Apply cross-reference mapping transformation (idempotent).

        Maps identifiers using a cross-reference table, typically for
        beneficiary MBI resolution. Checks if transformation has already
        been applied to avoid duplicate processing.

        Args:
            df: Input LazyFrame to transform
            xref_config: Configuration dictionary containing:
                - table: Name of xref table (default: 'beneficiary_xref')
                - join_key: Column to join on (default: 'bene_mbi_id')
                - xref_key: Key column in xref table (default: 'prvs_num')
                - current_column: Current value column (default: 'crnt_num')
                - output_column: Output column name (default: 'current_bene_mbi_id')
            catalog: Catalog instance for accessing storage paths
            logger: Logger instance for recording operations

        Returns:
            pl.LazyFrame: DataFrame with xref mapping applied

        Note:
            Skips transformation if output column already exists.
    """
    if xref_config is None:
        logger.warning("xref_config is None, skipping xref transform")
        return df

    if catalog is None or not hasattr(catalog, "storage_config") or catalog.storage_config is None:
        logger.warning("catalog or storage_config is None, skipping xref transform")
        return df

    xref_table = xref_config.get("table", "beneficiary_xref")
    join_key = xref_config.get("join_key", "bene_mbi_id")
    xref_key = xref_config.get("xref_key", "prvs_num")
    current_col = xref_config.get("current_column", "crnt_num")
    output_col = xref_config.get("output_column", "current_bene_mbi_id")

    logger.info(f"Applying xref from {xref_table} on {join_key}")

    if output_col in df.collect_schema().names():
        logger.info(f"Xref column {output_col} already exists, skipping")
        return df

    xref_path = catalog.storage_config.get_data_path("silver") / f"{xref_table}.parquet"
    if not xref_path.exists():
        logger.warning(f"Xref table {xref_path} not found, skipping xref transform")
        return df

    xref_df = pl.scan_parquet(str(xref_path))

    df = df.join(
        xref_df.select([xref_key, current_col]), left_on=join_key, right_on=xref_key, how="left"
    )

    df = df.with_columns(pl.coalesce([pl.col(current_col), pl.col(join_key)]).alias(output_col))

    if current_col in df.collect_schema().names():
        df = df.drop(current_col)

    return df


@transform_method(enable_composition=True, threshold=5.0)
@transform(
    name="apply_beneficiary_mbi_mapping",
    tier=["silver"],
    description="Apply beneficiary MBI mapping with transitive closure resolution",
    sql_enabled=False,
)
@register_crosswalk(name="beneficiary_mbi_mapping")
def apply_beneficiary_mbi_mapping(df: pl.LazyFrame, logger: Any) -> pl.LazyFrame:
    """
    Apply beneficiary MBI mapping transformation with transitive closure.

        This transformation:
        1. Deduplicates mappings per prvs_num (keeping most recent)
        2. Builds transitive closure to resolve chains (A→B→C becomes A→B, B→C, and A→C)
        3. Identifies mapping types and validates MBIs

        Args:
            df: Input LazyFrame from CCLF9 staging
            logger: Logger instance for recording operations

        Returns:
            Transformed LazyFrame with MBI mappings including transitive closure
    """
    from .._expressions import BeneficiaryMbiMappingExpression

    logger.info("Starting beneficiary_mbi_mapping transformation with transitive closure")
    df = df.unique(subset=["prvs_num", "crnt_num"], keep="last")
    mappings = df.select(["prvs_num", "crnt_num"]).collect()
    mapping_dict = {}
    for row in mappings.iter_rows():
        prvs, crnt = row
        if prvs not in mapping_dict:
            mapping_dict[prvs] = set()
        mapping_dict[prvs].add(crnt)
    closure_rows = []

    def find_all_descendants(start_mbi, current_mbi=None, path=None):
        """Recursively find all reachable MBIs from a starting point."""
        if current_mbi is None:
            current_mbi = start_mbi
            path = [start_mbi]

        if current_mbi in mapping_dict:
            for next_mbi in mapping_dict[current_mbi]:
                if next_mbi not in path:
                    closure_rows.append(
                        {"prvs_num": start_mbi, "crnt_num": next_mbi, "mapping_distance": len(path)}
                    )
                    find_all_descendants(start_mbi, next_mbi, path + [next_mbi])

    for prvs in mapping_dict:
        find_all_descendants(prvs)

    if closure_rows:
        closure_df = pl.DataFrame(closure_rows).lazy()
        df = df.join(closure_df, on=["prvs_num", "crnt_num"], how="outer", coalesce=True)

    metadata_exprs = BeneficiaryMbiMappingExpression.build_metadata_expressions()
    df = df.with_columns(metadata_exprs)

    return df
