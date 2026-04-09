"""
Enterprise crosswalk expression builder with HCMPI integration.

 comprehensive enterprise-wide Medicare Beneficiary Identifier
(MBI) crosswalk capabilities, including integration with Harmony Cares Master Person
Indentifier (HCMPI) data for beneficiary identity resolution across the healthcare
enterprise.

The enterprise crosswalk consolidates MBI mappings from multiple CMS sources and
enriches them with HCMPI linkage to create a unified view of beneficiary identity
that spans historical identifier changes and healthcare system boundaries.

Core Concepts

**MBI (Medicare Beneficiary Identifier)**: 11-character identifier used by CMS
**HCMPI (HarmonyCares Master Person Index)**: Enterprise master patient identifier
**Transitive Closure**: Resolving indirect MBI mappings through chains
**Self-Mapping**: Identity records where prvs_num equals crnt_num
**Chain Propagation**: Flowing HCMPI values through MBI change chains

Data Sources

- **CCLF8**: Current beneficiary eligibility with MBIs and demographics
- **CCLF9**: Historical MBI crosswalk (previous → current mappings)
- **HCMPI**: Enterprise patient identifiers linked to MBIs
- **MRN**: Medical Record Numbers for facility-level linkage

Enterprise Crosswalk Architecture

The enterprise crosswalk is built through several stages:

1. **Collection**: Gather all MBIs from CCLF8 and CCLF9 sources
2. **Self-Mapping**: Create identity records for all unique MBIs
3. **HCMPI Join**: Link MBIs to enterprise patient identifiers
4. **Transitive Closure**: Resolve multi-hop MBI chains
5. **Propagation**: Flow HCMPI values backward through chains
6. **Validation**: Apply quality checks and format validation

Why Enterprise Crosswalk Matters

Healthcare data often contains the same beneficiary under different identifiers:

- **Scenario 1**: Beneficiary changes from MBI_A → MBI_B → MBI_C over time
- **Scenario 2**: Multiple sites/records/facilities use different MRNs for same patient
- **Scenario 3**: Historical claims use old MBI, new claims use current MBI
- **Solution**: Enterprise crosswalk provides single source of truth

Without enterprise crosswalk:
  - Cannot accurately count unique beneficiaries
  - Patient history is fragmented across identifiers
  - Risk adjustment scores are incomplete
  - Care coordination is impaired

With enterprise crosswalk:
  - All identifiers resolve to single HCMPI
  - Complete longitudinal patient history
  - Accurate attribution and risk scoring
  - Seamless care coordination

Key Operations

**HCMPI Join**: Links MBIs to enterprise master person index
**Self-Mapping**: Creates identity records for resolution
**Chain Propagation**: Flows HCMPI through historical MBI chains
**Transitive Closure**: Resolves multi-hop identifier chains
**Validation**: Ensures data quality and format compliance

Use Cases

1. **Beneficiary attribution**: Link all claims to canonical patient ID
2. **Longitudinal analysis**: Track patient history across MBI changes
3. **Risk adjustment**: Calculate accurate RAF scores with complete history
4. **Care coordination**: Identify patient across healthcare system
5. **Data quality**: Detect and resolve identifier inconsistencies

HCMPI Propagation

Initial state:
  - MBI_OLD → MBI_CURRENT (from CCLF9)
  - MBI_CURRENT has HCMPI=12345 (from CCLF8)
  - MBI_OLD has HCMPI=null

After propagation:
  - MBI_OLD inherits HCMPI=12345
  - Both MBIs now link to same patient in enterprise system

This ensures historical claims with MBI_OLD are correctly attributed.

Notes
- Enterprise crosswalk is typically materialized as a reference table
- HCMPI propagation ensures all historical identifiers link to current patient
- Self-mappings enable consistent identity resolution logic
- Transitive closure resolves chains of any depth
- Validation ensures data quality for downstream analytics

See Also
- _bene_mbi_map.py: Beneficiary MBI mapping with transitive closure
- _xref.py: XREF mapping application in pipeline
- _builder.py: Pipeline orchestration
"""


import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression


@register_expression(
    "enterprise_crosswalk",
    schemas=["bronze", "silver"],
    dataset_types=["crosswalk", "eligibility"],
    callable=False,
    description="Build enterprise MBI crosswalk with HCMPI and transitive closure",
)
class EnterpriseCrosswalkExpression:
    """
    Build expressions for enterprise-wide MBI crosswalk with HCMPI integration.

        This expression builder creates idempotent expressions for:
        - Collecting all MBIs from various sources (CCLF8, CCLF9)
        - Creating self-mappings for identity resolution
        - Integrating HCMPI and MRN data
        - Building transitive closure for chain resolution
    """

    @staticmethod
    @expression(name="hcmpi_join", tier=["silver"], idempotent=True, sql_enabled=True)
    def build_hcmpi_join_expression(
        mbi_column: str, hcmpi_df: pl.LazyFrame, suffix: str = ""
    ) -> pl.Expr:
        """
        Build expression for joining HCMPI data on an MBI column.

                Creates a Polars expression that performs a lookup from an MBI column
                to the enterprise HCMPI (HarmonyCares Master Person Index) identifier.
                This enables linking beneficiary identifiers to the enterprise master
                patient index for unified identity resolution.

                The expression filters HCMPI data to MBI records only (excluding MRN
                and other identifier types), then creates a map-based lookup that can
                be applied to any MBI column in the dataset.

                Parameters

                mbi_column : str
                    Name of the column containing MBI values to look up.
                    Example: "prvs_num", "crnt_num", "bene_mbi_id"

                hcmpi_df : pl.LazyFrame
                    LazyFrame containing HCMPI mappings with required columns:
                    - Identifier: The MBI value to match against
                    - IdentifierSrcField: Source field name (must contain "mbi")
                    - HCMPI: The enterprise patient identifier (Int64)

                suffix : str, optional
                    Suffix to append to the resulting HCMPI column name.
                    Default is empty string.
                    Example: "_prvs" creates "hcmpi_prvs", "_crnt" creates "hcmpi_crnt"

                Returns

                pl.Expr
                    Polars expression that performs HCMPI lookup for the specified
                    MBI column. Returns Int64 HCMPI value or None if no match found.

                Notes
                - Only processes records where IdentifierSrcField contains "mbi"
                - Case-insensitive matching on IdentifierSrcField
                - Returns first matching HCMPI if multiple exist (uses limit(1))
                - Null MBI values return None
                - Uses map_elements for flexible lookup logic
        """
        mbi_hcmpi = (
            hcmpi_df.filter(pl.col("IdentifierSrcField").str.to_lowercase().str.contains("mbi"))
            .select(
                [pl.col("Identifier").alias("_lookup_mbi"), pl.col("HCMPI").alias(f"hcmpi{suffix}")]
            )
            .unique()
        )

        return pl.col(mbi_column).map_elements(
            lambda mbi: mbi_hcmpi.filter(pl.col("_lookup_mbi") == mbi)
            .select(f"hcmpi{suffix}")
            .limit(1)
            .collect()
            .item()
            if mbi
            else None,
            return_dtype=pl.Int64,
        )

    @staticmethod
    @expression(name="xref_mapping_metadata", tier=["silver"], idempotent=True, sql_enabled=True)
    def build_xref_mapping_metadata_expr() -> list[pl.Expr]:
        """
        Build metadata expressions for beneficiary_xref mappings.

        Returns:
            List of expressions to add metadata columns to xref records
        """
        import datetime

        return [
            pl.lit("xref").alias("mapping_type"),
            pl.lit(None, dtype=pl.String).alias("hcmpi"),
            pl.lit(None, dtype=pl.String).alias("mrn"),
            pl.lit(datetime.datetime.now()).alias("created_at"),
            pl.lit("EnterpriseCrosswalkExpression").alias("created_by"),
            (
                (pl.col("prvs_num").str.len_chars() == 11)
                & (pl.col("crnt_num").str.len_chars() == 11)
            ).alias("is_valid_mbi_format"),
            pl.lit(False).alias("has_circular_reference"),
            pl.lit(1).alias("chain_depth"),  # Xref mappings have depth=1
            pl.lit("beneficiary_xref").alias("source_system"),
        ]

    @staticmethod
    @expression(name="self_mapping_metadata", tier=["silver"], idempotent=True, sql_enabled=True)
    def build_self_mapping_metadata_expr(mbi_column: str = "bene_mbi_id") -> list[pl.Expr]:
        """
        Build metadata expressions for self-mapping records.

        Args:
            mbi_column: Name of the MBI column to use for self-mapping

        Returns:
            List of expressions to create self-mapping metadata
        """
        import datetime

        return [
            pl.col(mbi_column).alias("prvs_num"),
            pl.col(mbi_column).alias("crnt_num"),
            pl.lit("self").alias("mapping_type"),
            pl.lit(None, dtype=pl.String).alias("hcmpi"),
            pl.lit(None, dtype=pl.String).alias("mrn"),
            pl.lit(datetime.datetime.now()).alias("created_at"),
            pl.lit("EnterpriseCrosswalkExpression").alias("created_by"),
            (pl.col(mbi_column).str.len_chars() == 11).alias("is_valid_mbi_format"),
            pl.lit(False).alias("has_circular_reference"),
            pl.lit(0).alias("chain_depth"),
            pl.lit("beneficiary_demographics").alias("source_system"),
        ]

    @staticmethod
    def propagate_hcmpi_through_chains(
        crosswalk_df: pl.DataFrame, mbi_mapping_df: pl.DataFrame, hcmpi_mappings: pl.DataFrame
    ) -> pl.DataFrame:
        """
        Propagate HCMPI values through MBI chains.

                When a current MBI (from CCLF8) has HCMPI linkage, propagate that
                HCMPI to all its previous MBIs from the chain.

                Args:
                    crosswalk_df: Enterprise crosswalk with HCMPI linkage
                    mbi_mapping_df: Beneficiary MBI mappings (CCLF9 chains)
                    hcmpi_mappings: HCMPI to MBI mappings

                Returns:
                    Updated crosswalk with propagated HCMPI values
        """
        with_hcmpi = (
            crosswalk_df.filter(pl.col("hcmpi").is_not_null())
            .select(["prvs_num", "hcmpi"])
            .unique()
        )

        previous_mbis = mbi_mapping_df.join(
            with_hcmpi.rename({"prvs_num": "crnt_num"}), on="crnt_num", how="inner"
        )

        if len(previous_mbis) > 0:
            inherited_hcmpi = previous_mbis.select(
                [pl.col("prvs_num"), pl.col("hcmpi").alias("inherited_hcmpi")]
            ).unique()

            crosswalk_df = crosswalk_df.join(inherited_hcmpi, on="prvs_num", how="left")

            crosswalk_df = crosswalk_df.with_columns(
                [pl.coalesce(["hcmpi", "inherited_hcmpi"]).alias("hcmpi")]
            )

            if "inherited_hcmpi" in crosswalk_df.columns:
                crosswalk_df = crosswalk_df.drop(["inherited_hcmpi"])

        return crosswalk_df

    @staticmethod
    @expression(name="transitive_closure", tier=["silver"], idempotent=True, sql_enabled=False)
    def build_transitive_expressions(mappings: pl.DataFrame) -> list[pl.Expr]:
        """
        Build expressions for transitive closure resolution.

                Args:
                    mappings: DataFrame with prvs_num and crnt_num columns

                Returns:
                    List of expressions for transitive mappings
        """
        expressions = []

        mapping_dict = {}
        for row in mappings.to_dicts():
            if row.get("prvs_num") and row.get("crnt_num"):
                mapping_dict[row["prvs_num"]] = row["crnt_num"]

        def resolve_chain(mbi: str, visited: set = None) -> tuple[str, int]:
            if visited is None:
                visited = set()
            if mbi in visited:
                return mbi, 0
            visited.add(mbi)
            if mbi not in mapping_dict:
                return mbi, 0
            next_mbi = mapping_dict[mbi]
            if next_mbi == mbi:
                return mbi, 0
            final_mbi, depth = resolve_chain(next_mbi, visited)
            return final_mbi, depth + 1

        for start_mbi in mapping_dict.keys():
            final_mbi, depth = resolve_chain(start_mbi)
            if depth > 1:
                expressions.append(
                    pl.when(pl.col("prvs_num") == start_mbi).then(
                        pl.struct(
                            [
                                pl.lit(final_mbi).alias("transitive_crnt"),
                                pl.lit(depth).alias("transitive_depth"),
                            ]
                        )
                    )
                )

        return expressions

    @staticmethod
    @expression(name="hcmpi_coalesce", tier=["silver"], idempotent=True, sql_enabled=True)
    def build_hcmpi_coalesce_expr() -> pl.Expr:
        """
        Build expression to coalesce HCMPI from multiple join columns.

        Prioritizes:
        1. Existing hcmpi value (if already populated)
        2. HCMPI from prvs_num join
        3. HCMPI from crnt_num join

        Returns:
            Expression that coalesces HCMPI values
        """
        return pl.coalesce(
            [
                pl.col("hcmpi"),
                pl.col("hcmpi_prvs"),
                pl.col("hcmpi_crnt"),
            ]
        ).alias("hcmpi")

    @staticmethod
    @expression(name="chain_depth_update", tier=["silver"], idempotent=True, sql_enabled=True)
    def build_chain_depth_update_expr() -> pl.Expr:
        """
        Build expression to update chain_depth for xref mappings.

        Direct xref mappings (non-chain) should have depth = 1.
        Chain mappings computed from transitive closure have depth > 1.

        Returns:
            Expression that updates chain_depth
        """
        return (
            pl.when(pl.col("mapping_type") == "xref")
            .then(pl.lit(1))
            .otherwise(pl.col("chain_depth"))
            .alias("chain_depth")
        )

    @staticmethod
    @expression(name="ent_xwalk_validation", tier=["silver"], idempotent=True, sql_enabled=True)
    def build_validation_expressions() -> list[pl.Expr]:
        """
        Build expressions for MBI validation and quality checks.

                Returns:
                    List of validation expressions
        """
        return [
            (
                (pl.col("prvs_num").str.len_chars() == 11)
                & (pl.col("crnt_num").str.len_chars() == 11)
            ).alias("is_valid_mbi_format"),
            pl.when(pl.col("mapping_type") != "self")
            .then(pl.col("prvs_num") != pl.col("crnt_num"))
            .otherwise(True)
            .alias("no_invalid_self_ref"),
            pl.lit("ACOHarmony").alias("created_by"),
            pl.lit("enterprise_crosswalk").alias("source_system"),
        ]

