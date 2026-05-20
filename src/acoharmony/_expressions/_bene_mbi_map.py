"""
Beneficiary MBI mapping expression builder with transitive closure.

 expression builders for creating beneficiary Medicare
Beneficiary Identifier (MBI) mappings with transitive closure support. It
handles the conversion of previous MBIs to current MBIs, tracking chains of
MBI changes over time.

The transitive closure ensures that if beneficiary A's MBI changed to B, and
B's MBI changed to C, we create a direct mapping from A to C. This is critical
for accurate beneficiary identification across historical data.

Key Features

- Direct MBI mappings (previous -> current)
- Transitive closure computation (chain resolution)
- Chain depth tracking for audit trails
- Effective date management for temporal accuracy
- Validation expressions for data quality
- Metadata enrichment for lineage tracking

Schema Requirements

Input schemas must contain:
- prvs_num: Previous MBI (11 characters)
- crnt_num: Current MBI (11 characters)
- prvs_id_efctv_dt: Effective date of the previous MBI
- prvs_id_obslt_dt: Obsolete date of the previous MBI

Use Cases

1. **Historical beneficiary reconciliation**: Link claims across MBI changes
2. **Data quality validation**: Detect circular MBI mappings
3. **Audit trail maintenance**: Track depth of MBI change chains
4. **Temporal alignment**: Ensure dates align with MBI transitions


Notes

- Self-mappings (prvs_num == crnt_num) are automatically filtered out
- Maximum chain depth is limited to 10 to prevent infinite loops
- Transitive mappings are marked with is_transitive=True for identification
- Date ranges are preserved through transitive chains for temporal accuracy
"""

import polars as pl

from acoharmony._decor8 import expression

from ._registry import register_expression


@register_expression(
    "beneficiary_mbi_mapping",
    schemas=["bronze", "silver"],
    dataset_types=["eligibility", "crosswalk"],
    callable=False,
    description="Create MBI mapping with transitive closure for beneficiary identification",
)
class BeneficiaryMbiMappingExpression:
    """
    Build expressions for beneficiary MBI mapping with transitive closure.

        This expression builder creates idempotent expressions for:
        - Direct MBI mappings (prvs -> crnt)
        - Transitive closure for chain resolution (A->B->C creates A->C)
        - Chain depth tracking
        - Effective date management
    """

    @staticmethod
    @staticmethod
    @expression(name="bene_mbi_validation", tier=["silver"], idempotent=True, sql_enabled=True)
    def build_validation_expressions() -> list[pl.Expr]:
        """
        Build expressions for validating MBI mappings.

                Creates a set of validation expressions to ensure MBI mapping quality:
                - Format validation: Ensures MBIs are exactly 11 characters
                - Crosswalk validation: Confirms mappings are not self-referential
                - Date validation: Checks for presence of effective dates
                - Transitivity detection: Identifies transitive vs. direct mappings

                Returns

                list[pl.Expr]
                    List of Polars expressions for validation, each creating a boolean column:
                    - is_valid_format: True if both prvs_num and crnt_num are 11 characters
                    - is_crosswalk: True if prvs_num != crnt_num (not self-mapping)
                    - has_effective_date: True if prvs_id_efctv_dt is not null
                    - is_transitive: True if chain_depth > 1 (transitive mapping)
        """
        return [
            # Valid MBI format (11 characters)
            (
                (pl.col("prvs_num").str.len_chars() == 11)
                & (pl.col("crnt_num").str.len_chars() == 11)
            ).alias("is_valid_format"),
            # Not self-mapping
            (pl.col("prvs_num") != pl.col("crnt_num")).alias("is_crosswalk"),
            # Has dates
            pl.col("prvs_id_efctv_dt").is_not_null().alias("has_effective_date"),
            # Chain indicator
            (pl.col("chain_depth") > 1).alias("is_transitive"),
        ]

    @staticmethod
    @expression(name="bene_mbi_metadata", tier=["silver"], idempotent=True, sql_enabled=True)
    def build_metadata_expressions() -> list[pl.Expr]:
        """
        Build expressions for adding metadata to MBI mappings.

                Creates standardized metadata expressions for data lineage tracking,
                audit trails, and system documentation. These expressions add columns
                indicating when and how the mapping was created.

                Returns

                list[pl.Expr]
                    List of Polars expressions that add metadata columns:
                    - created_by: System identifier ("ACOHarmony")
                    - created_at: Timestamp of creation (datetime)
                    - load_date: Date of data load (date)
                    - source_system: Original data source ("CCLF9")

                Notes

                - created_at uses datetime.now() which captures the exact timestamp
                - load_date uses date.today() for the calendar date
                - These are literal values applied uniformly to all rows
                - Source system is hardcoded to "CCLF9" (CMS CCLF 9 file format)
        """
        from datetime import date, datetime

        return [
            pl.lit("ACOHarmony").alias("created_by"),
            pl.lit(datetime.now()).alias("created_at"),
            pl.lit(date.today()).alias("load_date"),
            pl.lit("CCLF9").alias("source_system"),
        ]
