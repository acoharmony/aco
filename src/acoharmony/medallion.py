# © 2025 HarmonyCares
# All rights reserved.

"""
Medallion architecture enums and Unity Catalog namespace support.

Provides clear terminology and type-safe enums for the medallion architecture
(Bronze/Silver/Gold) aligned with Unity Catalog naming conventions.
"""

from enum import Enum


class MedallionLayer(Enum):
    """
    Medallion architecture layers aligned with Unity Catalog schemas.

        The medallion architecture is a data design pattern used to logically
        organize data in a lakehouse, with each layer representing a different
        level of data quality and refinement.

        Attributes

        BRONZE : str
            Raw, unprocessed data ingested from source systems
        SILVER : str
            Cleaned, validated, and enriched data
        GOLD : str
            Business-level aggregated data ready for analytics
    """

    BRONZE = "bronze"  # Raw, unprocessed
    SILVER = "silver"  # Cleaned, validated
    GOLD = "gold"  # Business-level aggregates

    @property
    def unity_schema(self) -> str:
        """
        Unity Catalog schema name.

                Returns

                str
                    The schema name for Unity Catalog (bronze/silver/gold)
        """
        return self.value

    @property
    def data_tier(self) -> str:
        """
        Data tier name (same as unity_schema - bronze/silver/gold).

                Returns

                str
                    Tier name (bronze/silver/gold)
                'silver'
        """
        return self.value

    @classmethod
    def from_tier(cls, tier: str) -> "MedallionLayer":
        """
        Convert tier name to medallion layer.

                Parameters

                tier : str
                    Tier name (bronze/silver/gold)

                Returns

                MedallionLayer
                    Corresponding medallion layer
        """
        return cls(tier.lower())

    @classmethod
    def from_unity_schema(cls, schema: str) -> "MedallionLayer":
        """
        Convert Unity Catalog schema name to medallion layer.

                Parameters

                schema : str
                    Unity Catalog schema name (bronze/silver/gold)

                Returns

                MedallionLayer
                    Corresponding medallion layer
        """
        return cls(schema.lower())


class UnityCatalogNamespace:
    """
    Represents a fully qualified Unity Catalog table name.

        Unity Catalog uses a three-level namespace: catalog.schema.table
        where:
        - catalog: Top-level namespace (e.g., "main", "dev", "prod")
        - schema: Database/namespace within catalog (e.g., "bronze", "silver", "gold")
        - table: Actual data table (e.g., "cclf1", "institutional_claim")
        main.bronze.cclf1
        MedallionLayer.BRONZE
        main.silver.institutional_claim
    """

    def __init__(self, catalog: str, schema: str, table: str):
        """
        Initialize Unity Catalog namespace.

                Parameters

                catalog : str
                    Catalog name (e.g., "main", "dev", "prod")
                schema : str
                    Schema name (e.g., "bronze", "silver", "gold")
                table : str
                    Table name (e.g., "cclf1", "institutional_claim")
        """
        self.catalog = catalog
        self.schema = schema
        self.table = table

    @property
    def full_name(self) -> str:
        """
        Returns fully qualified table name.

                Returns

                str
                    Full table name in format: catalog.schema.table
        """
        return f"{self.catalog}.{self.schema}.{self.table}"

    @property
    def medallion_layer(self) -> MedallionLayer:
        """
        Returns the medallion layer for this schema.

                Returns

                MedallionLayer
                    The medallion layer based on the schema name
        """
        return MedallionLayer.from_unity_schema(self.schema)

    def __str__(self) -> str:
        """String representation."""
        return self.full_name

    def __repr__(self) -> str:
        """Detailed representation."""
        return f"UnityCatalogNamespace(catalog='{self.catalog}', schema='{self.schema}', table='{self.table}')"
