# © 2025 HarmonyCares
# All rights reserved.

"""
Schema-driven catalog for ACO Harmony.
- Catalog handles schema metadata and discovery
- Parsers handle file reading based on format
- Transform classes handle data transformations
"""

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import polars as pl

from . import parsers
from ._registry import SchemaRegistry
from ._store import StorageBackend
from .medallion import MedallionLayer, UnityCatalogNamespace


@dataclass
class TableMetadata:
    """
    Metadata definition for a data table.

        This represents the structure and storage configuration for a
        single table in the data pipeline, aligned with Unity Catalog
        and medallion architecture standards.

        Attributes

        name : str
            Table name (e.g., "cclf1", "institutional_claim")
        description : str
            Human-readable description
        columns : List[Dict[str, Any]]
            List of column definitions
        storage : Dict[str, Any]
            Storage configuration
        file_format : Dict[str, Any]
            File format specifications
        medallion_layer : Optional[MedallionLayer]
            Which layer this table belongs to (Bronze/Silver/Gold)
        unity_catalog : str
            Unity Catalog name (default: "main")
    """

    name: str
    description: str
    columns: list[dict[str, Any]]
    storage: dict[str, Any]
    file_format: dict[str, Any]

    # Unity Catalog and Medallion properties
    medallion_layer: MedallionLayer | None = None
    unity_catalog: str = "main"

    polars: dict[str, Any] = field(default_factory=dict)

    # Multi-record file support (TPARC, etc.)
    record_types: dict[str, Any] | None = None

    # Multi-sheet Excel support (PYRED, etc.)
    sheets: list[dict[str, Any]] | None = None
    matrix_fields: list[dict[str, Any]] | None = None

    @property
    def unity_namespace(self) -> UnityCatalogNamespace | None:
        """
        Returns Unity Catalog namespace for this table.

                Returns

                UnityCatalogNamespace or None
                    Unity Catalog namespace if medallion_layer is set
        """
        if self.medallion_layer is None:
            return None
        return UnityCatalogNamespace(
            catalog=self.unity_catalog, schema=self.medallion_layer.unity_schema, table=self.name
        )

    @property
    def full_table_name(self) -> str | None:
        """
        Returns fully qualified Unity Catalog table name.

                Returns

                str or None
                    Full table name (catalog.schema.table) if medallion_layer is set
        """
        ns = self.unity_namespace
        return ns.full_name if ns else None

    @property
    def data_tier(self) -> str | None:
        """
        Legacy data tier name before we moved to DB/medallion (raw/processed/curated).

                Returns

                str or None
                    Legacy tier name if medallion_layer is set
        """
        return self.medallion_layer.data_tier if self.medallion_layer else None


class Catalog:
    """
    Schema catalog with .

        - Prevoiusly read YAML schemas from src/acoharmony/_schemas/
        - Now leverages _tables module
        - Delegates file reading to parsers
        - Provides metadata for pipeline orchestration
            True

        Get schema metadata:
            'cclf1'
            'bronze'
    """

    def __init__(self, storage_config: StorageBackend | None = None):
        """
        Initialize catalog with storage configuration.

                Parameters

                storage_config : StorageBackend, optional
                    Storage backend configuration. Defaults to local storage.

                Use custom storage profile:
        """
        self.storage_config = storage_config or StorageBackend()
        self._table_metadata: dict[str, TableMetadata] = {}
        self._load_table_metadata()

    def _load_table_metadata(self):
        """
        Load all table metadata from the SchemaRegistry.

                Reads schema definitions registered via _tables Pydantic models
                and creates TableMetadata objects.
        """
        # Ensure _tables models are imported so SchemaRegistry is populated
        from . import _tables as _  # noqa: F401

        for schema_name in SchemaRegistry.list_schemas():
            data = SchemaRegistry.get_full_table_config(schema_name)
            if not data:
                continue

            # Determine medallion layer
            storage = data.get("storage", {})
            medallion_layer = None
            ml_str = storage.get("medallion_layer") or data.get("medallion_layer")
            if ml_str:
                medallion_layer = MedallionLayer.from_tier(ml_str)
            elif data.get("tier"):
                medallion_layer = MedallionLayer.from_tier(data["tier"])
            elif storage.get("tier"):
                medallion_layer = MedallionLayer.from_tier(storage["tier"])

            # Get file_format from parser config
            file_format = data.get("file_format", {})

            # Get sheets/matrix from sheets config
            sheets_cfg = SchemaRegistry.get_sheets_config(schema_name)

            metadata = TableMetadata(
                name=data["name"],
                description=data.get("description", ""),
                columns=data.get("columns", []),
                storage=storage,
                file_format=file_format,
                medallion_layer=medallion_layer,
                unity_catalog=storage.get("unity_catalog", data.get("unity_catalog", "main")),
                polars=data.get("polars", {}),
                record_types=data.get("record_types"),
                sheets=sheets_cfg.get("sheets"),
                matrix_fields=sheets_cfg.get("matrix_fields"),
            )

            self._table_metadata[metadata.name] = metadata

    def get_table_metadata(self, table_name: str) -> TableMetadata | None:
        """
        Get metadata for a table.

                Parameters

                table_name : str
                    Name of the table (e.g., "cclf1", "institutional_claim")

                Returns

                TableMetadata or None
                    Table metadata if found, None otherwise
        """
        return self._table_metadata.get(table_name)

    def get_schema(self, table_name: str) -> TableMetadata | None:
        """
        Get schema for a table.

                Parameters

                table_name : str
                    Name of the table/schema to retrieve

                Returns

                TableMetadata or None
                    Schema object if found, None otherwise
                'cclf1'
                ['cur_clm_uniq_id', 'clm_efctv_dt']
                ['institutional_claim', 'physician_claim', 'dme_claim']
                True
        """
        return self.get_table_metadata(table_name)

    def list_tables(self, medallion_layer: MedallionLayer | None = None) -> list[str]:
        """
        List all available tables, optionally filtered by medallion layer.

                Parameters

                medallion_layer : MedallionLayer, optional
                    Filter by Bronze/Silver/Gold layer

                Returns

                List[str]
                    Names of all available tables
                54
                Bronze tables: ['cclf0', 'cclf1', 'cclf2', 'cclf3', 'cclf4']
                Claim tables: ['institutional_claim', 'physician_claim', 'dme_claim', 'medical_claim']
        """
        if medallion_layer:
            return [
                name
                for name, metadata in self._table_metadata.items()
                if metadata.medallion_layer == medallion_layer
            ]
        return list(self._table_metadata.keys())

    def scan_table(self, table_name: str, file_path: str | None = None) -> pl.LazyFrame:
        """
        Scan a table directly using polars.

                 - no complex transformers needed.

                Parameters

                table_name : str
                    Name of the table to scan
                file_path : str, optional
                    Override file path. If not provided, uses schema configuration.

                Returns

                pl.LazyFrame
                    Lazy frame for efficient processing
                (5, 2)
                Loaded 100 claims
                Top 10 members by spend: (10, 3)
        """

        schema = self.get_table_metadata(table_name)
        if not schema:
            raise ValueError(f"Schema not found for table: {table_name}")

        # Determine file path
        if file_path is None:
            file_path = self._get_table_file_path(schema)

        # Check if we're reading from processed tier (parquet)
        if str(file_path).endswith(".parquet"):
            # Direct scan for processed parquet files
            lf = pl.scan_parquet(file_path)
        else:
            # Use parser module to handle raw file formats
            lf = parsers.parse_file(file_path, schema)

            # Apply schema transformations (renaming, filtering, etc.)
            lf = parsers.apply_schema_transformations(lf, schema)

            # Apply column types based on schema
            lf = parsers.apply_column_types(lf, schema)

        return lf

    def get_file_patterns(self, table_name: str) -> dict[str, str]:
        """
        Get file patterns from schema storage configuration.

                Parameters

                table_name : str
                    Name of the table

                Returns

                Dict[str, str]
                    Mapping of pattern names to glob patterns
                {'default': 'ZC1P.ACO.ZC1Y*.D*.T*'}
                {'default': 'ACOSSP.P.ALR*.D*.T*'}
                alr: ACOSSP.P.ALR*.D*.T*
                bar: ACOSSP.P.BAR*.D*.T*
        """
        schema = self.get_table_metadata(table_name)
        if not schema:
            return {}

        patterns = schema.storage.get("file_patterns", {})

        # Clean patterns - remove metadata entries
        if isinstance(patterns, dict):
            return {
                k: v
                for k, v in patterns.items()
                if not isinstance(v, dict) and k not in ["date_extraction", "metadata_extraction"]
            }
        elif isinstance(patterns, list):
            return {f"pattern_{i}": p for i, p in enumerate(patterns)}
        elif isinstance(patterns, str):
            return {"default": patterns}
        else:
            return {}

    def discover_files(self, table_name: str, tier: str = "bronze") -> dict[str, list[Path]]:
        """
        Discover files for a table using schema-defined patterns.

                Parameters

                table_name : str
                    Name of the table
                tier : str, default 'bronze'
                    Storage tier to search in (bronze/silver/gold)

                Returns

                Dict[str, List[Path]]
                    Mapping of pattern names to discovered file paths
                Pattern 'default': 12 files
                  - ZC1P.ACO.ZC1Y24.D240915.T0000000
                  - ZC1P.ACO.ZC1Y24.D240815.T0000000
                dict_keys(['medical_claim'])
                0
        """
        patterns = self.get_file_patterns(table_name)
        if not patterns:
            return {}

        search_path = self.storage_config.get_path(tier)
        discovered = {}

        for name, pattern in patterns.items():
            files = list(search_path.glob(f"**/{pattern}"))
            if files:
                files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
                discovered[name] = files

        return discovered

    def _get_table_file_path(self, schema: TableMetadata, tier: str = "silver") -> str:
        """Get file path for a table using medallion architecture (default: silver layer)."""
        tier_config = schema.storage.get(tier, {})

        if tier_config and "output_name" in tier_config:
            output_name = tier_config["output_name"]
        else:
            output_name = f"{schema.name}.parquet"

        return str(self.storage_config.get_path(tier) / output_name)

    def get_unity_schema(self, medallion_layer: MedallionLayer) -> str:
        """
        Get Unity Catalog schema name for medallion layer.

                Parameters

                medallion_layer : MedallionLayer
                    Bronze/Silver/Gold layer

                Returns

                str
                    Unity schema name ("bronze", "silver", "gold")
        """
        return medallion_layer.unity_schema

    def get_medallion_layer(self, unity_schema: str) -> MedallionLayer:
        """
        Get medallion layer from Unity Catalog schema name.

                Parameters

                unity_schema : str
                    Unity schema name ("bronze", "silver", "gold")

                Returns

                MedallionLayer
                    MedallionLayer enum
        """
        return MedallionLayer.from_unity_schema(unity_schema)

    def get_data_tier(self, medallion_layer: MedallionLayer) -> str:
        """
        Get legacy data tier name for medallion layer.

                Parameters

                medallion_layer : MedallionLayer
                    Bronze/Silver/Gold layer

                Returns

                str
                    Legacy tier name (raw/processed/curated)
        """
        return medallion_layer.data_tier
