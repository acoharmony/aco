# © 2025 HarmonyCares
# All rights reserved.

"""
Schema-driven configuration using Polars introspection.

This module generates notebook configurations by leveraging Polars'
built-in dtype inspection and statistical methods rather than
hardcoding assumptions about data types.
"""

from dataclasses import dataclass


@dataclass
class NotebookConfig:
    """
    Minimal configuration for notebook generation.

        The actual configuration happens dynamically in the notebook
        by inspecting the data with Polars.
    """

    # Basic schema info
    schema_name: str
    schema_description: str
    storage_tier: str
    data_path: str

    # Marimo app settings
    app_width: str = "medium"
    hide_code: bool = True
    html_head_file: str = "/home/care/acoharmony/notebooks/harmonycares-head.html"

    # Display settings
    show_footer: bool = True
    show_tracking: bool = True
    logo_url: str = "https://harmonycaresaco.com/img/logo.svg"

    # Optional hints for display (but not required)
    primary_key: str | None = None
    default_sort_column: str | None = None
    max_display_rows: int = 100

    @classmethod
    def from_schema(cls, schema, storage_config=None) -> "NotebookConfig":
        """
        Create minimal config from schema.

                Args:
                    schema: Schema object with metadata
                    storage_config: Optional storage configuration (uses default if not provided)

                The notebook will use Polars to discover:
                - Column dtypes
                - Numeric vs categorical columns
                - Date columns
                - Null counts
                - Cardinality for categoricals
                - Min/max for numerics
        """
        # Get storage configuration
        if storage_config is None:
            from .._store import StorageBackend

            storage_config = StorageBackend()

        # Determine data path based on storage tier using storage backend
        storage_tier = schema.storage.get("tier") if hasattr(schema, "storage") else "silver"
        if storage_tier == "bronze":
            # Bronze schemas output to silver after processing
            tier_path = storage_config.get_path("silver")
        else:
            tier_path = storage_config.get_path(storage_tier)

        # Build the data path
        from pathlib import Path

        if isinstance(tier_path, Path):
            data_path = str(tier_path / f"{schema.name}.parquet")
        else:
            data_path = f"{tier_path}/{schema.name}.parquet"

        # Look for a good default sort column
        default_sort = None
        if hasattr(schema, "columns"):
            # Common patterns for sorting
            for pattern in ["total_spend", "date", "created", "updated", "amount"]:
                for col in schema.columns:
                    col_name = (
                        col.get("output_name", col.get("name", ""))
                        if isinstance(col, dict)
                        else str(col)
                    )
                    if pattern in col_name.lower():
                        default_sort = col_name
                        break
                if default_sort:
                    break

        return cls(
            schema_name=schema.name,
            schema_description=schema.description,
            storage_tier=storage_tier,
            data_path=data_path,
            default_sort_column=default_sort,
        )
