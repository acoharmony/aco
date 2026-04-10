# © 2025 HarmonyCares
# All rights reserved.

"""
Notebook generator using existing ACOHarmony infrastructure.

This module leverages the existing Catalog and StorageBackend to generate
Marimo notebooks from schema definitions.
"""

from pathlib import Path
from typing import Any

import jinja2

from acoharmony import Catalog
from acoharmony._store import StorageBackend

from .config import NotebookConfig


class NotebookGenerator:
    """Generate Marimo notebooks using ACOHarmony's existing infrastructure."""

    def __init__(
        self, storage_backend: StorageBackend | None = None, output_dir: Path | None = None
    ):
        """
        Initialize notebook generator with existing ACOHarmony components.

                Args:
                    storage_backend: Storage backend to use (defaults to StorageBackend())
                    output_dir: Directory for generated notebooks
        """
        # Use existing ACOHarmony infrastructure
        self.storage = storage_backend or StorageBackend()
        self.catalog = Catalog(storage_config=self.storage)

        # Template directory
        self.template_dir = Path(__file__).parent / "templates"

        # Output directory - use workspace if not specified
        if output_dir is None:
            # Use the storage backend to determine output location
            base_path = self.storage.get_data_path()
            if isinstance(base_path, Path):
                output_dir = base_path.parent / "notebooks" / "generated"
            else:
                output_dir = Path("/home/care/acoharmony/notebooks/generated")

        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize Jinja2
        self.jinja_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(str(self.template_dir)),
            trim_blocks=True,
            lstrip_blocks=True,
            keep_trailing_newline=True,
        )

    def get_schema_with_full_details(self, schema_name: str) -> dict[str, Any]:
        """
        Get table metadata from catalog with full column details.

                Args:
                    schema_name: Name of the table

                Returns:
                    Dictionary with table metadata including full column information
        """
        schema = self.catalog.get_table_metadata(schema_name)
        return {
            "name": schema.name,
            "description": schema.description,
            "storage": schema.storage,
            "columns": schema.columns,
            "file_format": schema.file_format,
            "medallion_layer": schema.medallion_layer,
            "unity_catalog": schema.unity_catalog,
        }

    def get_data_path_for_schema(self, schema_name: str) -> str:
        """
        Get the correct data path for a schema using storage backend.

                Args:
                    schema_name: Name of the table

                Returns:
                    Path to the parquet file for this table
        """
        metadata = self.catalog.get_table_metadata(schema_name)

        # Determine tier from medallion layer
        if metadata.medallion_layer:
            tier = (
                metadata.medallion_layer.data_tier
            )  # bronze->raw, silver->processed, gold->curated
        else:
            # Fallback to storage config
            tier = "processed"  # Default
            if hasattr(metadata, "storage") and isinstance(metadata.storage, dict):
                tier = metadata.storage.get("tier", "processed")

        # Use storage backend to get correct path
        data_path = self.storage.get_data_path(tier)
        if isinstance(data_path, Path):
            return str(data_path / f"{schema_name}.parquet")
        else:
            # For cloud storage, construct the path
            return f"{data_path}/{schema_name}.parquet"

    def create_notebook(
        self,
        schema_name: str,
        template_name: str = "dashboard.py.j2",
        output_name: str | None = None,
    ) -> Path:
        """
        Generate a Marimo notebook for a schema.

                Args:
                    schema_name: Name of the schema to generate notebook for
                    template_name: Template to use
                    output_name: Optional custom output file name

                Returns:
                    Path to generated notebook
        """
        # Get schema with full details
        schema_dict = self.get_schema_with_full_details(schema_name)

        # Create schema object for config generation
        class SchemaObj:
            def __init__(self, d):
                for k, v in d.items():
                    setattr(self, k, v)

        schema_obj = SchemaObj(schema_dict)

        # Update data path in schema
        schema_obj.data_path = self.get_data_path_for_schema(schema_name)

        # Create notebook config
        config = NotebookConfig.from_schema(schema_obj)

        # Override data path with correct one
        config.data_path = schema_obj.data_path

        # Get template
        template = self.jinja_env.get_template(template_name)

        # Render template
        notebook_content = template.render(config=config)

        # Determine output file name
        if output_name is None:
            output_name = f"{schema_name}_dashboard.py"
        output_path = self.output_dir / output_name

        # Write notebook
        with open(output_path, "w") as f:
            f.write(notebook_content)

        print(f"Generated notebook: {output_path}")
        return output_path

    def list_raw_schemas(self) -> list[str]:
        """
        List all schemas with raw storage tier.

                Returns:
                    List of table names that are in bronze layer (raw data)
        """
        from ..medallion import MedallionLayer

        # Use catalog to get bronze layer tables
        bronze_tables = self.catalog.list_tables(MedallionLayer.BRONZE)

        return sorted(bronze_tables)

    def create_notebooks_for_raw_schemas(self) -> list[Path]:
        """
        Generate notebooks for all raw schemas.

                Returns:
                    List of generated notebook paths
        """
        generated = []
        raw_schemas = self.list_raw_schemas()

        for schema_name in raw_schemas:
            try:
                path = self.create_notebook(schema_name)
                generated.append(path)
                print(f"[OK] Generated notebook for {schema_name}")
            except Exception as e:  # ALLOWED: Batch notebook generation - print error, continue with remaining schemas
                print(f"[ERROR] Failed to generate notebook for {schema_name}: {e}")

        print(f"\nGenerated {len(generated)} notebooks in {self.output_dir}")
        return generated
