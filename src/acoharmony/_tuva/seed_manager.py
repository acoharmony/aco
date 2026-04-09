# © 2025 HarmonyCares
# All rights reserved.

"""
Tuva seed management - download and convert seeds to silver parquet files.

Instead of using dbt seed (which crashes on large CSVs), we:
1. Parse dbt_project.yml to extract seed definitions
2. Download CSV files directly from S3 (tuva-public-resources bucket)
3. Stream convert to parquet in silver using Polars (flat structure)

This gives us:
- No memory issues from dbt seed
- Streaming conversion with Polars
- Flat structure in silver: schema_table.parquet
- Same data as dbt seed but faster and more reliable
"""

from pathlib import Path

import duckdb
import polars as pl
import yaml

from .._log import LogWriter
from .._store import StorageBackend
from ..medallion import MedallionLayer

logger = LogWriter("tuva.seed_manager")


class TuvaSeedManager:
    """
    Manages Tuva seed data - download and convert to silver parquet files.

    Workflow:
    1. Parse dbt_project.yml to extract seed definitions (S3 paths)
    2. Download CSVs directly from S3 using Polars streaming
    3. Convert to parquet in silver with flat naming: schema_table.parquet
    """

    def __init__(
        self,
        tuva_project_dir: Path | None = None,
        duckdb_path: Path | None = None,
        storage: StorageBackend | None = None,
    ):
        """
        Initialize seed manager.

        Args:
            tuva_project_dir: Path to Tuva dbt project (default: _depends/repos/tuva)
            duckdb_path: Path to DuckDB database (default: workspace/acoharmony.duckdb)
            storage: Storage backend for bronze layer
        """
        self.storage = storage or StorageBackend()

        # Default to tuva project in _depends
        if tuva_project_dir is None:
            package_root = Path(__file__).parent.parent.parent.parent
            tuva_project_dir = (
                package_root / "src" / "acoharmony" / "_tuva" / "_depends" / "repos" / "tuva"
            )
        self.tuva_project_dir = Path(tuva_project_dir)

        # Default to acoharmony.duckdb in workspace
        if duckdb_path is None:
            duckdb_path = Path("/opt/s3/data/workspace/acoharmony.duckdb")
        self.duckdb_path = Path(duckdb_path)

        # Bronze and silver layer paths
        self.bronze_path = self.storage.get_path(MedallionLayer.BRONZE)
        self.silver_path = self.storage.get_path(MedallionLayer.SILVER)

        # Bronze subdirectory for Tuva seed CSVs
        self.bronze_seeds_path = self.bronze_path / "tuva_seeds"
        self.bronze_seeds_path.mkdir(parents=True, exist_ok=True)

        self.log = LogWriter("tuva.seed_manager")
        self.log.info(f"Tuva project: {self.tuva_project_dir}")
        self.log.info(f"DuckDB: {self.duckdb_path}")
        self.log.info(f"Bronze seeds: {self.bronze_seeds_path}")
        self.log.info(f"Silver layer: {self.silver_path}")

        # Load seed schemas to get proper column names
        self.seed_schemas = self._load_seed_schemas()

    def _load_seed_schemas(self) -> dict[str, list[str]]:
        """
        Load seed schema YAML files to extract proper column names.

        Returns:
            Dictionary mapping seed dbt_name (e.g., "cms_hcc__icd_10_cm_mappings")
            to list of column names
        """
        schemas = {}
        seeds_dir = self.tuva_project_dir / "seeds"

        # Find all .yml files recursively (includes both *_seeds.yml and direct table schemas)
        schema_files = list(seeds_dir.rglob("*.yml"))
        self.log.info(f"Loading seed schemas from {len(schema_files)} YAML files...")

        for schema_file in schema_files:
            try:
                with open(schema_file) as f:
                    schema_yaml = yaml.safe_load(f)

                # Extract seeds from YAML
                seed_list = schema_yaml.get("seeds", [])
                for seed in seed_list:
                    seed_name = seed.get("name")
                    columns = seed.get("columns", [])

                    if seed_name and columns:
                        # Extract column names
                        column_names = [col.get("name") for col in columns if col.get("name")]
                        schemas[seed_name] = column_names
                        self.log.debug(
                            f"  Loaded schema for {seed_name}: {len(column_names)} columns"
                        )

            except (
                Exception
            ) as e:  # ALLOWED: Batch loading - continue with remaining files, return what succeeded
                self.log.warning(f"Failed to parse schema file {schema_file}: {e}", exc_info=True)
                # Continue with remaining schema files

        self.log.info(f"Loaded schemas for {len(schemas)} seeds")
        return schemas

    def parse_seed_definitions(self) -> list[dict]:
        """
        Parse dbt_project.yml to extract seed definitions.

        Returns:
            List of seed definitions with schema, table, s3_path, csv_filename
        """
        dbt_project_yml = self.tuva_project_dir / "dbt_project.yml"

        with open(dbt_project_yml) as f:
            config = yaml.safe_load(f)

        # Extract custom_bucket_name (defaults to tuva-public-resources)
        bucket = config.get("vars", {}).get("custom_bucket_name", "tuva-public-resources")

        seeds = []
        seed_config = config.get("seeds", {}).get("the_tuva_project", {})

        def extract_seeds_recursive(parent_schema, config_dict):
            """Recursively extract seed definitions from nested structure."""
            for key, value in config_dict.items():
                if not isinstance(value, dict):
                    continue

                # Check if this has a post-hook (it's a seed definition)
                post_hook = value.get("+post-hook")
                if post_hook and "load_seed" in post_hook:
                    # Extract path and filename from post-hook
                    parts = post_hook.split("'")
                    s3_path = None
                    csv_filename = None
                    for part in parts:
                        if part.startswith("/"):
                            s3_path = part
                        elif part.endswith(".csv"):
                            csv_filename = part

                    if s3_path and csv_filename:
                        # Determine schema from parent_schema (already contains full path)
                        # For nested: value_sets/ccsr/ccsr__dxccsr_v2023_1_body_systems
                        # parent_schema = "value_sets_ccsr"
                        # key = "ccsr__dxccsr_v2023_1_body_systems"
                        # Schema should be: value_sets_ccsr
                        schema = parent_schema if parent_schema else key.split("__")[0]

                        # Table name: remove all prefixes
                        # ccsr__dxccsr_v2023_1_body_systems → dxccsr_v2023_1_body_systems
                        if "__" in key:
                            table = key.split("__", 1)[1]
                        else:
                            table = key

                        seeds.append(
                            {
                                "schema": schema,
                                "table": table,
                                "dbt_name": key,
                                "s3_bucket": bucket,
                                "s3_path": s3_path.lstrip("/"),
                                "csv_filename": csv_filename,
                                "full_s3_uri": f"s3://{bucket}/{s3_path.lstrip('/')}/{csv_filename}",
                            }
                        )
                else:
                    # Recurse into nested structure
                    next_parent = f"{parent_schema}_{key}" if parent_schema else key
                    extract_seeds_recursive(next_parent, value)

        # Start extraction from top level
        for schema_name, schema_config in seed_config.items():
            if isinstance(schema_config, dict):
                extract_seeds_recursive(schema_name, schema_config)

        self.log.info(f"Parsed {len(seeds)} seed definitions from dbt_project.yml")
        return seeds

    def download_seed_csv(
        self,
        seed_def: dict,
        overwrite: bool = False,
    ) -> Path | None:
        """
        Download seed CSV from S3 to bronze.

        Args:
            seed_def: Seed definition dict with s3_bucket, s3_path, csv_filename, schema, table
            overwrite: Overwrite existing CSV file

        Returns:
            Path to downloaded CSV file, or None if skipped
        """
        schema_name = seed_def["schema"]
        table_name = seed_def["table"]
        csv_filename = seed_def["csv_filename"]

        # Save to bronze with flat naming: schema_table.csv
        flat_name = f"{schema_name}_{table_name}.csv"
        output_file = self.bronze_seeds_path / flat_name

        if output_file.exists() and not overwrite:
            self.log.info(f"Skipping {schema_name}.{table_name} (already exists)")
            return None

        # Use HTTPS URL for public S3 bucket
        # Files are compressed with pattern: {name}.csv_0_0_0.csv.gz
        s3_bucket = seed_def["s3_bucket"]
        s3_path = seed_def["s3_path"]
        csv_filename = seed_def["csv_filename"]
        # Add _0_0_0.csv.gz suffix
        compressed_filename = f"{csv_filename}_0_0_0.csv.gz"
        https_url = f"https://{s3_bucket}.s3.amazonaws.com/{s3_path}/{compressed_filename}"

        self.log.info(f"Downloading {schema_name}.{table_name} from {https_url}")

        try:
            # Get expected column names from schema if available
            dbt_name = seed_def.get("dbt_name")
            expected_columns = self.seed_schemas.get(dbt_name) if dbt_name else None

            # Download compressed CSV from S3 using HTTPS (public bucket, no auth needed)
            # Tuva seed CSVs do NOT have headers - we need to assign them from schema
            if expected_columns:
                # Read without headers and assign column names from schema
                df = pl.read_csv(
                    https_url,
                    has_header=False,
                    new_columns=expected_columns,
                    infer_schema_length=10000,
                    null_values=["\\N", ""],  # Tuva uses \N for nulls
                )
                self.log.debug(f"  [OK] Assigned columns from schema: {expected_columns}")
            else:
                # No schema available, read with headers (will use first row or generate names)
                self.log.warning(
                    f"  No schema found for {dbt_name}, reading with auto-generated columns"
                )
                df = pl.read_csv(
                    https_url,
                    has_header=False,
                    infer_schema_length=10000,
                    null_values=["\\N", ""],
                )

            # Write CSV to bronze WITH headers
            df.write_csv(output_file, include_header=True)

            row_count = len(df)
            self.log.info(f"  [OK] Downloaded {row_count:,} rows → {output_file}")

            return output_file

        except Exception as e:  # ALLOWED: Returns None to indicate error
            self.log.error(f"  [ERROR] Failed to download {schema_name}.{table_name}: {e}")
            return None

    def convert_csv_to_parquet(
        self,
        csv_path: Path,
        overwrite: bool = False,
    ) -> Path | None:
        """
        Convert bronze CSV to silver parquet.

        Args:
            csv_path: Path to CSV file in bronze
            overwrite: Overwrite existing parquet file

        Returns:
            Path to created parquet file, or None if skipped
        """
        # Extract schema_table from filename
        flat_name = csv_path.stem  # e.g., "terminology_icd_10_cm"
        output_file = self.silver_path / f"{flat_name}.parquet"

        if output_file.exists() and not overwrite:
            self.log.info(f"Skipping {flat_name} (already exists)")
            return None

        self.log.info(f"Converting {flat_name}: CSV → Parquet")

        try:
            # Streaming read CSV and write parquet
            # Bronze CSVs now have headers (added during download)
            df = pl.read_csv(
                csv_path,
                has_header=True,
                infer_schema_length=10000,
                null_values=["\\N", ""],
            )

            df.write_parquet(output_file, compression="zstd")

            row_count = len(df)
            self.log.info(f"  [OK] Converted {row_count:,} rows → {output_file}")

            return output_file

        except Exception as e:  # ALLOWED: Returns None to indicate error
            self.log.error(f"  [ERROR] Failed to convert {flat_name}: {e}")
            return None

    def download_all_seeds(self, overwrite: bool = False) -> dict[str, Path]:
        """
        Download all Tuva seed CSVs from S3 to bronze.

        Args:
            overwrite: Overwrite existing CSV files

        Returns:
            Dictionary mapping "schema_table" to CSV path in bronze
        """
        self.log.info("=" * 60)
        self.log.info("Downloading Tuva seeds: S3 → Bronze CSV")
        self.log.info("=" * 60)

        seeds = self.parse_seed_definitions()
        self.log.info(f"Found {len(seeds)} seeds to download")

        downloaded = {}
        skipped = 0

        for i, seed_def in enumerate(seeds, 1):
            schema_name = seed_def["schema"]
            table_name = seed_def["table"]
            flat_name = f"{schema_name}_{table_name}"

            self.log.info(f"\n[{i}/{len(seeds)}] {flat_name}")

            output_path = self.download_seed_csv(seed_def, overwrite=overwrite)

            if output_path:
                downloaded[flat_name] = output_path
            else:
                skipped += 1

        self.log.info("\n" + "=" * 60)
        self.log.info(f"Download complete: {len(downloaded)} downloaded, {skipped} skipped")
        self.log.info("=" * 60)

        return downloaded

    def convert_all_seeds(self, overwrite: bool = False) -> dict[str, Path]:
        """
        Convert all bronze CSVs to silver parquet files.

        Args:
            overwrite: Overwrite existing parquet files

        Returns:
            Dictionary mapping "schema_table" to parquet path in silver
        """
        self.log.info("=" * 60)
        self.log.info("Converting seeds: Bronze CSV → Silver Parquet")
        self.log.info("=" * 60)

        # Get all CSV files in bronze/tuva_seeds
        csv_files = list(self.bronze_seeds_path.glob("*.csv"))
        self.log.info(f"Found {len(csv_files)} CSV files in bronze")

        converted = {}
        skipped = 0

        for i, csv_path in enumerate(csv_files, 1):
            flat_name = csv_path.stem

            self.log.info(f"\n[{i}/{len(csv_files)}] {flat_name}")

            output_path = self.convert_csv_to_parquet(csv_path, overwrite=overwrite)

            if output_path:
                converted[flat_name] = output_path
            else:
                skipped += 1

        self.log.info("\n" + "=" * 60)
        self.log.info(f"Conversion complete: {len(converted)} converted, {skipped} skipped")
        self.log.info("=" * 60)

        return converted

    def sync_all_seeds(self, overwrite: bool = False) -> dict[str, Path]:
        """
        Download and convert all Tuva seeds: S3 → Bronze CSV → Silver Parquet.

        Args:
            overwrite: Overwrite existing files

        Returns:
            Dictionary mapping "schema_table" to parquet path in silver
        """
        # Step 1: Download CSVs to bronze
        self.download_all_seeds(overwrite=overwrite)

        # Step 2: Convert bronze CSVs to silver parquet
        return self.convert_all_seeds(overwrite=overwrite)

    def run_dbt_deps(self) -> int:
        """
        Run dbt deps to install package dependencies.

        Returns:
            Return code from dbt deps command
        """
        import subprocess

        logger.info("Running dbt deps to install dependencies...")

        # Use profiles from cclf_connector/profiles directory
        profiles_dir = self.tuva_project_dir / "cclf_connector" / "profiles"

        cmd = ["dbt", "deps", "--profiles-dir", str(profiles_dir)]

        # Run from tuva project directory
        result = subprocess.run(
            cmd,
            cwd=self.tuva_project_dir,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            logger.error(f"dbt deps failed: {result.stderr}")
            logger.error(f"dbt deps stdout: {result.stdout}")
            raise RuntimeError(f"dbt deps failed with code {result.returncode}\n{result.stderr}")

        logger.info("dbt deps completed successfully")
        return result.returncode

    def run_dbt_seed(self, select: str | None = None) -> int:
        """
        Run dbt seed to populate DuckDB with seed data.

        Args:
            select: Optional dbt select expression (e.g., "terminology.*")

        Returns:
            Return code from dbt seed command
        """
        import subprocess

        logger.info("Running dbt seed to populate DuckDB...")

        # Use profiles from cclf_connector/profiles directory
        profiles_dir = self.tuva_project_dir / "cclf_connector" / "profiles"

        cmd = ["dbt", "seed", "--profiles-dir", str(profiles_dir)]
        if select:
            cmd.extend(["--select", select])

        # Run from tuva project directory
        result = subprocess.run(
            cmd,
            cwd=self.tuva_project_dir,
            capture_output=True,
            text=True,
        )

        if result.returncode != 0:
            logger.error(f"dbt seed failed: {result.stderr}")
            logger.error(f"dbt seed stdout: {result.stdout}")
            raise RuntimeError(f"dbt seed failed with code {result.returncode}\n{result.stderr}")

        logger.info("dbt seed completed successfully")
        return result.returncode

    def get_seed_tables(self, schema_pattern: str = "%") -> list[tuple[str, str]]:
        """
        Get list of seed tables from DuckDB.

        Args:
            schema_pattern: Schema pattern to filter (default: all schemas)

        Returns:
            List of (schema_name, table_name) tuples
        """
        con = duckdb.connect(str(self.duckdb_path), read_only=True)

        query = """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_schema LIKE ?
          AND table_schema NOT IN ('information_schema', 'pg_catalog')
        ORDER BY table_schema, table_name
        """

        result = con.execute(query, [schema_pattern]).fetchall()
        con.close()

        return result

    def export_seed_to_parquet(
        self, schema_name: str, table_name: str, overwrite: bool = False
    ) -> Path:
        """
        Export a single seed table to parquet in silver (flat structure).

        Args:
            schema_name: DuckDB schema name (e.g., "terminology", "value_sets")
            table_name: Table name (e.g., "icd_10_cm")
            overwrite: Overwrite existing parquet file

        Returns:
            Path to created parquet file
        """
        # Flat naming: schema_table.parquet (single underscore)
        flat_name = f"{schema_name}_{table_name}.parquet"
        output_file = self.silver_path / flat_name

        if output_file.exists() and not overwrite:
            logger.info(f"Skipping {schema_name}.{table_name} (already exists)")
            return output_file

        logger.info(f"Exporting {schema_name}.{table_name} → {output_file}")

        # Read from DuckDB and write to parquet using Polars
        con = duckdb.connect(str(self.duckdb_path), read_only=True)

        # Use Polars for efficient parquet writing
        df = pl.read_database(
            f'SELECT * FROM "{schema_name}"."{table_name}"',
            connection=con,
        )

        con.close()

        # Write to parquet
        df.write_parquet(output_file, compression="zstd")

        row_count = len(df)
        logger.info(f"  Exported {row_count:,} rows to {output_file}")

        return output_file

    def export_all_seeds(
        self,
        schema_filter: list[str] | None = None,
        overwrite: bool = False,
    ) -> dict[str, Path]:
        """
        Export all seed tables to silver parquet files (flat structure).

        Args:
            schema_filter: Optional list of schemas to export (e.g., ["terminology", "value_sets"])
            overwrite: Overwrite existing parquet files

        Returns:
            Dictionary mapping "schema_table" to parquet path
        """
        logger.info("=" * 60)
        logger.info("Exporting Tuva seeds to silver parquet files (flat structure)")
        logger.info("=" * 60)

        # Get all seed tables
        if schema_filter:
            all_tables = []
            for schema in schema_filter:
                all_tables.extend(self.get_seed_tables(schema))
        else:
            all_tables = self.get_seed_tables()

        logger.info(f"Found {len(all_tables)} seed tables to export")

        exported = {}
        for schema_name, table_name in all_tables:
            try:
                output_path = self.export_seed_to_parquet(
                    schema_name, table_name, overwrite=overwrite
                )
                # Use flat naming for key
                flat_name = f"{schema_name}_{table_name}"
                exported[flat_name] = output_path
            except (
                Exception
            ) as e:  # ALLOWED: Batch export - continue with remaining seeds, return what succeeded
                logger.error(f"Failed to export {schema_name}.{table_name}: {e}", exc_info=True)

        logger.info("=" * 60)
        logger.info(f"Export complete: {len(exported)} seeds exported to silver")
        logger.info("=" * 60)

        return exported

    def generate_dbt_sources_yml(self, output_path: Path | None = None) -> str:
        """
        Generate dbt sources.yml that points to silver parquet files (flat structure).

        This allows dbt models to reference seeds via {{ source('tuva_seeds', 'terminology_icd_10_cm') }}
        instead of {{ ref('terminology_icd_10_cm') }}.

        Args:
            output_path: Optional path to write sources.yml (default: tuva/models/sources.yml)

        Returns:
            Generated YAML content
        """
        # Get all parquet files in silver that match seed naming pattern
        tables = []

        for parquet_file in self.silver_path.glob("*.parquet"):
            # Check if it's a seed file (has schema prefix like terminology_, value_sets_, etc.)
            if "_" in parquet_file.stem:
                tables.append(
                    {
                        "name": parquet_file.stem,
                        "meta": {
                            "parquet_path": str(parquet_file),
                        },
                    }
                )

        # Generate YAML
        lines = [
            "version: 2",
            "",
            "sources:",
            "  - name: tuva_seeds",
            "    description: Tuva seed data (terminology, value sets, reference data) stored as parquet in silver (flat structure)",
            "    meta:",
            f"      silver_path: {self.silver_path}",
            "    tables:",
        ]

        for table in sorted(tables, key=lambda t: t["name"]):
            lines.append(f"      - name: {table['name']}")
            lines.append("        meta:")
            lines.append(f"          parquet_path: {table['meta']['parquet_path']}")

        yaml_content = "\n".join(lines)

        # Write to file if path provided
        if output_path:
            output_path = Path(output_path)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(yaml_content)
            logger.info(f"Generated dbt sources.yml: {output_path}")

        return yaml_content
