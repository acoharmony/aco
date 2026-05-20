# © 2025 HarmonyCares
# All rights reserved.

"""
Reference data transformation for Tuva seeds and lookup tables.

This module transforms reference data (seeds) from Tuva into silver layer parquet
files. Instead of using dbt seed (which can crash on large CSVs), this module:

1. Parses dbt_project.yml to extract seed definitions
2. Downloads CSV files directly from S3 (tuva-public-resources bucket)
3. Streams convert to parquet in silver using Polars

Reference Data Categories:
- terminology: ICD-10-CM, ICD-9-CM, HCPCS, CPT, NDC, etc.
- value_sets: CCSR, clinical groupings, quality measures
- cms_hcc: CMS-HCC risk adjustment mappings
- data_quality: Data quality checks and validation rules
- ed_classification: Emergency department classification
- reference: General reference and lookup tables

What are Reference Transforms?
==============================

Reference transforms download and convert static lookup tables (seeds) that are
required for clinical analytics. These tables rarely change and include:

- Medical code terminologies (ICD-10, CPT, HCPCS, NDC)
- Value sets for quality measures
- CMS-HCC risk adjustment mappings
- Clinical classification systems (CCSR)
- Reference tables for data quality

Key Features
============

1. **Direct S3 Download** - Downloads from public Tuva bucket (no auth needed)
2. **Streaming Conversion** - Uses Polars for memory-efficient processing
3. **Schema-Driven** - Extracts column names from seed YAML files
4. **Flat Naming** - Stores as schema_table.parquet in silver
5. **Idempotent** - Safe to re-run without duplicating data
6. **No DuckDB Required** - Bypasses dbt seed entirely

Performance Considerations
==========================

1. **Memory Efficiency** - Streams large CSVs without loading into memory
2. **Compression** - Uses zstd compression for parquet files
3. **Parallel Processing** - Can download/convert multiple seeds concurrently
4. **Incremental Updates** - Only downloads/converts missing or changed files

"""

import re
from pathlib import Path
from typing import Any

import polars as pl
import yaml

from .._decor8 import explain, timeit, traced
from .._log import LogWriter
from ..medallion import MedallionLayer
from ..result import ResultStatus, TransformResult


class ReferenceStage:
    """Declarative reference data stage with syntactic sugar."""

    def __init__(
        self,
        name: str,
        schema: str,
        table: str,
        s3_uri: str,
        group: str,
        order: int,
        columns: list[str] | None = None,
        description: str = "",
        optional: bool = True,  # Most seeds are optional
    ):
        """
        Define a reference data stage.

                Args:
                    name: Unique identifier (dbt_name from schema)
                    schema: Schema name (terminology, value_sets, etc.)
                    table: Table name
                    s3_uri: Full S3 URI for download
                    group: Logical grouping (terminology, clinical, risk_adjustment, etc.)
                    order: Execution order within pipeline
                    columns: Expected column names from schema YAML
                    description: Human-readable description
                    optional: If True, skip if download fails
        """
        self.name = name
        self.schema = schema
        self.table = table
        self.s3_uri = s3_uri
        self.group = group
        self.order = order
        self.columns = columns or []
        self.description = description
        self.optional = optional
        self.skip_reason: str | None = None

    @property
    def flat_name(self) -> str:
        """Get flat naming: schema_table."""
        return f"{self.schema}_{self.table}"

    def __repr__(self) -> str:
        opt = " [OPTIONAL]" if self.optional else ""
        cols = f" ({len(self.columns)} cols)" if self.columns else ""
        return f"ReferenceStage({self.order}: {self.flat_name} [{self.group}]{cols}{opt})"


_DATABASE_FOLDERS = {
    "concept_library": "concept-library",
    "reference_data": "reference-data",
    "terminology": "terminology",
    "value_sets": "value-sets",
    "provider_data": "provider-data",
    "synthetic_data": "synthetic-data",
}


_LOAD_VERSIONED_SEED_RE = re.compile(
    r"load_versioned_seed\s*\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+\.csv)['\"]"
)


def _parse_load_versioned_seed(post_hook: str) -> tuple[str, str] | None:
    """Extract (database, filename) from a load_versioned_seed macro call.

    Tuva post-hook format:
        "{{ load_versioned_seed('reference_data','calendar.csv') }}"
    Returns None if the post-hook isn't a load_versioned_seed call.
    """
    m = _LOAD_VERSIONED_SEED_RE.search(post_hook)
    if not m:
        return None
    return m.group(1), m.group(2)


def parse_tuva_seed_definitions(tuva_project_dir: Path, logger: Any) -> list[ReferenceStage]:
    """Parse dbt_project.yml into ReferenceStage objects.

    Resolves the versioned-seed S3 URI scheme used by current Tuva:
        s3://{bucket}/{folder}/{version}/{filename}.gz
    where folder is a database-specific override (concept_library →
    concept-library) and version comes from per-database overrides in
    vars.tuva_seed_versions, falling back to vars.tuva_seed_version.
    """
    dbt_project_yml = tuva_project_dir / "dbt_project.yml"

    with open(dbt_project_yml) as f:
        config = yaml.safe_load(f)

    project_vars = config.get("vars", {})
    bucket = project_vars.get("custom_bucket_name", "tuva-public-resources")
    default_version = str(project_vars.get("tuva_seed_version", "1.0.0"))
    version_overrides = project_vars.get("tuva_seed_versions", {}) or {}

    def version_for(database: str) -> str:
        return str(version_overrides.get(database, default_version))

    stages: list[ReferenceStage] = []
    seed_config = config.get("seeds", {}).get("the_tuva_project", {})
    seed_schemas = _load_seed_schemas(tuva_project_dir, logger)

    def extract_seeds_recursive(parent_schema, config_dict, order_counter):
        for key, value in config_dict.items():
            if not isinstance(value, dict):
                continue

            post_hook = value.get("+post-hook")
            parsed = _parse_load_versioned_seed(post_hook) if post_hook else None
            if parsed is None:
                next_parent = f"{parent_schema}_{key}" if parent_schema else key
                extract_seeds_recursive(next_parent, value, order_counter)
                continue

            database, csv_filename = parsed
            folder = _DATABASE_FOLDERS.get(database)
            if folder is None:
                logger.warning(
                    f"Skipping seed {key}: unknown database '{database}' "
                    f"(not in {sorted(_DATABASE_FOLDERS)})"
                )
                continue

            schema = parent_schema if parent_schema else key.split("__")[0]
            table = key.split("__", 1)[1] if "__" in key else key
            group = _categorize_schema(schema)
            version = version_for(database)
            s3_uri = (
                f"https://{bucket}.s3.amazonaws.com/{folder}/{version}/{csv_filename}.gz"
            )

            stages.append(
                ReferenceStage(
                    name=key,
                    schema=schema,
                    table=table,
                    s3_uri=s3_uri,
                    group=group,
                    order=order_counter[0],
                    columns=seed_schemas.get(key, []),
                    description=f"{schema}.{table} reference data",
                )
            )
            order_counter[0] += 1

    order_counter = [1]
    for schema_name, schema_config in seed_config.items():
        if isinstance(schema_config, dict):
            extract_seeds_recursive(schema_name, schema_config, order_counter)

    logger.info(f"Parsed {len(stages)} reference data definitions from dbt_project.yml")
    return stages


def _load_seed_schemas(tuva_project_dir: Path, logger: Any) -> dict[str, list[str]]:
    """
    Load seed schema YAML files to extract proper column names.

        Args:
            tuva_project_dir: Path to Tuva dbt project
            logger: Logger instance

        Returns:
            Dictionary mapping seed dbt_name to list of column names
    """
    schemas = {}
    seeds_dir = tuva_project_dir / "seeds"

    if not seeds_dir.exists():
        logger.warning(f"Seeds directory not found: {seeds_dir}")
        return schemas

    # Find all *_seeds.yml files recursively
    schema_files = list(seeds_dir.rglob("*_seeds.yml"))
    logger.info(f"Loading seed schemas from {len(schema_files)} YAML files...")

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
                    logger.debug(f"  Loaded schema for {seed_name}: {len(column_names)} columns")

        except Exception as e:  # ALLOWED: Batch loading - continue with remaining files
            logger.warning(f"Failed to parse schema file {schema_file}: {e}")

    logger.info(f"Loaded schemas for {len(schemas)} seeds")
    return schemas


def _categorize_schema(schema_name: str) -> str:
    """Categorize schema into logical group."""
    if schema_name.startswith("terminology"):
        return "terminology"
    elif schema_name.startswith("value_sets"):
        return "value_sets"
    elif schema_name.startswith("cms_hcc"):
        return "risk_adjustment"
    elif schema_name.startswith("data_quality"):
        return "data_quality"
    elif schema_name.startswith("ed_classification"):
        return "clinical"
    elif schema_name.startswith("ahrq"):
        return "quality_measures"
    else:
        return "reference"


@traced()
@explain(
    why="Failed to download Tuva reference data CSV from S3 public bucket",
    how="Verify internet connectivity and that the S3 URI from Tuva dbt_project.yml is accessible. Check that bronze/tuva_seeds directory exists and is writable",
    causes=[
        "Network timeout or firewall blocking tuva-public-resources.s3.amazonaws.com",
        "S3 file not found at the expected path (Tuva project may have moved/renamed files)",
        "Bronze directory not writable or disk full",
        "S3 URL malformed in dbt_project.yml post-hook",
    ],
)
@timeit(log_level="debug", threshold=10.0)
def download_reference_stage(
    stage: ReferenceStage,
    bronze_path: Path,
    logger: Any,
    overwrite: bool = False,
) -> Path | None:
    """
    Download a single reference data CSV from S3 to bronze.

        Args:
            stage: ReferenceStage definition
            bronze_path: Path to bronze layer directory
            logger: Logger instance
            overwrite: Overwrite existing CSV file

        Returns:
            Path to downloaded CSV file, or None if skipped/failed
    """
    # Create bronze/tuva_seeds directory
    bronze_seeds_path = bronze_path / "tuva_seeds"
    bronze_seeds_path.mkdir(parents=True, exist_ok=True)

    # Save with flat naming
    output_file = bronze_seeds_path / f"{stage.flat_name}.csv"

    if output_file.exists() and not overwrite:
        logger.info(f"  ⊙ {stage.flat_name} already exists in bronze")
        return output_file

    logger.info(f"  ↓ Downloading {stage.flat_name} from S3...")

    try:
        # Versioned Tuva seeds ship gzipped with headers; Polars auto-detects
        # gzip from the .gz suffix and uses the embedded column names. We
        # disable schema inference (everything → String) because several
        # Tuva seeds mix types within a column in ways that the first 10K
        # rows don't reveal (e.g. ICD-9 'E0000' codes after numeric ones,
        # E_DISABL='SS799' rows in svi_us). Silver consumers cast as needed.
        df = pl.read_csv(
            stage.s3_uri,
            has_header=True,
            infer_schema=False,
            null_values=["\\N", ""],
        )
        if stage.columns and set(stage.columns) != set(df.columns):
            logger.debug(
                f"    schema YAML lists {len(stage.columns)} cols, "
                f"file has {len(df.columns)}; using file headers"
            )

        # Write CSV to bronze WITH headers
        df.write_csv(output_file, include_header=True)

        row_count = len(df)
        logger.info(f"    [OK] {row_count:,} rows → {output_file.name}")

        return output_file

    except Exception as e:  # ALLOWED: Returns None for optional stages
        if stage.optional:
            logger.warning(f"    ⚠ Optional stage skipped: {e}")
            stage.skip_reason = str(e)
            return None
        else:
            logger.error(f"    [ERROR] Required stage failed: {e}")
            raise


@traced()
@explain(
    why="Failed to convert Tuva reference data CSV to parquet format",
    how="Verify the CSV file exists in bronze/tuva_seeds directory, has valid format (Tuva seeds use \\N for nulls, no headers), and silver directory is writable",
    causes=[
        "CSV file malformed, corrupted, or missing from bronze/tuva_seeds",
        "CSV has unexpected format (wrong delimiter, encoding, or structure)",
        "Silver directory permission denied or not accessible",
        "Disk full or insufficient space for parquet conversion",
        "Column name mismatch between CSV data and schema YAML",
    ],
)
@timeit(log_level="debug", threshold=5.0)
def convert_reference_stage(
    stage: ReferenceStage,
    bronze_path: Path,
    silver_path: Path,
    logger: Any,
    overwrite: bool = False,
) -> Path | None:
    """
    Convert bronze CSV to silver parquet for a single reference stage.

        Args:
            stage: ReferenceStage definition
            bronze_path: Path to bronze layer directory
            silver_path: Path to silver layer directory
            logger: Logger instance
            overwrite: Overwrite existing parquet file

        Returns:
            Path to created parquet file, or None if skipped/failed
    """
    # Input CSV in bronze/tuva_seeds
    csv_path = bronze_path / "tuva_seeds" / f"{stage.flat_name}.csv"

    if not csv_path.exists():
        if stage.optional:
            logger.debug(f"  ⊙ {stage.flat_name} not found in bronze (optional)")
            return None
        else:
            raise FileNotFoundError(f"Required CSV not found: {csv_path}")

    # Output parquet in silver
    output_file = silver_path / f"{stage.flat_name}.parquet"

    if output_file.exists() and not overwrite:
        logger.info(f"  ⊙ {stage.flat_name} already exists in silver")
        return output_file

    logger.info(f"  → Converting {stage.flat_name} to parquet...")

    try:
        # Read CSV from bronze with schema inference disabled — see
        # download_reference_stage for the rationale (Tuva seeds mix types
        # within columns in ways the first 10K rows don't reveal).
        df = pl.read_csv(
            csv_path,
            has_header=True,
            infer_schema=False,
            null_values=["\\N", ""],
        )

        # Write parquet to silver
        df.write_parquet(output_file, compression="zstd")

        row_count = len(df)
        logger.info(f"    [OK] {row_count:,} rows → {output_file.name}")

        return output_file

    except Exception as e:  # ALLOWED: Returns None for optional stages
        if stage.optional:
            logger.warning(f"    ⚠ Optional stage skipped: {e}")
            stage.skip_reason = str(e)
            return None
        else:
            logger.error(f"    [ERROR] Required stage failed: {e}")
            raise


def transform_all_reference_data(
    executor: Any,
    logger: Any,
    overwrite: bool = False,
    download_only: bool = False,
    convert_only: bool = False,
) -> dict[str, TransformResult]:
    """
    Transform ALL Tuva reference data (seeds) into silver layer.

        This is the main entry point for reference data transformation. It:
        1. Parses dbt_project.yml to discover all seeds
        2. Downloads CSVs from S3 to bronze/tuva_seeds
        3. Converts CSVs to parquet in silver

        Args:
            executor: Executor instance with storage and catalog access
            logger: Logger instance
            overwrite: Overwrite existing files
            download_only: Only download CSVs, don't convert to parquet
            convert_only: Only convert existing CSVs, don't download

        Returns:
            Dictionary mapping flat_name to TransformResult
    """
    results = {}

    # Get storage paths from executor
    bronze_path = executor.storage_config.get_path(MedallionLayer.BRONZE)
    silver_path = executor.storage_config.get_path(MedallionLayer.SILVER)

    # Find Tuva project directory
    package_root = Path(__file__).parent.parent.parent.parent
    tuva_project_dir = package_root / "src" / "acoharmony" / "_tuva" / "_depends" / "repos" / "tuva"

    if not tuva_project_dir.exists():
        logger.error(f"Tuva project not found: {tuva_project_dir}")
        raise FileNotFoundError(f"Tuva project not found: {tuva_project_dir}")

    # Parse seed definitions
    logger.info("=" * 80)
    logger.info("REFERENCE DATA TRANSFORMATION: Tuva Seeds → Silver")
    logger.info("=" * 80)

    stages = parse_tuva_seed_definitions(tuva_project_dir, logger)
    logger.info(f"Found {len(stages)} reference data stages")

    # Group stages by category
    by_group = {}
    for stage in stages:
        if stage.group not in by_group:
            by_group[stage.group] = []
        by_group[stage.group].append(stage)

    logger.info(f"Grouped into {len(by_group)} categories:")
    for group, group_stages in sorted(by_group.items()):
        logger.info(f"  {group}: {len(group_stages)} seeds")

    # Process each stage
    logger.info("\n" + "=" * 80)
    logger.info("PROCESSING STAGES")
    logger.info("=" * 80)

    for stage in sorted(stages, key=lambda s: s.order):
        logger.info(f"\n[{stage.order}/{len(stages)}] {stage.flat_name} [{stage.group}]")

        try:
            # Step 1: Download CSV to bronze (unless convert_only)
            csv_path = None
            if not convert_only:
                csv_path = download_reference_stage(stage, bronze_path, logger, overwrite)

            # Step 2: Convert to parquet in silver (unless download_only)
            parquet_path = None
            if not download_only:
                parquet_path = convert_reference_stage(
                    stage, bronze_path, silver_path, logger, overwrite
                )

            # Create result
            if parquet_path:
                results[stage.flat_name] = TransformResult(
                    status=ResultStatus.SUCCESS,
                    message=f"{stage.flat_name}: Transformed to {parquet_path.name}",
                    output_path=str(parquet_path),
                )
            elif csv_path:
                results[stage.flat_name] = TransformResult(
                    status=ResultStatus.SUCCESS,
                    message=f"{stage.flat_name}: Downloaded to {csv_path.name}",
                    output_path=str(csv_path),
                )
            else:
                msg = (
                    f"{stage.flat_name}: {stage.skip_reason}"
                    if stage.skip_reason
                    else f"{stage.flat_name}: Already exists or skipped (optional)"
                )
                results[stage.flat_name] = TransformResult(
                    status=ResultStatus.SKIPPED,
                    message=msg,
                )

        except Exception as e:  # ALLOWED: Pipeline continues with partial results
            logger.error(f"  [ERROR] {stage.flat_name} failed: {e}")
            results[stage.flat_name] = TransformResult(
                status=ResultStatus.FAILURE,
                message=f"{stage.flat_name}: {str(e)}",
            )

    # Summary
    logger.info("\n" + "=" * 80)
    successful = sum(1 for r in results.values() if r.status == ResultStatus.SUCCESS)
    skipped = sum(1 for r in results.values() if r.status == ResultStatus.SKIPPED)
    failed = sum(1 for r in results.values() if r.status == ResultStatus.FAILURE)

    logger.info("[OK] Reference Data Transformation Complete:")
    logger.info(f"  {successful} successful, {skipped} skipped, {failed} failed")
    if skipped or failed:
        for name, r in results.items():
            if r.status in (ResultStatus.SKIPPED, ResultStatus.FAILURE):
                tag = "skipped" if r.status == ResultStatus.SKIPPED else "FAILED"
                logger.info(f"  {tag}: {name} — {r.message}")
    logger.info("=" * 80)

    return results


def transform_reference_category(
    category: str,
    executor: Any,
    logger: Any,
    overwrite: bool = False,
) -> dict[str, TransformResult]:
    """
    Transform reference data for a specific category.

        Categories:
        - terminology: ICD-10-CM, ICD-9-CM, CPT, HCPCS, NDC, etc.
        - value_sets: CCSR, clinical groupings, quality measures
        - risk_adjustment: CMS-HCC mappings
        - data_quality: Data quality checks
        - clinical: Clinical classification systems
        - quality_measures: AHRQ quality measures
        - reference: General reference tables

        Args:
            category: Category name (terminology, value_sets, etc.)
            executor: Executor instance with storage and catalog access
            logger: Logger instance
            overwrite: Overwrite existing files

        Returns:
            Dictionary mapping flat_name to TransformResult
    """
    # Get all stages and filter by category
    bronze_path = executor.storage_config.get_path(MedallionLayer.BRONZE)
    silver_path = executor.storage_config.get_path(MedallionLayer.SILVER)

    package_root = Path(__file__).parent.parent.parent.parent
    tuva_project_dir = package_root / "src" / "acoharmony" / "_tuva" / "_depends" / "repos" / "tuva"

    all_stages = parse_tuva_seed_definitions(tuva_project_dir, logger)
    category_stages = [s for s in all_stages if s.group == category]

    logger.info(f"Processing {len(category_stages)} seeds in category: {category}")

    results = {}
    for stage in category_stages:
        logger.info(f"\n{stage.flat_name}")

        try:
            # Download and convert
            download_reference_stage(stage, bronze_path, logger, overwrite)
            parquet_path = convert_reference_stage(
                stage, bronze_path, silver_path, logger, overwrite
            )

            if parquet_path:
                results[stage.flat_name] = TransformResult(
                    status=ResultStatus.SUCCESS,
                    message=f"{stage.flat_name}: Transformed to {parquet_path.name}",
                    output_path=str(parquet_path),
                )
            else:
                results[stage.flat_name] = TransformResult(
                    status=ResultStatus.SKIPPED,
                    message=f"{stage.flat_name}: Skipped",
                )

        except Exception as e:  # ALLOWED: Category processing continues with partial results
            logger.error(f"  [ERROR] Failed: {e}")
            results[stage.flat_name] = TransformResult(
                status=ResultStatus.FAILURE,
                message=f"{stage.flat_name}: {str(e)}",
            )

    return results


def list_available_seeds(tuva_project_dir: Path | None = None) -> dict[str, list[str]]:
    """
    List all available Tuva seeds grouped by category.

        Args:
            tuva_project_dir: Optional path to Tuva project (default: auto-detect)

        Returns:
            Dictionary mapping category to list of seed flat_names
    """
    if tuva_project_dir is None:
        package_root = Path(__file__).parent.parent.parent.parent
        tuva_project_dir = (
            package_root / "src" / "acoharmony" / "_tuva" / "_depends" / "repos" / "tuva"
        )

    logger = LogWriter("reference.list")
    stages = parse_tuva_seed_definitions(tuva_project_dir, logger)

    # Group by category
    by_category = {}
    for stage in stages:
        if stage.group not in by_category:
            by_category[stage.group] = []
        by_category[stage.group].append(stage.flat_name)

    return by_category
