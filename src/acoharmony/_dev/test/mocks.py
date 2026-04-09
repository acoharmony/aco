# © 2025 HarmonyCares
# All rights reserved.

"""
Generate synthetic test mock data from production silver/gold parquet files.

This tool scans silver and gold parquet files, samples real data to understand
schemas and realistic value distributions, then generates synthetic test fixtures
that can be used for testing _expressions without requiring production data.

The generation is recursive and dependency-aware to avoid memory issues:
1. Scan metadata first (schemas, row counts, sample values)
2. Generate mocks in dependency order (similar to pipeline execution)
3. Write fixtures incrementally to avoid holding everything in memory

"""

from __future__ import annotations

import gc
import json
import random
import string
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from acoharmony._store import StorageBackend
from acoharmony.medallion import MedallionLayer


@dataclass
class ColumnMetadata:
    """Metadata about a column from sampled data."""

    name: str
    dtype: str
    null_count: int
    null_percentage: float
    sample_values: list[Any] = field(default_factory=list)
    unique_count: int | None = None
    min_value: Any | None = None
    max_value: Any | None = None


@dataclass
class TableMetadata:
    """Metadata about a table from production data."""

    name: str
    layer: str  # 'silver' or 'gold'
    path: str
    row_count: int
    columns: dict[str, ColumnMetadata]
    dependencies: list[str] = field(default_factory=list)


class MockDataGenerator:
    """Generate synthetic test data based on production parquet schemas."""

    def __init__(
        self,
        storage: StorageBackend | None = None,
        sample_size: int = 1000,
        sample_values_per_column: int = 20,
    ):
        """
        Initialize mock data generator.

                Args:
                    storage: Storage backend (defaults to dev profile)
                    sample_size: Number of rows to sample from each table for metadata
                    sample_values_per_column: Number of sample values to collect per column
        """
        self.storage = storage or StorageBackend()
        self.sample_size = sample_size
        self.sample_values_per_column = sample_values_per_column
        self.metadata_cache: dict[str, TableMetadata] = {}

    def scan_table_metadata(
        self, path: Path, layer: str, table_name: str
    ) -> TableMetadata | None:
        """
        Scan a parquet file and extract metadata without loading full table.

                Args:
                    path: Path to parquet file
                    layer: 'silver' or 'gold'
                    table_name: Name of the table

                Returns:
                    TableMetadata or None if file cannot be read
        """
        try:
            # Read schema first (fast, no data load)
            schema = pl.read_parquet_schema(path)

            # Sample data for value distributions using scan_parquet
            df_sample = pl.scan_parquet(path).head(self.sample_size).collect()
            row_count = len(df_sample)

            # Extract column metadata
            columns = {}
            for col_name in schema.keys():
                col = df_sample[col_name]
                null_count = col.null_count()
                null_pct = null_count / row_count if row_count > 0 else 0.0

                sample_vals = []
                min_val = None
                max_val = None
                unique_count = None

                try:
                    # Skip Date/Datetime columns entirely - we'll generate synthetic dates from scratch
                    # Trying to read invalid dates causes Rust panics
                    if col.dtype not in [pl.Date, pl.Datetime]:
                        sample_vals = (
                            col.drop_nulls()
                            .unique()
                            .head(self.sample_values_per_column)
                            .to_list()
                        )
                        sample_vals = [self._serialize_value(v) for v in sample_vals]

                    unique_count = col.n_unique()

                    # Get min/max only for numeric types
                    if col.dtype in [pl.Int64, pl.Int32, pl.Float64]:
                        min_val = self._serialize_value(col.min())
                        max_val = self._serialize_value(col.max())
                except Exception:
                    pass

                columns[col_name] = ColumnMetadata(
                    name=col_name,
                    dtype=str(schema[col_name]),
                    null_count=null_count,
                    null_percentage=null_pct,
                    sample_values=sample_vals,
                    unique_count=unique_count,
                    min_value=min_val,
                    max_value=max_val,
                )

            # Infer dependencies from column names (basic heuristic)
            dependencies = self._infer_dependencies(columns)

            return TableMetadata(
                name=table_name,
                layer=layer,
                path=str(path),
                row_count=row_count,
                columns=columns,
                dependencies=dependencies,
            )

        except Exception as e:
            print(f"Error scanning {path}: {e}")
            return None

    def _serialize_value(self, value: Any) -> Any:
        """Convert value to JSON-serializable format."""
        if value is None:
            return None
        if isinstance(value, date | datetime):
            return value.isoformat()
        if isinstance(value, int | float | str | bool):
            return value
        # Try to convert to string as fallback
        return str(value)

    def _infer_dependencies(self, columns: dict[str, ColumnMetadata]) -> list[str]:
        """
        Infer table dependencies from column names.

                This is a heuristic approach - looks for common patterns like:
                - bene_mbi_id -> depends on beneficiary data
                - clm_id -> depends on claims data
        """
        dependencies = []
        col_names = set(columns.keys())

        # Common dependency patterns
        if "bene_mbi_id" in col_names or "mbi" in col_names:
            dependencies.append("beneficiary")

        if any("clm" in col.lower() for col in col_names):
            dependencies.append("claims")

        if any("provider" in col.lower() or "prvdr" in col.lower() for col in col_names):
            dependencies.append("provider")

        return dependencies

    def scan_all_tables(
        self, layers: list[str] | None = None
    ) -> dict[str, TableMetadata]:
        """
        Scan all parquet files in silver/gold layers.

                Args:
                    layers: List of layers to scan (default: ['silver', 'gold'])

                Returns:
                    Dictionary mapping table_name to TableMetadata
        """
        if layers is None:
            layers = ["silver", "gold"]

        all_metadata = {}

        for layer_name in layers:
            layer = MedallionLayer.from_tier(layer_name)
            layer_path = self.storage.get_path(layer)

            if not layer_path.exists():
                print(f"Layer path does not exist: {layer_path}")
                continue

            print(f"\nScanning {layer_name} layer: {layer_path}")

            # Find all parquet files
            parquet_files = list(layer_path.glob("*.parquet"))
            print(f"Found {len(parquet_files)} parquet files")

            for parquet_file in sorted(parquet_files):
                table_name = parquet_file.stem
                print(f"  Scanning {table_name}...", end=" ")

                metadata = self.scan_table_metadata(parquet_file, layer_name, table_name)
                if metadata:
                    all_metadata[table_name] = metadata
                    print(
                        f"[OK] ({metadata.row_count:,} rows, {len(metadata.columns)} cols)"
                    )
                else:
                    print("[ERROR] (failed)")

                # Force garbage collection after each table to prevent memory buildup
                gc.collect()

        self.metadata_cache = all_metadata
        return all_metadata

    def generate_synthetic_value(
        self, col_meta: ColumnMetadata, col_name: str
    ) -> Any:
        """
        Generate a synthetic value for a column based on metadata.

                Args:
                    col_meta: Column metadata
                    col_name: Column name (used for heuristics)

                Returns:
                    Synthetic value matching column type
        """
        # Honor null percentage (with some randomness)
        if random.random() < col_meta.null_percentage * 0.8:  # 80% of original null rate
            return None

        # Use sample values if available
        if col_meta.sample_values:
            return random.choice(col_meta.sample_values)

        # Generate based on column name patterns
        col_lower = col_name.lower()

        # MBI patterns
        if "mbi" in col_lower and "String" in col_meta.dtype:
            return self._generate_mbi()

        # NPI patterns
        if "npi" in col_lower:
            return str(random.randint(1000000000, 9999999999))

        # TIN patterns
        if "tin" in col_lower:
            return str(random.randint(100000000, 999999999))

        # Date patterns
        if "date" in col_lower or col_lower.endswith("_dt"):
            if "Date" in col_meta.dtype and "Datetime" not in col_meta.dtype:
                if col_meta.min_value and col_meta.max_value:
                    min_date = datetime.fromisoformat(col_meta.min_value).date()
                    max_date = datetime.fromisoformat(col_meta.max_value).date()
                    delta = (max_date - min_date).days
                    return min_date + timedelta(days=random.randint(0, delta))
                return date(2024, random.randint(1, 12), random.randint(1, 28))
            elif "Datetime" in col_meta.dtype:
                return datetime(
                    2024,
                    random.randint(1, 12),
                    random.randint(1, 28),
                    random.randint(0, 23),
                    random.randint(0, 59),
                )

        # Generate by dtype with min/max bounds
        if "Int" in col_meta.dtype:
            if col_meta.min_value is not None and col_meta.max_value is not None:
                return random.randint(col_meta.min_value, col_meta.max_value)
            return random.randint(0, 10000)

        elif "Float" in col_meta.dtype:
            if col_meta.min_value is not None and col_meta.max_value is not None:
                return random.uniform(col_meta.min_value, col_meta.max_value)
            return random.uniform(0.0, 10000.0)

        elif "String" in col_meta.dtype:
            if "code" in col_lower or "cd" in col_lower:
                return random.choice(["01", "02", "03", "A", "B", "C"])
            elif "name" in col_lower:
                return random.choice(["SMITH", "JONES", "WILLIAMS", "BROWN", "DAVIS"])
            elif "id" in col_lower:
                return f"ID{random.randint(10000, 99999)}"
            return f"VALUE_{random.randint(1000, 9999)}"

        elif "Boolean" in col_meta.dtype:
            return random.choice([True, False])

        return None

    def _generate_mbi(self) -> str:
        """Generate realistic MBI (Medicare Beneficiary Identifier)."""
        # MBI format: 1 + alpha + alphanum + num + alpha + alphanum + num + alpha + alpha + num + num
        # Exclude: S, L, O, I, B, Z
        chars1 = "".join(
            c
            for c in string.ascii_uppercase
            if c not in ["S", "L", "O", "I", "B", "Z"]
        )
        chars2 = chars1 + string.digits

        return (
            "1"
            + random.choice(chars1)
            + random.choice(chars2)
            + random.choice(string.digits)
            + random.choice(chars1)
            + random.choice(chars2)
            + random.choice(string.digits)
            + random.choice(chars1)
            + random.choice(chars1)
            + random.choice(string.digits)
            + random.choice(string.digits)
        )

    def generate_synthetic_dataframe(
        self, table_name: str, n_rows: int = 100
    ) -> pl.DataFrame:
        """
        Generate synthetic DataFrame for a table.

                Args:
                    table_name: Name of table to generate
                    n_rows: Number of rows to generate

                Returns:
                    Synthetic DataFrame with realistic data
        """
        if table_name not in self.metadata_cache:
            raise ValueError(
                f"Table '{table_name}' not in cache. Run scan_all_tables first."
            )

        metadata = self.metadata_cache[table_name]
        data = {}

        for col_name, col_meta in metadata.columns.items():
            values = [
                self.generate_synthetic_value(col_meta, col_name) for _ in range(n_rows)
            ]
            data[col_name] = values

        return pl.DataFrame(data)

    def save_metadata_schema(self, output_path: Path) -> None:
        """
        Save metadata cache to JSON for use in test fixtures.

                Args:
                    output_path: Path to save JSON schema file
        """
        # Convert metadata to serializable format
        schema_dict = {
            name: {
                "layer": meta.layer,
                "row_count": meta.row_count,
                "dependencies": meta.dependencies,
                "columns": {
                    col_name: asdict(col_meta)
                    for col_name, col_meta in meta.columns.items()
                },
            }
            for name, meta in self.metadata_cache.items()
        }

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w") as f:
            json.dump(schema_dict, f, indent=2)

        print(f"\nMetadata schema saved to: {output_path}")

    def save_synthetic_fixtures(
        self,
        output_dir: Path,
        tables: list[str] | None = None,
        n_rows: int = 100,
        force: bool = False,
    ) -> None:
        """
        Generate and save synthetic parquet fixtures for testing.

                Args:
                    output_dir: Directory to save fixtures
                    tables: List of table names (None = all tables)
                    n_rows: Number of rows per fixture
                    force: If True, regenerate existing fixtures; if False, skip existing
        """
        output_dir.mkdir(parents=True, exist_ok=True)

        tables_to_generate = tables or list(self.metadata_cache.keys())

        print(f"\nGenerating synthetic fixtures ({n_rows} rows each):")
        if not force:
            print("  (Use --force to regenerate existing fixtures)")

        skipped_count = 0
        generated_count = 0

        for table_name in sorted(tables_to_generate):
            if table_name not in self.metadata_cache:
                print(f"  [ERROR] {table_name} (not in metadata cache)")
                continue

            output_path = output_dir / f"{table_name}.parquet"

            # Skip if fixture already exists and force=False
            if output_path.exists() and not force:
                print(f"  ⊘ {table_name} (already exists, skipping)")
                skipped_count += 1
                continue

            try:
                df = self.generate_synthetic_dataframe(table_name, n_rows)

                # Use lazy write via sink_parquet for memory efficiency
                df.lazy().sink_parquet(output_path)

                action = "regenerated" if output_path.exists() else "created"
                print(
                    f"  [OK] {table_name} → {output_path} ({len(df):,} rows, {len(df.columns)} cols) [{action}]"
                )
                generated_count += 1

                # Delete dataframe and force garbage collection to prevent memory buildup
                del df
                gc.collect()

            except Exception as e:
                print(f"  [ERROR] {table_name}: {e}")

        if skipped_count > 0:
            print(f"\n  Skipped {skipped_count} existing fixture(s)")
        if generated_count > 0:
            print(f"  Generated {generated_count} new fixture(s)")


def generate_test_mocks(
    layers: list[str] | None = None,
    tables: list[str] | None = None,
    output_dir: str | Path = "/opt/s3/data/workspace/logs/dev/fixtures",
    n_rows: int = 100,
    sample_size: int = 1000,
    dry_run: bool = False,
    force: bool = False,
) -> None:
    """
    Main entry point to generate test mocks from production data.

        Args:
            layers: Layers to scan (default: ['silver', 'gold'])
            tables: Specific tables to generate (default: all found tables)
            output_dir: Where to save generated fixtures
            n_rows: Number of rows per synthetic fixture
            sample_size: Number of rows to sample for metadata extraction
            dry_run: If True, only scan and print what would be generated
            force: If True, regenerate existing fixtures; if False, skip existing

    """
    output_dir = Path(output_dir)

    print("=" * 80)
    print("ACO Harmony Test Mock Generator")
    print("=" * 80)

    # Initialize generator
    generator = MockDataGenerator(sample_size=sample_size)

    # Scan tables
    metadata = generator.scan_all_tables(layers=layers)

    if not metadata:
        print("\n[FAILED] No tables found to generate mocks from")
        return

    print(f"\n📊 Found {len(metadata)} tables:")
    for name, meta in sorted(metadata.items()):
        print(
            f"  - {name:30} ({meta.layer:6} | {meta.row_count:>8,} rows | {len(meta.columns):>3} cols)"
        )

    if dry_run:
        print(f"\n🔍 DRY RUN - Would generate {len(tables) if tables else len(metadata)} synthetic fixtures")
        print(f"   Output directory: {output_dir}")
        print(f"   Rows per fixture: {n_rows}")
        return

    # Save metadata schema
    schema_path = output_dir / "schemas.json"
    generator.save_metadata_schema(schema_path)

    # Generate synthetic fixtures
    generator.save_synthetic_fixtures(output_dir, tables=tables, n_rows=n_rows, force=force)

    print("\n[SUCCESS] Mock generation complete!")
    print(f"   Fixtures: {output_dir}")
    print(f"   Schema: {schema_path}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate test mocks from production data")
    parser.add_argument(
        "--layers",
        nargs="+",
        default=["silver", "gold"],
        help="Layers to scan (default: silver gold)",
    )
    parser.add_argument(
        "--tables", nargs="+", help="Specific tables to generate (default: all)"
    )
    parser.add_argument(
        "--output-dir",
        default="/opt/s3/data/workspace/logs/dev/fixtures",
        help="Output directory for fixtures",
    )
    parser.add_argument(
        "--n-rows", type=int, default=100, help="Rows per synthetic fixture"
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Sample size for metadata extraction",
    )
    parser.add_argument("--dry-run", action="store_true", help="Only scan, don't generate")
    parser.add_argument(
        "--force", action="store_true", help="Regenerate existing fixtures"
    )

    args = parser.parse_args()

    generate_test_mocks(
        layers=args.layers,
        tables=args.tables,
        output_dir=args.output_dir,
        n_rows=args.n_rows,
        sample_size=args.sample_size,
        dry_run=args.dry_run,
        force=args.force,
    )
