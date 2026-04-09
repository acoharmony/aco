# © 2025 HarmonyCares
# All rights reserved.

"""
Populate DuckDB test database with fixture data organized by schema/layer.

Creates a DuckDB database at /opt/s3/data/workspace/logs/dev/test.duckdb with:
- test schema: All fixture tables (for backward compatibility)
- bronze schema: Tables from bronze layer fixtures
- silver schema: Tables from silver layer fixtures
- gold schema: Tables from gold layer fixtures

Each table is created as a view over the parquet files using scan_parquet.
This allows the DuckDB to reference the parquet data without duplicating storage.

"""

from __future__ import annotations

import gc
from pathlib import Path

import duckdb


def populate_test_duckdb(
    fixtures_dir: str | Path = "/opt/s3/data/workspace/logs/dev/fixtures",
    db_path: str | Path = "/opt/s3/data/workspace/logs/dev/test.duckdb",
    force: bool = False,
) -> None:
    """
    Populate DuckDB test database with fixture parquet files.

    Args:
        fixtures_dir: Directory containing fixture parquet subdirectories
        db_path: Path to DuckDB database file
        force: If True, recreate database; if False, error if exists
    """
    fixtures_dir = Path(fixtures_dir)
    db_path = Path(db_path)

    # Check if database exists
    if db_path.exists() and not force:
        print(f"[FAILED] Database already exists: {db_path}")
        print("   Use --force to recreate")
        return

    if db_path.exists() and force:
        print(f"🗑  Removing existing database: {db_path}")
        db_path.unlink()

    # Check for layer directories
    bronze_dir = fixtures_dir / "bronze"
    silver_dir = fixtures_dir / "silver"
    gold_dir = fixtures_dir / "gold"

    layers = {
        "bronze": bronze_dir,
        "silver": silver_dir,
        "gold": gold_dir,
    }

    print("=" * 80)
    print("ACO Harmony Test DuckDB Populator")
    print("=" * 80)
    print(f"Fixtures: {fixtures_dir}")
    print(f"Database: {db_path}")
    print()

    # Connect to DuckDB
    con = duckdb.connect(str(db_path))

    try:
        # Create schemas
        print("Creating schemas...")
        con.execute("CREATE SCHEMA IF NOT EXISTS fixtures")
        con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        con.execute("CREATE SCHEMA IF NOT EXISTS silver")
        con.execute("CREATE SCHEMA IF NOT EXISTS gold")
        print("  [OK] fixtures, bronze, silver, gold\n")

        # Track stats
        stats = {"fixtures": 0, "bronze": 0, "silver": 0, "gold": 0}

        print("Creating tables from parquet fixtures:\n")

        # Process each layer
        for layer_name, layer_dir in layers.items():
            if not layer_dir.exists():
                print(f"⚠  {layer_name}/ directory not found, skipping")
                continue

            parquet_files = sorted(layer_dir.glob("*.parquet"))

            if not parquet_files:
                print(f"⚠  No parquet files in {layer_name}/, skipping")
                continue

            print(f"{layer_name.upper()} Layer ({len(parquet_files)} tables):")

            for parquet_file in parquet_files:
                table_name = parquet_file.stem

                try:
                    # Create view in layer-specific schema
                    con.execute(
                        f"CREATE OR REPLACE VIEW {layer_name}.{table_name} AS "
                        f"SELECT * FROM read_parquet('{parquet_file}')"
                    )
                    stats[layer_name] += 1

                    # Also create in fixtures schema (all fixtures in one place for convenience)
                    con.execute(
                        f"CREATE OR REPLACE VIEW fixtures.{table_name} AS "
                        f"SELECT * FROM {layer_name}.{table_name}"
                    )
                    stats["fixtures"] += 1

                    # Get row count
                    result = con.execute(
                        f"SELECT COUNT(*) FROM {layer_name}.{table_name}"
                    ).fetchone()
                    row_count = result[0] if result else 0

                    print(f"  [OK] {table_name:50} ({row_count:,} rows)")

                    # Force garbage collection periodically
                    if stats[layer_name] % 50 == 0:
                        gc.collect()

                except Exception as e:
                    print(f"  [ERROR] {table_name}: {e}")

            print()

        print(f"[SUCCESS] Database populated successfully!")
        print(f"\n📊 Summary:")
        print(f"   fixtures schema: {stats['fixtures']:3} tables (all layers combined)")
        print(f"   bronze schema:   {stats['bronze']:3} tables")
        print(f"   silver schema:   {stats['silver']:3} tables")
        print(f"   gold schema:     {stats['gold']:3} tables")
        print(f"\n   Database: {db_path}")
        print(f"   Size: {db_path.stat().st_size / 1024:.1f} KB")

        # Show example queries
        print(f"\n📝 Example queries:")
        print(f"   SELECT * FROM fixtures.cclf1 LIMIT 10;")
        print(f"   SELECT * FROM silver.beneficiary_demographics LIMIT 10;")
        print(f"   SELECT * FROM bronze.plaru_meta LIMIT 10;")
        print(f"   SELECT * FROM gold.medical_claim LIMIT 10;")
        print(f"   SHOW ALL TABLES;")

    finally:
        con.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Populate DuckDB test database with fixture parquet files"
    )
    parser.add_argument(
        "--fixtures-dir",
        default="/opt/s3/data/workspace/logs/dev/fixtures",
        help="Directory containing fixture parquet subdirectories",
    )
    parser.add_argument(
        "--db-path",
        default="/opt/s3/data/workspace/logs/dev/test.duckdb",
        help="Path to DuckDB database file",
    )
    parser.add_argument("--force", action="store_true", help="Recreate database if exists")

    args = parser.parse_args()

    populate_test_duckdb(
        fixtures_dir=args.fixtures_dir, db_path=args.db_path, force=args.force
    )
