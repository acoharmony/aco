# © 2025 HarmonyCares
# All rights reserved.

"""
Organize fixture parquet files into bronze/silver/gold subdirectories.

Reads schemas.json to determine which layer each fixture belongs to,
then moves the parquet files into the appropriate subdirectory.

"""

from __future__ import annotations

import json
import shutil
from pathlib import Path


def organize_fixtures(
    fixtures_dir: str | Path = "/opt/s3/data/workspace/logs/dev/fixtures",
    dry_run: bool = False,
) -> None:
    """
    Organize fixture parquet files into bronze/silver/gold subdirectories.

    Args:
        fixtures_dir: Directory containing fixture parquet files
        dry_run: If True, only show what would be done
    """
    fixtures_dir = Path(fixtures_dir)

    # Load schemas.json
    schemas_file = fixtures_dir / "schemas.json"
    if not schemas_file.exists():
        print(f"[FAILED] schemas.json not found: {schemas_file}")
        print("   Run 'aco dev generate-mocks' first")
        return

    with open(schemas_file) as f:
        schemas = json.load(f)

    # Create subdirectories
    bronze_dir = fixtures_dir / "bronze"
    silver_dir = fixtures_dir / "silver"
    gold_dir = fixtures_dir / "gold"

    if not dry_run:
        bronze_dir.mkdir(exist_ok=True)
        silver_dir.mkdir(exist_ok=True)
        gold_dir.mkdir(exist_ok=True)

    print("=" * 80)
    print("ACO Harmony Fixture Organizer")
    print("=" * 80)
    print(f"Fixtures: {fixtures_dir}")
    if dry_run:
        print("DRY RUN - no files will be moved\n")
    print()

    # Find all parquet files in root
    parquet_files = sorted(fixtures_dir.glob("*.parquet"))

    if not parquet_files:
        print(f"[FAILED] No parquet fixtures found in: {fixtures_dir}")
        return

    stats = {"bronze": 0, "silver": 0, "gold": 0, "unknown": 0}

    print(f"Organizing {len(parquet_files)} fixture files:\n")

    for parquet_file in parquet_files:
        table_name = parquet_file.stem

        # Determine layer from schemas.json
        layer = schemas.get(table_name, {}).get("layer", "unknown")

        # Map to directory
        if layer == "bronze":
            dest_dir = bronze_dir
        elif layer == "silver":
            dest_dir = silver_dir
        elif layer == "gold":
            dest_dir = gold_dir
        else:
            # Default to silver for unknown
            dest_dir = silver_dir
            layer = "silver (default)"

        dest_file = dest_dir / parquet_file.name

        if dry_run:
            print(f"  Would move: {table_name:50} → {layer}/")
        else:
            shutil.move(str(parquet_file), str(dest_file))
            print(f"  [OK] {table_name:50} → {layer}/")

        stats[layer.split()[0]] += 1

    print(f"\n[SUCCESS] Organization complete!")
    print(f"\n📊 Summary:")
    print(f"   bronze/: {stats['bronze']:3} tables")
    print(f"   silver/: {stats['silver']:3} tables")
    print(f"   gold/:   {stats['gold']:3} tables")

    if not dry_run:
        print(f"\n📁 Directory structure:")
        print(f"   {fixtures_dir}/")
        print(f"   ├── bronze/  ({stats['bronze']} files)")
        print(f"   ├── silver/  ({stats['silver']} files)")
        print(f"   ├── gold/    ({stats['gold']} files)")
        print(f"   └── schemas.json")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Organize fixture parquet files into medallion subdirectories"
    )
    parser.add_argument(
        "--fixtures-dir",
        default="/opt/s3/data/workspace/logs/dev/fixtures",
        help="Directory containing fixture parquet files",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done")

    args = parser.parse_args()

    organize_fixtures(fixtures_dir=args.fixtures_dir, dry_run=args.dry_run)
