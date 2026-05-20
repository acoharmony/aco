#!/usr/bin/env python3
"""
Test to check which columns are present vs required by schema.

This helps identify what's actually missing before adding schema compliance.
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import sys
from pathlib import Path

import polars as pl
import yaml

# Add parent path
sys.path.append(str(Path(__file__).parent.parent.parent.parent))


def check_schema_compliance():
    """Check what columns exist vs what schema requires."""

    # Load schema
    with open("/home/care/acoharmony/src/acoharmony/_schemas/consolidated_alignment.yml") as f:
        schema = yaml.safe_load(f)

    required_columns = [col["output_name"] for col in schema["columns"]]
    print(f"Schema requires {len(required_columns)} columns")

    # Try to load existing consolidated alignment result
    try:
        # Use fixtures_dir from conftest to avoid hard-coded paths
        from pathlib import Path
        fixtures_dir = Path(__file__).parent.parent.parent / "workspace" / "silver"
        df = pl.scan_parquet(fixtures_dir / "consolidated_alignment.parquet")
        existing_columns = df.collect_schema().names()
        print(f"Current implementation has {len(existing_columns)} columns")

        # Find missing columns
        missing = set(required_columns) - set(existing_columns)
        extra = set(existing_columns) - set(required_columns)

        print(f"\nMissing {len(missing)} columns:")
        for col in sorted(missing):
            print(f"  - {col}")

        print(f"\nExtra {len(extra)} columns (not in schema):")
        for col in sorted(extra):
            print(f"  + {col}")

        # Group missing columns by category
        print("\n\nMissing columns by category:")

        temporal = [c for c in missing if "block_" in c or "is_first" in c or "is_last" in c]
        if temporal:
            print(f"\nTemporal ({len(temporal)}):")
            for col in temporal:
                print(f"  - {col}")

        provider = [c for c in missing if "provider" in c and c not in temporal]
        if provider:
            print(f"\nProvider ({len(provider)}):")
            for col in provider:
                print(f"  - {col}")

        program = [
            c
            for c in missing
            if any(p in c for p in ["reach", "mssp", "ffs"]) and c not in temporal + provider
        ]
        if program:
            print(f"\nProgram ({len(program)}):")
            for col in program:
                print(f"  - {col}")

        enrollment = [c for c in missing if "enrollment" in c or "continuous" in c]
        if enrollment:
            print(f"\nEnrollment ({len(enrollment)}):")
            for col in enrollment:
                print(f"  - {col}")

        validation = [
            c
            for c in missing
            if any(v in c for v in ["valid", "eligible", "error", "rule", "exclude"])
        ]
        if validation:
            print(f"\nValidation ({len(validation)}):")
            for col in validation:
                print(f"  - {col}")

        metadata = [
            c
            for c in missing
            if any(m in c for m in ["process", "timestamp", "lineage", "quality", "audit"])
        ]
        if metadata:
            print(f"\nMetadata ({len(metadata)}):")
            for col in metadata:
                print(f"  - {col}")

        other = [
            c
            for c in missing
            if c not in temporal + provider + program + enrollment + validation + metadata
        ]
        if other:
            print(f"\nOther ({len(other)}):")
            for col in other:
                print(f"  - {col}")

    except Exception as e:
        print(f"Could not load existing consolidated_alignment: {e}")
        print("Will check after running pipeline")

    return required_columns


if __name__ == "__main__":
    required_columns = check_schema_compliance()
