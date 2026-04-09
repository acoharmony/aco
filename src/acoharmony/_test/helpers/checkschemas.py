#!/usr/bin/env python3
"""
Check all schemas for required blocks and structure.
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import json
from pathlib import Path

import yaml


def check_schema(schema_file: Path) -> dict:
    """Check a single schema file for required blocks."""
    with open(schema_file) as f:
        data = yaml.safe_load(f)

    schema_name = schema_file.stem

    result = {
        "name": schema_name,
        "has_name": "name" in data,
        "has_description": "description" in data,
        "has_columns": "columns" in data and len(data.get("columns", [])) > 0,
        "has_storage": "storage" in data,
        "has_file_format": "file_format" in data,
        "has_file_patterns": False,
        "has_keys": "keys" in data,
        "has_pipeline": "pipeline" in data,
        "has_transformations": "transformations" in data,
        "has_polars": "polars" in data,
        "column_count": len(data.get("columns", [])),
        "file_type": None,
        "is_raw_data": False,
    }

    # Check storage details
    if result["has_storage"]:
        storage = data.get("storage", {})
        result["has_file_patterns"] = "file_patterns" in storage
        if result["has_file_patterns"]:
            patterns = storage.get("file_patterns", {})
            # Check if it's a raw data schema (has file patterns for discovery)
            result["is_raw_data"] = bool(patterns) and not isinstance(patterns, str)

    # Check file format
    if result["has_file_format"]:
        file_format = data.get("file_format", {})
        result["file_type"] = file_format.get("type", "unknown")

    return result


def main():
    schemas_dir = Path("/home/care/acoharmony/src/acoharmony/_schemas")
    schemas = list(schemas_dir.glob("*.yml"))

    results = []
    for schema_file in sorted(schemas):
        result = check_schema(schema_file)
        results.append(result)

    # Categorize schemas
    raw_schemas = [r for r in results if r["is_raw_data"]]
    processed_schemas = [r for r in results if not r["is_raw_data"]]

    # Print summary
    print("=" * 80)
    print("SCHEMA ANALYSIS SUMMARY")
    print("=" * 80)
    print(f"Total schemas: {len(results)}")
    print(f"Raw data schemas: {len(raw_schemas)}")
    print(f"Processed/derived schemas: {len(processed_schemas)}")
    print()

    # Check for missing required blocks
    print("SCHEMAS WITH MISSING BLOCKS:")
    print("-" * 40)

    issues = []
    for r in results:
        missing = []
        if not r["has_name"]:
            missing.append("name")
        if not r["has_description"]:
            missing.append("description")
        if not r["has_columns"]:
            missing.append("columns")
        if not r["has_storage"]:
            missing.append("storage")
        if not r["has_file_format"]:
            missing.append("file_format")

        if missing:
            issues.append(f"{r['name']}: Missing {', '.join(missing)}")

    if issues:
        for issue in issues:
            print(f"  - {issue}")
    else:
        print("  [SUCCESS] All schemas have required blocks!")

    print()
    print("RAW DATA SCHEMAS (with file patterns):")
    print("-" * 40)
    for r in raw_schemas:
        print(f"  - {r['name']}: {r['file_type']} format, {r['column_count']} columns")

    print()
    print("FILE FORMATS USED:")
    print("-" * 40)
    formats = {}
    for r in results:
        fmt = r["file_type"] or "not specified"
        formats[fmt] = formats.get(fmt, 0) + 1

    for fmt, count in sorted(formats.items()):
        print(f"  - {fmt}: {count} schemas")

    print()
    print("SCHEMAS WITH PIPELINES:")
    print("-" * 40)
    pipeline_schemas = [r for r in results if r["has_pipeline"]]
    if pipeline_schemas:
        for r in pipeline_schemas:
            print(f"  - {r['name']}")
    else:
        print("  None found")

    print()
    print("SCHEMAS WITH TRANSFORMATIONS:")
    print("-" * 40)
    transform_schemas = [r for r in results if r["has_transformations"]]
    if transform_schemas:
        for r in transform_schemas:
            print(f"  - {r['name']}")
    else:
        print("  None found")

    # Save detailed results to JSON
    output_file = Path("/home/care/acoharmony/schema_analysis.json")
    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    print()
    print(f"Detailed results saved to: {output_file}")

    return results


if __name__ == "__main__":
    main()
