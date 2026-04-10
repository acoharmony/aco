#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""Extract and document ACO metadata from CCLF filenames."""

import glob
import re
from collections import defaultdict
from datetime import datetime
from pathlib import Path

from acoharmony._log import get_logger
from acoharmony._registry import SchemaRegistry
from acoharmony._store import StorageBackend

logger = get_logger("dev.aco_metadata")


def extract_aco_metadata(filename):
    """
    Extract ACO ID, CCLF type, program, and other metadata from filename.

        Pattern: P.<ACO_ID>.ACO.ZC<CCLF_TYPE>[W]<PROGRAM><YEAR>.D<DATE>.T<TIME>
    """
    match = re.match(
        r"P\.([A-Z0-9]+)\.ACO\.ZC([0-9A-Z])(W?)([YR])(\d{2})\.D(\d{6})\.T(\d{7})", filename
    )

    if match:
        return {
            "aco_id": match.group(1),
            "cclf_type": match.group(2),
            "is_weekly": match.group(3) == "W",
            "program": match.group(4),
            "year": match.group(5),
            "date": match.group(6),
            "time": match.group(7),
            "program_full": f"{match.group(4)}{match.group(5)}",
        }
    return None


def load_schema_file_patterns():
    """Load file patterns from all registered schemas via SchemaRegistry."""
    # Ensure _tables models are imported so SchemaRegistry is populated
    from acoharmony import _tables as _  # noqa: F401

    all_patterns = {}
    for schema_name in SchemaRegistry.list_schemas():
        storage_cfg = SchemaRegistry.get_storage_config(schema_name)
        patterns = storage_cfg.get("file_patterns")
        if patterns:
            all_patterns[schema_name] = patterns
            logger.debug(f"Loaded patterns for {schema_name}: {patterns}")
    return all_patterns


def generate_aco_metadata():
    """
    Generate ACO metadata documentation in docs folder.

        Returns

        bool
            True if successful, False otherwise
    """
    logger.info("Starting ACO metadata extraction using schema file patterns")

    # Get the storage configuration to find raw data path
    try:
        import os

        profile = os.getenv("ACO_PROFILE", "local")
        storage_config = StorageBackend(profile=profile)
        raw_data_path = storage_config.get_data_path("raw")
        logger.info(f"Using raw data path: {raw_data_path}")
    except Exception as e:
        import os

        from acoharmony._exceptions import StorageBackendError
        profile = os.getenv("ACO_PROFILE")
        raise StorageBackendError.from_initialization_error(e, profile) from e

    docs_dir = Path("docs")
    docs_dir.mkdir(exist_ok=True)

    # Load file patterns from schemas
    schema_patterns = load_schema_file_patterns()
    logger.info(f"Loaded file patterns from {len(schema_patterns)} schemas")

    # Group files by ACO and type
    aco_files = defaultdict(lambda: defaultdict(list))
    pattern_matches = defaultdict(list)

    # Search for files matching the patterns
    if raw_data_path.exists():
        for schema_name, patterns in schema_patterns.items():
            if not patterns:  # Skip if no patterns defined
                continue
            for pattern_type, pattern_value in patterns.items():
                # Skip nested structures (like report_year_extraction)
                if isinstance(pattern_value, dict):
                    continue

                # Remove quotes if present
                pattern = (
                    pattern_value.strip("'\"")
                    if isinstance(pattern_value, str)
                    else str(pattern_value)
                )

                # Convert pattern to glob pattern
                glob_pattern = str(raw_data_path / pattern)
                matching_files = glob.glob(glob_pattern)

                if matching_files:
                    logger.info(
                        f"Found {len(matching_files)} files matching {schema_name}.{pattern_type}: {pattern}"
                    )
                    pattern_matches[schema_name].extend(matching_files)

                for filepath in matching_files:
                    filename = Path(filepath).name
                    metadata = extract_aco_metadata(filename)
                    if metadata:
                        aco_id = metadata["aco_id"]
                        cclf_type = metadata["cclf_type"]
                        aco_files[aco_id][cclf_type].append(
                            {
                                "filename": filename,
                                "metadata": metadata,
                                "schema": schema_name,
                                "pattern_type": pattern_type,
                            }
                        )

    # Generate markdown documentation
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    content = ["# ACO Metadata Analysis\n"]
    content.append(f"*Generated on {timestamp}*\n")
    content.append("\n## Overview\n")
    content.append("Analysis of CCLF files in workspace directory.\n")

    # Summary section
    content.append("\n## Summary Statistics\n")
    total_files = sum(len(files) for aco in aco_files.values() for files in aco.values())
    content.append(f"- **Total ACOs**: {len(aco_files)}\n")
    content.append(f"- **Total CCLF files**: {total_files}\n")
    content.append(f"- **ACO IDs**: {', '.join(sorted(aco_files.keys()))}\n")
    content.append(f"- **Schemas with data**: {', '.join(sorted(pattern_matches.keys()))}\n")
    content.append(f"- **Raw data path**: {raw_data_path}\n")

    # Check for weekly files
    weekly_count = 0
    for aco in aco_files.values():
        for files in aco.values():
            weekly_count += sum(1 for f in files if f["metadata"]["is_weekly"])

    if weekly_count > 0:
        content.append(f"- **Weekly files (W suffix)**: {weekly_count}\n")

    # Schema pattern analysis
    if pattern_matches:
        content.append("\n## Schema File Pattern Analysis\n")
        content.append("\n| Schema | Files Found | Pattern Types |\n")
        content.append("|--------|-------------|---------------|\n")

        for schema in sorted(pattern_matches.keys()):
            file_count = len(pattern_matches[schema])
            # Find all pattern types for this schema
            pattern_types = set()
            for aco in aco_files.values():
                for files in aco.values():
                    for f in files:
                        if f.get("schema") == schema:
                            pattern_types.add(f.get("pattern_type", ""))
            pattern_types_str = ", ".join(sorted(pattern_types)) if pattern_types else "-"
            content.append(f"| {schema} | {file_count} | {pattern_types_str} |\n")

    # Detailed breakdown by ACO
    content.append("\n## ACO Details\n")

    for aco_id in sorted(aco_files.keys()):
        content.append(f"\n### ACO ID: {aco_id}\n")

        # Create table for this ACO
        content.append("\n| CCLF Type | Regular Files | Weekly Files | Programs |\n")
        content.append("|-----------|---------------|--------------|----------|\n")

        for cclf_type in sorted(aco_files[aco_id].keys()):
            files = aco_files[aco_id][cclf_type]

            # Group by program and wash status
            regular_files = [f for f in files if not f["metadata"]["is_weekly"]]
            weekly_files = [f for f in files if f["metadata"]["is_weekly"]]

            # Count programs
            regular_programs = defaultdict(int)
            for f in regular_files:
                regular_programs[f["metadata"]["program_full"]] += 1

            weekly_programs = defaultdict(int)
            for f in weekly_files:
                weekly_programs[f["metadata"]["program_full"]] += 1

            reg_count = len(regular_files) if regular_files else "-"
            week_count = len(weekly_files) if weekly_files else "-"

            programs = []
            if regular_programs:
                programs.extend([f"{p}:{c}" for p, c in sorted(regular_programs.items())])
            if weekly_programs:
                programs.extend([f"{p}(W):{c}" for p, c in sorted(weekly_programs.items())])

            prog_str = ", ".join(programs) if programs else "-"

            content.append(f"| CCLF{cclf_type} | {reg_count} | {week_count} | {prog_str} |\n")

    # File naming pattern documentation
    content.append("\n## CCLF File Naming Pattern\n")
    content.append("\n```\n")
    content.append("P.<ACO_ID>.ACO.ZC<CCLF_TYPE>[W]<PROGRAM><YEAR>.D<DATE>.T<TIME>\n")
    content.append("```\n\n")
    content.append("Where:\n")
    content.append("- **ACO_ID**: ACO identifier (e.g., D0259, A2671)\n")
    content.append("- **CCLF_TYPE**: 0-9, A, B (file type)\n")
    content.append("- **W**: Optional weekly indicator\n")
    content.append("- **PROGRAM**: Y (yearly) or R (runout)\n")
    content.append("- **YEAR**: 2-digit year\n")
    content.append("- **DATE**: YYMMDD format\n")
    content.append("- **TIME**: 7-digit timestamp\n")

    # Write to docs folder
    output_path = docs_dir / "ACO_METADATA.md"
    try:
        with open(output_path, "w") as f:
            f.write("".join(content))

        logger.info(f"Successfully generated ACO metadata documentation at {output_path}")
        logger.info(f"Analyzed {len(aco_files)} ACOs with {total_files} files")
        return True

    except Exception as e:  # ALLOWED: Returns False to indicate error as part of API contract
        logger.error(f"Failed to write ACO metadata documentation: {e}")
        return False
