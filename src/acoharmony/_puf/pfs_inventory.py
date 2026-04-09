# © 2025 HarmonyCares
# All rights reserved.

"""
Physician Fee Schedule (PFS) Public Use Files inventory loader and helpers.

This module loads PFS inventory from pfs_data.yaml and provides convenient
access functions for querying and batch processing PFS files.

Integration with _cite module:
- Generates DownloadTask objects for batch downloading
- Maps files to schema definitions in _schemas
- Provides filtering and search capabilities

"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .models import (
    DatasetInventory,
    DownloadTask,
    FileCategory,
    FileMetadata,
    RuleMetadata,
    RuleType,
    YearInventory,
)

# Module-level cache for loaded inventory
_INVENTORY_CACHE: DatasetInventory | None = None


def get_data_file_path() -> Path:
    """
    Get path to pfs_data.yaml file.

    Returns:
        Path to YAML data file
    """
    return Path(__file__).parent / "pfs_data.yaml"


def load_inventory(force_reload: bool = False) -> DatasetInventory:
    """
    Load PFS inventory from YAML file with caching.

    Args:
        force_reload: If True, reload from file even if cached

    Returns:
        DatasetInventory object validated by Pydantic

    Raises:
        FileNotFoundError: If pfs_data.yaml not found
        ValueError: If YAML is invalid or doesn't match schema
    """
    global _INVENTORY_CACHE

    # Return cached if available
    if _INVENTORY_CACHE is not None and not force_reload:
        return _INVENTORY_CACHE

    # Load from YAML
    yaml_path = get_data_file_path()
    if not yaml_path.exists():
        raise FileNotFoundError(f"PFS data file not found: {yaml_path}")

    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    # Convert to structured model
    inventory = _parse_yaml_to_inventory(data)

    # Cache and return
    _INVENTORY_CACHE = inventory
    return inventory


def _parse_yaml_to_inventory(data: dict[str, Any]) -> DatasetInventory:
    """
    Parse YAML data into DatasetInventory with validation.

    Args:
        data: Raw YAML data

    Returns:
        Validated DatasetInventory
    """
    # Build year inventories
    years_dict = {}

    for year_str, year_data in data.get("years", {}).items():
        # Build rules for this year
        rules_dict = {}

        for rule_type_str, rule_data in year_data.items():
            # Skip if not a valid rule type
            if rule_type_str not in ["Proposed", "Final", "Correction", "Interim Final"]:
                continue

            # Build files for this rule
            files_dict = {}
            for file_key, file_data in rule_data.get("files", {}).items():
                file_meta = FileMetadata(
                    key=file_key,
                    url=file_data["url"],
                    category=file_data.get("category", "other"),
                    format=file_data.get("format"),
                    description=file_data.get("description"),
                    schema_mapping=file_data.get("schema_mapping"),
                )
                files_dict[file_key] = file_meta

            # Build rule metadata
            rule_meta = RuleMetadata(
                year=rule_data.get("year", f"CY {year_str}"),
                rule_type=RuleType(rule_type_str),
                citation=rule_data.get("citation"),
                doc_id=rule_data.get("doc_id"),
                link=rule_data.get("link"),
                xml=rule_data.get("xml"),
                files=files_dict,
            )
            rules_dict[rule_type_str] = rule_meta

        # Build year inventory
        year_inv = YearInventory(year=year_str, rules=rules_dict)
        years_dict[year_str] = year_inv

    # Build dataset inventory
    inventory = DatasetInventory(
        dataset_name=data.get("dataset_name", "Medicare Physician Fee Schedule"),
        dataset_key=data.get("dataset_key", "pfs"),
        source_agency=data.get("source_agency", "CMS"),
        description=data.get("description"),
        years=years_dict,
    )

    return inventory


def get_inventory() -> DatasetInventory:
    """
    Get loaded PFS inventory (convenience wrapper).

    Returns:
        DatasetInventory object
    """
    return load_inventory()


def get_year(year: str) -> YearInventory | None:
    """
    Get inventory for a specific year.

    Args:
        year: Year string (e.g., "2024")

    Returns:
        YearInventory or None if not found
    """
    inventory = get_inventory()
    return inventory.get_year(year)


def get_rule(year: str, rule_type: str | RuleType) -> RuleMetadata | None:
    """
    Get specific rule metadata.

    Args:
        year: Year string
        rule_type: Rule type (Proposed, Final, Correction)

    Returns:
        RuleMetadata or None if not found
    """
    year_inv = get_year(year)
    if not year_inv:
        return None

    return year_inv.get_rule(rule_type)


def get_files_for_year(year: str, rule_type: str | RuleType | None = None) -> list[FileMetadata]:
    """
    Get all files for a year, optionally filtered by rule type.

    Args:
        year: Year string
        rule_type: Optional rule type filter

    Returns:
        List of FileMetadata
    """
    year_inv = get_year(year)
    if not year_inv:
        return []

    if rule_type:
        rule = year_inv.get_rule(rule_type)
        if not rule:
            return []
        return list(rule.files.values())

    # All files for year
    return year_inv.get_all_files()


def get_files_by_category(
    category: str | FileCategory, year: str | None = None
) -> list[tuple[str, str, FileMetadata]]:
    """
    Get all files matching a category, optionally filtered by year.

    Args:
        category: File category to filter by
        year: Optional year filter

    Returns:
        List of tuples: (year, rule_type, FileMetadata)
    """
    inventory = get_inventory()
    results = []

    # Convert category to enum if string
    if isinstance(category, str):
        try:
            category = FileCategory(category)
        except ValueError:
            return []

    # Filter years
    years_to_search = [year] if year else inventory.list_available_years()

    for year_str in years_to_search:
        year_inv = inventory.get_year(year_str)
        if not year_inv:
            continue

        for rule_type, rule in year_inv.rules.items():
            for file_meta in rule.files.values():
                if file_meta.category == category:
                    results.append((year_str, rule_type, file_meta))

    return results


def get_files_by_schema(schema_name: str) -> list[tuple[str, str, FileMetadata]]:
    """
    Get all files that map to a specific schema.

    Args:
        schema_name: Schema name from _schemas (e.g., "pprvu_inputs")

    Returns:
        List of tuples: (year, rule_type, FileMetadata)
    """
    inventory = get_inventory()
    results = []

    for year_str in inventory.list_available_years():
        year_inv = inventory.get_year(year_str)
        if not year_inv:
            continue

        for rule_type, rule in year_inv.rules.items():
            for file_meta in rule.files.values():
                if not file_meta.schema_mapping:
                    continue

                # Handle comma-separated schema mappings
                schemas = [s.strip() for s in file_meta.schema_mapping.split(",")]
                if schema_name in schemas:
                    results.append((year_str, rule_type, file_meta))

    return results


def create_download_tasks(
    year: str | None = None,
    rule_type: str | RuleType | None = None,
    category: str | FileCategory | None = None,
    priority: int = 5,
    force_refresh: bool = False,
    tags: list[str] | None = None,
) -> list[DownloadTask]:
    """
    Create download tasks for batch processing with _cite module.

    Args:
        year: Optional year filter
        rule_type: Optional rule type filter
        category: Optional category filter
        priority: Priority 1-10 (lower is higher)
        force_refresh: Force re-download if already cached
        tags: Additional tags for citation tracking

    Returns:
        List of DownloadTask objects ready for batch processing
    """
    inventory = get_inventory()
    tasks = []

    if tags is None:
        tags = []

    # Determine years to process
    years_to_process = [year] if year else inventory.list_available_years()

    for year_str in years_to_process:
        year_inv = inventory.get_year(year_str)
        if not year_inv:
            continue

        for rule_type_str, rule in year_inv.rules.items():
            # Apply rule type filter
            if rule_type and rule_type_str != str(rule_type):
                continue

            for file_meta in rule.files.values():
                # Apply category filter
                if category and file_meta.category != category:
                    continue

                task = DownloadTask(
                    file_metadata=file_meta,
                    year=year_str,
                    rule_type=RuleType(rule_type_str),
                    priority=priority,
                    force_refresh=force_refresh,
                    tags=tags,
                )
                tasks.append(task)

    # Sort by priority (lower number = higher priority)
    tasks.sort(key=lambda t: t.priority)

    return tasks


def list_available_years() -> list[str]:
    """
    List all years with PFS data.

    Returns:
        Sorted list of year strings
    """
    inventory = get_inventory()
    return inventory.list_available_years()


def get_latest_year() -> YearInventory | None:
    """
    Get inventory for most recent year.

    Returns:
        YearInventory for latest year or None
    """
    inventory = get_inventory()
    return inventory.get_latest_year()


def search_files(
    search_term: str, search_in: str = "all"
) -> list[tuple[str, str, str, FileMetadata]]:
    """
    Search for files by keyword in key, description, or category.

    Args:
        search_term: Term to search for (case-insensitive)
        search_in: Where to search - "key", "description", "category", or "all"

    Returns:
        List of tuples: (year, rule_type, file_key, FileMetadata)
    """
    inventory = get_inventory()
    results = []
    search_term_lower = search_term.lower()

    for year_str in inventory.list_available_years():
        year_inv = inventory.get_year(year_str)
        if not year_inv:
            continue

        for rule_type, rule in year_inv.rules.items():
            for file_key, file_meta in rule.files.items():
                match = False

                if search_in in ["key", "all"]:
                    if search_term_lower in file_key.lower():
                        match = True

                if search_in in ["description", "all"] and file_meta.description:
                    if search_term_lower in file_meta.description.lower():
                        match = True

                if search_in in ["category", "all"]:
                    category_str = file_meta.category.value if hasattr(file_meta.category, 'value') else str(file_meta.category)
                    if search_term_lower in category_str.lower():
                        match = True

                if match:
                    results.append((year_str, rule_type, file_key, file_meta))

    return results
