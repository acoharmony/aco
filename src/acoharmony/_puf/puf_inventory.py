# © 2025 HarmonyCares
# All rights reserved.

"""
Unified PUF inventory loader supporting multiple CMS datasets.

Supports:
- PFS (Physician Fee Schedule) rule files
- RVU (Relative Value Unit) quarterly files

"""

from __future__ import annotations

from pathlib import Path

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

# Module-level cache for loaded inventories
_INVENTORY_CACHE: dict[str, DatasetInventory] = {}

# Available dataset keys and their YAML files
DATASETS = {
    "pfs": "pfs_data.yaml",  # Physician Fee Schedule rule files
    "rvu": "rvu_data.yaml",  # Relative Value Unit quarterly files
    "zipcarrier": "zipcarrier_data.yaml",  # ZIP code carrier locality files
}


def get_data_file_path(dataset_key: str) -> Path:
    """
    Get path to dataset YAML file.

    Args:
        dataset_key: Dataset identifier ("pfs" or "rvu")

    Returns:
        Path to YAML data file

    Raises:
        ValueError: If dataset_key is not recognized
    """
    if dataset_key not in DATASETS:
        raise ValueError(
            f"Unknown dataset: {dataset_key}. Available: {list(DATASETS.keys())}"
        )

    return Path(__file__).parent / DATASETS[dataset_key]


def load_dataset(dataset_key: str, force_reload: bool = False) -> DatasetInventory:
    """
    Load dataset inventory from YAML file with caching.

    Args:
        dataset_key: Dataset identifier ("pfs" or "rvu")
        force_reload: If True, reload from file even if cached

    Returns:
        DatasetInventory object validated by Pydantic

    Raises:
        FileNotFoundError: If YAML file not found
        ValueError: If YAML is invalid or doesn't match schema
    """
    global _INVENTORY_CACHE

    # Return cached if available
    if dataset_key in _INVENTORY_CACHE and not force_reload:
        return _INVENTORY_CACHE[dataset_key]

    # Load from YAML
    data_file = get_data_file_path(dataset_key)

    if not data_file.exists():
        raise FileNotFoundError(f"Dataset file not found: {data_file}")

    with open(data_file) as f:
        raw_data = yaml.safe_load(f)

    # Parse and validate with Pydantic
    years = {}
    for year_str, year_data in raw_data.get("years", {}).items():
        rules = {}
        for rule_key, rule_data in year_data.get("rules", {}).items():
            files = {}
            for file_key, file_data in rule_data.get("files", {}).items():
                # Move quarter field to metadata if present (for RVU files)
                file_dict = dict(file_data)
                if "quarter" in file_dict:
                    if "metadata" not in file_dict:
                        file_dict["metadata"] = {}
                    file_dict["metadata"]["quarter"] = file_dict.pop("quarter")

                files[file_key] = FileMetadata(**file_dict)

            rules[rule_key] = RuleMetadata(
                rule_type=rule_data.get("rule_type", "Final"),
                metadata=rule_data.get("metadata", {}),
                files=files,
            )

        years[year_str] = YearInventory(
            year=year_str,
            metadata=year_data.get("metadata", {}),
            rules=rules,
        )

    inventory = DatasetInventory(
        dataset_name=raw_data.get("dataset_name", "Unknown Dataset"),
        dataset_key=dataset_key,
        description=raw_data.get("description", ""),
        base_url=raw_data.get("base_url", ""),
        years=years,
    )

    # Cache and return
    _INVENTORY_CACHE[dataset_key] = inventory
    return inventory


def load_all_datasets() -> dict[str, DatasetInventory]:
    """
    Load all available datasets.

    Returns:
        Dictionary mapping dataset_key to DatasetInventory
    """
    return {key: load_dataset(key) for key in DATASETS.keys()}


def create_download_tasks(
    dataset_key: str,
    year: str | None = None,
    rule_type: str | RuleType | None = None,
    category: str | FileCategory | None = None,
    quarter: str | None = None,
    priority: int = 5,
    force_refresh: bool = False,
    tags: list[str] | None = None,
) -> list[DownloadTask]:
    """
    Create download tasks for files in a dataset.

    Args:
        dataset_key: Dataset identifier ("pfs" or "rvu")
        year: Optional year filter
        rule_type: Optional rule type filter (PFS only)
        category: Optional category filter
        quarter: Optional quarter filter ("A", "B", "C", "D" - RVU only)
        priority: Task priority (1-10, lower = higher priority)
        force_refresh: Force re-download even if exists
        tags: Optional list of tags for the tasks

    Returns:
        List of DownloadTask objects ready for processing
    """
    inventory = load_dataset(dataset_key)
    tasks = []

    if tags is None:
        tags = []

    # Add dataset tag
    tags = ["puf", dataset_key] + tags

    for year_str, year_inv in inventory.years.items():
        # Apply year filter
        if year and year_str != year:
            continue

        # Process rules (works for both PFS and RVU)
        for _rule_key, rule_meta in year_inv.rules.items():
            # Apply rule_type filter
            if rule_type:
                rt = rule_type.value if isinstance(rule_type, RuleType) else rule_type
                if rule_meta.rule_type != rt:
                    continue

            for _file_key, file_meta in rule_meta.files.items():
                # Apply quarter filter (for RVU files)
                if quarter:
                    file_quarter = file_meta.metadata.get("quarter") if file_meta.metadata else None
                    if file_quarter != quarter:
                        continue

                # Apply category filter
                if category:
                    cat = category.value if isinstance(category, FileCategory) else category
                    file_cat = (
                        file_meta.category.value
                        if hasattr(file_meta.category, "value")
                        else str(file_meta.category)
                    )
                    if file_cat != cat:
                        continue

                # Create task
                task = DownloadTask(
                    file_metadata=file_meta,
                    year=year_str,
                    rule_type=rule_meta.rule_type,
                    priority=priority,
                    force_refresh=force_refresh,
                    tags=tags + [year_str, rule_meta.rule_type.lower()],
                )
                tasks.append(task)

    return tasks


def list_available_datasets() -> dict[str, str]:
    """
    List all available datasets with descriptions.

    Returns:
        Dictionary mapping dataset_key to description
    """
    result = {}
    for dataset_key in DATASETS.keys():
        try:
            inv = load_dataset(dataset_key)
            result[dataset_key] = inv.description
        except Exception as e:
            result[dataset_key] = f"Error loading: {e}"

    return result
