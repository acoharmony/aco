"""Schema crosswalks for mapping between different data sources.

This module provides YAML-based schema crosswalks that serve as reference documentation
for column mappings and data type information between source and target schemas.

Example:
    >>> from acoharmony._crosswalks import load_crosswalk
    >>> crosswalk = load_crosswalk('bar_to_crr')
    >>> print(crosswalk['crosswalk']['target']['table'])
    beneficiaryalignmentreportmonthlydata
"""

from pathlib import Path
from typing import Any

import yaml

__all__ = [
    'load_crosswalk',
    'get_mapping',
    'get_mappings',
    'list_crosswalks',
    'get_target_info',
    'get_source_info',
]


def _get_crosswalks_dir() -> Path:
    """Get the directory containing crosswalk YAML files."""
    return Path(__file__).parent


def list_crosswalks() -> list[str]:
    """List all available crosswalk configurations.

    Returns:
        List of crosswalk names (without .yaml extension)
    """
    crosswalks_dir = _get_crosswalks_dir()
    return [
        f.stem for f in crosswalks_dir.glob("*.yaml")
        if not f.name.startswith('_')
    ]


def load_crosswalk(name: str) -> dict[str, Any]:
    """Load a crosswalk configuration from YAML.

    Args:
        name: Name of the crosswalk (without .yaml extension)

    Returns:
        Dictionary containing the crosswalk configuration

    Raises:
        FileNotFoundError: If the crosswalk file doesn't exist
        yaml.YAMLError: If the YAML is invalid
    """
    crosswalk_path = _get_crosswalks_dir() / f"{name}.yaml"

    if not crosswalk_path.exists():
        available = list_crosswalks()
        raise FileNotFoundError(
            f"Crosswalk '{name}' not found. Available crosswalks: {', '.join(available)}"
        )

    with open(crosswalk_path) as f:
        return yaml.safe_load(f)


def get_mapping(crosswalk_name: str, source_column: str) -> dict[str, Any] | None:
    """Get the mapping for a specific source column.

    Args:
        crosswalk_name: Name of the crosswalk
        source_column: Source column name to look up

    Returns:
        Mapping dictionary if found, None otherwise
    """
    crosswalk = load_crosswalk(crosswalk_name)
    mappings = crosswalk['crosswalk']['mappings']

    for mapping in mappings:
        if mapping['source_column'] == source_column:
            return mapping

    return None


def get_mappings(crosswalk_name: str) -> list[dict[str, Any]]:
    """Get all column mappings from a crosswalk.

    Args:
        crosswalk_name: Name of the crosswalk

    Returns:
        List of mapping dictionaries
    """
    crosswalk = load_crosswalk(crosswalk_name)
    return crosswalk['crosswalk'].get('mappings', [])


def get_target_info(crosswalk_name: str) -> dict[str, Any]:
    """Get target schema information.

    Args:
        crosswalk_name: Name of the crosswalk

    Returns:
        Dictionary containing target schema information
    """
    crosswalk = load_crosswalk(crosswalk_name)
    return crosswalk['crosswalk']['target']


def get_source_info(crosswalk_name: str) -> dict[str, Any]:
    """Get source schema information.

    Args:
        crosswalk_name: Name of the crosswalk

    Returns:
        Dictionary containing source schema information
    """
    crosswalk = load_crosswalk(crosswalk_name)
    return crosswalk['crosswalk']['source']
