# © 2025 HarmonyCares
# All rights reserved.

"""Inventory/local-file comparison helpers for ACOMS.

The comparison mechanics are intentionally shared with the 4icli workflow:
both integrations maintain an inventory JSON, scan bronze/archive by filename,
and persist a not-downloaded state file for the follow-up download command.
"""

from __future__ import annotations

from collections import Counter
from typing import Any

from acoharmony._4icli.comparison import (
    export_to_csv,
    format_size,
    save_not_downloaded_state,
    scan_directory,
)

from .inventory import InventoryResult


def compare_inventory(
    inventory: InventoryResult,
    local_files: set[str],
    year_filter: int | None = None,
    category_filter: str | None = None,
    state_tracker: Any | None = None,
) -> dict[str, Any]:
    """Compare ACOMS inventory to local files with optional state tracking."""
    filtered_files = inventory.files
    if year_filter:
        filtered_files = [file for file in filtered_files if file.year == year_filter]
    if category_filter:
        filtered_files = [file for file in filtered_files if file.category == category_filter]

    hash_tracked_files = set()
    if state_tracker:
        hash_tracked_files = {state.filename for state in state_tracker._file_cache.values()}

    all_known_files = local_files | hash_tracked_files
    have = []
    missing = []
    for file_entry in filtered_files:
        if file_entry.filename in all_known_files:
            have.append(file_entry)
        else:
            missing.append(file_entry)

    total_size = sum(file.size_bytes for file in missing if file.size_bytes is not None)
    missing_by_year = Counter(file.year for file in missing)
    missing_by_category = Counter(file.category for file in missing)
    missing_by_type_code = Counter(
        file.file_type_code for file in missing if file.file_type_code is not None
    )

    return {
        "total_inventory": len(filtered_files),
        "have": have,
        "missing": missing,
        "have_count": len(have),
        "missing_count": len(missing),
        "total_size_bytes": total_size,
        "missing_by_year": dict(missing_by_year),
        "missing_by_category": dict(missing_by_category),
        "missing_by_type_code": dict(missing_by_type_code),
    }


__all__ = [
    "compare_inventory",
    "export_to_csv",
    "format_size",
    "save_not_downloaded_state",
    "scan_directory",
]
