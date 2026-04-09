# © 2025 HarmonyCares
# All rights reserved.

"""
CMS Public Use Files (PUF) Module.

This module provides inventory management and batch processing capabilities for
CMS public use files, with a focus on Physician Fee Schedule (PFS) data.

Key Features:
- Structured inventory of CMS PFS rules and data files (2002-present)
- Integration with _cite module for automated downloads
- Pydantic models for data validation
- Batch processing utilities
- Progress tracking and reporting

Main Components:
- models: Pydantic models for metadata validation
- pfs_inventory: PFS-specific inventory and query functions
- utils: Batch processing and validation utilities

Example Usage:
    >>> from acoharmony._puf import pfs_inventory, utils
    >>>
    >>> # Get all files for 2024 Final Rule
    >>> tasks = pfs_inventory.create_download_tasks("2024", rule_type="Final")
    >>>
    >>> # Batch download
    >>> results = utils.batch_download(tasks)
    >>>
    >>> # Search for specific files
    >>> gpci_files = pfs_inventory.get_files_by_category("gpci")
    >>>
    >>> # Get files by schema mapping
    >>> rvu_files = pfs_inventory.get_files_by_schema("pprvu_inputs")
"""

# Export models
from .models import (
    DatasetInventory,
    DownloadTask,
    FileCategory,
    FileFormat,
    FileMetadata,
    RuleMetadata,
    RuleType,
    YearInventory,
)

# Export pfs_inventory functions
from .pfs_inventory import (
    create_download_tasks,
    get_files_by_category,
    get_files_by_schema,
    get_files_for_year,
    get_inventory,
    get_latest_year,
    get_rule,
    get_year,
    list_available_years,
    load_inventory,
    search_files,
)

# Export state tracking
from .puf_state import PUFFileEntry, PUFInventoryState, PUFStateTracker

# Export unpack utilities
from .puf_unpack import make_puf_filename, unpack_puf_zips

# Export utils functions
from .utils import (
    batch_download,
    check_download_status,
    generate_download_manifest,
    get_corpus_files_for_year,
    validate_file_downloads,
)

__all__ = [
    # Models
    "DatasetInventory",
    "YearInventory",
    "RuleMetadata",
    "FileMetadata",
    "DownloadTask",
    "RuleType",
    "FileCategory",
    "FileFormat",
    # State tracking
    "PUFFileEntry",
    "PUFInventoryState",
    "PUFStateTracker",
    # Unpack utilities
    "unpack_puf_zips",
    "make_puf_filename",
    # Inventory functions
    "get_inventory",
    "load_inventory",
    "get_year",
    "get_rule",
    "get_files_for_year",
    "get_files_by_category",
    "get_files_by_schema",
    "create_download_tasks",
    "list_available_years",
    "get_latest_year",
    "search_files",
    # Utility functions
    "batch_download",
    "generate_download_manifest",
    "check_download_status",
    "validate_file_downloads",
    "get_corpus_files_for_year",
]
