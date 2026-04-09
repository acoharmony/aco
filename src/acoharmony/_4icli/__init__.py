# © 2025 HarmonyCares
# All rights reserved.

"""
4Innovation DataHub CLI integration for ACO Harmony.

 a Python interface to the 4icli binary for downloading
ACO REACH program files from the 4Innovation DataHub API. The 4icli tool
provides access to three main categories:

- Beneficiary List files
- CCLF (Claim and Claim Line Feed) files
- Reports (including various report types)

The integration allows acoharmony to programmatically download and process
ACO REACH data files while maintaining audit trails and error handling.

"""

from .client import FourICLI
from .config import FourICLIConfig
from .inventory import FileInventoryEntry, InventoryDiscovery, InventoryResult
from .models import DataHubCategory, DataHubQuery, DateFilter, DownloadResult, FileTypeCode
from .state import FileDownloadState, FourICLIStateTracker

__all__ = [
    "FourICLI",
    "FourICLIConfig",
    "DataHubCategory",
    "FileTypeCode",
    "DownloadResult",
    "DateFilter",
    "DataHubQuery",
    "FourICLIStateTracker",
    "FileDownloadState",
    "InventoryDiscovery",
    "InventoryResult",
    "FileInventoryEntry",
]
