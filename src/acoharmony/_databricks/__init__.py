# © 2025 HarmonyCares
# All rights reserved.

"""
Databricks integration for ACO Harmony.

Provides tools for transferring parquet files from silver/gold layers
to Databricks-compatible formats with tracking and change detection.
"""

from ._transfer import DatabricksTransferManager

__all__ = ["DatabricksTransferManager"]
