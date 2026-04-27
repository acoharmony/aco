# © 2025 HarmonyCares
# All rights reserved.

"""
File transfer module: move files from bronze (or any local source) to a
destination directory, driven by per-use-case "transfer profiles."

A profile is a single Python class registered via ``@register_profile`` that
declares: which files to consider (source rule), where to put them
(destination + optional rename), and how to verify they made it
(verifier). Each profile lives as one file under ``_xfr/_profiles/`` to
mirror the ``_tables/`` pattern.

This module deliberately reuses everything else: schema patterns from
``acoharmony._registry``, log parsing from ``acoharmony._parsers._mabel_log``,
storage paths from ``acoharmony._store.StorageBackend``.
"""

from .profile import (
    CompositeRule,
    DirectoryVerifier,
    LiteralPatternRule,
    LogVerifier,
    MonthlyMatchRule,
    SchemaPatternRule,
    SourceRule,
    TransferProfile,
    Verifier,
    list_profiles,
    register_profile,
    resolve_profile,
)
from .selector import select_files
from .state import XfrStateTracker
from .transfer import FileStatus, TransferRecord, send_pending

__all__ = [
    "CompositeRule",
    "DirectoryVerifier",
    "FileStatus",
    "LiteralPatternRule",
    "LogVerifier",
    "MonthlyMatchRule",
    "SchemaPatternRule",
    "SourceRule",
    "TransferProfile",
    "TransferRecord",
    "Verifier",
    "XfrStateTracker",
    "list_profiles",
    "register_profile",
    "resolve_profile",
    "select_files",
    "send_pending",
]
