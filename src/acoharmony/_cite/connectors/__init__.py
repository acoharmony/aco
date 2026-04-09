# © 2025 HarmonyCares
# All rights reserved.

"""
Citation connectors for domain-specific sources.

Connectors are specialized handlers for specific citation sources
(CMS, Federal Register, eCFR, arXiv, PubMed, etc.) that understand domain-specific structure.
"""

from __future__ import annotations

from ._cms import CMSConnector, IOMHandler, PFSHandler
from ._ecfr import ECFRConnector
from ._federal_register import FederalRegisterConnector

__all__ = [
    "CMSConnector",
    "IOMHandler",
    "PFSHandler",
    "FederalRegisterConnector",
    "ECFRConnector",
]
