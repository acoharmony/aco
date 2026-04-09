# © 2025 HarmonyCares
# All rights reserved.

"""
Private implementation of expression generation system.

This module contains the reorganized expression builders split into
logical components for better maintainability. All expression builders
are registered through a decorator-based registry system.

The public API is exposed through the main _expressions module.
"""

# Quality measure implementations moved to _transforms/
from ._bene_mbi_map import BeneficiaryMbiMappingExpression

# Import CCLF-specific expressions
from ._cclf_adr import CclfAdrExpression
from ._cclf_claim_filters import (
    CclfClaimFilterExpression,
    CclfRevenueCenterValidationExpression,
)
from ._ccsr import CcsrExpression
from ._chronic_conditions import ChronicConditionsExpression

# Import all expression builders to register them
from ._claim_id_match import ClaimIdMatchExpression
from ._cms_hcc import CmsHccExpression
from ._ed_classification import EdClassificationExpression
from ._ent_xwalk import EnterpriseCrosswalkExpression

# Import intermediate expressions
from ._ffs_first_dates import FfsFirstDatesExpression
from ._file_version import FileVersionExpression
from ._financial_pmpm import FinancialPmpmExpression
from ._last_ffs_service import LastFfsServiceExpression
from ._provider_alignment import ProviderAlignmentExpression
from ._provider_attribution import ProviderAttributionExpression

# Quality measure framework moved to _transforms/
from ._quality_measures import QualityMeasuresExpression
from ._reach_bnmr_multi_table import ReachBnmrMultiTableExpression
from ._readmissions import ReadmissionsExpression

# Import enhanced analytics expressions
from ._registry import ExpressionRegistry, register_expression
from ._response_code_parser import ResponseCodeParserExpression
from ._risk_stratification import RiskStratificationExpression
from ._service_category import ServiceCategoryExpression
from ._signature_lifecycle import SignatureLifecycleExpression
from ._voluntary_alignment import VoluntaryAlignmentExpression

__all__ = [
    "ExpressionRegistry",
    "register_expression",
    # CCLF-specific expressions
    "CclfAdrExpression",
    "CclfClaimFilterExpression",
    "CclfRevenueCenterValidationExpression",
    # Individual expression classes
    "BeneficiaryMbiMappingExpression",
    "ClaimIdMatchExpression",
    "EnterpriseCrosswalkExpression",
    "FfsFirstDatesExpression",
    "FileVersionExpression",
    "LastFfsServiceExpression",
    "VoluntaryAlignmentExpression",
    "ProviderAlignmentExpression",
    "ProviderAttributionExpression",
    "ResponseCodeParserExpression",
    "SignatureLifecycleExpression",
    "ReachBnmrMultiTableExpression",
    # Tuva-derived expressions
    "CmsHccExpression",
    "ReadmissionsExpression",
    "ChronicConditionsExpression",
    "FinancialPmpmExpression",
    "QualityMeasuresExpression",
    "ServiceCategoryExpression",
    "EdClassificationExpression",
    "CcsrExpression",
    "RiskStratificationExpression",
]
