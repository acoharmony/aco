# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass models for all table schemas.

Generated from YAML schemas in _schemas/.

Each model provides:
- Runtime type validation with Pydantic
- Field-level validators for data quality
- Complete metadata from YAML
- IDE autocomplete and type checking
"""

from .aco_alignment import AcoAlignment
from .aco_financial_guarantee_amount import AcoFinancialGuaranteeAmount
from .alr import Alr
from .alternative_payment_arrangement_report import AlternativePaymentArrangementReport
from .annual_beneficiary_level_quality_report import AnnualBeneficiaryLevelQualityReport
from .annual_quality_report import AnnualQualityReport
from .bar import Bar
from .beneficiary_data_sharing_exclusion_file import BeneficiaryDataSharingExclusionFile
from .beneficiary_demographics import BeneficiaryDemographics
from .beneficiary_hedr_transparency_files import BeneficiaryHedrTransparencyFiles
from .beneficiary_xref import BeneficiaryXref
from .blqqr_acr import BlqqrAcr
from .blqqr_dah import BlqqrDah
from .blqqr_exclusions import BlqqrExclusions
from .blqqr_uamcc import BlqqrUamcc
from .bnex import Bnex
from .cclf0 import Cclf0
from .cclf1 import Cclf1
from .cclf2 import Cclf2
from .cclf3 import Cclf3
from .cclf4 import Cclf4
from .cclf5 import Cclf5
from .cclf6 import Cclf6
from .cclf7 import Cclf7
from .cclf8 import Cclf8
from .cclf9 import Cclf9
from .cclf_management_report import CclfManagementReport
from .cclfa import Cclfa
from .cclfb import Cclfb
from .census import Census
from .cms_geo_zips import CmsGeoZips
from .cms_inquiry import CmsInquiry
from .consolidated_alignment import ConsolidatedAlignment
from .eligibility import Eligibility
from .email_unsubscribes import EmailUnsubscribes
from .emails import Emails
from .engagement import Engagement
from .enrollment import Enrollment
from .enterprise_crosswalk import EnterpriseCrosswalk
from .estimated_cisep_change_threshold_report import EstimatedCisepChangeThresholdReport
from .ffs_first_dates import FfsFirstDates
from .gaf_inputs import GafInputs
from .gcm import Gcm
from .gpci_inputs import GpciInputs
from .hcmpi_master import HcmpiMaster
from .hdai_reach import HdaiReach
from .last_ffs_service import LastFfsService
from .mailed import Mailed
from .mbi_crosswalk import MbiCrosswalk
from .mexpr import Mexpr
from .needs_signature import NeedsSignature
from .office_zip import OfficeZip
from .palmr import Palmr
from .participant_list import ParticipantList
from .pbvar import Pbvar
from .pe_inputs_equipment import PeInputsEquipment
from .pe_inputs_labor import PeInputsLabor
from .pe_inputs_supplies import PeInputsSupplies
from .pe_summary import PeSummary
from .pecos_terminations_monthly_report import PecosTerminationsMonthlyReport
from .pfs_rates import PfsRates
from .plaru import Plaru
from .pprvu_inputs import PprvuInputs
from .preliminary_alignment_estimate import PreliminaryAlignmentEstimate
from .preliminary_alternative_payment_arrangement_report_156 import (
    PreliminaryAlternativePaymentArrangementReport156,
)
from .preliminary_benchmark_report_for_dc import PreliminaryBenchmarkReportForDc
from .preliminary_benchmark_report_unredacted import PreliminaryBenchmarkReportUnredacted
from .prospective_plus_opportunity_report import ProspectivePlusOpportunityReport
from .pyred import Pyred
from .quarterly_beneficiary_level_quality_report import QuarterlyBeneficiaryLevelQualityReport
from .quarterly_quality_report import QuarterlyQualityReport
from .rap import Rap
from .reach_bnmr import ReachBnmr
from .recon import Recon
from .rel_patient_program import RelPatientProgram
from .risk_adjustment_data import RiskAdjustmentData
from .salesforce_account import SalesforceAccount
from .sbmdm import Sbmdm
from .sbmepi import Sbmepi
from .sbmhh import Sbmhh
from .sbmhs import Sbmhs
from .sbmip import Sbmip
from .sbmopl import Sbmopl
from .sbmpb import Sbmpb
from .sbmsn import Sbmsn
from .sbnabp import Sbnabp
from .sbqr import Sbqr
from .shadow_bundle_reach import ShadowBundleReach
from .sva import Sva
from .sva_submissions import SvaSubmissions
from .tparc import Tparc
from .voluntary_alignment import VoluntaryAlignment
from .vwyearmo_engagement import VwyearmoEngagement
from .zip_to_county import ZipToCounty

__all__ = [
    "AcoAlignment",
    "AcoFinancialGuaranteeAmount",
    "Alr",
    "AlternativePaymentArrangementReport",
    "BlqqrAcr",
    "BlqqrDah",
    "BlqqrExclusions",
    "BlqqrUamcc",
    "AnnualBeneficiaryLevelQualityReport",
    "AnnualQualityReport",
    "Bar",
    "BeneficiaryDataSharingExclusionFile",
    "BeneficiaryDemographics",
    "BeneficiaryHedrTransparencyFiles",
    "BeneficiaryXref",
    "Cclf0",
    "Cclf1",
    "Cclf2",
    "Cclf3",
    "Cclf4",
    "Cclf5",
    "Cclf6",
    "Cclf7",
    "Cclf8",
    "Cclf9",
    "CclfManagementReport",
    "Cclfa",
    "Cclfb",
    "Census",
    "ConsolidatedAlignment",
    "Eligibility",
    "EmailUnsubscribes",
    "Emails",
    "Engagement",
    "Enrollment",
    "EnterpriseCrosswalk",
    "EstimatedCisepChangeThresholdReport",
    "FfsFirstDates",
    "HcmpiMaster",
    "HdaiReach",
    "LastFfsService",
    "Mailed",
    "MbiCrosswalk",
    "Mexpr",
    "NeedsSignature",
    "OfficeZip",
    "Palmr",
    "ParticipantList",
    "Pbvar",
    "PecosTerminationsMonthlyReport",
    "Plaru",
    "PreliminaryAlignmentEstimate",
    "PreliminaryAlternativePaymentArrangementReport156",
    "PreliminaryBenchmarkReportForDc",
    "PreliminaryBenchmarkReportUnredacted",
    "ProspectivePlusOpportunityReport",
    "ProviderList",
    "Pyred",
    "QuarterlyBeneficiaryLevelQualityReport",
    "QuarterlyQualityReport",
    "Rap",
    "ReachBnmr",
    "Recon",
    "RelPatientProgram",
    "RiskAdjustmentData",
    "SalesforceAccount",
    "Sbnabp",
    "Sbqr",
    "ShadowBundleReach",
    "Sva",
    "SvaSubmissions",
    "Tparc",
    "VoluntaryAlignment",
    "VwyearmoEngagement",
    "ZipToCounty",
    "Bnex",
    "CmsGeoZips",
    "CmsInquiry",
    "GafInputs",
    "Gcm",
    "GpciInputs",
    "PeInputsEquipment",
    "PeInputsLabor",
    "PeInputsSupplies",
    "PeSummary",
    "PfsRates",
    "PprvuInputs",
    "Sbmdm",
    "Sbmepi",
    "Sbmhh",
    "Sbmhs",
    "Sbmip",
    "Sbmopl",
    "Sbmpb",
    "Sbmsn",
]
