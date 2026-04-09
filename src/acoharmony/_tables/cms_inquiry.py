# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for cms_inquiry schema.

Generated from: _schemas/cms_inquiry.yml
"""

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
    with_transform,
)


@register_schema(
    name="cms_inquiry",
    version=2,
    tier="bronze",
    description="CMS template for submissions to help desk that require claims data",
)
@with_parser(type="excel", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(tier="bronze", medallion_layer="bronze")
@dataclass
class CmsInquiry:
    """
    CMS template for submissions to help desk that require claims data
    """

    beneficiary_mbi: str = Field(description="Medicare Beneficiary Identifier")
    beneficiary_first_name: str | None = Field(default=None, description="Beneficiary First Name")
    beneficiary_last_name: str | None = Field(default=None, description="Beneficiary Last Name")
    beneficiary_zip_5: str | None = Field(default=None, description="5-digit ZIP code")
    rndrg_prvdr_npi_num: str | None = Field(
        default=None,
        description="Rendering Provider NPI Number - NPI of provider rendering service from PECOS",
    )
    clm_rndrg_prvdr_tax_num: str | None = Field(
        default=None,
        description="Claim Provider Tax Number - SSN or EIN of provider receiving payment",
    )
    org_npi: str | None = Field(
        default=None, description="Organizational National Provider Identifier"
    )
    ccn: str | None = Field(default=None, description="CMS Certification Number")
    clm_cntl_num: str | None = Field(
        default=None,
        description="Claim Control Number - Unique number assigned by Medicare carrier",
    )
    clm_src: str | None = Field(
        default=None, description="claim source (full CCLF file name/pattern)"
    )
    clm_type_cd: str | None = Field(
        default=None,
        description="Claim Type Code - Type of claim (71=RIC O local carrier non-DMEPOS, 72=RIC O local carrier DMEPOS)",
    )
