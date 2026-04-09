# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for pfs_rates schema.

Generated from: _schemas/pfs_rates.yml
"""

from datetime import date

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
    with_transform,
)


@register_schema(
    name="pfs_rates",
    version=2,
    tier="gold",
    description="Medicare Physician Fee Schedule payment rates by HCPCS code and office location with year-over-year comparison",
    file_patterns={"default": "pfs_rates_*.parquet"},
)
@with_parser(type="parquet", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_transform()
@with_storage(
    tier="gold",
    file_patterns={"default": "pfs_rates_*.parquet"},
    medallion_layer="gold",
    gold={
        "output_name": "pfs_rates.parquet",
        "refresh_frequency": "annual",
        "last_updated_by": "aco pipeline pfs_rates",
    },
)
@dataclass
class PfsRates:
    """
    Medicare Physician Fee Schedule payment rates by HCPCS code and office location with year-over-year comparison
    """

    hcpcs_code: str = Field(description="HCPCS/CPT procedure code")
    office_name: str = Field(description="Office location name from office_zip mapping")
    office_zip: str = Field(description="Office ZIP code (5-digit)")
    state: str = Field(description="Two-letter state code")
    locality: str = Field(description="Medicare locality code")
    year: int = Field(description="Payment year (e.g., 2026)")
    work_rvu: float = Field(description="Work Relative Value Unit")
    nf_pe_rvu: float = Field(description="Non-Facility Practice Expense RVU")
    mp_rvu: float = Field(description="Malpractice RVU")
    pw_gpci: float = Field(description="Physician Work Geographic Practice Cost Index")
    pe_gpci: float = Field(description="Practice Expense Geographic Practice Cost Index")
    mp_gpci: float = Field(description="Malpractice Geographic Practice Cost Index")
    conversion_factor: float = Field(description="Medicare conversion factor for the year")
    work_payment: float = Field(description="Work payment component (work_rvu × pw_gpci)")
    pe_payment: float = Field(
        description="Practice expense payment component (nf_pe_rvu × pe_gpci)"
    )
    mp_payment: float = Field(description="Malpractice payment component (mp_rvu × mp_gpci)")
    total_rvu_adjusted: float = Field(description="Sum of geographically adjusted RVUs")
    payment_rate: float = Field(
        description="Final payment amount (total_rvu_adjusted × conversion_factor)"
    )
    facility_type: str = Field(description="Facility setting (non_facility, facility, both)")
    calculation_date: date | None = Field(description="Timestamp when rates were calculated")
    hcpcs_description: str | None = Field(
        default=None, description="HCPCS code description from PPRVU file"
    )
    carrier: str | None = Field(default=None, description="Medicare carrier number")
    geo_locality_name: str | None = Field(
        default=None, description="Geographic locality name from GPCI file"
    )
    f_pe_rvu: float | None = Field(
        default=None, description="Facility Practice Expense RVU (for future use)"
    )
    prior_year: int | None = Field(default=None, description="Previous year for comparison")
    prior_work_rvu: float | None = Field(default=None, description="Prior year work RVU")
    prior_nf_pe_rvu: float | None = Field(
        default=None, description="Prior year non-facility PE RVU"
    )
    prior_mp_rvu: float | None = Field(default=None, description="Prior year malpractice RVU")
    prior_conversion_factor: float | None = Field(
        default=None, description="Prior year conversion factor"
    )
    prior_payment_rate: float | None = Field(
        default=None, description="Prior year final payment amount"
    )
    rate_change_dollars: float | None = Field(
        default=None, description="Dollar change from prior year (current - prior)"
    )
    rate_change_percent: float | None = Field(
        default=None, description="Percent change from prior year ((current - prior) / prior × 100)"
    )
    market: str | None = Field(default=None, description="Market name from office_zip")
    region_name: str | None = Field(default=None, description="Region name from office_zip")
