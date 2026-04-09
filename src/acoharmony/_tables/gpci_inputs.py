# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for gpci_inputs schema.

Generated from: _schemas/gpci_inputs.yml
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
    name="gpci_inputs",
    version=2,
    tier="bronze",
    description="ADDENDUM E. FINAL CY 2026 GEOGRAPHIC PRACTICE COST INDICES (GPCIs) BY STATE AND MEDICARE LOCALITY",
    file_patterns={"annual": "*GPCI*.csv", "annual_xlsx": "*GPCI*.xlsx"},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_transform()
@with_storage(
    tier="bronze",
    file_patterns={"annual": "*GPCI*.csv", "annual_xlsx": "*GPCI*.xlsx"},
    medallion_layer="bronze",
    silver={
        "output_name": "gpci_inputs.parquet",
        "refresh_frequency": "annual",
        "last_updated_by": "aco transform gpci_inputs",
    },
)
@dataclass
class GpciInputs:
    """
    ADDENDUM E. FINAL CY 2026 GEOGRAPHIC PRACTICE COST INDICES (GPCIs) BY STATE AND MEDICARE LOCALITY
    """

    geo_locality_state_cd: str = Field(description="Geographic Locality State Code")
    geo_locality_num: str = Field(description="Geographic Locality Number")
    medicare_administrative_contractor_id: str | None = Field(
        default=None, description="Medicare Administrative Contractor (MAC) ID"
    )
    geo_locality_name: str | None = Field(default=None, description="Geographic Locality Name")
    pw_gpci: float | None = Field(default=None, description="Physician Work GPCI")
    pe_gpci: float | None = Field(default=None, description="Practice Expense GPCI")
    pe_mp_gpci: float | None = Field(default=None, description="Malpractice GPCI")
