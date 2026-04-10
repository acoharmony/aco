# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for gaf_inputs schema.

Generated from: _schemas/gaf_inputs.yml
"""

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
)


@register_schema(
    name="gaf_inputs",
    version=2,
    tier="bronze",
    description="ADDENDUM D. FINAL CY 20XX GEOGRAPHIC ADJUSTMENT FACTORS (GAFs) BY STATE AND MEDICARE LOCALITY",
    file_patterns={"annual": "*GAF*.csv", "annual_xlsx": "*GAF*.xlsx"},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_storage(
    tier="bronze",
    file_patterns={"annual": "*GAF*.csv", "annual_xlsx": "*GAF*.xlsx"},
    medallion_layer="bronze",
    silver={
        "output_name": "gaf_inputs.parquet",
        "refresh_frequency": "annual",
        "last_updated_by": "aco transform gaf_inputs",
    },
)
@dataclass
class GafInputs:
    """
    ADDENDUM D. FINAL CY 20XX GEOGRAPHIC ADJUSTMENT FACTORS (GAFs) BY STATE AND MEDICARE LOCALITY
    """

    geo_locality_state_cd: str = Field(description="Geographic Locality State Code")
    geo_locality_num: str = Field(description="Geographic Locality Number")
    medicare_administrative_contractor_id: str | None = Field(
        default=None, description="Medicare Administrative Contractor (MAC) ID"
    )
    geo_locality_name: str | None = Field(default=None, description="Geographic Locality Name")
    cy_gaf_with_work_floor: float | None = Field(
        default=None, description="Current Year GAF with Work Floor"
    )
    cy_plus_one_gaf_with_work_floor: float | None = Field(
        default=None, description="Current Year Plus One GAF with Work Floor"
    )
    inter_year_percent_change: float | None = Field(
        default=None, description="Inter-Year Percent Change"
    )
