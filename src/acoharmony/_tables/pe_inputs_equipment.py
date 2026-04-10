# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for pe_inputs_equipment schema.

Generated from: _schemas/pe_inputs_equipment.yml
"""

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
)


@register_schema(
    name="pe_inputs_equipment",
    version=2,
    tier="bronze",
    description="Practice Expense Equipment Inputs - Equipment cost inputs for practice expense calculations",
    file_patterns={"annual": "*PE_EQUIPMENT*.csv", "annual_xlsx": "*PE_EQUIPMENT*.xlsx"},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_storage(
    tier="bronze",
    file_patterns={"annual": "*PE_EQUIPMENT*.csv", "annual_xlsx": "*PE_EQUIPMENT*.xlsx"},
    medallion_layer="bronze",
    silver={
        "output_name": "pe_inputs_equipment.parquet",
        "refresh_frequency": "annual",
        "last_updated_by": "aco transform pe_inputs_equipment",
    },
)
@dataclass
class PeInputsEquipment:
    """
    Practice Expense Equipment Inputs - Equipment cost inputs for practice expense calculations
    """

    hcpcs: str = Field(description="HCPCS Code")
    source: str = Field(description="Source of equipment data (CMS or RUC)")
    category: str | None = Field(default=None, description="Equipment Category")
    cms_code: str | None = Field(default=None, description="CMS Equipment Code")
    useful_life: int | None = Field(default=None, description="Useful Life of Equipment in Years")
    price: float | None = Field(default=None, description="Equipment Price")
    utilization_rate: float | None = Field(default=None, description="Equipment Utilization Rate")
    minutes_per_year: float | None = Field(default=None, description="Minutes of Use Per Year")
    nf_time: float | None = Field(default=None, description="Non-Facility Time in Minutes")
    f_time: float | None = Field(default=None, description="Facility Time in Minutes")
    global_period: str | None = Field(default=None, description="Global Period")
