# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for pe_inputs_supplies schema.

Generated from: _schemas/pe_inputs_supplies.yml
"""

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
)


@register_schema(
    name="pe_inputs_supplies",
    version=2,
    tier="bronze",
    description="Practice Expense Supply Inputs - Medical supply cost inputs for practice expense calculations",
    file_patterns={"annual": "*PE_SUPPLIES*.csv", "annual_xlsx": "*PE_SUPPLIES*.xlsx"},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_storage(
    tier="bronze",
    file_patterns={"annual": "*PE_SUPPLIES*.csv", "annual_xlsx": "*PE_SUPPLIES*.xlsx"},
    medallion_layer="bronze",
    silver={
        "output_name": "pe_inputs_supplies.parquet",
        "refresh_frequency": "annual",
        "last_updated_by": "aco transform pe_inputs_supplies",
    },
)
@dataclass
class PeInputsSupplies:
    """
    Practice Expense Supply Inputs - Medical supply cost inputs for practice expense calculations
    """

    hcpcs: str = Field(description="HCPCS Code")
    source: str = Field(description="Source of supply data (CMS or RUC)")
    category: str | None = Field(default=None, description="Supply Category")
    cms_code: str | None = Field(default=None, description="CMS Supply Code")
    description: str | None = Field(default=None, description="Supply Description")
    unit: str | None = Field(
        default=None, description="Unit of Measure (item, pair, kit, ml, oz, pack, box, foot)"
    )
    price: float | None = Field(default=None, description="Unit Price")
    nf_quantity: float | None = Field(default=None, description="Non-Facility Quantity Used")
    f_quantity: float | None = Field(default=None, description="Facility Quantity Used")
    global_period: str | None = Field(default=None, description="Global Period")
