# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for pe_inputs_labor schema.

Generated from: _schemas/pe_inputs_labor.yml
"""

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
)


@register_schema(
    name="pe_inputs_labor",
    version=2,
    tier="bronze",
    description="Practice Expense Labor Inputs - Labor cost and time inputs for practice expense calculations",
    file_patterns={"annual": "*PE_LABOR*.csv", "annual_xlsx": "*PE_LABOR*.xlsx"},
)
@with_parser(
    type="csv", delimiter=",", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_storage(
    tier="bronze",
    file_patterns={"annual": "*PE_LABOR*.csv", "annual_xlsx": "*PE_LABOR*.xlsx"},
    medallion_layer="bronze",
    silver={
        "output_name": "pe_inputs_labor.parquet",
        "refresh_frequency": "annual",
        "last_updated_by": "aco transform pe_inputs_labor",
    },
)
@dataclass
class PeInputsLabor:
    """
    Practice Expense Labor Inputs - Labor cost and time inputs for practice expense calculations
    """

    hcpcs: str = Field(description="HCPCS Code")
    labor_code: str = Field(description="Labor Type Code")
    description: str | None = Field(default=None, description="Labor Type Description")
    rate_per_minute: float | None = Field(default=None, description="Labor Rate Per Minute")
    nf_pre_service_time: float | None = Field(
        default=None, description="Non-Facility Pre-Service Time in Minutes"
    )
    nf_intra_service_time: float | None = Field(
        default=None, description="Non-Facility Intra-Service Time in Minutes"
    )
    nf_post_service_time: float | None = Field(
        default=None, description="Non-Facility Post-Service Time in Minutes"
    )
    f_pre_service_time: float | None = Field(
        default=None, description="Facility Pre-Service Time in Minutes"
    )
    f_intra_service_time: float | None = Field(
        default=None, description="Facility Intra-Service Time in Minutes"
    )
    f_post_service_time: float | None = Field(
        default=None, description="Facility Post-Service Time in Minutes"
    )
    global_period: str | None = Field(default=None, description="Global Period")
