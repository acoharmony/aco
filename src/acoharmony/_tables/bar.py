# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for bar schema.

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from datetime import date

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_four_icli,
    with_parser,
    with_storage,
)
from acoharmony._validators.field_validators import (
    MBI,
    ZIP5,
    mbi_validator,
    zip5_validator,
)


@register_schema(
    name="bar",
    version=2,
    tier="bronze",
    description="Beneficiary Alignment Report (ALGC/ALGR) - Excel file containing beneficiary assignment information for ACO REACH",
    file_patterns={"reach": ["*ALGC*.xlsx", "*ALGR*.xlsx"]},
)
@with_parser(type="excel", encoding="utf-8", has_header=False, embedded_transforms=True)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*ALGC*.xlsx", "*ALGR*.xlsx"]},
    medallion_layer="bronze",
    silver={
        "output_name": "bar.parquet",
        "refresh_frequency": "monthly",
        "last_updated_by": "aco transform bar",
    },
)
@with_four_icli(
    category="Beneficiary List",
    file_type_code=159,
    file_pattern="P.D????.ALG???.RP.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="monthly",
    default_date_filter={"createdWithinLastMonth": True},
)
@dataclass
class Bar:
    """
    Beneficiary Alignment Report (ALGC/ALGR) - Excel file containing beneficiary assignment information for ACO REACH

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Bar.schema_name() -> str
        - Bar.schema_metadata() -> dict
        - Bar.parser_config() -> dict
        - Bar.transform_config() -> dict
        - Bar.lineage_config() -> dict
    """

    beneficiary_mbi_id: str = MBI(
        alias="Beneficiary_MBI_ID",
        description="Medicare Beneficiary Identifier",
    )
    beneficiary_alignment_effective_start_date: date | None = Field(
        alias="Beneficiary_Alignment_Effective_Start_Date",
        default=None,
        description="Alignment effective start date",
    )
    beneficiary_alignment_effective_termination_date: date | None = Field(
        alias="Beneficiary_Alignment_Effective_Termination_Date",
        default=None,
        description="Alignment effective termination date",
    )
    beneficiary_first_name: str | None = Field(
        alias="Beneficiary_First_Name",
    )
    beneficiary_last_name: str | None = Field(
        alias="Beneficiary_Last_Name",
    )
    beneficiary_line_1_address: str | None = Field(
        alias="Beneficiary_Line_1_Address",
    )
    beneficiary_line_2_address: str | None = Field(
        alias="Beneficiary_Line_2_Address",
    )
    beneficiary_line_3_address: str | None = Field(
        alias="Beneficiary_Line_3_Address",
    )
    beneficiary_line_4_address: str | None = Field(
        alias="Beneficiary_Line_4_Address",
    )
    beneficiary_line_5_address: str | None = Field(
        alias="Beneficiary_Line_5_Address",
    )
    beneficiary_line_6_address: str | None = Field(
        alias="Beneficiary_Line_6_Address",
    )
    beneficiary_city: str | None = Field(default=None, description="City", alias="Beneficiary City")
    beneficiary_usps_state_code: str | None = Field(
        alias="Beneficiary_USPS_State_Code",
    )
    beneficiary_zip_5: str | None = Field(
        alias="Beneficiary_Zip_5",
    )
    beneficiary_zip_4: str | None = ZIP5(
        alias="Beneficiary_Zip_4",
    )
    beneficiary_state_county_of_residence_ssa: str | None = Field(
        alias="Beneficiary_State_County_of_Residence_SSA",
        default=None,
        description="SSA State-County code",
    )
    beneficiary_state_county_of_residence_fips: str | None = Field(
        alias="Beneficiary_State_County_of_Residence_FIPS",
        default=None,
        description="FIPS county code",
    )
    beneficiary_gender: str | None = Field(
        alias="Beneficiary_Gender",
    )
    race_ethnicity: str | None = Field(
        alias="Race_Ethnicity",
    )
    beneficiary_date_of_birth: date | None = Field(
        alias="Beneficiary_Date_of_Birth",
    )
    beneficiary_age: int | None = Field(
        alias="Beneficiary_Age",
    )
    beneficiary_date_of_death: date | None = Field(
        alias="Beneficiary_Date_of_Death",
    )
    beneficiary_eligibility_alignment_year_1: str | None = Field(
        alias="Beneficiary_Eligibility_Alignment_Year_1",
        default=None,
        description="Eligibility for Year 1",
    )
    beneficiary_eligibility_alignment_year_2: str | None = Field(
        alias="Beneficiary_Eligibility_Alignment_Year_2",
        default=None,
        description="Eligibility for Year 2",
    )
    beneficiary_part_d_coverage_alignment_year_1: str | None = Field(
        alias="Beneficiary_Part_D_Coverage_Alignment_Year_1",
        default=None,
        description="Part D Coverage Year 1",
    )
    beneficiary_part_d_coverage_alignment_year_2: str | None = Field(
        alias="Beneficiary_Part_D_Coverage_Alignment_Year_2",
        default=None,
        description="Part D Coverage Year 2",
    )
    newly_aligned_beneficiary_flag: str | None = Field(
        alias="Newly_Aligned_Beneficiary_Flag",
    )
    prospective_plus_alignment: str | None = Field(
        alias="Prospective_Plus_Alignment",
    )
    claim_based_alignment_indicator: str | None = Field(
        alias="Claim_Based_Alignment_Indicator",
        default=None,
        description="Claims-based alignment flag",
    )
    voluntary_alignment_type: str | None = Field(
        alias="Voluntary_Alignment_Type",
    )
    mobility_impairment_indicator: str | None = Field(
        alias="Mobility_Impairment_Indicator",
    )
    frailty_indicator: str | None = Field(
        alias="Frailty_Indicator",
    )
    medium_risk_with_unplanned_admissions_indicator: str | None = Field(
        alias="Medium_Risk_with_Unplanned_Admissions_Indicator",
        default=None,
        description="Medium risk with unplanned admissions",
    )
    high_risk_score_indicator: str | None = Field(
        alias="High_Risk_Score_Indicator",
    )

    # Field Validators (from centralized _validators module)
    _validate_beneficiary_mbi_id = mbi_validator("beneficiary_mbi_id")
    _validate_beneficiary_zip_5 = zip5_validator("beneficiary_zip_5")
    _validate_beneficiary_zip_4 = zip5_validator("beneficiary_zip_4")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Bar":
        """Create instance from dictionary."""
        return cls(**data)
