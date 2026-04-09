# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for recon schema.

Generated from: _schemas/recon.yml

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from decimal import Decimal

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
    with_transform,
)


@register_schema(name="recon", version=2, tier="bronze", description="""\2""")
@with_parser(
    type="delimited", delimiter="|", encoding="utf-8", has_header=False, embedded_transforms=False
)
@with_transform()
@with_storage(
    medallion_layer="gold",
    bronze={"output_name": "reconciliation_report.parquet"},
    silver={"output_name": "reconciliation_report.parquet"},
    gold={"output_name": "reconciliation_report.parquet"},
)
@dataclass
class Recon:
    """
    Shared Savings/Value Agreement Report - Delimited file with financial performance metrics

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Recon.schema_name() -> str
        - Recon.schema_metadata() -> dict
        - Recon.parser_config() -> dict
        - Recon.transform_config() -> dict
        - Recon.lineage_config() -> dict
    """

    aco_id: str = Field(description="ACO Identifier")
    performance_year: int = Field(description="Performance Year (YYYY)")
    report_quarter: str | None = Field(
        default=None, description="Report Quarter (Q1-Q4, or ANNUAL)"
    )
    total_assigned_benes: int | None = Field(
        default=None, description="Total Assigned Beneficiaries"
    )
    person_years: Decimal | None = Field(default=None, description="Total Person Years")
    avg_risk_score: Decimal | None = Field(default=None, description="Average HCC Risk Score")
    benchmark_expenditure: Decimal | None = Field(default=None, description="Benchmark Expenditure")
    actual_expenditure: Decimal | None = Field(default=None, description="Actual Expenditure")
    gross_savings: Decimal | None = Field(default=None, description="Gross Savings/Loss Amount")
    msr_percent: Decimal | None = Field(default=None, description="Minimum Savings Rate Percentage")
    msr_amount: Decimal | None = Field(default=None, description="Minimum Savings Rate Amount")
    mlr_percent: Decimal | None = Field(default=None, description="Minimum Loss Rate Percentage")
    mlr_amount: Decimal | None = Field(default=None, description="Minimum Loss Rate Amount")
    quality_score: Decimal | None = Field(default=None, description="Overall Quality Score")
    quality_adjustment: Decimal | None = Field(
        default=None, description="Quality Score Adjustment Factor"
    )
    earned_savings: Decimal | None = Field(default=None, description="Earned Shared Savings")
    owed_losses: Decimal | None = Field(default=None, description="Owed Shared Losses")
    final_sharing_rate: Decimal | None = Field(default=None, description="Final Sharing Rate")
    risk_arrangement: str | None = Field(
        default=None, description="Risk Arrangement Type (ONE_SIDED, TWO_SIDED, ENHANCED, GLOBAL)"
    )
    benchmark_type: str | None = Field(
        default=None, description="Benchmark Type (HISTORICAL, REGIONAL, BLEND)"
    )
    trend_factor: Decimal | None = Field(default=None, description="Trend Factor Applied")
    regional_adjustment: Decimal | None = Field(
        default=None, description="Regional Adjustment Factor"
    )
    prior_savings_adjustment: Decimal | None = Field(
        default=None, description="Prior Savings Adjustment"
    )
    sequestration_amount: Decimal | None = Field(
        default=None, description="Sequestration Reduction Amount"
    )
    total_part_a_exp: Decimal | None = Field(default=None, description="Total Part A Expenditure")
    total_part_b_exp: Decimal | None = Field(default=None, description="Total Part B Expenditure")
    total_part_d_exp: Decimal | None = Field(default=None, description="Total Part D Expenditure")
    ip_admits_per_1000: Decimal | None = Field(
        default=None, description="Inpatient Admits per 1000 Person Years"
    )
    readmit_rate: Decimal | None = Field(default=None, description="30-Day Readmission Rate")
    er_visits_per_1000: Decimal | None = Field(
        default=None, description="ER Visits per 1000 Person Years"
    )
    awv_rate: Decimal | None = Field(default=None, description="Annual Wellness Visit Rate")
    generic_rx_rate: Decimal | None = Field(default=None, description="Generic Prescription Rate")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Recon":
        """Create instance from dictionary."""
        return cls(**data)
