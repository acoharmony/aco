# © 2025 HarmonyCares
# All rights reserved.

"""
Medicare Physician Fee Schedule (PFS) Payment Rate Calculation Expressions

Mathematical Foundation
=======================

The Medicare Physician Fee Schedule uses a resource-based relative value scale (RBRVS)
to determine payment rates for physician services. The system assigns relative values
to each HCPCS code based on three resource components, then adjusts these values for
geographic cost variations.

Core Payment Formula
--------------------

The fundamental payment equation is:

    Payment = [(Work_RVU × Work_GPCI) + (PE_RVU × PE_GPCI) + (MP_RVU × MP_GPCI)] × CF

Where:
    - RVU (Relative Value Unit): Measure of resource intensity for each service component
    - GPCI (Geographic Practice Cost Index): Local cost adjustment factor (range: ~0.7 to 1.5)
    - CF (Conversion Factor): Dollar multiplier set annually by CMS (e.g., $32.74 for 2024)

Three Resource Components
--------------------------

1. Work Component (Work_RVU × Work_GPCI)
   - Represents physician time, skill, training, and intensity required
   - Work_RVU: National relative value (same for all locations)
   - Work_GPCI: Adjusts for geographic differences in physician wage rates
   - Example: 99347 (Home visit, established, 25 min) has Work_RVU = 1.92

2. Practice Expense Component (PE_RVU × PE_GPCI)
   - Covers overhead costs: staff salaries, rent, equipment, supplies
   - PE_RVU varies by setting:
     * Non-Facility (NF_PE_RVU): Services in physician office/home (higher)
     * Facility (F_PE_RVU): Services in hospital/facility (lower, as facility provides overhead)
   - PE_GPCI: Adjusts for local costs of office space, staff wages, supplies
   - Example: 99347 has NF_PE_RVU = 1.31, F_PE_RVU = 0.20

3. Malpractice Component (MP_RVU × MP_GPCI)
   - Reflects professional liability insurance costs
   - MP_RVU: National relative malpractice cost
   - MP_GPCI: Adjusts for state-level malpractice insurance premium variations
   - Example: 99347 has MP_RVU = 0.15

Geographic Adjustment via GPCI
-------------------------------

GPCI values adjust payment for local cost variations:
- GPCI = 1.00: National average cost level
- GPCI > 1.00: Higher than average costs (e.g., Manhattan = 1.088 for work)
- GPCI < 1.00: Lower than average costs (e.g., Alabama = 0.966 for work)
- GPCI Floor: By law, GPCIs cannot fall below 1.00 in certain states (Alaska policy)

Each Medicare locality (defined by carrier + locality code) has unique GPCI values
for all three components, reflecting local cost structures.

Conversion Factor
-----------------

The conversion factor (CF) translates relative values into dollar payments:
- Set annually by CMS through rulemaking
- Reflects budget neutrality requirements and statutory updates
- Recent values: 2024 = $32.7476, 2025 = $33.2875, 2026 = $34.6062
- Applied uniformly across all HCPCS codes and locations

Complete Calculation Example
-----------------------------

Calculate payment for HCPCS 99347 (home visit) in Manhattan, NY (locality 00):

Step 1: Identify RVUs (from PPRVU file)
    Work_RVU    = 1.92
    NF_PE_RVU   = 1.31
    MP_RVU      = 0.15

Step 2: Identify GPCIs (from GPCI file for NY locality 00)
    Work_GPCI   = 1.088
    PE_GPCI     = 1.459
    MP_GPCI     = 1.494

Step 3: Calculate geographic adjustments
    Work_Payment    = 1.92 × 1.088 = 2.089
    PE_Payment      = 1.31 × 1.459 = 1.911
    MP_Payment      = 0.15 × 1.494 = 0.224
    Total_Adjusted  = 2.089 + 1.911 + 0.224 = 4.224

Step 4: Apply conversion factor (2026 = $34.6062)
    Payment_Rate    = 4.224 × $34.6062 = $146.17

Comparison: Same code in rural Alabama (locality 01)
    Work_GPCI = 0.966, PE_GPCI = 0.871, MP_GPCI = 0.525
    Total_Adjusted = (1.92×0.966) + (1.31×0.871) + (0.15×0.525) = 3.072
    Payment_Rate = 3.072 × $34.6062 = $106.31
    Geographic Difference = $146.17 - $106.31 = $39.86 (37.5% higher in Manhattan)

Year-Over-Year Comparison
--------------------------

Payment changes between years result from:
1. RVU changes: CMS may revalue services based on utilization reviews
2. GPCI changes: Updated annually based on cost data (typically small changes)
3. Conversion factor changes: Reflects Medicare budget requirements
4. Budget neutrality adjustments: RVU increases offset by CF decreases

Formula for YoY comparison:
    Dollar_Change = Payment_Year2 - Payment_Year1
    Percent_Change = (Dollar_Change / Payment_Year1) × 100

References
----------

- 42 CFR 414.22 - Relative value units (RVUs)
- 42 CFR 414.26 - Geographic adjustment factors
- Social Security Act §1848 - Payment for physicians' services
- CMS-1784-F: CY 2026 Physician Fee Schedule Final Rule
- Federal Register notices for annual fee schedule updates

Data Sources
------------

- PPRVU File: Contains RVU values for all HCPCS codes (updated annually)
- GPCI File: Contains locality-specific adjustment factors (updated annually)
- Addenda: Published with each year's PFS final rule
- Available at: https://www.cms.gov/medicare/payment/fee-schedules/physician
"""

from typing import Literal

import polars as pl
from pydantic import BaseModel, Field

from .._decor8 import expression
from ._registry import register_expression


class PFSRateCalcConfig(BaseModel):
    """
    Configuration for PFS payment rate calculation.

    Attributes:
        hcpcs_codes: List of HCPCS/CPT codes to calculate rates for
        year: Payment year (defaults to most recent available)
        prior_year: Previous year for comparison (defaults to year - 1)
        facility_type: Setting type for PE RVU selection
        conversion_factor: Override conversion factor (if not from citation)
        prior_conversion_factor: Override prior year CF
        include_comparison: Whether to calculate year-over-year comparison
        use_home_visit_codes: Use predefined home visit code list
    """

    hcpcs_codes: list[str] = Field(
        default_factory=list,
        description="List of HCPCS/CPT codes to calculate rates for",
    )
    year: int | None = Field(
        default=None,
        description="Payment year (None = most recent available)",
    )
    prior_year: int | None = Field(
        default=None,
        description="Prior year for comparison (None = year - 1)",
    )
    facility_type: Literal["non_facility", "facility", "both"] = Field(
        default="non_facility",
        description="Facility setting type",
    )
    conversion_factor: float | None = Field(
        default=None,
        ge=0,
        description="Override conversion factor for current year",
    )
    prior_conversion_factor: float | None = Field(
        default=None,
        ge=0,
        description="Override conversion factor for prior year",
    )
    include_comparison: bool = Field(
        default=True,
        description="Calculate year-over-year comparison",
    )
    use_home_visit_codes: bool = Field(
        default=False,
        description="Use predefined home visit HCPCS code list",
    )


@register_expression(
    "pfs_rate_calc",
    schemas=["gold"],
    dataset_types=["pfs", "payment"],
    callable=False,
    description="Calculate Medicare PFS payment rates with geographic adjustments",
)
class PFSRateCalcExpression:
    """
    Expressions for calculating Medicare PFS payment rates.

    Provides component calculations for:
    - Work payment (work RVU × work GPCI)
    - Practice expense payment (PE RVU × PE GPCI)
    - Malpractice payment (MP RVU × MP GPCI)
    - Total geographically adjusted payment
    - Year-over-year payment changes
    """

    @staticmethod
    @expression(
        name="calculate_work_payment",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def calculate_work_payment(
        work_rvu_col: str = "work_rvu",
        pw_gpci_col: str = "pw_gpci",
    ) -> pl.Expr:
        """
        Calculate physician work payment component.

        Formula: work_payment = work_rvu × pw_gpci

        Args:
            work_rvu_col: Column name for work RVU
            pw_gpci_col: Column name for physician work GPCI

        Returns:
            Expression calculating work payment component
        """
        return pl.col(work_rvu_col) * pl.col(pw_gpci_col)

    @staticmethod
    @expression(
        name="calculate_pe_payment",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def calculate_pe_payment(
        pe_rvu_col: str = "nf_pe_rvu",
        pe_gpci_col: str = "pe_gpci",
    ) -> pl.Expr:
        """
        Calculate practice expense payment component.

        Formula: pe_payment = pe_rvu × pe_gpci

        Args:
            pe_rvu_col: Column name for PE RVU (nf_pe_rvu or f_pe_rvu)
            pe_gpci_col: Column name for practice expense GPCI

        Returns:
            Expression calculating practice expense payment component
        """
        return pl.col(pe_rvu_col) * pl.col(pe_gpci_col)

    @staticmethod
    @expression(
        name="calculate_mp_payment",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def calculate_mp_payment(
        mp_rvu_col: str = "mp_rvu",
        mp_gpci_col: str = "mp_gpci",
    ) -> pl.Expr:
        """
        Calculate malpractice payment component.

        Formula: mp_payment = mp_rvu × mp_gpci

        Args:
            mp_rvu_col: Column name for malpractice RVU
            mp_gpci_col: Column name for malpractice GPCI

        Returns:
            Expression calculating malpractice payment component
        """
        return pl.col(mp_rvu_col) * pl.col(mp_gpci_col)

    @staticmethod
    @expression(
        name="calculate_total_rvu_adjusted",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def calculate_total_rvu_adjusted(
        work_payment_col: str = "work_payment",
        pe_payment_col: str = "pe_payment",
        mp_payment_col: str = "mp_payment",
    ) -> pl.Expr:
        """
        Calculate total geographically adjusted RVU.

        Formula: total_rvu_adjusted = work_payment + pe_payment + mp_payment

        Args:
            work_payment_col: Column name for work payment component
            pe_payment_col: Column name for PE payment component
            mp_payment_col: Column name for MP payment component

        Returns:
            Expression calculating total adjusted RVU
        """
        return pl.col(work_payment_col) + pl.col(pe_payment_col) + pl.col(mp_payment_col)

    @staticmethod
    @expression(
        name="calculate_payment_rate",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def calculate_payment_rate(
        total_rvu_adjusted_col: str = "total_rvu_adjusted",
        conversion_factor_col: str = "conversion_factor",
    ) -> pl.Expr:
        """
        Calculate final Medicare payment rate.

        Formula: payment_rate = total_rvu_adjusted × conversion_factor

        Args:
            total_rvu_adjusted_col: Column name for total adjusted RVU
            conversion_factor_col: Column name for conversion factor

        Returns:
            Expression calculating final payment amount
        """
        return pl.col(total_rvu_adjusted_col) * pl.col(conversion_factor_col)

    @staticmethod
    @expression(
        name="calculate_rate_change_dollars",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def calculate_rate_change_dollars(
        current_rate_col: str = "payment_rate",
        prior_rate_col: str = "prior_payment_rate",
    ) -> pl.Expr:
        """
        Calculate dollar change from prior year.

        Formula: rate_change_dollars = current_rate - prior_rate

        Args:
            current_rate_col: Column name for current year payment rate
            prior_rate_col: Column name for prior year payment rate

        Returns:
            Expression calculating dollar change
        """
        return pl.col(current_rate_col) - pl.col(prior_rate_col)

    @staticmethod
    @expression(
        name="calculate_rate_change_percent",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def calculate_rate_change_percent(
        current_rate_col: str = "payment_rate",
        prior_rate_col: str = "prior_payment_rate",
    ) -> pl.Expr:
        """
        Calculate percent change from prior year.

        Formula: rate_change_percent = ((current_rate - prior_rate) / prior_rate) × 100

        Args:
            current_rate_col: Column name for current year payment rate
            prior_rate_col: Column name for prior year payment rate

        Returns:
            Expression calculating percent change
        """
        return (pl.col(current_rate_col) - pl.col(prior_rate_col)) / pl.col(prior_rate_col) * 100

    @staticmethod
    def select_pe_rvu_column(
        facility_type: Literal["non_facility", "facility"] = "non_facility",
    ) -> str:
        """
        Select appropriate PE RVU column based on facility type.

        This is a utility method that returns a column name string,
        not a Polars expression — so it must NOT use the @expression decorator.

        Args:
            facility_type: Setting type (non_facility or facility)

        Returns:
            Column name to use for PE RVU
        """
        return "nf_pe_rvu" if facility_type == "non_facility" else "f_pe_rvu"

    @staticmethod
    def validate_gpci(gpci_col: str, default_value: float = 1.0) -> pl.Expr:
        """
        Validate and handle missing GPCI values.

        If GPCI is null or zero, use default value (typically 1.0 for national average).

        Args:
            gpci_col: Column name for GPCI value
            default_value: Default GPCI value for missing data

        Returns:
            Expression with validated GPCI values
        """
        return (
            pl.when((pl.col(gpci_col).is_null()) | (pl.col(gpci_col) == 0))
            .then(pl.lit(default_value))
            .otherwise(pl.col(gpci_col))
        )

    @staticmethod
    def build_payment_calculation(
        work_rvu: str = "work_rvu",
        pe_rvu: str = "nf_pe_rvu",
        mp_rvu: str = "mp_rvu",
        pw_gpci: str = "pw_gpci",
        pe_gpci: str = "pe_gpci",
        mp_gpci: str = "mp_gpci",
        conversion_factor: str = "conversion_factor",
    ) -> dict[str, pl.Expr]:
        """
        Build complete set of payment calculation expressions.

        Returns dictionary of column names to expressions for:
        - work_payment
        - pe_payment
        - mp_payment
        - total_rvu_adjusted
        - payment_rate

        Args:
            work_rvu: Work RVU column name
            pe_rvu: Practice expense RVU column name
            mp_rvu: Malpractice RVU column name
            pw_gpci: Physician work GPCI column name
            pe_gpci: Practice expense GPCI column name
            mp_gpci: Malpractice GPCI column name
            conversion_factor: Conversion factor column name

        Returns:
            Dictionary mapping output column names to calculation expressions
        """
        return {
            "work_payment": pl.col(work_rvu) * pl.col(pw_gpci),
            "pe_payment": pl.col(pe_rvu) * pl.col(pe_gpci),
            "mp_payment": pl.col(mp_rvu) * pl.col(mp_gpci),
            "total_rvu_adjusted": (
                (pl.col(work_rvu) * pl.col(pw_gpci))
                + (pl.col(pe_rvu) * pl.col(pe_gpci))
                + (pl.col(mp_rvu) * pl.col(mp_gpci))
            ),
            "payment_rate": (
                (
                    (pl.col(work_rvu) * pl.col(pw_gpci))
                    + (pl.col(pe_rvu) * pl.col(pe_gpci))
                    + (pl.col(mp_rvu) * pl.col(mp_gpci))
                )
                * pl.col(conversion_factor)
            ),
        }
