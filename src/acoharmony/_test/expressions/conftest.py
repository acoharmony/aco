# © 2025 HarmonyCares
# All rights reserved.

"""
Pytest configuration for _expressions tests.

Provides shared fixtures and utilities for testing expression transformations.
"""

import os
from datetime import date

import polars as pl
import pytest

from acoharmony._expressions import ExpressionRegistry


def pytest_configure(config):
    """Configure pytest with custom settings."""
    # Set ACO_PROFILE to dev for all tests
    os.environ["ACO_PROFILE"] = "dev"

    # Register markers
    config.addinivalue_line("markers", "unit: Unit tests (fast, no external dependencies)")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "slow: Slow running tests")


@pytest.fixture(autouse=True)
def reset_expression_registry():
    """Reset expression registry before each test."""
    # Store original state
    original_builders = ExpressionRegistry._builders.copy()
    original_metadata = ExpressionRegistry._metadata.copy()

    yield

    # Restore original state
    ExpressionRegistry._builders = original_builders
    ExpressionRegistry._metadata = original_metadata


@pytest.fixture
def sample_dataframe():
    """Create a sample DataFrame for testing."""
    return pl.DataFrame({
        "bene_id": ["B001", "B002", "B003", "B001"],
        "mbi": ["M001", "M002", "M003", "M001"],
        "claim_id": ["C001", "C002", "C003", "C004"],
        "clm_from_dt": [date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1), date(2023, 4, 1)],
        "amount": [100.0, 200.0, 150.0, 175.0],
    })


@pytest.fixture
def beneficiary_dataframe():
    """Create a beneficiary DataFrame for testing."""
    return pl.DataFrame({
        "bene_id": ["B001", "B002", "B003"],
        "mbi": ["M001", "M002", "M003"],
        "dob": [date(1950, 1, 1), date(1960, 2, 15), date(1970, 3, 30)],
        "death_dt": [None, None, date(2023, 1, 15)],
        "sex": ["M", "F", "M"],
    })


@pytest.fixture
def claims_dataframe():
    """Create a claims DataFrame for testing."""
    return pl.DataFrame({
        "claim_id": ["C001", "C002", "C003"],
        "bene_id": ["B001", "B002", "B003"],
        "clm_from_dt": [date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)],
        "clm_thru_dt": [date(2023, 1, 10), date(2023, 2, 10), date(2023, 3, 10)],
        "paid_amt": [100.0, 200.0, 150.0],
    })


@pytest.fixture
def alignment_dataframe():
    """Create an alignment DataFrame for testing."""
    return pl.DataFrame({
        "bene_id": ["B001", "B002", "B003"],
        "aco_id": ["ACO001", "ACO001", "ACO002"],
        "align_start_dt": [date(2023, 1, 1), date(2023, 1, 1), date(2023, 6, 1)],
        "align_end_dt": [date(2023, 12, 31), date(2023, 12, 31), date(2023, 12, 31)],
    })


@pytest.fixture
def provider_dataframe():
    """Create a provider DataFrame for testing."""
    return pl.DataFrame({
        "npi": ["1234567890", "0987654321", "1111111111"],
        "tax_id": ["TAX001", "TAX002", "TAX003"],
        "name": ["Provider A", "Provider B", "Provider C"],
    })


# Import fixtures from test_injector_fixtures
@pytest.fixture
def sample_medical_claims():
    """Generate sample medical claims for testing."""
    return pl.DataFrame(
        {
            "patient_id": ["P001", "P001", "P002", "P002", "P003"] * 4,
            "claim_id": [f"CLM{i:04d}" for i in range(20)],
            "claim_type": ["inpatient", "outpatient", "professional", "inpatient", "outpatient"]
            * 4,
            "diagnosis_code": [
                "E119",  # Type 2 diabetes (no periods - matches Tuva format)
                "I509",  # Heart failure
                "J440",  # COPD
                "E1165",  # Type 2 diabetes with hyperglycemia
                "I10",  # Hypertension
            ]
            * 4,
            "procedure_code": ["99214", "99213", "99232", "99285", "99214"] * 4,
            "claim_start_date": [date(2024, 1, i % 28 + 1) for i in range(20)],
            "claim_end_date": [date(2024, 1, (i % 28 + 1) + 3) for i in range(20)],
            "admission_date": [
                date(2024, 1, i % 28 + 1) if i % 5 == 0 else None for i in range(20)
            ],
            "discharge_date": [
                date(2024, 1, (i % 28 + 1) + 5) if i % 5 == 0 else None for i in range(20)
            ],
            "paid_amount": [float(100 + i * 50) for i in range(20)],
            "allowed_amount": [float(120 + i * 60) for i in range(20)],
            "encounter_type": ["inpatient", "outpatient", "professional", "inpatient", "outpatient"]
            * 4,
            "encounter_id": [f"ENC{i:04d}" for i in range(20)],
        }
    )


@pytest.fixture
def sample_eligibility():
    """Generate sample eligibility data for testing."""
    return pl.DataFrame(
        {
            "patient_id": ["P001", "P002", "P003"],
            "gender": ["M", "F", "M"],
            "birth_date": [date(1950, 1, 1), date(1960, 2, 15), date(1970, 3, 30)],
            "enrollment_start": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1)],
            "enrollment_end": [date(2024, 12, 31), date(2024, 12, 31), date(2024, 12, 31)],
            "age": [74, 64, 54],
        }
    )


@pytest.fixture
def sample_cms_hcc_mapping():
    """Generate sample CMS HCC ICD-10 mapping seed data (matches Tuva schema)."""
    return pl.DataFrame(
        {
            "payment_year": [2024] * 7,
            "diagnosis_code": [
                "E119",  # Type 2 diabetes (no periods in Tuva data)
                "E1165",  # Type 2 diabetes with hyperglycemia
                "I509",  # Heart failure
                "I501",  # Left ventricular failure
                "J440",  # COPD with acute lower respiratory infection
                "J441",  # COPD with acute exacerbation
                "I10",  # Essential hypertension
            ],
            "cms_hcc_v24": [19, 19, 85, 85, 111, 111, None],
            "cms_hcc_v24_flag": ["Yes"] * 6 + ["No"],
            "cms_hcc_v28": ["19", "19", "85", "85", "111", "111", None],
            "cms_hcc_v28_flag": ["Yes"] * 6 + ["No"],
        }
    )


@pytest.fixture
def sample_cms_hcc_disease_factors():
    """Generate sample CMS HCC disease factors seed data (coefficients)."""
    return pl.DataFrame(
        {
            "model_version": ["CMS-HCC-V28"] * 4,
            "factor_type": ["Disease"] * 4,
            "enrollment_status": ["Continuing"] * 4,
            "medicaid_status": ["No"] * 4,
            "dual_status": ["No"] * 4,
            "orec": ["0"] * 4,
            "institutional_status": ["No"] * 4,
            "hcc_code": [19, 85, 111, 1],
            "description": [
                "Diabetes without Complication",
                "Congestive Heart Failure",
                "Chronic Obstructive Pulmonary Disease",
                "HIV/AIDS",
            ],
            "coefficient": [0.118, 0.368, 0.328, 0.335],
        }
    )


@pytest.fixture
def sample_cms_hcc_disease_hierarchy():
    """Generate sample CMS HCC disease hierarchy seed data."""
    return pl.DataFrame(
        {
            "model_version": ["CMS-HCC-V28"] * 3,
            "hcc_code": [85, 85, 19],
            "description": ["Congestive Heart Failure"] * 2 + ["Diabetes without Complication"],
            "hccs_to_exclude": [86, 87, 20],  # Example hierarchy rules
        }
    )


# ============================================================================
# Synthetic Data Fixtures - Removed (tests._fixtures deleted)
# ============================================================================
# NOTE: Synthetic fixtures were removed when tests/_fixtures was deleted.
# These fixtures were never actually used in any tests.
# If you need test data, use the inline fixtures above or create test-specific data.
