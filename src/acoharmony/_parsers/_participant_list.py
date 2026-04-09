# © 2025 HarmonyCares
# All rights reserved.

"""
Custom parser for participant list files that handles multiple formats.

Normalizes both ACO REACH Participant List (51 columns) and D0259 Provider List (27 columns)
into a unified schema.
"""

import polars as pl


def normalize_participant_list(df: pl.DataFrame, filename: str) -> pl.DataFrame:
    """
    Normalize participant list data from different file formats.

    Detects format based on column names and maps to unified schema.

    Args:
        df: Raw dataframe from Excel file
        filename: Source filename for detecting format

    Returns:
        Normalized dataframe with unified schema
    """
    columns = df.columns

    # Detect format based on presence of key columns
    if "Entity ID" in columns and "Entity TIN" in columns:
        # ACO REACH Participant List format (51 columns)
        return _normalize_reach_format(df)
    elif "Billing TIN" in columns and "TIN Legal Bus Name" in columns:
        # D0259 Provider List format (27 columns)
        return _normalize_d0259_format(df)
    else:
        raise ValueError(f"Unknown participant list format in {filename}")


def _normalize_reach_format(df: pl.DataFrame) -> pl.DataFrame:
    """Normalize ACO REACH Participant List format (already in correct schema)."""
    # REACH format already matches our schema, just return as-is
    return df


def _normalize_d0259_format(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalize D0259 Provider List format to match ACO REACH schema.

    Maps D0259 columns to REACH equivalents and fills missing columns with nulls.
    """
    # Column mapping from D0259 to REACH schema
    column_mapping = {
        "Provider Type": "Provider Type",
        "Provider Class": "Provider Class",
        "Billing TIN": "Base Provider TIN",
        "Organization NPI": "Organization NPI",
        "CCN": "CCN",
        "Individual NPI(s)": "Individual NPI",
        "TIN Legal Bus Name": "Provider Legal Business Name",
        "Last Name": "Individual Last Name",
        "First Name": "Individual First Name",
        "I attest that this provider will use CEHRT in a manner sufficient to meet the applicable requirements of the Advanced Alternative Payment Model criterion under 42 CFR § 414.1415(a)(1)(iii), including any amendments thereto": "I attest that this provider will use CEHRT in a manner sufficient to meet the applicable requirements of the Advanced Alternative Payment Model criterion under 42 CFR § 414.1415(a)(1)(iii), including any amendments thereto",
        "CEHRT ID": "CEHRT ID",
        "I attest that this provider has an exception for Low-volume threshold, as defined in 42 CFR 414.1305": "I attest that this provider has an exception for Low-volume threshold, as defined in 42 CFR 414.1305",
        "I attest that this provider has an exception for not being an MIPS eligible clinician, as set forth in § 414.1310(b)(2)": "I attest that this provider has an exception for not being an MIPS eligible clinician, as set forth in § 414.1310(b)(2)",
        "I attest that this provider has an exception for Reweighting of the MIPS Promoting Interoperability, as set forth at 42 CFR 414.1380(c)(2)(i)": "I attest that this provider has an exception for Reweighting of the MIPS Promoting Interoperability, as set forth at 42 CFR 414.1380(c)(2)(i)",
        "Other": "Other",
        "Email": "Email",
    }

    # Rename columns according to mapping
    for old_col, new_col in column_mapping.items():
        if old_col in df.columns:
            df = df.rename({old_col: new_col})

    # Add missing columns with null values
    missing_columns = {
        "Entity ID": pl.lit(None, dtype=pl.Utf8),
        "Entity TIN": pl.lit(None, dtype=pl.Utf8),
        "Entity Legal Business Name": pl.lit(None, dtype=pl.Utf8),
        "Performance Year": pl.lit(None, dtype=pl.Utf8),
        "Sole Proprietor": pl.lit(None, dtype=pl.Utf8),
        "Sole Proprietor TIN": pl.lit(None, dtype=pl.Utf8),
        "Primary Care Services": pl.lit(None, dtype=pl.Utf8),
        "Specialty": pl.lit(None, dtype=pl.Utf8),
        "Base Provider TIN Status": pl.lit(None, dtype=pl.Utf8),
        "Base Provider TIN Dropped/Terminated Reason": pl.lit(None, dtype=pl.Utf8),
        "Effective Start Date": pl.lit(None, dtype=pl.Utf8),
        "Effective End Date": pl.lit(None, dtype=pl.Utf8),
        "Last Updated Date": pl.lit(None, dtype=pl.Utf8),
        "Ad-hoc Provider Addition Reason": pl.lit(None, dtype=pl.Utf8),
        "PECOS Check Results": pl.lit(None, dtype=pl.Utf8),
        "Uses CEHRT?": pl.lit(None, dtype=pl.Utf8),
        "Overlaps/Deficiencies": pl.lit(None, dtype=pl.Utf8),
        "Attestation (Y/N)": pl.lit(None, dtype=pl.Utf8),
        "Total Care Capitation % Reduction": pl.lit(None, dtype=pl.Utf8),
        "Primary Care Capitation % Reduction": pl.lit(None, dtype=pl.Utf8),
        "Advanced Payment % Reduction": pl.lit(None, dtype=pl.Utf8),
        "Cardiac and Pulmonary Rehabilitation": pl.lit(None, dtype=pl.Utf8),
        "Care Management Home Visit": pl.lit(None, dtype=pl.Utf8),
        "Concurrent Care for Hospice Beneficiaries": pl.lit(None, dtype=pl.Utf8),
        "Chronic Disease Management Reward (BEI)": pl.lit(None, dtype=pl.Utf8),
        "Cost Sharing for Part B Services (BEI)": pl.lit(None, dtype=pl.Utf8),
        "Diabetic Shoes": pl.lit(None, dtype=pl.Utf8),
        "Home Health Homebound Waiver": pl.lit(None, dtype=pl.Utf8),
        "Home Infusion Therapy": pl.lit(None, dtype=pl.Utf8),
        "Hospice Care Certification": pl.lit(None, dtype=pl.Utf8),
        "Medical Nutrition Therapy": pl.lit(None, dtype=pl.Utf8),
        "Nurse Practitioner Services": pl.lit(None, dtype=pl.Utf8),
        "Post Discharge Home Visit": pl.lit(None, dtype=pl.Utf8),
        "Skilled Nursing Facility (SNF) 3-Day Stay Waiver": pl.lit(None, dtype=pl.Utf8),
        "Telehealth": pl.lit(None, dtype=pl.Utf8),
    }

    # Add columns that don't exist
    for col_name, col_value in missing_columns.items():
        if col_name not in df.columns:
            df = df.with_columns(col_value.alias(col_name))

    return df
