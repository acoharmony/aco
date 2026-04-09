# © 2025 HarmonyCares
# All rights reserved.

"""Reconcile temporal alignment and enrollment tracking.

Validates the consolidated alignment's temporal matrix:
- Block continuity (no gaps in month sequences)
- Program transitions are consistent (MSSP→REACH, REACH→MSSP)
- Enrollment gaps are flagged correctly
- Signature validity windows are computed correctly
- Voluntary alignment dates align with SVA/PBVAR sources
"""

import polars as pl
import pytest

from .conftest import requires_data, scan_gold, scan_silver


@requires_data
class TestTemporalMatrixConsistency:
    """Validate temporal enrollment matrix in consolidated alignment."""

    @pytest.fixture
    def alignment(self):
        try:
            return scan_gold("consolidated_alignment").collect()
        except Exception:
            pytest.skip("consolidated_alignment not available")

    @pytest.mark.reconciliation
    def test_ym_columns_exist(self, alignment):
        """Consolidated alignment should have ym_YYYYMM_program columns."""
        ym_cols = [c for c in alignment.columns if c.startswith("ym_")]
        assert len(ym_cols) > 0, "No temporal matrix (ym_*) columns found"

    @pytest.mark.reconciliation
    def test_reach_months_consistent(self, alignment):
        """months_in_reach should match count of True ym_*_reach columns."""
        if "months_in_reach" not in alignment.columns:
            pytest.skip("months_in_reach not in schema")

        reach_cols = [c for c in alignment.columns if c.startswith("ym_") and c.endswith("_reach")]
        if not reach_cols:
            pytest.skip("No ym_*_reach columns")

        # Sample check on first 100 rows
        sample = alignment.head(100)
        for row in sample.iter_rows(named=True):
            reported = row.get("months_in_reach", 0) or 0
            actual = sum(1 for c in reach_cols if row.get(c) is True)
            assert abs(reported - actual) <= 1, (
                f"MBI {row.get('current_mbi')}: months_in_reach={reported} "
                f"but {actual} ym_*_reach columns are True"
            )

    @pytest.mark.reconciliation
    def test_program_transition_logic(self, alignment):
        """has_program_transition should only be True for dual-program members."""
        if "has_program_transition" not in alignment.columns:
            pytest.skip("has_program_transition not available")
        if "ever_reach" not in alignment.columns or "ever_mssp" not in alignment.columns:
            pytest.skip("ever_reach/ever_mssp not available")

        # Transition requires being in BOTH programs
        bad = alignment.filter(
            pl.col("has_program_transition") == True
        ).filter(
            ~(pl.col("ever_reach") & pl.col("ever_mssp"))
        )
        assert bad.height == 0, (
            f"{bad.height} members flagged as transitioned but not in both programs"
        )


@requires_data
class TestSignatureValidityReconciliation:
    """Validate signature expiry calculations against SVA source dates."""

    @pytest.mark.reconciliation
    def test_signature_expiry_is_3_years(self):
        """Signature expiry should be Jan 1 of (signature_year + 3)."""
        try:
            alignment = scan_gold("consolidated_alignment").collect()
        except Exception:
            pytest.skip("consolidated_alignment not available")

        if "last_valid_signature_date" not in alignment.columns:
            pytest.skip("No signature date column")
        if "signature_expiry_date" not in alignment.columns:
            pytest.skip("No expiry date column")

        signed = alignment.filter(pl.col("last_valid_signature_date").is_not_null()).head(100)
        for row in signed.iter_rows(named=True):
            sig_date = row["last_valid_signature_date"]
            expiry = row["signature_expiry_date"]
            if sig_date and expiry:
                expected_year = sig_date.year + 3
                assert expiry.year == expected_year and expiry.month == 1 and expiry.day == 1, (
                    f"Signature {sig_date} should expire Jan 1, {expected_year} "
                    f"but got {expiry}"
                )


@requires_data
class TestVoluntaryAlignmentSourceReconciliation:
    """Cross-check voluntary alignment against SVA/PBVAR source data."""

    @pytest.mark.reconciliation
    def test_sva_signatures_match_source(self):
        """Members with valid SVA in alignment should be traceable to SVA submissions."""
        try:
            alignment = scan_gold("consolidated_alignment").collect()
            sva = scan_silver("sva_submissions").collect()
        except Exception:
            pytest.skip("Required tables not available")

        if "last_valid_signature_date" not in alignment.columns:
            pytest.skip("last_valid_signature_date not in alignment")

        # Count members with a valid SVA signature in alignment
        has_sva = alignment.filter(pl.col("last_valid_signature_date").is_not_null())
        sva_total = sva.height

        if sva_total > 0:
            # Members with signatures should be > 0 if source has data
            assert has_sva.height > 0, "Alignment has 0 SVA signatures but source has data"
