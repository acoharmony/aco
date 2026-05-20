# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._expressions._signature_lifecycle module."""

from datetime import date, datetime

import polars as pl
import pytest

from acoharmony._expressions._signature_lifecycle import SignatureLifecycleExpression


class TestCalculateSignatureLifecycle:
    """Cover calculate_signature_lifecycle lines 73-82 and expression logic."""

    @pytest.mark.unit
    def test_with_defaults(self):
        """Default current_py and reference_date are applied."""
        exprs = SignatureLifecycleExpression.calculate_signature_lifecycle()
        assert isinstance(exprs, list)
        assert len(exprs) > 0

    @pytest.mark.unit
    def test_with_explicit_params(self):
        """Explicit PY and reference_date used for deterministic results."""
        df = pl.DataFrame({
            "last_valid_signature_date": [
                date(2024, 6, 15),
                date(2022, 3, 1),
                None,
            ],
        })
        exprs = SignatureLifecycleExpression.calculate_signature_lifecycle(
            current_py=2025,
            reference_date=datetime(2025, 4, 8),
        )
        result = df.with_columns(exprs)

        # Signature 2024 → expires Jan 1, 2027
        assert result["signature_expiry_date"][0] == date(2027, 1, 1)
        assert result["days_until_signature_expiry"][0] > 0
        assert result["signature_valid_for_current_py"][0] is True
        assert "2024-2026" in result["signature_valid_for_pys"][0]
        assert result["sva_outreach_priority"][0] == "active"

        # Signature 2022 → expires Jan 1, 2025 (already expired)
        assert result["signature_expiry_date"][1] == date(2025, 1, 1)
        assert result["days_until_signature_expiry"][1] < 0
        assert result["sva_outreach_priority"][1] == "expired"

        # Null → no_signature
        assert result["signature_expiry_date"][2] is None
        assert result["sva_outreach_priority"][2] == "no_signature"

    @pytest.mark.unit
    def test_with_datetime_reference_date(self):
        """reference_date as datetime gets converted to date."""
        exprs = SignatureLifecycleExpression.calculate_signature_lifecycle(
            current_py=2025,
            reference_date=datetime(2025, 4, 8, 12, 0),
        )
        assert isinstance(exprs, list)


class TestSignatureLifecycleDateNotDatetime:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_signature_lifecycle_date_not_datetime(self):
        """79->82: reference_date is date, not datetime."""
        from acoharmony._expressions._signature_lifecycle import SignatureLifecycleExpression
        exprs = SignatureLifecycleExpression.calculate_signature_lifecycle(
        current_py=2025, reference_date=date(2025, 4, 9)
        )
        assert len(exprs) > 0
