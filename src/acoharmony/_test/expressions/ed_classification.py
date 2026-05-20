# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._expressions._ed_classification module."""

import polars as pl
import pytest

from acoharmony._expressions._ed_classification import EdClassificationExpression


class TestClassifyEdVisits:
    """Cover classify_ed_visits lines 109-253."""

    @pytest.mark.unit
    def test_classify_basic(self):
        """ED visits get classified based on Johnston mapping."""
        # Note: unclassified = 1.0 - (edcnnpa + edcnpa + epct + noner), clipped to [0,1]
        # For correct classification, the 4 base probabilities should sum to ~1.0
        # so unclassified stays low and doesn't dominate.
        ed_visits = pl.DataFrame({
            "claim_id": ["E1", "E2", "E3"],
            "diagnosis_code_1": ["R1001", "S0601", "F1020"],
        }).lazy()
        johnston = pl.DataFrame({
            "icd10": ["R1001", "S0601", "F1020"],
            "edcnnpa": [0.1, 0.3, 0.3],
            "edcnpa": [0.1, 0.3, 0.3],
            "epct": [0.3, 0.2, 0.2],
            "noner": [0.4, 0.1, 0.1],
            "injury": [0.0, 0.9, 0.0],
            "psych": [0.0, 0.0, 0.0],
            "alcohol": [0.0, 0.0, 0.0],
            "drug": [0.0, 0.0, 0.9],
        }).lazy()

        result = EdClassificationExpression.classify_ed_visits(
            ed_visits, johnston, {}
        ).collect()

        assert "ed_classification_primary" in result.columns
        assert "unclassified" in result.columns
        assert "non_emergent" in result.columns

        classifications = result["ed_classification_primary"].to_list()
        # R1001: non_emergent=0.4 is highest → Non-Emergent
        assert classifications[0] == "Non-Emergent"
        # S0601: injury=0.9 is highest → Injury
        assert classifications[1] == "Injury"
        # F1020: drug=0.9 is highest → Drug Related
        assert classifications[2] == "Drug Related"


class TestPreventableEdFlag:
    """Cover build_preventable_ed_flag_expr line 270."""

    @pytest.mark.unit
    def test_preventable_flag(self):
        df = pl.DataFrame({
            "non_emergent": [0.3, 0.0],
            "emergent_primary_care": [0.2, 0.1],
            "emergent_ed_preventable": [0.1, 0.1],
        })
        result = df.select(EdClassificationExpression.build_preventable_ed_flag_expr())
        # 0.3 + 0.2 + 0.1 = 0.6 > 0.5 → True
        assert result["preventable_ed_flag"][0] is True
        # 0.0 + 0.1 + 0.1 = 0.2 ≤ 0.5 → False
        assert result["preventable_ed_flag"][1] is False
