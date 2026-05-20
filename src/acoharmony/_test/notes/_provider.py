# © 2025 HarmonyCares
"""Tests for acoharmony._notes._provider (ProviderPlugins)."""

from __future__ import annotations

from unittest.mock import patch

import polars as pl
import pytest

from acoharmony._notes import ProviderPlugins
from acoharmony._notes._provider import _free_text_filter


# ---------------------------------------------------------------------------
# fixtures
# ---------------------------------------------------------------------------


def _individual_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "provider_name": ["Alice Smith", "Bob Jones"],
            "tin": ["111", "222"],
            "npi": ["NPI1", "NPI2"],
            "provider_category": ["Individual Participant", "Individual Participant"],
            "performance_year": ["PY2024", "PY2024"],
            "entity_id": ["E1", "E1"],
        }
    )


def _preferred_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "provider_name": ["AcmeOrg", "Beta Health"],
            "organization": ["AcmeOrg", "Beta Health"],
            "tin": ["111", "333"],
            "npi": ["ORG1", "ORG2"],
            "provider_category": ["Preferred Provider", "Preferred Provider"],
            "provider_type": ["Organizational Provider", "Organizational Provider"],
            "provider_class": ["Participant Provider", "Other"],
            "performance_year": ["PY2024", "PY2024"],
            "entity_id": ["E1", "E2"],
        }
    )


def _participant_list_df() -> pl.DataFrame:
    """Source rows that look like participant_list before extraction."""
    return pl.DataFrame(
        {
            "individual_npi": ["NPI1", None, "NPI2"],
            "organization_npi": [None, "ORG1", None],
        }
    )


# ---------------------------------------------------------------------------
# extract_individual / extract_preferred / combine
# ---------------------------------------------------------------------------


class TestExtract:
    @pytest.mark.unit
    def test_individual_empty(self) -> None:
        out = ProviderPlugins().extract_individual(pl.DataFrame())
        assert out.is_empty()

    @pytest.mark.unit
    def test_individual_uses_expression(self) -> None:
        # Mock out the expression module to keep this a pure unit test
        df = _participant_list_df()
        with patch(
            "acoharmony._expressions.ProviderAlignmentExpression"
        ) as expr:
            expr.filter_has_individual_npi.return_value = (
                pl.col("individual_npi").is_not_null()
            )
            expr.select_individual_participant_columns.return_value = (
                "individual_npi",
            )
            out = ProviderPlugins().extract_individual(df)
        assert out.height == 2

    @pytest.mark.unit
    def test_preferred_empty(self) -> None:
        out = ProviderPlugins().extract_preferred(pl.DataFrame())
        assert out.is_empty()

    @pytest.mark.unit
    def test_preferred_uses_expression(self) -> None:
        df = _participant_list_df()
        with patch(
            "acoharmony._expressions.ProviderAlignmentExpression"
        ) as expr:
            expr.filter_has_organization_npi.return_value = (
                pl.col("organization_npi").is_not_null()
            )
            expr.select_preferred_provider_columns.return_value = (
                "organization_npi",
            )
            out = ProviderPlugins().extract_preferred(df)
        assert out.height == 1


class TestCombine:
    @pytest.mark.unit
    def test_both_empty(self) -> None:
        assert ProviderPlugins().combine(pl.DataFrame(), pl.DataFrame()).is_empty()

    @pytest.mark.unit
    def test_individual_only(self) -> None:
        out = ProviderPlugins().combine(_individual_df(), pl.DataFrame())
        assert out.height == 2

    @pytest.mark.unit
    def test_preferred_only(self) -> None:
        out = ProviderPlugins().combine(pl.DataFrame(), _preferred_df())
        assert out.height == 2

    @pytest.mark.unit
    def test_concats(self) -> None:
        common_cols = ["provider_name", "tin", "npi", "performance_year", "entity_id", "provider_category"]
        ind = _individual_df().select(common_cols)
        pref = _preferred_df().select(common_cols)
        out = ProviderPlugins().combine(ind, pref)
        assert out.height == 4


# ---------------------------------------------------------------------------
# rollups
# ---------------------------------------------------------------------------


def _all_providers() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "tin": ["111", "111", "222", "333"],
            "npi": ["A", "B", "C", "D"],
            "provider_category": [
                "Individual Participant",
                "Preferred Provider",
                "Individual Participant",
                "Preferred Provider",
            ],
            "provider_class": ["X", "Y", "X", "Y"],
            "performance_year": ["PY2024", "PY2024", "PY2024", "PY2025"],
            "entity_id": ["E1", "E1", "E1", "E2"],
            "organization": ["O1", "O1", "O1", "O2"],
        }
    )


class TestRollups:
    @pytest.mark.unit
    def test_category_breakdown(self) -> None:
        out = ProviderPlugins().category_breakdown(_all_providers())
        assert out.height == 2  # two (category, class) pairs
        assert {"unique_tins", "unique_npis"}.issubset(out.columns)

    @pytest.mark.unit
    def test_year_breakdown(self) -> None:
        out = ProviderPlugins().year_breakdown(_all_providers())
        assert out["performance_year"].to_list() == ["PY2024", "PY2025"]

    @pytest.mark.unit
    def test_entity_breakdown_sorted_desc(self) -> None:
        out = ProviderPlugins().entity_breakdown(_all_providers())
        assert out["count"][0] >= out["count"][-1]


# ---------------------------------------------------------------------------
# tin_grouped_*
# ---------------------------------------------------------------------------


class TestTinGroupedOrganizational:
    @pytest.mark.unit
    def test_empty_input(self) -> None:
        out = ProviderPlugins().tin_grouped_organizational(pl.DataFrame())
        assert out.is_empty()

    @pytest.mark.unit
    def test_no_match_returns_empty(self) -> None:
        # provider_class doesn't contain "Participant" → filter eliminates everything
        out = ProviderPlugins().tin_grouped_organizational(_preferred_df())
        # Only the row where class matches "Participant"
        assert out.height == 1
        assert out["tin"][0] == "111"

    @pytest.mark.unit
    def test_filters_to_organizational_participant(self) -> None:
        df = pl.DataFrame(
            {
                "provider_category": ["Other Category"],
                "provider_type": ["Organizational Provider"],
                "provider_class": ["Participant Provider"],
                "tin": ["1"],
                "npi": ["A"],
                "provider_name": ["x"],
                "performance_year": ["PY2024"],
                "entity_id": ["E"],
            }
        )
        # category != Preferred Provider → filtered out
        assert ProviderPlugins().tin_grouped_organizational(df).is_empty()


class TestTinGroupedIndividual:
    @pytest.mark.unit
    def test_empty_input(self) -> None:
        out = ProviderPlugins().tin_grouped_individual(pl.DataFrame(), pl.DataFrame())
        assert out.is_empty()

    @pytest.mark.unit
    def test_empty_after_category_filter(self) -> None:
        # individual_df with no rows where provider_category == "Individual Participant"
        df = pl.DataFrame(
            {"provider_category": ["X"], "tin": ["1"], "npi": ["A"], "entity_id": ["E"], "provider_name": ["n"], "performance_year": ["PY"]}
        )
        out = ProviderPlugins().tin_grouped_individual(df, _preferred_df())
        assert out.is_empty()

    @pytest.mark.unit
    def test_groups_with_org_context(self) -> None:
        out = ProviderPlugins().tin_grouped_individual(
            _individual_df(), _preferred_df()
        )
        assert out.height == 2
        assert "tin_org_class" in out.columns
        # Tin 111 has org context joined; tin 222 does not.
        as_dict = {row["tin"]: row for row in out.iter_rows(named=True)}
        assert as_dict["111"]["tin_org_provider_name"] == "AcmeOrg"
        assert as_dict["222"]["tin_org_provider_name"] is None


# ---------------------------------------------------------------------------
# tin_npi_map
# ---------------------------------------------------------------------------


class TestTinNpiMap:
    @pytest.mark.unit
    def test_empty(self) -> None:
        assert ProviderPlugins().tin_npi_map(pl.DataFrame()) == {}

    @pytest.mark.unit
    def test_groups_npis(self) -> None:
        df = pl.DataFrame({"tin": ["1", "1", "2"], "npi": ["A", "B", "C"]})
        with patch(
            "acoharmony._expressions.ProviderAlignmentExpression"
        ) as expr:
            expr.filter_non_facility_providers.return_value = pl.lit(True)
            out = ProviderPlugins().tin_npi_map(df)
        assert out == {"1": ["A", "B"], "2": ["C"]}


# ---------------------------------------------------------------------------
# search filters
# ---------------------------------------------------------------------------


class TestFreeTextFilter:
    @pytest.mark.unit
    def test_empty_term_returns_input(self) -> None:
        df = pl.DataFrame({"a": ["x"]})
        assert _free_text_filter(df, "", ["a"]).equals(df)

    @pytest.mark.unit
    def test_lowercase_match_across_columns(self) -> None:
        df = pl.DataFrame({"a": ["Apple", "banana"], "b": ["BANANA", "X"]})
        out = _free_text_filter(df, "ban", ["a", "b"])
        # Apple has BANANA in column b; banana matches column a — both rows.
        assert out.height == 2


class TestSearchHelpers:
    @pytest.mark.unit
    def test_search_individual_columns(self) -> None:
        ind = _individual_df()
        out = ProviderPlugins().search_individual(ind, "alice")
        assert out.height == 1
        assert out["provider_name"][0] == "Alice Smith"

    @pytest.mark.unit
    def test_search_preferred_columns(self) -> None:
        pref = _preferred_df()
        out = ProviderPlugins().search_preferred(pref, "ORG1")
        assert out.height == 1
