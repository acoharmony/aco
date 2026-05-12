"""Tests for acoharmony._expressions._facility_claims."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from datetime import date
from decimal import Decimal

import polars as pl
import pytest

from acoharmony._expressions._facility_claims import (
    DEFAULT_PROVIDER_CATEGORY,
    DEFAULT_PROVIDER_TYPE,
    FacilityClaimsExpression,
)


def _participants_fixture() -> pl.LazyFrame:
    """Two facility prefereds + one organizational preferred + one individual.

    The two facility rows share the same (tin, npi) across performance years to
    exercise the dedupe path in build_facility_rollup / build_facility_bene_detail.
    """
    return pl.LazyFrame(
        {
            "base_provider_tin": ["111", "111", "222", "333", "444"],
            "organization_npi": [
                "NPI_FAC1",  # facility (PY2025)
                "NPI_FAC1",  # same facility (PY2026)
                "NPI_FAC2",  # facility w/ no claims
                "NPI_ORG",   # organizational, NOT facility
                None,        # individual participant (no org NPI)
            ],
            "individual_npi": [None, None, None, None, "NPI_IND"],
            "provider_legal_business_name": [
                "Facility One LLC", "Facility One LLC",
                "Facility Two", "Org Group", None,
            ],
            "provider_type": [
                "Facility and Institutional Provider",
                "Facility and Institutional Provider",
                "Facility and Institutional Provider",
                "Organizational Provider",
                "Individual Practitioner/Professional",
            ],
            "provider_class": ["Preferred Provider"] * 4 + ["Participant Provider"],
            "performance_year": ["PY2025", "PY2026", "PY2025", "PY2025", "PY2025"],
            "entity_id": ["D0259"] * 5,
            "entity_tin": ["881823607"] * 5,
            "entity_legal_business_name": ["HarmonyCares ACO LLC"] * 5,
        }
    )


def _claims_fixture() -> pl.LazyFrame:
    """Six claims: 4 at NPI_FAC1 across 2 benes, 1 at NPI_ORG, 1 unrelated."""
    return pl.LazyFrame(
        {
            "facility_npi": [
                "NPI_FAC1", "NPI_FAC1", "NPI_FAC1", "NPI_FAC1",
                "NPI_ORG",
                "NPI_UNRELATED",
            ],
            "member_id": ["B1", "B1", "B1", "B2", "B3", "B4"],
            "claim_start_date": [
                date(2024, 1, 5), date(2024, 3, 10), date(2025, 2, 1), date(2024, 6, 15),
                date(2024, 4, 4),
                date(2025, 5, 5),
            ],
            "claim_type": [
                "institutional", "institutional", "institutional", "institutional",
                "professional",
                "institutional",
            ],
            "paid_amount": [
                Decimal("100.00"), Decimal("200.00"), Decimal("50.00"), Decimal("400.00"),
                Decimal("999.00"),
                Decimal("1.00"),
            ],
        }
    )


class TestFilterProviderFacet:
    @pytest.mark.unit
    def test_default_facet_matches_facility_preferred_only(self):
        df = _participants_fixture().with_columns(
            pl.when(pl.col("organization_npi").is_not_null())
            .then(pl.lit("Preferred Provider"))
            .otherwise(pl.lit("Individual Participant"))
            .alias("provider_category")
        )
        result = df.filter(FacilityClaimsExpression.filter_provider_facet()).collect()
        # Three facility-and-institutional rows survive (the two PY-dup'd
        # NPI_FAC1 rows + NPI_FAC2). The Organizational and Individual rows
        # are filtered out.
        assert sorted(result["organization_npi"].drop_nulls().to_list()) == [
            "NPI_FAC1",
            "NPI_FAC1",
            "NPI_FAC2",
        ]

    @pytest.mark.unit
    def test_substring_match_on_provider_type(self):
        """Substring match catches trailing qualifiers."""
        df = pl.DataFrame(
            {
                "provider_category": ["Preferred Provider"] * 2,
                "provider_type": [
                    "Facility and Institutional Provider - Hospital",
                    "Individual Practitioner/Professional",
                ],
            }
        )
        result = df.filter(FacilityClaimsExpression.filter_provider_facet())
        assert result.height == 1
        assert "Hospital" in result["provider_type"][0]

    @pytest.mark.unit
    def test_facet_parameters_override_default(self):
        df = pl.DataFrame(
            {
                "provider_category": ["Preferred Provider", "Preferred Provider"],
                "provider_type": ["Organizational Provider", "Facility and Institutional Provider"],
            }
        )
        result = df.filter(
            FacilityClaimsExpression.filter_provider_facet(
                provider_category="Preferred Provider",
                provider_type="Organizational",
            )
        )
        assert result["provider_type"].to_list() == ["Organizational Provider"]


class TestBuildFacilityRollup:
    @pytest.mark.unit
    def test_rollup_one_row_per_tin_npi_with_dedupe_across_py(self):
        participants = _participants_fixture()
        claims = _claims_fixture()
        result = FacilityClaimsExpression.build_facility_rollup(participants, claims).collect()
        # Exactly one row per (tin, npi); NPI_FAC1 appears once despite
        # being in two performance years on the participant side.
        keys = sorted({(r["tin"], r["npi"]) for r in result.iter_rows(named=True)})
        assert ("111", "NPI_FAC1") in keys
        # NPI_FAC2 had no claims → not in rollup (inner join).
        assert ("222", "NPI_FAC2") not in keys
        # The org-type preferred provider matched in claims but the facet
        # filter excludes it.
        assert all(npi != "NPI_ORG" for _, npi in keys)
        # Performance years carried as a sorted list of distinct values.
        fac1 = result.filter(pl.col("npi") == "NPI_FAC1").row(0, named=True)
        assert fac1["performance_years"] == ["PY2025", "PY2026"]

    @pytest.mark.unit
    def test_rollup_aggregates(self):
        result = (
            FacilityClaimsExpression.build_facility_rollup(
                _participants_fixture(), _claims_fixture()
            )
            .collect()
            .filter(pl.col("npi") == "NPI_FAC1")
            .row(0, named=True)
        )
        assert result["unique_bene_count"] == 2  # B1 + B2
        assert result["claim_count"] == 4
        assert result["first_service_date"] == date(2024, 1, 5)
        assert result["last_service_date"] == date(2025, 2, 1)
        assert result["claim_types"] == ["institutional"]
        assert result["total_paid_amount"] == Decimal("750.00")


class TestBuildFacilityBeneDetail:
    @pytest.mark.unit
    def test_bene_detail_one_row_per_bene_at_facility(self):
        result = FacilityClaimsExpression.build_facility_bene_detail(
            _participants_fixture(), _claims_fixture()
        ).collect()
        rows = {(r["npi"], r["member_id"]): r for r in result.iter_rows(named=True)}
        # B1 and B2 at NPI_FAC1; NPI_ORG / NPI_UNRELATED filtered out by facet+facility-NPI.
        assert set(rows.keys()) == {("NPI_FAC1", "B1"), ("NPI_FAC1", "B2")}
        b1 = rows[("NPI_FAC1", "B1")]
        assert b1["claim_count"] == 3
        assert b1["first_service_date"] == date(2024, 1, 5)
        assert b1["last_service_date"] == date(2025, 2, 1)
        assert b1["total_paid_amount"] == Decimal("350.00")


class TestMaterializeProviderCategory:
    @pytest.mark.unit
    def test_skips_when_provider_category_already_present(self):
        """If the column is already there, the helper is a no-op."""
        df = pl.LazyFrame(
            {
                "provider_category": ["Pre-Existing"],
                "individual_npi": ["X"],
                "organization_npi": [None],
            }
        )
        result = FacilityClaimsExpression._materialize_provider_category(df).collect()
        assert result["provider_category"].to_list() == ["Pre-Existing"]

    @pytest.mark.unit
    def test_derives_label_when_missing(self):
        df = pl.LazyFrame(
            {
                "individual_npi": ["X", None, None],
                "organization_npi": [None, "Y", None],
            }
        )
        result = FacilityClaimsExpression._materialize_provider_category(df).collect()
        assert result["provider_category"].to_list() == [
            "Individual Participant",
            "Preferred Provider",
            "Unknown",
        ]


class TestDefaults:
    @pytest.mark.unit
    def test_default_facet_is_what_we_promised(self):
        assert DEFAULT_PROVIDER_CATEGORY == "Preferred Provider"
        assert DEFAULT_PROVIDER_TYPE == "Facility and Institutional Provider"
