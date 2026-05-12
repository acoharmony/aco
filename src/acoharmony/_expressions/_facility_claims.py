# © 2025 HarmonyCares
# All rights reserved.

"""
Facility-claims expression builders.

Reusable expressions for joining the participant roster against the
medical-claims fact table to answer the question "which beneficiaries
received care at each ``(tin, npi)`` for a given provider facet?"

A "facet" is the pair ``(provider_category, provider_type)``. The default
facet is the one the user originally asked for:
``("Preferred Provider", "Facility and Institutional Provider")``.
All builders accept the facet as a parameter so the same machinery can
later answer the same question for any other facet (e.g. organizational
preferred providers, or individual participants).

Join key
--------
The participant roster (silver/participant_list) carries the facility
``organization_npi``. The medical-claims fact (gold/medical_claim) exposes
several NPI columns, but for facility/institutional providers only
``facility_npi`` matches at scale — ``rendering_tin``/``billing_tin`` are
sparse or absent in the gold layer. The join therefore matches on NPI
only, with ``base_provider_tin`` carried through to the output for
reporting.
"""

from __future__ import annotations

from typing import Any

import polars as pl

from .._decor8 import expression
from ._provider_alignment import ProviderAlignmentExpression
from ._registry import register_expression

# Default facet — what the user asked for.
DEFAULT_PROVIDER_CATEGORY: str = "Preferred Provider"
DEFAULT_PROVIDER_TYPE: str = "Facility and Institutional Provider"

# Column names used by the participant roster and the medical-claims fact.
# Pulled into module constants so callers/tests can mock them cleanly.
PARTICIPANT_NPI_COL: str = "organization_npi"
PARTICIPANT_TIN_COL: str = "base_provider_tin"
PARTICIPANT_NAME_COL: str = "provider_legal_business_name"
CLAIM_NPI_COL: str = "facility_npi"
CLAIM_MEMBER_COL: str = "member_id"
CLAIM_DATE_COL: str = "claim_start_date"


@register_expression(
    "facility_claims",
    schemas=["silver", "gold"],
    dataset_types=["provider", "claim"],
    callable=False,
    description="Filter and aggregate facility-grade provider claims",
)
class FacilityClaimsExpression:
    """
    Expressions for facility/institutional provider claim attribution.

    All filters are parameterized on the ``(provider_category,
    provider_type)`` facet so the same expressions answer the same
    question for any provider facet, not just facilities.
    """

    @staticmethod
    @expression(
        name="filter_provider_facet",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def filter_provider_facet(
        provider_category: str = DEFAULT_PROVIDER_CATEGORY,
        provider_type: str = DEFAULT_PROVIDER_TYPE,
    ) -> pl.Expr:
        """
        Filter participant-list rows to a ``(category, type)`` facet.

        Uses substring containment on ``provider_type`` because some
        source files include trailing qualifiers (e.g. "Facility and
        Institutional Provider - Hospital"). ``provider_category`` is
        an exact match.

        Parameters
        ----------
        provider_category : str
            Exact ``provider_category`` value. Defaults to
            ``"Preferred Provider"``.
        provider_type : str
            Substring matched against ``provider_type``. Defaults to
            ``"Facility and Institutional"`` (matches the participant
            roster's full label "Facility and Institutional Provider").
        """
        return (pl.col("provider_category") == provider_category) & (
            pl.col("provider_type").str.contains(provider_type, literal=True)
        )

    @staticmethod
    @expression(
        name="select_facet_join_columns",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def select_facet_join_columns() -> list[pl.Expr]:
        """
        Pick the columns from the participant roster that feed the join.

        The join itself is NPI-only; ``tin`` and ``provider_name`` ride
        along so they end up on every output row.
        """
        return [
            pl.col(PARTICIPANT_TIN_COL).alias("tin"),
            pl.col(PARTICIPANT_NPI_COL).alias("npi"),
            pl.col(PARTICIPANT_NAME_COL).alias("provider_name"),
            pl.col("provider_category"),
            pl.col("provider_type"),
            pl.col("provider_class"),
            pl.col("performance_year"),
            pl.col("entity_id"),
            pl.col("entity_tin"),
            pl.col("entity_legal_business_name"),
        ]

    @staticmethod
    @expression(
        name="claim_join_npi",
        tier=["silver", "gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def claim_join_npi_expr() -> pl.Expr:
        """Expose the medical_claim NPI column used as the join key."""
        return pl.col(CLAIM_NPI_COL).alias("npi")

    @staticmethod
    @expression(
        name="per_facility_rollup_aggs",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def per_facility_rollup_aggs() -> list[pl.Expr]:
        """
        Aggregations for grain A — one row per ``(tin, npi)`` facility.

        Emits:
        - ``unique_bene_count`` — distinct beneficiaries seen at this facility
        - ``claim_count`` — total claim rows attributed to this facility
        - ``first_service_date`` / ``last_service_date`` — DOS range
        - ``claim_types`` — sorted unique list of ``claim_type`` values
        - ``total_paid_amount`` — sum of ``paid_amount`` (Decimal stays Decimal)
        """
        return [
            pl.col(CLAIM_MEMBER_COL).n_unique().alias("unique_bene_count"),
            pl.len().alias("claim_count"),
            pl.col(CLAIM_DATE_COL).min().alias("first_service_date"),
            pl.col(CLAIM_DATE_COL).max().alias("last_service_date"),
            pl.col("claim_type").unique().sort().alias("claim_types"),
            pl.col("paid_amount").sum().alias("total_paid_amount"),
        ]

    @staticmethod
    @expression(
        name="per_facility_bene_aggs",
        tier=["gold"],
        idempotent=True,
        sql_enabled=True,
    )
    def per_facility_bene_aggs() -> list[pl.Expr]:
        """
        Aggregations for grain B — one row per ``(tin, npi, member_id)``.

        Emits per bene/facility:
        - ``claim_count`` — how many claims at this facility
        - ``first_service_date`` / ``last_service_date`` — DOS range
        - ``claim_types`` — sorted unique list of ``claim_type`` values
        - ``total_paid_amount`` — sum of ``paid_amount``
        """
        return [
            pl.len().alias("claim_count"),
            pl.col(CLAIM_DATE_COL).min().alias("first_service_date"),
            pl.col(CLAIM_DATE_COL).max().alias("last_service_date"),
            pl.col("claim_type").unique().sort().alias("claim_types"),
            pl.col("paid_amount").sum().alias("total_paid_amount"),
        ]

    @staticmethod
    def _materialize_provider_category(
        participants: pl.LazyFrame,
    ) -> pl.LazyFrame:
        """
        Stamp ``provider_category`` onto the participant roster if absent.

        The silver participant_list parquet doesn't carry an explicit
        ``provider_category`` column; the label is derived elsewhere by
        ``ProviderAlignmentExpression.build_provider_category_label``.
        Materializing it inline here means callers can pass the raw
        roster and the facet filter still works.
        """
        existing = participants.collect_schema().names()
        if "provider_category" in existing:
            return participants
        return participants.with_columns(
            ProviderAlignmentExpression.build_provider_category_label()
        )

    @staticmethod
    def build_facility_rollup(
        participants: pl.LazyFrame,
        claims: pl.LazyFrame,
        provider_category: str = DEFAULT_PROVIDER_CATEGORY,
        provider_type: str = DEFAULT_PROVIDER_TYPE,
    ) -> pl.LazyFrame:
        """
        Build the per-``(tin, npi)`` rollup (grain A).

        Composition helper that filters the participant roster to the
        requested facet, joins against ``claims`` on NPI, and aggregates.
        Sorted by ``unique_bene_count`` descending so the busiest
        facilities float to the top.
        """
        participants = FacilityClaimsExpression._materialize_provider_category(participants)
        # Collapse the participant roster to one row per (tin, npi) — the
        # roster repeats a facility across performance years, which would
        # otherwise double-count when joined to claims. ``performance_year``
        # becomes a sorted list of PYs in which the facility appears.
        facet_participants = (
            participants.filter(
                FacilityClaimsExpression.filter_provider_facet(
                    provider_category, provider_type
                )
            )
            .group_by([PARTICIPANT_TIN_COL, PARTICIPANT_NPI_COL])
            .agg(
                pl.col(PARTICIPANT_NAME_COL).first().alias("provider_name"),
                pl.col("provider_category").first().alias("provider_category"),
                pl.col("provider_type").first().alias("provider_type"),
                pl.col("provider_class").first().alias("provider_class"),
                pl.col("performance_year").unique().sort().alias("performance_years"),
                pl.col("entity_id").first().alias("entity_id"),
                pl.col("entity_tin").first().alias("entity_tin"),
                pl.col("entity_legal_business_name")
                .first()
                .alias("entity_legal_business_name"),
            )
            .rename({PARTICIPANT_TIN_COL: "tin", PARTICIPANT_NPI_COL: "npi"})
        )
        joined = claims.join(
            facet_participants,
            left_on=CLAIM_NPI_COL,
            right_on="npi",
            how="inner",
        )
        return (
            joined.group_by(
                [
                    "tin",
                    pl.col(CLAIM_NPI_COL).alias("npi"),
                    "provider_name",
                    "provider_category",
                    "provider_type",
                    "provider_class",
                    "performance_years",
                    "entity_id",
                    "entity_tin",
                    "entity_legal_business_name",
                ]
            )
            .agg(FacilityClaimsExpression.per_facility_rollup_aggs())
            .sort("unique_bene_count", descending=True)
        )

    @staticmethod
    def build_facility_bene_detail(
        participants: pl.LazyFrame,
        claims: pl.LazyFrame,
        provider_category: str = DEFAULT_PROVIDER_CATEGORY,
        provider_type: str = DEFAULT_PROVIDER_TYPE,
    ) -> pl.LazyFrame:
        """
        Build the per-``(tin, npi, member_id)`` detail (grain B).

        Same join as :meth:`build_facility_rollup`, but the aggregation
        also groups by ``member_id`` so the output has one row per
        bene-at-facility.
        """
        participants = FacilityClaimsExpression._materialize_provider_category(participants)
        # Same dedup as the rollup helper — see :meth:`build_facility_rollup`.
        facet_participants = (
            participants.filter(
                FacilityClaimsExpression.filter_provider_facet(
                    provider_category, provider_type
                )
            )
            .group_by([PARTICIPANT_TIN_COL, PARTICIPANT_NPI_COL])
            .agg(
                pl.col(PARTICIPANT_NAME_COL).first().alias("provider_name"),
                pl.col("performance_year").unique().sort().alias("performance_years"),
                pl.col("entity_id").first().alias("entity_id"),
                pl.col("entity_legal_business_name")
                .first()
                .alias("entity_legal_business_name"),
            )
            .rename({PARTICIPANT_TIN_COL: "tin", PARTICIPANT_NPI_COL: "npi"})
        )
        joined = claims.join(
            facet_participants,
            left_on=CLAIM_NPI_COL,
            right_on="npi",
            how="inner",
        )
        return (
            joined.group_by(
                [
                    "tin",
                    pl.col(CLAIM_NPI_COL).alias("npi"),
                    "provider_name",
                    CLAIM_MEMBER_COL,
                    "performance_years",
                    "entity_id",
                    "entity_legal_business_name",
                ]
            )
            .agg(FacilityClaimsExpression.per_facility_bene_aggs())
            .sort(["tin", "npi", "claim_count"], descending=[False, False, True])
        )
