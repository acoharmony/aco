# © 2025 HarmonyCares
# All rights reserved.

"""
Provider TIN/NPI rollups for the participant-list dashboard.

Distinguishes Individual Participants (clinicians, used for voluntary
alignment) from Preferred Providers (organizations, used for
claims-based attribution); rolls them up by category, performance
year, and entity; and produces TIN-grouped views with the NPIs that
share each TIN.

The expression-level filters live in
``acoharmony._expressions.ProviderAlignmentExpression``; this module
just composes them into dashboard-shaped frames.
"""

from __future__ import annotations

import polars as pl

from ._base import PluginRegistry


def _free_text_filter(df: pl.DataFrame, term: str, columns: list[str]) -> pl.DataFrame:
    """Lowercase-contains across the named columns; ``term`` is empty → no-op."""
    if not term:
        return df
    needle = term.lower()
    expr = pl.lit(False)
    for col in columns:
        expr = expr | pl.col(col).str.to_lowercase().str.contains(needle)
    return df.filter(expr)


class ProviderPlugins(PluginRegistry):
    """Participant-list TIN/NPI extraction + grouped rollups."""

    # ---- extraction (uses ProviderAlignmentExpression internally) ------

    def extract_individual(self, df: pl.DataFrame) -> pl.DataFrame:
        from acoharmony._expressions import ProviderAlignmentExpression

        if df.height == 0:
            return pl.DataFrame()
        return (
            df.lazy()
            .filter(ProviderAlignmentExpression.filter_has_individual_npi())
            .select(ProviderAlignmentExpression.select_individual_participant_columns())
            .collect()
        )

    def extract_preferred(self, df: pl.DataFrame) -> pl.DataFrame:
        from acoharmony._expressions import ProviderAlignmentExpression

        if df.height == 0:
            return pl.DataFrame()
        return (
            df.lazy()
            .filter(ProviderAlignmentExpression.filter_has_organization_npi())
            .select(ProviderAlignmentExpression.select_preferred_provider_columns())
            .collect()
        )

    def combine(
        self,
        individual: pl.DataFrame,
        preferred: pl.DataFrame,
    ) -> pl.DataFrame:
        if individual.height == 0 and preferred.height == 0:
            return pl.DataFrame()
        if individual.height == 0:
            return preferred
        if preferred.height == 0:
            return individual
        return pl.concat([individual, preferred], how="vertical")

    # ---- rollups -------------------------------------------------------

    def category_breakdown(self, all_providers: pl.DataFrame) -> pl.DataFrame:
        return (
            all_providers.group_by(["provider_category", "provider_class"])
            .agg(
                pl.len().alias("count"),
                pl.col("tin").n_unique().alias("unique_tins"),
                pl.col("npi").n_unique().alias("unique_npis"),
            )
            .sort(["provider_category", "count"], descending=[False, True])
        )

    def year_breakdown(self, all_providers: pl.DataFrame) -> pl.DataFrame:
        return (
            all_providers.group_by("performance_year")
            .agg(
                pl.len().alias("count"),
                pl.col("tin").n_unique().alias("unique_tins"),
                pl.col("npi").n_unique().alias("unique_npis"),
            )
            .sort("performance_year")
        )

    def entity_breakdown(self, all_providers: pl.DataFrame) -> pl.DataFrame:
        return (
            all_providers.group_by(["entity_id", "organization"])
            .agg(
                pl.len().alias("count"),
                pl.col("tin").n_unique().alias("unique_tins"),
                pl.col("npi").n_unique().alias("unique_npis"),
            )
            .sort("count", descending=True)
        )

    # ---- TIN-grouped views --------------------------------------------

    def tin_grouped_organizational(self, preferred: pl.DataFrame) -> pl.DataFrame:
        """Preferred Provider × Organizational × Participant rows grouped by TIN."""
        if preferred.is_empty():
            return pl.DataFrame()
        organizational = preferred.filter(
            (pl.col("provider_category") == "Preferred Provider")
            & (pl.col("provider_type").str.contains("Organizational", literal=True))
            & (pl.col("provider_class").str.contains("Participant", literal=True))
        )
        if organizational.height == 0:
            return pl.DataFrame()
        return (
            organizational.group_by(["tin", "entity_id"])
            .agg(
                pl.col("npi").unique().sort().alias("org_npi"),
                pl.col("npi").n_unique().alias("org_npi_count"),
                pl.col("provider_name").unique().sort().alias("provider_names"),
                pl.col("performance_year").first().alias("performance_year"),
            )
            .sort("org_npi_count", descending=True)
        )

    def tin_grouped_individual(
        self,
        individual: pl.DataFrame,
        preferred: pl.DataFrame,
    ) -> pl.DataFrame:
        """Individual Participant rows grouped by TIN, joined with org context."""
        if individual.is_empty():
            return pl.DataFrame()
        ind = individual.filter(pl.col("provider_category") == "Individual Participant")
        if ind.height == 0:
            return pl.DataFrame()
        org_context = (
            preferred.filter(
                (pl.col("provider_category") == "Preferred Provider")
                & (pl.col("provider_type").str.contains("Organizational", literal=True))
            )
            .select("tin", "provider_name", "provider_class")
            .rename(
                {"provider_name": "tin_org_provider_name", "provider_class": "tin_org_class"}
            )
            .unique(subset=["tin"])
        )
        return (
            ind.group_by(["tin", "entity_id"])
            .agg(
                pl.col("npi").unique().sort().alias("individual_npi"),
                pl.col("npi").n_unique().alias("individual_npi_count"),
                pl.col("provider_name").unique().sort().alias("individual_provider_names"),
                pl.col("performance_year").first().alias("performance_year"),
            )
            .join(org_context, on="tin", how="left")
            .sort(["tin_org_class", "individual_npi_count"], descending=[False, True])
        )

    # ---- TIN→NPI mapping (non-facility only) --------------------------

    def tin_npi_map(self, df: pl.DataFrame) -> dict[str, list[str]]:
        from acoharmony._expressions import ProviderAlignmentExpression

        if df.height == 0:
            return {}
        non_facility = (
            df.lazy()
            .filter(ProviderAlignmentExpression.filter_non_facility_providers())
            .collect()
        )
        grouped = (
            non_facility.group_by("tin")
            .agg(pl.col("npi").unique().sort())
            .sort("tin")
        )
        return {row["tin"]: row["npi"] for row in grouped.iter_rows(named=True)}

    # ---- search filters used by the dashboard --------------------------

    def search_individual(self, df: pl.DataFrame, term: str) -> pl.DataFrame:
        return _free_text_filter(df, term, ["provider_name", "npi", "tin"])

    def search_preferred(self, df: pl.DataFrame, term: str) -> pl.DataFrame:
        return _free_text_filter(df, term, ["organization", "npi", "tin"])
