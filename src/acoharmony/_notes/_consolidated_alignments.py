# © 2025 HarmonyCares
# All rights reserved.

"""
Consolidated alignment dashboard analytics.

Backs ``notebooks/consolidated_alignments.py``: thin facade over the
``acoharmony._transforms._notebook_*`` modules that already contain the
bulk of the analytic logic for program-distribution, enrollment trends,
transitions, voluntary-alignment outreach, office stats, vintage, and
cohort analysis.
"""

from __future__ import annotations

import re
from datetime import date
from pathlib import Path
from typing import Any

import polars as pl
from dateutil.relativedelta import relativedelta

from ._base import PluginRegistry

_SVA_CANONICAL_COLUMNS = [
    "aco_id",
    "bene_mbi",
    "bene_first_name",
    "bene_last_name",
    "bene_street_address",
    "city",
    "state",
    "zip",
    "provider_name",
    "sva_provider_name",
    "sva_npi",
    "sva_tin",
    "sva_signature_date",
    "sva_response_code",
    "processed_at",
    "source_file",
    "source_filename",
    "file_date",
    "medallion_layer",
]


_SVA_COLUMN_CANDIDATES = {
    "aco_id": ("aco_id",),
    "bene_mbi": ("bene_mbi", "beneficiary_s_mbi"),
    "bene_first_name": ("bene_first_name", "beneficiary_s_first_name"),
    "bene_last_name": ("bene_last_name", "beneficiary_s_last_name"),
    "bene_street_address": ("bene_street_address", "beneficiary_s_street_address"),
    "city": ("city",),
    "state": ("state",),
    "zip": ("zip",),
    "provider_name": (
        "provider_name",
        "provider_name_primary_place_the_beneficiary_receives_care_as_it_appears_on_the_signed_sva_letter",
    ),
    "sva_provider_name": (
        "sva_provider_name",
        "name_of_individual_participant_provider_associated_w_attestation",
    ),
    "sva_npi": ("sva_npi", "i_npi_for_individual_participant_provider_column_j"),
    "sva_tin": ("sva_tin", "tin_for_individual_participant_provider_column_j"),
    "sva_signature_date": ("sva_signature_date", "signature_date_on_sva_letter"),
    "sva_response_code": ("sva_response_code", "response_code_cms_to_fill_out"),
}


def _month_bounds(year_month: str) -> tuple[date, date]:
    year = int(year_month[:4])
    month = int(year_month[4:6])
    if month == 12:
        next_month = date(year + 1, 1, 1)
    else:
        next_month = date(year, month + 1, 1)
    return date(year, month, 1), next_month - relativedelta(days=1)


def _sva_column_key(column_name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", column_name.lower()).strip("_")


def _sva_file_date_from_name(path: Path) -> date | None:
    match = re.search(r"SVA(\d{8})", path.name, flags=re.IGNORECASE)
    if match is None:
        return None
    raw = match.group(1)
    try:
        return date(int(raw[:4]), int(raw[4:6]), int(raw[6:8]))
    except ValueError:
        return None


def _empty_sva_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "aco_id": pl.Utf8,
            "bene_mbi": pl.Utf8,
            "bene_first_name": pl.Utf8,
            "bene_last_name": pl.Utf8,
            "bene_street_address": pl.Utf8,
            "city": pl.Utf8,
            "state": pl.Utf8,
            "zip": pl.Utf8,
            "provider_name": pl.Utf8,
            "sva_provider_name": pl.Utf8,
            "sva_npi": pl.Utf8,
            "sva_tin": pl.Utf8,
            "sva_signature_date": pl.Date,
            "sva_response_code": pl.Utf8,
            "processed_at": pl.Datetime,
            "source_file": pl.Utf8,
            "source_filename": pl.Utf8,
            "file_date": pl.Date,
            "medallion_layer": pl.Utf8,
        }
    )


def _sva_text_expr(lookup: dict[str, str], output: str) -> pl.Expr:
    parts = [
        pl.col(lookup[candidate]).cast(pl.Utf8, strict=False)
        for candidate in _SVA_COLUMN_CANDIDATES[output]
        if candidate in lookup
    ]
    expr = pl.coalesce(parts) if parts else pl.lit(None, dtype=pl.Utf8)
    cleaned = expr.str.strip_chars()
    return pl.when(cleaned == "").then(None).otherwise(cleaned).alias(output)


def _sva_date_expr(lookup: dict[str, str], output: str) -> pl.Expr:
    parts = [
        pl.col(lookup[candidate]).cast(pl.Utf8, strict=False)
        for candidate in _SVA_COLUMN_CANDIDATES[output]
        if candidate in lookup
    ]
    raw = pl.coalesce(parts) if parts else pl.lit(None, dtype=pl.Utf8)
    text = raw.str.strip_chars()
    parsed = pl.coalesce(
        [
            text.str.to_date("%Y-%m-%d", strict=False),
            text.str.to_date("%m/%d/%Y", strict=False),
            text.str.to_date("%-m/%-d/%Y", strict=False),
            text.str.to_date("%m/%d/%y", strict=False),
            text.str.to_date("%-m/%-d/%y", strict=False),
        ]
    )
    return pl.when((text.is_null()) | (text == "")).then(None).otherwise(parsed).alias(output)


class ConsolidatedAlignmentsPlugins(PluginRegistry):
    """Consolidated alignment dashboard analytics."""

    # ---- loading ----------------------------------------------------

    def load_consolidated(self, gold_path: Path, silver_path: Path | None = None) -> pl.LazyFrame:
        path = Path(gold_path) / "consolidated_alignment.parquet"
        if not path.exists():
            return pl.LazyFrame()
        df = pl.scan_parquet(str(path))
        if silver_path is None:
            return df

        last_ffs_path = Path(silver_path) / "last_ffs_service.parquet"
        schema = df.collect_schema().names()
        if not last_ffs_path.exists() or "last_ffs_date" in schema or "current_mbi" not in schema:
            return df

        last_ffs = (
            pl.scan_parquet(str(last_ffs_path))
            .select([pl.col("bene_mbi").alias("current_mbi"), "last_ffs_date"])
            .unique(subset=["current_mbi"], keep="first")
        )
        return df.join(last_ffs, on="current_mbi", how="left")

    def load_emails(self, silver_path: Path) -> pl.LazyFrame:
        path = Path(silver_path) / "emails.parquet"
        return pl.scan_parquet(str(path)) if path.exists() else pl.LazyFrame()

    def load_mailed(self, silver_path: Path) -> pl.LazyFrame:
        path = Path(silver_path) / "mailed.parquet"
        return pl.scan_parquet(str(path)) if path.exists() else pl.LazyFrame()

    def load_sva(self, silver_path: Path, bronze_path: Path | None = None) -> pl.LazyFrame:
        """Load silver SVA and augment it with a newer raw CMS workbook when present."""
        silver_file = Path(silver_path) / "sva.parquet"
        silver = (
            pl.scan_parquet(str(silver_file)) if silver_file.exists() else _empty_sva_frame().lazy()
        )

        if bronze_path is None:
            return silver

        bronze_files = [
            path
            for path in Path(bronze_path).glob("*SVA????????.xlsx")
            if _sva_file_date_from_name(path) is not None
        ]
        if not bronze_files:
            return silver

        latest_bronze = max(
            bronze_files, key=lambda path: _sva_file_date_from_name(path) or date.min
        )
        latest_bronze_date = _sva_file_date_from_name(latest_bronze)
        if latest_bronze_date is None:
            return silver

        silver_schema = silver.collect_schema().names()
        silver_latest = None
        if "file_date" in silver_schema:
            try:
                silver_latest = (
                    silver.select(pl.col("file_date").cast(pl.Date, strict=False).max())
                    .collect()
                    .item()
                )
            except Exception:  # ALLOWED: stale or malformed local parquet fallback
                silver_latest = None

        if silver_latest is not None and silver_latest >= latest_bronze_date:
            return silver

        try:
            raw = pl.read_excel(latest_bronze, sheet_name="SVA_DATA")
        except Exception:  # ALLOWED: if the ad hoc raw workbook is unreadable, use silver
            return silver

        lookup = {_sva_column_key(column_name): column_name for column_name in raw.columns}
        required = {"aco_id", "bene_mbi", "sva_signature_date"}
        if not all(
            any(candidate in lookup for candidate in _SVA_COLUMN_CANDIDATES[column])
            for column in required
        ):
            return silver

        raw_normalized = (
            raw.lazy()
            .select(
                [
                    _sva_text_expr(lookup, "aco_id"),
                    _sva_text_expr(lookup, "bene_mbi"),
                    _sva_text_expr(lookup, "bene_first_name"),
                    _sva_text_expr(lookup, "bene_last_name"),
                    _sva_text_expr(lookup, "bene_street_address"),
                    _sva_text_expr(lookup, "city"),
                    _sva_text_expr(lookup, "state"),
                    _sva_text_expr(lookup, "zip"),
                    _sva_text_expr(lookup, "provider_name"),
                    _sva_text_expr(lookup, "sva_provider_name"),
                    _sva_text_expr(lookup, "sva_npi"),
                    _sva_text_expr(lookup, "sva_tin"),
                    _sva_date_expr(lookup, "sva_signature_date"),
                    _sva_text_expr(lookup, "sva_response_code"),
                    pl.lit(None, dtype=pl.Datetime).alias("processed_at"),
                    pl.lit("sva").alias("source_file"),
                    pl.lit(latest_bronze.name).alias("source_filename"),
                    pl.lit(latest_bronze_date).alias("file_date"),
                    pl.lit("bronze").alias("medallion_layer"),
                ]
            )
            .with_columns(
                [
                    pl.col("aco_id").str.to_uppercase(),
                    pl.col("bene_mbi").str.replace_all(r"\s+", "").str.to_uppercase(),
                    pl.col("state").str.to_uppercase(),
                    pl.col("sva_npi").str.replace(r"\.0$", ""),
                    pl.col("sva_tin").str.replace(r"\.0$", ""),
                    pl.col("zip").str.replace(r"\.0$", ""),
                    pl.col("sva_response_code").str.to_uppercase(),
                ]
            )
            .filter(
                pl.col("aco_id").str.contains(r"^D\d{4}$").fill_null(False)
                & pl.col("bene_mbi").str.contains(r"^[A-Z0-9]{11}$").fill_null(False)
            )
            .select(_SVA_CANONICAL_COLUMNS)
            .collect()
        )
        if raw_normalized.is_empty():
            return silver

        if not silver_file.exists():
            return raw_normalized.lazy()

        silver_df = silver.collect()
        return (
            pl.concat([silver_df, raw_normalized], how="diagonal_relaxed")
            .select(_SVA_CANONICAL_COLUMNS)
            .unique(subset=["bene_mbi", "sva_signature_date", "file_date"], keep="last")
            .lazy()
        )

    # ---- filters / utilities -----------------------------------------

    def living_filter(self, df: pl.LazyFrame | None) -> pl.Expr:
        """Filter expression: living beneficiaries only (handles missing death cols)."""
        if df is None:
            return pl.lit(True)
        try:
            schema_names = df.collect_schema().names()
        except Exception:  # ALLOWED: empty/invalid LazyFrame
            return pl.lit(True)
        cond = pl.lit(True)
        if "death_date" in schema_names:
            cond = cond & pl.col("death_date").is_null()
        if "bene_death_date" in schema_names:
            cond = cond & pl.col("bene_death_date").is_null()
        if "bene_date_of_death" in schema_names:
            cond = cond & pl.col("bene_date_of_death").is_null()
        return cond

    def extract_year_months(
        self, df: pl.LazyFrame, programs: tuple[str, ...] = ("reach", "mssp")
    ) -> tuple[str | None, list[str]]:
        schema = df.collect_schema().names()
        ym_cols = [c for c in schema if c.startswith("ym_")]
        if not ym_cols:
            return None, []
        year_months = sorted({c.split("_")[1] for c in ym_cols})
        active_counts = []
        for ym in year_months:
            flags = [
                pl.col(f"ym_{ym}_{program}").fill_null(False)
                for program in programs
                if f"ym_{ym}_{program}" in schema
            ]
            if flags:
                active_counts.append(pl.sum_horizontal(flags).sum().alias(ym))
        if active_counts:
            try:
                counts = (
                    df.filter(self.living_filter(df))
                    .select(active_counts)
                    .collect()
                    .row(0, named=True)
                )
                nonzero_months = [ym for ym in year_months if (counts.get(ym) or 0) > 0]
                if nonzero_months:
                    first_idx = year_months.index(nonzero_months[0])
                    last_idx = year_months.index(nonzero_months[-1])
                    year_months = year_months[first_idx : last_idx + 1]
            except Exception:  # ALLOWED: fall back to schema-derived months in notebooks
                pass
        return year_months[-1], year_months

    def basic_stats(self, df: pl.LazyFrame) -> dict[str, Any]:
        from acoharmony._transforms._notebook_utilities import calculate_basic_stats

        return calculate_basic_stats(df)

    # ---- distributions ----------------------------------------------

    def historical_program_distribution(self, df: pl.LazyFrame) -> pl.DataFrame:
        from acoharmony._transforms._notebook_utilities import (
            calculate_historical_program_distribution,
        )

        return calculate_historical_program_distribution(df)

    def current_program_distribution(
        self, df: pl.LazyFrame, most_recent_ym: str | None
    ) -> pl.DataFrame:
        from acoharmony._transforms._notebook_utilities import (
            calculate_current_program_distribution,
        )

        return calculate_current_program_distribution(df, most_recent_ym)

    # ---- voluntary alignment outreach ---------------------------------

    def voluntary_outreach_data(
        self, emails_df: pl.LazyFrame, mailed_df: pl.LazyFrame
    ) -> dict[str, pl.LazyFrame]:
        """Return ``{email_by_campaign, email_mbis, mailed_by_campaign, mailed_mbis}``."""
        from acoharmony._transforms._notebook_utilities import (
            prepare_voluntary_outreach_data,
        )

        email_by_campaign, email_mbis, mailed_by_campaign, mailed_mbis = (
            prepare_voluntary_outreach_data(emails_df, mailed_df)
        )
        return {
            "email_by_campaign": email_by_campaign,
            "email_mbis": email_mbis,
            "mailed_by_campaign": mailed_by_campaign,
            "mailed_mbis": mailed_mbis,
        }

    def quarterly_campaign_effectiveness(
        self,
        df_enriched: pl.LazyFrame,
        email_by_campaign: pl.LazyFrame,
        mailed_by_campaign: pl.LazyFrame,
    ) -> pl.DataFrame:
        from acoharmony._transforms._notebook_outreach import (
            calculate_quarterly_campaign_effectiveness,
        )

        return calculate_quarterly_campaign_effectiveness(
            df_enriched, email_by_campaign, mailed_by_campaign
        )

    def office_campaign_effectiveness(
        self,
        df_enriched: pl.LazyFrame,
        email_by_campaign: pl.LazyFrame,
        mailed_by_campaign: pl.LazyFrame,
    ) -> pl.DataFrame:
        from acoharmony._transforms._notebook_outreach import (
            calculate_office_campaign_effectiveness,
        )

        return calculate_office_campaign_effectiveness(
            df_enriched, email_by_campaign, mailed_by_campaign
        )

    def enhanced_campaign_performance(
        self, emails_df: pl.LazyFrame, mailed_df: pl.LazyFrame
    ) -> dict[str, Any]:
        from acoharmony._transforms._notebook_outreach import (
            calculate_enhanced_campaign_performance,
        )

        return calculate_enhanced_campaign_performance(emails_df, mailed_df)

    # ---- enrollment + trends ------------------------------------------

    def selected_month_enrollment(
        self, df: pl.LazyFrame, selected_ym: str | None
    ) -> dict[str, int] | None:
        if not selected_ym:
            return None
        df_active = df.filter(self.living_filter(df))
        schema = df_active.collect_schema().names()

        def status_expr(col: str) -> pl.Expr:
            if col in schema:
                return pl.col(col).fill_null(False)
            return pl.lit(False)

        reach = status_expr(f"ym_{selected_ym}_reach")
        mssp = status_expr(f"ym_{selected_ym}_mssp")
        if "last_ffs_date" in schema:
            _, month_end = _month_bounds(selected_ym)
            lookback_start = month_end - relativedelta(months=24)
            ffs = (
                pl.col("last_ffs_date")
                .cast(pl.Date, strict=False)
                .is_between(lookback_start, month_end, closed="both")
                .fill_null(False)
                & ~reach
                & ~mssp
            )
        else:
            ffs = status_expr(f"ym_{selected_ym}_ffs") & ~reach & ~mssp
        normalized = df_active.with_columns(
            [
                reach.alias("_selected_reach"),
                mssp.alias("_selected_mssp"),
                ffs.alias("_selected_ffs"),
            ]
        )
        id_column = next(
            (col for col in ("current_mbi", "bene_mbi", "mbi") if col in schema),
            None,
        )
        if id_column:
            population = normalized.group_by(id_column).agg(
                [
                    pl.col("_selected_reach").any(),
                    pl.col("_selected_mssp").any(),
                    pl.col("_selected_ffs").any(),
                ]
            )
        else:
            population = normalized.select(["_selected_reach", "_selected_mssp", "_selected_ffs"])
        result = population.select(
            [
                pl.col("_selected_reach").sum().alias("REACH"),
                pl.col("_selected_mssp").sum().alias("MSSP"),
                (pl.col("_selected_ffs") & ~pl.col("_selected_reach") & ~pl.col("_selected_mssp"))
                .sum()
                .alias("FFS"),
                (~(pl.col("_selected_reach") | pl.col("_selected_mssp") | pl.col("_selected_ffs")))
                .sum()
                .alias("Not Enrolled"),
                pl.len().alias("Living Beneficiaries"),
            ]
        ).collect()
        return {key: int(value or 0) for key, value in result.row(0, named=True).items()}

    def alignment_trends(self, df: pl.LazyFrame, year_months: list[str]) -> pl.DataFrame | None:
        from acoharmony._transforms._notebook_trends import (
            calculate_alignment_trends_over_time,
        )

        return calculate_alignment_trends_over_time(df, year_months)

    def transitions(self, df: pl.LazyFrame, prev_ym: str, curr_ym: str) -> pl.DataFrame:
        from acoharmony._transforms._notebook_transitions import (
            calculate_alignment_transitions,
        )

        result = calculate_alignment_transitions(df, curr_ym, [prev_ym, curr_ym])
        if isinstance(result, tuple):
            transitions, _, _ = result
        else:
            transitions = result
        return transitions if transitions is not None else pl.DataFrame()

    # ---- office breakdowns -------------------------------------------

    def office_enrollment(self, df: pl.LazyFrame, selected_ym: str) -> pl.DataFrame | None:
        from acoharmony._transforms._notebook_office_stats import (
            calculate_office_enrollment_stats,
        )

        return calculate_office_enrollment_stats(df, selected_ym)

    def office_alignment_types(self, df: pl.LazyFrame, selected_ym: str) -> pl.DataFrame | None:
        from acoharmony._transforms._notebook_office_stats import (
            calculate_office_alignment_types,
        )

        return calculate_office_alignment_types(df, selected_ym)

    def office_program_distribution(
        self, df: pl.LazyFrame, selected_ym: str
    ) -> pl.DataFrame | None:
        from acoharmony._transforms._notebook_office_stats import (
            calculate_office_program_distribution,
        )

        return calculate_office_program_distribution(df, selected_ym)

    def office_transitions(self, df: pl.LazyFrame) -> pl.DataFrame | None:
        from acoharmony._transforms._notebook_office_stats import (
            calculate_office_transition_stats,
        )

        return calculate_office_transition_stats(df)

    # ---- SVA -----------------------------------------------------------

    def sva_action_categories(self, df_enriched: pl.LazyFrame) -> pl.DataFrame:
        return (
            df_enriched.group_by("sva_action_needed")
            .agg(pl.len().alias("count"))
            .collect()
            .sort("count", descending=True)
        )

    # ---- sample preview ---------------------------------------------

    def sample(self, df: pl.LazyFrame, sample_size: int = 100) -> pl.DataFrame:
        desired = (
            "current_mbi",
            "consolidated_program",
            "has_voluntary_alignment",
            "months_in_reach",
            "months_in_mssp",
            "office_location",
            "has_valid_voluntary_alignment",
            "has_voluntary_outreach",
            "voluntary_email_count",
            "voluntary_letter_count",
        )
        available = df.collect_schema().names()
        cols = [c for c in desired if c in available]
        if "current_mbi" not in cols and "current_mbi" in available:
            cols.insert(0, "current_mbi")
        if not cols:
            return df.head(sample_size).collect()
        return df.select(cols).head(sample_size).collect()
