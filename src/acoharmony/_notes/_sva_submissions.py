# © 2025 HarmonyCares
# All rights reserved.

"""
SVA submissions dashboard analytics.

Backs ``notebooks/sva_submissions_dashboard.py``: loads vendor snapshot
archive, PALMR panel data, BAR/participant enrichment; classifies
exclusions and duplicates; produces invalid + valid CMS-ready exports;
computes provider panel-balancing recommendations; and supports JSON
snapshot diff + duplicate-name analysis.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry

EXCLUSION_PATTERNS = (
    "deceased",
    "designated different pcp",
    "provider not in network",
    "declined",
    "decline",
)

ALLOWED_PROVIDER_NPIS = (
    "1376709493",
    "1356607881",
    "1568005569",
    "1417337247",
    "1720498967",
    "1922454586",
    "1639331804",
)

PRIMARY_PROVIDERS_BY_STATE = {
    "FL": "1922454586",
    "MI": "1568005569",
    "WI": "1417337247",
    "NY": "1639331804",
    "TX": "1720498967",
    "NJ": "1376709493",
}

DEFAULT_PROVIDER_STATE_ASSIGNMENTS = {
    "1922454586": {"FL"},
    "1568005569": {"MI"},
    "1417337247": {"WI"},
    "1639331804": {"NY"},
    "1720498967": {"TX"},
    "1356607881": set(),
    "1376709493": {"NJ"},
}


class SvaSubmissionsPlugins(PluginRegistry):
    """SVA submissions dashboard analytics."""

    # ---- raw loaders ------------------------------------------------

    def load_sources(self, silver_path: Path) -> dict[str, Any]:
        silver = Path(silver_path)
        sub_path = silver / "sva_submissions.parquet"
        sva_path = silver / "sva.parquet"
        bar_path = silver / "bar.parquet"
        participants_path = silver / "participant_list.parquet"
        palmr_path = silver / "palmr.parquet"

        if sub_path.exists():
            archive = pl.read_parquet(sub_path)
            if archive.height > 0:
                most_recent = archive.select("file_date").max().item()
                df_submissions = archive.filter(pl.col("file_date") == most_recent)
            else:
                df_submissions = archive
        else:
            archive = pl.DataFrame()
            df_submissions = pl.DataFrame()

        df_sva = pl.read_parquet(sva_path) if sva_path.exists() else pl.DataFrame()
        df_bar = pl.read_parquet(bar_path) if bar_path.exists() else pl.DataFrame()
        df_participants = (
            pl.read_parquet(participants_path)
            if participants_path.exists()
            else pl.DataFrame()
        )

        if (silver / "identity_timeline.parquet").exists():
            from acoharmony._transforms._identity_timeline import (
                current_mbi_lookup_lazy,
            )

            df_xwalk = current_mbi_lookup_lazy(silver).collect()
        else:
            df_xwalk = pl.DataFrame()

        if palmr_path.exists():
            palmr_archive = pl.read_parquet(palmr_path)
            if palmr_archive.height > 0 and "file_date" in palmr_archive.columns:
                most_recent = palmr_archive.select("file_date").max().item()
                df_palmr = palmr_archive.filter(pl.col("file_date") == most_recent)
            else:
                df_palmr = palmr_archive
        else:
            df_palmr = pl.DataFrame()

        return {
            "submissions_archive": archive,
            "submissions": df_submissions,
            "sva": df_sva,
            "xwalk": df_xwalk,
            "participants": df_participants,
            "bar": df_bar,
            "palmr": df_palmr,
        }

    # ---- transformations ---------------------------------------------

    def parse_dates(self, df: pl.DataFrame) -> pl.DataFrame:
        sig_dtype = df.schema.get("signature_date")
        if sig_dtype == pl.Utf8:
            sig_expr = (
                pl.col("signature_date")
                .str.to_datetime(strict=False)
                .alias("signature_datetime")
            )
        else:
            sig_expr = (
                pl.col("signature_date").cast(pl.Datetime).alias("signature_datetime")
            )
        return df.with_columns(
            pl.col("created_at")
            .str.to_datetime(format="%B %d, %Y, %I:%M %p")
            .alias("created_datetime"),
            sig_expr,
        ).with_columns(
            pl.col("created_datetime").dt.date().alias("created_date"),
            pl.col("signature_datetime").dt.date().alias("signature_date_parsed"),
        )

    def default_date_range(self, df_sva: pl.DataFrame) -> tuple[date, date]:
        if df_sva.height > 0 and "file_date" in df_sva.columns:
            sva_str = df_sva.select("file_date").max().item()
            sva_date = (
                date.fromisoformat(sva_str) if sva_str else date(2025, 11, 1)
            )
            start = sva_date + timedelta(days=1)
        else:
            start = date(2025, 11, 1)
        return start, date.today()

    def filter_date_range(
        self, df: pl.DataFrame, start_date: date, end_date: date
    ) -> pl.DataFrame:
        return df.filter(
            (pl.col("created_date") >= start_date)
            & (pl.col("created_date") <= end_date)
        )

    def identify_exclusions(
        self, df: pl.DataFrame
    ) -> tuple[pl.DataFrame, tuple[str, ...]]:
        return (
            df.with_columns(
                pl.col("transcriber_notes")
                .str.to_lowercase()
                .str.contains("|".join(EXCLUSION_PATTERNS))
                .fill_null(False)
                .alias("should_exclude")
            ),
            EXCLUSION_PATTERNS,
        )

    def flag_duplicate_completed(
        self, df: pl.DataFrame, df_all: pl.DataFrame
    ) -> pl.DataFrame:
        completed_mbis = (
            df_all.filter(pl.col("status") == "Completed")
            .select("mbi")
            .unique()["mbi"]
            .to_list()
        )
        return df.with_columns(
            pl.col("mbi").is_in(completed_mbis).alias("has_completed_sva")
        )

    def invalid_export(self, df: pl.DataFrame) -> pl.DataFrame:
        return (
            df.filter(
                (pl.col("status") == "Invalid")
                & ~pl.col("should_exclude")
                & ~pl.col("has_completed_sva")
            )
            .sort("created_date", descending=True)
            .unique(subset=["mbi"], keep="first")
            .select(
                [
                    "sva_id",
                    "mbi",
                    "beneficiary_first_name",
                    "beneficiary_last_name",
                    "signature_date",
                    "created_date",
                    "practice_name",
                    "provider_name",
                    "provider_npi",
                    "city",
                    "state",
                    "zip",
                    "address_primary_line",
                    "submission_source",
                    "transcriber_notes",
                    "status",
                    "network_id",
                ]
            )
            .sort("created_date", descending=True)
        )

    def enrich_valid_records(
        self,
        df: pl.DataFrame,
        df_xwalk: pl.DataFrame,
        df_participants: pl.DataFrame,
        df_bar: pl.DataFrame,
        today: date | None = None,
    ) -> pl.DataFrame:
        if df_xwalk.height > 0:
            xwalk_lookup = df_xwalk.select(["prvs_num", "crnt_num"]).unique()
            enriched = df.join(
                xwalk_lookup, left_on="mbi", right_on="prvs_num", how="left"
            ).rename({"crnt_num": "crosswalk_mbi"})
        else:
            enriched = df.with_columns(pl.lit(None).alias("crosswalk_mbi"))

        if df_participants.height > 0:
            combos = df_participants.select(
                pl.col("base_provider_tin").alias("tin"),
                pl.col("individual_npi").alias("npi"),
            ).unique()
            enriched = (
                enriched.join(
                    combos.with_columns(pl.lit(True).alias("_tin_npi_exists")),
                    left_on=["tin", "provider_npi"],
                    right_on=["tin", "npi"],
                    how="left",
                )
                .with_columns(
                    pl.col("_tin_npi_exists").fill_null(False).alias("tin_npi_match")
                )
                .drop("_tin_npi_exists")
            )
        else:
            enriched = enriched.with_columns(pl.lit(False).alias("tin_npi_match"))

        if df_bar.height > 0:
            ref_today = today or date.today()
            current_bar = (
                df_bar.filter(
                    pl.col("end_date").is_null() | (pl.col("end_date") >= ref_today)
                )
                .select(["bene_mbi"])
                .unique()
                .with_columns(pl.lit("Active").alias("bar_status"))
            )
            enriched = (
                enriched.join(
                    current_bar, left_on="mbi", right_on="bene_mbi", how="left"
                )
                .with_columns(
                    pl.col("bar_status")
                    .fill_null("Not in BAR")
                    .alias("current_bar_status")
                )
                .drop("bar_status")
            )
        else:
            enriched = enriched.with_columns(
                pl.lit("Unknown").alias("current_bar_status")
            )
        return enriched

    def valid_export(
        self,
        df: pl.DataFrame,
        df_sva: pl.DataFrame,
        df_xwalk: pl.DataFrame,
        df_participants: pl.DataFrame,
        df_bar: pl.DataFrame,
        today: date | None = None,
    ) -> pl.DataFrame:
        filtered = df.filter(
            (pl.col("status") == "Completed") & ~pl.col("should_exclude")
        )
        if df_sva.height > 0:
            already = df_sva.select(
                pl.col("bene_mbi").alias("mbi"),
                pl.col("sva_signature_date").alias("signature_date_parsed"),
            ).unique()
            new = filtered.join(
                already, on=["mbi", "signature_date_parsed"], how="anti"
            )
        else:
            new = filtered
        deduped = new.sort("created_date", descending=True).unique(
            subset=["mbi"], keep="first"
        )
        enriched = self.enrich_valid_records(
            deduped, df_xwalk, df_participants, df_bar, today
        )
        return enriched.select(
            [
                "sva_id",
                "mbi",
                "crosswalk_mbi",
                "beneficiary_first_name",
                "beneficiary_last_name",
                "signature_date",
                "signature_date_parsed",
                "created_date",
                "practice_name",
                "provider_name",
                "provider_npi",
                "tin",
                "tin_npi_match",
                "current_bar_status",
                "city",
                "state",
                "zip",
                "address_primary_line",
                "submission_source",
                "transcriber_notes",
                "status",
                "network_id",
            ]
        ).sort("created_date", descending=True)

    def apply_pipeline(
        self,
        df_submissions: pl.DataFrame,
        df_sva: pl.DataFrame,
        df_xwalk: pl.DataFrame,
        df_participants: pl.DataFrame,
        df_bar: pl.DataFrame,
        start_date: date,
        end_date: date,
        today: date | None = None,
    ) -> dict[str, Any]:
        parsed = self.parse_dates(df_submissions)
        filtered = self.filter_date_range(parsed, start_date, end_date)
        with_excl, exclusion_patterns = self.identify_exclusions(filtered)
        flagged = self.flag_duplicate_completed(with_excl, parsed)
        invalid = self.invalid_export(flagged)
        valid = self.valid_export(
            flagged, df_sva, df_xwalk, df_participants, df_bar, today
        )
        return {
            "filtered": filtered,
            "flagged": flagged,
            "invalid": invalid,
            "valid": valid,
            "exclusion_patterns": exclusion_patterns,
        }

    # ---- summary stats -----------------------------------------------

    def summary_stats(
        self,
        df_submissions: pl.DataFrame,
        df_filtered: pl.DataFrame,
        df_flagged: pl.DataFrame,
        df_invalid: pl.DataFrame,
        df_valid: pl.DataFrame,
    ) -> dict[str, Any]:
        return {
            "total_all_time": df_submissions.height,
            "total_in_range": df_filtered.height,
            "total_excluded": df_flagged.filter(pl.col("should_exclude")).height,
            "total_with_completed": df_flagged.filter(
                (pl.col("status") == "Invalid") & pl.col("has_completed_sva")
            ).height,
            "total_invalid_export": df_invalid.height,
            "total_valid_export": df_valid.height,
            "status_counts": df_filtered.group_by("status")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True),
            "source_counts": df_filtered.group_by("submission_source")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True),
        }

    def exclusion_breakdown(
        self, df_flagged: pl.DataFrame, exclusion_patterns: tuple[str, ...]
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        excluded = df_flagged.filter(pl.col("should_exclude"))
        rows = []
        for pattern in exclusion_patterns:
            count = excluded.filter(
                pl.col("transcriber_notes").str.to_lowercase().str.contains(pattern)
            ).height
            rows.append({"Reason": pattern.title(), "Count": count})
        return pl.DataFrame(rows), excluded

    def invalid_with_completed(self, df_flagged: pl.DataFrame) -> pl.DataFrame:
        return (
            df_flagged.filter(
                (pl.col("status") == "Invalid") & pl.col("has_completed_sva")
            )
            .select(
                [
                    "mbi",
                    "beneficiary_first_name",
                    "beneficiary_last_name",
                    "status",
                    "transcriber_notes",
                    "practice_name",
                    "created_date",
                ]
            )
            .sort("created_date", descending=True)
            .head(20)
        )

    # ---- PALMR panel distribution ------------------------------------

    def palmr_panel_distribution(
        self,
        df_palmr: pl.DataFrame,
        df_bar: pl.DataFrame,
        df_participants: pl.DataFrame,
    ) -> dict[str, Any]:
        if df_palmr.is_empty():
            return {"available": False}
        palmr_date = (
            df_palmr.select("file_date").max().item()
            if "file_date" in df_palmr.columns
            else "N/A"
        )
        df_palmr_with_state = df_palmr
        if df_bar.height > 0:
            bar_states = df_bar.select(["bene_mbi", "bene_state"]).unique()
            df_palmr_with_state = df_palmr.join(bar_states, on="bene_mbi", how="left")

        panel_by_npi = (
            df_palmr.group_by(["prvdr_npi", "prvdr_tin"])
            .agg(
                pl.len().alias("panel_size"),
                pl.col("bene_mbi").n_unique().alias("unique_patients"),
            )
            .sort("panel_size", descending=True)
        )
        if df_participants.height > 0:
            provider_names = df_participants.select(
                pl.col("individual_npi").alias("prvdr_npi"),
                pl.col("base_provider_tin").alias("prvdr_tin"),
                pl.concat_str(
                    [
                        pl.col("individual_first_name"),
                        pl.lit(" "),
                        pl.col("individual_last_name"),
                    ]
                ).alias("provider_name"),
            ).unique()
            panel_by_npi = panel_by_npi.join(
                provider_names, on=["prvdr_npi", "prvdr_tin"], how="left"
            ).select(
                [
                    "prvdr_npi",
                    "provider_name",
                    "prvdr_tin",
                    "panel_size",
                    "unique_patients",
                ]
            )
        else:
            panel_by_npi = panel_by_npi.with_columns(
                pl.lit(None).alias("provider_name")
            ).select(
                [
                    "prvdr_npi",
                    "provider_name",
                    "prvdr_tin",
                    "panel_size",
                    "unique_patients",
                ]
            )

        if "bene_state" in df_palmr_with_state.columns:
            state_by_provider = (
                df_palmr_with_state.group_by(["prvdr_npi", "prvdr_tin", "bene_state"])
                .agg(
                    pl.len().alias("patient_count"),
                    pl.col("bene_mbi").n_unique().alias("unique_patients"),
                )
                .sort(
                    ["prvdr_npi", "prvdr_tin", "patient_count"],
                    descending=[False, False, True],
                )
            )
        else:
            state_by_provider = pl.DataFrame()

        distribution = (
            panel_by_npi.with_columns(
                pl.when(pl.col("panel_size") < 100)
                .then(pl.lit("< 100"))
                .when(pl.col("panel_size") < 250)
                .then(pl.lit("100-249"))
                .when(pl.col("panel_size") < 500)
                .then(pl.lit("250-499"))
                .when(pl.col("panel_size") < 1000)
                .then(pl.lit("500-999"))
                .otherwise(pl.lit("1000+"))
                .alias("panel_range")
            )
            .group_by("panel_range")
            .agg(pl.len().alias("npi_count"))
            .sort("panel_range")
        )
        return {
            "available": True,
            "palmr_date": palmr_date,
            "total_npis": panel_by_npi.height,
            "total_patients": df_palmr.select("bene_mbi").n_unique(),
            "avg_panel": panel_by_npi.select("panel_size").mean().item(),
            "median_panel": panel_by_npi.select("panel_size").median().item(),
            "distribution": distribution,
            "panel_by_npi": panel_by_npi,
            "state_by_provider": state_by_provider,
        }

    # ---- provider enrichment + balancing -----------------------------

    def enrich_providers_with_states(
        self,
        df_palmr: pl.DataFrame,
        df_participants: pl.DataFrame,
    ) -> pl.DataFrame:
        if df_palmr.is_empty() or df_participants.is_empty():
            return pl.DataFrame()
        state_info = (
            df_participants.filter(
                pl.col("individual_npi").is_not_null()
                & pl.col("individual_npi").is_in(list(ALLOWED_PROVIDER_NPIS))
                & pl.col("state_cd").is_not_null()
                & pl.col("provider_type").str.contains("Individual Practitioner")
            )
            .select(
                pl.col("individual_npi").alias("prvdr_npi"),
                pl.col("base_provider_tin").alias("prvdr_tin"),
                pl.concat_str(
                    [
                        pl.col("individual_first_name"),
                        pl.lit(" "),
                        pl.col("individual_last_name"),
                    ]
                ).alias("provider_name"),
                pl.col("state_cd").alias("provider_state"),
            )
            .unique(subset=["prvdr_npi", "prvdr_tin", "provider_state"])
        )
        effective_dates = (
            df_participants.filter(
                pl.col("individual_npi").is_not_null()
                & pl.col("individual_npi").is_in(list(ALLOWED_PROVIDER_NPIS))
                & pl.col("effective_start_date").is_not_null()
                & (pl.col("provider_type") == "Individual Provider")
            )
            .with_columns(
                pl.col("effective_start_date").cast(pl.Date).alias("parsed_start_date")
            )
            .group_by(["individual_npi", "base_provider_tin"])
            .agg(
                pl.col("parsed_start_date").min().alias("earliest_effective_date")
            )
            .select(
                pl.col("individual_npi").alias("prvdr_npi"),
                pl.col("base_provider_tin").alias("prvdr_tin"),
                pl.col("earliest_effective_date"),
            )
        )
        current_panels = (
            df_palmr.group_by(["prvdr_npi", "prvdr_tin"])
            .agg(pl.len().alias("current_panel"))
        )
        return (
            state_info.join(effective_dates, on=["prvdr_npi", "prvdr_tin"], how="left")
            .join(current_panels, on=["prvdr_npi", "prvdr_tin"], how="left")
            .with_columns(pl.col("current_panel").fill_null(0))
        )

    def panel_balancing_recommendations(
        self,
        df_valid: pl.DataFrame,
        providers_enriched: pl.DataFrame,
        selected_providers: set[tuple[str, str, str]],
        df_bar: pl.DataFrame,
    ) -> dict[str, dict[str, str]]:
        if (
            df_valid.is_empty()
            or providers_enriched.is_empty()
            or df_bar.is_empty()
            or not selected_providers
        ):
            return {}
        panels = providers_enriched.filter(
            pl.struct(["prvdr_npi", "prvdr_tin", "provider_state"]).map_elements(
                lambda x: (
                    x["prvdr_npi"],
                    x["prvdr_tin"],
                    x["provider_state"],
                )
                in selected_providers,
                return_dtype=pl.Boolean,
            )
        ).rename({"current_panel": "current_panel_size"})
        if panels.is_empty():
            return {}
        bar_states = df_bar.select(["bene_mbi", "bene_state"]).unique()
        valid_with_state = df_valid.join(
            bar_states, left_on="mbi", right_on="bene_mbi", how="left"
        ).with_columns(
            pl.coalesce(["bene_state", "state"]).alias("final_state")
        ).select(["mbi", "final_state", "signature_date_parsed"])

        recommendations: dict[str, dict[str, str]] = {}
        for row in valid_with_state.iter_rows(named=True):
            state = row.get("final_state")
            mbi = row["mbi"]
            sig = row.get("signature_date_parsed")
            if not state:
                continue
            state_match = panels.filter(pl.col("provider_state") == state)
            if sig and state_match.height > 0:
                state_match = state_match.filter(
                    pl.col("earliest_effective_date").is_null()
                    | (pl.col("earliest_effective_date") <= sig)
                )
            if state_match.is_empty():
                continue
            primary = PRIMARY_PROVIDERS_BY_STATE.get(state)
            best = None
            if primary:
                primary_match = state_match.filter(
                    pl.col("prvdr_npi") == primary
                )
                if primary_match.height > 0:
                    best = primary_match.limit(1)
            if best is None or best.is_empty():
                best = state_match.sort("current_panel_size").limit(1)
            if best.height > 0:
                d = best.to_dicts()[0]
                recommendations[mbi] = {"npi": d["prvdr_npi"], "tin": d["prvdr_tin"]}
                panels = panels.with_columns(
                    pl.when(
                        (pl.col("prvdr_npi") == d["prvdr_npi"])
                        & (pl.col("prvdr_tin") == d["prvdr_tin"])
                        & (pl.col("provider_state") == d["provider_state"])
                    )
                    .then(pl.col("current_panel_size") + 1)
                    .otherwise(pl.col("current_panel_size"))
                    .alias("current_panel_size")
                )
        return recommendations

    # ---- CMS export schema --------------------------------------------

    def cms_sva_export(
        self,
        df_valid: pl.DataFrame,
        recommendations: dict[str, dict[str, str]],
        providers_enriched: pl.DataFrame,
        df_bar: pl.DataFrame,
        today: date | None = None,
    ) -> pl.DataFrame:
        if df_valid.is_empty():
            return pl.DataFrame()
        ref_today = today or date.today()
        bar_data = (
            df_bar.select(
                ["bene_mbi", "bene_state", "bene_city", "bene_address_line_1", "bene_zip_5"]
            ).unique()
            if df_bar.height > 0
            else pl.DataFrame()
        )
        cms = df_valid.with_columns(
            pl.lit("D0259").alias("aco_id"),
            pl.col("mbi").alias("bene_mbi"),
        )
        if recommendations:
            cms = cms.with_columns(
                pl.col("mbi")
                .map_elements(
                    lambda m: recommendations.get(m, {}).get("npi"),
                    return_dtype=pl.String,
                )
                .fill_null(pl.col("provider_npi"))
                .alias("recommended_npi"),
                pl.col("mbi")
                .map_elements(
                    lambda m: recommendations.get(m, {}).get("tin"),
                    return_dtype=pl.String,
                )
                .fill_null(pl.col("tin"))
                .alias("recommended_tin"),
            )
            if not providers_enriched.is_empty():
                names = providers_enriched.select(
                    pl.col("prvdr_npi"),
                    pl.col("prvdr_tin"),
                    pl.col("provider_name").alias("recommended_provider_name"),
                ).unique(subset=["prvdr_npi", "prvdr_tin"])
                cms = cms.join(
                    names,
                    left_on=["recommended_npi", "recommended_tin"],
                    right_on=["prvdr_npi", "prvdr_tin"],
                    how="left",
                ).with_columns(
                    pl.coalesce(
                        ["recommended_provider_name", "provider_name"]
                    ).alias("final_provider_name")
                )
            else:
                cms = cms.with_columns(pl.col("provider_name").alias("final_provider_name"))
        else:
            cms = cms.with_columns(
                pl.col("provider_npi").alias("recommended_npi"),
                pl.col("tin").alias("recommended_tin"),
                pl.col("provider_name").alias("final_provider_name"),
            )
        if bar_data.height > 0:
            cms = cms.join(bar_data, on="bene_mbi", how="left")
        else:
            cms = cms.with_columns(
                pl.lit(None).cast(pl.String).alias("bene_state"),
                pl.lit(None).cast(pl.String).alias("bene_city"),
                pl.lit(None).cast(pl.String).alias("bene_address_line_1"),
                pl.lit(None).cast(pl.String).alias("bene_zip_5"),
            )
        return cms.select(
            "aco_id",
            "bene_mbi",
            pl.col("beneficiary_first_name").alias("bene_first_name"),
            pl.col("beneficiary_last_name").alias("bene_last_name"),
            pl.col("bene_address_line_1")
            .fill_null(pl.col("address_primary_line"))
            .alias("bene_street_address"),
            pl.col("bene_city").fill_null(pl.col("city")).alias("city"),
            pl.col("bene_state")
            .fill_null(pl.col("state"))
            .cast(pl.Categorical)
            .alias("bene_state_final"),
            pl.col("bene_zip_5").fill_null(pl.col("zip")).alias("zip"),
            pl.col("final_provider_name").alias("provider_name"),
            pl.col("final_provider_name").alias("sva_provider_name"),
            pl.col("recommended_npi").alias("sva_npi"),
            pl.col("recommended_tin").alias("sva_tin"),
            pl.col("signature_date_parsed").alias("sva_signature_date"),
            pl.lit(None).cast(pl.Categorical).alias("sva_response_code"),
            pl.lit(None).cast(pl.String).alias("processed_at"),
            pl.lit("sva_submissions").alias("source_file"),
            pl.lit(f"sva_export_{ref_today.strftime('%Y%m%d')}.csv").alias("source_filename"),
            pl.lit(ref_today.isoformat()).alias("file_date"),
            pl.lit("gold").alias("medallion_layer"),
        )

    # ---- snapshot diff + duplicate names ------------------------------

    def snapshot_diff(self, archive: pl.DataFrame) -> dict[str, Any] | None:
        if archive.is_empty() or "file_date" not in archive.columns:
            return None
        dates = archive.select("file_date").unique().sort("file_date", descending=True)
        if dates.height < 2:
            return {"available": False, "snapshot_count": dates.height}
        most_recent = dates.item(0, 0)
        second = dates.item(1, 0)
        recent = archive.filter(pl.col("file_date") == most_recent)
        prior = archive.filter(pl.col("file_date") == second)
        new = recent.join(prior.select("mbi").unique(), on="mbi", how="anti")
        removed = prior.join(recent.select("mbi").unique(), on="mbi", how="anti")
        common = recent.join(
            prior.select("mbi").unique(), on="mbi", how="inner"
        ).select("mbi").unique()
        recent_common = recent.join(common, on="mbi", how="inner")
        prior_common = prior.join(common, on="mbi", how="inner")
        status_cmp = recent_common.select(
            "mbi",
            pl.col("beneficiary_first_name").alias("first_name"),
            pl.col("beneficiary_last_name").alias("last_name"),
            pl.col("status").alias("current_status"),
            pl.col("created_at").alias("current_created_at"),
        ).join(
            prior_common.select(
                "mbi",
                pl.col("status").alias("previous_status"),
                pl.col("created_at").alias("previous_created_at"),
            ),
            on="mbi",
            how="inner",
        )
        status_changes = status_cmp.filter(
            pl.col("current_status") != pl.col("previous_status")
        )
        return {
            "available": True,
            "most_recent": most_recent,
            "second_recent": second,
            "most_recent_count": recent.height,
            "second_recent_count": prior.height,
            "new": new,
            "removed": removed,
            "status_changes": status_changes,
        }

    def duplicate_names(self, df_submissions: pl.DataFrame) -> dict[str, Any]:
        if df_submissions.is_empty():
            return {"available": False}
        groups = (
            df_submissions.group_by(
                ["beneficiary_first_name", "beneficiary_last_name"]
            )
            .agg(
                pl.col("mbi").alias("mbis"),
                pl.col("mbi").n_unique().alias("unique_mbis"),
                pl.len().alias("record_count"),
                pl.col("status").alias("statuses"),
                pl.col("created_at").alias("created_dates"),
            )
            .filter(pl.col("unique_mbis") > 1)
            .sort("unique_mbis", descending=True)
        )
        if groups.is_empty():
            return {"available": True, "groups": groups, "records": pl.DataFrame()}
        pairs = groups.select(["beneficiary_first_name", "beneficiary_last_name"])
        records = (
            df_submissions.join(
                pairs,
                on=["beneficiary_first_name", "beneficiary_last_name"],
                how="inner",
            )
            .select(
                [
                    "beneficiary_first_name",
                    "beneficiary_last_name",
                    "mbi",
                    "status",
                    "created_at",
                    "provider_name",
                    "provider_npi",
                    "tin",
                ]
            )
            .sort(
                ["beneficiary_last_name", "beneficiary_first_name", "mbi"]
            )
        )
        return {"available": True, "groups": groups, "records": records}
