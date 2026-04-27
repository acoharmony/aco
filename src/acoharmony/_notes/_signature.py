# © 2025 HarmonyCares
# All rights reserved.

"""
Signature-requirement (chase-list) analytics.

Backs ``notebooks/signature_requirement_analysis.py``: builds the MBI
crosswalk, loads SVA / PBVAR / BAR / provider list, computes signature
history, classifies recency status, builds the chase list with reasons,
and runs SVA data-quality validation (TIN/NPI, deceased, terminated).
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry

JANNETTE_NPI = "1285636043"


class SignaturePlugins(PluginRegistry):
    """Signature-requirement analytics."""

    # ---- crosswalk ---------------------------------------------------

    def load_crosswalk(self, silver_path: Path) -> dict[str, Any]:
        """Build bidirectional MBI map plus HCMPI lookup and history counts."""
        from acoharmony._transforms._identity_timeline import (
            current_mbi_with_hcmpi_lookup_lazy,
        )

        crosswalk = current_mbi_with_hcmpi_lookup_lazy(Path(silver_path)).collect()
        mbi_map: dict[str, str] = {}
        mbi_to_hcmpi: dict[str, str] = {}
        mbi_history_count: dict[str, int] = {}
        relationships: dict[str, set[str]] = {}

        for row in crosswalk.iter_rows(named=True):
            crnt = row.get("crnt_num")
            prvs = row.get("prvs_num")
            hcmpi = row.get("hcmpi")
            if crnt and prvs:
                relationships.setdefault(crnt, set()).add(prvs)
                if hcmpi:
                    mbi_to_hcmpi[crnt] = hcmpi
                    mbi_to_hcmpi[prvs] = hcmpi

        for crnt, prvs_set in relationships.items():
            mbi_map[crnt] = crnt
            for prvs in prvs_set:
                mbi_map[prvs] = crnt
            mbi_history_count[crnt] = len(prvs_set)

        for row in crosswalk.iter_rows(named=True):
            crnt = row.get("crnt_num")
            if crnt and crnt not in mbi_map:
                mbi_map[crnt] = crnt
                mbi_history_count[crnt] = 0

        return {
            "mbi_map": mbi_map,
            "mbi_to_hcmpi": mbi_to_hcmpi,
            "mbi_history_count": mbi_history_count,
            "total_mbis": len(mbi_map),
            "unique_current_mbis": len(relationships),
        }

    # ---- raw loaders -------------------------------------------------

    def load_bar(self, silver_path: Path) -> pl.DataFrame:
        return pl.read_parquet(Path(silver_path) / "bar.parquet")

    def load_provider_list(self, silver_path: Path) -> dict[str, Any]:
        """Returns ``{providers, valid_combos, npi_to_name, totals}``.

        The silver participant_list is row-per-NPI with ``base_provider_tin``
        as the TIN column and ``individual_npi`` (singular) as the NPI.
        """
        df = pl.read_parquet(Path(silver_path) / "participant_list.parquet")
        providers = df.filter(pl.col("provider_class") == "Participant Provider")
        valid_combos: set[tuple[str, str]] = set()
        npi_to_name: dict[str, str] = {}
        all_npis: set[str] = set()
        for row in providers.iter_rows(named=True):
            tin = row.get("base_provider_tin")
            npi = row.get("individual_npi")
            first = row.get("individual_first_name") or ""
            last = row.get("individual_last_name") or ""
            full = f"{first} {last}".strip() or None
            if tin and npi:
                tin_s = str(tin).strip()
                npi_s = str(npi).strip()
                if not tin_s or not npi_s:
                    continue
                valid_combos.add((tin_s, npi_s))
                all_npis.add(npi_s)
                if full:
                    npi_to_name[npi_s] = full
        return {
            "providers": providers,
            "valid_combos": valid_combos,
            "npi_to_name": npi_to_name,
            "total_providers": providers.height,
            "unique_tins": providers.select("base_provider_tin").n_unique(),
            "unique_npis": len(all_npis),
        }

    def load_sva(
        self, silver_path: Path, mbi_map: dict[str, str]
    ) -> dict[str, Any]:
        """Loads SVA, normalises MBIs, splits into pending vs approved."""
        sva = pl.read_parquet(Path(silver_path) / "sva.parquet")
        normalised = [mbi_map.get(m, m) for m in sva["bene_mbi"].to_list()]
        sva = sva.with_columns(pl.Series("normalized_mbi", normalised))

        pbvar_path = Path(silver_path) / "pbvar.parquet"
        most_recent_pbvar: Any = None
        if pbvar_path.exists():
            pbvar = pl.read_parquet(pbvar_path)
            if pbvar.height > 0:
                most_recent_pbvar = pbvar.select(pl.col("file_date").max()).item()

        if most_recent_pbvar:
            pending = sva.filter(pl.col("file_date") > most_recent_pbvar)
            approved = sva.filter(pl.col("file_date") <= most_recent_pbvar)
        else:
            pending = pl.DataFrame()
            approved = sva

        signed = sva.filter(pl.col("sva_signature_date").is_not_null())
        return {
            "sva": sva,
            "signed": signed,
            "pending": pending,
            "approved": approved,
            "total_unique": sva.select("normalized_mbi").n_unique(),
            "unique_signed": signed.select("normalized_mbi").n_unique(),
            "pending_unique": (
                pending.select("normalized_mbi").n_unique() if pending.height else 0
            ),
            "most_recent_pbvar": most_recent_pbvar,
        }

    def load_pbvar_for_history(
        self, silver_path: Path, mbi_map: dict[str, str]
    ) -> pl.DataFrame:
        """Reshape PBVAR to match the SVA schema for downstream concat."""
        pbvar_path = Path(silver_path) / "pbvar.parquet"
        if not pbvar_path.exists():
            return pl.DataFrame()
        pbvar = pl.read_parquet(pbvar_path)
        if pbvar.height == 0:
            return pl.DataFrame()
        normalised = [mbi_map.get(m, m) for m in pbvar["bene_mbi"].to_list()]
        pbvar = pbvar.with_columns(pl.Series("normalized_mbi", normalised))
        return pbvar.select(
            pl.col("aco_id"),
            pl.col("bene_mbi"),
            pl.col("normalized_mbi"),
            pl.col("bene_first_name"),
            pl.col("bene_last_name"),
            pl.col("bene_line_1_address").alias("bene_street_address"),
            pl.col("bene_city").alias("city"),
            pl.col("bene_state").alias("state"),
            pl.col("bene_zipcode").alias("zip"),
            pl.col("provider_name"),
            pl.col("practitioner_name").alias("sva_provider_name"),
            pl.col("sva_npi"),
            pl.col("sva_tin"),
            pl.col("sva_signature_date"),
            pl.lit(None).cast(pl.String).alias("sva_response_code"),
            pl.col("processed_at"),
            pl.col("source_file"),
            pl.col("source_filename"),
            pl.col("file_date"),
            pl.col("medallion_layer"),
        )

    # ---- signature history -------------------------------------------

    def combine_signatures(
        self, pbvar_df: pl.DataFrame, signed_sva: pl.DataFrame
    ) -> pl.DataFrame:
        """Concat PBVAR + SVA via diagonal so missing columns null-fill."""
        if pbvar_df.height == 0:
            return signed_sva
        pbvar_clean = pbvar_df
        for col in pbvar_clean.columns:
            if pbvar_clean[col].dtype == pl.Categorical:
                pbvar_clean = pbvar_clean.with_columns(pl.col(col).cast(pl.String))
        sva_clean = signed_sva
        for col in sva_clean.columns:
            if sva_clean[col].dtype == pl.Categorical:
                sva_clean = sva_clean.with_columns(pl.col(col).cast(pl.String))
        return pl.concat([pbvar_clean, sva_clean], how="diagonal")

    def signature_history(
        self, all_signatures: pl.DataFrame
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Per-MBI signature history + frequency distribution table."""
        sorted_sigs = all_signatures.sort("sva_signature_date").with_columns(
            pl.when(pl.col("sva_provider_name").is_not_null())
            .then(pl.col("sva_provider_name").str.strip_chars().str.to_uppercase())
            .otherwise(None)
            .alias("normalized_provider_name")
        )
        history = sorted_sigs.group_by("normalized_mbi").agg(
            pl.col("sva_signature_date").count().alias("total_signature_count"),
            pl.col("sva_signature_date").min().alias("earliest_signature_date"),
            pl.col("sva_signature_date").max().alias("most_recent_signature_date"),
            pl.col("sva_npi").last().alias("most_recent_provider_npi"),
            pl.col("sva_tin").last().alias("most_recent_provider_tin"),
            pl.col("normalized_provider_name").last().alias("most_recent_provider_name"),
            pl.col("source_filename").last().alias("most_recent_source_file"),
            pl.col("sva_signature_date").sort().alias("all_signature_dates"),
            pl.col("sva_npi").unique().sort().alias("all_provider_npis"),
            pl.col("sva_tin").unique().sort().alias("all_provider_tins"),
        )
        freq = (
            history.group_by("total_signature_count")
            .agg(pl.len().alias("beneficiary_count"))
            .sort("total_signature_count")
        )
        return history, freq

    def count_signature_sources(
        self, all_signatures: pl.DataFrame, silver_path: Path
    ) -> dict[str, int]:
        if all_signatures.is_empty():
            return {
                "total_records": 0,
                "pbvar_records": 0,
                "sva_records": 0,
                "unique_beneficiaries": 0,
            }
        pbvar_path = Path(silver_path) / "pbvar.parquet"
        if pbvar_path.exists():
            pbvar = all_signatures.filter(
                pl.col("source_filename").str.contains("PBVAR")
            )
            sva = all_signatures.filter(
                ~pl.col("source_filename").str.contains("PBVAR")
            )
            return {
                "total_records": all_signatures.height,
                "pbvar_records": pbvar.height,
                "sva_records": sva.height,
                "unique_beneficiaries": all_signatures.select(
                    "normalized_mbi"
                ).n_unique(),
            }
        return {
            "total_records": all_signatures.height,
            "pbvar_records": 0,
            "sva_records": all_signatures.height,
            "unique_beneficiaries": all_signatures.select(
                "normalized_mbi"
            ).n_unique(),
        }

    # ---- BAR cohort -------------------------------------------------

    def current_cohort(
        self,
        bar_df: pl.DataFrame,
        mbi_map: dict[str, str],
        mbi_to_hcmpi: dict[str, str],
        mbi_history_count: dict[str, int],
        today: date | None = None,
    ) -> dict[str, Any]:
        algc25 = (
            bar_df.filter(pl.col("source_filename").str.contains("ALGC25"))
            .select("source_filename")
            .unique()
            .sort("source_filename")
        )
        if algc25.height > 0:
            most_recent_file = algc25["source_filename"].to_list()[-1]
        else:
            most_recent_file = bar_df.select(pl.col("source_filename").max()).item()

        cohort = bar_df.filter(pl.col("source_filename") == most_recent_file)
        normalised: list[str] = []
        hcmpis: list[str] = []
        history_counts: list[int] = []
        for mbi in cohort["bene_mbi"].to_list():
            n = mbi_map.get(mbi, mbi)
            normalised.append(n)
            hcmpis.append(str(mbi_to_hcmpi.get(mbi)) if mbi_to_hcmpi.get(mbi) else "")
            history_counts.append(mbi_history_count.get(n, 0))
        cohort = cohort.with_columns(
            pl.Series("normalized_mbi", normalised),
            pl.Series("hcmpi", hcmpis),
            pl.Series("previous_mbi_count", history_counts),
        )
        living_not_deceased = cohort.filter(pl.col("bene_date_of_death").is_null())
        ref_today = today or datetime.today().date()
        active = living_not_deceased.filter(
            pl.col("end_date").is_null() | (pl.col("end_date") >= ref_today)
        )
        deceased = cohort.filter(pl.col("bene_date_of_death").is_not_null())
        terminated = living_not_deceased.filter(
            pl.col("end_date").is_not_null() & (pl.col("end_date") < ref_today)
        )
        return {
            "most_recent_file": most_recent_file,
            "cohort": cohort,
            "active": active,
            "deceased": deceased,
            "terminated": terminated,
        }

    # ---- recency thresholds ------------------------------------------

    def date_thresholds(self, today: date | None = None) -> dict[str, date | int]:
        ref_today = today or date.today()
        cy = ref_today.year
        return {
            "current_year": cy,
            "current_year_start": date(cy, 1, 1),
            "last_year_h2_start": date(cy - 1, 7, 1),
            "last_year_start": date(cy - 1, 1, 1),
            "two_years_ago_start": date(cy - 2, 1, 1),
            "signature_cutoff": date(cy - 1, 1, 1),
        }

    # ---- validation + chase list -------------------------------------

    def signature_status_categories(
        self,
        active_cohort: pl.DataFrame,
        history: pl.DataFrame,
        valid_combos: set[tuple[str, str]],
        npi_to_name: dict[str, str],
        thresholds: dict[str, date | int],
        today: date | None = None,
    ) -> pl.DataFrame:
        """Join cohort with history; classify recency; flag invalid providers."""
        joined = active_cohort.join(history, on="normalized_mbi", how="left")
        ref_today = today or date.today()

        valid_results: list[bool] = []
        provider_names: list[str] = []
        for row in joined.iter_rows(named=True):
            tin = row.get("most_recent_provider_tin")
            npi = row.get("most_recent_provider_npi")
            valid_results.append(
                bool(tin and npi and (str(tin), str(npi)) in valid_combos)
            )
            provider_names.append(
                npi_to_name.get(str(npi), "") if npi else ""
            )
        joined = joined.with_columns(
            pl.Series("has_valid_provider", valid_results),
            pl.Series("provider_name_from_list", provider_names),
        )

        cy = thresholds["current_year"]
        return joined.with_columns(
            pl.when(pl.col("total_signature_count").is_null())
            .then(pl.lit("Never Signed"))
            .when(~pl.col("has_valid_provider"))
            .then(pl.lit("Invalid Provider"))
            .when(
                pl.col("most_recent_signature_date").cast(pl.Date, strict=False)
                >= thresholds["current_year_start"]
            )
            .then(pl.lit(f"Current Year ({cy})"))
            .when(
                pl.col("most_recent_signature_date").cast(pl.Date, strict=False)
                >= thresholds["last_year_h2_start"]
            )
            .then(pl.lit(f"Recent ({cy - 1} H2)"))
            .when(
                pl.col("most_recent_signature_date").cast(pl.Date, strict=False)
                >= thresholds["last_year_start"]
            )
            .then(pl.lit(f"Aging ({cy - 1} H1)"))
            .when(
                pl.col("most_recent_signature_date").cast(pl.Date, strict=False)
                >= thresholds["two_years_ago_start"]
            )
            .then(pl.lit(f"Old ({cy - 2})"))
            .otherwise(pl.lit(f"Very Old (Pre-{cy - 2})"))
            .alias("signature_recency_status"),
            pl.when(pl.col("most_recent_signature_date").is_not_null())
            .then(
                (
                    pl.lit(ref_today)
                    - pl.col("most_recent_signature_date").cast(pl.Date, strict=False)
                ).dt.total_days()
            )
            .otherwise(pl.lit(None))
            .alias("days_since_last_signature"),
        )

    def chase_list(
        self,
        status_categories: pl.DataFrame,
        pending_sva: pl.DataFrame,
        thresholds: dict[str, date | int],
    ) -> dict[str, Any]:
        cy = thresholds["current_year"]
        cutoff = thresholds["signature_cutoff"]
        pending_mbis = (
            set(pending_sva.select("normalized_mbi").unique().to_series().to_list())
            if pending_sva.height
            else set()
        )
        chase = status_categories.filter(
            pl.col("bene_date_of_death").is_null()
            & ~pl.col("normalized_mbi").is_in(list(pending_mbis))
            & (
                pl.col("total_signature_count").is_null()
                | (
                    pl.col("most_recent_signature_date").cast(pl.Date, strict=False)
                    < cutoff
                )
                | (pl.col("signature_recency_status") == "Invalid Provider")
                | (pl.col("most_recent_provider_npi") == JANNETTE_NPI)
            )
        )
        with_reason = chase.with_columns(
            pl.when(pl.col("total_signature_count").is_null())
            .then(pl.lit("Never Signed"))
            .when(pl.col("most_recent_provider_npi") == JANNETTE_NPI)
            .then(pl.lit("Jannette Alignment"))
            .when(pl.col("signature_recency_status") == "Invalid Provider")
            .then(pl.lit("Invalid TIN/NPI Combo"))
            .when(
                pl.col("most_recent_signature_date").cast(pl.Date, strict=False) < cutoff
            )
            .then(pl.lit(f"Signature Before {cy - 1}"))
            .otherwise(pl.lit("Other"))
            .alias("chase_reason")
        )
        action_list = with_reason.select(
            [
                "bene_mbi",
                "normalized_mbi",
                "hcmpi",
                "previous_mbi_count",
                "bene_first_name",
                "bene_last_name",
                "start_date",
                "end_date",
                "signature_recency_status",
                "total_signature_count",
                "most_recent_signature_date",
                "most_recent_source_file",
                "days_since_last_signature",
                "most_recent_provider_npi",
                "most_recent_provider_tin",
                "most_recent_provider_name",
                "provider_name_from_list",
                "all_signature_dates",
                "chase_reason",
            ]
        ).sort(["chase_reason", "bene_last_name", "bene_first_name"])
        reason_counts = (
            with_reason.group_by("chase_reason")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
        )
        jannette_count = chase.filter(
            pl.col("most_recent_provider_npi") == JANNETTE_NPI
        ).height
        requirement_summary = (
            status_categories.group_by("signature_recency_status")
            .agg(
                pl.len().alias("beneficiary_count"),
                pl.col("bene_mbi").n_unique().alias("unique_beneficiaries"),
            )
            .sort("beneficiary_count", descending=True)
        )
        stats = self._chase_stats(
            chase, status_categories, pending_mbis
        )
        return {
            "chase_list": chase,
            "with_reason": with_reason,
            "action_list": action_list,
            "reason_counts": reason_counts,
            "jannette_count": jannette_count,
            "requirement_summary": requirement_summary,
            "pending_mbis": pending_mbis,
            "stats": stats,
        }

    def _chase_stats(
        self,
        chase: pl.DataFrame,
        status_categories: pl.DataFrame,
        pending_mbis: set[str],
    ) -> dict[str, int]:
        if chase.is_empty():
            return {
                "total_chase": 0,
                "never_signed": 0,
                "old_signature": 0,
                "invalid_combo": 0,
                "total_active": status_categories.height,
                "pending_count": len(pending_mbis),
            }
        never = chase.filter(pl.col("total_signature_count").is_null()).height
        invalid = chase.filter(
            pl.col("signature_recency_status") == "Invalid Provider"
        ).height
        old = chase.height - never - invalid
        return {
            "total_chase": chase.height,
            "never_signed": never,
            "old_signature": old,
            "invalid_combo": invalid,
            "total_active": status_categories.height,
            "pending_count": len(pending_mbis),
        }

    # ---- provider analysis -------------------------------------------

    def invalid_provider_summary(
        self, status_categories: pl.DataFrame
    ) -> pl.DataFrame:
        invalid = status_categories.filter(
            pl.col("signature_recency_status") == "Invalid Provider"
        )
        if invalid.is_empty():
            return invalid
        return (
            invalid.group_by(
                [
                    "most_recent_provider_tin",
                    "most_recent_provider_npi",
                    "most_recent_provider_name",
                ]
            )
            .agg(
                pl.len().alias("affected_beneficiaries"),
                pl.col("most_recent_signature_date").max().alias("latest_signature"),
            )
            .sort("affected_beneficiaries", descending=True)
        )

    def provider_signature_stats(
        self, status_categories: pl.DataFrame
    ) -> pl.DataFrame:
        return (
            status_categories.filter(
                pl.col("most_recent_provider_npi").is_not_null()
            )
            .group_by("most_recent_provider_npi")
            .agg(
                pl.col("most_recent_provider_name")
                .mode()
                .first()
                .alias("provider_name"),
                pl.len().alias("active_beneficiary_count"),
                pl.col("most_recent_signature_date").max().alias("latest_signature_date"),
                pl.col("signature_recency_status").mode().first().alias("most_common_status"),
            )
            .sort("active_beneficiary_count", descending=True)
        )

    def chase_provider_summary(self, with_reason: pl.DataFrame) -> pl.DataFrame:
        has_sigs = with_reason.filter(
            pl.col("most_recent_provider_npi").is_not_null()
        )
        if has_sigs.is_empty():
            return has_sigs
        return (
            has_sigs.group_by(
                ["most_recent_provider_npi", "most_recent_provider_name"]
            )
            .agg(
                pl.len().alias("beneficiaries_needing_signature"),
                pl.col("chase_reason").mode().first().alias("primary_reason"),
            )
            .sort("beneficiaries_needing_signature", descending=True)
        )

    # ---- export -----------------------------------------------------

    def export_chase_list(
        self,
        with_reason: pl.DataFrame,
        output_dir: Path,
    ) -> Path:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        path = output_dir / "signature_chase_list.csv"
        export_df = with_reason.select(
            [
                "bene_mbi",
                "normalized_mbi",
                "hcmpi",
                "previous_mbi_count",
                "bene_first_name",
                "bene_last_name",
                "bene_gender",
                "bene_state",
                "most_recent_signature_date",
                "days_since_last_signature",
                "signature_recency_status",
                "most_recent_provider_npi",
                "most_recent_provider_tin",
                "most_recent_provider_name",
                "chase_reason",
            ]
        )
        export_df.write_csv(path)
        return path

    def export_analysis_results(
        self,
        status_categories: pl.DataFrame,
        action_list: pl.DataFrame,
        provider_stats: pl.DataFrame,
        output_dir: Path,
    ) -> dict[str, Path]:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        analysis = output_dir / f"comprehensive_signature_analysis_{ts}.parquet"
        action = output_dir / f"signatures_needed_{ts}.parquet"
        providers = output_dir / f"provider_signature_stats_{ts}.parquet"
        status_categories.write_parquet(analysis)
        action_list.write_parquet(action)
        provider_stats.write_parquet(providers)
        return {"analysis": analysis, "action": action, "providers": providers}

    # ---- SVA data quality validation ---------------------------------

    def most_recent_sva_date(self, silver_path: Path) -> Any:
        path = Path(silver_path) / "sva.parquet"
        if not path.exists():
            return None
        df = pl.read_parquet(path)
        if df.is_empty():
            return None
        return df.select(pl.col("file_date").max()).item()

    def most_recent_pbvar_date(self, silver_path: Path) -> Any:
        path = Path(silver_path) / "pbvar.parquet"
        if not path.exists():
            return None
        df = pl.read_parquet(path)
        if df.is_empty():
            return None
        return df.select(pl.col("file_date").max()).item()

    def validate_sva_tin_npi(
        self, silver_path: Path, valid_combos: set[tuple[str, str]]
    ) -> tuple[pl.DataFrame | None, pl.DataFrame]:
        path = Path(silver_path) / "sva.parquet"
        if not path.exists():
            return None, pl.DataFrame()
        df = pl.read_parquet(path)
        recent_date = df.select(pl.col("file_date").max()).item()
        recent = df.filter(pl.col("file_date") == recent_date)
        invalid: list[dict[str, Any]] = []
        for row in recent.iter_rows(named=True):
            tin = row.get("sva_tin")
            npi = row.get("sva_npi")
            if tin and npi and (str(tin), str(npi)) not in valid_combos:
                invalid.append(
                    {
                        "bene_mbi": row.get("bene_mbi"),
                        "sva_tin": str(tin),
                        "sva_npi": str(npi),
                        "sva_provider_name": row.get("sva_provider_name"),
                        "sva_signature_date": row.get("sva_signature_date"),
                    }
                )
        return recent, pl.DataFrame(invalid) if invalid else pl.DataFrame()

    def validate_sva_status(
        self,
        sva_recent: pl.DataFrame | None,
        bar_df: pl.DataFrame,
        mbi_map: dict[str, str],
        today: date | None = None,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        if sva_recent is None or sva_recent.is_empty() or bar_df.is_empty():
            return pl.DataFrame(), pl.DataFrame()
        most_recent_file = bar_df.select(pl.col("source_filename").max()).item()
        current_bar = bar_df.filter(pl.col("source_filename") == most_recent_file)
        normalised = [mbi_map.get(m, m) for m in sva_recent["bene_mbi"].to_list()]
        sva = sva_recent.with_columns(pl.Series("normalized_mbi", normalised))
        joined = sva.join(
            current_bar.select(
                [
                    "bene_mbi",
                    "bene_date_of_death",
                    "end_date",
                    "bene_first_name",
                    "bene_last_name",
                ]
            ),
            left_on="normalized_mbi",
            right_on="bene_mbi",
            how="left",
            suffix="_bar",
        )
        deceased = joined.filter(pl.col("bene_date_of_death").is_not_null()).select(
            [
                "bene_mbi",
                "normalized_mbi",
                "bene_first_name",
                "bene_last_name",
                "bene_date_of_death",
                "sva_signature_date",
                "sva_provider_name",
            ]
        )
        ref_today = today or datetime.today().date()
        terminated = joined.filter(
            pl.col("end_date").is_not_null() & (pl.col("end_date") < ref_today)
        ).select(
            [
                "bene_mbi",
                "normalized_mbi",
                "bene_first_name",
                "bene_last_name",
                "end_date",
                "sva_signature_date",
                "sva_provider_name",
            ]
        )
        return deceased, terminated
