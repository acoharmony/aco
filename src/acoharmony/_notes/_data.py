# © 2025 HarmonyCares
# All rights reserved.

"""
Data-loading helpers for notebooks.

Wrap medallion-tier parquet scans, identity resolution, and the
patient/member analytic projections used by the patient and claims
notebooks. Notebooks call these directly — no inline polars logic.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

import polars as pl

from ._base import PluginRegistry

MedallionTier = Literal["bronze", "silver", "gold"]


_MEDICAL_CLAIM_COLUMNS = [
    "claim_id",
    "claim_line_number",
    "claim_type",
    "member_id",
    "person_id",
    "claim_start_date",
    "claim_end_date",
    "claim_line_start_date",
    "claim_line_end_date",
    "admission_date",
    "discharge_date",
    "place_of_service_code",
    "bill_type_code",
    "revenue_center_code",
    "hcpcs_code",
    "hcpcs_modifier_1",
    "hcpcs_modifier_2",
    "rendering_npi",
    "rendering_tin",
    "billing_npi",
    "billing_tin",
    "facility_npi",
    "paid_amount",
    "allowed_amount",
    "charge_amount",
    "diagnosis_code_1",
    "diagnosis_code_2",
    "diagnosis_code_3",
]

_PHARMACY_CLAIM_COLUMNS = [
    "claim_id",
    "claim_line_number",
    "member_id",
    "person_id",
    "dispensing_date",
    "ndc_code",
    "prescribing_provider_npi",
    "dispensing_provider_npi",
    "quantity",
    "days_supply",
    "refills",
    "paid_date",
    "paid_amount",
    "allowed_amount",
    "charge_amount",
    "coinsurance_amount",
    "copayment_amount",
    "deductible_amount",
    "in_network_flag",
]

_ELIGIBILITY_COLUMNS = [
    "person_id",
    "member_id",
    "subscriber_id",
    "gender",
    "race",
    "birth_date",
    "death_date",
    "death_flag",
    "enrollment_start_date",
    "enrollment_end_date",
    "payer",
    "payer_type",
    "plan",
]


class DataPlugins(PluginRegistry):
    """Tiered dataset accessors + composed queries for notebook orchestration."""

    # ---- generic medallion loaders --------------------------------------

    def _load_dataset(
        self,
        tier: MedallionTier,
        dataset_name: str,
        lazy: bool,
        path: Path | None,
    ) -> pl.LazyFrame | pl.DataFrame:
        base = Path(path) if path else Path(self.storage.get_path(tier))
        file_path = base / f"{dataset_name}.parquet"
        if not file_path.exists():
            raise FileNotFoundError(f"Dataset not found: {file_path}")
        return pl.scan_parquet(file_path) if lazy else pl.read_parquet(file_path)

    def load_gold_dataset(
        self,
        dataset_name: str,
        lazy: bool = True,
        path: Path | None = None,
    ) -> pl.LazyFrame | pl.DataFrame:
        return self._load_dataset("gold", dataset_name, lazy, path)

    def load_silver_dataset(
        self,
        dataset_name: str,
        lazy: bool = True,
        path: Path | None = None,
    ) -> pl.LazyFrame | pl.DataFrame:
        return self._load_dataset("silver", dataset_name, lazy, path)

    # ---- header metadata ------------------------------------------------

    def dataset_metadata(
        self,
        datasets: dict[str, tuple[str, str]],
        tier: MedallionTier = "gold",
    ) -> dict[str, dict[str, Any]]:
        """
        Lightweight rows / columns / date-range / last-run digest for a header.

        ``datasets`` maps display-name → (filename, date_column). Returns one
        entry per dataset that exists on disk; missing files are skipped.
        """
        base = Path(self.storage.get_path(tier))
        tracking_dir = Path(self.storage.get_path("logs")) / "tracking"
        out: dict[str, dict[str, Any]] = {}
        for name, (filename, date_col) in datasets.items():
            file_path = base / filename
            if not file_path.exists():
                continue
            lf = pl.scan_parquet(str(file_path))
            rows = lf.select(pl.len()).collect().item()
            cols = len(lf.collect_schema())
            stats = lf.select(
                pl.col(date_col).min().alias("min_d"),
                pl.col(date_col).max().alias("max_d"),
            ).collect()
            min_d = stats["min_d"][0]
            max_d = stats["max_d"][0]
            entry: dict[str, Any] = {
                "rows": rows,
                "columns": cols,
                "min_date": str(min_d) if min_d else "N/A",
                "max_date": str(max_d) if max_d else "N/A",
                "last_run": None,
            }
            tracker = tracking_dir / f"{name}_state.json"
            if tracker.exists():
                try:
                    last_run = json.loads(tracker.read_text()).get("last_run", "")
                    if last_run:
                        entry["last_run"] = last_run[:19]
                except (OSError, json.JSONDecodeError):
                    pass  # ALLOWED: header best-effort, missing tracker is fine
            out[name] = entry
        return out

    # ---- identity resolution -------------------------------------------

    def resolve_identity(
        self,
        mbi: str,
        timeline_lf: pl.LazyFrame | None = None,
    ) -> dict[str, Any]:
        """
        Look up an MBI's identity-timeline chain.

        Returns ``{hcmpi, current_mbi, history, chain_df}``. ``chain_df`` is
        ``None`` when the MBI isn't found (in which case ``current_mbi`` is the
        input and ``history`` is just ``[mbi]``).
        """
        empty = {
            "hcmpi": None,
            "current_mbi": mbi,
            "history": [mbi] if mbi else [],
            "chain_df": None,
        }
        if not mbi:
            return empty
        if timeline_lf is None:
            timeline_lf = self.load_silver_dataset("identity_timeline", lazy=True)

        chain_id_df = (
            timeline_lf.filter(pl.col("mbi") == mbi)
            .select("chain_id")
            .unique()
            .collect()
        )
        if chain_id_df.height == 0:
            return empty
        chain_id = chain_id_df["chain_id"][0]
        chain_df = timeline_lf.filter(pl.col("chain_id") == chain_id).collect()
        if chain_df.height == 0:
            return empty

        hcmpi_vals = chain_df.filter(pl.col("hcmpi").is_not_null())["hcmpi"]
        hcmpi = hcmpi_vals[0] if len(hcmpi_vals) > 0 else None
        leaf = chain_df.filter(pl.col("hop_index") == 0)
        current_mbi = leaf["mbi"][0] if leaf.height > 0 else mbi
        history = chain_df.select("mbi").unique()["mbi"].to_list()
        return {
            "hcmpi": hcmpi,
            "current_mbi": current_mbi,
            "history": history,
            "chain_df": chain_df,
        }

    # ---- patient-level fetchers ----------------------------------------

    def get_demographics(
        self,
        mbi: str,
        demographics_lf: pl.LazyFrame | None = None,
    ) -> pl.DataFrame | None:
        if not mbi:
            return None
        if demographics_lf is None:
            demographics_lf = self.load_silver_dataset("beneficiary_demographics", lazy=True)
        df = demographics_lf.filter(pl.col("bene_mbi_id") == mbi).collect()
        return df if df.height > 0 else None

    def get_alignment(
        self,
        mbi: str,
        alignment_lf: pl.LazyFrame | None = None,
    ) -> pl.DataFrame | None:
        if not mbi:
            return None
        if alignment_lf is None:
            alignment_lf = self.load_gold_dataset("consolidated_alignment", lazy=True)
        df = alignment_lf.filter(pl.col("current_mbi") == mbi).collect()
        return df if df.height > 0 else None

    def get_chronic_conditions(
        self,
        current_mbi: str,
        hcmpi: str | None = None,
        conditions_lf: pl.LazyFrame | None = None,
    ) -> pl.DataFrame | None:
        """Look up by current_mbi first, fall back to hcmpi (gold person_id varies)."""
        if not current_mbi:
            return None
        if conditions_lf is None:
            conditions_lf = self.load_gold_dataset("chronic_conditions_wide", lazy=True)
        df = conditions_lf.filter(pl.col("person_id") == current_mbi).collect()
        if df.height == 0 and hcmpi:
            df = conditions_lf.filter(pl.col("person_id") == str(hcmpi)).collect()
        return df if df.height > 0 else None

    def get_patient_medical_lines(
        self,
        mbi: str,
        medical_lf: pl.LazyFrame | None = None,
    ) -> pl.DataFrame | None:
        if not mbi:
            return None
        if medical_lf is None:
            medical_lf = self.load_gold_dataset("medical_claim", lazy=True)
        df = (
            medical_lf.filter(pl.col("person_id") == mbi)
            .sort("claim_start_date", descending=True)
            .collect()
        )
        return df if df.height > 0 else None

    def get_patient_pharmacy_lines(
        self,
        mbi: str,
        pharmacy_lf: pl.LazyFrame | None = None,
    ) -> pl.DataFrame | None:
        if not mbi:
            return None
        if pharmacy_lf is None:
            pharmacy_lf = self.load_gold_dataset("pharmacy_claim", lazy=True)
        df = (
            pharmacy_lf.filter(pl.col("person_id") == mbi)
            .sort("dispensing_date", descending=True)
            .collect()
        )
        return df if df.height > 0 else None

    def get_yearly_spend_and_utilization(
        self,
        mbi: str,
        medical_lf: pl.LazyFrame | None = None,
        pharmacy_lf: pl.LazyFrame | None = None,
    ) -> pl.DataFrame:
        """
        Per-year spend by claim category + utilization counts, joined with pharmacy.

        Output columns: ``year``, the seven spend categories, ``total_medical_spend``,
        ``ip_admissions``, ``er_visits``, ``em_visits``, ``awv_visits``,
        ``pharmacy_spend``, ``pharmacy_claims_count``, ``total_spend``. Empty
        DataFrame when the patient has no claims.
        """
        if medical_lf is None:
            medical_lf = self.load_gold_dataset("medical_claim", lazy=True)
        if pharmacy_lf is None:
            pharmacy_lf = self.load_gold_dataset("pharmacy_claim", lazy=True)

        medical = (
            medical_lf.filter(pl.col("person_id") == mbi)
            .with_columns(pl.col("claim_start_date").dt.year().alias("year"))
            .group_by("year")
            .agg(
                pl.when(
                    pl.col("bill_type_code").str.starts_with("11")
                    | pl.col("bill_type_code").str.starts_with("12")
                ).then(pl.col("paid_amount")).otherwise(0).sum().alias("inpatient_spend"),
                pl.when(pl.col("bill_type_code").str.starts_with("13"))
                .then(pl.col("paid_amount")).otherwise(0).sum().alias("outpatient_spend"),
                pl.when(
                    pl.col("bill_type_code").str.starts_with("21")
                    | pl.col("bill_type_code").str.starts_with("22")
                ).then(pl.col("paid_amount")).otherwise(0).sum().alias("snf_spend"),
                pl.when(
                    pl.col("bill_type_code").str.starts_with("81")
                    | pl.col("bill_type_code").str.starts_with("82")
                ).then(pl.col("paid_amount")).otherwise(0).sum().alias("hospice_spend"),
                pl.when(
                    pl.col("bill_type_code").str.starts_with("32")
                    | pl.col("bill_type_code").str.starts_with("33")
                    | pl.col("bill_type_code").str.starts_with("34")
                ).then(pl.col("paid_amount")).otherwise(0).sum().alias("home_health_spend"),
                pl.when(pl.col("bill_type_code").is_null() | (pl.col("bill_type_code") == ""))
                .then(pl.col("paid_amount")).otherwise(0).sum().alias("part_b_carrier_spend"),
                pl.col("paid_amount").sum().alias("total_medical_spend"),
                pl.when(
                    pl.col("bill_type_code").str.starts_with("11")
                    | pl.col("bill_type_code").str.starts_with("12")
                ).then(1).otherwise(0).sum().alias("ip_admissions"),
                pl.when(
                    pl.col("revenue_center_code").is_in(
                        ["0450", "0451", "0452", "0453", "0454", "0455", "0456", "0457", "0458", "0459"]
                    )
                    | (pl.col("place_of_service_code") == "23")
                ).then(1).otherwise(0).sum().alias("er_visits"),
                pl.when(
                    ((pl.col("hcpcs_code") >= "99201") & (pl.col("hcpcs_code") <= "99215"))
                    | ((pl.col("hcpcs_code") >= "99241") & (pl.col("hcpcs_code") <= "99245"))
                    | ((pl.col("hcpcs_code") >= "99281") & (pl.col("hcpcs_code") <= "99285"))
                ).then(1).otherwise(0).sum().alias("em_visits"),
                pl.when(pl.col("hcpcs_code").is_in(["G0438", "G0439"]))
                .then(1).otherwise(0).sum().alias("awv_visits"),
            )
            .sort("year", descending=True)
            .collect()
        )

        pharmacy = (
            pharmacy_lf.filter(pl.col("person_id") == mbi)
            .with_columns(pl.col("dispensing_date").dt.year().alias("year"))
            .group_by("year")
            .agg(
                pl.col("paid_amount").sum().alias("pharmacy_spend"),
                pl.len().alias("pharmacy_claims_count"),
            )
            .collect()
        )

        if medical.height == 0 and pharmacy.height == 0:
            return pl.DataFrame()

        zero_spend = [
            "inpatient_spend", "outpatient_spend", "snf_spend", "hospice_spend",
            "home_health_spend", "part_b_carrier_spend", "total_medical_spend",
        ]
        zero_util = ["ip_admissions", "er_visits", "em_visits", "awv_visits"]

        if medical.height == 0:
            joined = pharmacy.with_columns(
                *[pl.lit(0.0).alias(c) for c in zero_spend],
                *[pl.lit(0).alias(c) for c in zero_util],
            )
        elif pharmacy.height == 0:
            joined = medical.with_columns(
                pl.lit(0.0).alias("pharmacy_spend"),
                pl.lit(0).alias("pharmacy_claims_count"),
            )
        else:
            joined = medical.join(pharmacy, on="year", how="full", coalesce=True).fill_null(0)

        return joined.with_columns(
            (pl.col("total_medical_spend") + pl.col("pharmacy_spend")).alias("total_spend")
        ).sort("year", descending=True)

    # ---- member-level fetchers (eligibility / claims search) ------------

    def get_member_eligibility(
        self,
        member_ids: list[str],
        eligibility_lf: pl.LazyFrame | None = None,
    ) -> pl.DataFrame | None:
        if not member_ids:
            return None
        if eligibility_lf is None:
            eligibility_lf = self.load_gold_dataset("eligibility", lazy=True)
        result = (
            eligibility_lf.filter(pl.col("member_id").is_in(member_ids))
            .select(_ELIGIBILITY_COLUMNS)
            .collect()
        )
        return result if result.height > 0 else None

    def get_medical_claims(
        self,
        filters: dict[str, Any] | None = None,
        medical_claim_lf: pl.LazyFrame | None = None,
    ) -> pl.DataFrame | None:
        """Member ID / HCPCS / NPI / TIN / date-range claim line search."""
        filters = filters or {}
        if medical_claim_lf is None:
            medical_claim_lf = self.load_gold_dataset("medical_claim", lazy=True)
        query = medical_claim_lf

        if filters.get("member_ids"):
            query = query.filter(pl.col("member_id").is_in(filters["member_ids"]))
        if filters.get("hcpcs_codes"):
            codes = filters["hcpcs_codes"]
            query = query.filter(
                pl.col("hcpcs_code").is_in(codes)
                | pl.col("hcpcs_modifier_1").is_in(codes)
                | pl.col("hcpcs_modifier_2").is_in(codes)
            )
        if filters.get("npi_codes"):
            npis = filters["npi_codes"]
            query = query.filter(
                pl.col("rendering_npi").is_in(npis)
                | pl.col("billing_npi").is_in(npis)
                | pl.col("facility_npi").is_in(npis)
            )
        if filters.get("tin_codes"):
            query = query.filter(pl.col("billing_tin").is_in(filters["tin_codes"]))
        if "start_date" in filters:
            query = query.filter(pl.col("claim_start_date") >= filters["start_date"])
        if "end_date" in filters:
            query = query.filter(pl.col("claim_start_date") <= filters["end_date"])

        result = (
            query.select(_MEDICAL_CLAIM_COLUMNS)
            .sort("claim_start_date", descending=True)
            .collect()
        )
        return result if result.height > 0 else None

    def get_pharmacy_claims(
        self,
        member_ids: list[str],
        pharmacy_claim_lf: pl.LazyFrame | None = None,
    ) -> pl.DataFrame | None:
        if not member_ids:
            return None
        if pharmacy_claim_lf is None:
            pharmacy_claim_lf = self.load_gold_dataset("pharmacy_claim", lazy=True)
        result = (
            pharmacy_claim_lf.filter(pl.col("member_id").is_in(member_ids))
            .select(_PHARMACY_CLAIM_COLUMNS)
            .sort("dispensing_date", descending=True)
            .collect()
        )
        return result if result.height > 0 else None
