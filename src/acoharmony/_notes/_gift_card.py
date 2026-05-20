# © 2025 HarmonyCares
# All rights reserved.

"""
Gift-card distribution analytics.

Backs ``notebooks/gift_card_analysis.py``: data-quality checks across
GCM / BAR / HDAI sources, address enrichment from CCLF8, mailing-list
generation with usaddress parsing, duplicate analysis, three-source AWV
comparison (with medical claims), and DirectDelivery order formatting.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry

AWV_HCPCS_CODES = ("G0438", "G0439")


class GiftCardPlugins(PluginRegistry):
    """Gift-card distribution analytics."""

    # ---- raw loading ------------------------------------------------

    def load_sources(self, silver_path: Path) -> dict[str, pl.LazyFrame]:
        """Load GCM / BAR (filtered to current REACH) / HDAI (latest)."""
        from acoharmony._expressions._current_reach import (
            build_current_reach_with_bar_expr,
        )

        silver = Path(silver_path)
        gcm_path = silver / "gcm.parquet"
        bar_path = silver / "bar.parquet"
        hdai_path = silver / "hdai_reach.parquet"

        lf_gcm = pl.scan_parquet(gcm_path) if gcm_path.exists() else pl.LazyFrame()

        if bar_path.exists():
            lf_bar_all = pl.scan_parquet(bar_path)
            schema = lf_bar_all.collect_schema().names()
            lf_bar = lf_bar_all.filter(build_current_reach_with_bar_expr(df_schema=schema))
        else:
            lf_bar = pl.LazyFrame()

        if hdai_path.exists():
            lf_hdai_all = pl.scan_parquet(hdai_path)
            max_date = lf_hdai_all.select(pl.col("file_date").max()).collect().item()
            lf_hdai = lf_hdai_all.filter(pl.col("file_date") == max_date)
        else:
            lf_hdai = pl.LazyFrame()

        return {"gcm": lf_gcm, "bar": lf_bar, "hdai": lf_hdai}

    def get_most_recent_data(self, lf: pl.LazyFrame) -> pl.LazyFrame:
        try:
            schema = lf.collect_schema()
            if len(schema) == 0:
                return lf
        except Exception:  # ALLOWED: empty/invalid LazyFrame
            return lf
        names = schema.names()
        filtered = lf
        if "file_date" in names and schema["file_date"] != pl.Null:
            most_recent = lf.select(pl.col("file_date").max()).collect().item()
            filtered = lf.filter(pl.col("file_date") == most_recent)
        if "mbi" in names:
            if "processed_at" in names:
                filtered = (
                    filtered.sort("processed_at", descending=True)
                    .unique(subset=["mbi"], keep="first")
                )
            else:
                filtered = filtered.unique(subset=["mbi"], keep="first")
        return filtered

    # ---- data quality summaries -------------------------------------

    def data_overview(
        self, lf_gcm: pl.LazyFrame, lf_bar: pl.LazyFrame, lf_hdai: pl.LazyFrame
    ) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "Data Source": [
                    "GCM (Gift Card Management)",
                    "BAR (Beneficiary Alignment)",
                    "HDAI REACH",
                ],
                "Record Count": [
                    lf_gcm.select(pl.len()).collect().item(),
                    lf_bar.select(pl.len()).collect().item(),
                    lf_hdai.select(pl.len()).collect().item(),
                ],
            }
        )

    def source_overlap(
        self, lf_gcm: pl.LazyFrame, lf_bar: pl.LazyFrame, lf_hdai: pl.LazyFrame
    ) -> pl.DataFrame:
        gcm_latest = self.get_most_recent_data(lf_gcm)
        bar_mbis = set(lf_bar.select("bene_mbi").collect()["bene_mbi"].to_list())
        gcm_mbis = set(gcm_latest.select("mbi").collect()["mbi"].to_list())
        hdai_mbis = set(lf_hdai.select("mbi").collect()["mbi"].to_list())
        return pl.DataFrame(
            {
                "Category": [
                    "Total Current REACH BAR",
                    "Total GCM Patients",
                    "Total HDAI Patients",
                    "GCM patients NOT on latest BAR",
                    "HDAI patients NOT on latest BAR",
                    "Patients in both GCM+HDAI but NOT on BAR",
                ],
                "Count": [
                    len(bar_mbis),
                    len(gcm_mbis),
                    len(hdai_mbis),
                    len(gcm_mbis - bar_mbis),
                    len(hdai_mbis - bar_mbis),
                    len((gcm_mbis & hdai_mbis) - bar_mbis),
                ],
            }
        )

    def gift_card_status_dist(self, lf_gcm: pl.LazyFrame) -> pl.DataFrame:
        return (
            lf_gcm.group_by("gift_card_status")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .collect()
        )

    def deceased_check(
        self, lf_gcm: pl.LazyFrame, lf_bar: pl.LazyFrame, lf_hdai: pl.LazyFrame
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        gcm_latest = self.get_most_recent_data(lf_gcm)
        joined = (
            lf_bar.select(["bene_mbi", "bene_date_of_death"])
            .join(
                gcm_latest.select(["mbi", "gift_card_status", "awv_date", "hcmpi"]),
                left_on="bene_mbi",
                right_on="mbi",
                how="left",
            )
            .join(
                lf_hdai.select(["mbi", "patient_dod"]),
                left_on="bene_mbi",
                right_on="mbi",
                how="left",
                suffix="_hdai",
            )
            .with_columns(
                pl.col("bene_date_of_death").is_not_null().alias("deceased_in_bar"),
                pl.col("patient_dod").is_not_null().alias("deceased_in_hdai"),
                pl.col("gift_card_status").is_not_null().alias("in_gcm"),
            )
            .collect()
        )
        total = joined.height
        deceased_bar = int(joined["deceased_in_bar"].sum())
        deceased_in_gcm = int(
            (joined["deceased_in_bar"] & joined["in_gcm"]).sum()
        )
        summary = pl.DataFrame(
            {
                "Status": [
                    "Total Current REACH BAR Patients",
                    "BAR patients marked DECEASED",
                    "Deceased BAR patients also in GCM",
                ],
                "Count": [total, deceased_bar, deceased_in_gcm],
            }
        )
        deceased_list = joined.filter(
            pl.col("deceased_in_bar") | pl.col("deceased_in_hdai")
        ).select(
            [
                "hcmpi",
                "bene_mbi",
                "in_gcm",
                "gift_card_status",
                "awv_date",
                "bene_date_of_death",
                "patient_dod",
            ]
        )
        return summary, deceased_list

    def awv_comparison(
        self, lf_gcm: pl.LazyFrame, lf_bar: pl.LazyFrame, lf_hdai: pl.LazyFrame
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        gcm_latest = self.get_most_recent_data(lf_gcm)
        cmp = (
            lf_bar.select(["bene_mbi"])
            .join(
                gcm_latest.select(["mbi", "awv_date", "awv_status", "hcmpi"]),
                left_on="bene_mbi",
                right_on="mbi",
                how="left",
            )
            .join(
                lf_hdai.select(["mbi", "most_recent_awv_date"]),
                left_on="bene_mbi",
                right_on="mbi",
                how="left",
            )
            .with_columns(
                pl.col("awv_date").str.to_date().alias("awv_date_parsed"),
                pl.col("most_recent_awv_date").alias("hdai_awv_date_parsed"),
            )
            .with_columns(
                (pl.col("awv_date_parsed") == pl.col("hdai_awv_date_parsed")).alias("dates_match"),
                (pl.col("awv_date_parsed") - pl.col("hdai_awv_date_parsed"))
                .dt.total_days()
                .alias("date_diff_days"),
            )
            .collect()
        )
        total = cmp.height
        both = int(
            (
                cmp["awv_date_parsed"].is_not_null()
                & cmp["hdai_awv_date_parsed"].is_not_null()
            ).sum()
        )
        match = int(cmp["dates_match"].sum() or 0)
        differ = int((~cmp["dates_match"]).sum() or 0)
        summary = pl.DataFrame(
            {
                "Status": [
                    "Total Current REACH BAR",
                    "AWV in both GCM and HDAI",
                    "AWV Dates Match",
                    "AWV Dates Differ",
                ],
                "Count": [total, both, match, differ],
            }
        )
        mismatches = cmp.filter(~pl.col("dates_match")).select(
            [
                "hcmpi",
                "bene_mbi",
                "awv_date",
                "most_recent_awv_date",
                "date_diff_days",
                "awv_status",
            ]
        )
        return summary, mismatches

    def missing_addresses(
        self, lf_gcm: pl.LazyFrame
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        df = lf_gcm.collect()
        with_flag = df.with_columns(
            (
                pl.col("patientaddress").is_null()
                | (pl.col("patientaddress") == "null")
                | (pl.col("patientaddress").str.strip_chars() == "")
            ).alias("address_missing")
        )
        missing = int(with_flag["address_missing"].sum() or 0)
        summary = pl.DataFrame(
            {
                "Status": ["Total GCM Records", "Missing Addresses"],
                "Count": [with_flag.height, missing],
            }
        )
        missing_list = with_flag.filter(pl.col("address_missing")).select(
            [
                "hcmpi",
                "mbi",
                "gift_card_status",
                "patientaddress",
                "patientcity",
                "patientstate",
                "patientzip",
            ]
        )
        return summary, missing_list

    def visit_metrics(self, lf_gcm: pl.LazyFrame) -> pl.DataFrame:
        return lf_gcm.select(
            pl.col("roll12_awv_enc").mean().alias("Avg AWV Visits"),
            pl.col("roll12_awv_enc").median().alias("Median AWV Visits"),
            pl.col("roll12_em").mean().alias("Avg E&M Visits"),
            pl.col("roll12_em").median().alias("Median E&M Visits"),
        ).collect()

    # ---- medical claim AWV --------------------------------------------

    def medical_claim_awv(
        self,
        gold_path: Path,
        cutoff_date: str = "2025-01-01",
    ) -> pl.LazyFrame:
        path = Path(gold_path) / "medical_claim.parquet"
        if not path.exists():
            return pl.LazyFrame()
        lf = pl.scan_parquet(path)
        wellness = lf.filter(
            (pl.col("hcpcs_code").cast(pl.Utf8).is_in(list(AWV_HCPCS_CODES)))
            & (pl.col("claim_line_start_date") >= pl.lit(cutoff_date).str.to_date())
        )
        return (
            wellness.sort("claim_line_start_date", descending=True)
            .group_by("member_id")
            .agg(
                pl.col("claim_line_start_date").first().alias("claim_awv_date"),
                pl.col("hcpcs_code").first().alias("claim_awv_code"),
                pl.col("claim_id").first().alias("claim_id"),
                pl.col("source_filename").first().alias("claim_source_date"),
            )
            .rename({"member_id": "mbi"})
        )

    def three_source_comparison(
        self,
        lf_gcm: pl.LazyFrame,
        lf_bar: pl.LazyFrame,
        lf_hdai: pl.LazyFrame,
        lf_claim_awv: pl.LazyFrame,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        gcm_latest = self.get_most_recent_data(lf_gcm)
        bar_active = lf_bar.select(
            pl.col("bene_mbi").alias("mbi"),
            pl.col("bene_date_of_death").alias("bar_death_date"),
            pl.lit(True).alias("in_bar"),
            pl.col("start_date").alias("bar_start_date"),
            pl.col("end_date").alias("bar_end_date"),
        )
        cmp = (
            bar_active.join(
                gcm_latest.select(["mbi", "awv_date", "gift_card_status", "hcmpi"]),
                on="mbi",
                how="left",
            )
            .with_columns(
                pl.col("awv_date").is_not_null().alias("has_awv_gcm"),
                pl.col("awv_date").alias("gcm_awv_date"),
            )
            .join(
                lf_hdai.select(["mbi", "most_recent_awv_date", "patient_dod"]),
                on="mbi",
                how="left",
            )
            .with_columns(
                pl.col("most_recent_awv_date").is_not_null().alias("has_awv_hdai"),
                pl.col("most_recent_awv_date").alias("hdai_awv_date"),
                pl.col("patient_dod").alias("hdai_death_date"),
            )
        )
        claim_dedup = (
            lf_claim_awv.select(
                ["mbi", "claim_awv_date", "claim_awv_code", "claim_id", "claim_source_date"]
            )
            .sort("claim_awv_date", descending=True)
            .unique(subset=["mbi"], keep="first")
        )
        cmp = cmp.join(claim_dedup, on="mbi", how="left").with_columns(
            pl.col("claim_awv_date").is_not_null().alias("has_awv_claim")
        )
        df = cmp.collect()
        df = df.with_columns(
            pl.col("gcm_awv_date").str.to_date().alias("gcm_awv_date_parsed")
        ).with_columns(
            (
                pl.col("has_awv_gcm").fill_null(False)
                & pl.col("has_awv_hdai").fill_null(False)
                & pl.col("has_awv_claim").fill_null(False)
            ).alias("all_three_agree_yes"),
            (
                ~pl.col("has_awv_gcm").fill_null(False)
                & ~pl.col("has_awv_hdai").fill_null(False)
                & ~pl.col("has_awv_claim").fill_null(False)
            ).alias("all_three_agree_no"),
            (pl.col("gcm_awv_date_parsed") != pl.col("hdai_awv_date")).alias("gcm_hdai_date_diff"),
            (pl.col("gcm_awv_date_parsed") != pl.col("claim_awv_date")).alias("gcm_claim_date_diff"),
            (pl.col("hdai_awv_date") != pl.col("claim_awv_date")).alias("hdai_claim_date_diff"),
            (pl.col("has_awv_gcm").fill_null(False) != pl.col("has_awv_hdai").fill_null(False)).alias("gcm_hdai_disagree"),
            (pl.col("has_awv_gcm").fill_null(False) != pl.col("has_awv_claim").fill_null(False)).alias("gcm_claim_disagree"),
            (pl.col("has_awv_hdai").fill_null(False) != pl.col("has_awv_claim").fill_null(False)).alias("hdai_claim_disagree"),
        )
        total = df.height
        all_yes = int(df["all_three_agree_yes"].sum() or 0)
        all_no = int(df["all_three_agree_no"].sum() or 0)
        any_disagree = total - all_yes - all_no
        gcm_only = int(
            (
                df["has_awv_gcm"].fill_null(False)
                & ~df["has_awv_hdai"].fill_null(False)
                & ~df["has_awv_claim"].fill_null(False)
            ).sum()
            or 0
        )
        hdai_only = int(
            (
                ~df["has_awv_gcm"].fill_null(False)
                & df["has_awv_hdai"].fill_null(False)
                & ~df["has_awv_claim"].fill_null(False)
            ).sum()
            or 0
        )
        claim_only = int(
            (
                ~df["has_awv_gcm"].fill_null(False)
                & ~df["has_awv_hdai"].fill_null(False)
                & df["has_awv_claim"].fill_null(False)
            ).sum()
            or 0
        )
        date_mismatches = int(
            (
                df["gcm_hdai_date_diff"].fill_null(False)
                | df["gcm_claim_date_diff"].fill_null(False)
                | df["hdai_claim_date_diff"].fill_null(False)
            ).sum()
            or 0
        )
        summary = pl.DataFrame(
            {
                "Metric": [
                    "Total Current REACH BAR",
                    "All 3 Sources Agree - Has AWV",
                    "All 3 Sources Agree - No AWV",
                    "Sources Disagree on AWV Presence",
                    "AWV in GCM Only",
                    "AWV in HDAI Only",
                    "AWV in CCLF Only",
                    "AWV Date Mismatches",
                ],
                "Count": [
                    total,
                    all_yes,
                    all_no,
                    any_disagree,
                    gcm_only,
                    hdai_only,
                    claim_only,
                    date_mismatches,
                ],
            }
        )
        disagreements = df.filter(
            pl.col("gcm_hdai_disagree")
            | pl.col("gcm_claim_disagree")
            | pl.col("hdai_claim_disagree")
            | pl.col("gcm_hdai_date_diff").fill_null(False)
            | pl.col("gcm_claim_date_diff").fill_null(False)
            | pl.col("hdai_claim_date_diff").fill_null(False)
        ).select(
            [
                "mbi",
                "hcmpi",
                "has_awv_gcm",
                "has_awv_hdai",
                "has_awv_claim",
                "gcm_awv_date",
                "hdai_awv_date",
                "claim_awv_date",
                "claim_awv_code",
                "claim_id",
                "claim_source_date",
                "gift_card_status",
                "bar_start_date",
                "bar_end_date",
                "gcm_hdai_disagree",
                "gcm_claim_disagree",
                "hdai_claim_disagree",
            ]
        )
        return summary, disagreements

    # ---- address enrichment + mailing list -----------------------------

    def enrich_gcm_addresses(self, silver_path: Path) -> pl.DataFrame:
        silver = Path(silver_path)
        gcm = pl.scan_parquet(silver / "gcm.parquet")
        bene_demo = pl.scan_parquet(silver / "cclf8.parquet")
        bene_demo_latest = (
            bene_demo.with_columns(
                pl.col("file_date")
                .rank(method="ordinal", descending=True)
                .over("bene_mbi_id")
                .alias("row_num")
            )
            .filter(pl.col("row_num") == 1)
            .select(
                pl.col("bene_mbi_id"),
                pl.col("bene_line_1_adr").alias("demo_address_line_1"),
                pl.col("bene_line_2_adr").alias("demo_address_line_2"),
                pl.col("bene_city").alias("demo_city"),
                pl.col("bene_state").alias("demo_state"),
                pl.col("bene_zip").alias("demo_zip"),
            )
        )
        gcm_enriched = gcm.join(
            bene_demo_latest, left_on="mbi", right_on="bene_mbi_id", how="left"
        )

        def _missing(col: str) -> pl.Expr:
            return (
                pl.col(col).is_null()
                | (pl.col(col) == "null")
                | (pl.col(col).str.strip_chars() == "")
            )

        return (
            gcm_enriched.with_columns(
                pl.col("patientaddress").cast(pl.Utf8),
                pl.col("patientaddress2").cast(pl.Utf8),
                pl.col("patientcity").cast(pl.Utf8),
                pl.col("patientstate").cast(pl.Utf8),
                pl.col("patientzip").cast(pl.Utf8),
            )
            .with_columns(
                _missing("patientaddress").alias("address_was_missing"),
                pl.when(_missing("patientaddress"))
                .then(pl.col("demo_address_line_1"))
                .otherwise(pl.col("patientaddress"))
                .alias("patientaddress_enriched"),
                pl.when(_missing("patientaddress2"))
                .then(pl.col("demo_address_line_2"))
                .otherwise(pl.col("patientaddress2"))
                .alias("patientaddress2_enriched"),
                pl.when(_missing("patientcity"))
                .then(pl.col("demo_city"))
                .otherwise(pl.col("patientcity"))
                .alias("patientcity_enriched"),
                pl.when(_missing("patientstate"))
                .then(pl.col("demo_state"))
                .otherwise(pl.col("patientstate"))
                .alias("patientstate_enriched"),
                pl.when(_missing("patientzip"))
                .then(pl.col("demo_zip"))
                .otherwise(pl.col("patientzip"))
                .alias("patientzip_enriched"),
            )
            .collect()
        )

    def parse_address(self, address_str: str) -> tuple[str, str | None]:
        """Split via usaddress; returns ``(street, apt)`` or ``(input, None)`` on failure."""
        if not address_str or not address_str.strip():
            return (address_str, None)
        try:
            import usaddress

            parsed, _ = usaddress.tag(address_str)
            street_components = (
                "AddressNumber",
                "AddressNumberPrefix",
                "AddressNumberSuffix",
                "StreetNamePreDirectional",
                "StreetNamePreModifier",
                "StreetNamePreType",
                "StreetName",
                "StreetNamePostType",
                "StreetNamePostDirectional",
                "StreetNamePostModifier",
            )
            apt_components = (
                "OccupancyType",
                "OccupancyIdentifier",
                "SubaddressType",
                "SubaddressIdentifier",
            )
            street = " ".join(parsed[c] for c in street_components if c in parsed) or address_str
            apt = " ".join(parsed[c] for c in apt_components if c in parsed) or None
            return (street, apt)
        except Exception:  # ALLOWED: usaddress fails on malformed input
            return (address_str, None)

    def gift_card_distribution(self, silver_path: Path) -> pl.DataFrame:
        from acoharmony._expressions._current_reach import (
            build_current_reach_with_bar_expr,
        )

        silver = Path(silver_path)
        bar = pl.scan_parquet(silver / "bar.parquet")
        hdai = pl.scan_parquet(silver / "hdai_reach.parquet")
        bar_schema = bar.collect_schema().names()
        current_reach_expr = build_current_reach_with_bar_expr(df_schema=bar_schema)
        bar_latest = bar.filter(current_reach_expr).with_columns(
            pl.col("bene_mbi").alias("mbi_bar"),
            pl.col("bene_date_of_death").alias("bar_death_date"),
            pl.lit(True).alias("is_alive_bar"),
            pl.lit(True).alias("in_bar"),
            pl.col("start_date").alias("bar_start_date"),
            pl.col("end_date").alias("bar_end_date"),
        )
        most_recent_bar_date = (
            bar.filter(current_reach_expr).select(pl.col("file_date").max()).collect().item()
        )
        most_recent_hdai_date = (
            hdai.select(pl.col("file_date").max()).collect().item()
        )
        hdai_latest = hdai.filter(
            (pl.col("file_date") == most_recent_hdai_date)
            & pl.col("patient_dod").is_null()
        ).with_columns(
            pl.col("mbi").alias("mbi_hdai"),
            pl.lit(None).cast(pl.Date).alias("hdai_death_date"),
            pl.lit(True).alias("is_alive_hdai"),
            pl.lit(True).alias("in_hdai"),
            pl.col("enrollment_status").alias("hdai_enrollment_status"),
            pl.col("most_recent_awv_date").alias("hdai_awv_date"),
            pl.col("last_em_visit").alias("hdai_last_em"),
        )
        gcm_enriched_df = self.enrich_gcm_addresses(silver_path)
        gcm = pl.LazyFrame(gcm_enriched_df).with_columns(
            pl.col("mbi").alias("mbi_gcm"),
            pl.lit(True).alias("in_gcm"),
            pl.col("awv_date").alias("gcm_awv_date"),
            pl.col("awv_status").alias("gcm_awv_status"),
            pl.col("gift_card_status"),
            pl.col("lc_status_current").alias("gcm_lifecycle_status"),
            pl.col("roll12_awv_enc").alias("gcm_roll12_awv"),
            pl.col("roll12_em").alias("gcm_roll12_em"),
            pl.col("patientaddress_enriched").alias("patientaddress"),
            pl.col("patientaddress2_enriched").alias("patientaddress2"),
            pl.col("patientcity_enriched").alias("patientcity"),
            pl.col("patientstate_enriched").alias("patientstate"),
            pl.col("patientzip_enriched").alias("patientzip"),
        )
        gcm_bar = gcm.join(
            bar_latest.select(
                [
                    "bene_mbi",
                    "bar_death_date",
                    "is_alive_bar",
                    "in_bar",
                    "bar_start_date",
                    "bar_end_date",
                    "bene_first_name",
                    "bene_last_name",
                    "bene_address_line_1",
                    "bene_address_line_2",
                    "bene_city",
                    "bene_state",
                    "bene_zip_5",
                ]
            ),
            left_on="mbi",
            right_on="bene_mbi",
            how="left",
        )
        gcm_bar_hdai = gcm_bar.join(
            hdai_latest.select(
                [
                    "mbi",
                    "hdai_death_date",
                    "is_alive_hdai",
                    "in_hdai",
                    "hdai_enrollment_status",
                    "hdai_awv_date",
                    "hdai_last_em",
                    "patient_first_name",
                    "patient_last_name",
                    "patient_address",
                    "patient_city",
                    "patient_state",
                    "patient_zip",
                ]
            ),
            left_on="mbi",
            right_on="mbi",
            how="left",
            suffix="_hdai",
        )
        result = gcm_bar_hdai.with_columns(
            pl.col("in_bar").fill_null(False).alias("in_bar"),
            pl.col("in_hdai").fill_null(False).alias("in_hdai"),
            pl.lit(True).alias("is_alive"),
            pl.lit(None).cast(pl.Date).alias("death_date"),
            pl.when(pl.col("in_bar").fill_null(False) & pl.col("in_hdai").fill_null(False))
            .then(pl.lit("GCM+BAR+HDAI"))
            .when(pl.col("in_bar").fill_null(False))
            .then(pl.lit("GCM+BAR"))
            .when(pl.col("in_hdai").fill_null(False))
            .then(pl.lit("GCM+HDAI"))
            .otherwise(pl.lit("GCM Only"))
            .alias("data_source_status"),
            pl.coalesce(["bene_first_name", "patient_first_name"]).alias("first_name"),
            pl.coalesce(["bene_last_name", "patient_last_name"]).alias("last_name"),
            pl.coalesce(["bene_address_line_1", "patient_address", "patientaddress"]).alias("address_line_1"),
            pl.coalesce(["bene_address_line_2", "patientaddress2"]).alias("address_line_2"),
            pl.coalesce(["bene_city", "patient_city", "patientcity"]).alias("city"),
            pl.coalesce(["bene_state", "patient_state", "patientstate"]).alias("state"),
            pl.coalesce(["bene_zip_5", "patient_zip", "patientzip"]).alias("zip"),
            pl.lit(most_recent_bar_date).alias("bar_report_date"),
            pl.lit(most_recent_hdai_date).alias("hdai_report_date"),
        )
        final = result.select(
            [
                "hcmpi",
                "mbi",
                "first_name",
                "last_name",
                "is_alive",
                "death_date",
                "in_gcm",
                "in_bar",
                "in_hdai",
                "data_source_status",
                "gift_card_status",
                "gcm_lifecycle_status",
                "gcm_awv_date",
                "hdai_awv_date",
                "gcm_awv_status",
                "gcm_roll12_awv",
                "gcm_roll12_em",
                "hdai_last_em",
                "bar_start_date",
                "bar_end_date",
                "hdai_enrollment_status",
                "address_line_1",
                "address_line_2",
                "city",
                "state",
                "zip",
                "bar_report_date",
                "hdai_report_date",
                "payer",
                "payer_current",
                "total_count",
            ]
        ).collect()
        return self._apply_address_parsing(final)

    def _apply_address_parsing(self, df: pl.DataFrame) -> pl.DataFrame:
        streets: list[Any] = []
        apts: list[Any] = []
        for row in df.iter_rows(named=True):
            addr1 = row.get("address_line_1")
            addr2 = row.get("address_line_2")
            if not addr1 or not str(addr1).strip() or str(addr1) == "null":
                streets.append(addr1)
                apts.append(addr2)
                continue
            if addr2 and str(addr2).strip() and str(addr2) != "null":
                streets.append(addr1)
                apts.append(addr2)
                continue
            street, apt = self.parse_address(str(addr1))
            streets.append(street if street else addr1)
            apts.append(apt)
        return df.with_columns(
            pl.Series("address_line_1", streets),
            pl.Series("address_line_2", apts),
        )

    def gift_card_mailing_list(
        self, silver_path: Path
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        gcm_enriched = self.enrich_gcm_addresses(silver_path)
        distro = self.gift_card_distribution(silver_path)
        raw = distro.filter(
            pl.col("in_bar")
            & pl.col("address_line_1").is_not_null()
            & (pl.col("address_line_1") != "null")
            & (pl.col("address_line_1").str.strip_chars() != "")
        ).select(
            [
                "hcmpi",
                "mbi",
                "first_name",
                "last_name",
                "address_line_1",
                "address_line_2",
                "city",
                "state",
                "zip",
                "gift_card_status",
                "gcm_awv_date",
                "hdai_awv_date",
                "gcm_awv_status",
                "data_source_status",
                "bar_start_date",
                "bar_end_date",
                "hdai_enrollment_status",
                "payer",
                "payer_current",
            ]
        )
        mailing = raw.unique(
            subset=["hcmpi", "mbi", "first_name", "last_name", "address_line_1"],
            keep="first",
        )
        return gcm_enriched, mailing

    def analyze_duplicates(
        self, mailing_list_raw: pl.DataFrame
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        natural_key = ["hcmpi", "mbi", "first_name", "last_name", "address_line_1"]
        with_count = mailing_list_raw.with_columns(
            pl.struct(natural_key).count().over(natural_key).alias("duplicate_count")
        )
        duplicates = with_count.filter(pl.col("duplicate_count") > 1)
        total_raw = mailing_list_raw.height
        total_dup = duplicates.height
        unique_groups = duplicates.select(natural_key).unique().height
        with_key = duplicates.with_columns(
            pl.concat_str(
                [
                    pl.col("hcmpi").cast(pl.Utf8),
                    pl.lit("||"),
                    pl.col("mbi").cast(pl.Utf8),
                    pl.lit("||"),
                    pl.col("first_name").cast(pl.Utf8),
                    pl.lit("||"),
                    pl.col("last_name").cast(pl.Utf8),
                    pl.lit("||"),
                    pl.col("address_line_1").cast(pl.Utf8),
                ],
                ignore_nulls=False,
            ).alias("natural_key_composite")
        )
        varying: list[str] = []
        if total_dup > 0:
            for col in duplicates.columns:
                if col in natural_key or col == "duplicate_count":
                    continue
                distinct_per_group = (
                    with_key.group_by("natural_key_composite")
                    .agg(pl.col(col).n_unique().alias("distinct_count"))
                )
                max_distinct = distinct_per_group["distinct_count"].max()
                if max_distinct is not None and max_distinct > 1:
                    varying.append(col)
        summary = pl.DataFrame(
            {
                "Metric": [
                    "Total Raw Mailing List Records",
                    "Total Duplicate Records (same natural key)",
                    "Unique Natural Key Groups with Duplicates",
                    "Max Duplicates per Natural Key",
                    "Columns That Vary Within Duplicate Groups",
                ],
                "Value": [
                    str(total_raw),
                    str(total_dup),
                    str(unique_groups),
                    str(duplicates["duplicate_count"].max()) if total_dup else "0",
                    ", ".join(varying) if varying else "None",
                ],
            }
        )
        if total_dup > 0:
            sort_cols = ["natural_key_composite"] + varying if varying else [
                "natural_key_composite"
            ]
            details = with_key.sort(sort_cols).select(
                natural_key + ["duplicate_count"] + varying
            )
        else:
            details = pl.DataFrame(
                {
                    col: pl.Series([], dtype=mailing_list_raw.schema[col])
                    for col in natural_key
                }
            ).with_columns(pl.Series("duplicate_count", [], dtype=pl.Int64))
        return summary, details

    def format_direct_delivery(
        self,
        mailing_list: pl.DataFrame,
        card_name: str = "Visa",
        card_value: float = 50.0,
        shipping_method: str = "Standard",
        country_code: str = "US",
        message: str = "Thank you for completing your Annual Wellness Visit!",
        sender_name: str = "HarmonyCares",
    ) -> pl.DataFrame:
        return mailing_list.select(
            pl.col("first_name").alias("First Name"),
            pl.col("last_name").alias("Last Name"),
            pl.lit(None).cast(pl.Utf8).alias("Company"),
            pl.col("mbi").alias("Recipient ID (Optional)"),
            pl.col("address_line_1").alias("Street Address"),
            pl.col("address_line_2").alias("Apt #, Floor, etc., (optional)"),
            pl.col("city").alias("City"),
            pl.col("state").alias("State/Province/Region"),
            pl.col("zip").alias("Postal Code"),
            pl.lit(country_code).alias("Country Code"),
            pl.lit(shipping_method).alias("Shipping Method"),
            pl.lit(card_name).alias("Card Name"),
            pl.lit(None).cast(pl.Utf8).alias("Available Denominations"),
            pl.lit(card_value).alias("Card Value"),
            pl.concat_str(
                [pl.col("first_name"), pl.lit(" "), pl.col("last_name")],
                ignore_nulls=True,
            ).alias("To"),
            pl.lit(sender_name).alias("From"),
            pl.lit(message).alias("Message"),
            pl.lit(None).cast(pl.Utf8).alias("Card Carrier"),
        )

    def mailing_list_summary(self, mailing_list: pl.DataFrame) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "Metric": [
                    "Total Eligible for Mailing",
                    "Has AWV Date in GCM",
                    "Has AWV Date in HDAI",
                    "Gift Card Status: Not Distributed",
                ],
                "Count": [
                    mailing_list.height,
                    mailing_list.filter(pl.col("gcm_awv_date").is_not_null()).height,
                    mailing_list.filter(pl.col("hdai_awv_date").is_not_null()).height,
                    mailing_list.filter(
                        pl.col("gift_card_status").is_null()
                        | (pl.col("gift_card_status") == "")
                    ).height,
                ],
            }
        )
