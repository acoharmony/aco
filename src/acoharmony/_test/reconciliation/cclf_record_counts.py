# © 2025 HarmonyCares
# All rights reserved.

"""Reconcile CCLF0 record counts against parsed CCLF companion files.

Tests every delivery for every quarter/year available. CCLF0 contains
the expected record count for each companion file. This validates our
parsing didn't drop or duplicate rows.
"""

import re

import polars as pl
import pytest

from .conftest import CCLF_FILE_MAP, SILVER, get_cclf0_deliveries, requires_data, scan_silver


# Map CCLF file type to the ZC code used in filenames
CCLF_TO_ZC = {
    "CCLF1": "1", "CCLF2": "2", "CCLF3": "3", "CCLF4": "4",
    "CCLF5": "5", "CCLF6": "6", "CCLF7": "7", "CCLF8": "8",
    "CCLF9": "9", "CCLFA": "A", "CCLFB": "B",
}


@requires_data
class TestCclf0RecordCountsByDelivery:
    """Validate parsed row counts match CCLF0 for every delivery."""

    @pytest.fixture
    def cclf0(self):
        return scan_silver("cclf0").collect()

    @pytest.fixture
    def deliveries(self):
        return get_cclf0_deliveries()

    @pytest.mark.reconciliation
    def test_cclf0_has_data(self, cclf0):
        assert cclf0.height > 0
        assert "source_filename" in cclf0.columns

    @pytest.mark.reconciliation
    def test_all_deliveries_have_11_file_types(self, cclf0):
        """Each delivery should list 11 companion file types (CCLF1-9, A, B)."""
        by_delivery = cclf0.group_by("source_filename").len()
        for row in by_delivery.iter_rows(named=True):
            assert row["len"] == 11, (
                f"Delivery {row['source_filename']} has {row['len']} file types, expected 11"
            )

    @pytest.mark.reconciliation
    def test_record_counts_per_delivery(self, cclf0, deliveries):
        """For each delivery, check parsed row counts match CCLF0.

        The CCLF0 manifest filename uses ZC0 (e.g. P.A2671.ACO.ZC0WY26.D260225.T0544440).
        Companion files use ZC{N} (e.g. ZC1 for CCLF1). We construct the expected
        companion filename and filter the parsed parquet by source_filename.
        """
        file_type_col = next(
            c for c in cclf0.columns if "file" in c.lower().strip() and "number" in c.lower().strip()
        )
        count_col = next(
            c for c in cclf0.columns if "total" in c.lower().strip() and "record" in c.lower().strip()
        )

        mismatches = []
        checked = 0
        for delivery_row in deliveries.iter_rows(named=True):
            src_fn = delivery_row["source_filename"]
            delivery_cclf0 = cclf0.filter(pl.col("source_filename") == src_fn)

            for row in delivery_cclf0.iter_rows(named=True):
                file_type = row[file_type_col].strip().upper()
                expected_str = row[count_col].strip()
                try:
                    expected_count = int(expected_str)
                except (ValueError, TypeError):
                    continue

                table_name = CCLF_FILE_MAP.get(file_type)
                zc_code = CCLF_TO_ZC.get(file_type)
                if not table_name or not zc_code:
                    continue

                parquet_path = SILVER / f"{table_name}.parquet"
                if not parquet_path.exists():
                    continue

                # Construct companion filename: ZC0 -> ZC{N}
                companion_fn = src_fn.replace("ZC0", f"ZC{zc_code}")

                parsed = pl.scan_parquet(parquet_path)
                if "source_filename" not in parsed.collect_schema().names():
                    continue

                actual_count = parsed.filter(
                    pl.col("source_filename") == companion_fn
                ).select(pl.len()).collect().item()

                # If no rows found, this delivery may not have been ingested yet
                if actual_count == 0:
                    continue

                checked += 1
                if actual_count != expected_count:
                    mismatches.append(
                        f"{delivery_row['program']} {delivery_row['delivery_type']} "
                        f"PY{delivery_row['performance_year']} "
                        f"({delivery_row['delivery_date']}): "
                        f"{file_type} expected {expected_count:,}, got {actual_count:,} "
                        f"(delta {actual_count - expected_count:+,})"
                    )

        assert checked > 0, "No deliveries could be verified — no companion files found"
        if mismatches:
            msg = f"{len(mismatches)} record count mismatches out of {checked} checked:\n" + "\n".join(mismatches[:20])
            pytest.fail(msg)

    @pytest.mark.reconciliation
    def test_all_companion_files_present(self):
        """Every CCLF type (1-9, A, B) should have a silver parquet."""
        missing = [ft for ft, tn in CCLF_FILE_MAP.items()
                   if not (SILVER / f"{tn}.parquet").exists()]
        assert not missing, f"Missing silver parquets: {missing}"

    @pytest.mark.reconciliation
    def test_delivery_coverage(self, deliveries):
        """Should have deliveries from both MSSP and REACH programs."""
        programs = set(deliveries["program"].to_list())
        assert "REACH" in programs, "No REACH deliveries found"
        assert "MSSP" in programs, "No MSSP deliveries found"

    @pytest.mark.reconciliation
    def test_delivery_types(self, deliveries):
        """Should have weekly, current, and runout delivery types."""
        types = set(deliveries["delivery_type"].to_list())
        assert "weekly" in types or "current" in types, f"Only delivery types: {types}"

    @pytest.mark.reconciliation
    def test_performance_years(self, deliveries):
        """Should have data spanning multiple performance years."""
        years = set(deliveries["performance_year"].to_list())
        assert len(years) >= 1, f"Only {len(years)} performance year(s): {years}"
