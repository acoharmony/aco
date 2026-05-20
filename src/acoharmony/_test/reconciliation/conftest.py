# © 2025 HarmonyCares
# All rights reserved.

"""Shared fixtures for reconciliation tests.

All reconciliation tests depend on real data at /opt/s3/data/workspace/.
They are skipped automatically when data is unavailable.
"""

import re
from pathlib import Path

import polars as pl
import pytest

WORKSPACE = Path("/opt/s3/data/workspace")
BRONZE = WORKSPACE / "bronze"
SILVER = WORKSPACE / "silver"
GOLD = WORKSPACE / "gold"

requires_data = pytest.mark.skipif(
    not SILVER.exists(), reason="Reconciliation data not available"
)


@pytest.fixture
def bronze_path():
    return BRONZE


@pytest.fixture
def silver_path():
    return SILVER


@pytest.fixture
def gold_path():
    return GOLD


def scan_silver(table_name: str) -> pl.LazyFrame:
    """Scan a silver-tier parquet file."""
    path = SILVER / f"{table_name}.parquet"
    if not path.exists():
        pytest.skip(f"{table_name}.parquet not found in silver")
    return pl.scan_parquet(path)


def scan_gold(table_name: str) -> pl.LazyFrame:
    """Scan a gold-tier parquet file."""
    path = GOLD / f"{table_name}.parquet"
    if not path.exists():
        pytest.skip(f"{table_name}.parquet not found in gold")
    return pl.scan_parquet(path)


def get_cclf0_deliveries() -> pl.DataFrame:
    """Get all CCLF0 deliveries with parsed metadata."""
    df = scan_silver("cclf0").collect()
    deliveries = df.select("source_filename", "file_date").unique().sort("file_date")
    result = []
    for row in deliveries.iter_rows(named=True):
        fn = row["source_filename"]
        m = re.match(r"P\.(A\d+|D\d+)\.ACO\.ZC0(\w)Y?(\d+)\.D(\d{6})\.T", fn)
        if m:
            aco, ftype, year, delivery = m.groups()
            type_map = {"W": "weekly", "Y": "current", "R": "runout"}
            result.append({
                "source_filename": fn,
                "file_date": row["file_date"],
                "aco_id": aco,
                "program": "MSSP" if aco.startswith("A") else "REACH",
                "delivery_type": type_map.get(ftype, ftype),
                "performance_year": f"20{year}",
                "delivery_date": delivery,
            })
    return pl.DataFrame(result)


CCLF_FILE_MAP = {
    "CCLF1": "cclf1",
    "CCLF2": "cclf2",
    "CCLF3": "cclf3",
    "CCLF4": "cclf4",
    "CCLF5": "cclf5",
    "CCLF6": "cclf6",
    "CCLF7": "cclf7",
    "CCLF8": "cclf8",
    "CCLF9": "cclf9",
    "CCLFA": "cclfa",
    "CCLFB": "cclfb",
}
