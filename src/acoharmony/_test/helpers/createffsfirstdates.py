#!/usr/bin/env python3
"""
Extract first date of service for each beneficiary from CCLF5/provider_list match.
This creates a simplified table that can be used by the consolidated alignment.
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import sys
from datetime import datetime
from pathlib import Path

import polars as pl


def create_ffs_first_dates():
    """
    Create a table of first FFS dates for each beneficiary.
    Saves to processed/ffs_first_dates.parquet
    """
    print("=" * 80)
    print("FFS FIRST DATES EXTRACTION")
    print("=" * 80)
    print(f"Start time: {datetime.now()}")

    processed_path = Path("/home/care/acoharmony/workspace/processed")

    # Input files
    cclf5_path = processed_path / "cclf5.parquet"
    provider_path = processed_path / "provider_list.parquet"

    # Output file
    output_path = processed_path / "ffs_first_dates.parquet"

    print("\n1. CHECKING INPUT FILES:")
    if not cclf5_path.exists():
        print(f"  [ERROR] CCLF5 not found: {cclf5_path}")
        return False
    print(f"  [OK] CCLF5 found: {cclf5_path}")

    if not provider_path.exists():
        print(f"  [ERROR] Provider list not found: {provider_path}")
        return False
    print(f"  [OK] Provider list found: {provider_path}")

    print("\n2. CREATING LAZY PIPELINE:")

    # Scan files lazily
    cclf5 = pl.scan_parquet(str(cclf5_path))
    provider_list = pl.scan_parquet(str(provider_path))

    # Get unique provider TINs
    print("  - Extracting unique provider TINs...")
    valid_tins = provider_list.select("billing_tin").unique()

    # Create the pipeline - completely lazy
    print("  - Building join and aggregation pipeline...")
    ffs_first_dates = (
        cclf5
        # Join with valid provider TINs
        .join(valid_tins, left_on="clm_rndrg_prvdr_tax_num", right_on="billing_tin", how="inner")
        # Group by beneficiary and get first date
        .group_by("bene_mbi_id")
        .agg([pl.min("clm_line_from_dt").alias("ffs_first_date"), pl.len().alias("claim_count")])
        # Add some metadata
        .with_columns(
            [pl.col("bene_mbi_id").alias("bene_mbi"), pl.lit(datetime.now()).alias("extracted_at")]
        )
        # Select final columns
        .select(["bene_mbi", "ffs_first_date", "claim_count", "extracted_at"])
    )

    print("  [OK] Pipeline created (fully lazy)")

    # Get schema without collecting
    print("\n3. VALIDATING SCHEMA:")
    try:
        schema = ffs_first_dates.collect_schema()
        print(f"  [OK] Schema valid with {len(schema)} columns:")
        for name, dtype in schema.items():
            print(f"    - {name}: {dtype}")
    except Exception as e:
        print(f"  [ERROR] Schema validation failed: {e}")
        return False

    print("\n4. EXECUTING PIPELINE:")
    print("  Writing to:", output_path)
    print("  This will process ~12M CCLF5 records...")
    print("  Please wait...")

    start_time = datetime.now()

    try:
        # THE ONLY COLLECTION POINT - writes directly to disk
        ffs_first_dates.sink_parquet(str(output_path))

        elapsed = datetime.now() - start_time
        print(f"  [OK] Write completed in {elapsed.total_seconds():.1f} seconds")

    except Exception as e:
        print(f"  [ERROR] Write failed: {e}")
        import traceback

        traceback.print_exc()
        return False

    print("\n5. VERIFICATION:")
    if output_path.exists():
        file_size = output_path.stat().st_size / (1024 * 1024)
        print(f"  [OK] File size: {file_size:.2f} MB")

        # Quick verification
        verify_df = pl.scan_parquet(str(output_path))
        stats = verify_df.select(
            [
                pl.len().alias("total_beneficiaries"),
                pl.col("ffs_first_date").min().alias("earliest_date"),
                pl.col("ffs_first_date").max().alias("latest_date"),
                pl.col("claim_count").mean().alias("avg_claims_per_bene"),
            ]
        ).collect()

        print(f"  [OK] Total beneficiaries with FFS: {stats['total_beneficiaries'][0]:,}")
        print(f"  [OK] Date range: {stats['earliest_date'][0]} to {stats['latest_date'][0]}")
        print(f"  [OK] Average claims per beneficiary: {stats['avg_claims_per_bene'][0]:.1f}")

        # Show sample
        print("\n  Sample data (first 5 rows):")
        sample = pl.scan_parquet(str(output_path)).head(5).collect()
        print(sample)

        print("\n" + "=" * 80)
        print("[SUCCESS] FFS FIRST DATES EXTRACTION COMPLETED")
        print("=" * 80)
        print(f"Output saved to: {output_path}")
        print(f"End time: {datetime.now()}")

        return True
    else:
        print("  [ERROR] Output file was not created")
        return False


if __name__ == "__main__":

    success = create_ffs_first_dates()
    sys.exit(0 if success else 1)
