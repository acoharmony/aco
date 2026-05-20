#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""
CLI commands for CMS Public Use Files (PUF) management.

Provides commands for:
- Inventory discovery and management
- File downloads via _cite integration
- Status monitoring
- Comparison of inventory to downloaded files
- ZIP extraction with metadata-aware renaming
"""

from datetime import datetime
from pathlib import Path

from . import pfs_inventory
from .models import FileCategory, RuleType
from .puf_state import PUFStateTracker
from .puf_unpack import unpack_puf_zips


def cmd_inventory(args):
    """
    Manage PUF file inventory.

    This command syncs the state tracker with the current dataset inventory
    and shows what files are available.

    Args:
        args: Command-line arguments from argparse
    """
    print("=" * 80)
    print("PUF Inventory Management")
    print("=" * 80)
    print()

    # Get dataset key (default to 'pfs' for backward compatibility)
    dataset_key = getattr(args, "dataset", "pfs")

    # Load state tracker for the dataset
    tracker = PUFStateTracker.load()

    # Get filters from args
    year = getattr(args, "year", None)
    rule_type = getattr(args, "rule_type", None)
    force_refresh = getattr(args, "force", False)

    # Convert rule_type string to enum if needed
    if rule_type:
        try:
            rule_type = RuleType(rule_type)
        except ValueError:
            print(f"Invalid rule type: {rule_type}")
            print(f"Valid types: {', '.join(rt.value for rt in RuleType)}")
            return 1

    # Sync with inventory
    print(f"Syncing with {dataset_key.upper()} inventory...")
    if year:
        print(f"  Year filter: {year}")
    if rule_type:
        print(f"  Rule type filter: {rule_type.value}")
    print()

    new_count = tracker.sync_with_inventory(
        year=year, rule_type=rule_type, force_refresh=force_refresh, dataset_key=dataset_key
    )

    if new_count > 0:
        print(f"Added {new_count} new files to inventory")
    else:
        print("No new files added - inventory is up to date")

    print()

    # Scan filesystem to update download/extraction status
    print("Scanning filesystem for downloaded files...")
    scan_stats = tracker.scan_filesystem(verbose=False)
    tracker.save()

    print("Filesystem scan complete:")
    print(f"  Found in archive: {scan_stats['found_in_archive']}")
    print(f"  Found in bronze: {scan_stats['found_in_bronze']}")
    print(f"  Found in cite corpus: {scan_stats['found_in_cite']}")
    print(f"  Newly marked as downloaded: {scan_stats['marked_downloaded']}")
    print(f"  Newly marked as extracted: {scan_stats['marked_extracted']}")
    print()

    # Get summary
    summary = tracker.get_summary()

    print("=" * 80)
    print("Inventory Summary")
    print("=" * 80)
    print()
    print(f"Dataset: {summary['dataset_name']}")
    print(f"Last updated: {summary['last_updated']}")
    print()
    print(f"Total files:      {summary['total_files']}")
    print(f"Downloaded:       {summary['downloaded_files']} ({summary['download_percentage']:.1f}%)")
    print(f"Pending:          {summary['pending_files']}")
    print(f"Failed:           {summary['failed_files']}")
    print()

    # Show extraction statistics
    extracted_count = sum(1 for f in tracker.state.files.values() if f.extracted)
    extracted_pct = (extracted_count / summary['total_files'] * 100) if summary['total_files'] > 0 else 0
    print("Extraction Status:")
    print(f"  Extracted: {extracted_count} ({extracted_pct:.1f}%)")
    print()

    # Show file locations
    in_archive = sum(1 for f in tracker.state.files.values() if f.found_in_archive)
    in_bronze = sum(1 for f in tracker.state.files.values() if f.found_in_bronze)
    in_cite = sum(1 for f in tracker.state.files.values() if f.found_in_cite_corpus)
    print("File Locations:")
    print(f"  Archive:      {in_archive}")
    print(f"  Bronze:       {in_bronze}")
    print(f"  Cite corpus:  {in_cite}")
    print()

    # Breakdown by year
    if tracker.state.files:
        years = {}
        for file_entry in tracker.state.files.values():
            year_key = file_entry.year
            if year_key not in years:
                years[year_key] = {"total": 0, "downloaded": 0, "pending": 0}
            years[year_key]["total"] += 1
            if file_entry.downloaded:
                years[year_key]["downloaded"] += 1
            else:
                years[year_key]["pending"] += 1

        print("Files by Year:")
        for year_key in sorted(years.keys()):
            stats = years[year_key]
            print(
                f"  {year_key}: {stats['total']:>3} total "
                f"({stats['downloaded']:>3} downloaded, {stats['pending']:>3} pending)"
            )
        print()

        # Breakdown by category
        categories = {}
        for file_entry in tracker.state.files.values():
            cat = file_entry.category
            if cat not in categories:
                categories[cat] = {"total": 0, "downloaded": 0, "pending": 0}
            categories[cat]["total"] += 1
            if file_entry.downloaded:
                categories[cat]["downloaded"] += 1
            else:
                categories[cat]["pending"] += 1

        print("Files by Category:")
        for cat in sorted(categories.keys()):
            stats = categories[cat]
            print(
                f"  {cat:30s}: {stats['total']:>3} total "
                f"({stats['downloaded']:>3} downloaded, {stats['pending']:>3} pending)"
            )
        print()

    print(f"State file: {tracker.get_state_path()}")
    print()

    return 0


def cmd_need_download(args):
    """
    Show PUF files that need downloading.

    Compares the inventory against downloaded files and shows what's missing.

    Args:
        args: Command-line arguments from argparse
    """
    print("=" * 80)
    print("PUF Files Needing Download")
    print("=" * 80)
    print()

    # Get dataset key
    dataset_key = getattr(args, "dataset", "pfs")

    # Load state tracker
    tracker = PUFStateTracker.load()

    # Sync with inventory first to ensure we have the dataset files
    print(f"Syncing with {dataset_key.upper()} inventory...")
    tracker.sync_with_inventory(dataset_key=dataset_key)
    print()

    # Scan filesystem first to ensure we have up-to-date status
    print("Scanning filesystem for existing files...")
    scan_stats = tracker.scan_filesystem(verbose=False)
    tracker.save()

    print(f"  Files in archive: {scan_stats['found_in_archive']}")
    print(f"  Files in bronze: {scan_stats['found_in_bronze']}")
    print(f"  Files in cite corpus: {scan_stats['found_in_cite']}")
    print()

    # Get filters from args
    year = getattr(args, "year", None)
    rule_type = getattr(args, "rule_type", None)
    category = getattr(args, "category", None)
    schema_name = getattr(args, "schema", None)
    limit = getattr(args, "limit", 20)

    # Convert rule_type string to enum if needed
    if rule_type:
        try:
            rule_type = RuleType(rule_type)
        except ValueError:
            print(f"Invalid rule type: {rule_type}")
            print(f"Valid types: {', '.join(rt.value for rt in RuleType)}")
            return 1

    # Convert category string to enum if needed
    if category:
        try:
            category = FileCategory(category)
        except ValueError:
            print(f"Invalid category: {category}")
            print("Run 'aco puf categories' to see valid categories")
            return 1

    # Show filters
    filters_applied = []
    if year:
        filters_applied.append(f"year={year}")
    if rule_type:
        filters_applied.append(f"rule_type={rule_type.value}")
    if category:
        filters_applied.append(f"category={category.value}")
    if schema_name:
        filters_applied.append(f"schema={schema_name}")

    if filters_applied:
        print(f"Filters: {', '.join(filters_applied)}")
        print()

    # Get needed downloads
    needed_tasks = tracker.get_needed_downloads(
        year=year, rule_type=rule_type, category=category, schema_name=schema_name
    )

    if not needed_tasks:
        print("No files need downloading!")
        print()
        if filters_applied:
            print("Try removing some filters or run 'aco puf inventory' to sync.")
        return 0

    # Summary
    print(f"Found {len(needed_tasks)} files that need downloading")
    print()

    # Breakdown by year
    by_year = {}
    for task in needed_tasks:
        if task.year not in by_year:
            by_year[task.year] = 0
        by_year[task.year] += 1

    print("Needed Downloads by Year:")
    for year_key in sorted(by_year.keys()):
        print(f"  {year_key}: {by_year[year_key]:>3} files")
    print()

    # Breakdown by category
    by_category = {}
    for task in needed_tasks:
        cat = (
            task.file_metadata.category.value
            if hasattr(task.file_metadata.category, "value")
            else str(task.file_metadata.category)
        )
        if cat not in by_category:
            by_category[cat] = 0
        by_category[cat] += 1

    print("Needed Downloads by Category:")
    for cat in sorted(by_category.keys()):
        print(f"  {cat:30s}: {by_category[cat]:>3} files")
    print()

    # Show sample of needed files
    print(f"Sample Files (showing {min(limit, len(needed_tasks))} of {len(needed_tasks)}):")
    print("-" * 80)
    for task in needed_tasks[:limit]:
        file_meta = task.file_metadata
        rule_type_str = task.rule_type.value if hasattr(task.rule_type, "value") else str(task.rule_type)
        category_str = (
            file_meta.category.value if hasattr(file_meta.category, "value") else str(file_meta.category)
        )
        print(f"{task.year} {rule_type_str}: {file_meta.key}")
        print(f"  Category: {category_str}")
        print(f"  URL: {file_meta.url}")
        if file_meta.schema_mapping:
            print(f"  Schema: {file_meta.schema_mapping}")
    print()

    # Save needed downloads list for download command
    from .puf_state import PUFFileEntry

    needed_entries = []
    for task in needed_tasks:
        entry = PUFFileEntry.from_download_task(task, downloaded=False)
        needed_entries.append(entry.to_dict())

    workspace = Path("/opt/s3/data/workspace")
    state_dir = workspace / "logs" / "tracking"
    state_dir.mkdir(parents=True, exist_ok=True)
    state_path = state_dir / "puf_notdownloaded_state.json"

    import json

    with open(state_path, "w") as f:
        json.dump(
            {
                "generated_at": datetime.now().isoformat(),
                "total_missing": len(needed_entries),
                "files": needed_entries,
            },
            f,
            indent=2,
        )

    print(f"Saved {len(needed_entries)} files to: {state_path}")
    print()
    print("To download these files, run:")
    print("  aco puf download")
    print()

    return 0


def cmd_download(args):
    """
    Download PUF files identified by the last 'need-download' command.

    By default, downloads the files saved from the last 'aco puf need-download' run.

    Args:
        args: Command-line arguments from argparse
    """
    print("=" * 80)
    print("PUF File Download")
    print("=" * 80)
    print()

    # Check for saved missing files list from need-download
    workspace = Path("/opt/s3/data/workspace")
    state_path = workspace / "logs" / "tracking" / "puf_notdownloaded_state.json"

    if not state_path.exists():
        print("No saved download list found!")
        print(f"Expected file: {state_path}")
        print()
        print("Please run 'aco puf need-download' first to identify files to download.")
        return 1

    # Load the missing files list
    import json

    with open(state_path) as f:
        state_data = json.load(f)

    files_list = state_data.get("files", [])

    if not files_list:
        print("The saved download list is empty!")
        print("Run 'aco puf need-download' first to check for files.")
        return 0

    # Show summary
    print(f"Saved on: {state_data.get('generated_at', 'unknown')}")
    print(f"Total files to download: {state_data.get('total_missing', len(files_list))}")
    print()

    # Reconstruct download tasks
    from .puf_state import PUFFileEntry

    needed_files = []
    for entry_data in files_list:
        needed_files.append(PUFFileEntry.from_dict(entry_data))

    # Get limit from args
    limit = getattr(args, "limit", None)
    if limit:
        print(f"Limit: downloading first {limit} files only")
        needed_files = needed_files[:limit]
        print()

    # Initialize tracker
    tracker = PUFStateTracker.load()

    # Import _cite transform
    from .._transforms._cite import transform_cite

    # Track statistics
    total_downloaded = 0
    total_errors = []

    print("=" * 80)
    print("Downloading Files")
    print("=" * 80)
    print()

    # Download each file
    for idx, file_entry in enumerate(needed_files, 1):
        print(f"[{idx}/{len(needed_files)}] {file_entry.year} {file_entry.rule_type}: {file_entry.file_key}")
        print(f"  URL: {file_entry.url}")

        try:
            # Step 1: Create citation via _cite
            result_lf = transform_cite(
                url=file_entry.url,
                force_refresh=False,
                note=f"{file_entry.year} {file_entry.rule_type} - {file_entry.file_key}",
                tags=[
                    "puf",
                    "cms",
                    file_entry.year,
                    file_entry.rule_type.lower(),
                    file_entry.category,
                ],
            )

            # Collect result to get corpus path
            result_df = result_lf.collect()
            corpus_path = None
            if len(result_df) > 0 and "corpus_path" in result_df.columns:
                corpus_path = str(result_df["corpus_path"][0])

            # Step 2: Actually download the ZIP file to archive
            from urllib.parse import urlparse

            import requests

            # Create archive directory structure
            # For RVU/zipcarrier: archive/{year}/{dataset_key}/
            # For PFS: archive/{year}/{rule_type}/
            workspace = Path("/opt/s3/data/workspace")
            if file_entry.dataset_key in ("rvu", "zipcarrier"):
                archive_dir = workspace / "archive" / file_entry.year / file_entry.dataset_key
            else:
                archive_dir = workspace / "archive" / file_entry.year / file_entry.rule_type.lower().replace(" ", "_")
            archive_dir.mkdir(parents=True, exist_ok=True)

            # Extract filename from URL or use file_key
            url_path = urlparse(file_entry.url).path
            filename = Path(url_path).name
            if not filename or not filename.lower().endswith('.zip'):
                # Fallback to file_key + .zip
                filename = f"{file_entry.file_key}.zip"

            # Download the ZIP file
            zip_path = archive_dir / filename
            response = requests.get(file_entry.url, timeout=60, allow_redirects=True)
            response.raise_for_status()

            with open(zip_path, 'wb') as f:
                f.write(response.content)

            file_size = zip_path.stat().st_size
            print(f"  [OK] Downloaded ZIP: {filename} ({file_size / (1024*1024):.2f} MB)")

            tracker.mark_downloaded(
                year=file_entry.year,
                rule_type=file_entry.rule_type,
                file_key=file_entry.file_key,
                corpus_path=corpus_path,
                file_size_bytes=file_size,
            )

            total_downloaded += 1

        except Exception as e:  # ALLOWED: Continues processing remaining files despite error
            error_msg = f"Error downloading {file_entry.file_key}: {e}"
            print(f"  [ERROR] {error_msg}")
            total_errors.append(error_msg)

            # Mark as failed
            tracker.mark_failed(
                year=file_entry.year,
                rule_type=file_entry.rule_type,
                file_key=file_entry.file_key,
                error_message=str(e),
            )

            # Continue to next file
            continue

        print()

    # Summary
    print("=" * 80)
    print("Download Summary")
    print("=" * 80)
    print(f"Files downloaded: {total_downloaded}")
    print(f"Errors:           {len(total_errors)}")
    print()

    if total_errors:
        print("Errors encountered:")
        for error in total_errors[:10]:
            print(f"  - {error}")
        if len(total_errors) > 10:
            print(f"  ... and {len(total_errors) - 10} more")
        print()

    # Suggest next steps
    if total_downloaded > 0:
        print("Next steps:")
        print("  1. Run 'aco puf inventory' to update inventory status")
        print("  2. Run 'aco puf need-download' to verify all files are downloaded")
        print()

    return 0 if not total_errors else 1


def cmd_list_years(args):
    """
    List all available years in the PFS inventory.

    Args:
        args: Command-line arguments from argparse
    """
    years = pfs_inventory.list_available_years()

    print("=" * 80)
    print("Available Years in PFS Inventory")
    print("=" * 80)
    print()
    print(f"Total years: {len(years)}")
    print()

    for year in years:
        year_inv = pfs_inventory.get_year(year)
        if year_inv:
            rule_types = list(year_inv.rules.keys())
            file_count = len(year_inv.get_all_files())
            print(f"  {year}: {file_count:>3} files ({', '.join(rule_types)})")

    print()
    return 0


def cmd_list_categories(args):
    """
    List all file categories.

    Args:
        args: Command-line arguments from argparse
    """
    print("=" * 80)
    print("PUF File Categories")
    print("=" * 80)
    print()

    # Group categories by type
    categories_by_type = {
        "RVU and Payment Files": [
            FileCategory.ADDENDA,
            FileCategory.PPRVU,
            FileCategory.PE_RVU,
            FileCategory.CONVERSION_FACTOR,
        ],
        "Geographic Files": [FileCategory.GPCI, FileCategory.GAF, FileCategory.LOCALITY],
        "Practice Expense Inputs": [
            FileCategory.DIRECT_PE_INPUTS,
            FileCategory.CLINICAL_LABOR,
            FileCategory.EQUIPMENT,
            FileCategory.SUPPLIES,
            FileCategory.PE_WORKSHEET,
            FileCategory.PE_SUMMARY,
            FileCategory.ALT_METHODOLOGY_PE,
            FileCategory.INDIRECT_COST_INDICES,
        ],
        "Physician Time and Work": [
            FileCategory.PHYSICIAN_TIME,
            FileCategory.PHYSICIAN_WORK,
            FileCategory.PEHR,
        ],
        "Malpractice": [FileCategory.MALPRACTICE, FileCategory.MALPRACTICE_OVERRIDE],
        "Crosswalks and Utilization": [
            FileCategory.ANALYTIC_CROSSWALK,
            FileCategory.UTILIZATION_CROSSWALK,
            FileCategory.CPT_CODES,
            FileCategory.PLACEHOLDER,
        ],
        "Policy Lists": [
            FileCategory.TELEHEALTH,
            FileCategory.DESIGNATED_CARE,
            FileCategory.INVASIVE_CARDIOLOGY,
            FileCategory.MPPR,
            FileCategory.OPPS_CAP,
            FileCategory.PHASE_IN,
        ],
        "Impact and Specialty": [
            FileCategory.IMPACT,
            FileCategory.SPECIALTY_ASSIGNMENT,
            FileCategory.SPECIALTY_IMPACTS,
        ],
        "E&M Specific": [FileCategory.EM_GUIDELINES, FileCategory.EM_CODES, FileCategory.EM_IMPACT],
        "CY 2026+ New Categories": [
            FileCategory.EFFICIENCY_ADJUSTMENT,
            FileCategory.PROCEDURE_SHARES,
            FileCategory.RADIATION_SERVICES,
            FileCategory.SKIN_SUBSTITUTE,
        ],
    }

    for category_type, categories in categories_by_type.items():
        print(f"{category_type}:")
        for cat in categories:
            print(f"  {cat.value}")
        print()

    return 0


def cmd_search(args):
    """
    Search for files by keyword.

    Args:
        args: Command-line arguments from argparse
    """
    search_term = args.search_term if hasattr(args, "search_term") else ""
    search_in = args.search_in if hasattr(args, "search_in") else "all"

    if not search_term:
        print("Error: search term required")
        return 1

    print("=" * 80)
    print(f"Searching for: '{search_term}'")
    print("=" * 80)
    print()

    results = pfs_inventory.search_files(search_term, search_in=search_in)

    if not results:
        print("No matching files found")
        return 0

    print(f"Found {len(results)} matching files:")
    print()

    for year, rule_type, file_key, file_meta in results:
        category_str = (
            file_meta.category.value if hasattr(file_meta.category, "value") else str(file_meta.category)
        )
        print(f"{year} {rule_type}: {file_key}")
        print(f"  Category: {category_str}")
        print(f"  URL: {file_meta.url}")
        if file_meta.description:
            print(f"  Description: {file_meta.description}")
        if file_meta.schema_mapping:
            print(f"  Schema: {file_meta.schema_mapping}")
        print()

    return 0


def cmd_unpack(args):
    """
    Unpack PUF ZIP files with metadata-aware renaming.

    Args:
        args: Command-line arguments from argparse
    """
    print("=" * 80)
    print("PUF ZIP Extraction")
    print("=" * 80)
    print()

    # Get filters from args
    year = getattr(args, "year", None)
    rule_type = getattr(args, "rule_type", None)
    category = getattr(args, "category", None)
    dry_run = getattr(args, "dry_run", False)

    if dry_run:
        print("[DRY RUN MODE - No files will be extracted]")
        print()

    # Show filters
    filters_applied = []
    if year:
        filters_applied.append(f"year={year}")
    if rule_type:
        filters_applied.append(f"rule_type={rule_type}")
    if category:
        filters_applied.append(f"category={category}")

    if filters_applied:
        print(f"Filters: {', '.join(filters_applied)}")
        print()

    # Unpack ZIPs
    stats = unpack_puf_zips(
        year=year, rule_type=rule_type, category=category, dry_run=dry_run, verbose=True
    )

    return 0 if stats["failed"] == 0 else 1
