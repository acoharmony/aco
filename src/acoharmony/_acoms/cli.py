# © 2025 HarmonyCares
# All rights reserved.

"""CLI commands for ACOMS DataHub inventory and downloads."""

from __future__ import annotations

import argparse
import json
import sys
import traceback
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

from .._log import LogWriter
from .client import Acoms
from .comparison import (
    compare_inventory,
    export_to_csv,
    format_size,
    save_not_downloaded_state,
    scan_directory,
)
from .config import AcomsConfig, get_current_year
from .inventory import FileInventoryEntry, InventoryDiscovery, InventoryResult
from .models import DateFilter, normalize_category
from .state import AcomsStateTracker


def add_acoms_subparsers(subparsers) -> argparse.ArgumentParser:
    """Register the top-level ``aco acoms`` parser."""
    acoms_parser = subparsers.add_parser("acoms", help="ACOMS DataHub file management")
    acoms_subparsers = acoms_parser.add_subparsers(
        dest="acoms_command",
        help="ACOMS commands",
    )
    _add_acoms_commands(acoms_subparsers)
    return acoms_parser


def _add_acoms_commands(acoms_subparsers) -> None:
    """Add ACOMS inventory/download subcommands to an argparse subparser group."""
    inventory_parser = acoms_subparsers.add_parser(
        "inventory",
        help="Discover and manage ACOMS DataHub file inventory",
    )
    inventory_parser.add_argument(
        "--force",
        action="store_true",
        help="Force rebuild entire inventory from scratch",
    )
    inventory_parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="Starting year for scan (default: 2022)",
    )
    inventory_parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Ending year for scan (default: current year)",
    )

    need_download_parser = acoms_subparsers.add_parser(
        "need-download",
        help="Query remote ACOMS DataHub and show files not downloaded locally",
    )
    need_download_parser.add_argument(
        "--start-year",
        type=int,
        default=None,
        help="Start year for remote query (default: 2022)",
    )
    need_download_parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="End year for remote query (default: current year)",
    )
    need_download_parser.add_argument(
        "--year",
        type=int,
        help="Filter by specific year for display/save",
    )
    need_download_parser.add_argument(
        "--category",
        help=(
            "Filter by category "
            "(CCLF, Reports, Monthly Exclusion Files, Shadow Bundles Data Files, "
            "PC Flex Reports)"
        ),
    )
    need_download_parser.add_argument(
        "--export",
        type=str,
        metavar="FILE",
        help="Export missing files to CSV",
    )
    need_download_parser.add_argument(
        "--show-have",
        action="store_true",
        help="Also show files we already have in bronze/archive",
    )
    need_download_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Limit number of missing files to display (default: 20)",
    )

    download_parser = acoms_subparsers.add_parser(
        "download",
        help="Download files from last need-download run",
    )
    download_parser.add_argument(
        "--updated-after",
        type=str,
        metavar="YYYY-MM-DD",
        help="Optional date filter: only download files updated after this date",
    )
    download_parser.add_argument("--year", type=int, help="Download only a specific year")
    download_parser.add_argument("--category", help="Download only a specific category")

    acoms_subparsers.add_parser("list", help="List ACOMS DataHub folders and file types")


def _default_start_year(args) -> int:
    return args.start_year if getattr(args, "start_year", None) else 2022


def _default_end_year(args) -> int:
    return args.end_year if getattr(args, "end_year", None) else get_current_year()


def _filter_category(category: str | None) -> str | None:
    if not category:
        return None
    return normalize_category(category).value


def _print_inventory_summary(result: InventoryResult) -> None:
    print()
    print("=" * 80)
    print("ACOMS Inventory Summary")
    print("=" * 80)
    print()
    print(f"Total files: {result.total_files}")
    if result.years:
        print(f"Years: {min(result.years)}-{max(result.years)}")
    else:
        print("Years: n/a")
    if result.duration_seconds:
        print(f"Duration: {result.duration_seconds:.1f} seconds")
    else:
        print("Duration: N/A")
    print()

    print("Files by Year:")
    for year in sorted(result.files_by_year.keys(), key=int):
        print(f"  {year}: {result.files_by_year[year]:>3} files")
    print()

    print("Files by Category:")
    for category in sorted(result.files_by_category.keys()):
        print(f"  {category:30s}: {result.files_by_category[category]:>3} files")
    print()

    files_with_codes = sum(1 for file_entry in result.files if file_entry.file_type_code)
    print(f"Files with type codes: {files_with_codes} / {result.total_files}")
    print()

    if result.errors:
        print(f"Errors: {len(result.errors)}")
        for error in result.errors[:5]:
            print(f"  - {error}")
        if len(result.errors) > 5:
            print(f"  ... and {len(result.errors) - 5} more")
    else:
        print("No errors encountered.")
    print()


def cmd_inventory(args) -> int:
    """Discover ACOMS DataHub inventory and save it to tracking state."""
    config = AcomsConfig.from_profile()
    log_writer = LogWriter(name="acoms-cli")
    discovery = InventoryDiscovery(config=config, log_writer=log_writer)
    inventory_path = discovery.get_inventory_path()

    start_year = _default_start_year(args)
    end_year = _default_end_year(args)
    force = bool(getattr(args, "force", False))

    if inventory_path.exists() and not force:
        print(f"Inventory file exists: {inventory_path}")
        print("Loading existing ACOMS inventory...")
        try:
            result = InventoryResult.load_from_json(inventory_path)
        except Exception as e:
            print(f"Error loading inventory: {e}")
            print("Use --force to rebuild inventory from scratch.")
            return 1

        print(f"Loaded inventory with {result.total_files} files")
        print(f"Last scan: {result.completed_at}")
        print()

        existing_years = set(result.years)
        requested_years = set(range(start_year, end_year + 1))
        new_years = requested_years - existing_years

        if new_years:
            print(f"Scanning new years: {sorted(new_years)}")
            for year in sorted(new_years):
                year_result = discovery.discover_years(
                    aco_id=result.aco_id,
                    start_year=year,
                    end_year=year,
                )
                year_result = discovery.enrich_with_file_type_codes(year_result)
                result.files.extend(year_result.files)
                result.years.extend(year_result.years)
                result.total_files = len(result.files)
                for file_entry in year_result.files:
                    result.files_by_year[file_entry.year] = (
                        result.files_by_year.get(file_entry.year, 0) + 1
                    )
                    result.files_by_category[file_entry.category] = (
                        result.files_by_category.get(file_entry.category, 0) + 1
                    )
            result.completed_at = datetime.now()
            result.save_to_json(inventory_path)
            print(f"Updated inventory saved to: {inventory_path}")
        else:
            print("All requested years already in inventory.")
            print("Use --force to rebuild entire inventory.")
    else:
        print(
            "Force flag set - rebuilding ACOMS inventory" if force else "Creating ACOMS inventory"
        )
        print()
        print(f"Scanning years {start_year}-{end_year}...")
        print("This may take several minutes due to API rate limiting.")
        print()

        try:
            aco_id = config.resolve_aco_id()
        except ValueError as e:
            print(f"Error: {e}")
            return 1

        print(f"Using ACO ID: {aco_id}")
        print()

        result = discovery.discover_years(
            aco_id=aco_id,
            start_year=start_year,
            end_year=end_year,
        )
        print()
        print("Enriching ACOMS inventory with file type codes...")
        result = discovery.enrich_with_file_type_codes(result)
        result.save_to_json(inventory_path)
        print()
        print(f"Inventory saved to: {inventory_path}")

    _print_inventory_summary(result)
    return 0


def cmd_need_download(args) -> int:
    """Show ACOMS files available remotely but absent from local storage."""
    config = AcomsConfig.from_profile()
    log_writer = LogWriter(name="acoms")
    discovery = InventoryDiscovery(config=config, log_writer=log_writer)

    inventory_path = discovery.get_inventory_path()
    if not inventory_path.exists():
        print("Error: ACOMS inventory file not found.")
        print("Run 'aco acoms inventory' to create the inventory first.")
        return 1

    start_year = _default_start_year(args)
    end_year = _default_end_year(args)

    try:
        aco_id = config.resolve_aco_id()
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    print("=" * 80)
    print("Querying Remote ACOMS DataHub")
    print("=" * 80)
    print(f"Checking years: {start_year} to {end_year}")
    print(f"Using ACO ID: {aco_id}")
    print()

    inventory = discovery.discover_years(
        aco_id=aco_id,
        start_year=start_year,
        end_year=end_year,
    )
    print()
    print("Enriching with ACOMS file type codes...")
    inventory = discovery.enrich_with_file_type_codes(inventory)
    print(
        f"Remote ACOMS inventory: {inventory.total_files} files across {len(inventory.years)} years"
    )
    print()

    print(f"Scanning bronze directory: {config.bronze_dir}")
    bronze_files = scan_directory(config.bronze_dir, "bronze directory", recursive=False)
    print(f"Found {len(bronze_files)} files in bronze directory")

    print(f"Scanning archive directory: {config.archive_dir}")
    archive_files = scan_directory(config.archive_dir, "archive directory", recursive=False)
    print(f"Found {len(archive_files)} files in archive directory")

    state_tracker = AcomsStateTracker(log_writer=log_writer)
    print(f"Loaded {len(state_tracker._file_cache)} hash-tracked ACOMS files from state")

    all_files = bronze_files | archive_files
    print(f"Total files in bronze + archive: {len(all_files)}")
    print()

    year_filter = getattr(args, "year", None)
    try:
        category_filter = _filter_category(getattr(args, "category", None))
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    filters_applied = []
    if year_filter:
        filters_applied.append(f"year={year_filter}")
    if category_filter:
        filters_applied.append(f"category={category_filter}")

    print("=" * 80)
    print("ACOMS Comparison Results")
    print("=" * 80)
    if filters_applied:
        print(f"Filters: {', '.join(filters_applied)}")
    print()

    results = compare_inventory(
        inventory,
        all_files,
        year_filter=year_filter,
        category_filter=category_filter,
        state_tracker=state_tracker,
    )

    print(f"Total in inventory:        {results['total_inventory']} files")
    if results["total_inventory"] > 0:
        have_pct = results["have_count"] / results["total_inventory"] * 100
        missing_pct = results["missing_count"] / results["total_inventory"] * 100
        print(f"Files in bronze + archive: {results['have_count']} files ({have_pct:.1f}%)")
        print(f"Missing:                   {results['missing_count']} files ({missing_pct:.1f}%)")
    else:
        print(f"Files in bronze + archive: {results['have_count']} files")
        print(f"Missing:                   {results['missing_count']} files")
    print(f"Total size missing:        {format_size(results['total_size_bytes'])}")
    print()

    if results["missing_by_year"]:
        print("Missing Files by Year:")
        for year in sorted(results["missing_by_year"].keys()):
            print(f"  {year}: {results['missing_by_year'][year]:>3} files")
        print()

    if results["missing_by_category"]:
        print("Missing Files by Category:")
        for category in sorted(results["missing_by_category"].keys()):
            print(f"  {category:30s}: {results['missing_by_category'][category]:>3} files")
        print()

    if results["missing_by_type_code"]:
        print("Missing Files by Type Code:")
        for type_code in sorted(results["missing_by_type_code"].keys()):
            print(f"  Code {type_code}: {results['missing_by_type_code'][type_code]:>3} files")
        print()

    limit = getattr(args, "limit", 20)
    if results["missing"]:
        print(
            "Sample Missing Files "
            f"(showing {min(limit, len(results['missing']))} of {len(results['missing'])}):"
        )
        print("-" * 80)
        for file_entry in results["missing"][:limit]:
            print(file_entry.filename)
            print(
                f"  Year: {file_entry.year}, Category: {file_entry.category}, "
                f"Type: {file_entry.file_type_code or 'unknown'}, "
                f"Size: {format_size(file_entry.size_bytes)}"
            )
            if file_entry.last_updated:
                print(f"  Last Updated: {file_entry.last_updated}")
        print()

    if getattr(args, "show_have", False) and results["have"]:
        print(
            "Sample Files in Bronze + Archive "
            f"(showing {min(limit, len(results['have']))} of {len(results['have'])}):"
        )
        print("-" * 80)
        for file_entry in results["have"][:limit]:
            print(file_entry.filename)
            print(
                f"  Year: {file_entry.year}, Category: {file_entry.category}, "
                f"Size: {format_size(file_entry.size_bytes)}"
            )
        print()

    if getattr(args, "export", None):
        export_path = Path(args.export)
        export_to_csv(results["missing"], export_path)
        print(f"Exported {len(results['missing'])} missing files to: {export_path}")
        print()

    state_path = config.log_dir / "tracking" / "acoms_notdownloaded_state.json"
    if results["missing"]:
        print("=" * 80)
        save_not_downloaded_state(results["missing"], state_path)
        print(f"Saved {len(results['missing'])} missing files to: {state_path}")
        print()
        print("To download these files, run:")
        print("  aco acoms download")
        print()

    if results["missing_count"] > 0:
        print("=" * 80)
        print("Recommendations")
        print("=" * 80)
        print(
            f"You are missing {results['missing_count']} files totaling "
            f"{format_size(results['total_size_bytes'])}"
        )
        print("To narrow the next run, use:")
        print("  aco acoms download --category <category> --year <year>")
        print()

    return 0


def _load_missing_state(state_path: Path) -> tuple[dict, list[FileInventoryEntry]]:
    with open(state_path) as f:
        state_data = json.load(f)

    files = [
        FileInventoryEntry(
            filename=entry["filename"],
            category=entry["category"],
            file_type_code=entry.get("file_type_code"),
            year=entry["year"],
            size_bytes=entry.get("size_bytes"),
            last_updated=entry.get("last_updated"),
            discovered_at=entry.get("discovered_at"),
        )
        for entry in state_data.get("files", [])
    ]
    return state_data, files


def _oldest_updated_after(files: list[FileInventoryEntry]) -> str | None:
    oldest = None
    for file_entry in files:
        if not file_entry.last_updated:
            continue
        try:
            parsed = datetime.fromisoformat(file_entry.last_updated.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            continue
        if oldest is None or parsed < oldest:
            oldest = parsed

    if oldest:
        return (oldest - timedelta(days=1)).strftime("%Y-%m-%d")
    return None


def cmd_download(args) -> int:
    """Download files identified by the last ``need-download`` run."""
    config = AcomsConfig.from_profile()
    log_writer = LogWriter(name="acoms-cli")
    state_path = config.log_dir / "tracking" / "acoms_notdownloaded_state.json"

    if not state_path.exists():
        print("=" * 80)
        print("No saved ACOMS missing files list found")
        print("=" * 80)
        print(f"Expected file: {state_path}")
        print("Run 'aco acoms need-download' first.")
        return 1

    print("=" * 80)
    print("Using files from last ACOMS need-download run")
    print("=" * 80)
    print(f"Loading missing files list from: {state_path}")
    print()

    state_data, missing_files = _load_missing_state(state_path)
    if not missing_files:
        print("The saved missing files list is empty.")
        return 0

    print(f"Saved on: {state_data.get('generated_at', 'unknown')}")
    print(f"Total missing files: {state_data.get('total_missing', len(missing_files))}")
    print(f"Total size: {state_data.get('total_size_formatted', 'unknown')}")
    print()

    year_filter = getattr(args, "year", None)
    try:
        category_filter = _filter_category(getattr(args, "category", None))
    except ValueError as e:
        print(f"Error: {e}")
        return 1

    if year_filter:
        missing_files = [
            file_entry for file_entry in missing_files if file_entry.year == year_filter
        ]
    if category_filter:
        missing_files = [
            file_entry for file_entry in missing_files if file_entry.category == category_filter
        ]

    state_tracker = AcomsStateTracker(log_writer=log_writer)
    truly_missing_files = []
    already_tracked = 0
    for file_entry in missing_files:
        if file_entry.filename in state_tracker._file_cache:
            already_tracked += 1
        else:
            truly_missing_files.append(file_entry)

    if already_tracked:
        print(f"Filtered out {already_tracked} files already in ACOMS state")
        print(f"Remaining to download: {len(truly_missing_files)} files")
        print()

    missing_files = truly_missing_files
    if not missing_files:
        print("No ACOMS downloads needed.")
        return 0

    downloads_by_group: dict[tuple[str, int, int | None], list[FileInventoryEntry]]
    downloads_by_group = defaultdict(list)
    for file_entry in missing_files:
        downloads_by_group[
            (file_entry.category, file_entry.year, file_entry.file_type_code)
        ].append(file_entry)

    bronze_dir = config.bronze_dir
    download_plan: dict[tuple[str, int, int | None], dict[str, object]] = {}
    for key, files in downloads_by_group.items():
        files_not_in_bronze = [
            file_entry for file_entry in files if not (bronze_dir / file_entry.filename).exists()
        ]
        if not files_not_in_bronze:
            continue

        download_plan[key] = {
            "files": files_not_in_bronze,
            "oldest_date": _oldest_updated_after(files_not_in_bronze),
            "file_count": len(files_not_in_bronze),
        }

    if not download_plan:
        print("All selected ACOMS files already exist in bronze.")
        return 0

    print("ACOMS download plan:")
    for (category, year, file_type_code), info in download_plan.items():
        suffix = f" (--updatedAfter={info['oldest_date']})" if info["oldest_date"] else ""
        type_label = file_type_code if file_type_code is not None else "category"
        print(f"  {category} / Year {year} / Type {type_label}: {info['file_count']} files{suffix}")
    print()

    client = Acoms(config=config, log_writer=log_writer)
    total_downloaded = 0
    total_errors: list[str] = []

    print("=" * 80)
    print("Downloading Missing ACOMS Files")
    print("=" * 80)
    print()

    for idx, ((category, year, file_type_code), info) in enumerate(download_plan.items(), 1):
        print(f"[{idx}/{len(download_plan)}] {category} / Year {year} / Type {file_type_code}")
        print(f"  Expected files: {info['file_count']}")

        date_filter = None
        effective_updated_after = info["oldest_date"] or getattr(args, "updated_after", None)
        if effective_updated_after:
            date_filter = DateFilter(updated_after=str(effective_updated_after))
            print(f"  Date filter: --updatedAfter={effective_updated_after}")

        try:
            category_enum = normalize_category(category)
            result = client.download(
                category=category_enum,
                year=year,
                file_type_code=file_type_code,
                date_filter=date_filter,
            )
        except Exception as e:
            error = f"Error downloading {category} year {year}: {e}"
            print(f"  [ERROR] {error}")
            total_errors.append(error)
            continue

        if result.success:
            total_downloaded += len(result.files_downloaded)
            print(f"  [OK] Downloaded {len(result.files_downloaded)} files")
        else:
            print(f"  [ERROR] Download failed: {', '.join(result.errors)}")
            total_errors.extend(result.errors)
        print()

    print("=" * 80)
    print("ACOMS Download Summary")
    print("=" * 80)
    print(f"Download requests:     {len(download_plan)}")
    print(f"Files downloaded:      {total_downloaded}")
    print(f"Expected missing files: {sum(info['file_count'] for info in download_plan.values())}")
    print(f"Errors:                {len(total_errors)}")
    print(f"Downloaded files are in: {config.bronze_dir}")
    print()

    if total_errors:
        print("Errors encountered:")
        for error in total_errors[:10]:
            print(f"  - {error}")
        if len(total_errors) > 10:
            print(f"  ... and {len(total_errors) - 10} more")
        print()

    if total_downloaded > 0:
        print("Next steps:")
        print("  1. Run 'aco acoms inventory --force' to refresh inventory")
        print("  2. Run 'aco acoms need-download' to verify all files are present")
        print()

    return 0 if not total_errors else 1


def cmd_list(args) -> int:
    """List ACOMS DataHub folders and file types."""
    del args
    config = AcomsConfig.from_profile()
    client = Acoms(
        config=config,
        log_writer=LogWriter(name="acoms-cli"),
        enable_duplicate_detection=False,
    )
    definitions = client.list_file_types()

    print("ACOMS DataHub Folders & File Types")
    print("=" * 80)
    by_category: dict[str, list[tuple[str, int]]] = defaultdict(list)
    for definition in definitions:
        by_category[definition.category].append((definition.name, definition.code))

    for category in sorted(by_category.keys()):
        print(category)
        for name, code in sorted(by_category[category], key=lambda item: item[1]):
            print(f"  Code {code}: {name}")
        print()

    return 0


def main() -> None:
    """Standalone CLI entry point for ACOMS."""
    from acoharmony import __version__

    parser = argparse.ArgumentParser(
        prog="aco-acoms",
        description="ACO Harmony - ACOMS DataHub File Management",
    )
    parser.add_argument("--version", action="version", version=f"ACO Harmony {__version__}")
    subparsers = parser.add_subparsers(dest="acoms_command", help="ACOMS commands")
    _add_acoms_commands(subparsers)

    args = parser.parse_args()
    if not args.acoms_command:
        parser.print_help()
        sys.exit(1)

    try:
        if args.acoms_command == "inventory":
            sys.exit(cmd_inventory(args))
        if args.acoms_command == "need-download":
            sys.exit(cmd_need_download(args))
        if args.acoms_command == "download":
            sys.exit(cmd_download(args))
        if args.acoms_command == "list":
            sys.exit(cmd_list(args))
        parser.print_help()
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
