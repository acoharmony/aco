#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""
CLI commands for 4icli DataHub integration.

Provides commands for:
- Inventory discovery and management
- File downloads
- Status monitoring
- Comparison of inventory to local files
"""

import os
import subprocess
import sys
from datetime import datetime
from getpass import getpass
from pathlib import Path

from .._log import LogWriter
from .client import FourICLI
from .comparison import (
    compare_inventory,
    export_to_csv,
    format_size,
    save_not_downloaded_state,
    scan_directory,
)
from .config import FourICLIConfig, get_current_year
from .inventory import InventoryDiscovery, InventoryResult
from .models import DataHubCategory


def _find_deploy_dir() -> Path:
    """Locate the repo's deploy/ directory.

    Walks up from this module to find a parent containing both deploy/.env and
    deploy/images/4icli/bootstrap.sh. Raises if not found — `aco 4icli setup`
    only runs from a repo checkout, not a wheel install.
    """
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "deploy"
        if (candidate / ".env").exists() and (
            candidate / "images" / "4icli" / "bootstrap.sh"
        ).exists():
            return candidate
    raise FileNotFoundError(
        "Could not locate deploy/ directory. `aco 4icli setup` must be run "
        "from a repo checkout containing deploy/.env and "
        "deploy/images/4icli/bootstrap.sh."
    )


def _read_env_file(env_path: Path) -> dict[str, str]:
    """Parse a flat KEY=VALUE .env file into a dict, ignoring blank/comment lines."""
    values: dict[str, str] = {}
    if not env_path.exists():
        return values
    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        values[key.strip()] = value.strip()
    return values


def _update_env_file(env_path: Path, updates: dict[str, str]) -> None:
    """Rewrite env_path with `updates` applied, preserving comments/order.

    Keys present in the file are updated in place; new keys are appended.
    """
    lines = env_path.read_text().splitlines() if env_path.exists() else []
    seen: set[str] = set()
    new_lines: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in updates:
                new_lines.append(f"{key}={updates[key]}")
                seen.add(key)
                continue
        new_lines.append(line)
    for key, value in updates.items():
        if key not in seen:
            new_lines.append(f"{key}={value}")
    env_path.write_text("\n".join(new_lines) + "\n")


def _mask(secret: str) -> str:
    """Render a secret as `abcd…wxyz` for safe display next to a prompt."""
    if not secret:
        return "(unset)"
    if len(secret) <= 8:
        return "…"
    return f"{secret[:4]}…{secret[-4:]}"


def cmd_setup(args):
    """Prompt for fresh portal-issued KEY/SECRET, persist to .env, run bootstrap.

    Operator-facing entry point used after 4Innovation rotates credentials in
    the portal. Writes the new pair to deploy/.env and then exec's
    deploy/images/4icli/bootstrap.sh, which:
      1. spins up a throwaway 4icli container,
      2. runs `4icli configure --key/--secret`,
      3. verifies via a real `datahub -v` call,
      4. on success, copies the resulting config.txt to $BRONZE/config.txt
         (the source of truth read by the runtime container's entrypoint).
    """
    deploy_dir = _find_deploy_dir()
    env_path = deploy_dir / ".env"
    bootstrap = deploy_dir / "images" / "4icli" / "bootstrap.sh"

    current = _read_env_file(env_path)
    current_key = current.get("FOURICLI_API_KEY", "")
    current_secret = current.get("FOURICLI_API_SECRET", "")
    current_apm = current.get("FOURICLI_APM_ID", "") or "D0259"

    print("=" * 72)
    print("4icli setup — refresh credentials after a 4Innovation portal rotation")
    print("=" * 72)
    print(f"Writing to: {env_path}")
    print("Press Enter at any prompt to keep the current value.")
    print()

    key_prompt = f"FOURICLI_API_KEY [current: {_mask(current_key)}]: "
    new_key = input(key_prompt).strip() or current_key
    if not new_key:
        print("[ERROR] No key provided and none in .env. Aborting.")
        return 1

    secret_prompt = f"FOURICLI_API_SECRET [current: {_mask(current_secret)}]: "
    new_secret = getpass(secret_prompt).strip() or current_secret
    if not new_secret:
        print("[ERROR] No secret provided and none in .env. Aborting.")
        return 1

    apm_prompt = f"FOURICLI_APM_ID [current: {current_apm}]: "
    new_apm = input(apm_prompt).strip() or current_apm

    if new_key == current_key and new_secret == current_secret:
        print()
        print("[WARN] Key and secret match what's already in .env.")
        print(
            "       If the runtime container is currently failing auth, the "
            "existing pair is likely revoked — bootstrap will reproduce the failure."
        )
        confirm = input("Continue anyway? [y/N]: ").strip().lower()
        if confirm != "y":
            print("Aborted.")
            return 1

    updates = {
        "FOURICLI_API_KEY": new_key,
        "FOURICLI_API_SECRET": new_secret,
        "FOURICLI_APM_ID": new_apm,
    }
    _update_env_file(env_path, updates)
    print()
    print(f"[OK] Updated {env_path}")
    print()
    print(f"Running {bootstrap} ...")
    print("-" * 72)
    sys.stdout.flush()

    env = os.environ.copy()
    env.update(updates)
    result = subprocess.run([str(bootstrap)], env=env, check=False)

    print("-" * 72)
    if result.returncode == 0:
        print("[OK] Bootstrap succeeded.")
        print(
            "Next: docker compose -f deploy/docker-compose.yml restart 4icli"
        )
    else:
        print(f"[ERROR] Bootstrap exited with code {result.returncode}.")
        print(
            "$BRONZE/config.txt was not modified — fix the credentials and "
            "re-run `aco 4icli setup`."
        )
    return result.returncode


def cmd_need_download(args):
    """
    Show files available in DataHub but not downloaded locally.

        Queries the remote DataHub in real-time using 4icli commands to see what files
        are available, then compares against local bronze and archive directories.

        Args:
            args: Command-line arguments from argparse
    """
    # Load configuration
    config = FourICLIConfig.from_profile()
    log_writer = LogWriter(name="4icli")
    discovery = InventoryDiscovery(log_writer=log_writer)

    # Check if inventory exists
    inventory_path = discovery.get_inventory_path()
    if not inventory_path.exists():
        print("Error: Inventory file not found.")
        print("Run 'aco 4icli inventory' to create the inventory first.")
        return 1

    # Determine years to check
    start_year = args.start_year if hasattr(args, "start_year") and args.start_year else 2022
    end_year = (
        args.end_year if hasattr(args, "end_year") and args.end_year else get_current_year()
    )

    # Query remote DataHub to get current inventory
    print("=" * 80)
    print("Querying Remote DataHub (this may take a few minutes)...")
    print("=" * 80)
    print(f"Checking years: {start_year} to {end_year}")
    print(f"Using APM ID: {config.default_apm_id}")
    print()

    inventory = discovery.discover_years(
        apm_id=config.default_apm_id,
        start_year=start_year,
        end_year=end_year,
    )

    # Enrich with file type codes from schemas
    print()
    print("Enriching with file type codes from schemas...")
    inventory = discovery.enrich_with_file_type_codes(inventory)

    print(
        f"Remote DataHub inventory: {inventory.total_files} files across {len(inventory.years)} years"
    )
    print()

    # Scan bronze directory (non-recursive - flat structure)
    # Bronze contains: downloaded files + extracted files (all flat)
    bronze_dir = config.bronze_dir
    print(f"Scanning bronze directory: {bronze_dir}")
    bronze_files = scan_directory(bronze_dir, "bronze directory", recursive=False)
    print(f"Found {len(bronze_files)} files in bronze directory")

    # Scan archive directory (non-recursive - flat structure)
    # Archive contains: ZIP files moved after extraction (all flat)
    archive_dir = config.data_path / "archive"
    print(f"Scanning archive directory: {archive_dir}")
    archive_files = scan_directory(archive_dir, "archive directory", recursive=False)
    print(f"Found {len(archive_files)} files in archive directory")

    # Load hash-aware state tracker for duplicate detection
    from .state import FourICLIStateTracker

    state_tracker = FourICLIStateTracker(log_writer=log_writer)
    print(f"Loaded {len(state_tracker._file_cache)} hash-tracked files from state")

    # Combine bronze + archive files
    all_files = bronze_files | archive_files
    print(f"Total files in bronze + archive: {len(all_files)}")
    print()

    # Compare
    print("=" * 80)
    print("Comparison Results")
    print("=" * 80)
    print()

    # Get filters
    year_filter = args.year if hasattr(args, "year") else None
    category_filter = args.category if hasattr(args, "category") else None

    filters_applied = []
    if year_filter:
        filters_applied.append(f"year={year_filter}")
    if category_filter:
        filters_applied.append(f"category={category_filter}")

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

    # Print summary
    print(f"Total in inventory:        {results['total_inventory']} files")

    if results['total_inventory'] > 0:
        print(
            f"Files in bronze + archive: {results['have_count']} files ({results['have_count'] / results['total_inventory'] * 100:.1f}%)"
        )
        print(
            f"Missing:                   {results['missing_count']} files ({results['missing_count'] / results['total_inventory'] * 100:.1f}%)"
        )
    else:
        print(f"Files in bronze + archive: {results['have_count']} files")
        print(f"Missing:                   {results['missing_count']} files")

    print(f"Total size missing:        {format_size(results['total_size_bytes'])}")
    print()

    # Breakdown by year
    if results["missing_by_year"]:
        print("Missing Files by Year:")
        for year in sorted(results["missing_by_year"].keys()):
            count = results["missing_by_year"][year]
            print(f"  {year}: {count:>3} files")
        print()

    # Breakdown by category
    if results["missing_by_category"]:
        print("Missing Files by Category:")
        for category in sorted(results["missing_by_category"].keys()):
            count = results["missing_by_category"][category]
            print(f"  {category:20s}: {count:>3} files")
        print()

    # Breakdown by type code
    if results["missing_by_type_code"]:
        print("Missing Files by Type Code:")
        for type_code in sorted(results["missing_by_type_code"].keys()):
            count = results["missing_by_type_code"][type_code]
            print(f"  Code {type_code}: {count:>3} files")
        print()

    # Show sample of missing files
    limit = args.limit if hasattr(args, "limit") else 20
    show_have = args.show_have if hasattr(args, "show_have") else False

    if results["missing"]:
        print(
            f"Sample Missing Files (showing {min(limit, len(results['missing']))} of {len(results['missing'])}):"
        )
        print("-" * 80)
        for file_entry in results["missing"][:limit]:
            print(f"{file_entry.filename}")
            print(
                f"  Year: {file_entry.year}, Category: {file_entry.category}, Size: {format_size(file_entry.size_bytes)}"
            )
            if file_entry.last_updated:
                print(f"  Last Updated: {file_entry.last_updated}")
        print()

    # Show files we have if requested
    if show_have and results["have"]:
        print(
            f"Sample Files in Bronze + Archive (showing {min(limit, len(results['have']))} of {len(results['have'])}):"
        )
        print("-" * 80)
        for file_entry in results["have"][:limit]:
            print(f"{file_entry.filename}")
            print(
                f"  Year: {file_entry.year}, Category: {file_entry.category}, Size: {format_size(file_entry.size_bytes)}"
            )
        print()

    # Export if requested
    if hasattr(args, "export") and args.export:
        export_path = Path(args.export)
        print(f"Exporting missing files to: {export_path}")
        export_to_csv(results["missing"], export_path)
        print(f"Exported {len(results['missing'])} files")
        print()

    # ALWAYS save the missing files list so download command can use it
    state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"
    if results["missing"]:
        print("=" * 80)
        save_not_downloaded_state(results["missing"], state_path)
        print(f"Saved {len(results['missing'])} missing files to: {state_path}")
        print()
        print("To download these files, run:")
        print("  aco 4icli download")
        print()

    # Summary recommendation
    if results["missing_count"] > 0:
        print("=" * 80)
        print("Recommendations:")
        print("=" * 80)
        print(
            f"You are missing {results['missing_count']} files totaling {format_size(results['total_size_bytes'])}"
        )
        print()
        print("To download missing files, use:")
        print("  aco 4icli download --category <category> --year <year>")
        print()

    return 0


def cmd_download(args):
    """
    Download files identified by the last 'need-download' command.

        By default, downloads the files saved from the last 'aco 4icli need-download' run.
        This creates a clean workflow:
          1. aco 4icli need-download  → Queries remote DataHub, finds missing files
          2. aco 4icli download       → Downloads those specific files

        The missing files list is saved in:
        {workspace}/logs/tracking/4icli_notdownloaded_state.json

        Args:
            args: Command-line arguments from argparse
    """
    # Load configuration
    config = FourICLIConfig.from_profile()
    log_writer = LogWriter(name="4icli-cli")

    # Check for saved missing files list from need-download
    state_path = config.log_dir / "tracking" / "4icli_notdownloaded_state.json"

    if state_path.exists():
        print("=" * 80)
        print("Using files from last 'need-download' run")
        print("=" * 80)
        print(f"Loading missing files list from: {state_path}")
        print()

        # Load the missing files list saved by need-download
        import json

        with open(state_path) as f:
            state_data = json.load(f)

        # The state file has structure: {generated_at, total_missing, files: [...]}
        files_list = state_data.get("files", [])

        if not files_list:
            print("The saved missing files list is empty!")
            print("Run 'aco 4icli need-download' first to check for missing files.")
            return 0

        # Show summary from saved state
        print(f"Saved on: {state_data.get('generated_at', 'unknown')}")
        print(f"Total missing files: {state_data.get('total_missing', len(files_list))}")
        print(f"Total size: {state_data.get('total_size_formatted', 'unknown')}")
        print()

        # Reconstruct FileInventoryEntry objects
        from .inventory import FileInventoryEntry

        missing_files = []
        for entry_data in files_list:
            missing_files.append(
                FileInventoryEntry(
                    filename=entry_data["filename"],
                    category=entry_data["category"],
                    file_type_code=entry_data.get("file_type_code"),
                    year=entry_data["year"],
                    size_bytes=entry_data.get("size_bytes"),
                    last_updated=entry_data.get("last_updated"),
                    discovered_at=entry_data.get("discovered_at"),
                )
            )

    else:
        print("=" * 80)
        print("No saved missing files list found")
        print("=" * 80)
        print(f"Expected file: {state_path}")
        print()
        print("Please run 'aco 4icli need-download' first to identify missing files.")
        print("This will query the remote DataHub and save the list of files to download.")
        return 1

    # Determine date filter (optional)
    updated_after = getattr(args, "updated_after", None)

    if updated_after:
        print(f"Using date filter: --updated-after={updated_after}")
        print()

    # Load schema registry to get file type codes
    from .registry import get_file_type_codes

    file_type_codes = get_file_type_codes()

    if not file_type_codes:
        print("Error: No file type codes found in schema registry!")
        print("Ensure schemas have fourIcli.fileTypeCode defined.")
        return 1

    print(f"Schema registry: {len(file_type_codes)} file type codes registered")
    print()

    # Load state tracker to check which files we already have
    from .state import FourICLIStateTracker

    state_tracker = FourICLIStateTracker(log_writer=log_writer)

    # Filter out files that already exist in our state with same filename
    # This prevents re-downloading files when using broad category/year filters
    truly_missing_files = []
    already_have_count = 0

    for file_entry in missing_files:
        # Check if file exists in state (by filename)
        if file_entry.filename in state_tracker._file_cache:
            log_writer.debug(
                f"Skipping {file_entry.filename} - already in state tracker"
            )
            already_have_count += 1
        else:
            truly_missing_files.append(file_entry)

    if already_have_count > 0:
        print(f"Filtered out {already_have_count} files already in local state")
        print(f"Remaining to download: {len(truly_missing_files)} files")
        print()

    # Use the filtered list
    missing_files = truly_missing_files

    if not missing_files:
        print("All missing files are already tracked in state!")
        print("No downloads needed.")
        return 0

    # Group missing files by (category, year, file_type_code) for more precise downloads
    # This prevents re-downloading files when using filters
    # For each group, we'll use the OLDEST last_updated date as the --LastUpdated filter
    from collections import defaultdict
    from datetime import datetime

    downloads_by_category_year_type = defaultdict(list)

    for file_entry in missing_files:
        category = file_entry.category
        year = file_entry.year
        file_type_code = file_entry.file_type_code

        # Include files with:
        # 1. No file_type_code (null) - can still be downloaded by category/year
        # 2. file_type_code that exists in our schema registry
        if file_type_code is None or file_type_code in file_type_codes:
            # Group by file_type_code for precise downloads (prevents re-downloading existing files)
            key = (category, year, file_type_code)
            downloads_by_category_year_type[key].append(file_entry)
        else:
            # file_type_code exists but not in registry - skip it
            log_writer.debug(
                f"Skipping file {file_entry.filename} - no schema for file_type_code={file_type_code}"
            )

    if not downloads_by_category_year_type:
        print("No files to download with registered schemas!")
        print()
        print("Missing files don't match any schema file type codes.")
        print("To add support for new file types, add fileTypeCode to schema YAML files.")
        return 0

    print(f"Grouped into {len(downloads_by_category_year_type)} download requests by category/year/type")
    print()

    # Calculate date filters for each group and check if files exist in bronze
    from pathlib import Path

    bronze_dir = Path(config.bronze_dir)
    downloads_with_dates = {}

    for key, files in downloads_by_category_year_type.items():
        # Check which files in this group are actually missing from bronze
        files_not_in_bronze = []
        for file_entry in files:
            bronze_path = bronze_dir / file_entry.filename
            if not bronze_path.exists():
                files_not_in_bronze.append(file_entry)
            else:
                log_writer.debug(
                    f"Skipping {file_entry.filename} - already exists in bronze directory"
                )

        # Skip this download group if all files already exist in bronze
        if not files_not_in_bronze:
            log_writer.info(
                f"Skipping download group {key} - all files already in bronze"
            )
            continue

        # Find the OLDEST last_updated date in this group
        # This ensures we download all missing files in this category/year/type
        oldest_date = None
        for file_entry in files_not_in_bronze:
            if file_entry.last_updated:
                try:
                    # Parse the ISO format timestamp
                    file_date = datetime.fromisoformat(
                        file_entry.last_updated.replace("Z", "+00:00")
                    )
                    if oldest_date is None or file_date < oldest_date:
                        oldest_date = file_date
                except (ValueError, AttributeError):
                    continue

        # Subtract one day from oldest_date to make --updatedAfter inclusive
        # (--updatedAfter 2025-11-09 excludes files updated ON 2025-11-09)
        if oldest_date:
            from datetime import timedelta

            oldest_date = oldest_date - timedelta(days=1)

        downloads_with_dates[key] = {
            "files": files_not_in_bronze,
            "oldest_date": oldest_date.strftime("%Y-%m-%d") if oldest_date else None,
            "file_count": len(files_not_in_bronze),
        }

    print("Download plan (using file_type_code and --LastUpdated filters):")
    for (category, year, file_type_code), info in downloads_with_dates.items():
        print(f"  {category} / Year {year} / Type {file_type_code}: {info['file_count']} files", end="")
        if info["oldest_date"]:
            print(f" (--LastUpdated={info['oldest_date']})")
        else:
            print(" (no date filter)")
    print()

    # Initialize download client
    client = FourICLI(config=config, log_writer=log_writer)

    # Track statistics
    total_downloaded = 0
    total_errors = []
    download_count = 0

    print("=" * 80)
    print("Downloading Missing Files (Date-Filtered)")
    print("=" * 80)
    print()

    # Download each group using date filters and file_type_code
    for idx, ((category, year, file_type_code), info) in enumerate(downloads_with_dates.items(), 1):
        download_count += 1

        print(f"[{idx}/{len(downloads_with_dates)}] {category} / Year {year} / Type {file_type_code}")
        print(f"  Expected files: {info['file_count']}")
        if info["oldest_date"]:
            print(f"  Date filter: --LastUpdated={info['oldest_date']}")
        else:
            print("  Date filter: None (downloading all)")

        try:
            # Map category string to enum
            category_enum = None
            if category:
                for cat in DataHubCategory:
                    if cat.value == category:
                        category_enum = cat
                        break

            # Convert file_type_code int to FileTypeCode enum
            from .models import FileTypeCode

            file_type_code_enum = None
            if file_type_code is not None:
                try:
                    file_type_code_enum = FileTypeCode(file_type_code)
                except ValueError:
                    log_writer.warning(
                        f"Unknown file_type_code {file_type_code}, downloading without type filter"
                    )

            # Build date filter using the oldest date from missing files
            date_filter = None
            if info["oldest_date"]:
                from .models import DateFilter

                date_filter = DateFilter(updated_after=info["oldest_date"])
            elif updated_after:
                # User provided explicit date filter
                from .models import DateFilter

                date_filter = DateFilter(updated_after=updated_after)

            # Download with date filter and file_type_code filter
            # This downloads only the specific file type, preventing re-downloads of other types
            result = client.download(
                category=category_enum,
                year=year,
                file_type_code=file_type_code_enum,
                date_filter=date_filter,
            )

            if result.success:
                total_downloaded += len(result.files_downloaded)
                print(f"  [OK] Downloaded {len(result.files_downloaded)} files")
            else:
                print(f"  [ERROR] Download failed: {', '.join(result.errors)}")
                total_errors.extend(result.errors)

        except Exception as e:  # ALLOWED: Continues processing remaining items despite error
            error_msg = f"Error downloading {category} year {year}: {e}"
            print(f"  [ERROR] {error_msg}")
            total_errors.append(error_msg)
            # Continue to next group even if this one fails
            continue

        print()

    # Summary
    print("=" * 80)
    print("Download Summary")
    print("=" * 80)
    print(f"Download requests:     {download_count}")
    print(f"Files downloaded:      {total_downloaded}")
    print(
        f"Expected missing files: {sum(info['file_count'] for info in downloads_with_dates.values())}"
    )
    print(f"Errors:                {len(total_errors)}")
    print()

    if total_errors:
        print("Errors encountered:")
        for error in total_errors[:10]:
            print(f"  - {error}")
        if len(total_errors) > 10:
            print(f"  ... and {len(total_errors) - 10} more")
        print()

    print(f"Downloaded files are in: {config.bronze_dir}")
    print()

    # Suggest next steps
    if total_downloaded > 0:
        print("Next steps:")
        print("  1. Run 'aco 4icli inventory --force' to update inventory with new files")
        print("  2. Run 'aco 4icli need-download' to verify all files are downloaded")
        print()

    return 0 if not total_errors else 1


def cmd_inventory(args):
    """
    Manage DataHub file inventory.

        This command discovers all available files in the CMS DataHub across
        multiple years and stores the inventory in:
        {workspace}/logs/tracking/4icli_inventory_state.json

        Args:
            args: Command-line arguments from argparse
    """
    # Load configuration to get APM ID
    config = FourICLIConfig.from_profile()
    log_writer = LogWriter(name="4icli-cli")
    discovery = InventoryDiscovery(request_delay=2.0, log_writer=log_writer)

    inventory_path = discovery.get_inventory_path()

    # Determine years to scan
    start_year = args.start_year if hasattr(args, "start_year") and args.start_year else 2022
    end_year = (
        args.end_year if hasattr(args, "end_year") and args.end_year else get_current_year()
    )

    # Check for force flag
    force = args.force if hasattr(args, "force") else False

    # Check if inventory exists
    if inventory_path.exists() and not force:
        print(f"Inventory file exists: {inventory_path}")
        print("Loading existing inventory...")

        try:
            result = InventoryResult.load_from_json(inventory_path)
            print(f"Loaded inventory with {result.total_files} files")
            print(f"Last scan: {result.completed_at}")
            print()

            # Check if we need to scan new years
            existing_years = set(result.years)
            requested_years = set(range(start_year, end_year + 1))
            new_years = requested_years - existing_years

            if new_years:
                print(f"Scanning new years: {sorted(new_years)}")
                print()

                # Scan only new years
                for year in sorted(new_years):
                    print(f"Scanning year {year}...")
                    year_result = discovery.discover_years(
                        apm_id=result.apm_id,
                        start_year=year,
                        end_year=year,
                        categories=[
                            DataHubCategory.BENEFICIARY_LIST,
                            DataHubCategory.CCLF,
                            DataHubCategory.REPORTS,
                        ],
                    )

                    # Merge with existing inventory
                    result.files.extend(year_result.files)
                    result.years.extend(year_result.years)
                    result.total_files = len(result.files)

                    # Update statistics
                    for file_entry in year_result.files:
                        year_key = int(file_entry.year) if file_entry.year else 0
                        result.files_by_year[year_key] = result.files_by_year.get(year_key, 0) + 1
                        result.files_by_category[file_entry.category] = (
                            result.files_by_category.get(file_entry.category, 0) + 1
                        )

                result.completed_at = datetime.now()

                # Enrich with file type codes
                print()
                print("Enriching inventory with file type codes...")
                result = discovery.enrich_with_file_type_codes(result)

                # Save updated inventory
                result.save_to_json(inventory_path)
                print(f"Updated inventory saved to: {inventory_path}")
            else:
                print("All requested years already in inventory.")
                print("Use --force to rebuild entire inventory.")

        except Exception as e:  # ALLOWED: Returns None to indicate error
            print(f"Error loading inventory: {e}")
            print("Use --force to rebuild inventory from scratch.")
            return

    else:
        if force:
            print("Force flag set - rebuilding entire inventory from scratch")
        else:
            print("No existing inventory found - creating new inventory")

        print()
        print(f"Scanning years {start_year}-{end_year}...")
        print("This may take several minutes due to API rate limiting.")
        print()

        # Get APM ID from config or environment
        apm_id = config.default_apm_id
        if not apm_id:
            print("Error: APM ID not configured.")
            print("Set one of these environment variables: FOURICLI_APM_ID, ACO_APM_ID")
            print("Or add 'default_apm_id' to your profile config under 'fouricli' section")
            return 1

        print(f"Using APM ID: {apm_id}")
        print()

        # Full inventory discovery
        result = discovery.discover_years(
            apm_id=apm_id,
            start_year=start_year,
            end_year=end_year,
            categories=[
                DataHubCategory.BENEFICIARY_LIST,
                DataHubCategory.CCLF,
                DataHubCategory.REPORTS,
            ],
        )

        # Enrich with file type codes
        print()
        print("Enriching inventory with file type codes from schemas...")
        result = discovery.enrich_with_file_type_codes(result)

        # Save
        result.save_to_json(inventory_path)
        print()
        print(f"Inventory saved to: {inventory_path}")

    # Print summary
    print()
    print("=" * 80)
    print("Inventory Summary")
    print("=" * 80)
    print()
    print(f"Total files: {result.total_files}")
    print(f"Years: {min(result.years)}-{max(result.years)}")
    print(f"Duration: {result.duration_seconds:.1f} seconds" if result.duration_seconds else "N/A")
    print()

    print("Files by Year:")
    for year in sorted(result.files_by_year.keys(), key=lambda x: int(x)):
        count = result.files_by_year[year]
        print(f"  {year}: {count:>3} files")
    print()

    print("Files by Category:")
    for category in sorted(result.files_by_category.keys()):
        count = result.files_by_category[category]
        print(f"  {category:20s}: {count:>3} files")
    print()

    # Count files with type codes
    files_with_codes = sum(1 for f in result.files if f.file_type_code is not None)
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


def main():
    """Standalone CLI entry point for 4icli (skinny install)."""
    import argparse
    import sys
    import traceback

    from acoharmony import __version__

    parser = argparse.ArgumentParser(
        prog="aco-4icli",
        description="ACO Harmony - CMS DataHub File Management (4icli)",
    )
    parser.add_argument("--version", action="version", version=f"ACO Harmony {__version__}")

    subparsers = parser.add_subparsers(dest="command", help="4icli commands")

    # inventory subcommand
    inventory_parser = subparsers.add_parser(
        "inventory", help="Discover and manage DataHub file inventory"
    )
    inventory_parser.add_argument(
        "--force",
        action="store_true",
        help="Force rebuild entire inventory from scratch (default: incremental update)",
    )
    inventory_parser.add_argument(
        "--start-year", type=int, default=None, help="Starting year for scan (default: 2022)"
    )
    inventory_parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="Ending year for scan (default: current year)",
    )

    # need-download subcommand
    need_download_parser = subparsers.add_parser(
        "need-download", help="Query remote DataHub and show files not downloaded locally"
    )
    need_download_parser.add_argument(
        "--start-year", type=int, default=None, help="Start year for remote query (default: 2022)"
    )
    need_download_parser.add_argument(
        "--end-year",
        type=int,
        default=None,
        help="End year for remote query (default: current year)",
    )
    need_download_parser.add_argument(
        "--year", type=int, help="Filter by specific year (for display only)"
    )
    need_download_parser.add_argument(
        "--category", help="Filter by category (e.g., CCLF, 'Beneficiary List', Reports)"
    )
    need_download_parser.add_argument(
        "--export", type=str, metavar="FILE", help="Export missing files to CSV"
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

    # download subcommand
    download_parser = subparsers.add_parser(
        "download", help="Download files from last need-download run"
    )
    download_parser.add_argument(
        "--updated-after",
        type=str,
        metavar="YYYY-MM-DD",
        help="Optional date filter: only download files updated after this date",
    )

    # setup subcommand — prompts for fresh KEY/SECRET, runs bootstrap
    subparsers.add_parser(
        "setup", help="Refresh 4i credentials after a portal rotation"
    )

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    try:
        if args.command == "inventory":
            cmd_inventory(args)
        elif args.command == "need-download":
            cmd_need_download(args)
        elif args.command == "download":
            cmd_download(args)
        elif args.command == "setup":
            sys.exit(cmd_setup(args))
        else:
            parser.print_help()
            sys.exit(1)
    except Exception as e:
        print(f"[ERROR] {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
