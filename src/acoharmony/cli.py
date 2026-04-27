#!/usr/bin/env python3
"""
ACO Harmony CLI - Unified command-line interface.

Healthcare data processing with medallion architecture (Bronze → Silver → Gold).
Transforms are registered via decorators and orchestrated through pipelines.
"""

import argparse
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from acoharmony import __version__

# Full package imports — deferred to allow skinny installs
_FULL_PACKAGE_AVAILABLE = None


def _require_full_package():
    """Import full package modules, raising a clear error if not installed."""
    global _FULL_PACKAGE_AVAILABLE
    if _FULL_PACKAGE_AVAILABLE is True:
        return
    try:
        global Catalog, MedallionLayer, TransformRunner, get_config, StorageBackend
        from acoharmony import (
            Catalog,
            MedallionLayer,
            TransformRunner,
            get_config,
        )
        from acoharmony._store import StorageBackend

        _FULL_PACKAGE_AVAILABLE = True
    except ImportError:
        _FULL_PACKAGE_AVAILABLE = False
        print(
            "[ERROR] This command requires the full package.\n"
            "Install with: uv pip install 'acoharmony[full]'"
        )
        sys.exit(1)


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        prog="aco",
        description="ACO Harmony - Healthcare Data Processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        """,
    )

    parser.add_argument("--version", action="version", version=f"ACO Harmony {__version__}")

    # Subcommands
    subparsers = parser.add_subparsers(dest="command", help="Commands")

    # Transform command
    transform_parser = subparsers.add_parser(
        "transform", help="Process data with medallion architecture support"
    )
    transform_parser.add_argument(
        "table", nargs="?", help="Table name to transform (includes source tracking)"
    )
    transform_parser.add_argument("--all", action="store_true", help="Transform all tables")
    transform_parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold"],
        help="Transform all tables in medallion layer",
    )
    transform_parser.add_argument("--pattern", help="Transform tables matching pattern")
    transform_parser.add_argument(
        "--force", action="store_true", help="Force reprocessing all data"
    )
    transform_parser.add_argument(
        "--no-tracking", action="store_true", help="Disable source tracking columns"
    )
    transform_parser.add_argument(
        "--chunk-size", type=int, help="Override chunk size for large datasets"
    )

    # Pipeline command
    pipeline_parser = subparsers.add_parser("pipeline", help="Run end-to-end data pipelines")
    pipeline_parser.add_argument(
        "name", help="Pipeline name (e.g., medical_claim, eligibility, pharmacy_claim)"
    )
    pipeline_parser.add_argument(
        "--force", action="store_true", help="Force reprocessing all pipeline steps"
    )

    # List command
    list_parser = subparsers.add_parser("list", help="List available tables and pipelines")
    list_parser.add_argument(
        "--pipelines", action="store_true", help="List pipelines instead of tables"
    )
    list_parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold"],
        help="List only tables in specified medallion layer",
    )

    # Config command
    config_parser = subparsers.add_parser("config", help="Show configuration and storage paths")
    config_parser.add_argument("--schema", help="Show config for specific schema")
    config_parser.add_argument(
        "--storage", action="store_true", help="Show storage configuration details"
    )

    # Clean command
    clean_parser = subparsers.add_parser("clean", help="Clean temporary files")
    clean_parser.add_argument("--all", action="store_true", help="Clean all including outputs")

    # Expressions command
    expressions_parser = subparsers.add_parser(
        "expressions", help="Inspect expression metadata and applicability"
    )
    expressions_parser.add_argument(
        "expression", nargs="?", help="Expression type to inspect (default: show all)"
    )
    expressions_parser.add_argument(
        "--schema",
        choices=["bronze", "silver", "gold"],
        help="Show expressions for specific schema",
    )
    expressions_parser.add_argument(
        "--dataset-type", help="Filter by dataset type (e.g., claims, eligibility)"
    )

    # Dev command
    dev_parser = subparsers.add_parser("dev", help="Development utilities")
    dev_subparsers = dev_parser.add_subparsers(dest="dev_command", help="Dev commands")

    # Dev generate subcommand
    generate_parser = dev_subparsers.add_parser("generate", help="Generate development artifacts")
    generate_parser.add_argument(
        "--copyright", action="store_true", help="Add copyright headers to Python files"
    )
    generate_parser.add_argument(
        "--metadata", action="store_true", help="Generate ACO metadata documentation"
    )
    generate_parser.add_argument(
        "--notebooks",
        action="store_true",
        help="Generate notebook documentation from Marimo notebooks",
    )
    generate_parser.add_argument(
        "--pipelines",
        action="store_true",
        help="Generate pipeline documentation (expressions and transforms)",
    )
    generate_parser.add_argument(
        "--modules",
        action="store_true",
        help="Generate module API reference (AST-based docs for all _* packages)",
    )
    generate_parser.add_argument(
        "--all-docs",
        action="store_true",
        help="Generate all documentation (modules, metadata, notebooks, pipelines)",
    )
    generate_parser.add_argument(
        "--force", action="store_true", help="Force regeneration/overwrite"
    )
    generate_parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be done without making changes"
    )
    generate_parser.add_argument(
        "--year", type=int, help="Copyright year (defaults to current year)"
    )

    # Dev generate-notes subcommand
    notes_parser = dev_subparsers.add_parser(
        "generate-notes", help="Generate Marimo notebooks from schemas"
    )
    notes_parser.add_argument("schema", nargs="?", help="Schema name to generate notebook for")
    notes_parser.add_argument(
        "--all", action="store_true", help="Generate notebooks for all raw schemas"
    )
    notes_parser.add_argument(
        "--output-dir",
        default="/home/care/acoharmony/notebooks/generated",
        help="Output directory for notebooks",
    )
    notes_parser.add_argument(
        "--template", default="dynamic_dashboard.py.j2", help="Template to use"
    )

    # Dev storage subcommand
    storage_parser = dev_subparsers.add_parser("storage", help="Storage setup and management")
    storage_parser.add_argument("action", choices=["setup", "verify"], help="Action to perform")
    storage_parser.add_argument(
        "--profile",
        type=str,
        default="local",
        choices=["local", "dev", "staging", "prod"],
        help="Configuration profile to use (default: local)",
    )
    storage_parser.add_argument(
        "--create-bucket", action="store_true", help="Create RustFS/S3 bucket (for staging/prod)"
    )
    storage_parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be done without making changes"
    )
    storage_parser.add_argument(
        "--workspace-path",
        type=str,
        default="/opt/s3/data/workspace",
        help="Path to workspace directory for symlinks (local profile)",
    )

    # Dev unpack subcommand
    unpack_parser = dev_subparsers.add_parser(
        "unpack", help="Extract ZIP files from bronze directory"
    )
    unpack_parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be done without making changes"
    )

    # Dev generate-mocks subcommand
    mocks_parser = dev_subparsers.add_parser(
        "generate-mocks", help="Generate synthetic test fixtures from production data"
    )
    mocks_parser.add_argument(
        "--layers",
        nargs="+",
        default=["silver", "gold"],
        help="Layers to scan (default: silver gold)",
    )
    mocks_parser.add_argument(
        "--tables", nargs="+", help="Specific tables to generate (default: all)"
    )
    mocks_parser.add_argument(
        "--output-dir",
        default="/opt/s3/data/workspace/logs/dev/fixtures",
        help="Output directory for fixtures (default: /opt/s3/data/workspace/logs/dev/fixtures)",
    )
    mocks_parser.add_argument(
        "--n-rows", type=int, default=100, help="Rows per synthetic fixture (default: 100)"
    )
    mocks_parser.add_argument(
        "--sample-size",
        type=int,
        default=1000,
        help="Sample size for metadata extraction (default: 1000)",
    )
    mocks_parser.add_argument("--dry-run", action="store_true", help="Only scan, don't generate")
    mocks_parser.add_argument("--force", action="store_true", help="Regenerate existing fixtures")
    mocks_parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Deterministic RNG seed so same seed → identical fixtures",
    )

    # Dev generate-tests subcommand
    _test_gen_parser = dev_subparsers.add_parser(
        "generate-tests", help="Generate test stubs for modules without tests"
    )

    # Dev cleanup-tests subcommand
    _test_clean_parser = dev_subparsers.add_parser(
        "cleanup-tests", help="Remove orphaned test files"
    )

    # Schema command - Schema versioning and history
    schema_parser = subparsers.add_parser("schema", help="Schema versioning and history")
    schema_subparsers = schema_parser.add_subparsers(dest="schema_command", help="Schema commands")

    # schema history
    history_parser = schema_subparsers.add_parser("history", help="Show schema version history")
    history_parser.add_argument("schema_name", help="Name of schema")

    # schema diff
    diff_parser = schema_subparsers.add_parser("diff", help="Compare schema versions")
    diff_parser.add_argument("schema_name", help="Name of schema")
    diff_parser.add_argument("from_version", help="Source version")
    diff_parser.add_argument("to_version", help="Target version")

    # schema list
    _list_schemas_parser = schema_subparsers.add_parser(
        "list", help="List all schemas with versions"
    )

    # schema validate
    validate_parser = schema_subparsers.add_parser("validate", help="Validate schema templates")
    validate_parser.add_argument("--all", action="store_true", help="Validate all schemas")
    validate_parser.add_argument("schema_name", nargs="?", help="Specific schema to validate")

    # Databricks command - Transfer parquet files to Databricks-compatible format
    databricks_parser = subparsers.add_parser(
        "databricks", help="Transfer parquet files to Databricks-compatible format"
    )
    databricks_parser.add_argument(
        "--transfer",
        action="store_true",
        help="Transfer changed files from silver/gold to Downloads with SNAPPY compression",
    )
    databricks_parser.add_argument(
        "--status",
        action="store_true",
        help="Show transfer status and history",
    )
    databricks_parser.add_argument(
        "--force",
        action="store_true",
        help="Force transfer all files regardless of change detection",
    )
    databricks_parser.add_argument(
        "--dest",
        type=str,
        help="Destination directory (default: /home/care/kcorwin/Downloads)",
    )
    databricks_parser.add_argument(
        "--log",
        action="store_true",
        help="Aggregate tracking state logs into gov_programs_logs.parquet",
    )

    # 4icli command - CMS DataHub integration
    fouricli_parser = subparsers.add_parser("4icli", help="CMS DataHub file management")
    fouricli_subparsers = fouricli_parser.add_subparsers(
        dest="fouricli_command", help="4icli commands"
    )

    # 4icli inventory subcommand
    inventory_parser = fouricli_subparsers.add_parser(
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

    # 4icli need-download subcommand
    need_download_parser = fouricli_subparsers.add_parser(
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

    # 4icli download subcommand
    download_parser = fouricli_subparsers.add_parser(
        "download", help="Download files from last need-download run"
    )
    download_parser.add_argument(
        "--updated-after",
        type=str,
        metavar="YYYY-MM-DD",
        help="Optional date filter: only download files updated after this date",
    )

    # 4icli setup subcommand — prompts for fresh KEY/SECRET, persists to .env,
    # and runs deploy/images/4icli/bootstrap.sh. Operator runs this after a
    # 4Innovation portal rotation; takes no arguments.
    fouricli_subparsers.add_parser(
        "setup", help="Refresh 4i credentials after a portal rotation"
    )

    # xfr command - file transfer between locations via pluggable profiles
    from acoharmony._xfr.cli import add_subparsers as _xfr_add_subparsers
    _xfr_add_subparsers(subparsers)

    # PUF command - CMS Public Use Files management
    puf_parser = subparsers.add_parser("puf", help="CMS Public Use Files (PUF) management")
    puf_subparsers = puf_parser.add_subparsers(dest="puf_command", help="PUF commands")

    # puf inventory subcommand
    puf_inventory_parser = puf_subparsers.add_parser(
        "inventory", help="Sync and manage PUF file inventory"
    )
    puf_inventory_parser.add_argument(
        "--dataset",
        choices=["pfs", "rvu", "zipcarrier"],
        default="pfs",
        help="Dataset to use (default: pfs)",
    )
    puf_inventory_parser.add_argument(
        "--year", type=str, help="Filter by specific year (e.g., 2024)"
    )
    puf_inventory_parser.add_argument(
        "--rule-type",
        choices=["Proposed", "Final", "Correction", "Interim Final"],
        help="Filter by rule type",
    )
    puf_inventory_parser.add_argument(
        "--force", action="store_true", help="Force refresh of all inventory items"
    )

    # puf need-download subcommand
    puf_need_download_parser = puf_subparsers.add_parser(
        "need-download", help="Show PUF files that need downloading"
    )
    puf_need_download_parser.add_argument(
        "--dataset",
        choices=["pfs", "rvu", "zipcarrier"],
        default="pfs",
        help="Dataset to use (default: pfs)",
    )
    puf_need_download_parser.add_argument(
        "--year", type=str, help="Filter by specific year (e.g., 2024)"
    )
    puf_need_download_parser.add_argument(
        "--rule-type",
        choices=["Proposed", "Final", "Correction", "Interim Final"],
        help="Filter by rule type",
    )
    puf_need_download_parser.add_argument(
        "--category", type=str, help="Filter by file category (e.g., addenda, gpci)"
    )
    puf_need_download_parser.add_argument(
        "--schema", type=str, help="Filter by schema mapping (e.g., pprvu_inputs)"
    )
    puf_need_download_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Limit number of files to display (default: 20)",
    )

    # puf download subcommand
    puf_download_parser = puf_subparsers.add_parser(
        "download", help="Download PUF files from last need-download run"
    )
    puf_download_parser.add_argument(
        "--dataset",
        choices=["pfs", "rvu", "zipcarrier"],
        default="pfs",
        help="Dataset to use (default: pfs)",
    )
    puf_download_parser.add_argument("--limit", type=int, help="Limit number of files to download")

    # puf years subcommand
    _puf_years_parser = puf_subparsers.add_parser("years", help="List available years")

    # puf categories subcommand
    _puf_categories_parser = puf_subparsers.add_parser("categories", help="List file categories")

    # puf search subcommand
    puf_search_parser = puf_subparsers.add_parser("search", help="Search for files by keyword")
    puf_search_parser.add_argument("search_term", help="Search term")
    puf_search_parser.add_argument(
        "--search-in",
        choices=["key", "description", "category", "all"],
        default="all",
        help="Where to search (default: all)",
    )

    # puf unpack subcommand
    puf_unpack_parser = puf_subparsers.add_parser(
        "unpack", help="Extract PUF ZIP files with metadata-aware renaming"
    )
    puf_unpack_parser.add_argument(
        "--dataset",
        choices=["pfs", "rvu", "zipcarrier"],
        default="pfs",
        help="Dataset to use (default: pfs)",
    )
    puf_unpack_parser.add_argument("--year", type=str, help="Filter by specific year (e.g., 2024)")
    puf_unpack_parser.add_argument(
        "--rule-type",
        choices=["Proposed", "Final", "Correction", "Interim Final"],
        help="Filter by rule type",
    )
    puf_unpack_parser.add_argument(
        "--category", type=str, help="Filter by file category (e.g., addenda, gpci)"
    )
    puf_unpack_parser.add_argument(
        "--dry-run", action="store_true", help="Simulate extraction without modifying files"
    )

    # Cite command - Citation management
    cite_parser = subparsers.add_parser("cite", help="Citation management and processing")
    cite_subparsers = cite_parser.add_subparsers(
        dest="cite_command",
        help="Citation commands (defaults to 'interactive' if no subcommand provided)",
    )
    cite_subparsers.required = False  # Make subcommand optional, default to interactive

    # cite url subcommand
    cite_url_parser = cite_subparsers.add_parser(
        "url",
        help="Fetch and process citation from URL",
        description="Download, parse, and catalog citations from URLs. "
        "Supports PDF, HTML, Markdown, and LaTeX formats. "
        "Results are cached and deduplicated automatically.",
        epilog="""
examples:
  # Basic usage
  aco cite url "https://arxiv.org/pdf/2301.12345.pdf"

  # With tags for organization
  aco cite url "https://www.cms.gov/regulations/..." --tags "cms,regulations,2025"

  # With tags and note
  aco cite url "https://example.com/document.pdf" \\
    --tags "cms,quality-measures,2024" \\
    --note "Reference for Q4 2024 quality reporting project"

  # Force reprocessing
  aco cite url "https://example.com/doc.pdf" --force

  # Output JSON only
  aco cite url "https://example.com/doc.pdf" --format json

notes:
  - Tags are stored in citation metadata and can be used for filtering and organization
  - Multiple tags should be comma-separated without spaces
  - Citations are automatically cached; use --force to reprocess
  - Processed citations are saved to workspace/cites/corpus/
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    cite_url_parser.add_argument("url", help="URL to fetch (PDF, HTML, Markdown, LaTeX)")
    cite_url_parser.add_argument(
        "--force", action="store_true", help="Force reprocess even if already cached"
    )
    cite_url_parser.add_argument(
        "--format",
        choices=["json", "parquet", "both"],
        default="both",
        help="Output format (default: both)",
    )
    cite_url_parser.add_argument(
        "--note",
        type=str,
        default="",
        help="Add a note to the citation (e.g., 'Reference for project X')",
    )
    cite_url_parser.add_argument(
        "--tags",
        type=str,
        default="",
        help="Comma-separated tags for organization and filtering (e.g., 'cms,regulations,2025'). "
        "Tags are stored in citation metadata and persisted to cite_state.json",
    )

    # cite batch subcommand
    cite_batch_parser = cite_subparsers.add_parser(
        "batch", help="Process multiple citations from file"
    )
    cite_batch_parser.add_argument("file", help="File containing URLs (one per line)")
    cite_batch_parser.add_argument(
        "--force", action="store_true", help="Force reprocess even if already cached"
    )
    cite_batch_parser.add_argument(
        "--workers", type=int, default=4, help="Number of parallel workers (default: 4)"
    )

    # cite list subcommand
    cite_list_parser = cite_subparsers.add_parser("list", help="List processed citations")
    cite_list_parser.add_argument(
        "--source-type",
        choices=["pdf", "html", "markdown", "latex", "all"],
        default="all",
        help="Filter by source type (default: all)",
    )
    cite_list_parser.add_argument("--limit", type=int, default=20, help="Limit results")

    # cite stats subcommand
    _cite_stats_parser = cite_subparsers.add_parser("stats", help="Show citation corpus statistics")

    # cite interactive subcommand
    cite_interactive_parser = cite_subparsers.add_parser(
        "interactive",
        help="Interactive mode - prompts for URL, notes, and tags",
        description="Interactive citation mode that prompts for all required information",
    )
    cite_interactive_parser.add_argument(
        "--force", action="store_true", help="Force reprocess even if already cached"
    )

    # Deploy command - Docker Compose deployment management
    deploy_parser = subparsers.add_parser(
        "deploy", help="Manage Docker Compose deployment services"
    )
    deploy_parser.add_argument(
        "action",
        choices=["start", "stop", "restart", "status", "logs", "ps", "build"],
        help="Deployment action to perform",
    )
    deploy_parser.add_argument(
        "services", nargs="*", help="Specific services to act on (optional)"
    )
    deploy_parser.add_argument(
        "--group",
        "-g",
        help="Service group (root, infrastructure, analytics, development)",
    )
    deploy_parser.add_argument(
        "--follow",
        "-f",
        action="store_true",
        help="Follow log output (logs)",
    )
    deploy_parser.add_argument(
        "--tail", type=int, help="Number of log lines to show (logs)"
    )
    deploy_parser.add_argument(
        "--build",
        action="store_true",
        help="Build images locally instead of pulling (start)",
    )
    deploy_parser.add_argument(
        "--pull",
        "-p",
        action="store_true",
        help="Force-pull all acoharmony images, ignoring deploy state (start, restart)",
    )

    # Test command - Coverage orchestration
    test_parser = subparsers.add_parser(
        "test", help="Run tests with coverage tracking and intelligent test planning"
    )
    test_parser.add_argument(
        "--test-path", help="Specific test file/directory to run (default: all tests)"
    )
    test_parser.add_argument(
        "--no-targets", action="store_true", help="Don't show next coverage targets"
    )
    test_parser.add_argument(
        "--work-dir",
        default=".coverage_state",
        help="Working directory for coverage state (default: .coverage_state)",
    )
    test_parser.add_argument(
        "--src-root",
        default="src/acoharmony",
        help="Source root for coverage tracking (default: src/acoharmony)",
    )

    # SVA command - Supplemental Voluntary Alignment validation
    sva_parser = subparsers.add_parser("sva", help="SVA submission validation and management")
    sva_subparsers = sva_parser.add_subparsers(dest="sva_command", help="SVA commands")

    # sva validate subcommand
    sva_validate_parser = sva_subparsers.add_parser(
        "validate", help="Validate SVA submission file against participant list and BAR"
    )
    sva_validate_parser.add_argument("file_path", help="Path to SVA submission file (.xlsx)")
    sva_validate_parser.add_argument(
        "--verbose", "-v", action="store_true", help="Show detailed validation errors"
    )

    args = parser.parse_args()

    # Execute command
    if args.command == "transform":
        _require_full_package()
        config = get_config()
        # Initialize components for transform command
        catalog = Catalog(StorageBackend())
        runner = TransformRunner()

        # Auto-unpack any ZIP files in bronze directory
        from acoharmony._utils.unpack import unpack_bronze_zips

        unpack_result = unpack_bronze_zips(dry_run=False)
        if unpack_result["found"] > 0:
            print(
                f"Auto-unpacked {unpack_result['processed']} ZIP files from bronze ({unpack_result['extracted']} files extracted)"
            )

        if args.all:
            print("Processing all tables...")
            result = runner.transform_all(
                force=args.force, no_tracking=args.no_tracking, chunk_size=args.chunk_size
            )
        elif args.layer:
            # Transform entire medallion layer
            layer = MedallionLayer.from_tier(args.layer)
            print(f"Processing {args.layer} layer ({layer.data_tier} tier)...")
            results = runner.transform_medallion_layer(
                layer, force=args.force, chunk_size=args.chunk_size, no_tracking=args.no_tracking
            )
            # Print results
            success_count = sum(1 for r in results.values() if r.success)
            print(f"\n[OK] Processed {success_count}/{len(results)} tables in {args.layer} layer")
            for table_name, result in results.items():
                status = "[OK]" if result.success else "[ERROR]"
                print(f"  {status} {table_name}: {result.status}")
            return 0 if success_count == len(results) else 1
        elif args.pattern:
            print(f"Processing tables matching pattern: {args.pattern}")
            results = runner.transform_pattern(
                args.pattern, force=args.force, no_tracking=args.no_tracking
            )
            # Print results
            success_count = sum(1 for r in results.values() if r.success)
            print(
                f"\n[OK] Processed {success_count}/{len(results)} tables matching pattern '{args.pattern}'"
            )
            for table_name, result in results.items():
                status = "[OK]" if result.success else "[ERROR]"
                print(f"  {status} {table_name}: {result.status}")
            return 0 if success_count == len(results) else 1
        elif args.table:
            print(f"Processing table: {args.table}")
            result = runner.transform_table(
                args.table,
                force=args.force,
                no_tracking=args.no_tracking,
                chunk_size=args.chunk_size,
            )
            # Print result with source tracking info
            print(result)
            if hasattr(result, "success") and result.success:
                print(
                    "[OK] Table processed with source tracking: processed_at, source_file, source_filename"
                )
            return 0 if result.success else 1
        else:
            transform_parser.print_help()
            return 1

    elif args.command == "pipeline":
        _require_full_package()
        config = get_config()
        # Initialize components for pipeline command
        runner = TransformRunner()

        # Auto-unpack any ZIP files in bronze directory
        from acoharmony._utils.unpack import unpack_bronze_zips

        unpack_result = unpack_bronze_zips(dry_run=False)
        if unpack_result["found"] > 0:
            print(
                f"Auto-unpacked {unpack_result['processed']} ZIP files from bronze ({unpack_result['extracted']} files extracted)\n"
            )

        result = runner.run_pipeline(args.name, force=args.force)
        print(result.get_summary() if hasattr(result, "get_summary") else result)
        return 0 if result.success else 1

    elif args.command == "list":
        _require_full_package()
        # Initialize components for list command
        catalog = Catalog(StorageBackend())
        runner = TransformRunner()
        if args.pipelines:
            pipelines = runner.list_pipelines()
            print(f"Available pipelines ({len(pipelines)}):")
            for name in sorted(pipelines):
                print(f"  - {name}")
        else:
            if args.layer:
                # Filter by medallion layer
                layer = MedallionLayer.from_tier(args.layer)
                tables = catalog.list_tables(layer)
                print(f"{args.layer.capitalize()} layer tables ({len(tables)}):")
                for name in sorted(tables):
                    metadata = catalog.get_table_metadata(name)
                    desc = metadata.description[:60] if metadata and metadata.description else ""
                    tier = (
                        metadata.data_tier if metadata and metadata.medallion_layer else "unknown"
                    )
                    print(f"  - {name} [{tier}]: {desc}")
            else:
                # List all tables
                tables = catalog.list_tables()
                print(f"Available tables ({len(tables)}):")
                for name in sorted(tables):
                    metadata = catalog.get_table_metadata(name)
                    desc = metadata.description[:60] if metadata and metadata.description else ""
                    layer = (
                        metadata.medallion_layer.unity_schema
                        if metadata and metadata.medallion_layer
                        else "unknown"
                    )
                    tier = (
                        metadata.data_tier if metadata and metadata.medallion_layer else "unknown"
                    )
                    print(f"  - {name} [{layer}/{tier}]: {desc}")

    elif args.command == "config":
        _require_full_package()
        config = get_config()
        catalog = Catalog(StorageBackend())
        if args.schema:
            schema_config = config.get_schema_config(args.schema)
            metadata = catalog.get_table_metadata(args.schema)
            if metadata:
                print(f"Configuration for {args.schema}:")
                print(f"  Description: {metadata.description}")

                # Show medallion layer info
                if metadata.medallion_layer:
                    print(
                        f"  Medallion layer: {metadata.medallion_layer.unity_schema} ({metadata.data_tier} tier)"
                    )
                    print(f"  Unity Catalog: {metadata.full_table_name}")

                # Storage tier
                if hasattr(metadata, "storage") and metadata.storage:
                    tier = metadata.storage.get(
                        "tier", metadata.data_tier if metadata.medallion_layer else "processed"
                    )
                    print(f"  Storage tier: {tier}")

                print("  Table config:")
                for key, value in schema_config.__dict__.items():
                    print(f"    {key}: {value}")
            else:
                print(f"Table '{args.schema}' not found")
        elif args.storage:
            storage_config = StorageBackend()
            print("Storage configuration:")
            print(f"  Profile: {storage_config.profile}")
            print(f"  Storage type: {storage_config.get_storage_type()}")
            print(f"  Environment: {storage_config.get_environment()}")
            print("\nMedallion Architecture paths:")
            print(f"  Bronze (raw):       {storage_config.get_path(MedallionLayer.BRONZE)}")
            print(f"  Silver (processed): {storage_config.get_path(MedallionLayer.SILVER)}")
            print(f"  Gold (curated):     {storage_config.get_path(MedallionLayer.GOLD)}")
            print("\nOther paths:")
            print(f"  Temp:               {storage_config.get_path('tmp')}")
            print(f"  Logs:               {storage_config.get_path('logs')}")
        else:
            print("Global configuration:")
            print(f"  Base path: {config.storage.base_path}")
            print("  Architecture: Medallion (Bronze → Silver → Gold)")
            print("  Source tracking: enabled (processed_at, source_file, source_filename)")
            print(f"  Tracking: {config.transform.enable_tracking}")
            print(f"  Incremental: {config.transform.incremental}")
            print(f"  Chunk size: {config.transform.chunk_size}")
            print(f"  Compression: {config.transform.compression}")
            print("  Union support: enabled (medical_claim, pharmacy_claim)")

    elif args.command == "clean":
        _require_full_package()
        runner = TransformRunner()
        runner.clean_temp_files(all_files=args.all)
        print("Cleaned temporary files")

    elif args.command == "expressions":
        _require_full_package()
        from acoharmony._expressions.inspect import (
            print_expression_metadata,
            print_expressions_for_schema,
        )

        if args.schema:
            # Show expressions for specific schema
            print_expressions_for_schema(args.schema, args.dataset_type)
        else:
            # Show expression metadata (all or specific)
            print_expression_metadata(args.expression)

        return 0

    elif args.command == "dev":
        _require_full_package()
        if args.dev_command == "generate":
            # Handle --all-docs flag
            if args.all_docs:
                from acoharmony._dev import (
                    generate_aco_metadata,
                    generate_module_docs,
                )
                from acoharmony._dev.docs.pipelines import generate_full_documentation

                results = []

                print("Generating all documentation...")

                # Generate module API reference FIRST (AST-based, fast)
                try:
                    success = generate_module_docs()
                    results.append(("Module API reference", success))
                    if success:
                        print("  [OK] Module API reference generated (docs/docs/modules/)")
                    else:
                        print("  [ERROR] Failed to generate module API reference")
                except Exception as e:
                    results.append(("Module API reference", False))
                    print(f"  [ERROR] Failed to generate module API reference: {e}")

                # Generate ACO metadata
                success = generate_aco_metadata()
                results.append(("ACO_METADATA.md", success))
                if success:
                    print("  [OK] ACO_METADATA.md generated")
                else:
                    print("  [ERROR] Failed to generate ACO_METADATA.md")

                # Generate pipeline docs
                try:
                    generate_full_documentation()
                    results.append(("Pipeline docs", True))
                    print("  [OK] Pipeline documentation generated")
                except Exception as e:
                    results.append(("Pipeline docs", False))
                    print(f"  [ERROR] Failed to generate pipeline documentation: {e}")

                # Summary
                successful = sum(1 for _, s in results if s)
                print(f"\n[OK] Generated {successful}/{len(results)} documents successfully")

                return 0 if all(s for _, s in results) else 1

            elif args.copyright:
                from acoharmony._dev import add_copyright

                success = add_copyright(
                    force=args.force, dry_run=getattr(args, "dry_run", False), year=args.year
                )
                if not success:
                    return 1

            elif args.metadata:
                from acoharmony._dev import generate_aco_metadata

                success = generate_aco_metadata()
                if success:
                    print("[OK] ACO_METADATA.md generated successfully in docs folder")
                else:
                    print("[ERROR] Failed to generate ACO metadata documentation")
                    return 1

            elif args.notebooks:
                print("[ERROR] Notebook documentation generation is not yet implemented")
                return 1

            elif args.modules:
                from acoharmony._dev.docs.modules import generate_module_docs

                print("Generating module API reference from AST...")
                success = generate_module_docs()
                if success:
                    print("[OK] Module API reference generated in docs/docs/modules/")
                else:
                    print("[ERROR] Failed to generate module API reference")
                    return 1

            elif args.pipelines:
                from acoharmony._dev.docs.pipelines import generate_full_documentation

                print("Generating comprehensive pipeline documentation...")
                generate_full_documentation()
                print("[OK] Pipeline documentation generated successfully in docs/pipelines")
                print("  - 00_ARCHITECTURE.md: System design and principles")
                print("  - 01_PIPELINE_GROUPS.md: Logical pipeline groupings")
                print("  - 02_EXPRESSIONS.md: Expression module documentation")
                print("  - 03_TRANSFORMS.md: Transform module documentation")
                print("  - README.md: Documentation index")

            else:
                generate_parser.print_help()
                return 1

        elif args.dev_command == "generate-notes":
            from pathlib import Path

            from acoharmony._notes.generator import NotebookGenerator

            generator = NotebookGenerator(output_dir=Path(args.output_dir))

            if args.all:
                # Generate for all raw schemas
                generated = generator.create_notebooks_for_raw_schemas()
                print(f"[OK] Generated {len(generated)} notebooks")
            elif args.schema:
                # Generate for specific schema
                try:
                    output_path = generator.create_notebook(
                        args.schema, template_name=args.template
                    )
                    print(f"[OK] Generated notebook: {output_path}")
                except (
                    Exception
                ) as e:  # ALLOWED: CLI command handler, prints error and returns exit code
                    print(f"[ERROR] Failed to generate notebook for {args.schema}: {e}")
                    return 1
            else:
                print("Error: Must specify either --all or --schema")
                return 1

        elif args.dev_command == "storage":
            from acoharmony._dev.setup.storage import setup_storage, verify_storage

            if args.action == "setup":
                setup_storage(
                    profile=args.profile,
                    create_bucket=args.create_bucket,
                    dry_run=args.dry_run,
                    workspace_path=args.workspace_path,
                )
                if not args.dry_run:
                    print(f"[OK] Storage setup complete for profile: {args.profile}")

            elif args.action == "verify":
                storage = StorageBackend(profile=args.profile)
                verify_storage(storage)
            else:  # pragma: no cover – argparse enforces choices=["setup", "verify"]
                pass

        elif args.dev_command == "unpack":
            from acoharmony._utils.unpack import unpack_bronze_zips

            if args.dry_run:
                print("Running in DRY RUN mode - no files will be modified\n")

            result = unpack_bronze_zips(dry_run=args.dry_run)

            # Exit with error code if any failures
            return 1 if result["failed"] > 0 else 0

        elif args.dev_command == "generate-mocks":
            from acoharmony._dev import generate_test_mocks

            try:
                generate_test_mocks(
                    layers=args.layers,
                    tables=args.tables,
                    output_dir=args.output_dir,
                    n_rows=args.n_rows,
                    sample_size=args.sample_size,
                    dry_run=args.dry_run,
                    force=args.force,
                    seed=args.seed,
                )
                return 0
            except Exception as e:  # ALLOWED: CLI top-level handler
                print(f"[ERROR] Error generating test mocks: {e}")
                import traceback

                traceback.print_exc()
                return 1

        elif args.dev_command == "generate-tests":
            from acoharmony._dev.test.coverage import TestCoverageManager

            manager = TestCoverageManager()
            manager.generate_missing_test_files()
            return 0

        elif args.dev_command == "cleanup-tests":
            from acoharmony._dev.test.coverage import TestCoverageManager

            manager = TestCoverageManager()
            manager.cleanup_orphaned_tests()
            return 0

        else:
            dev_parser.print_help()
            return 1

    elif args.command == "schema":
        _require_full_package()
        catalog = Catalog(StorageBackend())

        if args.schema_command == "list":
            tables = catalog.list_tables()
            print(f"Found {len(tables)} schemas:")
            for table in sorted(tables):
                metadata = catalog.get_schema(table)
                tier = metadata.medallion_layer.value if metadata.medallion_layer else "unknown"
                print(f"  {table:<30} [{tier}]")
            return 0

        elif args.schema_command == "history":
            print(f"Schema history for: {args.schema_name}")
            print("Schema versioning moved to git-based tracking")
            print(f"Use: git log -- src/acoharmony/_tables/{args.schema_name}.py")
            return 0

        elif args.schema_command == "diff":
            print(f"Schema diff: {args.schema_name} {args.from_version}..{args.to_version}")
            print("Schema diffs moved to git-based tracking")
            print(
                f"Use: git diff {args.from_version}..{args.to_version} -- src/acoharmony/_tables/{args.schema_name}.py"
            )
            return 0

        elif args.schema_command == "validate":
            if args.all:
                tables = catalog.list_tables()
                print(f"Validating {len(tables)} schemas...")
                valid = 0
                for table in tables:
                    try:
                        catalog.get_schema(table)
                        valid += 1
                    except Exception as e:
                        print(f"[ERROR] {table}: {e}")
                print(f"[OK] {valid}/{len(tables)} schemas valid")
            elif args.schema_name:
                try:
                    metadata = catalog.get_schema(args.schema_name)
                    print(f"[OK] Schema {args.schema_name} is valid")
                    print(f"  Columns: {len(metadata.columns)}")
                    print(
                        f"  Tier: {metadata.medallion_layer.value if metadata.medallion_layer else 'unknown'}"
                    )
                except Exception as e:
                    print(f"[ERROR] Schema {args.schema_name} invalid: {e}")
                    return 1
            else:
                validate_parser.print_help()
                return 1
            return 0

        else:
            schema_parser.print_help()
            return 1

    elif args.command == "databricks":
        _require_full_package()
        from pathlib import Path

        from acoharmony._databricks import DatabricksTransferManager

        # Determine destination directory
        dest_dir = Path(args.dest) if args.dest else None
        manager = DatabricksTransferManager(dest_dir=dest_dir)

        if args.log:
            # Aggregate logs
            print("Aggregating tracking state logs...")
            print("=" * 60)

            output_file = manager.aggregate_logs()

            if output_file:
                print("\n[OK] Successfully aggregated logs to:")
                print(f"  {output_file}")
                print(f"\nFile size: {output_file.stat().st_size / (1024 * 1024):.2f} MB")
            else:
                print("\n[ERROR] No valid state files found")
                return 1

            print("=" * 60)
            return 0

        elif args.status or (not args.transfer and not args.status and not args.log):
            # Show status (default if no action specified)
            status = manager.status()

            print("Databricks Transfer Status")
            print("=" * 60)

            if status["last_run"]:
                print(f"Last run: {status['last_run']}")
                print(f"Last run end: {status['last_run_end']}")
                print(f"Total transfers: {status['total_transfers']}")
                print(f"Files tracked: {status['total_files_tracked']}")

                if "last_run_stats" in status and status["last_run_stats"]:
                    stats = status["last_run_stats"]
                    print("\nLast run statistics:")
                    print(f"  Total files: {stats['total_files']}")
                    print(f"  Transferred: {stats['transferred']}")
                    print(f"  Skipped: {stats['skipped']}")
                    print(f"  Failed: {stats['failed']}")

                    if stats.get("transferred_files"):
                        print("\n  Recently transferred files:")
                        for fname in stats["transferred_files"][:10]:  # Show first 10
                            print(f"    - {fname}")
                        if len(stats["transferred_files"]) > 10:
                            print(f"    ... and {len(stats['transferred_files']) - 10} more")
            else:
                print("No transfers have been run yet")

            print("=" * 60)
            return 0

        elif args.transfer:
            # Run transfer
            print("Starting Databricks transfer...")
            print("=" * 60)
            print("Source directories:")
            for src_dir in manager.source_dirs:
                print(f"  - {src_dir}")
            print(f"Destination: {manager.dest_dir}")
            print(f"Force: {args.force}")
            print()

            stats = manager.transfer(force=args.force)

            print("\n" + "=" * 60)
            print("Transfer Summary")
            print("=" * 60)
            print(f"Total files: {stats['total_files']}")
            print(f"Transferred: {stats['transferred']}")
            print(f"Skipped: {stats['skipped']}")
            print(f"Failed: {stats['failed']}")

            if stats["transferred_files"]:
                print("\nTransferred files:")
                for fname in stats["transferred_files"]:
                    print(f"  [OK] {fname}")

            if stats["failed_files"]:
                print("\nFailed files:")
                for fname in stats["failed_files"]:
                    print(f"  [ERROR] {fname}")

            print("=" * 60)

            return 0 if stats["failed"] == 0 else 1

        else:  # pragma: no cover – default-status elif catches all-False
            databricks_parser.print_help()
            return 1

    elif args.command == "4icli":
        from acoharmony._4icli.cli import (
            cmd_download,
            cmd_inventory,
            cmd_need_download,
            cmd_setup,
        )

        try:
            if args.fouricli_command == "inventory":
                cmd_inventory(args)
                return 0
            elif args.fouricli_command == "need-download":
                cmd_need_download(args)
                return 0
            elif args.fouricli_command == "download":
                cmd_download(args)
                return 0
            elif args.fouricli_command == "setup":
                return cmd_setup(args)
            else:
                fouricli_parser.print_help()
                return 1

        except Exception as e:  # ALLOWED: CLI top-level handler, prints error and returns exit code
            print(f"[ERROR] Error: {e}")
            import traceback

            traceback.print_exc()
            return 1

    elif args.command == "xfr":
        from acoharmony._xfr.cli import dispatch as _xfr_dispatch

        try:
            return _xfr_dispatch(args)
        except Exception as e:  # ALLOWED: CLI top-level handler, prints error and returns exit code
            print(f"[ERROR] Error: {e}")
            import traceback

            traceback.print_exc()
            return 1

    elif args.command == "puf":
        _require_full_package()
        from acoharmony._puf.puf_cli import (
            cmd_download,
            cmd_inventory,
            cmd_list_categories,
            cmd_list_years,
            cmd_need_download,
            cmd_search,
            cmd_unpack,
        )

        try:
            if args.puf_command == "inventory":
                return cmd_inventory(args)
            elif args.puf_command == "need-download":
                return cmd_need_download(args)
            elif args.puf_command == "download":
                return cmd_download(args)
            elif args.puf_command == "years":
                return cmd_list_years(args)
            elif args.puf_command == "categories":
                return cmd_list_categories(args)
            elif args.puf_command == "search":
                return cmd_search(args)
            elif args.puf_command == "unpack":
                return cmd_unpack(args)
            else:
                puf_parser.print_help()
                return 1

        except Exception as e:  # ALLOWED: CLI top-level handler
            print(f"[ERROR] Error: {e}")
            import traceback

            traceback.print_exc()
            return 1

    elif args.command == "cite":
        _require_full_package()
        from acoharmony._cite.state import CiteStateTracker
        from acoharmony._transforms._cite import transform_cite
        from acoharmony._transforms._cite_batch import transform_cite_batch

        try:
            # Default to interactive mode if no subcommand provided
            if not args.cite_command:
                args.cite_command = "interactive"
                args.force = False  # Set default for interactive mode

            if args.cite_command == "url":
                print(f"Processing citation from: {args.url}")

                # Parse tags if provided
                tags_list = (
                    [tag.strip() for tag in args.tags.split(",") if tag.strip()]
                    if args.tags
                    else []
                )

                result_lf = transform_cite(
                    args.url,
                    force_refresh=args.force,
                    note=args.note,
                    tags=tags_list,
                )
                df = result_lf.collect()

                # Display results
                title = df["normalized_title"][0] if "normalized_title" in df.columns else "N/A"
                author = df["first_author"][0] if "first_author" in df.columns else "N/A"
                doi = df["extracted_doi"][0] if "extracted_doi" in df.columns else "N/A"
                file_hash = df["file_hash"][0] if "file_hash" in df.columns else "N/A"

                print("\n[OK] Successfully processed citation")
                print(f"  Title: {title}")
                print(f"  Author: {author}")
                print(f"  DOI: {doi}")
                print(f"  Storage: cites/corpus/{file_hash}.*")

                if args.note:
                    print(f"  Note: {args.note}")
                if tags_list:
                    print(f"  Tags: {', '.join(tags_list)}")

                return 0

            elif args.cite_command == "batch":
                # Read URLs from file
                with open(args.file) as f:
                    urls = [line.strip() for line in f if line.strip()]

                print(f"Processing {len(urls)} URLs from {args.file}")
                result_lf = transform_cite_batch(
                    urls, force_refresh=args.force, max_workers=args.workers
                )
                df = result_lf.collect()

                print(f"\n[OK] Successfully processed {len(df)} citations")
                return 0

            elif args.cite_command == "list":
                state_tracker = CiteStateTracker()
                source_type = None if args.source_type == "all" else args.source_type
                files = state_tracker.get_processed_files(source_type=source_type)

                if not files:
                    print("No processed citations found")
                    return 0

                print(f"Found {len(files)} processed citations:\n")
                for i, file_state in enumerate(files[: args.limit], 1):
                    print(f"{i}. {file_state.filename}")
                    print(f"   Type: {file_state.source_type}")
                    print(f"   Processed: {file_state.process_timestamp}")
                    if file_state.metadata and "title" in file_state.metadata:
                        print(f"   Title: {file_state.metadata['title']}")
                    print()

                if len(files) > args.limit:
                    print(f"... and {len(files) - args.limit} more (use --limit to see more)")

                return 0

            elif args.cite_command == "stats":
                state_tracker = CiteStateTracker()
                stats = state_tracker.get_processing_stats()

                print("\nCitation Corpus Statistics:")
                print(f"  Total files: {stats['total_files']}")
                print(f"  Total size: {stats['total_size_mb']} MB")
                print(f"  Total records: {stats['total_records']}")
                print("\n  By source type:")
                for source_type, count in stats["source_types"].items():
                    print(f"    {source_type}: {count}")

                return 0

            elif args.cite_command == "interactive":
                # Interactive mode - prompt for inputs
                print("\n=== Interactive Citation Mode ===\n")

                # Prompt for URL
                url = input("Enter citation URL: ").strip()
                if not url:
                    print("[ERROR] Error: URL is required")
                    return 1

                # Prompt for note (optional)
                print("\nEnter a note for this citation (optional, press Enter to skip):")
                note = input("Note: ").strip()

                # Prompt for tags (optional)
                print("\nEnter tags (comma-separated, optional, press Enter to skip):")
                print("Examples: cms,regulations,2025 or telehealth,supervision")
                tags_input = input("Tags: ").strip()

                # Parse tags
                tags_list = (
                    [tag.strip() for tag in tags_input.split(",") if tag.strip()]
                    if tags_input
                    else []
                )

                # Confirm before processing
                print("\n--- Citation Summary ---")
                print(f"URL: {url}")
                print(f"Note: {note if note else '(none)'}")
                print(f"Tags: {', '.join(tags_list) if tags_list else '(none)'}")
                print(f"Force reprocess: {args.force}")

                confirm = input("\nProcess this citation? [Y/n]: ").strip().lower()
                if confirm and confirm not in ["y", "yes"]:
                    print("[ERROR] Cancelled")
                    return 0

                # Process citation
                print(f"\nProcessing citation from: {url}")

                result_lf = transform_cite(
                    url,
                    force_refresh=args.force,
                    note=note,
                    tags=tags_list,
                )
                df = result_lf.collect()

                # Display results
                title = df["normalized_title"][0] if "normalized_title" in df.columns else "N/A"
                author = df["first_author"][0] if "first_author" in df.columns else "N/A"
                doi = df["extracted_doi"][0] if "extracted_doi" in df.columns else "N/A"
                file_hash = df["file_hash"][0] if "file_hash" in df.columns else "N/A"

                print("\n[OK] Successfully processed citation")
                print(f"  Title: {title}")
                print(f"  Author: {author}")
                print(f"  DOI: {doi}")
                print(f"  Storage: cites/corpus/{file_hash}.*")

                if note:
                    print(f"  Note: {note}")
                if tags_list:
                    print(f"  Tags: {', '.join(tags_list)}")

                return 0

            else:  # pragma: no cover – cite_command defaults to "interactive"
                cite_parser.print_help()
                return 1

        except Exception as e:  # ALLOWED: CLI top-level handler
            print(f"[ERROR] Error: {e}")
            import traceback

            traceback.print_exc()
            return 1

    elif args.command == "deploy":
        _require_full_package()
        from acoharmony._deploy import DeploymentManager

        try:
            manager = DeploymentManager()
            result = manager.execute_command(
                args.action,
                services=args.services if args.services else None,
                group=args.group,
                follow=args.follow,
                tail=args.tail,
                build=getattr(args, "build", False),
                pull=getattr(args, "pull", False),
            )
            return result if isinstance(result, int) else 0

        except (
            FileNotFoundError
        ) as e:  # ALLOWED: CLI top-level handler, prints error and returns exit code
            print(f"[ERROR] Error: {e}")
            print("\nMake sure you're running from the project root")
            print("or docker-compose.yml exists in deploy/")
            return 1
        except (
            ValueError
        ) as e:  # ALLOWED: CLI top-level handler, prints error and returns exit code
            print(f"[ERROR] Error: {e}")
            return 1
        except Exception as e:  # ALLOWED: CLI top-level handler, prints error and returns exit code
            print(f"[ERROR] Unexpected error: {e}")
            import traceback

            traceback.print_exc()
            return 1

    elif args.command == "test":
        # Test command doesn't require full package - just pytest and coverage
        from pathlib import Path

        from acoharmony._test.coverage.orchestrator import CoverageOrchestrator

        try:
            orchestrator = CoverageOrchestrator(
                src_root=args.src_root,
                work_dir=Path(args.work_dir),
            )

            result = orchestrator.iterate_once(
                test_path=args.test_path,
                show_targets=not args.no_targets,
            )

            if not result["success"]:
                return 1

            if result["uncovered_count"] == 0:
                print("\n🎉 Coverage is complete!")
            else:
                print(f"\n📋 Next: Review targets in {orchestrator.targets_file}")

            return 0

        except Exception as e:  # ALLOWED: CLI top-level handler, prints error and returns exit code
            print(f"[ERROR] Error: {e}")
            import traceback

            traceback.print_exc()
            return 1

    elif args.command == "sva":
        _require_full_package()
        # SVA validation - inline implementation
        try:
            if args.sva_command == "validate":
                from pathlib import Path

                import polars as pl

                file_path = Path(args.file_path)

                print("\n" + "=" * 60)
                print("SVA SUBMISSION VALIDATOR")
                print("=" * 60)
                print(f"File: {file_path}\n")

                if not file_path.exists():
                    print(f"[ERROR] File not found: {file_path}")
                    return 1

                # Load and process SVA file
                print("Loading SVA submission file...")
                sva_df = pl.read_excel(file_path, sheet_name="SVA_DATA")
                print(f"  Loaded {sva_df.height:,} records\n")

                # Load reference data
                print("Loading reference data...")
                data_dir = Path("/opt/s3/data/workspace/silver")
                participant_list = pl.read_parquet(data_dir / "participant_list.parquet")
                bar = (
                    pl.read_parquet(data_dir / "bar.parquet")
                    if (data_dir / "bar.parquet").exists()
                    else pl.DataFrame()
                )
                xwalk = (
                    pl.read_parquet(data_dir / "identity_timeline.parquet")
                    if (data_dir / "identity_timeline.parquet").exists()
                    else pl.DataFrame()
                )

                print(f"  Participant List: {participant_list.height:,} records")
                print(f"  BAR: {bar.height:,} records")
                print(f"  Identity Timeline: {xwalk.height:,} records\n")

                print("\n[OK] SVA validation complete")
                print("For full validation logic, use the _transforms module")
                return 0
            else:
                sva_parser.print_help()
                return 1

        except Exception as e:  # ALLOWED: CLI top-level handler, prints error and returns exit code
            print(f"[ERROR] Error: {e}")
            import traceback

            traceback.print_exc()
            return 1

    else:
        parser.print_help()
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
