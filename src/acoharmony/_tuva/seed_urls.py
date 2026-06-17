# © 2025 HarmonyCares
# All rights reserved.

"""Extract and display S3 bucket URLs from Tuva seed definitions."""

from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path

import yaml

DEFAULT_TUVA_PROJECT_DIR = (
    Path(__file__).parent
    / "_depends"
    / "repos"
    / "cclf_connector"
    / "dbt_packages"
    / "the_tuva_project"
)


def parse_seed_definitions(tuva_project_dir: Path) -> list[dict]:
    """Parse dbt_project.yml to extract seed definitions."""
    dbt_project_yml = tuva_project_dir / "dbt_project.yml"

    with open(dbt_project_yml) as f:
        config = yaml.safe_load(f)

    # Extract custom_bucket_name (defaults to tuva-public-resources)
    bucket = config.get("vars", {}).get("custom_bucket_name", "tuva-public-resources")

    seeds = []
    seed_config = config.get("seeds", {}).get("the_tuva_project", {})

    def extract_seeds_recursive(parent_schema, config_dict):
        """Recursively extract seed definitions from nested structure."""
        for key, value in config_dict.items():
            if not isinstance(value, dict):
                continue

            # Check if this has a post-hook (it's a seed definition)
            post_hook = value.get("+post-hook")
            if post_hook and "load_seed" in post_hook:
                # Extract path and filename from post-hook
                parts = post_hook.split("'")
                s3_path = None
                csv_filename = None
                for part in parts:
                    if part.startswith("/"):
                        s3_path = part
                    elif part.endswith(".csv"):
                        csv_filename = part

                if s3_path and csv_filename:
                    # Determine schema from parent_schema
                    schema = parent_schema if parent_schema else key.split("__")[0]

                    # Table name: remove all prefixes
                    if "__" in key:
                        table = key.split("__", 1)[1]
                    else:
                        table = key

                    seeds.append(
                        {
                            "schema": schema,
                            "table": table,
                            "dbt_name": key,
                            "s3_bucket": bucket,
                            "s3_path": s3_path.lstrip("/"),
                            "csv_filename": csv_filename,
                            "full_s3_uri": f"s3://{bucket}/{s3_path.lstrip('/')}/{csv_filename}",
                        }
                    )
            else:
                # Recurse into nested structure
                next_parent = f"{parent_schema}_{key}" if parent_schema else key
                extract_seeds_recursive(next_parent, value)

    # Start extraction from top level
    for schema_name, schema_config in seed_config.items():
        extract_seeds_recursive(schema_name, schema_config)

    return sorted(seeds, key=lambda x: (x["schema"], x["table"]))


def add_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--json", action="store_true", help="Output as JSON array")
    parser.add_argument("--csv", action="store_true", help="Output as CSV file")
    parser.add_argument("--output", type=str, help="Output CSV file path")
    parser.add_argument("--https", action="store_true", help="Convert S3 URIs to HTTPS URLs")
    parser.add_argument(
        "--tuva-project-dir",
        type=Path,
        default=DEFAULT_TUVA_PROJECT_DIR,
        help=f"Tuva dbt project directory. Default: {DEFAULT_TUVA_PROJECT_DIR}",
    )


def compressed_seed_url(seed: dict) -> str:
    return (
        f"https://{seed['s3_bucket']}.s3.amazonaws.com/"
        f"{seed['s3_path']}/{seed['csv_filename']}_0_0_0.csv.gz"
    )


def print_seed_urls(args: argparse.Namespace) -> int:
    seeds = parse_seed_definitions(args.tuva_project_dir)

    if args.https:
        urls = [compressed_seed_url(seed) for seed in seeds]
        if args.json:
            print(json.dumps(urls, indent=2))
        elif args.csv:
            output_file = args.output or "bucket_urls.csv"
            with open(output_file, "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["URL"])
                for url in urls:
                    writer.writerow([url])
            print(f"CSV written to {output_file}")
        else:
            for url in urls:
                print(url)
        return 0

    if args.json:
        output = [
            {
                "schema": seed["schema"],
                "table": seed["table"],
                "dbt_name": seed["dbt_name"],
                "s3_uri": seed["full_s3_uri"],
                "https_url": compressed_seed_url(seed),
            }
            for seed in seeds
        ]
        print(json.dumps(output, indent=2))
    elif args.csv:
        output_file = args.output or "bucket_urls.csv"
        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["Schema", "Table", "DBT Name", "S3 URI", "HTTPS URL"])
            for seed in seeds:
                writer.writerow(
                    [
                        seed["schema"],
                        seed["table"],
                        seed["dbt_name"],
                        seed["full_s3_uri"],
                        compressed_seed_url(seed),
                    ]
                )
        print(f"CSV written to {output_file}")
    else:
        print(f"Found {len(seeds)} seeds from {args.tuva_project_dir}\n")
        for i, seed in enumerate(seeds, 1):
            print(f"{i}. {seed['schema']}.{seed['table']}")
            print(f"   DBT Name: {seed['dbt_name']}")
            print(f"   S3 URI:   {seed['full_s3_uri']}")
            print(f"   HTTPS:    {compressed_seed_url(seed)}")
            print()
    return 0


def cmd_seed_urls(args: argparse.Namespace) -> int:
    return print_seed_urls(args)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract all S3 bucket URLs from Tuva seed definitions"
    )
    add_arguments(parser)
    args = parser.parse_args()
    return print_seed_urls(args)


if __name__ == "__main__":
    raise SystemExit(main())
