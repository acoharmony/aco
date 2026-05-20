#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""
Setup script for Tuva Health dependencies.

This script clones/updates external Tuva repositories into the repos/ directory.
These repositories are not tracked in git (see .gitignore).

"""

import argparse
import shutil
import subprocess
from pathlib import Path

import yaml


def setup_dependencies(update: bool = False):
    """Clone or update Tuva dependencies."""
    depends_dir = Path(__file__).parent
    repos_dir = depends_dir / "repos"
    repos_dir.mkdir(exist_ok=True)

    # Load repository manifest
    with open(depends_dir / "repositories.yml") as f:
        config = yaml.safe_load(f)

    for repo in config["repositories"]:
        repo_path = repos_dir / repo["name"]

        # An empty directory (or one without .git) is treated as missing — a
        # half-initialized clone fooled the previous check into reporting
        # "already exists" while the reference_data pipeline crashed looking
        # for dbt_project.yml.
        is_real_clone = repo_path.is_dir() and (repo_path / ".git").exists()

        if is_real_clone:
            if update:
                print(f"Updating {repo['name']}...")
                subprocess.run(["git", "pull"], cwd=repo_path, check=True)
            else:
                print(f"[OK] {repo['name']} already exists (use --update to refresh)")
        else:
            if repo_path.exists():
                # Empty/incomplete directory — git clone refuses to write into
                # a non-empty path, and an empty path is fine to remove first.
                print(f"Removing incomplete {repo['name']} dir before reclone...")
                shutil.rmtree(repo_path)
            print(f"Cloning {repo['name']}...")
            subprocess.run(
                [
                    "git",
                    "clone",
                    "--depth",
                    "1",
                    "--branch",
                    repo["branch"],
                    repo["url"],
                    str(repo_path),
                ],
                check=True,
            )
            print(f"[OK] Cloned {repo['name']}")

    print("\n[SUCCESS] All dependencies ready!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup Tuva Health dependencies")
    parser.add_argument("--update", action="store_true", help="Update existing repositories")
    args = parser.parse_args()

    setup_dependencies(update=args.update)
