# © 2025 HarmonyCares
# All rights reserved.

"""
Storage setup functionality for ACO Harmony.

This module:
1. Creates the /data directory structure
2. Sets up symlinks for local profile (to workspace)
3. Creates RustFS buckets for staging profile
4. Verifies storage configuration

"""

import json
import os
import subprocess
from pathlib import Path

from acoharmony._log.writer import LogWriter
from acoharmony._store import StorageBackend

log = LogWriter("storage_setup")


def create_local_structure(base_path: Path, symlink_to: Path | None = None) -> None:
    """
    Create local directory structure.

        Args:
            base_path: Base path for data directories
            symlink_to: Optional path to create symlink to (for workspace)
    """
    subdirs = ["bronze", "silver", "gold", "tmp", "logs"]

    if symlink_to and symlink_to.exists():
        # Create symlinks to workspace directories
        log.logger.info(f"Creating symlinks from {base_path} to {symlink_to}")

        # Create base data directory if it doesn't exist
        base_path.mkdir(parents=True, exist_ok=True)

        for subdir in subdirs:
            source = symlink_to / subdir
            target = base_path / subdir

            # Create source directory if it doesn't exist
            source.mkdir(parents=True, exist_ok=True)

            # Remove existing symlink or directory
            if target.is_symlink():
                target.unlink()
            elif target.exists():
                log.logger.warning(f"Directory exists, not replacing: {target}")
                continue

            # Create symlink
            target.symlink_to(source)
            log.logger.info(f"  Created symlink: {target} -> {source}")

        # Also create symlink for docs directory (use existing docs/ not docs_site/)
        docs_source = Path("/home/care/acoharmony/docs")
        docs_target = Path("/home/care/docs")

        # Only create symlink if source exists
        if not docs_source.exists():
            log.logger.warning(f"  Docs source directory does not exist: {docs_source}")
            return

        # Remove existing docs symlink or directory if needed
        if docs_target.is_symlink():
            docs_target.unlink()
            log.logger.info(f"  Removed existing docs symlink: {docs_target}")
        elif docs_target.exists() and not docs_target.is_dir():
            log.logger.warning(f"  Cannot replace non-directory at {docs_target}")

        # Create the docs symlink
        if not docs_target.exists():
            docs_target.symlink_to(docs_source)
            log.logger.info(f"  Created docs symlink: {docs_target} -> {docs_source}")
    else:
        # Create actual directories
        log.logger.info(f"Creating directory structure at {base_path}")

        for subdir in subdirs:
            dir_path = base_path / subdir
            dir_path.mkdir(parents=True, exist_ok=True)
            log.logger.info(f"  Created directory: {dir_path}")


def setup_s3api_bucket(config: StorageBackend) -> None:
    """
    Setup RustFS bucket for staging environment.

        Args:
            config: Storage configuration
    """
    storage_config = config.config.get("storage", {})

    endpoint = storage_config.get("endpoint", "http://s3api:10001")
    bucket = storage_config.get("bucket", "aco")
    access_key = os.getenv("S3_ACCESS_KEY", storage_config.get("access_key", "s3apiadmin"))
    secret_key = os.getenv("S3_SECRET_KEY", storage_config.get("secret_key", "s3apiadmin"))

    log.logger.info(f"Setting up RustFS bucket: {bucket}")
    log.logger.info(f"  Endpoint: {endpoint}")

    # Try to use mc (RustFS client) if available
    try:
        # Configure mc alias
        mc_alias = "aco-staging"
        cmd = ["mc", "alias", "set", mc_alias, endpoint, access_key, secret_key]
        subprocess.run(cmd, check=True, capture_output=True, text=True)
        log.logger.info(f"  Configured mc alias: {mc_alias}")

        # Create bucket if it doesn't exist
        cmd = ["mc", "mb", f"{mc_alias}/{bucket}", "--ignore-existing"]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            log.logger.info(f"  Created bucket: {bucket}")
        else:
            log.logger.info(f"  Bucket already exists: {bucket}")

        # Create directory structure in bucket
        subdirs = ["bronze", "silver", "gold", "tmp", "logs"]
        for subdir in subdirs:
            # Create a .keep file to ensure directory exists
            cmd = ["mc", "cp", "/dev/null", f"{mc_alias}/{bucket}/{subdir}/.keep"]
            subprocess.run(cmd, capture_output=True, text=True)
            log.logger.info(f"    Created bucket directory: {subdir}/")

    except FileNotFoundError:  # ALLOWED: Dev tool setup - logs instructions, user needs to install mc client
        log.logger.warning("RustFS client (mc) not found. Install it with:")
        log.logger.warning("  wget https://dl.min.io/client/mc/release/linux-amd64/mc")
        log.logger.warning("  chmod +x mc")
        log.logger.warning("  sudo mv mc /usr/local/bin/")

        # Provide Python alternative using boto3
        log.logger.info("\nAlternatively, using boto3:")
        """
        from botocore.client import Config

        s3 = boto3.client(
            's3',
            endpoint_url='{endpoint}',
            aws_access_key_id='{access_key}',
            aws_secret_access_key='{secret_key}',
            region_name='us-east-1'
        )

        # Create bucket
        s3.create_bucket(Bucket='{bucket}')

        # Create directories
        for subdir in ['bronze', 'silver', 'gold', 'tmp', 'logs']:
            s3.put_object(Bucket='{bucket}', Key=f'{{subdir}}/.keep', Body=b'')
        """
    except subprocess.CalledProcessError as e:  # ALLOWED: Dev tool setup - logs error, user needs to fix RustFS configuration
        log.logger.error(f"Error setting up RustFS: {e}")
        if e.stderr:
            log.logger.error(f"  {e.stderr}")


def setup_databricks_catalog(config: StorageBackend) -> None:
    """
    Setup Databricks Unity Catalog structure.

        Args:
            config: Storage configuration
    """
    storage_config = config.config.get("storage", {})

    catalog = storage_config.get("catalog", "main")
    schema = storage_config.get("schema", "aco_harmony")

    log.logger.info("Databricks Unity Catalog setup:")
    log.logger.info(f"  Catalog: {catalog}")
    log.logger.info(f"  Schema: {schema}")

    """
    -- Run these commands in Databricks SQL:

    -- Create schema if not exists
    CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}
    COMMENT 'ACO Harmony data processing';

    -- Create volume for data files
    CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.data
    COMMENT 'ACO Harmony data files';

    -- Create tables for each tier
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.raw_files (
        file_name STRING,
        file_path STRING,
        file_size BIGINT,
        upload_time TIMESTAMP,
        processed BOOLEAN
    ) USING DELTA
    COMMENT 'Raw file inventory';

    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.processed_data (
        -- Define your schema here
    ) USING DELTA
    COMMENT 'Processed ACO data';

    -- Grant permissions
    GRANT ALL PRIVILEGES ON SCHEMA {catalog}.{schema} TO `aco-harmony-users`;
    GRANT ALL PRIVILEGES ON VOLUME {catalog}.{schema}.data TO `aco-harmony-users`;
    """


def verify_storage(config: StorageBackend) -> None:
    """
    Verify storage configuration and access.

        Args:
            config: Storage configuration
    """
    log.logger.info("Verifying storage configuration...")

    backend = config.get_storage_type()
    log.logger.info(f"  Backend: {backend}")
    log.logger.info(f"  Environment: {config.get_environment()}")

    # Test path access
    try:
        for tier in ["bronze", "silver", "gold", "tmp", "logs"]:
            path = config.get_path(tier)
            log.logger.info(f"  {tier}: {path}")

            if backend == "local" and isinstance(path, Path):
                if path.exists():
                    log.logger.info("    [OK] Path exists")
                else:
                    log.logger.warning("    [ERROR] Path does not exist")
    except Exception as e:  # ALLOWED: Dev tool verification - log error, continue with remaining checks
        log.logger.error(f"Error accessing paths: {e}")

    # Show connection parameters (without secrets)
    params = config.get_connection_params()
    safe_params = {}
    for k, v in params.items():
        if v is not None:
            # Hide sensitive values
            if "key" in k.lower() or "token" in k.lower():
                safe_params[k] = "***"
            # Convert Path objects to strings
            elif isinstance(v, Path):
                safe_params[k] = str(v)
            else:
                safe_params[k] = v
    log.logger.info(f"  Connection params: {json.dumps(safe_params, indent=4)}")


def setup_storage(
    profile: str = "local",
    create_bucket: bool = False,
    dry_run: bool = False,
    workspace_path: str = "/opt/s3/data/workspace",
) -> None:
    """
    Setup ACO Harmony storage structure.

        Args:
            profile: Configuration profile to use (local, dev, staging, prod)
            create_bucket: Create RustFS/S3 bucket (for staging/prod)
            dry_run: Show what would be done without making changes
            workspace_path: Path to workspace directory for symlinks (local profile)
    """
    log.logger.info(f"Setting up storage for profile: {profile}")

    # Load configuration
    config = StorageBackend(profile=profile)
    backend = config.get_storage_type()

    if dry_run:
        log.logger.info("DRY RUN - No changes will be made")
        verify_storage(config)
        return

    # Setup based on backend type
    if backend == "local":
        if profile == "local":
            # For local profile, create symlinks to workspace
            data_path = Path("/home/care/acoharmony/data")
            workspace_path_obj = Path(workspace_path)
            create_local_structure(data_path, symlink_to=workspace_path_obj)
        else:
            # For dev profile, create actual directories
            data_path = config.get_data_path()
            if isinstance(data_path, Path):
                create_local_structure(data_path)

    elif backend == "s3api" and create_bucket:
        setup_s3api_bucket(config)

    elif backend == "databricks":
        setup_databricks_catalog(config)

    elif backend == "s3" and create_bucket:
        log.logger.info("S3 bucket creation not implemented - use AWS CLI or console")

    # Verify the setup
    verify_storage(config)

    log.logger.info("Storage setup complete!")
