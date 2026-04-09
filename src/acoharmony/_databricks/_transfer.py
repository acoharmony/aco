# © 2025 HarmonyCares
# All rights reserved.

"""
Databricks transfer manager for converting and tracking parquet file transfers.

Handles conversion from ZSTD to SNAPPY compression and tracks which files
have been transferred to ensure only changed files are copied.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)


class DatabricksTransferManager:
    """
    Manages transfer of parquet files from silver/gold layers to Databricks-compatible format.

    Tracks file changes using modification time and size, converts ZSTD compression
    to SNAPPY for Databricks compatibility, and maintains transfer history.
    """

    def __init__(
        self,
        source_dirs: list[Path] | None = None,
        dest_dir: Path | None = None,
        tracking_dir: Path | None = None,
    ):
        """
        Initialize the transfer manager.

        Args:
            source_dirs: List of source directories (default: [silver, gold])
            dest_dir: Destination directory (default: /home/care/kcorwin/Downloads)
            tracking_dir: Directory for tracking state (default: /opt/s3/data/workspace/logs/databricks)
        """
        self.source_dirs = source_dirs or [
            Path("/opt/s3/data/workspace/silver"),
            Path("/opt/s3/data/workspace/gold"),
        ]
        self.dest_dir = dest_dir or Path("/home/care/kcorwin/Downloads")
        self.tracking_dir = tracking_dir or Path("/opt/s3/data/workspace/logs/databricks")

        # Ensure directories exist
        self.dest_dir.mkdir(parents=True, exist_ok=True)
        self.tracking_dir.mkdir(parents=True, exist_ok=True)

        self.state_file = self.tracking_dir / "transfer_state.json"
        self.state = self._load_state()

    def _load_state(self) -> dict[str, Any]:
        """Load transfer state from JSON file."""
        if self.state_file.exists():
            try:
                with open(self.state_file) as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load state file: {e}")
                return {"files": {}, "last_run": None, "total_transfers": 0}
        return {"files": {}, "last_run": None, "total_transfers": 0}

    def _save_state(self) -> None:
        """Save transfer state to JSON file."""
        try:
            with open(self.state_file, "w") as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save state file: {e}")

    def _get_file_signature(self, file_path: Path) -> dict[str, Any]:
        """
        Get signature of file for change detection.

        Uses modification time and size for efficient change detection.
        """
        stat = file_path.stat()
        return {
            "mtime": stat.st_mtime,
            "size": stat.st_size,
            "path": str(file_path),
        }

    def _has_file_changed(self, file_path: Path) -> bool:
        """Check if file has changed since last transfer."""
        current_sig = self._get_file_signature(file_path)
        file_key = str(file_path)

        if file_key not in self.state["files"]:
            return True

        prev_sig = self.state["files"][file_key]
        return (
            current_sig["mtime"] != prev_sig["mtime"]
            or current_sig["size"] != prev_sig["size"]
        )

    def _get_parquet_codec(self, file_path: Path) -> str:
        """Get compression codec of parquet file."""
        try:
            pf = pq.ParquetFile(file_path)
            if pf.metadata.num_row_groups > 0:
                return pf.metadata.row_group(0).column(0).compression
            return "UNKNOWN"
        except Exception as e:
            logger.error(f"Failed to read codec from {file_path}: {e}")
            return "ERROR"

    def _convert_and_transfer(self, source_file: Path, dest_file: Path) -> bool:
        """
        Convert parquet file to SNAPPY compression and transfer to destination.

        Returns True if successful, False otherwise.
        """
        try:
            # Read source file
            table = pq.read_table(source_file)

            # Get current codec for logging
            current_codec = self._get_parquet_codec(source_file)

            # Write with SNAPPY compression
            pq.write_table(
                table,
                dest_file,
                compression='snappy',
                use_dictionary=True,
                use_deprecated_int96_timestamps=False
            )

            # Verify codec
            new_codec = self._get_parquet_codec(dest_file)

            # Log transfer
            src_size_mb = source_file.stat().st_size / (1024 * 1024)
            dst_size_mb = dest_file.stat().st_size / (1024 * 1024)

            logger.info(
                f"Transferred {source_file.name}: "
                f"{current_codec} → {new_codec} "
                f"({src_size_mb:.2f} MB → {dst_size_mb:.2f} MB)"
            )

            return True

        except Exception as e:
            logger.error(f"Failed to transfer {source_file}: {e}")
            return False

    def transfer(self, force: bool = False) -> dict[str, Any]:
        """
        Transfer changed parquet files from source directories to destination.

        Args:
            force: If True, transfer all files regardless of change detection

        Returns:
            Dictionary with transfer statistics
        """
        run_start = datetime.now()
        logger.info(f"Starting Databricks transfer at {run_start.isoformat()}")

        # Collect all parquet files from source directories
        all_files = []
        for source_dir in self.source_dirs:
            if source_dir.exists():
                parquet_files = sorted(source_dir.glob("*.parquet"))
                all_files.extend(parquet_files)
                logger.info(f"Found {len(parquet_files)} files in {source_dir}")

        logger.info(f"Total files found: {len(all_files)}")

        # Track statistics
        stats = {
            "total_files": len(all_files),
            "transferred": 0,
            "skipped": 0,
            "failed": 0,
            "transferred_files": [],
            "skipped_files": [],
            "failed_files": [],
        }

        # Process each file
        for source_file in all_files:
            dest_file = self.dest_dir / source_file.name

            # Check if transfer is needed
            if not force and not self._has_file_changed(source_file):
                logger.debug(f"Skipping {source_file.name} (unchanged)")
                stats["skipped"] += 1
                stats["skipped_files"].append(source_file.name)
                continue

            # Transfer file
            logger.info(f"Transferring {source_file.name}...")
            if self._convert_and_transfer(source_file, dest_file):
                # Update state
                self.state["files"][str(source_file)] = self._get_file_signature(source_file)
                self.state["files"][str(source_file)]["transferred_at"] = run_start.isoformat()
                self.state["files"][str(source_file)]["dest_path"] = str(dest_file)

                stats["transferred"] += 1
                stats["transferred_files"].append(source_file.name)
            else:
                stats["failed"] += 1
                stats["failed_files"].append(source_file.name)

        # Update global state
        run_end = datetime.now()
        self.state["last_run"] = run_start.isoformat()
        self.state["last_run_end"] = run_end.isoformat()
        self.state["total_transfers"] = self.state.get("total_transfers", 0) + stats["transferred"]
        self.state["last_run_stats"] = stats

        # Save state
        self._save_state()

        # Log summary
        duration = (run_end - run_start).total_seconds()
        logger.info(
            f"\nTransfer complete in {duration:.1f}s:\n"
            f"  Total files: {stats['total_files']}\n"
            f"  Transferred: {stats['transferred']}\n"
            f"  Skipped: {stats['skipped']}\n"
            f"  Failed: {stats['failed']}"
        )

        return stats

    def status(self) -> dict[str, Any]:
        """Get current transfer status."""
        return {
            "last_run": self.state.get("last_run"),
            "last_run_end": self.state.get("last_run_end"),
            "total_transfers": self.state.get("total_transfers", 0),
            "total_files_tracked": len(self.state.get("files", {})),
            "last_run_stats": self.state.get("last_run_stats", {}),
        }

    def aggregate_logs(self, tracking_dir: Path | None = None, output_file: Path | None = None) -> Path:
        """
        Aggregate all tracking state logs into a comprehensive parquet file.

        Includes:
        - Transform state logs (schema transforms, file processing)
        - 4icli inventory and file tracking
        - Databricks transfer state

        Args:
            tracking_dir: Directory containing tracking state JSON files
            output_file: Path to output parquet file (default: Downloads/gov_programs_logs.parquet)

        Returns:
            Path to the generated parquet file
        """
        tracking_dir = tracking_dir or Path("/opt/s3/data/workspace/logs/tracking")
        output_file = output_file or self.dest_dir / "gov_programs_logs.parquet"

        logger.info(f"Aggregating logs from {tracking_dir}")

        # Collect all state JSON files
        state_files = list(tracking_dir.glob("*_state.json"))
        logger.info(f"Found {len(state_files)} state files")

        # Parse transform state files
        transform_records = []
        fouricli_files = []
        fouricli_inventory = None

        for state_file in state_files:
            try:
                with open(state_file) as f:
                    state_data = json.load(f)

                # Check if this is 4icli_state.json (file tracking)
                if state_file.name == "4icli_state.json":
                    logger.info("Processing 4icli file tracking state")
                    for _filename, file_data in state_data.items():
                        fouricli_files.append({
                            "record_type": "4icli_file",
                            "filename": file_data.get("filename"),
                            "category": file_data.get("category"),
                            "file_type_code": file_data.get("file_type_code"),
                            "file_size_bytes": file_data.get("file_size"),
                            "file_hash": file_data.get("file_hash"),
                            "download_timestamp": file_data.get("download_timestamp"),
                            "last_seen_remote": file_data.get("last_seen_remote"),
                            "source_path": file_data.get("source_path"),
                            "remote_size_bytes": file_data.get("remote_metadata", {}).get("size") if file_data.get("remote_metadata") else None,
                            "remote_created": file_data.get("remote_metadata", {}).get("created") if file_data.get("remote_metadata") else None,
                            "remote_modified": file_data.get("remote_metadata", {}).get("modified") if file_data.get("remote_metadata") else None,
                        })
                    continue

                # Check if this is 4icli_inventory_state.json
                if state_file.name == "4icli_inventory_state.json":
                    logger.info("Processing 4icli inventory state")
                    fouricli_inventory = {
                        "record_type": "4icli_inventory_summary",
                        "apm_id": state_data.get("apm_id"),
                        "total_files": state_data.get("total_files"),
                        "categories": ",".join(state_data.get("categories", [])),
                        "years": ",".join(map(str, state_data.get("years", []))),
                        "files_by_category": str(state_data.get("files_by_category", {})),
                        "files_by_year": str(state_data.get("files_by_year", {})),
                    }

                    # Also process individual files from inventory
                    for file_record in state_data.get("files", []):
                        fouricli_files.append({
                            "record_type": "4icli_inventory_file",
                            "filename": file_record.get("filename"),
                            "category": file_record.get("category"),
                            "file_type_code": file_record.get("file_type_code"),
                            "file_size_bytes": file_record.get("size_bytes"),
                            "year": file_record.get("year"),
                            "last_updated": file_record.get("last_updated"),
                            "discovered_at": file_record.get("discovered_at"),
                        })
                    continue

                # Regular transform state file
                record = {
                    "record_type": "transform_state",
                    "transform_name": state_data.get("transform_name"),
                    "last_run": state_data.get("last_run"),
                    "last_success": state_data.get("last_success"),
                    "total_runs": state_data.get("total_runs", 0),
                    "successful_runs": state_data.get("successful_runs", 0),
                    "failed_runs": state_data.get("failed_runs", 0),
                    "last_run_records": state_data.get("metadata", {}).get("last_run_records"),
                    "last_run_files": state_data.get("metadata", {}).get("last_run_files"),
                    "last_run_output": state_data.get("metadata", {}).get("last_run_output"),
                    "last_run_message": state_data.get("metadata", {}).get("last_run_message"),
                    "processed_files_count": len(state_data.get("files_processed", {}).get("processed", [])),
                    "state_file": str(state_file),
                }

                transform_records.append(record)

            except Exception as e:
                logger.warning(f"Failed to parse {state_file.name}: {e}")
                continue

        # Combine all records
        all_records = []

        # Add transform records
        all_records.extend(transform_records)

        # Add 4icli inventory summary if exists
        if fouricli_inventory:
            all_records.append(fouricli_inventory)

        # Add 4icli file records
        all_records.extend(fouricli_files)

        if not all_records:
            logger.warning("No valid records found")
            return None

        # Create DataFrame and write to parquet
        df = pl.DataFrame(all_records)

        # Sort: transforms by last_run, 4icli files by download_timestamp/discovered_at
        if "last_run" in df.columns:
            df = df.sort("last_run", descending=True, nulls_last=True)

        # Write to parquet with SNAPPY compression
        df.write_parquet(output_file, compression="snappy")

        logger.info(
            f"Aggregated {len(transform_records)} transform logs, "
            f"{len(fouricli_files)} 4icli file records, "
            f"and {'1' if fouricli_inventory else '0'} inventory summary to {output_file}"
        )
        logger.info(f"File size: {output_file.stat().st_size / (1024*1024):.2f} MB")

        return output_file
