# © 2025 HarmonyCares
# All rights reserved.

"""
The execution of schema-defined transformations
on data, applying transforms in the correct order and managing results.
"""

from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import (
    check_not_empty,
    runner_method,
)
from ..result import TransformResult
from ._registry import register_processor


@register_processor("schema_transformer")
class SchemaTransformer:
    """
    Executes schema transformations on data.
        - Transform application in correct order
        - Union operations across multiple sources
        - Result tracking and reporting
        - Output file generation
    """

    def __init__(self, storage_config: Any, catalog: Any, logger: Any):
        """
        Initialize schema transformer.

                Args:
                    storage_config: Storage configuration
                    catalog: Schema catalog
                    logger: Logger instance
        """
        self.storage_config = storage_config
        self.catalog = catalog
        self.logger = logger

    @runner_method(
        schema_arg="schema_name",
        threshold=0.5,
        track_memory=True,
        validate_args_types={"schema_name": str, "force": bool, "no_tracking": bool},
    )
    def transform_schema(
        self,
        schema_name: str,
        df: pl.LazyFrame,
        tracker: Any,
        force: bool = False,
        chunk_size: int | None = None,
        no_tracking: bool = False,
    ) -> TransformResult:
        """
        Execute schema transformation on data.

                Args:
                    schema_name: Name of schema to transform
                    df: Input LazyFrame
                    tracker: Transform tracker
                    force: Force reprocessing
                    chunk_size: Optional chunk size for processing
                    no_tracking: Disable tracking

                Returns:
                    TransformResult: Transformation results
        """
        start_time = datetime.now()

        # Get table metadata
        schema = self.catalog.get_table_metadata(schema_name)
        if not schema:
            return TransformResult.transform_error(f"Schema '{schema_name}' not found")

        # Note: _is_processed check removed - FileProcessor handles incremental logic
        # and returns None when no new files to process

        # Initialize tracking
        if not no_tracking:
            tracker.start_transform()

        # Apply transform if one exists for this schema
        from acoharmony import _transforms  # noqa: F401 - import registers transforms

        # Try to get and apply the transform for this schema
        transformed_df = df
        try:
            # Check if there's a transform module for this schema
            if hasattr(_transforms, f"_{schema_name}"):
                transform_mod = getattr(_transforms, f"_{schema_name}")
                if hasattr(transform_mod, "apply_transform"):
                    self.logger.debug(f"Applying transform for {schema_name}")
                    transformed_df = transform_mod.apply_transform(
                        df, schema, self.catalog, self.logger, force
                    )
        except Exception as e:  # ALLOWED: Transform is optional, continue without it
            self.logger.debug(f"No transform applied for {schema_name}: {e}")
            transformed_df = df

        # Write output
        output_path = self._write_output(transformed_df, schema_name, chunk_size, force)

        # Calculate metrics
        (datetime.now() - start_time).total_seconds()

        # Get row count if possible
        try:
            row_count = transformed_df.select(pl.len()).collect()[0, 0]
        except Exception as e:  # ALLOWED: Row count is optional metric, continue with 0
            self.logger.warning(f"Could not get row count: {e}")
            row_count = 0

        # Track files as processed AFTER successful write
        if not no_tracking:
            # Get pending files from tracker metadata (set by file_processor)
            pending_files = tracker.state.metadata.get("_pending_files", [])
            for file_path in pending_files:
                tracker.track_file(file_path, "processed")
            # Clear pending files from metadata
            if "_pending_files" in tracker.state.metadata:
                del tracker.state.metadata["_pending_files"]

        # Update tracking
        if not no_tracking:
            tracker.complete_transform(
                success=True, records=row_count, files=1, output=str(output_path)
            )

        return TransformResult.transform_ok(
            records=row_count,
            files=1,
            output=str(output_path),
            message=f"Transformed {row_count:,} records",
        )

    @check_not_empty(param_name="df")
    def _write_output(
        self, df: pl.LazyFrame, schema_name: str, chunk_size: int | None = None, force: bool = False
    ) -> Path:
        """
        Write transformed data to output, appending to existing data if present.

                If the data contains an ``_output_table`` column (from multi-sheet
                Excel parsing), each partition is written to its own parquet file
                with only its non-null columns, producing tidy per-sheet outputs.

                Args:
                    df: Transformed data (new data to add)
                    schema_name: Schema name
                    chunk_size: Optional chunk size for streaming
                    force: Force overwrite instead of append

                Returns:
                    Path: Output file path (or directory path for multi-output)
        """
        output_path = self.storage_config.get_path("silver")
        output_path.mkdir(parents=True, exist_ok=True)

        # Detect multi-output: if _output_table column exists, split per partition
        try:
            schema_cols = df.collect_schema().names()
        except Exception:
            schema_cols = []

        if "_output_table" in schema_cols:
            return self._write_multi_output(df, schema_name, output_path, force)

        output_file = output_path / f"{schema_name}.parquet"
        self._write_single_output(df, output_file, schema_name, chunk_size, force)
        return output_file

    def _write_multi_output(
        self, df: pl.LazyFrame, schema_name: str, output_path: Path, force: bool
    ) -> Path:
        """Write separate parquet files per _output_table partition."""
        collected = df.collect()

        partitions = collected["_output_table"].unique().drop_nulls().sort().to_list()
        total_rows = 0

        # Metadata columns to keep on every partition
        meta_cols = {"sheet_type", "_output_table", "processed_at", "source_file",
                     "source_filename", "file_date", "medallion_layer"}

        for table_name in partitions:
            partition = collected.filter(pl.col("_output_table") == table_name)

            # Drop columns that are entirely null for this partition
            keep = []
            for col in partition.columns:
                if col in meta_cols or partition[col].drop_nulls().len() > 0:
                    keep.append(col)
            partition = partition.select(keep)

            # Drop the _output_table column (it's encoded in the filename)
            if "_output_table" in partition.columns:  # pragma: no branch
                partition = partition.drop("_output_table")

            out_file = output_path / f"{table_name}.parquet"

            if out_file.exists() and force:
                out_file.unlink()
            elif out_file.exists():
                # Append mode
                existing = pl.read_parquet(out_file)
                partition = pl.concat([existing, partition], how="diagonal_relaxed")

            partition.write_parquet(out_file, compression="zstd")
            total_rows += len(partition)
            self.logger.info(
                f"[OK] Wrote {len(partition):,} rows × {len(partition.columns)} cols "
                f"to silver/{out_file.name}"
            )

        self.logger.info(f"[OK] Wrote {total_rows:,} total rows across {len(partitions)} files")
        return output_path

    def _write_single_output(
        self,
        df: pl.LazyFrame,
        output_file: Path,
        schema_name: str,
        chunk_size: int | None,
        force: bool,
    ) -> None:
        """Write a single parquet file, with append support."""
        # Check if file exists and handle append mode
        should_append = False

        if output_file.exists():
            if force:
                self.logger.info(f"  Force mode: overwriting existing {output_file.name}")
                output_file.unlink()
            else:
                file_size = output_file.stat().st_size
                if file_size < 100:
                    self.logger.warning(f"  Existing {output_file.name} is empty/corrupted, overwriting")
                    output_file.unlink()
                else:
                    try:
                        existing_df = pl.scan_parquet(output_file)
                        _ = existing_df.collect_schema()
                        should_append = True
                        self.logger.info(f"  Appending new data to existing {output_file.name}")
                    except Exception as e:
                        self.logger.warning(f"  Could not read existing file, overwriting: {e}")
                        try:
                            output_file.unlink()
                        except Exception:  # ALLOWED: Best effort cleanup
                            pass

        try:
            if should_append:
                bronze_path = self.storage_config.get_path("bronze")
                temp_dir = bronze_path / ".temp"
                temp_dir.mkdir(parents=True, exist_ok=True)
                temp_file = temp_dir / f"{schema_name}_temp.parquet"

                existing_df = pl.scan_parquet(output_file)
                combined_df = pl.concat([existing_df, df], how="diagonal_relaxed")

                if chunk_size:
                    combined_df.sink_parquet(temp_file, compression="zstd", row_group_size=chunk_size)
                else:
                    combined_df.sink_parquet(temp_file, compression="zstd")

                import shutil

                shutil.move(str(temp_file), str(output_file))
            else:
                if chunk_size:
                    df.sink_parquet(output_file, compression="zstd", row_group_size=chunk_size)
                else:
                    df.sink_parquet(output_file, compression="zstd")

        except Exception as e:  # ALLOWED: Logs error and returns, caller handles the error condition
            self.logger.error(f"Failed to write parquet data: {str(e)}")
            if output_file.exists():
                try:
                    output_file.unlink()
                    self.logger.info("  Cleaned up corrupted output file")
                except Exception:  # ALLOWED: Best effort cleanup
                    pass
            raise

        try:
            row_count = pl.scan_parquet(output_file).select(pl.len()).collect()[0, 0]
            self.logger.info(f"[OK] Wrote {row_count:,} rows to silver/{output_file.name}")
        except Exception:
            self.logger.info(f"[OK] Wrote output to silver/{output_file.name}")

    def _is_processed(self, schema_name: str) -> bool:
        """
        Check if schema has already been processed.

                Args:
                    schema_name: Schema name

                Returns:
                    bool: True if already processed
        """
        output_path = self.storage_config.get_path("silver")
        output_file = output_path / f"{schema_name}.parquet"
        return output_file.exists()
