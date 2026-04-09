# © 2025 HarmonyCares
# All rights reserved.

"""
File processing component for handling raw data files.

 the parsing and initial processing of raw data files,
including chunked processing for large files and tracking of processed items.
"""

import tempfile
from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import (
    check_not_empty,
    parser_method,
    validate_path_exists,
)
from ._memory import MemoryManager
from ._registry import register_processor


@register_processor("file_processor")
class FileProcessor:
    """
    Handles raw file processing with memory-aware strategies.
        - File discovery and pattern matching
        - Chunked processing for large files
        - Temporary file management
        - Source tracking metadata
        - Processing manifests
    """

    def __init__(self, storage_config: Any, catalog: Any, logger: Any):
        """
        Initialize file processor.

                Args:
                    storage_config: Storage configuration for paths
                    catalog: Schema catalog for definitions
                    logger: Logger instance
        """
        self.storage_config = storage_config
        self.catalog = catalog
        self.logger = logger
        self.memory_manager = MemoryManager()

    @parser_method(
        threshold=10.0,
        track_memory=True,
        validate_args_types={"schema_name": str, "force": bool},
    )
    def process_raw_files(
        self, schema_name: str, schema: Any, tracker: Any, force: bool = False
    ) -> pl.LazyFrame | None:
        """
        Process raw files for a schema with adaptive strategies.

                This method:
                1. Discovers files matching schema patterns
                2. Checks processing manifest for already-processed files
                3. Chooses processing strategy (chunked vs. in-memory)
                4. Applies source tracking
                5. Returns combined LazyFrame or dict of LazyFrames (for multi-output)

                Args:
                    schema_name: Name of the schema
                    schema: Schema definition with file patterns
                    tracker: Transform tracker for state management
                    force: Force reprocessing even if already done

                Returns:
                    Optional[pl.LazyFrame]: Combined data from all files
        """
        # Get file patterns from schema
        patterns = self._get_file_patterns(schema)
        if not patterns:
            self.logger.info(f"No file patterns defined for {schema_name}")
            return None

        # Discover matching files
        all_files = self._discover_files(schema_name, patterns)
        if not all_files:
            self.logger.info(f"No files found for {schema_name}")
            return None

        total_discovered = len(all_files)

        # Check manifest for already-processed files
        files_to_process = all_files
        already_processed_count = 0

        if not force:
            files_to_process = self._filter_processed_files(schema_name, all_files, tracker)
            already_processed_count = total_discovered - len(files_to_process)

            if not files_to_process:
                self.logger.info(
                    f"[OK] {schema_name}: {total_discovered} files in bronze, all already in silver (no new files to process)"
                )
                return None

        # Log what we're about to process
        if already_processed_count > 0:
            self.logger.info(
                f"Processing {schema_name}: {len(files_to_process)} new files, {already_processed_count} already in silver"
            )
        else:
            self.logger.info(f"Processing {schema_name}: {len(files_to_process)} files")

        # Process files based on size
        dataframes = []
        processed_file_paths = []  # Track which files were successfully parsed
        for file_path in files_to_process:
            self.logger.info(f"  → Processing {file_path.name}")

            # Determine processing strategy
            file_size = file_path.stat().st_size
            if self.memory_manager.should_use_chunked_processing(schema_name, file_size):
                df = self._process_file_chunked(file_path, schema, schema_name)
            else:
                df = self._process_file_direct(file_path, schema, schema_name)

            if df is not None:
                dataframes.append(df)
                # Store file path for tracking AFTER successful write
                processed_file_paths.append(file_path)

        # Combine all dataframes
        if dataframes:
            combined_df = self._combine_dataframes(dataframes)
            # Store file paths in tracker metadata for later tracking
            # Files will be tracked as "processed" only after successful silver write
            if hasattr(tracker, "_pending_files"):
                tracker._pending_files = processed_file_paths
            else:
                # Fallback: store in metadata for access by schema_transformer
                tracker.state.metadata["_pending_files"] = [str(f) for f in processed_file_paths]
            return combined_df

        return None

    def _get_file_patterns(self, schema: Any) -> list[str]:
        """
        Extract file patterns from schema.

                Args:
                    schema: Schema definition

                Returns:
                    List[str]: File patterns to match
        """
        if hasattr(schema, "storage") and "file_patterns" in schema.storage:
            patterns = schema.storage["file_patterns"]
            if patterns is None:
                return []
            # Handle single pattern string
            if isinstance(patterns, str):
                return [patterns]
            # Handle dict with pattern/patterns keys
            if isinstance(patterns, dict):
                # Check for common keys
                if "pattern" in patterns:
                    return [patterns["pattern"]]
                elif "patterns" in patterns:
                    return patterns["patterns"]
                # Check for program-specific patterns (reach, mssp, etc)
                all_patterns = []
                for _key, value in patterns.items():
                    if isinstance(value, list):
                        all_patterns.extend(value)
                    elif isinstance(value, str):
                        all_patterns.append(value)
                return all_patterns
            # Handle list directly
            if isinstance(patterns, list):
                return patterns
        return []

    def _discover_files(self, schema_name: str, patterns: list[str]) -> list[Path]:
        """
        Discover files matching patterns.

                Args:
                    schema_name: Schema name for path resolution
                    patterns: List of glob patterns

                Returns:
                    List[Path]: Discovered file paths
        """
        input_path = self.storage_config.get_path("bronze")
        files = []

        for pattern in patterns:
            full_pattern = input_path / pattern
            matches = list(full_pattern.parent.glob(full_pattern.name))
            files.extend(matches)

        # Filter out PDF files (no parser support)
        files = [f for f in files if f.suffix.lower() != ".pdf"]

        return sorted(set(files))  # Remove duplicates and sort

    def _filter_processed_files(
        self, schema_name: str, files: list[Path], tracker: Any
    ) -> list[Path]:
        """
        Filter out already-processed files using manifest.

                Args:
                    schema_name: Schema name
                    files: List of file paths
                    tracker: Transform tracker

                Returns:
                    List[Path]: Files that haven't been processed
        """
        # Get processed files from tracker state
        state = tracker.state
        if not state or not state.files_processed:
            return files

        processed_files = set(state.files_processed.get("processed", []))
        return [f for f in files if str(f) not in processed_files]

    @validate_path_exists(param_name="file_path")
    def _process_file_direct(
        self, file_path: Path, schema: Any, schema_name: str
    ) -> pl.LazyFrame | None:
        """
        Process file directly in memory.

                Args:
                    file_path: Path to file
                    schema: Schema definition
                    schema_name: Schema name

                Returns:
                    Optional[pl.LazyFrame]: Processed data
        """
        try:
            from ..parsers import parse_file

            # Parse file
            df = parse_file(file_path, schema, add_tracking=True, schema_name=schema_name)

            return df

        except Exception as e:  # ALLOWED: Returns None to indicate error
            self.logger.error(f"Error processing {file_path}: {e}", exc_info=True)
            return None

    @validate_path_exists(param_name="file_path")
    def _process_file_chunked(
        self, file_path: Path, schema: Any, schema_name: str
    ) -> pl.LazyFrame | None:
        """
        Process file in chunks for memory efficiency.

                Args:
                    file_path: Path to file
                    schema: Schema definition
                    schema_name: Schema name

                Returns:
                    Optional[pl.LazyFrame]: Path to temporary chunked file
        """
        try:
            from ..parsers import parse_file

            chunk_size = self.memory_manager.get_optimal_chunk_size()
            temp_dir = Path(tempfile.gettempdir()) / "acoharmony" / schema_name
            temp_dir.mkdir(parents=True, exist_ok=True)

            chunk_files = []
            offset = 0
            chunk_num = 0

            while True:
                # Parse chunk
                df = parse_file(
                    file_path,
                    schema,
                    add_tracking=True,
                    schema_name=schema_name,
                    limit=chunk_size,
                    offset=offset,
                )

                if df is None:
                    break

                # Collect and check if we got data
                collected = df.collect()
                if len(collected) == 0:
                    break

                # Write chunk to temp file
                chunk_file = temp_dir / f"{file_path.stem}_chunk_{chunk_num}.parquet"
                collected.write_parquet(chunk_file, compression="zstd")
                chunk_files.append(chunk_file)

                # Check if we got fewer rows than chunk size (last chunk)
                if len(collected) < chunk_size:
                    break

                offset += chunk_size
                chunk_num += 1

            # Return lazy scan of all chunks
            if chunk_files:
                return pl.scan_parquet(chunk_files)

            return None

        except Exception as e:  # ALLOWED: Returns None to indicate error
            self.logger.error(f"Error in chunked processing of {file_path}: {e}")
            return None

    @check_not_empty(param_name="dataframes")
    def _combine_dataframes(self, dataframes: list[pl.LazyFrame]) -> pl.LazyFrame:
        """
        Combine multiple dataframes efficiently.

                Args:
                    dataframes: List of LazyFrames to combine

                Returns:
                    pl.LazyFrame: Combined dataframe
        """
        if len(dataframes) == 1:
            return dataframes[0]

        # Use diagonal_relaxed concat to handle schema differences across file versions
        return pl.concat(dataframes, how="diagonal_relaxed")

    def _load_processed_data(self, schema_name: str) -> pl.LazyFrame | None:
        """
        Load already-processed data.

                Args:
                    schema_name: Schema name

                Returns:
                    Optional[pl.LazyFrame]: Processed data if exists
        """
        output_path = self.storage_config.get_path("silver")
        parquet_file = output_path / f"{schema_name}.parquet"

        if parquet_file.exists():
            return pl.scan_parquet(parquet_file)

        return None
