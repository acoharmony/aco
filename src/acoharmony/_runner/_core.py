# © 2025 HarmonyCares
# All rights reserved.

"""
Core TransformRunner that orchestrates all transformation operations.

 the main runner class that integrates all components
for executing data transformations.
"""

import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl

from .._catalog import Catalog
from .._decor8 import runner_method
from .._log.writer import LogWriter
from .._store import StorageBackend
from ..config import get_config
from ..medallion import MedallionLayer
from ..result import PipelineResult, TransformResult
from ..tracking import TransformTracker
from ._file_processor import FileProcessor
from ._memory import MemoryManager
from ._pipeline_executor import PipelineExecutor
from ._schema_transformer import SchemaTransformer


class TransformRunner:
    """
    Core execution engine for data transformation operations.

        TransformRunner orchestrates the entire transformation lifecycle from
        raw data ingestion through final output generation. It provides memory-
        efficient processing, error recovery, and comprehensive tracking for
        production-scale data transformations.

        Processing Modes:
            1. **Raw File Processing**: Parse and transform source files
            2. **Staged Processing**: Transform from intermediate schemas
            3. **Union Processing**: Combine multiple schemas
            4. **Pipeline Processing**: Multi-stage transformation chains

        Attributes:
            storage_config: Storage configuration for paths
            catalog: Schema catalog for definitions
            logger: Logger instance for operation tracking
            config: Global configuration settings
    """

    def __init__(self, storage_config: StorageBackend | None = None):
        """
        Initialize the TransformRunner.

                Sets up all necessary components including storage configuration,
                catalog access, logging, and tracking systems. The runner is
                stateless between operations, with all state managed through
                the tracking system.

                Args:
                    storage_config: Optional storage configuration override.
                                   Defaults to configuration from settings.

        """
        self.config = get_config()
        self.storage_config = storage_config or StorageBackend()
        self.catalog = Catalog()
        self.logger = LogWriter("TransformRunner")

        # Initialize components
        self.memory_manager = MemoryManager()
        self.file_processor = FileProcessor(self.storage_config, self.catalog, self.logger)
        self.schema_transformer = SchemaTransformer(self.storage_config, self.catalog, self.logger)
        self.pipeline_executor = PipelineExecutor(self)

        # Log initialization
        self.logger.info("TransformRunner initialized")
        memory_gb, total_gb = self.memory_manager.get_memory_info()
        self.logger.info(f"System memory: {memory_gb:.1f}GB available / {total_gb:.1f}GB total")

    @runner_method(
        schema_arg="schema_name",
        threshold=10.0,
        track_memory=True,
        validate_args_types={"schema_name": str, "force": bool, "no_tracking": bool},
    )
    def transform_schema(
        self,
        schema_name: str,
        force: bool = False,
        chunk_size: int | None = None,
        no_tracking: bool = False,
        **kwargs,
    ) -> TransformResult:
        """
        Transform a single schema from raw to processed.

                This is the primary method for transforming individual schemas.
                It handles the complete transformation lifecycle including:
                - File discovery and parsing
                - Transform application
                - Memory-aware processing
                - Output generation
                - Progress tracking

                Args:
                    schema_name: Name of the schema to transform
                    force: Force reprocessing even if already complete
                    chunk_size: Override chunk size for processing
                    no_tracking: Disable state tracking (for testing)
                    **kwargs: Additional arguments for specialized processing

                Returns:
                    TransformResult: Detailed result of the transformation

        """
        self.logger.info(f"Starting transformation for schema: {schema_name}")
        datetime.now()

        # Initialize tracker
        tracker = TransformTracker(schema_name) if not no_tracking else None

        # Get table metadata
        schema = self.catalog.get_table_metadata(schema_name)
        if not schema:
            return TransformResult.transform_error(f"Schema '{schema_name}' not found in catalog")

        # Determine input source
        if self._has_raw_files(schema):
            # Process from raw files
            df = self.file_processor.process_raw_files(schema_name, schema, tracker, force)
        elif self._has_staged_input(schema):
            # Process from staged schema
            df = self._load_staged_input(schema)
        else:
            return TransformResult.transform_error("No input source defined for schema")

        if df is None:
            return TransformResult.skipped("No data to process")

        # Apply transformations
        result = self.schema_transformer.transform_schema(
            schema_name, df, tracker, force, chunk_size, no_tracking
        )

        return result

    def transform_table(
        self,
        table_name: str,
        force: bool = False,
        chunk_size: int | None = None,
        no_tracking: bool = False,
        **kwargs,
    ) -> TransformResult:
        """
        Transform a single table through its medallion layer.

                This is the new preferred method name for transforming tables.
                It provides the same functionality as transform_schema but with
                clearer naming aligned with medallion architecture.

                Args:
                    table_name: Name of table to transform (e.g., "cclf1")
                    force: Force reprocessing even if already complete
                    chunk_size: Override chunk size for processing
                    no_tracking: Disable state tracking (for testing)
                    **kwargs: Additional arguments for specialized processing

                Returns:
                    TransformResult: Detailed result of the transformation

        """
        return self.transform_schema(table_name, force, chunk_size, no_tracking, **kwargs)

    def transform_medallion_layer(
        self, medallion_layer: MedallionLayer, force: bool = False, **kwargs
    ) -> dict[str, TransformResult]:
        """
        Transform all tables in a medallion layer.

                Processes all tables belonging to the specified medallion layer
                (Bronze/Silver/Gold), useful for bulk processing of data tiers.

                Args:
                    medallion_layer: Bronze/Silver/Gold layer to process
                    force: Force reprocessing
                    **kwargs: Additional arguments passed to transform operations

                Returns:
                    Dict mapping table names to TransformResults

        """
        tables = self.catalog.list_tables(medallion_layer)
        results = {}

        self.logger.info(f"Processing {len(tables)} tables in {medallion_layer.unity_schema} layer")

        for table_name in tables:
            try:
                result = self.transform_table(table_name, force, **kwargs)
                results[table_name] = result
            except (
                Exception
            ) as e:  # ALLOWED: Logs error and returns, caller handles the error condition
                self.logger.error(f"Error transforming {table_name}: {e}")
                results[table_name] = TransformResult.transform_error(str(e))

        return results

    def run_pipeline(self, pipeline_name: str, force: bool = False, **kwargs) -> PipelineResult:
        """
        Execute a multi-stage transformation pipeline.

                Pipelines define sequences of dependent transformations that must
                be executed in order. Each stage can depend on outputs from previous
                stages, allowing complex multi-step processing workflows.

                Available pipelines:
                    - institutional_claim: Process Part A institutional claims
                    - physician_claim: Process Part B physician claims
                    - consolidated_alignment: Build beneficiary alignment
                    - medical_claim: Combine Part A and B claims

                Args:
                    pipeline_name: Name of the pipeline to execute
                    force: Force reprocessing of all stages
                    **kwargs: Additional arguments passed to each stage

                Returns:
                    PipelineResult: Aggregated results from all pipeline stages

        """
        return self.pipeline_executor.run_pipeline(pipeline_name, force, **kwargs)

    def transform_pattern(
        self, pattern: str, force: bool = False, **kwargs
    ) -> dict[str, TransformResult]:
        """
        Transform multiple schemas matching a pattern.

                Batch process schemas whose names match a glob pattern. Useful for
                processing related schemas like all CCLF files or all claim types.

                Args:
                    pattern: Glob pattern to match schema names (e.g., 'cclf*')
                    force: Force reprocessing of all matching schemas
                    **kwargs: Additional arguments for transformation

                Returns:
                    Dict[str, TransformResult]: Results keyed by schema name

        """
        import fnmatch

        matching_schemas = [
            name for name in self.catalog.list_tables() if fnmatch.fnmatch(name, pattern)
        ]

        if not matching_schemas:
            self.logger.warning(f"No schemas match pattern: {pattern}")
            return {}

        self.logger.info(f"Found {len(matching_schemas)} schemas matching '{pattern}'")

        results = {}
        for schema_name in matching_schemas:
            self.logger.info(f"Processing {schema_name}")
            results[schema_name] = self.transform_schema(schema_name, force=force, **kwargs)

        return results

    def transform_all(self, force: bool = False, **kwargs) -> dict[str, TransformResult]:
        """
        Transform all available schemas.

                Process every schema defined in the catalog. This is typically used
                for full system processing or testing.

                Args:
                    force: Force reprocessing of all schemas
                    **kwargs: Additional arguments for transformation

                Returns:
                    Dict[str, TransformResult]: Results for all schemas

                Warning:
                    This can be a very long-running operation depending on data volume.
                    Consider using pipelines or patterns for more targeted processing.
        """
        return self.transform_pattern("*", force=force, **kwargs)

    def list_pipelines(self) -> list[str]:
        """
        List all available transformation pipelines.

                Returns:
                    List[str]: Names of available pipelines
        """
        return self.pipeline_executor.list_pipelines()

    def clean_temp_files(self, all_files: bool = False):
        """
        Clean up temporary files from interrupted processing.

                Temp files are created during chunked processing and normally
                cleaned up automatically.  manual cleanup
                for interrupted operations.

                Args:
                    all_files: If True, remove all temp files. If False, only
                              remove files older than 24 hours.
        """
        temp_dir = Path(tempfile.gettempdir()) / "acoharmony"

        if not temp_dir.exists():
            return

        import time

        now = time.time()
        age_limit = 86400  # 24 hours in seconds

        for schema_dir in temp_dir.iterdir():
            if schema_dir.is_dir():
                for file in schema_dir.glob("*.parquet"):
                    file_age = now - file.stat().st_mtime
                    if all_files or file_age > age_limit:
                        file.unlink()
                        self.logger.info(f"Removed temp file: {file}")

                # Remove empty directories
                if not list(schema_dir.iterdir()):
                    schema_dir.rmdir()

    # Private helper methods
    def _has_raw_files(self, schema: Any) -> bool:
        """Check if schema has raw file patterns defined."""
        return hasattr(schema, "storage") and "file_patterns" in schema.storage

    def _has_staged_input(self, schema: Any) -> bool:
        """Check if schema has staged input defined."""
        # Check new field (staging_source) or storage.staged_from
        if hasattr(schema, "staging_source") and schema.staging_source:
            return True
        return hasattr(schema, "storage") and "staged_from" in schema.storage


    def _load_staged_input(self, schema: Any) -> pl.LazyFrame | None:
        """Load data from staged schema."""
        # Check new field (staging_source) first, then fall back to storage.staged_from
        staged_from = None
        if hasattr(schema, "staging_source") and schema.staging_source:
            staged_from = schema.staging_source
        elif hasattr(schema, "storage") and "staged_from" in schema.storage:
            staged_from = schema.storage.get("staged_from")

        if not staged_from:
            return None

        input_path = self.storage_config.get_path("silver")
        input_file = input_path / f"{staged_from}.parquet"

        if input_file.exists():
            return pl.scan_parquet(input_file)

        return None
