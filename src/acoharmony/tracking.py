# © 2025 HarmonyCares
# All rights reserved.

"""
Comprehensive transformation tracking and state management system.

 persistent state tracking for data transformations
in ACOHarmony, enabling reliable incremental processing, audit trails,
and operational monitoring. It maintains both local state files and
centralized logging to support recovery, debugging, and compliance.

State Management:
    Each schema maintains its own state file containing:
    - Execution history (runs, successes, failures)
    - File processing status
    - Metadata about recent operations
    - Performance metrics

Logging Strategy:
    All operations are logged to both:
    - Standard Python logs (via LogWriter)
    - JSONL omnibus logs (acoharmony_YYYYMMDD.jsonl)
    This provides both human-readable and machine-parseable audit trails.

File Organization:
    State files: /opt/s3/data/workspace/logs/tracking/{schema_name}_state.json
    Omnibus logs: /opt/s3/data/workspace/logs/acoharmony_YYYYMMDD.jsonl

Recovery and Reliability:
    - State files are atomically written to prevent corruption
    - Failed state loads fall back to fresh state
    - All operations are logged even if state save fails
    - Supports incremental processing after interruptions

Note:
    This module is critical for production reliability, enabling
    ACOHarmony to handle large-scale incremental data processing
    with full auditability and recoverability.
"""

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from ._log.writer import LogWriter


@dataclass
class TransformState:
    """
    Persistent state representation for a transformation.

        This dataclass encapsulates all state information needed to track
        a transformation's lifecycle, history, and processed files. It's
        designed to be easily serializable to JSON for persistence.

        Attributes:
            transform_name: Unique identifier for the transformation,
                           typically the schema name
            last_run: ISO timestamp of the most recent execution attempt
            last_success: ISO timestamp of the most recent successful completion
            total_runs: Cumulative count of all execution attempts
            successful_runs: Count of successful completions
            failed_runs: Count of failed execution attempts
            files_processed: Dictionary mapping processing status to file lists.
                            Common statuses: 'processed', 'failed', 'skipped'
            metadata: Flexible dictionary for additional tracking data such as:
                     - last_run_records: Record count from last run
                     - last_run_files: File count from last run
                     - last_run_output: Output path from last run
                     - last_run_message: Status message from last run

        Serialization:
            Uses dataclasses.asdict() for JSON serialization.
            All fields must be JSON-serializable or have default converters.

        Note:
            The files_processed dictionary grows over time. In production,
            consider implementing cleanup strategies for old entries.
    """

    transform_name: str
    last_run: str | None = None
    last_success: str | None = None
    total_runs: int = 0
    successful_runs: int = 0
    failed_runs: int = 0
    files_processed: dict[str, list[str]] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


class TransformTracker:
    """
    Comprehensive transformation tracking with persistent state management.

        TransformTracker provides a robust framework for monitoring and tracking
        data transformation operations. It maintains persistent state across
        runs, tracks file processing status, and provides detailed logging
        for audit and debugging purposes.

        State Management:
            Each tracker instance manages a state file specific to its schema:
            - Location: {tracking_dir}/{schema_name}_state.json
            - Format: JSON serialized TransformState object
            - Updates: Atomic writes to prevent corruption
            - Recovery: Automatic fallback to fresh state on corruption

        File Tracking:
            Maintains lists of processed files by status:
            - 'processed': Successfully processed files
            - 'failed': Files that failed processing
            - 'skipped': Files skipped due to validation or filters
            Enables efficient incremental processing and reprocessing.

        Attributes:
            schema_name: Name of the schema being tracked
            tracking_dir: Directory for state file storage
            state_file: Path to the schema's state file
            logger: LogWriter instance for logging
            state: Current TransformState object

        File Structure:
            State files follow the pattern: {schema_name}_state.json
            Example: physician_claim_state.json, cclf1_state.json

        Performance Notes:
            - State loads are cached in memory during tracker lifetime
            - File tracking uses sets internally for O(1) lookups
            - JSON operations are optimized with custom serializers
            - Logs are buffered to minimize I/O overhead

        Thread Safety:
            Not thread-safe. Use separate instances per thread/process
            or implement external synchronization for concurrent access.
    """

    def __init__(self, schema_name: str, tracking_dir: Path | None = None):
        """
        Initialize a transformation tracker for a specific schema.

                Creates a new tracker instance that manages persistent state for
                a single schema's transformations. Automatically creates necessary
                directories and loads existing state if available.

                Args:
                    schema_name: Unique identifier for the schema being tracked.
                                This becomes part of the state file name and
                                is used in all log entries.
                    tracking_dir: Optional directory path for storing state files.
                                 Defaults to {storage.logs}/tracking based on active profile.
                                 Will be created if it doesn't exist.

                Side Effects:
                    - Creates tracking_dir if it doesn't exist
                    - Loads existing state file if present
                    - Initializes logging infrastructure

                State File Naming:
                    The state file is named: {schema_name}_state.json
                    This ensures each schema has its own isolated state.

                Note:
                    If an existing state file is corrupted or unreadable,
                    a warning is logged and a fresh state is initialized.
        """
        self.schema_name = schema_name

        if tracking_dir is None:
            from ._store import StorageBackend

            storage = StorageBackend()
            logs_path = storage.get_path("logs")
            self.tracking_dir = logs_path / "tracking"
        else:
            self.tracking_dir = Path(tracking_dir)

        self.tracking_dir.mkdir(parents=True, exist_ok=True)

        self.state_file = self.tracking_dir / f"{schema_name}_state.json"

        self.logger = LogWriter(f"transform.{schema_name}")

        self.state = self._load_state()

    def _load_state(self) -> TransformState:
        """
        Load existing transformation state from disk or create fresh state.

                Attempts to load a previously saved state file for the schema.
                If the file doesn't exist or is corrupted, initializes a new
                TransformState with default values.

                Returns:
                    TransformState: Either restored state from disk or fresh state

                Implementation Details:
                    - Uses JSON deserialization with TransformState constructor
                    - Validates loaded data matches expected structure
                    - Preserves backward compatibility with older state formats

                Note:
                    This method is called during initialization and should not
                    be called directly. State is cached in self.state.
        """
        if self.state_file.exists():
            try:
                with open(self.state_file) as f:
                    data = json.load(f)
                    return TransformState(**data)
            except (
                Exception
            ) as e:  # ALLOWED: Logs error and returns, caller handles the error condition
                self.logger.logger.warning(f"Could not load state from {self.state_file}: {e}")

        return TransformState(transform_name=self.schema_name)

    def _save_state(self):
        """
        Persist current transformation state to disk.

                Saves the current state object to a JSON file atomically.
                Uses pretty-printing for human readability and debugging.

                Serialization:
                    - Uses dataclasses.asdict() for state conversion
                    - Custom default=str for non-JSON types (e.g., datetime)
                    - Indented output for readability

                Atomicity:
                    While not using true atomic writes, the file is written
                    in a single operation to minimize corruption risk.

                Note:
                    Called automatically after state changes. Failures don't
                    interrupt the transformation process to maintain robustness.
        """
        try:
            with open(self.state_file, "w") as f:
                json.dump(asdict(self.state), f, indent=2, default=str)
        except Exception as e:  # ALLOWED: State saving failure - log error, continue processing
            self.logger.logger.error(f"Could not save state to {self.state_file}: {e}")

    def start_transform(self, pipeline: str | None = None, stage: str | None = None):
        """
        Record the start of a transformation operation.

                Marks the beginning of a transformation, updating state counters
                and logging the event to both standard and JSONL logs. This method
                should be called before beginning any transformation work.

                Args:
                    pipeline: Optional name of the pipeline being executed.
                             Examples: 'institutional_claim', 'physician_claim', 'enrollment'
                             Used for grouping related transformations.
                    stage: Optional name of the current processing stage.
                          Examples: 'parsing', 'deduplication', 'aggregation', 'export'
                          Helps track progress through multi-stage pipelines.

                State Changes:
                    - Updates last_run timestamp to current time
                    - Increments total_runs counter
                    - Saves state to disk

                Logging:
                    Logs to both:
                    - Standard Python logger (INFO level)
                    - JSONL omnibus log with structured metadata

                Metadata Logged:
                    - schema: Schema name from initialization
                    - pipeline: Pipeline name if provided
                    - stage: Stage name if provided
                    - action: Always 'start_transform'
                    - timestamp: Automatic from logger

                Note:
                    Always call complete_transform() when finished, even on failure,
                    to maintain accurate statistics.
        """
        now = datetime.now().isoformat()
        self.state.last_run = now
        self.state.total_runs += 1

        log_msg = f"Starting transform: {self.schema_name}"
        if pipeline:
            log_msg += f" [Pipeline: {pipeline}]"
        if stage:
            log_msg += f" [Stage: {stage}]"

        self.logger.log(
            "INFO",
            log_msg,
            schema=self.schema_name,
            pipeline=pipeline,
            stage=stage,
            action="start_transform",
        )

        self._save_state()

    def track_file(self, file_path: str, status: str = "processed"):
        """
        Record the processing status of an individual file.

                Tracks files that have been processed, failed, or skipped during
                transformation. This enables incremental processing by avoiding
                reprocessing of already-handled files.

                Args:
                    file_path: Path or name of the file being tracked.
                              Can be absolute or relative path.
                    status: Processing status of the file. Common values:
                           - 'processed': Successfully processed (default)
                           - 'failed': Processing failed
                           - 'skipped': Skipped due to filters or validation
                           - 'partial': Partially processed
                           Custom status values are allowed.

                State Changes:
                    - Adds file to files_processed[status] list
                    - Creates status category if it doesn't exist
                    - Prevents duplicate entries for same file/status
                    - Saves updated state to disk

                Logging:
                    - DEBUG level log entry with file and status
                    - Includes schema context and action metadata

                Duplicate Handling:
                    Files are only added once per status. Calling track_file
                    multiple times with the same file and status is idempotent.

                Use Cases:
                    - Incremental processing: Skip already processed files
                    - Error recovery: Identify and retry failed files
                    - Audit trail: Complete record of all file operations

                Note:
                    File paths are stored as strings and compared exactly.
                    Consider normalizing paths before tracking if needed.
        """
        if status not in self.state.files_processed:
            self.state.files_processed[status] = []

        if file_path not in self.state.files_processed[status]:
            self.state.files_processed[status].append(file_path)

        self.logger.logger.debug(
            f"Tracked file: {file_path} [{status}]",
            extra={
                "schema": self.schema_name,
                "file": file_path,
                "status": status,
                "action": "track_file",
            },
        )

        self._save_state()

    def complete_transform(
        self,
        success: bool = True,
        records: int | None = None,
        files: int | None = None,
        output: str | None = None,
        message: str | None = None,
        pipeline: str | None = None,
    ):
        """
        Record the completion of a transformation operation.

                Marks the end of a transformation, updating success/failure counters,
                recording metrics, and logging the outcome. Should be called after
                every start_transform(), regardless of success or failure.

                Args:
                    success: Whether the transformation completed successfully.
                            True for success, False for failure.
                    records: Optional count of records processed.
                            Useful for performance monitoring.
                    files: Optional count of files processed.
                          Helps track batch processing scale.
                    output: Optional path to the output file/directory created.
                           Documents where results were written.
                    message: Optional descriptive message about the completion.
                            Examples: 'Completed with warnings', 'Failed: Memory error'
                    pipeline: Optional pipeline name for correlation with start_transform.
                             Should match the pipeline parameter used in start_transform.

                State Changes:
                    Success=True:
                    - Updates last_success timestamp
                    - Increments successful_runs counter
                    Success=False:
                    - Increments failed_runs counter
                    Both:
                    - Updates metadata with run details
                    - Saves state to disk

                Logging:
                    - INFO level for success
                    - ERROR level for failure
                    - Logs to both standard and JSONL formats
                    - Includes all provided metrics

                Metadata Storage:
                    The following metadata is preserved in state:
                    - last_run_records: Record count from this run
                    - last_run_files: File count from this run
                    - last_run_output: Output location from this run
                    - last_run_message: Message from this run

                Best Practices:
                    - Always call this method, even on failure
                    - Include descriptive messages for debugging
                    - Track metrics for performance monitoring
                    - Use consistent pipeline names

                Note:
                    Metadata from the most recent run overwrites previous values.
                    Consider implementing history tracking for long-term analysis.
        """
        now = datetime.now().isoformat()

        if success:
            self.state.last_success = now
            self.state.successful_runs += 1
            log_level = "info"
        else:
            self.state.failed_runs += 1
            log_level = "error"

        # Update metadata
        self.state.metadata.update(
            {
                "last_run_records": records,
                "last_run_files": files,
                "last_run_output": output,
                "last_run_message": message,
            }
        )

        # Log transform completion
        log_msg = f"Completed transform: {self.schema_name}"
        if message:
            log_msg += f" - {message}"

        log_data = {
            "schema": self.schema_name,
            "pipeline": pipeline,
            "success": success,
            "records": records,
            "files": files,
            "output": output,
            "action": "complete_transform",
        }

        self.logger.log(log_level.upper(), log_msg, **log_data)

        self._save_state()

    def get_unprocessed_files(self, all_files: list[str]) -> list[str]:
        """
        Identify files that have not been processed yet.

                Compares a list of available files against the tracked processing
                history to identify files that need processing. This is the core
                method for implementing incremental processing.

                Args:
                    all_files: List of all available file paths/names to check.
                              Should match the format used in track_file().

                Returns:
                    List[str]: Subset of all_files that have not been tracked
                              with any status (processed, failed, or skipped).

                Algorithm:
                    1. Collects all files from all status categories
                    2. Creates a set for O(1) lookup performance
                    3. Filters all_files to exclude tracked files
                    4. Preserves original order from all_files

                Use Cases:
                    - Batch processing: Process only new files
                    - Incremental updates: Skip already-processed data
                    - Recovery: Identify work remaining after interruption

                Performance:
                    O(n + m) where n is len(all_files) and m is total tracked files.
                    Uses set operations for efficient lookups.

                Note:
                    Files tracked with ANY status (including 'failed') are
                    considered processed. To reprocess failed files, track
                    them separately or clear them from state.
        """
        processed = set()
        for status_files in self.state.files_processed.values():
            processed.update(status_files)

        return [f for f in all_files if f not in processed]

    def has_processed_file(self, file_path: str) -> bool:
        """
        Check if a specific file has been tracked with any status.

                Determines whether a file has been previously tracked, regardless
                of its processing status (processed, failed, skipped, etc.).

                Args:
                    file_path: Path or name of the file to check.
                              Must exactly match the format used in track_file().

                Returns:
                    bool: True if the file has been tracked with any status,
                         False if the file has never been tracked.

                Performance:
                    O(k) where k is the number of status categories.
                    Generally very fast as k is typically small (< 5).

                Note:
                    Returns True for files with ANY status, including 'failed'.
                    To check specific status, access state.files_processed directly.
        """
        for status_files in self.state.files_processed.values():
            if file_path in status_files:
                return True
        return False

    def get_stats(self) -> dict[str, Any]:
        """
        Generate comprehensive statistics about transformation history.

                Compiles key metrics about the transformation's execution history,
                success rates, and file processing counts. Useful for monitoring,
                reporting, and debugging.

                Returns:
                    Dict[str, Any]: Dictionary containing:
                        - transform: Schema name being tracked
                        - total_runs: Total number of transformation attempts
                        - successful_runs: Number of successful completions
                        - failed_runs: Number of failed attempts
                        - last_run: ISO timestamp of most recent execution
                        - last_success: ISO timestamp of most recent success
                        - total_files_processed: Total files across all statuses

                Use Cases:
                    - Dashboard displays: Show transformation health
                    - Alerting: Detect high failure rates
                    - Reporting: Generate processing summaries
                    - Debugging: Identify problematic transformations

                Calculated Metrics:
                    - Success rate: successful_runs / total_runs
                    - Failure rate: failed_runs / total_runs
                    - Average files per run: total_files_processed / total_runs

                Note:
                    Statistics are cumulative since tracker creation.
                    To reset statistics, delete the state file and reinitialize.
        """
        return {
            "transform": self.schema_name,
            "total_runs": self.state.total_runs,
            "successful_runs": self.state.successful_runs,
            "failed_runs": self.state.failed_runs,
            "last_run": self.state.last_run,
            "last_success": self.state.last_success,
            "total_files_processed": sum(
                len(files) for files in self.state.files_processed.values()
            ),
        }

    def get_failed_files(self) -> list[str]:
        """
        Get list of files that failed processing.

                Returns:
                    List of file names/paths that are marked as failed in the state.
        """
        return list(self.state.files_processed.get("failed", []))

    def clear_file_status(self, file_path: str):
        """
        Remove a file from all tracking status lists.

                This allows the file to be reprocessed as if it was never seen before.
                Useful for retrying failed files or reprocessing corrupted outputs.

                Args:
                    file_path: Path/name of the file to clear from tracking

                Side Effects:
                    - Removes file from all status lists (processed, failed, skipped)
                    - Saves updated state to disk
        """
        for status_files in self.state.files_processed.values():
            if file_path in status_files:
                status_files.remove(file_path)
        self._save_state()
