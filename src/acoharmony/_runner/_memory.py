# © 2025 HarmonyCares
# All rights reserved.

"""
Memory management utilities for adaptive data processing.

 utilities for detecting system memory constraints
and adjusting processing strategies to handle large datasets efficiently.
"""

import psutil

from .._decor8 import timeit
from ..config import get_config
from ._registry import register_processor


@register_processor("memory_manager")
class MemoryManager:
    """
    Manages memory-aware processing strategies.

         utilities to:
        - Detect available system memory
        - Determine optimal chunk sizes
        - Decide between streaming and in-memory processing
        - Monitor memory pressure during operations

        Configuration is loaded from active profile in pyproject.toml.
    """

    # Memory thresholds for different processing strategies
    LARGE_FILE_SIZE = 5 * 1024 * 1024 * 1024  # 5 GB

    def __init__(self):
        """Initialize with configuration from active profile."""
        self.config = get_config()
        self.chunk_size = self.config.processing.batch_size
        self.max_workers = self.config.processing.max_workers
        memory_str = self.config.processing.memory_limit
        self.memory_limit_bytes = self._parse_memory_limit(memory_str)
        self.min_available_memory = 2 * 1024 * 1024 * 1024  # 2 GB minimum

    @staticmethod
    def _parse_memory_limit(memory_str: str) -> int:
        """Parse memory limit string like '48GB' to bytes."""
        memory_str = memory_str.upper().strip()
        if memory_str.endswith("GB"):
            return int(float(memory_str[:-2]) * 1024 * 1024 * 1024)
        elif memory_str.endswith("MB"):
            return int(float(memory_str[:-2]) * 1024 * 1024)
        else:
            return int(memory_str)

    @staticmethod
    @timeit(log_level="debug")
    def get_memory_info() -> tuple[float, float]:
        """
        Get current memory usage information.

                Returns:
                    Tuple[float, float]: (available_gb, total_gb)
                        - available_gb: Available system memory in GB
                        - total_gb: Total system memory in GB

        """
        memory = psutil.virtual_memory()
        available_gb = memory.available / (1024**3)
        total_gb = memory.total / (1024**3)
        return available_gb, total_gb

    def should_use_chunked_processing(self, schema_name: str, file_size: int = None) -> bool:
        """
        Determine if chunked processing should be used.

                Makes intelligent decisions based on:
                - Schema type (known large schemas)
                - File size (if provided)
                - Available system memory
                - Profile memory limit

                Args:
                    schema_name: Name of the schema being processed
                    file_size: Optional file size in bytes

                Returns:
                    bool: True if chunked processing should be used

                Logic:
                    - Always chunk: CCLF7 (largest claims file)
                    - Chunk if file > 5GB
                    - Chunk if available memory < memory limit
                    - Otherwise use in-memory processing
        """
        # Check file size if provided
        if file_size and file_size > self.LARGE_FILE_SIZE:
            return True

        # Check available memory against profile limit
        available_gb, _ = self.get_memory_info()
        memory_limit_gb = self.memory_limit_bytes / (1024**3)

        if available_gb < (self.min_available_memory / (1024**3)):
            return True

        # If available memory is less than 80% of configured limit, use chunking
        if available_gb < (memory_limit_gb * 0.8):
            return True

        return False

    def get_optimal_chunk_size(self, available_memory: float = None) -> int:
        """
        Calculate optimal chunk size based on available memory and profile config.

                Args:
                    available_memory: Available memory in GB (auto-detected if None)

                Returns:
                    int: Number of rows to process per chunk

                Strategy:
                    Uses batch_size from profile config as base, then scales based on
                    available memory relative to configured memory limit.
        """
        if available_memory is None:
            available_memory, _ = self.get_memory_info()

        # Use profile batch_size as the base
        base_chunk_size = self.chunk_size

        # Scale based on available memory
        memory_limit_gb = self.memory_limit_bytes / (1024**3)
        memory_ratio = available_memory / memory_limit_gb

        # Adjust chunk size based on memory availability
        if memory_ratio < 0.3:  # Low memory
            return int(base_chunk_size * 0.5)
        elif memory_ratio < 0.6:  # Moderate memory
            return base_chunk_size
        elif memory_ratio < 0.9:  # Good memory
            return int(base_chunk_size * 1.5)
        else:  # Plenty of memory
            return int(base_chunk_size * 2)

    @classmethod
    def get_parquet_row_group_size(cls, num_columns: int = 100) -> int:
        """
        Calculate optimal Parquet row group size.

                Row group size affects:
                - Memory usage during read/write
                - Compression efficiency
                - Query performance

                Args:
                    num_columns: Approximate number of columns in dataset

                Returns:
                    int: Optimal row group size

                Formula:
                    Target ~100-200MB per row group for optimal performance
        """
        # Estimate bytes per row (rough approximation)
        bytes_per_row = num_columns * 20  # Assume average 20 bytes per column

        # Target 150MB per row group
        target_size = 150 * 1024 * 1024
        row_group_size = target_size // bytes_per_row

        # Clamp to reasonable bounds
        return max(10_000, min(1_000_000, row_group_size))
