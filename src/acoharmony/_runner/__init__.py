# © 2025 HarmonyCares
# All rights reserved.

"""
Private implementation of transformation runner system.

This module contains the reorganized runner components split into
logical modules for better maintainability. Components are organized
by functionality rather than class hierarchy.

The public API is exposed through the main _runner module.
"""

# Import the main runner
from ._core import TransformRunner
from ._file_processor import FileProcessor

# Import all components to register them
from ._memory import MemoryManager
from ._pipeline_executor import PipelineExecutor
from ._registry import RunnerRegistry, register_operation, register_processor
from ._schema_transformer import SchemaTransformer

__all__ = [
    "RunnerRegistry",
    "register_operation",
    "register_processor",
    "TransformRunner",
    # Component classes
    "MemoryManager",
    "FileProcessor",
    "SchemaTransformer",
    "PipelineExecutor",
]
