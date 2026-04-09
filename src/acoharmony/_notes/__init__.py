# © 2025 HarmonyCares
# All rights reserved.

"""
Private notebook generation submodule for ACOHarmony.

Provides:
- Schema-driven notebook generation using Jinja2 templates
- DRY plugin registry for reusable marimo notebook functions
- Automatic notebook creation based on schema and catalog metadata

"""

from .config import NotebookConfig
from .generator import NotebookGenerator
from .plugins import analysis, data, setup, ui, utils

__all__ = [
    "NotebookGenerator",
    "NotebookConfig",
    "setup",
    "ui",
    "data",
    "analysis",
    "utils",
]
