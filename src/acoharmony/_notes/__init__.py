# © 2025 HarmonyCares
# All rights reserved.

"""
Private notebook generation submodule for ACOHarmony.

Exposes the plugin singletons (``setup``, ``ui``, ``data``, ``analysis``,
``utils``, ``panels``) used by marimo notebooks. Notebooks should call
into these directly — keep notebooks declarative; put logic here.
"""

from ._analysis import AnalysisPlugins
from ._base import PluginRegistry
from ._cite import CitePlugins
from ._crosswalk import CrosswalkPlugins
from ._data import DataPlugins
from ._identity import IdentityPlugins
from ._panels import PanelPlugins
from ._quality import QualityPlugins
from ._setup import SetupPlugins
from ._ui import UIPlugins
from ._utils import UtilityPlugins
from .config import NotebookConfig
from .generator import NotebookGenerator

setup = SetupPlugins()
ui = UIPlugins()
data = DataPlugins()
analysis = AnalysisPlugins()
utils = UtilityPlugins()
panels = PanelPlugins(ui)
cite = CitePlugins()
crosswalk = CrosswalkPlugins()
identity = IdentityPlugins()
quality = QualityPlugins()

__all__ = [
    "NotebookGenerator",
    "NotebookConfig",
    "PluginRegistry",
    "SetupPlugins",
    "UIPlugins",
    "DataPlugins",
    "AnalysisPlugins",
    "UtilityPlugins",
    "PanelPlugins",
    "CitePlugins",
    "CrosswalkPlugins",
    "IdentityPlugins",
    "QualityPlugins",
    "setup",
    "ui",
    "data",
    "analysis",
    "utils",
    "panels",
    "cite",
    "crosswalk",
    "identity",
    "quality",
]
