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
from ._calendar import CalendarPlugins
from ._cite import CitePlugins
from ._crosswalk import CrosswalkPlugins
from ._data import DataPlugins
from ._identity import IdentityPlugins
from ._panels import PanelPlugins
from ._provider import ProviderPlugins
from ._quality import QualityPlugins
from ._reach import ReachPlugins
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
calendar = CalendarPlugins()
cite = CitePlugins()
crosswalk = CrosswalkPlugins()
identity = IdentityPlugins()
provider = ProviderPlugins()
quality = QualityPlugins()
reach = ReachPlugins()

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
    "CalendarPlugins",
    "CitePlugins",
    "CrosswalkPlugins",
    "IdentityPlugins",
    "ProviderPlugins",
    "QualityPlugins",
    "ReachPlugins",
    "setup",
    "ui",
    "data",
    "analysis",
    "utils",
    "panels",
    "calendar",
    "cite",
    "crosswalk",
    "identity",
    "provider",
    "quality",
    "reach",
]
