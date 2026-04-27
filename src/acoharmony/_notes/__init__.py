# © 2025 HarmonyCares
# All rights reserved.

"""
Private notebook generation submodule for ACOHarmony.

Exposes the plugin singletons (``setup``, ``ui``, ``data``, ``analysis``,
``utils``, ``panels``) used by marimo notebooks. Notebooks should call
into these directly — keep notebooks declarative; put logic here.
"""

from ._acr import AcrPlugins
from ._analysis import AnalysisPlugins
from ._base import PluginRegistry
from ._calendar import CalendarPlugins
from ._cite import CitePlugins
from ._crosswalk import CrosswalkPlugins
from ._data import DataPlugins
from ._hdai import HdaiPlugins
from ._hedr import HedrPlugins
from ._high_needs import HighNeedsPlugins
from ._home_visit import HomeVisitPlugins
from ._identity import IdentityPlugins
from ._mailers import MailersPlugins
from ._panels import PanelPlugins
from ._pfs import PfsPlugins
from ._pmpm import PmpmPlugins
from ._provider import ProviderPlugins
from ._quality import QualityPlugins
from ._reach import ReachPlugins
from ._setup import SetupPlugins
from ._sva import SvaPlugins
from ._tparc import TparcPlugins
from ._wound_care import WoundCarePlugins
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
acr = AcrPlugins()
calendar = CalendarPlugins()
cite = CitePlugins()
crosswalk = CrosswalkPlugins()
hdai = HdaiPlugins()
hedr = HedrPlugins()
high_needs = HighNeedsPlugins()
home_visit = HomeVisitPlugins()
identity = IdentityPlugins()
mailers = MailersPlugins()
pfs = PfsPlugins()
pmpm = PmpmPlugins()
provider = ProviderPlugins()
quality = QualityPlugins()
reach = ReachPlugins()
sva = SvaPlugins()
tparc = TparcPlugins()
wound_care = WoundCarePlugins()

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
    "AcrPlugins",
    "CalendarPlugins",
    "CitePlugins",
    "CrosswalkPlugins",
    "HdaiPlugins",
    "HedrPlugins",
    "HighNeedsPlugins",
    "HomeVisitPlugins",
    "IdentityPlugins",
    "MailersPlugins",
    "PfsPlugins",
    "PmpmPlugins",
    "ProviderPlugins",
    "QualityPlugins",
    "ReachPlugins",
    "SvaPlugins",
    "TparcPlugins",
    "WoundCarePlugins",
    "setup",
    "ui",
    "data",
    "analysis",
    "utils",
    "panels",
    "acr",
    "calendar",
    "cite",
    "crosswalk",
    "hdai",
    "hedr",
    "high_needs",
    "home_visit",
    "identity",
    "mailers",
    "pfs",
    "pmpm",
    "provider",
    "quality",
    "reach",
    "sva",
    "tparc",
    "wound_care",
]
