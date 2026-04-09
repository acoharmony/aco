# © 2025 HarmonyCares
# All rights reserved.

"""
Private implementation of file parsing system.

This module contains the reorganized parser implementations split into
logical components for better maintainability. All parsers are registered
through a decorator-based registry system.

The public API is exposed through the main parsers module.
"""

# Core parser imports — always available (skinny install)
from . import (
    _csv,
    _delimited,
    _ecfr_xml,
    _excel,
    _federal_register_xml,
    _fixed_width,
    _json,
    _mabel_log,
    _participant_list_excel,
    _tparc,
    _xml,
)
from ._date_extraction import extract_file_date

try:
    from ._model_aware import ModelAwareCoercer
except ImportError:
    ModelAwareCoercer = None
from ._parquet import parse_parquet
from ._registry import ParserRegistry, register_parser
from ._source_tracking import add_source_tracking

# Import transformation functions
from ._transformations import (
    apply_column_types,
    apply_schema_transformations,
)

# Optional parser imports — require full package dependencies
# These gracefully degrade when their third-party deps are missing
try:
    from . import _pdf as _pdf  # noqa: F401 (imported for registration)
except ImportError:
    pass

try:
    from . import _html as _html  # noqa: F401 (imported for registration)
except ImportError:
    pass

try:
    from . import _latex as _latex  # noqa: F401 (imported for registration)
except ImportError:
    pass

try:
    from . import _markdown as _markdown  # noqa: F401 (imported for registration)
except ImportError:
    pass

try:
    from . import _excel_multi_sheet as _excel_multi_sheet  # noqa: F401 (imported for registration)
except ImportError:
    pass

__all__ = [
    # Core parser modules (always available)
    "_csv",
    "_delimited",
    "_ecfr_xml",
    "_excel",
    "_federal_register_xml",
    "_fixed_width",
    "_json",
    "_mabel_log",
    "_participant_list_excel",
    "_tparc",
    "_xml",
    # Public API
    "ParserRegistry",
    "register_parser",
    "extract_file_date",
    "add_source_tracking",
    "parse_parquet",
    "apply_column_types",
    "apply_schema_transformations",
    "ModelAwareCoercer",
]
