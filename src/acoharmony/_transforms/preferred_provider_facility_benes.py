# © 2025 HarmonyCares
# All rights reserved.

"""
Pipeline-stage adapter for the per-``(tin, npi, member_id)`` facility bene detail.

Thin shim that satisfies the ``PipelineStage.module.execute(executor)``
contract used by :mod:`acoharmony._pipes._stage`. The actual data logic
lives in :func:`acoharmony._transforms._preferred_provider_claims.build_bene_detail_lazy`.
"""

from typing import Any

import polars as pl

from .._decor8 import measure_dataframe_size
from ..medallion import MedallionLayer
from ._preferred_provider_claims import build_bene_detail_lazy


@measure_dataframe_size()
def execute(executor: Any) -> pl.LazyFrame:
    """Return the bene-detail LazyFrame ready for ``sink_parquet`` streaming."""
    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)
    gold_path = storage.get_path(MedallionLayer.GOLD)
    return build_bene_detail_lazy(silver_path, gold_path)
