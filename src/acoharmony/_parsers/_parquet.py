# © 2025 HarmonyCares
# All rights reserved.


from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import parser_method, validate_file_format
from ._registry import register_parser


@register_parser("parquet")
@validate_file_format(param_name="file_path", formats=[".parquet", ".pq"])
@parser_method(
    threshold=1.0,
    validate_path="file_path",
)
def parse_parquet(file_path: Path, schema: Any, limit: int | None = None) -> pl.LazyFrame:
    """
    Parse Apache Parquet columnar storage files.

        Parquet is the preferred format for processed data due to its:
        - Efficient columnar storage
        - Built-in compression
        - Schema preservation
        - Fast read performance

        Parameters

        file_path : Path
            Path to the Parquet file
        schema : Any
            TableMetadata object (used for consistency, not parsing)
        limit : int | None
            Optional number of rows to read

        Returns

        pl.LazyFrame
            Lazily loaded Parquet data
    """
    lf = pl.scan_parquet(file_path)
    if limit:
        lf = lf.head(limit)
    return lf
