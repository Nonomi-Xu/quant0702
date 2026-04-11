from __future__ import annotations

import polars as pl

from .tapi_shared import TAPI_COLUMN, compute_tapi_base


def compute_tapi(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_tapi_base(frame).select("trade_date", "ts_code", TAPI_COLUMN)
