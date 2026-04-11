from __future__ import annotations

import polars as pl

from .tapi_shared import MATAPI_COLUMN, compute_tapi_base


def compute_moving_average_tapi_6(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_tapi_base(frame).select("trade_date", "ts_code", MATAPI_COLUMN)
