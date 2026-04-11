from __future__ import annotations

import polars as pl

from .dkx_shared import MADKX_COLUMN, compute_dkx_base


def compute_moving_average_dkx_10(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_dkx_base(frame).select("trade_date", "ts_code", MADKX_COLUMN)
