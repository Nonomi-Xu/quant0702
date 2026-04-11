from __future__ import annotations

import polars as pl

from .lwr_shared import LWR1_COLUMN, compute_lwr_base


def compute_lwr_1_9_3_3(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_lwr_base(frame).select("trade_date", "ts_code", LWR1_COLUMN)
