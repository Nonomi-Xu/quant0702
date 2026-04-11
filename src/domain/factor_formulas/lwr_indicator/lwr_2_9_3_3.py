from __future__ import annotations

import polars as pl

from .lwr_shared import LWR2_COLUMN, compute_lwr_base


def compute_lwr_2_9_3_3(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_lwr_base(frame).select("trade_date", "ts_code", LWR2_COLUMN)
