from __future__ import annotations

import polars as pl

from .bias_difference_shared import MABIAS_COLUMN, compute_bias_difference_base


def compute_moving_average_bias_36_6(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_bias_difference_base(frame).select("trade_date", "ts_code", MABIAS_COLUMN)
