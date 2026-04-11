from __future__ import annotations

import polars as pl

from .bias_difference_shared import BIAS36_COLUMN, compute_bias_difference_base


def compute_bias_36(frame: pl.DataFrame) -> pl.DataFrame:
    return compute_bias_difference_base(frame).select("trade_date", "ts_code", BIAS36_COLUMN)
