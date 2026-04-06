from __future__ import annotations

import polars as pl

from .macd_shared import MACD_DEA_COLUMN, compute_macd_base


def compute_macd_dea_12_26_9(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    MACD DEA 线，参数 SHORT=12, LONG=26, M=9。
    """
    return compute_macd_base(frame).select("trade_date", "ts_code", MACD_DEA_COLUMN)
