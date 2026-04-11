from __future__ import annotations

import polars as pl

from .rsi_shared import RSI_14_COLUMN, compute_rsi


def compute_relative_strength_index_14(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    相对强弱指标 RSI，参数 N=14。
    """
    return compute_rsi(frame, window=14, column_name=RSI_14_COLUMN).select(
        "trade_date",
        "ts_code",
        RSI_14_COLUMN,
    )
