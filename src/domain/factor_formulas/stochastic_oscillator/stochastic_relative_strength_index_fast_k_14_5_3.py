from __future__ import annotations

import polars as pl

from .stochastic_relative_strength_index_shared import (
    STOCHRSI_FASTK_COLUMN,
    compute_stochastic_relative_strength_index_base,
)


def compute_stochastic_relative_strength_index_fast_k_14_5_3(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    随机相对强弱指标 STOCHRSI 中的 FASTK，参数 RSI=14, N=5, M=3。
    """
    return compute_stochastic_relative_strength_index_base(frame).select("trade_date", "ts_code", STOCHRSI_FASTK_COLUMN)
