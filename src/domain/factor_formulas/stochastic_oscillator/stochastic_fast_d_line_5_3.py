from __future__ import annotations

import polars as pl

from .stochastic_fast_shared import STOCHF_FASTD_COLUMN, compute_stochastic_fast_base


def compute_stochastic_fast_d_line_5_3(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    快速随机指标 STOCHF 中的 FASTD，参数 N=5, M=3。
    """
    return compute_stochastic_fast_base(frame).select("trade_date", "ts_code", STOCHF_FASTD_COLUMN)
