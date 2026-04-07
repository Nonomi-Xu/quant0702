from __future__ import annotations

import polars as pl

from .kdj_shared import KDJ_K_COLUMN, compute_kdj_base


def compute_stochastic_k_line_9_3_3(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    KDJ 指标中的 K 线，参数 N=9, M1=3, M2=3。
    """
    return compute_kdj_base(frame).select("trade_date", "ts_code", KDJ_K_COLUMN)
