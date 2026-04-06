from __future__ import annotations

import polars as pl

from .kdj_shared import KDJ_D_COLUMN, compute_kdj_base


def compute_kdj_d_9_3_3(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    KDJ 指标中的 D 线，参数 N=9, M1=3, M2=3。
    """
    return compute_kdj_base(frame).select("trade_date", "ts_code", KDJ_D_COLUMN)
