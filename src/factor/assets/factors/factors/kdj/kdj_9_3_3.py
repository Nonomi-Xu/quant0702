from __future__ import annotations

import polars as pl

from .kdj_shared import KDJ_COLUMN, compute_kdj_base


def compute_kdj_9_3_3(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    KDJ 指标主线，参数 N=9, M1=3, M2=3。

    这里按你的字段名要求，`kdj` 输出对应 J 线。
    """
    return compute_kdj_base(frame).select("trade_date", "ts_code", KDJ_COLUMN)
