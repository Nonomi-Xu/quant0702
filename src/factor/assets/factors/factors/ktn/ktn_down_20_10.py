from __future__ import annotations

import polars as pl

from .ktn_shared import KTN_DOWN_COLUMN, compute_ktn_base


def compute_ktn_down_20_10(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    肯特纳交易通道下轨，参数 N=20, M=10。
    """
    return compute_ktn_base(frame).select("trade_date", "ts_code", KTN_DOWN_COLUMN)
