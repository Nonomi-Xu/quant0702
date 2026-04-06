from __future__ import annotations

import polars as pl

from .emv_shared import MAEMV_COLUMN, compute_emv_base


def compute_maemv_14_9(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    简易波动指标平滑线 MAEMV，参数 N=14, M=9。

    定义：

        MAEMV_t = MA(EMV_t, 9)
    """
    return compute_emv_base(frame).select("trade_date", "ts_code", MAEMV_COLUMN)
