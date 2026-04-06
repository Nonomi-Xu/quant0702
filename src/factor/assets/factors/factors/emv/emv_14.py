from __future__ import annotations

import polars as pl

from .emv_shared import EMV_COLUMN, compute_emv_base


def compute_emv_14(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    简易波动指标 EMV，参数 N=14。

    常见定义：

        VOLUME_t = MA(VOL_t, 14) / VOL_t
        MID_t = 100 * ((H_t + L_t) - (H_{t-1} + L_{t-1})) / (H_t + L_t)
        EMV_t = MA(MID_t * VOLUME_t * (H_t - L_t) / MA(H_t - L_t, 14), 14)
    """
    return compute_emv_base(frame).select("trade_date", "ts_code", EMV_COLUMN)
