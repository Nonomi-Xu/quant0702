from __future__ import annotations

import polars as pl

from .commodity_channel_index_shared import compute_commodity_channel_index


CCI_WINDOW = 14
CCI_COLUMN = f"commodity_channel_index_{CCI_WINDOW}"


def compute_commodity_channel_index_14(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    CCI 顺势指标，参数 N=14。

    定义：

        TP_t = (H_t + L_t + C_t) / 3
        MA_t = mean(TP_t, ..., TP_{t-13})
        MD_t = mean(|TP_t - MA_t|, ..., |TP_{t-13} - MA_t|)
        CCI_t = (TP_t - MA_t) / (0.015 * MD_t)
    """
    return compute_commodity_channel_index(frame, CCI_WINDOW, CCI_COLUMN)
