from __future__ import annotations

import polars as pl

from .power_shared import POWER_WINDOW, compute_power


BULL_POWER_COLUMN = f"bull_power_{POWER_WINDOW}"


def compute_bull_power_13(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    多头力道，参数 N=13。

    定义：

        BullPower_t = (H_t - EMA(C_t, 13)) / C_t
    """
    return compute_power(frame, "high_hfq", BULL_POWER_COLUMN)
