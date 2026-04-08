from __future__ import annotations

import polars as pl

from .power_shared import POWER_WINDOW, compute_power


BEAR_POWER_COLUMN = f"bear_power_{POWER_WINDOW}"


def compute_bear_power_13(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    空头力道，参数 N=13。

    定义：

        BearPower_t = (L_t - EMA(C_t, 13)) / C_t
    """
    return compute_power(frame, "low_hfq", BEAR_POWER_COLUMN)
