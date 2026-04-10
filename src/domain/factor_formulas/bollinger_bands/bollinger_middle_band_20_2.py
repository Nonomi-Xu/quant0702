from __future__ import annotations

import polars as pl


BOLL_N = 20
BOLL_P = 2
BOLL_MID_COLUMN = f"bollinger_middle_band_{BOLL_N}_{BOLL_P}"


def compute_bollinger_middle_band_20_2(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BOLL 中轨，参数 N=20, P=2。

    定义：

        MID_t = MA_20(C_t)
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=BOLL_N).over("ts_code").alias(BOLL_MID_COLUMN),
    )
