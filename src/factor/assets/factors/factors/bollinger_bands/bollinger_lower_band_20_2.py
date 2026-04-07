from __future__ import annotations

import polars as pl


BOLL_N = 20
BOLL_P = 2
BOLL_LOWER_COLUMN = f"bollinger_lower_band_{BOLL_N}_{BOLL_P}"


def compute_bollinger_lower_band_20_2(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BOLL 下轨，参数 N=20, P=2。

    定义：

        LOWER_t = MA_20(C_t) - 2 * STD_20(C_t)
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=BOLL_N).over("ts_code").alias("boll_mid"),
        pl.col("close_hfq").rolling_std(window_size=BOLL_N).over("ts_code").alias("boll_std"),
    ).select(
        "trade_date",
        "ts_code",
        (pl.col("boll_mid") - BOLL_P * pl.col("boll_std")).alias(BOLL_LOWER_COLUMN),
    )
