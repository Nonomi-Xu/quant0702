from __future__ import annotations

import polars as pl


BOLL_N = 20
BOLL_P = 2
BOLL_UPPER_COLUMN = f"boll_upper_{BOLL_N}_{BOLL_P}"


def compute_boll_upper_20_2(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    BOLL 上轨，参数 N=20, P=2。

    定义：

        UPPER_t = MA_20(C_t) + 2 * STD_20(C_t)
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=BOLL_N).over("ts_code").alias("boll_mid"),
        pl.col("close_hfq").rolling_std(window_size=BOLL_N).over("ts_code").alias("boll_std"),
    ).select(
        "trade_date",
        "ts_code",
        (pl.col("boll_mid") + BOLL_P * pl.col("boll_std")).alias(BOLL_UPPER_COLUMN),
    )
