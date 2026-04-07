from __future__ import annotations

import polars as pl


DFMA_N1 = 10
DFMA_N2 = 50
DFMA_DIF_COLUMN = f"parallel_line_difference_{DFMA_N1}_{DFMA_N2}"


def compute_parallel_line_difference_10_50(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    平行线差指标中的 DIF，参数 N1=10, N2=50。

    定义：

        DIF_t = MA_10(C_t) - MA_50(C_t)
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=DFMA_N1).over("ts_code").alias("simple_moving_average_10"),
        pl.col("close_hfq").rolling_mean(window_size=DFMA_N2).over("ts_code").alias("simple_moving_average_50"),
    ).select(
        "trade_date",
        "ts_code",
        (pl.col("simple_moving_average_10") - pl.col("simple_moving_average_50")).alias(DFMA_DIF_COLUMN),
    )
