from __future__ import annotations

import polars as pl


DFMA_N1 = 10
DFMA_N2 = 50
DFMA_M = 10
DFMA_DIFMA_COLUMN = f"parallel_line_difference_average_{DFMA_N1}_{DFMA_N2}_{DFMA_M}"


def compute_parallel_line_difference_average_10_50_10(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    平行线差指标中的 DIFMA，参数 N1=10, N2=50, M=10。

    定义：

        DIF_t = MA_10(C_t) - MA_50(C_t)
        DIFMA_t = MA_10(DIF_t)
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=DFMA_N1).over("ts_code").alias("simple_moving_average_10"),
        pl.col("close_hfq").rolling_mean(window_size=DFMA_N2).over("ts_code").alias("simple_moving_average_50"),
    ).with_columns(
        (pl.col("simple_moving_average_10") - pl.col("simple_moving_average_50")).alias("dfma_dif_base")
    ).select(
        "trade_date",
        "ts_code",
        pl.col("dfma_dif_base").rolling_mean(window_size=DFMA_M).over("ts_code").alias(DFMA_DIFMA_COLUMN),
    )
