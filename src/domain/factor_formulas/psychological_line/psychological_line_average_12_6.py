from __future__ import annotations

import polars as pl


PSY_N = 12
PSY_M = 6
PSYMA_COLUMN = f"psychological_line_average_{PSY_N}_{PSY_M}"


def compute_psychological_line_average_12_6(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    PSY 心理线平滑指标，参数 N=12, M=6。

    定义：

        PSY_t = COUNT(C_t > C_{t-1}, 12) / 12 * 100
        PSYMA_t = MA(PSY_t, 6)
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(pl.col("close_hfq") > pl.col("close_hfq").shift(1).over("ts_code"))
        .then(1.0)
        .otherwise(0.0)
        .alias("up_flag"),
    ).with_columns(
        (pl.col("up_flag").rolling_sum(window_size=PSY_N).over("ts_code") / PSY_N * 100).alias("psy_base")
    ).select(
        "trade_date",
        "ts_code",
        pl.col("psy_base").rolling_mean(window_size=PSY_M).over("ts_code").alias(PSYMA_COLUMN),
    )
