from __future__ import annotations

import polars as pl


EMA_WINDOW = 5
EMA_COLUMN = f"exponential_moving_average_{EMA_WINDOW}"


def compute_exponential_moving_average_5(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    指数移动平均 EMA，参数 N=5。

    定义：

        EMA_t = alpha * C_t + (1 - alpha) * EMA_{t-1}
        alpha = 2 / (N + 1)
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").ewm_mean(span=EMA_WINDOW, adjust=False).over("ts_code").alias(EMA_COLUMN),
    )
