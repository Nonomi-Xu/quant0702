from __future__ import annotations

import polars as pl


EXPMA_WINDOW = 12
EXPMA_COLUMN = f"expma_{EXPMA_WINDOW}"


def compute_expma_12(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    EMA 指数平均数指标，参数 N1=12。

    定义：

        EXPMA_t = alpha * C_t + (1 - alpha) * EXPMA_{t-1}
        alpha = 2 / (12 + 1)
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").ewm_mean(span=EXPMA_WINDOW, adjust=False).over("ts_code").alias(EXPMA_COLUMN),
    )
