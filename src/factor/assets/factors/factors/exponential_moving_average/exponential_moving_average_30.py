from __future__ import annotations

import polars as pl


EMA_WINDOW = 30
EMA_COLUMN = f"exponential_moving_average_{EMA_WINDOW}"


def compute_exponential_moving_average_30(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    指数移动平均 EMA，参数 N=30。
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").ewm_mean(span=EMA_WINDOW, adjust=False).over("ts_code").alias(EMA_COLUMN),
    )
