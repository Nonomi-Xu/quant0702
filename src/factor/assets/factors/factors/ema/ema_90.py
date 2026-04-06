from __future__ import annotations

import polars as pl


EMA_WINDOW = 90
EMA_COLUMN = f"ema_{EMA_WINDOW}"


def compute_ema_90(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    指数移动平均 EMA，参数 N=90。
    """
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").ewm_mean(span=EMA_WINDOW, adjust=False).over("ts_code").alias(EMA_COLUMN),
    )
