from __future__ import annotations

import polars as pl


MA_WINDOW = 120
MA_COLUMN = f"simple_moving_average_{MA_WINDOW}"


def compute_simple_moving_average_120(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("close_hfq").rolling_mean(window_size=MA_WINDOW).over("ts_code").alias(MA_COLUMN),
    )
