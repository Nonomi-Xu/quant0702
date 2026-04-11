from __future__ import annotations

import polars as pl


HMA_WINDOW = 3
HMA_COLUMN = f"high_moving_average_{HMA_WINDOW}"


def compute_high_moving_average_3(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("high_hfq").rolling_mean(window_size=HMA_WINDOW).over("ts_code").alias(HMA_COLUMN),
    )
