from __future__ import annotations

import polars as pl


LMA_WINDOW = 10
LMA_COLUMN = f"low_moving_average_{LMA_WINDOW}"


def compute_low_moving_average_10(frame: pl.DataFrame) -> pl.DataFrame:
    return frame.select(
        "trade_date",
        "ts_code",
        pl.col("low_hfq").rolling_mean(window_size=LMA_WINDOW).over("ts_code").alias(LMA_COLUMN),
    )
