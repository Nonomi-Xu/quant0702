from __future__ import annotations

import polars as pl


VMA_WINDOW = 5
VMA_COLUMN = f"variation_moving_average_{VMA_WINDOW}"


def compute_variation_moving_average_5(frame: pl.DataFrame) -> pl.DataFrame:
    vv = (pl.col("high_hfq") + pl.col("open_hfq") + pl.col("low_hfq") + pl.col("close_hfq")) / 4
    return frame.select(
        "trade_date",
        "ts_code",
        vv.rolling_mean(window_size=VMA_WINDOW).over("ts_code").alias(VMA_COLUMN),
    )
