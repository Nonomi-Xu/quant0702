from __future__ import annotations

import polars as pl


DEMA_10_COLUMN = "double_exponential_moving_average_10"
DEMA_20_COLUMN = "double_exponential_moving_average_20"
DEMA_60_COLUMN = "double_exponential_moving_average_60"


def compute_dema(frame: pl.DataFrame, window: int, column_name: str) -> pl.DataFrame:
    ema_1 = pl.col("close_hfq").ewm_mean(span=window, adjust=False).over("ts_code")
    ema_2 = ema_1.ewm_mean(span=window, adjust=False).over("ts_code")
    return frame.select(
        "trade_date",
        "ts_code",
        (2 * ema_1 - ema_2).alias(column_name),
    )
