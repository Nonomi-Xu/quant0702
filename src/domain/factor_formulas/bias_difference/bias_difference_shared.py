from __future__ import annotations

import polars as pl


BIAS36_COLUMN = "bias_36"
BIAS612_COLUMN = "bias_612"
MABIAS_COLUMN = "moving_average_bias_36_6"


def compute_bias_difference_base(frame: pl.DataFrame) -> pl.DataFrame:
    return (
        frame.select("trade_date", "ts_code", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("close_hfq").rolling_mean(window_size=3).over("ts_code").alias("ma_3"),
                pl.col("close_hfq").rolling_mean(window_size=6).over("ts_code").alias("ma_6"),
                pl.col("close_hfq").rolling_mean(window_size=12).over("ts_code").alias("ma_12"),
            ]
        )
        .with_columns(
            [
                (pl.col("ma_3") - pl.col("ma_6")).alias(BIAS36_COLUMN),
                (pl.col("ma_6") - pl.col("ma_12")).alias(BIAS612_COLUMN),
            ]
        )
        .with_columns(
            pl.col(BIAS36_COLUMN).rolling_mean(window_size=6).over("ts_code").alias(MABIAS_COLUMN)
        )
    )
