from __future__ import annotations

import polars as pl


CDLHARAMICROSS_COLUMN = "cdlharamicross"


def compute_cdlharamicross(frame: pl.DataFrame) -> pl.DataFrame:
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "high_hfq", "low_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("open_hfq").shift(1).over("ts_code").alias("open_1"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_1"),
            ]
        )
    )
    body = (pl.col("close_hfq") - pl.col("open_hfq")).abs()
    range_ = pl.col("high_hfq") - pl.col("low_hfq")
    is_doji = body <= range_ * 0.1
    prev_high = pl.max_horizontal("open_1", "close_1")
    prev_low = pl.min_horizontal("open_1", "close_1")
    curr_high = pl.max_horizontal("open_hfq", "close_hfq")
    curr_low = pl.min_horizontal("open_hfq", "close_hfq")
    bullish = (
        (pl.col("close_1") < pl.col("open_1"))
        & is_doji
        & (curr_high <= prev_high)
        & (curr_low >= prev_low)
    )
    bearish = (
        (pl.col("close_1") > pl.col("open_1"))
        & is_doji
        & (curr_high <= prev_high)
        & (curr_low >= prev_low)
    )
    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(bullish).then(1).when(bearish).then(-1).otherwise(0).alias(CDLHARAMICROSS_COLUMN),
    )
