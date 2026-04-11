from __future__ import annotations

import polars as pl


CDLKICKINGBYLENGTH_COLUMN = "cdlkickingbylength"


def compute_cdlkickingbylength(frame: pl.DataFrame) -> pl.DataFrame:
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "high_hfq", "low_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("open_hfq").shift(1).over("ts_code").alias("open_1"),
                pl.col("high_hfq").shift(1).over("ts_code").alias("high_1"),
                pl.col("low_hfq").shift(1).over("ts_code").alias("low_1"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_1"),
            ]
        )
    )
    prev_range = pl.col("high_1") - pl.col("low_1")
    curr_range = pl.col("high_hfq") - pl.col("low_hfq")
    prev_bull_marubozu = (
        (pl.col("close_1") > pl.col("open_1"))
        & ((pl.col("high_1") - pl.col("close_1")).abs() <= prev_range * 0.05)
        & ((pl.col("open_1") - pl.col("low_1")).abs() <= prev_range * 0.05)
    )
    prev_bear_marubozu = (
        (pl.col("close_1") < pl.col("open_1"))
        & ((pl.col("high_1") - pl.col("open_1")).abs() <= prev_range * 0.05)
        & ((pl.col("close_1") - pl.col("low_1")).abs() <= prev_range * 0.05)
    )
    curr_bull_marubozu = (
        (pl.col("close_hfq") > pl.col("open_hfq"))
        & ((pl.col("high_hfq") - pl.col("close_hfq")).abs() <= curr_range * 0.05)
        & ((pl.col("open_hfq") - pl.col("low_hfq")).abs() <= curr_range * 0.05)
    )
    curr_bear_marubozu = (
        (pl.col("close_hfq") < pl.col("open_hfq"))
        & ((pl.col("high_hfq") - pl.col("open_hfq")).abs() <= curr_range * 0.05)
        & ((pl.col("close_hfq") - pl.col("low_hfq")).abs() <= curr_range * 0.05)
    )
    bullish = prev_bear_marubozu & curr_bull_marubozu & (pl.col("open_hfq") > pl.col("high_1")) & (curr_range > prev_range)
    bearish = prev_bull_marubozu & curr_bear_marubozu & (pl.col("open_hfq") < pl.col("low_1")) & (curr_range > prev_range)
    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(bullish).then(1).when(bearish).then(-1).otherwise(0).alias(CDLKICKINGBYLENGTH_COLUMN),
    )
