from __future__ import annotations

import polars as pl


CDLTASUKIGAP_COLUMN = "cdltasukigap"


def compute_cdltasukigap(frame: pl.DataFrame) -> pl.DataFrame:
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
    bullish = (
        (pl.col("close_1") > pl.col("open_1"))
        & (pl.col("close_hfq") < pl.col("open_hfq"))
        & (pl.col("low_hfq") > pl.col("high_1"))
        & (pl.col("close_hfq") < pl.col("close_1"))
        & (pl.col("close_hfq") > pl.col("high_1"))
    )
    bearish = (
        (pl.col("close_1") < pl.col("open_1"))
        & (pl.col("close_hfq") > pl.col("open_hfq"))
        & (pl.col("high_hfq") < pl.col("low_1"))
        & (pl.col("close_hfq") > pl.col("close_1"))
        & (pl.col("close_hfq") < pl.col("low_1"))
    )
    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(bullish).then(1).when(bearish).then(-1).otherwise(0).alias(CDLTASUKIGAP_COLUMN),
    )
