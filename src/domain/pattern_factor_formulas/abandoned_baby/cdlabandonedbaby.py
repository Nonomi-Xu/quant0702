from __future__ import annotations

import polars as pl


CDLABANDONEDBABY_COLUMN = "cdlabandonedbaby"


def compute_cdlabandonedbaby(frame: pl.DataFrame) -> pl.DataFrame:
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "high_hfq", "low_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("open_hfq").shift(2).over("ts_code").alias("open_1"),
                pl.col("high_hfq").shift(2).over("ts_code").alias("high_1"),
                pl.col("low_hfq").shift(2).over("ts_code").alias("low_1"),
                pl.col("close_hfq").shift(2).over("ts_code").alias("close_1"),
                pl.col("open_hfq").shift(1).over("ts_code").alias("open_2"),
                pl.col("high_hfq").shift(1).over("ts_code").alias("high_2"),
                pl.col("low_hfq").shift(1).over("ts_code").alias("low_2"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_2"),
                pl.col("open_hfq").alias("open_3"),
                pl.col("high_hfq").alias("high_3"),
                pl.col("low_hfq").alias("low_3"),
                pl.col("close_hfq").alias("close_3"),
            ]
        )
        .with_columns(
            (((pl.col("open_2") - pl.col("close_2")).abs()) <= ((pl.col("high_2") - pl.col("low_2")) * 0.1)).alias("is_doji_2")
        )
    )

    bullish = (
        (pl.col("close_1") < pl.col("open_1"))
        & pl.col("is_doji_2")
        & (pl.col("high_2") < pl.col("low_1"))
        & (pl.col("close_3") > pl.col("open_3"))
        & (pl.col("low_3") > pl.col("high_2"))
    )
    bearish = (
        (pl.col("close_1") > pl.col("open_1"))
        & pl.col("is_doji_2")
        & (pl.col("low_2") > pl.col("high_1"))
        & (pl.col("close_3") < pl.col("open_3"))
        & (pl.col("high_3") < pl.col("low_2"))
    )

    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(bullish).then(1).when(bearish).then(-1).otherwise(0).alias(CDLABANDONEDBABY_COLUMN),
    )
