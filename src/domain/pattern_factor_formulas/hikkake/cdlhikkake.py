from __future__ import annotations

import polars as pl


CDLHIKKAKE_COLUMN = "cdlhikkake"


def compute_cdlhikkake(frame: pl.DataFrame) -> pl.DataFrame:
    ordered = (
        frame.select("trade_date", "ts_code", "high_hfq", "low_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("high_hfq").shift(2).over("ts_code").alias("high_2"),
                pl.col("low_hfq").shift(2).over("ts_code").alias("low_2"),
                pl.col("high_hfq").shift(1).over("ts_code").alias("high_1"),
                pl.col("low_hfq").shift(1).over("ts_code").alias("low_1"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_1"),
            ]
        )
    )
    inside = (pl.col("high_1") < pl.col("high_2")) & (pl.col("low_1") > pl.col("low_2"))
    bullish = inside & (pl.col("high_hfq") < pl.col("high_1")) & (pl.col("low_hfq") < pl.col("low_1"))
    bearish = inside & (pl.col("high_hfq") > pl.col("high_1")) & (pl.col("low_hfq") > pl.col("low_1"))
    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(bullish).then(1).when(bearish).then(-1).otherwise(0).alias(CDLHIKKAKE_COLUMN),
    )
