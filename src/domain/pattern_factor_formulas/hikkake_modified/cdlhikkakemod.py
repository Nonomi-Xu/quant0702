from __future__ import annotations

import polars as pl


CDLHIKKAKEMOD_COLUMN = "cdlhikkakemod"


def compute_cdlhikkakemod(frame: pl.DataFrame) -> pl.DataFrame:
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "high_hfq", "low_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("high_hfq").shift(2).over("ts_code").alias("high_2"),
                pl.col("low_hfq").shift(2).over("ts_code").alias("low_2"),
                pl.col("high_hfq").shift(1).over("ts_code").alias("high_1"),
                pl.col("low_hfq").shift(1).over("ts_code").alias("low_1"),
                pl.col("open_hfq").shift(1).over("ts_code").alias("open_1"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_1"),
            ]
        )
    )
    inside = (pl.col("high_1") < pl.col("high_2")) & (pl.col("low_1") > pl.col("low_2"))
    prev_bull = pl.col("close_1") > pl.col("open_1")
    prev_bear = pl.col("close_1") < pl.col("open_1")
    bullish = inside & prev_bear & (pl.col("high_hfq") > pl.col("high_1")) & (pl.col("close_hfq") > pl.col("high_1"))
    bearish = inside & prev_bull & (pl.col("low_hfq") < pl.col("low_1")) & (pl.col("close_hfq") < pl.col("low_1"))
    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(bullish).then(1).when(bearish).then(-1).otherwise(0).alias(CDLHIKKAKEMOD_COLUMN),
    )
