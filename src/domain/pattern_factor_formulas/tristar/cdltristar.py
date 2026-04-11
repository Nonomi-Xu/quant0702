from __future__ import annotations

import polars as pl


CDLTRISTAR_COLUMN = "cdltristar"


def compute_cdltristar(frame: pl.DataFrame) -> pl.DataFrame:
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
            ]
        )
    )
    doji_1 = (pl.col("close_1") - pl.col("open_1")).abs() <= (pl.col("high_1") - pl.col("low_1")) * 0.1
    doji_2 = (pl.col("close_2") - pl.col("open_2")).abs() <= (pl.col("high_2") - pl.col("low_2")) * 0.1
    doji_3 = (pl.col("close_hfq") - pl.col("open_hfq")).abs() <= (pl.col("high_hfq") - pl.col("low_hfq")) * 0.1
    bullish = doji_1 & doji_2 & doji_3 & (pl.col("high_2") < pl.col("low_1")) & (pl.col("high_hfq") < pl.col("low_2"))
    bearish = doji_1 & doji_2 & doji_3 & (pl.col("low_2") > pl.col("high_1")) & (pl.col("low_hfq") > pl.col("high_2"))
    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(bullish).then(1).when(bearish).then(-1).otherwise(0).alias(CDLTRISTAR_COLUMN),
    )
