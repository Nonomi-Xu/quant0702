from __future__ import annotations

import polars as pl


CDLBREAKAWAY_COLUMN = "cdlbreakaway"


def compute_cdlbreakaway(frame: pl.DataFrame) -> pl.DataFrame:
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("open_hfq").shift(4).over("ts_code").alias("open_1"),
                pl.col("close_hfq").shift(4).over("ts_code").alias("close_1"),
                pl.col("open_hfq").shift(3).over("ts_code").alias("open_2"),
                pl.col("close_hfq").shift(3).over("ts_code").alias("close_2"),
                pl.col("open_hfq").shift(2).over("ts_code").alias("open_3"),
                pl.col("close_hfq").shift(2).over("ts_code").alias("close_3"),
                pl.col("open_hfq").shift(1).over("ts_code").alias("open_4"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_4"),
                pl.col("open_hfq").alias("open_5"),
                pl.col("close_hfq").alias("close_5"),
            ]
        )
    )

    bullish = (
        (pl.col("close_1") < pl.col("open_1"))
        & (pl.col("close_2") < pl.col("open_2"))
        & (pl.col("close_3") < pl.col("open_3"))
        & (pl.col("close_4") < pl.col("open_4"))
        & (pl.col("close_5") > pl.col("open_5"))
        & (pl.col("close_5") > pl.col("open_2"))
    )
    bearish = (
        (pl.col("close_1") > pl.col("open_1"))
        & (pl.col("close_2") > pl.col("open_2"))
        & (pl.col("close_3") > pl.col("open_3"))
        & (pl.col("close_4") > pl.col("open_4"))
        & (pl.col("close_5") < pl.col("open_5"))
        & (pl.col("close_5") < pl.col("open_2"))
    )

    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(bullish).then(1).when(bearish).then(-1).otherwise(0).alias(CDLBREAKAWAY_COLUMN),
    )
