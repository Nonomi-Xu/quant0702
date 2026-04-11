from __future__ import annotations

import polars as pl


CDLCONCEALBABYSWALLOW_COLUMN = "cdlconcealbabyswallow"


def compute_cdlconcealbabyswallow(frame: pl.DataFrame) -> pl.DataFrame:
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "high_hfq", "low_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("open_hfq").shift(3).over("ts_code").alias("open_1"),
                pl.col("close_hfq").shift(3).over("ts_code").alias("close_1"),
                pl.col("open_hfq").shift(2).over("ts_code").alias("open_2"),
                pl.col("close_hfq").shift(2).over("ts_code").alias("close_2"),
                pl.col("open_hfq").shift(1).over("ts_code").alias("open_3"),
                pl.col("high_hfq").shift(1).over("ts_code").alias("high_3"),
                pl.col("low_hfq").shift(1).over("ts_code").alias("low_3"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_3"),
                pl.col("open_hfq").alias("open_4"),
                pl.col("high_hfq").alias("high_4"),
                pl.col("low_hfq").alias("low_4"),
                pl.col("close_hfq").alias("close_4"),
            ]
        )
    )

    signal = (
        (pl.col("close_1") < pl.col("open_1"))
        & (pl.col("close_2") < pl.col("open_2"))
        & (pl.col("close_3") < pl.col("open_3"))
        & (pl.col("high_3") < pl.col("open_2"))
        & (pl.col("close_4") < pl.col("open_4"))
        & (pl.col("high_4") > pl.col("high_3"))
        & (pl.col("low_4") < pl.col("low_3"))
    )

    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(1).otherwise(0).alias(CDLCONCEALBABYSWALLOW_COLUMN),
    )
