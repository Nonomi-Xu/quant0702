from __future__ import annotations

import polars as pl


CDL3STARSINSOUTH_COLUMN = "cdl3starsinsouth"


def compute_cdl3starsinsouth(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    CDL3STARSINSOUTH / Three Stars In The South.

    命中看涨形态 -> 1
    未命中      -> 0
    """
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
    )

    signal = (
        (pl.col("close_1") < pl.col("open_1"))
        & (pl.col("close_2") < pl.col("open_2"))
        & ((pl.col("high_2") - pl.col("low_2")) < (pl.col("high_1") - pl.col("low_1")))
        & (pl.col("low_2") > pl.col("low_1"))
        & ((pl.col("high_3") - pl.col("low_3")) < (pl.col("high_2") - pl.col("low_2")))
        & (pl.col("low_3") > pl.col("low_2"))
    )

    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(1).otherwise(0).alias(CDL3STARSINSOUTH_COLUMN),
    )
