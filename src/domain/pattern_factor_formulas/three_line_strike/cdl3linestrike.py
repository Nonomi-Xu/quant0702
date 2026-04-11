from __future__ import annotations

import polars as pl


CDL3LINESTRIKE_COLUMN = "cdl3linestrike"


def compute_cdl3linestrike(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    CDL3LINESTRIKE / Three-Line Strike.

    看涨命中 -> 1
    看跌命中 -> -1
    未命中   -> 0
    """
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("open_hfq").shift(3).over("ts_code").alias("open_1"),
                pl.col("close_hfq").shift(3).over("ts_code").alias("close_1"),
                pl.col("open_hfq").shift(2).over("ts_code").alias("open_2"),
                pl.col("close_hfq").shift(2).over("ts_code").alias("close_2"),
                pl.col("open_hfq").shift(1).over("ts_code").alias("open_3"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_3"),
                pl.col("open_hfq").alias("open_4"),
                pl.col("close_hfq").alias("close_4"),
            ]
        )
    )

    bullish = (
        (pl.col("close_1") < pl.col("open_1"))
        & (pl.col("close_2") < pl.col("open_2"))
        & (pl.col("close_3") < pl.col("open_3"))
        & (pl.col("close_2") < pl.col("close_1"))
        & (pl.col("close_3") < pl.col("close_2"))
        & (pl.col("close_4") > pl.col("open_4"))
        & (pl.col("open_4") < pl.col("close_3"))
        & (pl.col("close_4") > pl.col("open_1"))
    )
    bearish = (
        (pl.col("close_1") > pl.col("open_1"))
        & (pl.col("close_2") > pl.col("open_2"))
        & (pl.col("close_3") > pl.col("open_3"))
        & (pl.col("close_2") > pl.col("close_1"))
        & (pl.col("close_3") > pl.col("close_2"))
        & (pl.col("close_4") < pl.col("open_4"))
        & (pl.col("open_4") > pl.col("close_3"))
        & (pl.col("close_4") < pl.col("open_1"))
    )

    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(bullish).then(1).when(bearish).then(-1).otherwise(0).alias(CDL3LINESTRIKE_COLUMN),
    )
