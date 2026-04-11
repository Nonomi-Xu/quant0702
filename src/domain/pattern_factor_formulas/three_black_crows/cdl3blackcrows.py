from __future__ import annotations

import polars as pl


CDL3BLACKCROWS_COLUMN = "cdl3blackcrows"


def compute_cdl3blackcrows(frame: pl.DataFrame) -> pl.DataFrame:
    r"""
    CDL3BLACKCROWS / Three Black Crows.

    命中看跌形态 -> -1
    未命中      -> 0
    """
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("open_hfq").shift(2).over("ts_code").alias("open_1"),
                pl.col("close_hfq").shift(2).over("ts_code").alias("close_1"),
                pl.col("open_hfq").shift(1).over("ts_code").alias("open_2"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_2"),
                pl.col("open_hfq").alias("open_3"),
                pl.col("close_hfq").alias("close_3"),
            ]
        )
    )

    signal = (
        (pl.col("close_1") < pl.col("open_1"))
        & (pl.col("close_2") < pl.col("open_2"))
        & (pl.col("close_3") < pl.col("open_3"))
        & (pl.col("open_2") < pl.col("open_1"))
        & (pl.col("open_2") > pl.col("close_1"))
        & (pl.col("open_3") < pl.col("open_2"))
        & (pl.col("open_3") > pl.col("close_2"))
        & (pl.col("close_2") < pl.col("close_1"))
        & (pl.col("close_3") < pl.col("close_2"))
    )

    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(-1).otherwise(0).alias(CDL3BLACKCROWS_COLUMN),
    )
