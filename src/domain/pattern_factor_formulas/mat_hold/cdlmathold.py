from __future__ import annotations

import polars as pl


CDLMATHOLD_COLUMN = "cdlmathold"


def compute_cdlmathold(frame: pl.DataFrame) -> pl.DataFrame:
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("open_hfq").shift(4).over("ts_code").alias("open_1"),
                pl.col("close_hfq").shift(4).over("ts_code").alias("close_1"),
                pl.col("close_hfq").shift(3).over("ts_code").alias("close_2"),
                pl.col("close_hfq").shift(2).over("ts_code").alias("close_3"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_4"),
            ]
        )
    )
    signal = (
        (pl.col("close_1") > pl.col("open_1"))
        & (pl.col("close_2") < pl.col("close_1"))
        & (pl.col("close_3") < pl.col("close_1"))
        & (pl.col("close_4") < pl.col("close_1"))
        & (pl.col("close_hfq") > pl.col("close_1"))
    )
    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(1).otherwise(0).alias(CDLMATHOLD_COLUMN),
    )
