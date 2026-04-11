from __future__ import annotations

import polars as pl


CDLSTICKSANDWICH_COLUMN = "cdlsticksandwich"


def compute_cdlsticksandwich(frame: pl.DataFrame) -> pl.DataFrame:
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("open_hfq").shift(2).over("ts_code").alias("open_1"),
                pl.col("close_hfq").shift(2).over("ts_code").alias("close_1"),
                pl.col("open_hfq").shift(1).over("ts_code").alias("open_2"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_2"),
            ]
        )
    )
    signal = (
        (pl.col("close_1") < pl.col("open_1"))
        & (pl.col("close_2") > pl.col("open_2"))
        & (pl.col("close_hfq") < pl.col("open_hfq"))
        & ((pl.col("close_hfq") - pl.col("close_1")).abs() <= pl.col("close_1").abs() * 0.01)
    )
    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(1).otherwise(0).alias(CDLSTICKSANDWICH_COLUMN),
    )
