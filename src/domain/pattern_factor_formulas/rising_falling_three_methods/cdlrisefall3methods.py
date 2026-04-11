from __future__ import annotations

import polars as pl


CDLRISEFALL3METHODS_COLUMN = "cdlrisefall3methods"


def compute_cdlrisefall3methods(frame: pl.DataFrame) -> pl.DataFrame:
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
            ]
        )
    )
    body1 = (pl.col("close_1") - pl.col("open_1")).abs()
    bullish = (
        (pl.col("close_1") > pl.col("open_1"))
        & (pl.col("close_2") < pl.col("open_2"))
        & (pl.col("close_3") < pl.col("open_3"))
        & (pl.col("close_4") < pl.col("open_4"))
        & (pl.max_horizontal("open_2", "close_2") < pl.col("close_1"))
        & (pl.max_horizontal("open_3", "close_3") < pl.col("close_1"))
        & (pl.max_horizontal("open_4", "close_4") < pl.col("close_1"))
        & (pl.col("close_hfq") > pl.col("open_hfq"))
        & (pl.col("close_hfq") > pl.col("close_1"))
        & ((pl.col("close_hfq") - pl.col("open_hfq")).abs() >= body1 * 0.6)
    )
    bearish = (
        (pl.col("close_1") < pl.col("open_1"))
        & (pl.col("close_2") > pl.col("open_2"))
        & (pl.col("close_3") > pl.col("open_3"))
        & (pl.col("close_4") > pl.col("open_4"))
        & (pl.min_horizontal("open_2", "close_2") > pl.col("close_1"))
        & (pl.min_horizontal("open_3", "close_3") > pl.col("close_1"))
        & (pl.min_horizontal("open_4", "close_4") > pl.col("close_1"))
        & (pl.col("close_hfq") < pl.col("open_hfq"))
        & (pl.col("close_hfq") < pl.col("close_1"))
        & ((pl.col("close_hfq") - pl.col("open_hfq")).abs() >= body1 * 0.6)
    )
    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(bullish).then(1).when(bearish).then(-1).otherwise(0).alias(CDLRISEFALL3METHODS_COLUMN),
    )
