from __future__ import annotations

import polars as pl


CDLDARKCLOUDCOVER_COLUMN = "cdldarkcloudcover"


def compute_cdldarkcloudcover(frame: pl.DataFrame) -> pl.DataFrame:
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "high_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("open_hfq").shift(1).over("ts_code").alias("open_1"),
                pl.col("high_hfq").shift(1).over("ts_code").alias("high_1"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_1"),
                pl.col("open_hfq").alias("open_2"),
                pl.col("close_hfq").alias("close_2"),
            ]
        )
    )

    midpoint_1 = (pl.col("open_1") + pl.col("close_1")) / 2
    signal = (
        (pl.col("close_1") > pl.col("open_1"))
        & (pl.col("open_2") > pl.col("high_1"))
        & (pl.col("close_2") < pl.col("open_2"))
        & (pl.col("close_2") < midpoint_1)
        & (pl.col("close_2") > pl.col("open_1"))
    )

    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(-1).otherwise(0).alias(CDLDARKCLOUDCOVER_COLUMN),
    )
