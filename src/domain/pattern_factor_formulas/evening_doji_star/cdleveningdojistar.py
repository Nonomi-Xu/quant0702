from __future__ import annotations

import polars as pl


CDLEVENINGDOJISTAR_COLUMN = "cdleveningdojistar"


def compute_cdleveningdojistar(frame: pl.DataFrame) -> pl.DataFrame:
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "high_hfq", "low_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("open_hfq").shift(2).over("ts_code").alias("open_1"),
                pl.col("close_hfq").shift(2).over("ts_code").alias("close_1"),
                pl.col("high_hfq").shift(1).over("ts_code").alias("high_2"),
                pl.col("low_hfq").shift(1).over("ts_code").alias("low_2"),
                pl.col("open_hfq").shift(1).over("ts_code").alias("open_2"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_2"),
                pl.col("open_hfq").alias("open_3"),
                pl.col("close_hfq").alias("close_3"),
            ]
        )
    )
    body_2 = (pl.col("close_2") - pl.col("open_2")).abs()
    range_2 = pl.col("high_2") - pl.col("low_2")
    is_doji_2 = body_2 <= range_2 * 0.1
    midpoint_1 = (pl.col("open_1") + pl.col("close_1")) / 2
    signal = (
        (pl.col("close_1") > pl.col("open_1"))
        & is_doji_2
        & (pl.col("close_3") < pl.col("open_3"))
        & (pl.col("close_3") < midpoint_1)
    )
    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(-1).otherwise(0).alias(CDLEVENINGDOJISTAR_COLUMN),
    )
