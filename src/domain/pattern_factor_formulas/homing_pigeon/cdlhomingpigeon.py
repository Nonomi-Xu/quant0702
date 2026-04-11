from __future__ import annotations

import polars as pl


CDLHOMINGPIGEON_COLUMN = "cdlhomingpigeon"


def compute_cdlhomingpigeon(frame: pl.DataFrame) -> pl.DataFrame:
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(
            [
                pl.col("open_hfq").shift(1).over("ts_code").alias("open_1"),
                pl.col("close_hfq").shift(1).over("ts_code").alias("close_1"),
            ]
        )
    )
    prev_high = pl.max_horizontal("open_1", "close_1")
    prev_low = pl.min_horizontal("open_1", "close_1")
    curr_high = pl.max_horizontal("open_hfq", "close_hfq")
    curr_low = pl.min_horizontal("open_hfq", "close_hfq")
    signal = (
        (pl.col("close_1") < pl.col("open_1"))
        & (pl.col("close_hfq") < pl.col("open_hfq"))
        & (curr_high <= prev_high)
        & (curr_low >= prev_low)
    )
    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(1).otherwise(0).alias(CDLHOMINGPIGEON_COLUMN),
    )
