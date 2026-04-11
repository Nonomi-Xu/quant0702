from __future__ import annotations

import polars as pl


CDLADVANCEBLOCK_COLUMN = "cdladvanceblock"


def compute_cdladvanceblock(frame: pl.DataFrame) -> pl.DataFrame:
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

    body1 = (pl.col("close_1") - pl.col("open_1")).abs()
    body2 = (pl.col("close_2") - pl.col("open_2")).abs()
    body3 = (pl.col("close_3") - pl.col("open_3")).abs()
    signal = (
        (pl.col("close_1") > pl.col("open_1"))
        & (pl.col("close_2") > pl.col("open_2"))
        & (pl.col("close_3") > pl.col("open_3"))
        & (pl.col("close_2") > pl.col("close_1"))
        & (pl.col("close_3") > pl.col("close_2"))
        & (body1 > body2)
        & (body2 > body3)
    )

    return ordered.select("trade_date", "ts_code", pl.when(signal).then(-1).otherwise(0).alias(CDLADVANCEBLOCK_COLUMN))
