from __future__ import annotations

import polars as pl


CDLSHOOTINGSTAR_COLUMN = "cdlshootingstar"


def compute_cdlshootingstar(frame: pl.DataFrame) -> pl.DataFrame:
    body = (pl.col("close_hfq") - pl.col("open_hfq")).abs()
    upper_shadow = pl.col("high_hfq") - pl.max_horizontal("open_hfq", "close_hfq")
    lower_shadow = pl.min_horizontal("open_hfq", "close_hfq") - pl.col("low_hfq")
    ordered = (
        frame.select("trade_date", "ts_code", "open_hfq", "high_hfq", "low_hfq", "close_hfq")
        .sort(["ts_code", "trade_date"])
        .with_columns(pl.col("close_hfq").shift(1).over("ts_code").alias("close_1"))
    )
    signal = (
        (upper_shadow >= body * 2)
        & (lower_shadow <= body * 0.5)
        & (pl.col("close_1").is_not_null())
        & (pl.col("close_1") < pl.col("close_hfq"))
    )
    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(-1).otherwise(0).alias(CDLSHOOTINGSTAR_COLUMN),
    )
