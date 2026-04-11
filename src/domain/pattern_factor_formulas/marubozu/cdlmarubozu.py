from __future__ import annotations

import polars as pl


CDLMARUBOZU_COLUMN = "cdlmarubozu"


def compute_cdlmarubozu(frame: pl.DataFrame) -> pl.DataFrame:
    range_ = pl.col("high_hfq") - pl.col("low_hfq")
    bullish = (
        (pl.col("close_hfq") > pl.col("open_hfq"))
        & ((pl.col("high_hfq") - pl.col("close_hfq")).abs() <= range_ * 0.05)
        & ((pl.col("open_hfq") - pl.col("low_hfq")).abs() <= range_ * 0.05)
    )
    bearish = (
        (pl.col("close_hfq") < pl.col("open_hfq"))
        & ((pl.col("high_hfq") - pl.col("open_hfq")).abs() <= range_ * 0.05)
        & ((pl.col("close_hfq") - pl.col("low_hfq")).abs() <= range_ * 0.05)
    )
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(bullish).then(1).when(bearish).then(-1).otherwise(0).alias(CDLMARUBOZU_COLUMN),
    )
