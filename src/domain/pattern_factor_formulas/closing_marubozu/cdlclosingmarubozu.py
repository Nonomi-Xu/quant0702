from __future__ import annotations

import polars as pl


CDLCLOSINGMARUBOZU_COLUMN = "cdlclosingmarubozu"


def compute_cdlclosingmarubozu(frame: pl.DataFrame) -> pl.DataFrame:
    bullish = (
        (pl.col("close_hfq") > pl.col("open_hfq"))
        & ((pl.col("close_hfq") - pl.col("high_hfq")).abs() <= ((pl.col("high_hfq") - pl.col("low_hfq")) * 0.03))
    )
    bearish = (
        (pl.col("close_hfq") < pl.col("open_hfq"))
        & ((pl.col("close_hfq") - pl.col("low_hfq")).abs() <= ((pl.col("high_hfq") - pl.col("low_hfq")) * 0.03))
    )
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(bullish).then(1).when(bearish).then(-1).otherwise(0).alias(CDLCLOSINGMARUBOZU_COLUMN),
    )
