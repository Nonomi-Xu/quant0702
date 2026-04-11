from __future__ import annotations

import polars as pl


CDLBELTHOLD_COLUMN = "cdlbelthold"


def compute_cdlbelthold(frame: pl.DataFrame) -> pl.DataFrame:
    ordered = frame.select("trade_date", "ts_code", "open_hfq", "high_hfq", "low_hfq", "close_hfq")
    bullish = (
        (pl.col("close_hfq") > pl.col("open_hfq"))
        & ((pl.col("open_hfq") - pl.col("low_hfq")).abs() <= ((pl.col("high_hfq") - pl.col("low_hfq")) * 0.05))
    )
    bearish = (
        (pl.col("close_hfq") < pl.col("open_hfq"))
        & ((pl.col("open_hfq") - pl.col("high_hfq")).abs() <= ((pl.col("high_hfq") - pl.col("low_hfq")) * 0.05))
    )
    return ordered.select(
        "trade_date",
        "ts_code",
        pl.when(bullish).then(1).when(bearish).then(-1).otherwise(0).alias(CDLBELTHOLD_COLUMN),
    )
