from __future__ import annotations

import polars as pl


CDLDOJI_COLUMN = "cdldoji"


def compute_cdldoji(frame: pl.DataFrame) -> pl.DataFrame:
    body = (pl.col("close_hfq") - pl.col("open_hfq")).abs()
    range_ = pl.col("high_hfq") - pl.col("low_hfq")
    signal = body <= range_ * 0.1
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(1).otherwise(0).alias(CDLDOJI_COLUMN),
    )
