from __future__ import annotations

import polars as pl


CDLHAMMER_COLUMN = "cdlhammer"


def compute_cdlhammer(frame: pl.DataFrame) -> pl.DataFrame:
    body = (pl.col("close_hfq") - pl.col("open_hfq")).abs()
    upper_shadow = pl.col("high_hfq") - pl.max_horizontal("open_hfq", "close_hfq")
    lower_shadow = pl.min_horizontal("open_hfq", "close_hfq") - pl.col("low_hfq")
    signal = (lower_shadow >= body * 2) & (upper_shadow <= body * 0.5)
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(1).otherwise(0).alias(CDLHAMMER_COLUMN),
    )
