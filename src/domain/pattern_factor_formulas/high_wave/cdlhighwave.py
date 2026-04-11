from __future__ import annotations

import polars as pl


CDLHIGHWAVE_COLUMN = "cdlhighwave"


def compute_cdlhighwave(frame: pl.DataFrame) -> pl.DataFrame:
    body = (pl.col("close_hfq") - pl.col("open_hfq")).abs()
    upper_shadow = pl.col("high_hfq") - pl.max_horizontal("open_hfq", "close_hfq")
    lower_shadow = pl.min_horizontal("open_hfq", "close_hfq") - pl.col("low_hfq")
    signal = (upper_shadow >= body * 2) & (lower_shadow >= body * 2)
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(1).otherwise(0).alias(CDLHIGHWAVE_COLUMN),
    )
