from __future__ import annotations

import polars as pl


CDLTAKURI_COLUMN = "cdltakuri"


def compute_cdltakuri(frame: pl.DataFrame) -> pl.DataFrame:
    body = (pl.col("close_hfq") - pl.col("open_hfq")).abs()
    range_ = pl.col("high_hfq") - pl.col("low_hfq")
    upper_shadow = pl.col("high_hfq") - pl.max_horizontal("open_hfq", "close_hfq")
    lower_shadow = pl.min_horizontal("open_hfq", "close_hfq") - pl.col("low_hfq")
    signal = (
        (body <= range_ * 0.08)
        & (upper_shadow <= range_ * 0.1)
        & (lower_shadow >= range_ * 0.6)
    )
    return frame.select(
        "trade_date",
        "ts_code",
        pl.when(signal).then(1).otherwise(0).alias(CDLTAKURI_COLUMN),
    )
